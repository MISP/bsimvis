"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""

import json
import math
import re
import time
import zlib
from collections import Counter, defaultdict

from bsimvis.app.services.tag_taxonomy import tag_body, tag_policy, tag_prefixes
from bsimvis.app.services.index_service import normalize_timestamp

MAX_CLUSTER_NAME_LEN = 40
# Plan D5b: yara/avtype/ccip retired from cluster meta -- av:/yara:/ip: tags
# in tag_distribution cover the same ground (Decision 8). filetype is not
# here because cluster_dimensions() already builds filetype_distribution off
# the same (kept, Decision 5) raw field; listing it again just double-built
# an identical distribution.
DISTRIBUTION_FIELDS = (
    "filename",
    "md5",
)


def _tag_distribution_label(roots, min_coverage=0.0):
    """Most prevalent leaf under a `tag_distribution` axis, short-form, or None.

    Walks the highest-count child at each level -- same "most common wins"
    rule `Counter.most_common(1)` gave the legacy field lists -- and returns
    only the tag's last segment: `av:clamav:mirai` -> `mirai`, matching the
    short AV-family-style names this function has always produced.
    """
    if not roots:
        return None
    node = max(roots, key=lambda n: n.get("count", 0))
    while node.get("children"):
        node = max(node["children"], key=lambda c: c.get("count", 0))
    if node.get("coverage", 1.0) < min_coverage:
        return None
    tag_id = node.get("tag_id") or ""
    return tag_id.rsplit(":", 1)[-1] if tag_id else None


NAME_ANCHORS = 20
USER_TAG_NAME_MIN = 0.2  # share of members a user tag needs to name a cluster


def _member_key(m):
    """Member id without its collection / pool prefix, so builds and routes agree."""
    for sep in (":func:", ":file:"):
        if sep in m:
            return m.split(sep, 1)[1]
    return m


def _names_hash(prefix, kind):
    return f"{prefix}:cluster_names:{kind}"


def _name_ns(algo):
    """Names namespace: hierarchical_uf and hierarchical_snn cluster the same
    files, so a name survives a switch between the two engines."""
    return algo.replace(":snn", "")


def _same_ns(field, algo):
    return _name_ns(field.split("|", 1)[0]) == _name_ns(algo)


def _name_field(algo, e):
    # size too: a parent and a child can share the same anchors
    crc = zlib.crc32(",".join(e["anchors"]).encode())
    return f"{_name_ns(algo)}|{crc:08x}:{e['size']}"


def _name_entry(name, members, label=None):
    keys = sorted(
        {_member_key(m) for m in members}, key=lambda k: zlib.crc32(k.encode())
    )
    e = {"name": name, "size": len(keys), "anchors": keys[:NAME_ANCHORS]}
    if label is not None:
        e["label"] = str(label)
    return e


def save_cluster_name(r, prefix, kind, algo, members, name, replaces=None, label=None):
    """Remember an analyst name by a stable sample of its members (survives rebuilds).

    kind is "func" or "bin"; prefix is the collection or `global:pool:{id}`.
    An empty name forgets it. `replaces` is the cluster's previous custom name:
    only entries with that name that anchor some of the same members are dropped,
    so naming a sub-cluster never erases the name of the family around it.
    `label` is the cluster id, preferred by an incremental rebuild (stable ids)."""
    h = _names_hash(prefix, kind)
    e = _name_entry(name, members, label)
    for f, raw in (r.hgetall(h) or {}).items():
        f = f.decode() if isinstance(f, bytes) else f
        old = json.loads(raw)
        if (
            replaces
            and _same_ns(f, algo)
            and old["name"] == replaces
            and set(old["anchors"]) & set(e["anchors"])
        ):
            r.hdel(h, f)
    if name and e["anchors"]:
        r.hset(h, _name_field(algo, e), json.dumps(e))


class ClusterNameKeeper:
    """Analyst names saved by `save_cluster_name`, matched to a fresh build's clusters.

    A cluster takes a saved name when it holds at least half the anchors. With
    `stable_ids` (an incremental hierarchical run: node ids carry over) the
    cluster with the saved id wins; otherwise the size closest to the saved size
    (so a parent does not take a child's name). `pick(label, default)` ->
    (name, is_custom). `reanchor(labels)` moves the kept names onto those
    clusters' current members, so a name follows its cluster as it grows,
    shrinks or loses deleted files.

    `cluster_members` must hold every cluster the build has (every tree node),
    not only the written ones, or a name can land on the wrong node."""

    def __init__(self, r, prefix, kind, algo, cluster_members, stable_ids=False):
        self.names, self.matched = {}, {}
        self.r, self.h, self.algo = r, _names_hash(prefix, kind), algo
        self.members = cluster_members
        raw = r.hgetall(self.h) or {}
        self.entries = []  # (field, entry)
        for f, v in raw.items():
            f = f.decode() if isinstance(f, bytes) else f
            if _same_ns(f, algo):
                self.entries.append((f, json.loads(v)))
        if not self.entries:
            return
        where = defaultdict(list)  # anchor -> entry indexes
        for i, (_, e) in enumerate(self.entries):
            for a in e["anchors"]:
                where[a].append(i)
        hits = defaultdict(Counter)  # label -> entry -> anchors held
        sizes = {}
        for label, members in cluster_members.items():
            sizes[label] = len(members)
            for m in members:
                for i in where.get(_member_key(m), ()):
                    hits[label][i] += 1
        cands = []
        for label, cnt in hits.items():
            for i, n in cnt.items():
                e = self.entries[i][1]
                if n * 2 >= len(e["anchors"]):
                    same = stable_ids and e.get("label") == str(label)
                    cands.append(
                        (
                            0 if same else 1,
                            abs(sizes[label] - e["size"]) / max(e["size"], 1),
                            -n,
                            str(label),
                            label,
                            i,
                        )
                    )
        used = set()
        for *_, label, i in sorted(cands):  # same id, then closest size
            if label not in self.names and i not in used:
                self.names[label] = self.entries[i][1]["name"]
                self.matched[label] = i
                used.add(i)

    def pick(self, label, default):
        name = self.names.get(label)
        return (name, True) if name else (default, False)

    def reanchor(self, labels):
        for label in labels:
            i = self.matched.get(label)
            if i is None or not self.members.get(label):
                continue
            f, old = self.entries[i]
            e = _name_entry(old["name"], self.members[label], label)
            nf = _name_field(self.algo, e)
            if nf != f:
                self.r.hdel(self.h, f)
            self.r.hset(self.h, nf, json.dumps(e))
            self.entries[i] = (nf, e)


def default_bin_cluster_name(
    names_list, avtype_list, yara_list, fallback, tag_distribution=None
):
    """Short, human-meaningful default name for a binary cluster.

    A raw member filename (the old default) is often a long malware-scanner
    submission name, which reads as noise in a cluster list. AV family labels
    ('Emotet', 'Gafgyt') and YARA rule names are short and already describe
    what the cluster *is*, so prefer them; fall back to a truncated filename,
    then to the caller's generic placeholder.

    Plan D, Decision 8: the source is `tag_distribution`'s `family` then
    `yara` axis (mint on write's av:/yara: tags) when the caller has one,
    same preference order as the legacy `avtype_list`/`yara_list` it replaces.
    Callers that have not fully switched over may omit `tag_distribution`,
    which falls back to the legacy lists untouched.
    """
    if tag_distribution:
        label = (
            _tag_distribution_label(tag_distribution.get("user"), USER_TAG_NAME_MIN)
            or _tag_distribution_label(tag_distribution.get("family"))
            or _tag_distribution_label(tag_distribution.get("yara"))
        )
        if label:
            return label
    if avtype_list:
        return Counter(avtype_list).most_common(1)[0][0]
    if yara_list:
        return Counter(yara_list).most_common(1)[0][0]
    if names_list:
        name = Counter(names_list).most_common(1)[0][0]
        if len(name) > MAX_CLUSTER_NAME_LEN:
            name = name[: MAX_CLUSTER_NAME_LEN - 3] + "..."
        return name
    return fallback


# -- automatic file-cluster names --------------------------------------------
# Name = head [· qualifier [· qualifier]]: the family (AV / family: tags, else an
# unambiguous YARA family, else the parent's), then what sets the cluster apart
# from its parent (lift): build stem from file names ("DEMONS", "p*", "*net"),
# a readable YARA trait, architecture, packer, size. Prototype + evaluation on
# more_mirai_yara: 14 exact / 5 close / 1 wrong of 20 against analyst names.

_ARCH = {
    "ARM": "ARM",
    "AARCH64": "AArch64",
    "x86:LE:32": "x86",
    "x86:LE:64": "x86-64",
    "MIPS:LE": "MIPSel",
    "MIPS:BE": "MIPS",
    "PowerPC": "PowerPC",
    "SuperH4": "SH4",
    "68000": "m68k",
    "sparc": "SPARC",
    "RISCV": "RISC-V",
    "Loongarch": "LoongArch",
}
_NOT_FAMILY = {
    "unknown", "generic", "threat", "anomaly", "obfuscation", "ok", "trojan",
    "botnet", "linux", "elf", "backdoor", "exploit", "malware", "virus", "hacktool",
}  # fmt: skip
_FAMILY_RULE = re.compile(r"(?:Backdoor|Trojan|HackTool|DDoS)_Linux_([A-Za-z]+)_", re.I)
_ELASTIC_RULE = re.compile(r"^Linux_(?!Generic)[A-Za-z]+_([A-Za-z]+)_[0-9a-f]{8}$")
_ELF_FAMILY = re.compile(r"^ELF_([A-Za-z]+)$")
_HEX = re.compile(r"^[0-9a-f]{32,64}$")
_ARCH_TOK = re.compile(
    r"(x86_64|x86|i[3-6]86|amd64|aarch64|arm64|arm\d*|mipsel|mpsl|mips64|mips|"
    r"powerpc|ppc|sh4|sh2|m68k|sparc|spc|riscv\d*|loongarch\d*|arc|mipseb)"
)
_GENERIC_RULE = re.compile(r"^Linux_Botnet_Generic_(\w+)$")
_HASHY = re.compile(r"_[0-9a-f]{8}$")
_SKIP_TRAITS = {
    "XorObfuscatedConfig",
    "UPX_Protector",
    "Antivirus",
    "find",
    "stack_string",
}
_BASIS = {
    "stem": "files",
    "trait": "yara",
    "arch": "arch",
    "packer": "packer",
    "size": "size",
}


def _arch_of(m):
    lid = m.get("language_id") or ""
    for k, v in _ARCH.items():
        if lid.startswith(k):
            return v
    return lid.split(":")[0] or None


def _fam(tok):
    tok = (tok or "").strip().lower()
    if not tok or tok in _NOT_FAMILY or tok.startswith("cve") or len(tok) < 3:
        return None
    return tok


def _rule_of(parts):
    return parts[2].split("#", 1)[1] if "#" in parts[2] else ":".join(parts[3:])


def _families_of(m):
    """(strong, weak) lowercase families of one file: strong = AV labels and
    family: tags, weak = from YARA rule names (they often cross-fire)."""
    av, other = set(), set()
    for t in m.get("tags") or []:
        parts = str(t).split(":")
        if parts[0] == "av" and len(parts) >= 3:
            f = _fam(parts[2].split("#")[0])
            if f:
                av.add(f)
        elif parts[0] == "family" and len(parts) >= 2:
            f = _fam(parts[1])
            if f:
                av.add(f)
        elif parts[0] == "yara" and len(parts) >= 3:
            f = _fam(parts[2].split("#")[0])
            if f:
                other.add(f)
            rule = _rule_of(parts)
            mr = (
                _FAMILY_RULE.search(rule)
                or _ELF_FAMILY.match(rule)
                or _ELASTIC_RULE.match(rule)
            )
            if mr and _fam(mr.group(1)):
                other.add(_fam(mr.group(1)))
    return av, other


def _name_base(name):
    """Sample name without path and packing suffix; None for md5 / sha names."""
    if not name:
        return None
    base = name.rsplit("/", 1)[-1]
    for suf in (".unpacked", ".elf", ".bin", ".zip"):
        if base.lower().endswith(suf):
            base = base[: -len(suf)]
    return None if _HEX.match(base.lower()) else base


def _collapse(s):
    rep = re.match(r"^(.{2,4}?)\1+$", s)  # xnxnxnxn -> xnxn
    return rep.group(1) * 2 if rep else s


def name_tokens(name):
    """Words of 3+ characters in a sample name: its arch words and the pieces
    between them ('xnxnxnxnm68kxnxn' -> {'xnxn', 'm68k'}, 'pm68k' -> {'m68k'})."""
    base = _name_base(name)
    if not base:
        return set()
    low = base.lower()
    toks = {m.group(0) for m in _ARCH_TOK.finditer(low)}
    for piece in re.split(r"[^a-z0-9]+", _ARCH_TOK.sub(" ", low)):
        piece = _collapse(piece)
        if len(piece) >= 3 and not piece.isdigit():
            toks.add(piece)
    return toks


def stem_of(name):
    """Build label of a sample name: 'DEMONS.arm7' -> 'DEMONS',
    'xnxn..riscv64xnxn' -> 'xnxn'; None for md5 / sha names and for stems under
    3 characters ('pm68k' -> 'p' says nothing)."""
    base = _name_base(name)
    if not base:
        return None
    s = _ARCH_TOK.sub(" ", base.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    if not s:
        return None
    s = _collapse(s.split()[0])
    if s.isdigit() or len(s) < 3:
        return None
    m = re.search(re.escape(s), base, re.I)  # keep the name's case ('DEMONS')
    return base[m.start() : m.end()] if m else s


def _traits_of(m):
    """Readable YARA rule names: 'ELF_Toriilike_persist' -> 'Toriilike persist'."""
    out = set()
    for t in m.get("tags") or []:
        parts = str(t).split(":")
        if parts[0] != "yara" or len(parts) < 3:
            continue
        rule = _rule_of(parts)
        g = _GENERIC_RULE.match(rule)
        if g:
            rule = g.group(1)
        if (
            not rule
            or rule in _SKIP_TRAITS
            or _HASHY.search(rule)
            or _FAMILY_RULE.search(rule)
            or _ELF_FAMILY.match(rule)
            or rule.startswith(
                ("SUSP_", "MAL_", "Linux_Trojan", "Linux_Generic", "Linux_Exploit")
            )
        ):
            continue
        words = rule.replace("ELF_", "").replace("_", " ").split()
        out.add(" ".join(words[:3])[:24])
    return out


def _file_features(m):
    """-> (features {(kind, value)}, strong families, weak families) of one file."""
    av, other = _families_of(m)
    arch = _arch_of(m)
    f = {("arch", arch)} if arch else set()
    names = list(m.get("file_names") or []) + [m.get("file_name")]
    f |= {("stem", s) for s in map(stem_of, names) if s}
    f |= {("tok", t) for nm in names for t in name_tokens(nm)}
    f |= {("trait", t) for t in _traits_of(m)}
    tags = [str(t) for t in m.get("tags") or []]
    if any(t.startswith("packer:upx") for t in tags):
        f.add(("packer", "UPX"))
    n = m.get("function_count") or 0
    if isinstance(n, (int, float)) and n:
        if n < 20:
            f.add(("size", "stub"))
        elif n >= 1000:
            f.add(("size", "large"))
    botnet = any(t.startswith("yara:botnet:") for t in tags)
    return f, av, other, botnet


def _shares(feats):
    c = Counter(x for f in feats for x in f)
    return {k: v / max(1, len(feats)) for k, v in c.items()}


def _family_head(fams, weak_ok=True, plurality=False, compat=None):
    """fams: per member (strong, weak) -> (head, families, basis) or (None, set(), None).
    `plurality`: 40% and twice the runner-up is enough (a parent's lineage).
    `compat` (BinClusterNamer._cofire) breaks a tie between YARA families that
    all fire: Gafgyt rules hit Mirai-labelled files across the collection, Mirai
    rules do not hit Gafgyt-labelled ones, so Mirai + Gafgyt rules is Mirai."""
    n = len(fams)
    strong = Counter(f for st, _ in fams for f in st)
    top = [(f, c / n) for f, c in strong.most_common(2)]
    second = top[1][1] if len(top) > 1 else 0
    if top and (
        top[0][1] > 0.5 or plurality and top[0][1] >= 0.4 and top[0][1] >= 2 * second
    ):
        head, basis = [top[0][0]] + [f for f, sh in top[1:] if sh >= 0.3], "av"
    elif not weak_ok:
        return None, set(), None
    else:
        # YARA families cross-fire (Gafgyt rules hit Mirai): only an unambiguous one
        weak = Counter(f for _, wk in fams for f in wk)
        w = [(f, c / n) for f, c in weak.most_common(2)]
        if not w or w[0][1] < 0.8:
            return None, set(), None
        if len(w) > 1 and w[1][1] >= 0.5:
            cx = compat or {}
            wide = [f for f, c in weak.items() if c / n >= 0.5]
            keep = [
                f
                for f in wide
                if not any(
                    cx.get((f, g), 0) >= 0.5 and cx.get((g, f), 0) < 0.5
                    for g in wide
                    if g != f
                )
            ]
            if len(keep) != 1 or weak[keep[0]] / n < 0.8:
                return None, set(), None
            w = [(keep[0], 1.0)]
        head, basis = [w[0][0]], "yara"
    pretty = lambda f: f.capitalize() if f.islower() else f
    return " + ".join(pretty(f) for f in head), set(head), basis


def _shared_suffix(stems, min_share=0.5, min_len=3):
    """'cracknet', 'weednet', 'swatnet' -> '*net' when most stems end the same."""
    stems = sorted({s.lower() for s in stems if len(s) > min_len})
    if len(stems) < 3:
        return None
    best = None
    for L in range(min_len, 6):
        c = Counter(s[-L:] for s in stems if len(s) > L)
        if not c:
            break
        suf, k = c.most_common(1)[0]
        if k / len(stems) >= min_share:
            best = suf
    return f"*{best}" if best else None


class BinClusterNamer:
    """Automatic names for file clusters: `name(members, parent)` -> (name, basis).

    members / parent are lists of file metas (`{coll}:file:{md5}:meta`); parent
    is the tree parent's members (None for a root). `collection` is every
    clustered file's meta: the reference of a root, and where a YARA family's
    cross-firing on a parent family is measured. basis lists what the name was
    built from: av, yara, files, arch, size, packer, parent.

    Features, shares and root names are memoised per object (the object is held,
    so its id is never reused): pass the same meta and parent lists each time."""

    def __init__(self, collection=()):
        self.collection = list(collection)
        self._memo = {}

    def _cached(self, kind, obj, fn):
        k = (kind, id(obj))
        if k not in self._memo:
            self._memo[k] = (obj, fn(obj))
        return self._memo[k][1]

    def _feat(self, m):
        return self._cached("f", m, _file_features)

    def _cofire(self, metas, labelled=False):
        """{(weak, strong): share of the files with that weak family that carry
        the strong one}: a YARA family cross-firing on another family's builds.
        `labelled`: count only files that carry some strong label (unpacked
        payloads have none, and would dilute the share)."""

        def build(metas):
            weak_n, both = Counter(), Counter()
            for m in metas:
                _, st, wk, _ = self._feat(m)
                if labelled and not st:
                    continue
                for f in wk:
                    weak_n[f] += 1
                    for g in st:
                        both[(f, g)] += 1
            return {k: v / weak_n[k[0]] for k, v in both.items()}

        return self._cached("cl" if labelled else "c", metas, build)

    def name(self, members, parent=None, _root=False):
        if not members:
            return None, []
        per = [self._feat(m) for m in members]
        feats = [p[0] for p in per]
        n = len(members)
        mine = _shares(feats)
        own = [(p[1], p[2]) for p in per]
        head, head_fams, hb = _family_head(own, weak_ok=False)
        basis = [hb] if hb else []
        if head is None and parent:
            # family is a lineage property: unpacked payloads carry no AV label,
            # so take the parent's when some members show it in a tag
            ph, pf, _ = _family_head(
                [self._feat(m)[1:3] for m in parent], weak_ok=False, plurality=True
            )
            compat = self._cofire(self.collection or parent)
            evidence = (
                sum(
                    1
                    for (st, wk), p in zip(own, per)
                    if any(
                        f in pf or compat.get((f, g), 0) >= 0.3
                        for f in st | wk
                        for g in pf
                    )
                    or (not st and not wk and p[3])
                )
                / n
            )
            if ph and evidence >= 0.3:
                head, head_fams, basis = ph, pf, ["parent"]
        if head is None:
            head, head_fams, hb = _family_head(
                own,
                compat=self._cofire(
                    self.collection or parent or members, labelled=True
                ),
            )
            basis = [hb] if hb else []

        ref = parent or self.collection or members
        theirs = self._cached(
            "s", ref, lambda ref: _shares([self._feat(m)[0] for m in ref])
        )
        floor = 1 / max(2, len(ref))

        def score(k):
            lift = mine.get(k, 0) / max(theirs.get(k, 0), floor)
            return mine.get(k, 0) * math.log(max(lift, 1.0001))

        cands = []  # (score, text, kind)
        # architecture: one arch >= 80%, or two >= 90% together, else "N archs"
        archs = Counter(x[1] for f in feats for x in f if x[0] == "arch")
        if (
            archs
            and max(archs.values()) / n < 0.8
            and archs.get("MIPS")
            and archs.get("MIPSel")
        ):
            archs["MIPS"] += archs.pop("MIPSel")
        a = archs.most_common(2)
        arch_txt = None
        if a and a[0][1] / n >= 0.8:
            arch_txt = a[0][0]
            cands.append((score(("arch", a[0][0])) or 0.03, arch_txt, "arch"))
        elif len(a) == 2 and (a[0][1] + a[1][1]) / n >= 0.9:
            arch_txt = f"{a[0][0]}/{a[1][0]}"
            sc = min(score(("arch", a[0][0])), score(("arch", a[1][0])))
            cands.append((max(sc, 0.03), arch_txt, "arch"))
        elif len(archs) >= 4:
            arch_txt = f"{len(archs)} archs"
            cands.append((0.03, arch_txt, "arch"))

        # build stems from file names: the top one, two when both are common
        stems = Counter(x[1] for f in feats for x in f if x[0] == "stem")
        st = [(s, c / n) for s, c in stems.most_common(2) if c / n >= 0.3]
        pretty = lambda s: f"{s}*" if len(s) <= 2 else s
        # a word in (nearly) every file name beats build stems that split the
        # cluster: 'xnxn..m68kxnxn' + 'pm68k' -> 'm68k', not 'p* + xnxn'
        shared = max(
            ((c, t) for (k, t), c in mine.items() if k == "tok" and c >= 0.8),
            default=None,
        )
        # files with a word in their name but no stem ('pm68k') are one group
        bare = sum(
            1
            for f in feats
            if any(x[0] == "tok" for x in f) and not any(x[0] == "stem" for x in f)
        )
        top2 = stems.most_common(2)
        lead = top2[0][1] if top2 else 0
        runner = max(top2[1][1] if len(top2) > 1 else 0, bare)
        if shared and not (lead >= 0.4 * n and lead >= 2 * runner):
            tok = shared[1]
            if not _ARCH_TOK.fullmatch(tok):
                cands.append((score(("tok", tok)) * 1.5, tok, "stem"))
            elif not arch_txt:
                cands.append((score(("tok", tok)) or 0.03, tok, "arch"))
        elif (
            st and st[0][1] >= 0.3
        ):  # no shared word: a 30% build stem still says something
            txt = pretty(st[0][0])
            if len(st) == 2:
                txt += " + " + pretty(st[1][0])
            cands.append((score(("stem", st[0][0])) * 1.5, txt, "stem"))
        else:
            suf = _shared_suffix(list(stems))
            if suf:
                cands.append((0.6, suf, "stem"))

        fc_median = lambda: sorted(m.get("function_count") or 0 for m in members)[
            n // 2
        ]
        for kind in ("trait", "packer", "size"):
            best = max(
                ((score(k), k[1]) for k in mine if k[0] == kind and mine[k] >= 0.6),
                default=None,
            )
            if best and best[0] > 0.05 and best[1] != "large":
                cands.append((best[0], best[1], kind))
            if kind == "size" and mine.get(("size", "large"), 0) >= 0.6:
                # intrinsic: a big program stays labelled even under a big parent
                cands = [c for c in cands if c[2] != "size"]
                txt = f"{fc_median() / 1000:.1f}k-func"
                cands.append((max(best[0] if best else 0, 0.1), txt, "size"))

        cands.sort(key=lambda c: -c[0])
        # qualifiers repeating the family ("Mirai Botnet") add nothing
        cands = [
            c for c in cands if not set(re.findall(r"[a-z]+", c[1].lower())) & head_fams
        ]

        if mine.get(("size", "stub"), 0) >= 0.5:
            # packer stubs: what they are beats whose payload they carry
            upx = mine.get(("packer", "UPX"), 0) >= 0.5
            head = "UPX stubs" if upx else "Stubs"
            basis = ["size", "packer"] if upx else ["size"]
            cands = [c for c in cands if c[2] not in ("packer", "size", "trait")]
        elif head is None:
            big = [c for c in cands if c[2] == "size"]
            if big:
                head, basis = f"{big[0][1]} program", ["size"]
                cands.remove(big[0])
            elif mine.get(("size", "large"), 0) >= 0.6:
                head, basis = f"{fc_median() / 1000:.1f}k-func program", ["size"]
            elif cands:
                c = cands.pop(0)
                head, basis = c[1], [_BASIS[c[2]]]
            elif a:
                head, basis = f"{a[0][0]} program", ["arch"]
            else:
                return None, []

        parts, used = [head], set()
        for sc, txt, kind in cands:
            if len(parts) == 3 or kind in used or sc <= 0.02:
                continue
            if kind == "packer" and len(parts) > 1:
                continue
            if len(" · ".join(parts + [txt])) > MAX_CLUSTER_NAME_LEN:
                continue
            parts.append(txt)
            used.add(kind)
        # a bare family says less than its parent: keep the arch it shares
        if len(parts) == 1 and arch_txt and arch_txt != head:
            parts.append(arch_txt)
            cands.append((0, arch_txt, "arch"))
        # chosen by score, shown in reading order: family · build · trait · arch · size
        order = {"stem": 0, "trait": 1, "arch": 2, "size": 3, "packer": 4}
        kind_of = {txt: kind for _, txt, kind in cands}
        parts = [parts[0]] + sorted(
            parts[1:], key=lambda t: order.get(kind_of.get(t), 9)
        )
        basis += [_BASIS[kind_of[t]] for t in parts[1:] if t in kind_of]
        name = " · ".join(parts)

        # a child must not read like its parent: add what is left, else "core"
        if parent and not _root:
            pname = self._cached("n", parent, lambda p: self.name(p, None, True)[0])
            if pname == name:
                if arch_txt and arch_txt not in parts:
                    name, basis = f"{name} · {arch_txt}", basis + ["arch"]
                else:  # a strict subset of a same-named parent: its tighter core
                    name, basis = f"{name} · core", basis + ["parent"]
        if len(name) > MAX_CLUSTER_NAME_LEN:
            name = name[: MAX_CLUSTER_NAME_LEN - 1] + "…"
        return name, list(dict.fromkeys(basis))


NAME_SOURCES = ("analyst", "near", "tag", "auto")


def bin_cluster_name_fields(namer, members, parent, tag_distribution, fallback):
    """Name fields of a freshly built file cluster: a user tag on enough members
    ("tag"), else the namer ("auto"), else `fallback` (the legacy family / YARA /
    file-name rule, when the metas give the namer nothing). `name_default` is what
    a reset or a cleared near name goes back to. `parent` (the tree parent's metas,
    or None) is passed as is: reuse one list per node, the namer memoises by it."""
    known = [m for m in members if m]
    auto, basis = (None, [])
    if known:
        auto, basis = namer.name(known, parent or None)
    if not auto:
        auto, basis = fallback, []
    tag = _tag_distribution_label(
        (tag_distribution or {}).get("user"), USER_TAG_NAME_MIN
    )
    d = (
        {"name": tag, "source": "tag", "basis": ["tag"]}
        if tag
        else {"name": auto, "source": "auto", "basis": basis}
    )
    return {
        "cluster_name": d["name"],
        "name_source": d["source"],
        "name_basis": d["basis"],
        "auto_name": auto,
        "name_default": d,
    }


def restore_default_name(meta, fallback):
    """Back to the build's name (reset, or a near name that no longer applies)."""
    d = meta.get("name_default") or {
        "name": meta.get("auto_name") or fallback,
        "source": "auto",
        "basis": meta.get("name_basis") or [],
    }
    meta.update(
        cluster_name=d["name"],
        name_source=d["source"],
        name_basis=d["basis"],
        is_custom_name=False,
    )
    meta.pop("name_ref", None)
    return meta


def name_fields(meta):
    """The name contract of a file cluster meta for API rows, legacy metas included."""
    meta = meta or {}
    custom = bool(meta.get("is_custom_name"))
    return {
        "is_custom_name": custom,
        "name_source": meta.get("name_source") or ("analyst" if custom else "auto"),
        "name_basis": meta.get("name_basis") or [],
        "name_ref": meta.get("name_ref"),
        "auto_name": meta.get("auto_name"),
    }


# -- near-identical name inheritance -----------------------------------------
NEAR_SPLIT = 3  # a sibling this big merged in along the path is a real split


def near_tol(size):
    return max(2, int(size * 0.1))


def _near_tree(links):
    size, parent, kids = {}, {}, defaultdict(list)
    for l in real_links(links):
        c, p = str(l["child"]), str(l["parent"])
        size[c], parent[c] = int(l.get("size") or 0), p
        kids[p].append(c)
    return size, parent, kids


def _near_walk(tree, n, take=lambda c: True):
    """Nodes in n's chain: up, then down, every step with no sibling of
    >= NEAR_SPLIT files and a size within near_tol of n's. `take(c)` False
    stops the walk at c (c excluded)."""
    size, parent, kids = tree
    tol = near_tol(size[n])

    def clean(p, c):  # every other child of p is small
        return all(size[k] < NEAR_SPLIT for k in kids[p] if k != c)

    def close(c):
        return abs(size[c] - size[n]) <= tol and take(c)

    out, c = [], n
    while c in parent and parent[c] in size and clean(parent[c], c):
        c = parent[c]
        if not close(c):
            break
        out.append(c)
    stack = [n]
    while stack:
        p = stack.pop()
        nxt = [c for c in kids.get(p, ()) if clean(p, c) and close(c)]
        out += nxt
        stack += nxt
    return out


def near_names(links, named):
    """{cid: (named cid, delta)} for unnamed tree nodes in the same chain as an
    analyst-named node: reached by walking only up or only down from it, every
    step with no sibling of >= NEAR_SPLIT files, and a size within near_tol of
    the named node's. The smallest |delta| wins; named nodes never take one.
    `links` is the tree_links blob (sizes are tree sizes); ids come back as str."""
    tree = _near_tree(links)
    size = tree[0]
    named = {str(c) for c in named} & set(size)
    best = {}
    for n in named:
        for c in _near_walk(tree, n, lambda c: c not in named):
            d = size[c] - size[n]
            if c not in best or (abs(d), n) < (abs(best[c][1]), best[c][0]):
                best[c] = (n, d)
    return best


def near_chain(links, cid):
    """The chain around `cid` by the near-name rule, named nodes included:
    {"chain": [cid, ...] largest first, "above": the parent past the top or
    None, "below": [children >= NEAR_SPLIT files that start a real split or
    leave the tolerance]}. None when cid is not in the tree."""
    tree = _near_tree(links)
    size, parent, kids = tree
    cid = str(cid)
    if cid not in size:
        return None
    chain = sorted([cid] + _near_walk(tree, cid), key=lambda c: (-size[c], c))
    inside = set(chain)
    top = chain[0]
    above = parent.get(top)
    below = sorted(
        {k for c in chain for k in kids.get(c, ()) if k not in inside}
        - {k for k in size if size[k] < NEAR_SPLIT},
        key=lambda c: (-size[c], c),
    )
    return {"chain": chain, "above": above, "below": below}


def _near_keys(collection, algo_ns):
    base = f"{collection}:bin_cluster"
    return f"{base}:named:{algo_ns}", f"{base}:near:{algo_ns}"


def refresh_near_names(r, collection, algo_ns):
    """Apply / clear near names after a build or a rename. Reads the tree_links
    blob, the named set and the near hash, and only the metas of named and
    (formerly) near nodes. -> {cid: named cid} now applied."""
    named_k, near_k = _near_keys(collection, algo_ns)
    named = {_s(c) for c in r.smembers(named_k)}
    old = {_s(k): _s(v) for k, v in (r.hgetall(near_k) or {}).items()}
    if not named and not old:
        return {}
    raw = r.get(f"{collection}:bin_cluster:tree_links:{algo_ns}")
    want = near_names(json.loads(raw), named) if raw and named else {}
    cids = sorted(named | set(want) | set(old))
    meta_k = lambda c: f"{collection}:bin_cluster:{algo_ns}:{c}:meta"
    metas = {}
    for i in range(0, len(cids), 500):
        chunk = cids[i : i + 500]
        for c, v in zip(chunk, r.mget([meta_k(c) for c in chunk])):
            metas[c] = json.loads(v) if v else None
    applied, pipe = {}, r.pipeline(transaction=False)
    for c, (n, _) in want.items():
        src, m = metas.get(n), metas.get(c)
        if not src or not src.get("is_custom_name") or not m or m.get("is_custom_name"):
            continue
        ref = {
            "cluster_id": src.get("cluster_id"),
            "cluster_uuid": src.get("cluster_uuid"),
            "cluster_name": src.get("cluster_name"),
            "delta": int(m.get("member_count") or 0)
            - int(src.get("member_count") or 0),
        }
        applied[c] = n
        if m.get("name_source") == "near" and m.get("name_ref") == ref:
            continue
        m.update(
            cluster_name=src["cluster_name"],
            name_source="near",
            name_basis=[],
            name_ref=ref,
            is_custom_name=False,
        )
        pipe.set(meta_k(c), json.dumps(m))
    for c in set(old) - set(applied):
        m = metas.get(c)
        if m and m.get("name_source") == "near":
            pipe.set(
                meta_k(c), json.dumps(restore_default_name(m, f"Binary Cluster {c}"))
            )
    pipe.delete(near_k)
    if applied:
        pipe.hset(near_k, mapping=applied)
    pipe.execute()
    return applied


def default_func_cluster_name(fids, metas, fallback):
    """Name of a function cluster, by the rule N-way rows use: an analyst name, else
    the most common non-default name (never FUN_/sub_/thunk_), else a name carried
    by the members' tags, else `fallback`. `metas` maps function id -> function meta."""
    from bsimvis.app.services.bin_sim_tags import normalize_tags
    from bsimvis.app.services.nway_diff_service import (
        _DEFAULT_NAME,
        _fn_meta,
        _name_row,
    )

    fmeta = {f: _fn_meta(metas.get(f) or {}) for f in fids}
    weights = {f: float(fmeta[f].get("bsim_features_count") or 0) or 1.0 for f in fids}
    row = _name_row(list(fids), fmeta, weights) if fids else {}
    name = row.get("name")
    if row.get("user_named") and name:
        return name
    # an analyst's tag on enough members beats tool-made names (yara, capa, fid)
    tags = Counter(
        t.rsplit(":", 1)[-1]
        for f in fids
        for t in normalize_tags((metas.get(f) or {}).get("user_tags"))
    )
    if tags:
        top, n = tags.most_common(1)[0]
        if n >= USER_TAG_NAME_MIN * len(fids):
            return top
    if name and not _DEFAULT_NAME.match(name):
        return name
    # nothing better than a Ghidra default: the most common one, as before
    names = Counter(m["name"] for m in fmeta.values() if m.get("name"))
    return names.most_common(1)[0][0] if names else fallback


def collect_member_values(metas):
    """Flatten member file metadata into the value lists a cluster summary counts.

    `metas` is an iterable of per-file meta dicts. Ingest paths store these
    fields either as a scalar or as a list, so each one is coerced here.
    Returns (names, md5s, yara, avtype, filetype, cc_ip).
    """
    names_list = []
    md5s_list = []
    yara_list = []
    avtype_list = []
    filetype_list = []
    ccip_list = []

    for m in metas:
        if m.get("file_names"):
            names_list.extend(m["file_names"])
        elif m.get("file_name"):
            names_list.append(m["file_name"])

        if m.get("file_md5"):
            md5s_list.append(m["file_md5"])

        if m.get("yara"):
            yara_list.extend(m["yara"] if isinstance(m["yara"], list) else [m["yara"]])
        if m.get("avtype"):
            avtype_list.extend(
                m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
            )
        if m.get("filetype"):
            filetype_list.extend(
                m["filetype"] if isinstance(m["filetype"], list) else [m["filetype"]]
            )
        if m.get("cc_ip"):
            ccip_list.extend(
                m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
            )

    return names_list, md5s_list, yara_list, avtype_list, filetype_list, ccip_list


def build_freq(items, member_count, limit=None):
    """Return up to ``limit`` value frequencies for one cluster distribution.

    `percent` is a share of `member_count` -- the cluster's members -- never of
    `items`, which is flattened: a file carrying two yara hits contributes two
    entries. Every writer of a cluster `:meta` key has to use the same
    denominator, or the same cluster reports different percents depending on
    which path wrote it last.
    """
    return (
        [
            {
                "value": k,
                "count": v,
                "percent": round((v / member_count) * 100),
            }
            for k, v in Counter(items).most_common(limit)
        ]
        if items
        else []
    )


def cluster_dimensions(metas, member_count):
    """Return categorical metadata shown by cluster views."""
    values = {
        field: []
        for field in ("filetype", "architecture", "executable_format", "batch_uuid")
    }
    for meta in metas:
        if meta.get("filetype"):
            values["filetype"].extend(_values(meta["filetype"]))
        if meta.get("language_id"):
            values["architecture"].append(meta["language_id"])
        file_format = meta.get("file_format") or {}
        executable_format = file_format.get("Executable Format") or file_format.get(
            "format"
        )
        if executable_format:
            values["executable_format"].append(executable_format)
        if meta.get("batch_uuid"):
            values["batch_uuid"].append(meta["batch_uuid"])
    return {
        f"{field}_distribution": build_freq(items, member_count)
        for field, items in values.items()
    }


def build_tag_distribution(by_axis, member_count, limit=None):
    """Build namespace trees from per-member tag hits."""
    out = {}
    for axis, items in by_axis.items():
        counts = Counter(items)
        nodes = {}
        child_ids = defaultdict(set)
        parent_ids = set()
        for tag_id in counts:
            if ":" not in tag_id:
                continue
            chain = [p for p in tag_prefixes(tag_id) if p != axis]
            body, detail = tag_body(tag_id)
            if detail and body != axis:
                chain.append(body)
            if tag_id != axis:
                chain.append(tag_id)
            parent = None
            for node_id in chain:
                node = nodes.setdefault(
                    node_id,
                    {
                        "tag_id": node_id,
                        "count": counts.get(node_id, 0),
                        "children": [],
                    },
                )
                if parent is not None and node_id not in child_ids[parent["tag_id"]]:
                    child_ids[parent["tag_id"]].add(node_id)
                    parent_ids.add(node_id)
                    parent["children"].append(node)
                parent = node

        roots = [node for node_id, node in nodes.items() if node_id not in parent_ids]
        roots = sorted(roots, key=lambda n: (-n["count"], n["tag_id"]))
        if limit is not None:
            roots = roots[:limit]

        def finish(node):
            node["coverage"] = node["count"] / member_count if member_count else 0.0
            node["children"] = [finish(child) for child in node["children"]]
            return node

        out[axis] = [finish(node) for node in roots]
    return out


def flatten_tag_distribution(distribution, axes=None):
    """Every `tag_id` in a `tag_distribution`, across the given axes (or all).

    Plan D3: the legacy `yara_distribution`/`avtype_distribution` lists fed
    keyword search by their `value`s; this is the tag-sourced equivalent so a
    search keeps matching once a cluster's only evidence is a mint-on-write
    `av:`/`yara:` tag. Walks every node, not just leaves, so a namespace or a
    vendor segment is searchable too.
    """
    out = []

    def visit(nodes):
        for node in nodes or []:
            if node.get("tag_id"):
                out.append(node["tag_id"])
            visit(node.get("children"))

    for axis in axes if axes is not None else distribution.keys():
        visit(distribution.get(axis) or [])
    return out


def set_tag_distribution_score(distribution, score):
    def visit(nodes):
        for node in nodes:
            node["score"] = score
            visit(node.get("children", []))

    visit([node for nodes in distribution.values() for node in nodes])


def _values(value):
    if not value:
        return []
    return value if isinstance(value, list) else [value]


def _member_tag_values(meta):
    values = []

    for field, source in (("tags", "analysis"), ("user_tags", "user")):
        for tag in _values(meta.get(field)):
            tag = str(tag).strip()
            if not tag or not tag_policy(tag, source).aggregate:
                continue
            body, detail = tag_body(tag)
            candidates = set(tag_prefixes(tag))
            if detail or ":" in body:
                candidates.add(body)
            if detail:
                candidates.add(tag)
            elif source == "user":
                candidates.add(f"user:{body}")
            values.extend(
                (tag_policy(candidate).axis, candidate)
                for candidate in candidates
                if tag_policy(candidate).aggregate
            )
    return values


def normalize_tag_distribution(distribution, member_count, score=None):
    """Return the current nested shape, including older stored metadata."""
    if not distribution:
        return {}
    if all(item.get("tag_id") for values in distribution.values() for item in values):
        set_tag_distribution_score(distribution, score)
        return distribution

    out = {}
    for axis, values in distribution.items():
        exact = {item.get("value"): item for item in values if item.get("value")}
        nodes = {}
        parent_ids = set()
        for tag_id, item in exact.items():
            chain = [p for p in tag_prefixes(tag_id) if p != axis] + [tag_id]
            parent = None
            for node_id in chain:
                node = nodes.setdefault(node_id, {"tag_id": node_id, "children": []})
                source = exact.get(node_id, {})
                node["count"] = source.get("count", item.get("count", 0))
                node["coverage"] = source.get("percent", item.get("percent", 0)) / 100.0
                node["score"] = score
                if parent is not None and node_id not in {
                    c["tag_id"] for c in parent["children"]
                }:
                    parent["children"].append(node)
                    parent_ids.add(node_id)
                parent = node
        roots = [node for tag_id, node in nodes.items() if tag_id not in parent_ids]
        out[axis] = sorted(roots, key=lambda n: (-n["count"], n["tag_id"]))[:10]
    return out


def cluster_summary(metas, member_count=None, fields=DISTRIBUTION_FIELDS):
    """Build every distribution stored in one cluster's metadata."""
    metas = list(metas)
    member_count = len(metas) if member_count is None else member_count
    values = collect_member_values(metas)
    value_lists = dict(
        zip(
            ("yara", "avtype", "filetype", "ccip", "filename", "md5"),
            (values[2], values[3], values[4], values[5], values[0], values[1]),
        )
    )
    result = cluster_dimensions(metas, member_count)
    result.update(
        {
            f"{field}_distribution": build_freq(value_lists[field], member_count)
            for field in fields
            if field in value_lists
        }
    )

    by_axis = defaultdict(list)
    for meta in metas:
        by_member = defaultdict(set)
        for axis, value in _member_tag_values(meta):
            by_member[axis].add(value)
        for axis, member_values in by_member.items():
            by_axis[axis].extend(member_values)
    result["tag_distribution"] = build_tag_distribution(by_axis, member_count)
    return result


def inferred_tag_values(summary, cohesion, min_cohesion, min_coverage):
    """Deepest tag per axis the cluster's members actually support.

    Empty below `min_cohesion` -- a low-cohesion cluster's majority tag is
    coincidence, not a shared trait. Above it, each axis starts at its top
    root and walks down while a child's coverage still clears `min_coverage`,
    so a label never claims more specificity than the members carry. Every
    writer (threshold, hierarchical, pool) calls this one function; see
    Decisions 1 and 3 in plan_e_inferred_tag_gate.md.

    `build_tag_distribution` roots every axis at the bare namespace token
    (e.g. `av`), which is never itself a member value -- `tag_policy` requires
    a colon to resolve a real policy, so the token that has none never enters
    `counts` and its node sits at count 0. That placeholder is skipped before
    the coverage floor is applied, or every gated axis would read 0% and
    blank unconditionally.
    """
    if cohesion is None or cohesion < min_cohesion:
        return []
    out = []
    for distribution in (summary.get("tag_distribution") or {}).values():
        if not distribution:
            continue
        node = distribution[0]
        while node is not None and not node.get("count"):
            children = node.get("children") or []
            node = max(children, key=lambda c: c.get("coverage", 0.0), default=None)
        if node is None or node.get("coverage", 0.0) < min_coverage:
            continue
        best = node
        children = node.get("children") or []
        while children:
            candidate = max(children, key=lambda c: c.get("coverage", 0.0))
            if candidate.get("coverage", 0.0) < min_coverage:
                break
            best = candidate
            children = best.get("children") or []
        if best.get("tag_id"):
            out.append(best["tag_id"])
    return out


# (distribution field, inferred-tag prefix) for the file-level traits of a
# function cluster. Family tags (av:/yara:) are deliberately absent: a libc
# cluster inside mostly-mirai files would otherwise read as "mirai".
FILE_TRAIT_AXES = (
    ("architecture", "arch"),
    ("executable_format", "format"),
    ("filetype", "filetype"),
)


def function_cluster_stats(
    func_metas, file_metas, cohesion, min_cohesion, min_coverage, min_members=3
):
    """Stats stored on a function cluster's meta.

    `tag_distribution` / `inferred_tags` come from the functions' own tags.
    The parent files (one meta per distinct file) give the arch / format /
    filetype / batch distributions, `file_tag_distribution`, and
    `file_inferred_tags`, which only covers FILE_TRAIT_AXES. Both inferred
    lists stay empty below `min_members`, where coverage is coincidence.
    """
    n = len(func_metas)
    fsum = cluster_summary(func_metas, n)
    files = cluster_summary(file_metas, len(file_metas))
    set_tag_distribution_score(fsum["tag_distribution"], cohesion)
    set_tag_distribution_score(files["tag_distribution"], cohesion)
    out = {
        "tag_distribution": fsum["tag_distribution"],
        "file_tag_distribution": files["tag_distribution"],
        "architecture_distribution": files["architecture_distribution"],
        "executable_format_distribution": files["executable_format_distribution"],
        "filetype_distribution": files["filetype_distribution"],
        "batch_uuid_distribution": files["batch_uuid_distribution"],
        "inferred_tags": [],
        "file_inferred_tags": [],
    }
    if n < min_members or cohesion is None or cohesion < min_cohesion:
        return out
    out["inferred_tags"] = inferred_tag_values(
        fsum, cohesion, min_cohesion, min_coverage
    )
    for field, prefix in FILE_TRAIT_AXES:
        top = files[f"{field}_distribution"][:1]
        if top and top[0]["count"] / len(file_metas) >= min_coverage:
            out["file_inferred_tags"].append(f"{prefix}:{top[0]['value']}")
    return out


def resolve_hierarchical_inferred_tags(leaf_to_clusters, idx_to_id, gated_by_label):
    """Per-file inferred tags for a hierarchical tree (Decision 4).

    `leaf_to_clusters` maps each leaf to its surviving ancestors ordered
    deepest-first (`cluster_common.hierarchical_membership`'s shape).
    `gated_by_label` is {cluster_id: gated inferred-tag list}, already run
    through `inferred_tag_values` -- an empty list means that cluster failed
    the gate. A file takes the first (most specific) ancestor that passed,
    never a union of the tree it survives into.
    """
    resolved = {}
    for leaf, clusters in leaf_to_clusters.items():
        tags = []
        for c in clusters:
            gated = gated_by_label.get(c)
            if gated:
                tags = gated
                break
        resolved[idx_to_id[leaf]] = tags
    return resolved


def function_count_stats(metas):
    """Min / average / max function count over a cluster's member files.

    A cluster's member count says how many binaries it holds, never how big
    they are: a 50-file cluster of 200-function droppers and one of 40k-function
    statically linked binaries read the same in the listing. Files whose meta
    carries no `function_count` are skipped rather than counted as zero, so a
    partially enriched collection does not report a min of 0 for every cluster.
    Returns {} when no member reports one.
    """
    counts = []
    for m in metas:
        value = (m or {}).get("function_count")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        counts.append(int(value))
    if not counts:
        return {}
    return {
        "min": min(counts),
        "avg": round(sum(counts) / len(counts), 1),
        "max": max(counts),
        "files": len(counts),
    }


DAY_MS = 86_400_000
# Member date fields a cluster timeline covers, and how a multi-valued one reduces.
TIMELINE_FIELDS = {"entry_date": None, "first_seen": min, "last_seen": max}


def timeline_stats(metas):
    """Member dates per field: {field: {first, last, n, days: [[day_ms, count], ...]}}.

    `days` is the per-UTC-day member count, so a reader rebuilds the cumulative
    member curve and "new in the last N days" without loading a member. A field
    no member carries is left out (first_seen / last_seen are optional).
    """
    out = {}
    for field, reduce in TIMELINE_FIELDS.items():
        ts = [normalize_timestamp((m or {}).get(field), reduce) for m in metas]
        ts = [t for t in ts if t]
        if not ts:
            continue
        days = Counter(t - t % DAY_MS for t in ts)
        out[field] = {
            "first": min(ts),
            "last": max(ts),
            "n": len(ts),
            "days": [[d, n] for d, n in sorted(days.items())],
        }
    return out


def timeline_summary(timeline, now_ms=None, window_days=30):
    """Read-side activity of a stored timeline: upload span, members added in
    the last `window_days` days, and the seen-in-the-wild span (first_seen and
    last_seen together: a member only seen once still counts on both ends)."""
    tl = timeline or {}
    up = tl.get("entry_date") or {}
    now_ms = now_ms or int(time.time() * 1000)
    since = now_ms - window_days * DAY_MS
    since -= since % DAY_MS
    seen = [tl[f] for f in ("first_seen", "last_seen") if tl.get(f)]
    return {
        "first_added": up.get("first"),
        "last_added": up.get("last"),
        "new_members": sum(n for d, n in up.get("days") or [] if d >= since),
        "first_seen": min(s["first"] for s in seen) if seen else None,
        "last_seen": max(s["last"] for s in seen) if seen else None,
    }


def pick_best_shared_cluster(cids_a, cids_b, cluster_meta):
    """Highest-cohesion cluster shared by two functions, or None.

    cids_a/cids_b: iterables of cluster ids. cluster_meta: {cid -> meta dict with
    'cohesion_score'}. Returns the winning meta dict (not the id) or None when the two
    share no cluster present in cluster_meta.
    """
    best, best_coh = None, -1.0
    for cid in set(cids_a) & set(cids_b):
        m = cluster_meta.get(cid)
        if m is None:
            continue
        coh = float(m.get("cohesion_score", 0.0))
        if coh > best_coh:
            best, best_coh = m, coh
    return best


def pick_best_cluster(cids, cluster_meta):
    """Best (highest-cohesion) cluster for a single function, or None."""
    return pick_best_shared_cluster(cids, cids, cluster_meta)


def bin_cluster_ns(algo, is_container):
    """Namespace holding a file's binary clusters.

    Containers are clustered in their own tree and persisted under
    `{algo}:container`. Both trees number internal nodes from the same base, so
    the bare labels in `{file_id}:bin_clusters` collide across the two: label
    1073741828 names one cluster among containers and a different one among
    plain files. A label is only meaningful together with the namespace that
    wrote it.
    """
    from bsimvis.app.services.config_service import config_service

    if (
        config_service.get("clustering.bin_engine", "hierarchical_snn")
        == "hierarchical_snn"
    ):
        if not algo.endswith(":snn"):
            algo = f"{algo}:snn"
    return f"{algo}:container" if is_container else algo


def fetch_bin_cluster_meta(
    r, collection, entries, algo="unweighted_cosine", pool_id=None
):
    """Resolve per-file binary cluster labels to metadata, in one pipeline.

    entries: iterable of (cluster_ids, is_container) -- the raw
    `{file_id}:bin_clusters` members and the file's own container flag.

    Returns (meta_by_uuid, uuids_per_entry). Callers hand `cluster_uuid` to the
    client instead of the label: uuids are unique across namespaces and algos,
    labels are not. Labels with no metadata (written by another algo, or by a
    build that has since been cleared) are dropped.
    """
    norm = [
        (
            [c.decode() if isinstance(c, bytes) else str(c) for c in (cids or [])],
            bin_cluster_ns(algo, bool(is_container)),
        )
        for cids, is_container in entries
    ]

    keys = {}
    for cids, ns in norm:
        for cid in cids:
            if pool_id:
                # Pool clusters are keyed by uuid already and have no namespace.
                keys[(ns, cid)] = f"global:pool:{pool_id}:bin_cluster:{cid}:meta"
            else:
                keys[(ns, cid)] = f"{collection}:bin_cluster:{ns}:{cid}:meta"

    pairs = list(keys)
    pipe = r.pipeline(transaction=False)
    for k in pairs:
        pipe.get(keys[k])

    meta_by_pair = {}
    for k, raw in zip(pairs, pipe.execute()):
        if not raw:
            continue
        cm = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(cm, str):
            cm = json.loads(cm)
        if cm:
            meta_by_pair[k] = {**cm, **name_fields(cm)}

    meta_by_uuid, per_entry = {}, []
    for cids, ns in norm:
        uuids = []
        for cid in cids:
            cm = meta_by_pair.get((ns, cid))
            if not cm:
                continue
            key = cm.get("cluster_uuid") or cid
            meta_by_uuid[key] = cm
            uuids.append(key)
        per_entry.append(uuids)
    return meta_by_uuid, per_entry


_AXES = ["overall", "code", "library", "content"]


def fetch_bin_cluster_meta_all_axes(
    r, collection, entries, algo="unweighted_cosine", pool_id=None
):
    """Like fetch_bin_cluster_meta but fetches all 4 axes at once.

    entries: iterable of (cluster_ids_by_axis, is_container) where
             cluster_ids_by_axis is a dict {axis: [cid, ...]} as built by
             search_file when it SMEMBERS all 4 axis keys per file.

    Returns (meta_by_uuid, uuids_per_entry_by_axis).
    - meta_by_uuid: {uuid -> meta dict, with "axis" key injected}
    - uuids_per_entry_by_axis: list of {axis -> [uuid, ...]} per entry
    """
    # Build flat list of (axis, ns, cid, entry_idx) tuples to pipeline
    lookups = []
    norm_entries = []
    for entry_idx, (cids_by_axis, is_container) in enumerate(entries):
        by_axis = {}
        for axis in _AXES:
            axis_algo = f"{algo}:{axis}" if axis != "overall" else algo
            ns = bin_cluster_ns(axis_algo, bool(is_container))
            cids = [
                c.decode() if isinstance(c, bytes) else str(c)
                for c in (cids_by_axis.get(axis) or [])
            ]
            by_axis[axis] = (ns, cids)
            for cid in cids:
                lookups.append((entry_idx, axis, ns, cid))
        norm_entries.append(by_axis)

    # Single pipeline for all keys
    pipe = r.pipeline(transaction=False)
    unique_keys = {}
    for _, axis, ns, cid in lookups:
        if pool_id:
            key = f"global:pool:{pool_id}:bin_cluster:{cid}:meta"
        else:
            key = f"{collection}:bin_cluster:{ns}:{cid}:meta"
        unique_keys[(axis, ns, cid)] = key

    unique_list = list(unique_keys.items())
    for _, key in unique_list:
        pipe.get(key)

    parsed_meta = {}
    for (k_tuple, _), raw in zip(unique_list, pipe.execute()):
        if not raw:
            continue
        cm = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(cm, str):
            cm = json.loads(cm)
        if cm:
            parsed_meta[k_tuple] = {**cm, **name_fields(cm)}

    meta_by_uuid = {}
    # (entry_idx, axis, cid) -> uuid
    resolved = {}
    for entry_idx, axis, ns, cid in lookups:
        cm = parsed_meta.get((axis, ns, cid))
        if not cm:
            continue

        # Copy to avoid mutating shared dict if we inject axis
        # But wait, axis is the same for the same (axis, ns, cid)

        uuid = cm.get("cluster_uuid") or cid
        cm["axis"] = axis
        meta_by_uuid[uuid] = cm
        resolved[(entry_idx, axis, cid)] = uuid

    uuids_per_entry_by_axis = []
    for entry_idx, by_axis in enumerate(norm_entries):
        result = {}
        for axis, (ns, cids) in by_axis.items():
            uuids = []
            for cid in cids:
                uuid = resolved.get((entry_idx, axis, cid))
                if uuid:
                    uuids.append(uuid)
            if uuids:
                result[axis] = uuids
        uuids_per_entry_by_axis.append(result)

    return meta_by_uuid, uuids_per_entry_by_axis


def _walk(distribution):
    for nodes in distribution.values():
        stack = list(nodes)
        while stack:
            node = stack.pop()
            yield node
            stack.extend(node["children"])


def real_links(links):
    """Drop links to the synthetic global root: it is never a cluster, so it
    has no meta and every top-level cluster would show it as a nameless parent.
    Every real cluster is some link's child; the root never is."""
    children = {l["child"] for l in links}
    return [l for l in links if l["parent"] in children]


def get_tree_links(r, collection, algo, tree_links_prefix="cluster"):
    """child_to_parent / parent_to_children maps from the stored tree_links blob.

    One GET regardless of collection size -- callers used to rebuild this by
    reading every cluster's meta key just to find its parent.
    """
    links_key = f"{collection}:{tree_links_prefix}:tree_links:{algo}"
    links_raw = r.get(links_key)
    child_to_parent, parent_to_children = {}, {}
    if links_raw:
        try:
            links = real_links(json.loads(links_raw))
            for l in links:
                c, p = str(l["child"]), str(l["parent"])
                child_to_parent[c] = p
                parent_to_children.setdefault(p, []).append(c)
        except Exception:
            pass
    return child_to_parent, parent_to_children


def _tree_keys(collection, prefix, algo):
    base = f"{collection}:{prefix}"
    return (
        f"{base}:tree_links:{algo}",
        f"{base}:tree_parent:{algo}",
        f"{base}:tree_children:{algo}:",
        f"{base}:uuid2cid:{algo}",
    )


def _s(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def clear_tree_links(r, collection, prefix, algo):
    """Delete the tree_links blob and its per-node index."""
    r.delete(_tree_keys(collection, prefix, algo)[0])
    _clear_tree_index(r, collection, prefix, algo)
    if prefix == "bin_cluster":
        from bsimvis.app.services.cluster_display import clear_display

        clear_display(r, collection, algo)


def _clear_tree_index(r, collection, prefix, algo):
    _, parent_k, child_k, uuid_k = _tree_keys(collection, prefix, algo)
    parents = {_s(p) for p in r.hvals(parent_k)}
    r.delete(parent_k, uuid_k, *[child_k + p for p in parents])
    _UUID_MAPS.pop((collection, algo, prefix), None)


def rebuild_tree_index(r, collection, prefix, algo, links):
    """(Re)write the per-node index from `links`; the blob is left alone.

    `tree_parent:{algo}` is a child -> parent hash and
    `tree_children:{algo}:{pid}` a ZSET of children scored by size, so the
    detail view reads a node's neighbours without parsing the whole blob.
    The uuid -> cid cache is dropped: a rebuild re-mints uuids.
    """
    _, parent_k, child_k, _ = _tree_keys(collection, prefix, algo)
    _clear_tree_index(r, collection, prefix, algo)
    kids = defaultdict(dict)
    parent_of = {}
    for l in real_links(links):
        c, p = str(l["child"]), str(l["parent"])
        parent_of[c] = p
        kids[p][c] = l.get("size", 0)
    items = list(parent_of.items())
    for i in range(0, len(items), 5000):
        r.hset(parent_k, mapping=dict(items[i : i + 5000]))
    pipe = r.pipeline(transaction=False)
    for n, (p, children) in enumerate(kids.items(), 1):
        pipe.zadd(child_k + p, children)
        if n % 5000 == 0:
            pipe.execute()
    pipe.execute()
    return len(parent_of)


def write_tree_links(r, collection, prefix, algo, links):
    """Store the tree_links blob plus its per-node index."""
    r.set(_tree_keys(collection, prefix, algo)[0], json.dumps(links))
    rebuild_tree_index(r, collection, prefix, algo, links)


class Tree:
    """Parent / children lookups for one cluster tree.

    Reads the per-node index written by write_tree_links; a tree stored
    before that existed falls back to the tree_links blob.
    """

    def __init__(self, r, collection, algo, prefix="cluster", links=None):
        self.r, self.collection, self.algo, self.prefix = r, collection, algo, prefix
        _, self.parent_k, self.child_k, _ = _tree_keys(collection, prefix, algo)
        self.indexed = links is None and bool(r.exists(self.parent_k))
        if not self.indexed:
            self.c2p, self.p2c = links or get_tree_links(r, collection, algo, prefix)

    def parent(self, cid):
        if not self.indexed:
            return self.c2p.get(str(cid))
        p = self.r.hget(self.parent_k, str(cid))
        return _s(p) if p else None

    def children(self, cid, offset=0, n=None):
        """(ids, total): children by size desc, `n` of them from `offset`."""
        cid = str(cid)
        if self.indexed:
            key = self.child_k + cid
            end = -1 if n is None else offset + n - 1
            ids = [_s(c) for c in self.r.zrevrange(key, offset, end)]
            return ids, self.r.zcard(key)
        ids = self.p2c.get(cid, [])
        metas = get_cluster_metas(self.r, self.collection, self.algo, ids, self.prefix)
        ranked = sorted(
            ids, key=lambda c: metas.get(c, {}).get("member_count", 0), reverse=True
        )
        return ranked[offset : None if n is None else offset + n], len(ids)

    def info(self, cids):
        """({cid: parent}, {cid: child count}) for just these nodes."""
        cids = [str(c) for c in cids]
        if not cids:
            return {}, {}
        if not self.indexed:
            return (
                {c: self.c2p.get(c) for c in cids},
                {c: len(self.p2c.get(c, [])) for c in cids},
            )
        pipe = self.r.pipeline(transaction=False)
        pipe.hmget(self.parent_k, cids)
        for c in cids:
            pipe.zcard(self.child_k + c)
        res = pipe.execute()
        parents = {c: (_s(p) if p else None) for c, p in zip(cids, res[0])}
        return parents, dict(zip(cids, res[1:]))


def resolve_cluster_id_by_uuid(r, collection, algo, uuid, meta_prefix="cluster"):
    """cluster_id for a uuid.

    `uuid2cid` is a persistent uuid -> cid hash filled on first use (one
    pipelined meta scan per rebuild, dropped by write_tree_links), so every
    later lookup is an HGET plus a meta check that the uuid still matches.
    """
    uuid = str(uuid or "").lower()
    if not uuid:
        return None
    cache_k = _tree_keys(collection, meta_prefix, algo)[3]
    hit = r.hget(cache_k, uuid)
    if hit:
        hit = _s(hit)
        m = get_cluster_metas(r, collection, algo, [hit], meta_prefix).get(hit)
        if m and str(m.get("cluster_uuid", "")).lower() == uuid:
            return hit
        r.hdel(cache_k, uuid)

    # Only the union-find engine writes uf:uuid, and a later hierarchical build
    # leaves it behind: a hit must still match the meta, a miss is not final.
    uuid_key = f"{collection}:{meta_prefix}:{algo}:uf:uuid"
    for cid, u in (r.hgetall(uuid_key) or {}).items():
        if _s(u).lower() == uuid:
            cid = _s(cid)
            m = get_cluster_metas(r, collection, algo, [cid], meta_prefix).get(cid)
            if m and str(m.get("cluster_uuid", "")).lower() == uuid:
                r.hset(cache_k, uuid, cid)
                return cid
            break

    ns = (collection, algo, meta_prefix)
    if ns in _UUID_MAPS and time.time() - _UUID_MAPS_AT[ns] < 30:
        return _UUID_MAPS[ns].get(uuid)  # fresh map: no rescan for a bogus uuid

    _UUID_MAPS[ns] = _scan_uuid_map(r, collection, algo, meta_prefix)
    _UUID_MAPS_AT[ns] = time.time()
    items = list(_UUID_MAPS[ns].items())
    for i in range(0, len(items), 5000):
        r.hset(cache_k, mapping=dict(items[i : i + 5000]))
    return _UUID_MAPS[ns].get(uuid)


# ponytail: per-process throttle for the uuid scan (the result is persisted in
# uuid2cid); a build-time uuid hash would remove the first-hit scan (~3s at 130k).
_UUID_MAPS = {}
_UUID_MAPS_AT = {}


def _scan_uuid_map(r, collection, algo, meta_prefix):
    list_key = f"{collection}:{meta_prefix}:list:{algo}"
    meta_key_prefix = f"{collection}:{meta_prefix}:{algo}:"
    cids_raw = r.smembers(list_key) or set()
    if not cids_raw:
        # The hierarchical/HDBSCAN engine never populates the `list` set
        # (only the union-find engine does) -- same bounded SCAN the plain
        # listing route falls back to, never a blocking KEYS.
        pattern = f"{meta_key_prefix}*:meta"
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            for k in keys:
                k = k.decode() if isinstance(k, bytes) else k
                cids_raw.add(k[len(meta_key_prefix) : -len(":meta")])
            if cursor == 0:
                break
    cids = {c.decode() if isinstance(c, bytes) else str(c) for c in cids_raw}
    # The list set can lag an incremental rebuild; tree_links never does.
    c2p, _ = get_tree_links(r, collection, algo, meta_prefix)
    cids.update(c2p)
    cids.update(c2p.values())
    cids = list(cids)

    out = {}
    for i in range(0, len(cids), 5000):
        chunk = cids[i : i + 5000]
        pipe = r.pipeline(transaction=False)
        for cid in chunk:
            pipe.get(f"{meta_key_prefix}{cid}:meta")
        for cid, blob in zip(chunk, pipe.execute()):
            if not blob:
                continue
            m = json.loads(blob)
            if isinstance(m, str):
                m = json.loads(m)
            u = str(m.get("cluster_uuid") or "").lower()
            if u:
                out[u] = cid
    return out


def get_cluster_metas(r, collection, algo, cids, meta_prefix="cluster"):
    """Pipelined GET + parse of only the given cluster ids' meta blobs."""
    cids = [str(c) for c in cids]
    if not cids:
        return {}
    pipe = r.pipeline(transaction=False)
    for cid in cids:
        pipe.get(f"{collection}:{meta_prefix}:{algo}:{cid}:meta")
    raw = pipe.execute()
    out = {}
    for cid, blob in zip(cids, raw):
        if not blob:
            continue
        m = json.loads(blob) if not isinstance(blob, dict) else blob
        if isinstance(m, str):
            m = json.loads(m)
        out[cid] = m
    return out


def cluster_tree_slice(
    r,
    collection,
    algo,
    uuid,
    up=3,
    down=3,
    width=10,
    meta_prefix="cluster",
    tree=None,
):
    """A bounded window of the cluster tree around `uuid`, cheap at any collection size.

    Never reads more than the window needs: `up` ancestors on the path, plus
    each visited node's top-`width` children by size (one ZREVRANGE each).
    Returns (nodes: {cid: meta}, center_cid, hidden_above: int) so the caller
    can render "N more above" without walking further.
    """
    tree = tree or Tree(r, collection, algo, meta_prefix)
    center = resolve_cluster_id_by_uuid(r, collection, algo, uuid, meta_prefix)
    if center is None:
        return {}, None, 0

    # Ancestor chain, capped at `up` hops; count how many more exist above.
    ancestors = []
    curr = tree.parent(center)
    while curr is not None:
        ancestors.append(curr)
        curr = tree.parent(curr)
    shown_ancestors = ancestors[:up]
    hidden_above = max(0, len(ancestors) - up)

    needed = set(shown_ancestors)
    needed.add(center)

    frontier = [center]
    for _ in range(down):
        next_frontier = []
        for node in frontier:
            shown, _total = tree.children(node, 0, width)
            needed.update(shown)
            next_frontier.extend(shown)
        frontier = next_frontier

    metas = get_cluster_metas(r, collection, algo, needed, meta_prefix)
    return metas, center, hidden_above


def cluster_children_page(
    r,
    collection,
    algo,
    parent_cid,
    offset=0,
    width=10,
    meta_prefix="cluster",
    tree=None,
):
    """One page of a single node's children, largest first.

    Used for the tree's "+N more" row. Bounded by the page, never the
    node's whole fan-out or the collection.
    """
    tree = tree or Tree(r, collection, algo, meta_prefix)
    page, total = tree.children(parent_cid, offset, width)
    metas = get_cluster_metas(r, collection, algo, page, meta_prefix)
    return {c: metas[c] for c in page if c in metas}, total


def demo():
    """The collision this resolver exists for: one label, two namespaces."""

    class FakeRedis:
        def __init__(self, kv):
            self.kv, self.queue = kv, []

        def pipeline(self, transaction=False):
            return self

        def get(self, k):
            self.queue.append(k)

        def execute(self):
            out, self.queue = [self.kv.get(k) for k in self.queue], []
            return out

    label = "1073741828"
    col = "C"
    r = FakeRedis(
        {
            f"{col}:bin_cluster:unweighted_cosine:{label}:meta": json.dumps(
                {"cluster_uuid": "aaaa", "cluster_name": "elf-cluster"}
            ),
            f"{col}:bin_cluster:unweighted_cosine:container:{label}:meta": json.dumps(
                {"cluster_uuid": "bbbb", "cluster_name": "zip-cluster"}
            ),
        }
    )

    metas, per_entry = fetch_bin_cluster_meta(
        r, col, [([label], True), ([label], False), (["99"], False)]
    )
    assert per_entry == [["bbbb"], ["aaaa"], []], per_entry
    assert metas["bbbb"]["cluster_name"] == "zip-cluster", metas
    assert metas["aaaa"]["cluster_name"] == "elf-cluster", metas
    assert bin_cluster_ns("x", True) == "x:container"

    # Analyst names survive a rebuild: matched by anchors, closest size wins.
    class NameRedis:
        h = {}

        def hgetall(self, k):
            return dict(self.h)

        def hset(self, k, f, v):
            self.h[f] = v

        def hdel(self, k, f):
            self.h.pop(f, None)

    nr = NameRedis()
    old_members = [f"C:file:m{i}" for i in range(10)]
    save_cluster_name(nr, "C", "bin", "algo", old_members, "my family")
    fresh = {
        "7": [f"C:file:m{i}" for i in range(10)] + ["C:file:x1"],
        "8": [f"C:file:m{i}" for i in range(20, 25)],
    }
    keeper = ClusterNameKeeper(nr, "C", "bin", "algo", fresh)
    assert keeper.pick("7", "auto") == ("my family", True)
    assert keeper.pick("8", "auto") == ("auto", False)
    save_cluster_name(nr, "C", "bin", "algo", old_members, "", replaces="my family")
    assert ClusterNameKeeper(nr, "C", "bin", "algo", fresh).pick("7", "a") == (
        "a",
        False,
    )
    # Naming a sub-cluster keeps the family's name, and the other way round.
    nr.h = {}
    fam = [f"C:file:f{i}" for i in range(78)]
    save_cluster_name(nr, "C", "bin", "algo", fam, "Mirai")
    save_cluster_name(nr, "C", "bin", "algo", fam[:9], "SH4")
    k = ClusterNameKeeper(nr, "C", "bin", "algo", {"P": fam, "S": fam[:9]})
    assert k.pick("P", "a") == ("Mirai", True) and k.pick("S", "a") == ("SH4", True)
    # A second rename of the same cluster replaces its name.
    save_cluster_name(nr, "C", "bin", "algo", fam[:9], "SH4 builds", replaces="SH4")
    assert sorted(json.loads(v)["name"] for v in nr.h.values()) == [
        "Mirai",
        "SH4 builds",
    ]

    # Hierarchical: renamed node 5 (9 files) grows to 40 in one upload and a
    # 10-file sub-node 6 now holds most of the original files.
    nr.h = {}
    orig = [f"C:file:s{i}" for i in range(9)]
    save_cluster_name(nr, "C", "bin", "algo", orig, "SH4", label=5)
    grown = {5: orig + [f"C:file:n{i}" for i in range(31)], 6: orig + ["C:file:n0"]}
    # incremental run (stable ids): the name stays on node 5
    k = ClusterNameKeeper(nr, "C", "bin", "algo", grown, stable_ids=True)
    assert k.pick(5, "a") == ("SH4", True) and k.pick(6, "a") == ("a", False)
    # after reanchor the entry follows node 5, so a full rebuild (fresh ids,
    # same tree under labels 50/60) finds it by size too
    k.reanchor([5, 6])
    (e,) = [json.loads(v) for v in nr.h.values()]
    assert e["size"] == 40 and e["label"] == "5", e
    fresh = {50: grown[5], 60: grown[6]}
    assert ClusterNameKeeper(nr, "C", "bin", "algo", fresh).pick(50, "a") == (
        "SH4",
        True,
    )

    # A parent and a child with the same 20 anchors keep both names.
    nr.h = {}
    child = [f"C:file:c{i}" for i in range(30)]
    parent = child + ["C:file:zz1"]
    save_cluster_name(nr, "C", "bin", "algo", parent, "fam")
    save_cluster_name(nr, "C", "bin", "algo", child, "sub")
    k = ClusterNameKeeper(nr, "C", "bin", "algo", {"P": parent, "S": child})
    assert (k.pick("P", "")[0], k.pick("S", "")[0]) == ("fam", "sub"), k.names

    # hierarchical_uf <-> hierarchical_snn: names saved under one engine apply
    # to the other, legacy fields (no size suffix) included.
    nr.h = {}
    save_cluster_name(nr, "C", "bin", "algo:code:snn", orig, "SH4")
    assert ClusterNameKeeper(nr, "C", "bin", "algo:code", {1: orig}).pick(1, "")[0]
    nr.h = {
        "algo:code:snn|0000abcd": json.dumps(
            {"name": "old", "size": 9, "anchors": [o.split(":file:")[1] for o in orig]}
        )
    }
    assert ClusterNameKeeper(nr, "C", "bin", "algo:code", {1: orig}).pick(1, "") == (
        "old",
        True,
    )
    assert not ClusterNameKeeper(nr, "C", "bin", "other:code", {1: orig}).names

    # A user tag names the cluster only above the coverage floor.
    dist = {
        "user": [
            {"tag_id": "user:botnet", "count": 3, "coverage": 0.3, "children": []}
        ],
        "family": [
            {"tag_id": "av:x:mirai", "count": 9, "coverage": 0.9, "children": []}
        ],
    }
    assert default_bin_cluster_name([], [], [], "f", tag_distribution=dist) == "botnet"
    dist["user"][0]["coverage"] = 0.1
    assert default_bin_cluster_name([], [], [], "f", tag_distribution=dist) == "mirai"

    # inferred_tag_values: gated out below cohesion.
    summary = cluster_summary([{"tags": ["av:clamav:mirai"]}] * 10, member_count=10)
    assert inferred_tag_values(summary, 0.4, 0.5, 0.5) == []

    # Gated out below coverage: only 3/10 members carry the tag.
    metas = [{"tags": ["av:clamav:mirai"]}] * 3 + [{}] * 7
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == []

    # Deepest passing node chosen: root at 100%, its child at 60% (>= floor).
    metas = [{"tags": ["av:clamav:mirai"]}] * 6 + [{"tags": ["av:clamav"]}] * 4
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == ["av:clamav:mirai"]

    # Same shape, but the child falls under the floor -- root wins instead.
    metas = [{"tags": ["av:clamav:mirai"]}] * 3 + [{"tags": ["av:clamav"]}] * 7
    summary = cluster_summary(metas, member_count=10)
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == ["av:clamav"]

    # One member per prefix counted once: a single member with two leaf tags
    # sharing an ancestor ("av:clamav") contributes 1 to that ancestor, not 2.
    metas = [{"tags": ["av:clamav:mirai", "av:clamav:gafgyt"]}]
    summary = cluster_summary(metas, member_count=1)
    root = summary["tag_distribution"]["family"][0]
    assert root["tag_id"] == "av" and root["count"] == 0, root
    shared = root["children"][0]
    assert shared["tag_id"] == "av:clamav" and shared["count"] == 1, shared
    assert len(shared["children"]) == 2, shared["children"]

    # A `#detail` tail is the deepest node, hung off its body.
    summary = cluster_summary(
        [{"tags": ["yara:unknown:unknown#rule_x"]}] * 10, member_count=10
    )
    nodes = {n["tag_id"]: n for n in _walk(summary["tag_distribution"])}
    body = nodes["yara:unknown:unknown"]
    assert [c["tag_id"] for c in body["children"]] == [
        "yara:unknown:unknown#rule_x"
    ], body
    assert inferred_tag_values(summary, 0.9, 0.5, 0.5) == [
        "yara:unknown:unknown#rule_x"
    ]

    # resolve_hierarchical_inferred_tags: most specific passing cluster wins,
    # never a union of the ancestors a leaf survives into.
    resolved = resolve_hierarchical_inferred_tags(
        leaf_to_clusters={0: [20, 10], 1: [10]},
        idx_to_id={0: "fileA", 1: "fileB"},
        gated_by_label={20: [], 10: ["av:clamav"]},
    )
    assert resolved == {
        "fileA": ["av:clamav"],
        "fileB": ["av:clamav"],
    }, resolved

    # function_cluster_stats: file traits infer, family tags stay function-only,
    # and a tiny cluster infers nothing.
    funcs = [{"tags": ["yara:unknown:unknown#rule_x"]}] * 4
    files = [{"language_id": "ARM:LE:32:v7", "tags": ["av:clamav:mirai"]}] * 3
    st = function_cluster_stats(funcs, files, 0.9, 0.5, 0.5)
    assert st["inferred_tags"] == ["yara:unknown:unknown#rule_x"], st
    assert st["file_inferred_tags"] == ["arch:ARM:LE:32:v7"], st
    assert st["architecture_distribution"][0]["count"] == 3, st
    assert "family" in st["file_tag_distribution"], st
    tiny = function_cluster_stats(funcs[:2], files[:1], 0.9, 0.5, 0.5)
    assert tiny["inferred_tags"] == [] and tiny["file_inferred_tags"] == [], tiny
    low = function_cluster_stats(funcs, files, 0.2, 0.5, 0.5)
    assert low["inferred_tags"] == [] and low["file_inferred_tags"] == [], low

    day, t0 = DAY_MS, 20_000 * DAY_MS  # t0: 2024-10-04, ms
    tl = timeline_stats(
        [
            {"entry_date": t0 + 5},
            {"entry_date": t0 + 9, "first_seen": [t0 - 3 * day, t0 - 4 * day]},
            {"entry_date": (t0 + 10 * day) // 1000, "last_seen": "2024-10-03"},
            {},
        ]
    )
    assert tl["entry_date"] == {
        "first": t0 + 5,
        "last": t0 + 10 * day,
        "n": 3,
        "days": [[t0, 2], [t0 + 10 * day, 1]],
    }, tl
    assert tl["first_seen"]["first"] == t0 - 4 * day and "file_date" not in tl, tl
    s = timeline_summary(tl, now_ms=t0 + 10 * day + 7, window_days=5)
    assert s["new_members"] == 1 and s["last_added"] == t0 + 10 * day, s
    assert (s["first_seen"], s["last_seen"]) == (t0 - 4 * day, t0 - day), s
    assert timeline_summary(None)["new_members"] == 0
    assert timeline_summary({})["first_seen"] is None

    # Automatic file-cluster names: family head, then what sets a child apart.
    mk = lambda name, lid, tags=(), fc=200: {
        "file_name": name,
        "language_id": lid,
        "tags": list(tags),
        "function_count": fc,
    }
    mir = ["av:clamav:mirai#Unix.Trojan.Mirai-1"]
    assert stem_of("DEMONS.arm7") == "DEMONS" and stem_of("pm68k") is None
    assert name_tokens("xnxnxnxnxnxnxnxnm68kxnxn") == {"xnxn", "m68k"}
    assert name_tokens("pm68k") == {"m68k"}
    assert (
        stem_of("px86_i686") is None
        and stem_of("xnxnxnxnxnxnxnxnriscv64xnxn") == "xnxn"
    )
    assert stem_of("0123456789abcdef0123456789abcdef.unpacked") is None
    sh4 = [mk("boatnet.sh4", "SuperH4:LE:32:default", mir) for _ in range(5)]
    arm = [mk(f"parm{i}", "ARM:LE:32:v8", mir) for i in range(20)]
    coll = sh4 + arm
    namer = BinClusterNamer(coll)
    name, basis = namer.name(sh4, coll)
    assert name.startswith("Mirai · ") and "SH4" in name, name
    assert basis[0] == "av" and "arch" in basis, basis
    # 20 of a 25-file parent also named "Mirai · ARM": its core
    assert namer.name(arm, coll)[0] == "Mirai · ARM · core", namer.name(
        arm, coll
    )  # "p" is too short
    # unlabelled payloads inherit the parent's family when a tag hints at it
    yt = ["yara:botnet:mirai#Mirai_Botnet_Malware"]
    pay = [mk(f"dropper.arm{i}", "ARM:LE:32:v8", yt) for i in range(4)]
    assert namer.name(pay, arm + pay) == ("Mirai · dropper", ["parent", "files"])
    stubs = [mk(f"u{i}", "x86:LE:32:default", ["packer:upx"], 8) for i in range(4)]
    assert namer.name(stubs)[0].startswith("UPX stubs"), namer.name(stubs)
    assert namer.name([{}]) == (None, [])
    # a user tag beats the namer; empty metas fall back to the legacy rule
    tagged = {"user": [{"tag_id": "user:mine", "coverage": 0.5, "children": []}]}
    f = bin_cluster_name_fields(namer, sh4, coll, tagged, "fb")
    assert (f["cluster_name"], f["name_source"], f["name_basis"]) == (
        "mine",
        "tag",
        ["tag"],
    )
    assert f["auto_name"].startswith("Mirai") and f["name_default"]["source"] == "tag"
    f = bin_cluster_name_fields(namer, [{}, {}], None, None, "fb")
    assert (f["cluster_name"], f["name_source"]) == ("fb", "auto"), f
    m = {**f, "cluster_name": "X", "is_custom_name": True, "name_source": "analyst"}
    assert (
        restore_default_name(m, "z")["cluster_name"] == "fb" and not m["is_custom_name"]
    )
    assert name_fields({"is_custom_name": True})["name_source"] == "analyst"
    assert name_fields({})["name_source"] == "auto"

    # Near names: a chain 10 <- 11 <- 12 <- 13 (singletons merging in) ends at a
    # real split (a 5-file sibling) at 18; 9 and 8 hang below 10.
    L = lambda p, c, n: {"parent": p, "child": c, "size": n}
    links = [
        L(100, 18, 18),  # 100 is the synthetic root: dropped
        L(18, 13, 13),
        L(18, 5, 5),
        L(13, 12, 12),
        L(12, 11, 11),
        L(11, 10, 10),
        L(10, 9, 9),
        L(9, 8, 8),
        L(9, 2, 2),  # a 2-file sibling is not a split
        L(5, 4, 4),
    ]
    got = near_names(links, {10})
    assert got == {
        "11": ("10", 1),
        "12": ("10", 2),
        "9": ("10", -1),
        "8": ("10", -2),
    }, got
    assert "18" not in got and "13" not in got  # 13 is 3 off; 18 is past the split
    # two named nodes: each node takes the closer one, a named node takes none
    got = near_names(links, {10, 13})
    assert got["12"] == ("13", -1) and got["11"] == ("10", 1) and "10" not in got, got
    assert near_names(links, {5}) == {"4": ("5", -1)}
    ch = near_chain(links, 10)
    assert ch["chain"] == ["12", "11", "10", "9", "8"], ch["chain"]
    assert ch["above"] == "13" and ch["below"] == [], ch  # 13 is 3 off, 2 is small
    ch = near_chain(links, 12)  # 13 within 2 of 12; 18 merges the 5-file split
    assert ch["chain"][0] == "13" and ch["above"] == "18" and ch["below"] == ["9"], ch
    assert near_chain(links, 5)["chain"] == ["5", "4"] and near_chain(links, 99) is None

    print("cluster_utils demo OK")


if __name__ == "__main__":
    demo()
