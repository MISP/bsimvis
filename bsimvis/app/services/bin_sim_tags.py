"""Tag-based splitting of a binary-similarity score.

`build_bin_sim` produces one cohesion score per binary pair. That single number
mixes the boring mass (libc, compiler boilerplate) with the interesting mass
(the actual malware code), so two unrelated samples that both statically link
libc look related. This module splits the same matched flow by function tag, so
the caller can report "libc contributes 25% of the match, mirai_core 60%".

Matching is done on the *full* tag id (`origin:lib:libc:2.31:memcpy`); the
summary then rolls children up under their `origin:lib:libc:2.31` parent, so the
score is as precise as the Function ID analyzer made it while the UI still shows
one row per library version.

Tags come from several sources -- Function ID, a vendored-code bundle, a human,
the LLM, capa, and (once wired up) MITRE ATT&CK and the Malware Behavior Catalog
-- answering unrelated questions, so they are split onto separate axes (see
AXES below) and crossed by `AxisSplit` into one joint table. Every Sankey view
-- any single axis, or any pair of them -- is a marginal of that one table
(`joint_marginal`), so switching what the graph shows costs no backend work at
all.

Nothing here touches the pair score: the score is the matched edges alone, and
tagging only changes how it is broken down. That is why re-tagging is answered by
a resplit (`BinSimService.resplit_bin_sim`) rather than a rebuild.

It adds no asymptotic cost: everything here is O(1) per already-matched edge.
"""

from collections import defaultdict

# Two distinct kinds of "we can't attribute this to a library", kept apart on
# purpose: UNTAGGED means at least one side carries no tag at all (no evidence),
# MISMATCH means both sides are tagged but share nothing -- the interesting case,
# e.g. libc 2.31 matching libc 2.35, or libc matching uclibc.
TAG_UNTAGGED = "original_code"
TAG_MISMATCH = "tag_mismatch"

# --- Axes ------------------------------------------------------------------
# Tags answer unrelated questions and must not share one pool of mass:
#
#   origin   -- "whose code is this": libc, a vendored bundle, or nobody's
#               (original_code). Mutually exclusive, rows partition the pair.
#   severity -- "how bad is it": none/low/medium/high. One per function.
#   category -- "what does this code do": network, crypto, evasion. A function
#               can carry several, and carrying one says nothing about where the
#               code came from or how bad it is.
#   user     -- "what did a human mark on it": bookmark, ignore, free-form.
#   capa     -- "what does capa say it does": the same question as category, but
#               answered by a rule engine instead of a model, so the two are kept
#               apart and can be crossed against each other. Coverage is partial
#               by architecture (see capa_service), which is why an empty capa
#               axis never means "no capabilities".
#   mitre    -- which ATT&CK technique, verbatim from whatever writes `mitre:`
#               tags (`rulezet_service`, off the MISP attack-pattern galaxy).
#   yara     -- which vendored YARA rule matched, under
#               `yara:<category>:<family>:<rule_name>` -- another rule engine
#               answering the same question as capa/category, kept on its own
#               axis for the same reason.
#   family   -- "which named thing is this": Cobalt Strike, LockBit, UPX. Fed by
#               the `misp:` and `rulezet:` namespaces of the mirrored rules. A
#               different question from `category` (what the code does) and from
#               `origin` (whose code it is): a function can be original code
#               that does networking and belong to Mirai.
#   vuln     -- "which vulnerability is in play", `cve:`/`ghsa:`/`pysec:`. Kept
#               off `family` because a CVE names a bug in someone else's
#               software, not the thing the rule is looking for.
#
# `mbc:` had an axis here that nothing ever wrote. The namespace stays reserved
# in tag_taxonomy; the axis comes back when a producer does.
#
# Splitting one flat tag space by `conf / len(tags)` mixed them: a single
# behaviour tag on a libc function used to halve libc's mass, and the same tag on
# genuine original code evicted it from `original_code` entirely (that bucket
# only fills when a function has *no* tags). Routing by namespace keeps each
# axis's mass whole.
#
# Severity and category used to be one welded id (`flag:<risk>:<capability>`),
# which made "how much of the shared mass is high-severity network code"
# unanswerable without string surgery. They are separate axes now precisely so
# the Sankey can put one on each side.
AXIS_ORIGIN = "origin"
AXIS_SEVERITY = "severity"
AXIS_CATEGORY = "category"
AXIS_USER = "user"
AXIS_CAPA = "capa"
AXIS_MITRE = "mitre"
AXIS_YARA = "yara"
AXIS_FAMILY = "family"
AXIS_VULN = "vuln"

# Every axis, in the order the UI offers them.
AXES = (
    AXIS_ORIGIN, AXIS_SEVERITY, AXIS_CATEGORY, AXIS_USER, AXIS_CAPA,
    AXIS_MITRE, AXIS_YARA, AXIS_FAMILY, AXIS_VULN,
)

# namespace prefix -> axis. Priority is resolved separately (ORIGIN_PRIORITY),
# because for `origin:` it depends on the *second* segment, not the first.
# Several namespaces share `family` and `vuln`: an axis is a question, and
# `misp:tool:cobalt-strike` and `rulezet:runtime-packer:pe:upx` answer the same
# one whatever taxonomy they came out of.
TAG_NAMESPACES = {
    "origin": AXIS_ORIGIN,
    "severity": AXIS_SEVERITY,
    "category": AXIS_CATEGORY,
    "user": AXIS_USER,
    "capa": AXIS_CAPA,
    "mitre": AXIS_MITRE,
    "yara": AXIS_YARA,
    "misp": AXIS_FAMILY,
    "rulezet": AXIS_FAMILY,
    "cve": AXIS_VULN,
    "ghsa": AXIS_VULN,
    "pysec": AXIS_VULN,
}

# Priority only matters inside origin, where a function must resolve to one
# source; the other axes overlay and never compete. Library tags outrank bundle
# tags because Function ID matched actual bytes, while a bundle tag usually
# labels a whole binary: a statically linked memcpy inside a Mirai sample is
# still libc's, and calling it Mirai's is how a libc floor turns into a fake
# family attribution.
ORIGIN_PRIORITY = {"lib": 100, "stdlib": 100, "bundle": 50}
DEFAULT_ORIGIN_PRIORITY = 0

# Origin ids are `origin:kind:name:version[:func]`. Bundles have no natural
# version but carry this placeholder anyway, so the roll-up depth is one constant
# instead of a per-kind table. `parse_tag_id` hides it again for display.
ORIGIN_NO_VERSION = "unknown"

# A tag with no known namespace (a bare `mirai` typed into the tag box) lands on
# the user axis: it came from a human, and an unrecognised tag must never be able
# to silently empty `original_code` or dilute the LLM's behaviour percentages.
# Give it an `origin:bundle:` prefix to make it an origin.
DEFAULT_AXIS = AXIS_USER

# Bumped whenever the stored split changes shape. A doc carrying an older
# schema is stale no matter what its `tags_rev` says -- without this, a doc
# written by the two-axis code and one written here are indistinguishable, and
# the UI silently renders an axis that was never computed.
SPLIT_SCHEMA = 5

# Similarity is bucketed into fixed 5% bins so the UI can re-aggregate to any of
# its 5/10/20/25% split settings without the backend knowing which is selected.
TAG_BINS = 20


def normalize_tags(raw):
    """Function `tags` metadata -> {tag_id: weight}.

    Accepts the two shapes found in function meta: a plain list of tag ids, or a
    mapping of tag id to confidence. Anything else yields no tags.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            try:
                out[str(k)] = float(v)
            except (TypeError, ValueError):
                out[str(k)] = 1.0
        return out
    if isinstance(raw, (list, tuple, set)):
        return {str(k): 1.0 for k in raw if k}
    return {}


def merge_tag_fields(meta):
    """A function's tags from both fields of its metadata document.

    `tags` is what an analyzer wrote (Function ID); `user_tags` is what a human
    or the LLM added. Which axis a tag lands on is decided by its namespace, not
    by the field that carried it, so LLM findings reach the split without being
    promoted into the analyzer's namespace. The analyzer wins a duplicate id.
    """
    meta = meta or {}
    out = normalize_tags(meta.get("tags"))
    for tag_id, conf in normalize_tags(meta.get("user_tags")).items():
        out.setdefault(tag_id, conf)
    return out


def tag_axis(tag_id):
    """Which of the four questions this tag answers."""
    if tag_id in (TAG_UNTAGGED, TAG_MISMATCH):
        return AXIS_ORIGIN
    head = str(tag_id).split(":", 1)[0]
    return TAG_NAMESPACES.get(head, DEFAULT_AXIS)


def tag_priority(tag_id, tag_meta=None):
    """Priority of a tag: explicit metadata beats the namespace default.

    Looked up on the full id first, then on the display parent, so setting a
    priority on `origin:lib:libc:2.31` covers its 400 function-level children
    without touching each one.
    """
    meta = tag_meta or {}
    for key in (tag_id, tag_parent(tag_id)):
        entry = meta.get(key)
        if isinstance(entry, dict) and entry.get("priority") not in (None, ""):
            try:
                return int(entry["priority"])
            except (TypeError, ValueError):
                pass
    if tag_axis(tag_id) != AXIS_ORIGIN:
        return 0
    # `origin:lib:...` vs `origin:bundle:...` -- the kind is the second segment.
    parts = str(tag_id).split(":")
    kind = parts[1] if len(parts) > 1 else ""
    return ORIGIN_PRIORITY.get(kind, DEFAULT_ORIGIN_PRIORITY)


# Depth an axis rolls up to in the summary. Origin keeps
# `origin:kind:name:version` (`origin:lib:libc:2.31`) so versions stay separable
# -- which is why bundles carry a version segment too (`unknown` when there is
# none), rather than needing a per-kind depth. The other axes keep
# `namespace:name`, so `category:network:c2` and `category:network:dns` nest
# under the behaviour group they refine.
# `capa:` ids mirror a capa rule namespace (`capa:host-interaction:file-system`),
# which is 2-3 segments deep. Depth 2 rolls them up to capa's ~12 top-level
# namespaces, which is both the readable Sankey grouping and what keeps the joint
# key small: a function matching eight capa rules contributes one or two parents
# to its combo, not eight rule names.
# `family` is the one axis where the leaf *is* the answer -- the useful node is
# "Cobalt Strike", not "misp:tool" -- so its depth is set past the longest id it
# carries (`rulezet:runtime-packer:pe:upx`) and every family is its own row.
# `vuln` needs no depth for the same reason: a CVE id is already whole at 2.
_PARENT_DEPTH = {
    AXIS_ORIGIN: 4,
    AXIS_SEVERITY: 2,
    AXIS_CATEGORY: 2,
    AXIS_USER: 2,
    AXIS_CAPA: 2,
    AXIS_MITRE: 2,
    AXIS_YARA: 2,
    AXIS_FAMILY: 4,
    AXIS_VULN: 2,
}


def tag_parent(tag_id):
    """Display parent of a tag: `origin:lib:libc:2.31:memcpy` ->
    `origin:lib:libc:2.31`, `category:network:c2` -> `category:network`.

    Matching uses the full id; only the summary rolls up. Synthetic buckets and
    already-short ids are their own parent.
    """
    if tag_id in (TAG_UNTAGGED, TAG_MISMATCH):
        return tag_id
    parts = str(tag_id).split(":")
    depth = _PARENT_DEPTH[tag_axis(tag_id)]
    if len(parts) > depth:
        return ":".join(parts[:depth])
    return tag_id


def split_axes(tags, tag_meta=None):
    """{tag_id: conf} -> {axis: {tag_id: conf}}, one entry per axis in AXES.

    Origin is resolved by priority: only the highest-priority source on a
    function survives, so `origin:lib:libc:2.31` + `origin:bundle:mirai:unknown`
    counts once, as libc. Equal priorities keep the even split -- a genuine tie
    has no better answer than "half each". The other axes are never filtered;
    overlap is the point.
    """
    out = {axis: {} for axis in AXES}
    for tag_id, conf in (tags or {}).items():
        out[tag_axis(tag_id)][tag_id] = conf
    origin = out[AXIS_ORIGIN]
    if len(origin) > 1:
        best = max(tag_priority(t, tag_meta) for t in origin)
        out[AXIS_ORIGIN] = {
            t: c for t, c in origin.items() if tag_priority(t, tag_meta) == best
        }
    return out


def parse_tag_id(tag_id):
    """Derive (type, name, version) for display.

    Origin ids carry a kind segment (`origin:lib:libc:2.31`), so the displayed
    type is that kind rather than the literal `origin`; the other axes use their
    namespace as the type. A bundle's placeholder `unknown` version reads as no
    version at all, which is what the UI should show.

    Family and vuln ids name a thing rather than a group refined by a leaf, and
    they are not all the same depth (`misp:tool:cobalt-strike`,
    `rulezet:runtime-packer:pe:upx`, `cve:cve-2021-44228`), so the *last* segment
    is the name. Reading them positionally like a `category:` id would label
    Cobalt Strike "tool" and drop `upx` off the end of its id entirely.
    """
    if tag_id == TAG_UNTAGGED:
        return ("original_code", "Original Code", "")
    if tag_id == TAG_MISMATCH:
        return ("mismatch", "Tag mismatch", "")
    parts = str(tag_id).split(":")
    if len(parts) == 1:
        return (AXIS_USER, parts[0], "")
    if parts[0] == AXIS_ORIGIN:
        kind = parts[1]
        name = parts[2] if len(parts) > 2 else kind
        version = parts[3] if len(parts) > 3 else ""
        return (kind, name, "" if version == ORIGIN_NO_VERSION else version)
    if tag_axis(tag_id) in (AXIS_FAMILY, AXIS_VULN):
        return (parts[1] if len(parts) > 2 else parts[0], parts[-1], "")
    return (parts[0], parts[1], parts[2] if len(parts) > 2 else "")


def _bin_index(score):
    idx = int(score * TAG_BINS)
    if idx < 0:
        return 0
    if idx >= TAG_BINS:
        return TAG_BINS - 1
    return idx


class TagSplit:
    """Accumulates per-tag mass while a binary pair is being matched.

    Usage: `add_match` for every accepted greedy edge, `add_unique` for every
    function left over on either side, then `summary()` once the totals are known.
    """

    def __init__(self, fid_tags, untagged=TAG_UNTAGGED):
        # fid -> {tag_id: weight}; built once per build_bin_sim run, not per pair.
        self.fid_tags = fid_tags
        # Bucket for functions this axis knows nothing about. Provenance needs
        # one (`original_code` is a real answer to "whose code is this"); the
        # flags axis passes None, because "carries no flag" is the absence of a
        # finding, not a finding, and must not become a row competing with the
        # flags that were actually raised.
        self.untagged = untagged
        self._default = {untagged: 1.0} if untagged else {}
        self.cohesion = defaultdict(float)
        self.matched_w = defaultdict(float)
        self.matched_n = defaultdict(float)
        self.side_w = {"a": defaultdict(float), "b": defaultdict(float)}
        self.unique_w = {"a": defaultdict(float), "b": defaultdict(float)}
        self.unique_n = {"a": defaultdict(float), "b": defaultdict(float)}
        # Matched mass whose partner did not carry the same tag. Reported, not
        # rebucketed -- see add_match.
        self.mismatch_w = {"a": defaultdict(float), "b": defaultdict(float)}
        # tag_id -> partner tag id -> weight. `mismatch_w` says a tag's mass failed
        # to agree; this says what it disagreed *with*, which is the difference
        # between "libc has 12 drifted functions" and "libc 2.31 drifted to 2.35".
        self.mismatch_pairs = defaultdict(lambda: defaultdict(float))
        # tag_id -> bin index -> [count_a, weight_a, count_b, weight_b]. Both
        # sides are tracked because a match need not be tagged the same on each.
        self.bins = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0, 0.0, 0.0]))

    def add_match(self, fid_a, fid_b, score, w_a, w_b):
        """Attribute one matched pair. `w_a`/`w_b` are each side's feature counts.

        Each side is attributed to *its own* tags, exactly like `add_unique` does
        for leftovers. Requiring both sides to agree before a match counts toward
        a tag would strand every pair where only one side got recognised, and
        those stranded matches would make the tag look overwhelmingly unmatched:
        A's libc mass counted as libc when it failed to match, but as `untagged`
        when it succeeded. Disagreement is recorded per tag as `mismatch_*`
        instead of moving the mass somewhere else.
        """
        tags_a = self.fid_tags.get(fid_a) or self._default
        tags_b = self.fid_tags.get(fid_b) or self._default
        shared = set(tags_a) & set(tags_b)
        idx = _bin_index(score)

        for side, tags, weight, slot in (
            ("a", tags_a, w_a, 0),
            ("b", tags_b, w_b, 2),
        ):
            n = len(tags)
            for tag_id, conf in tags.items():
                # Confidence caps a tag's claim on its function; a shared tag is
                # only as strong as the weaker side's confidence.
                if tag_id in shared:
                    conf = min(tags_a[tag_id], tags_b[tag_id])
                frac = conf / n
                self.cohesion[tag_id] += score * weight * frac
                self.matched_w[tag_id] += weight * frac
                self.matched_n[tag_id] += frac
                self.side_w[side][tag_id] += weight * frac
                if tag_id not in shared and tag_id != self.untagged:
                    self.mismatch_w[side][tag_id] += weight * frac
                    # Roll the partner up to its display parent: the useful fact
                    # is "drifted to libc 2.35", not which of its 400 functions
                    # this particular edge landed on.
                    others = tags_b if side == "a" else tags_a
                    for partner in others:
                        if partner in shared or partner == self.untagged:
                            continue
                        self.mismatch_pairs[tag_id][tag_parent(partner)] += (
                            weight * frac / len(others)
                        )
                b = self.bins[tag_id][idx]
                b[slot] += frac
                b[slot + 1] += weight * frac

    def add_unique(self, fid, weight, side):
        """Attribute one unmatched function to its own tags (no peer to share with)."""
        tags = self.fid_tags.get(fid) or self._default
        n = len(tags)
        for tag_id, w in tags.items():
            frac = w / n
            self.unique_w[side][tag_id] += weight * frac
            self.unique_n[side][tag_id] += frac

    def summary(self, total_weight_a, total_weight_b, tag_meta=None):
        """Roll up to one row per display parent, with children nested.

        The side totals are each binary's full mass, so coverage answers "how
        much of binary A is this tag". `contribution_pct` is against the mass
        this split itself accounted for, so the rows sum to 100.
        All `*_pct` fields are 0-100.
        """
        tag_meta = tag_meta or {}
        total_weight = sum(self.matched_w.values())
        tag_ids = (
            set(self.matched_w) | set(self.unique_w["a"]) | set(self.unique_w["b"])
        )

        parents = defaultdict(list)
        for tag_id in tag_ids:
            parents[tag_parent(tag_id)].append(tag_id)

        out = []
        for parent, children in parents.items():
            rows = [
                self._row(t, total_weight, total_weight_a, total_weight_b, tag_meta)
                for t in children
            ]
            row = self._row(
                parent,
                total_weight,
                total_weight_a,
                total_weight_b,
                tag_meta,
                merge=rows,
            )
            # A parent that is just its single child adds a pointless nesting level.
            row["children"] = (
                sorted(rows, key=lambda x: -x["matched_weight"])
                if children != [parent]
                else []
            )
            out.append(row)
        return sorted(out, key=lambda x: -x["matched_weight"])

    def _row(self, tag_id, total_w, total_a, total_b, tag_meta, merge=None):
        if merge is None:
            matched_w = self.matched_w.get(tag_id, 0.0)
            mm_a = self.mismatch_w["a"].get(tag_id, 0.0)
            mm_b = self.mismatch_w["b"].get(tag_id, 0.0)
            matched_n = self.matched_n.get(tag_id, 0.0)
            cohesion = self.cohesion.get(tag_id, 0.0)
            w_a = self.side_w["a"].get(tag_id, 0.0)
            w_b = self.side_w["b"].get(tag_id, 0.0)
            uw_a = self.unique_w["a"].get(tag_id, 0.0)
            uw_b = self.unique_w["b"].get(tag_id, 0.0)
            un_a = self.unique_n["a"].get(tag_id, 0.0)
            un_b = self.unique_n["b"].get(tag_id, 0.0)
            bins = {str(k): list(v) for k, v in self.bins.get(tag_id, {}).items()}
            drift = dict(self.mismatch_pairs.get(tag_id, {}))
        else:
            matched_w = sum(r["matched_weight"] for r in merge)
            matched_n = sum(r["matched_count"] for r in merge)
            cohesion = sum(r["score"] * r["matched_weight"] for r in merge)
            w_a = sum(r["weight_a"] for r in merge)
            w_b = sum(r["weight_b"] for r in merge)
            uw_a = sum(r["unique_weight_a"] for r in merge)
            uw_b = sum(r["unique_weight_b"] for r in merge)
            un_a = sum(r["unique_count_a"] for r in merge)
            un_b = sum(r["unique_count_b"] for r in merge)
            mm_a = sum(r["mismatch_weight_a"] for r in merge)
            mm_b = sum(r["mismatch_weight_b"] for r in merge)
            bins = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
            drift = defaultdict(float)
            for r in merge:
                for k, v in r["bins"].items():
                    acc = bins[k]
                    for i in range(4):
                        acc[i] += v[i]
                for partner, w in r["drift"].items():
                    drift[partner] += w
            bins = dict(bins)
            drift = dict(drift)

        t_type, t_name, t_version = parse_tag_id(tag_id)
        meta = tag_meta.get(tag_id) or {}
        return {
            "tag_id": tag_id,
            "type": meta.get("type") or t_type,
            "name": meta.get("name") or t_name,
            "version": meta.get("version") or t_version,
            "source": meta.get("source", "system"),
            "color": meta.get("color", ""),
            "score": cohesion / matched_w if matched_w > 0 else 0.0,
            "contribution_pct": 100.0 * matched_w / total_w if total_w > 0 else 0.0,
            "matched_weight": matched_w,
            "matched_count": matched_n,
            "weight_a": w_a,
            "weight_b": w_b,
            "unique_weight_a": uw_a,
            "unique_weight_b": uw_b,
            "mismatch_weight_a": mm_a,
            "mismatch_weight_b": mm_b,
            "unique_count_a": un_a,
            "unique_count_b": un_b,
            "coverage_pct_a": 100.0 * (w_a + uw_a) / total_a if total_a > 0 else 0.0,
            "coverage_pct_b": 100.0 * (w_b + uw_b) / total_b if total_b > 0 else 0.0,
            "bins": bins,
            # partner tag id -> weight that failed to agree with it. Empty for
            # every tag that matched cleanly.
            "drift": drift,
        }


# Separator for a tag *set* used as one joint key. Severity, category and user
# tags overlap by design, so the crossing is keyed by the whole set a function
# carries -- the only way a flow diagram can stay both conserving and countable
# in whole functions. Each axis summary still reports every tag's full mass
# separately, where overlap is the point and rows are free to exceed 100%.
TAG_COMBO_SEP = " + "

# Separator between the three non-origin axes inside one joint key. A control
# character, so a user-typed tag can never forge a key boundary; JSON escapes it
# as  and JS splits on it unchanged.
AXIS_SEP = "\x1f"

# Order of the axes packed into the inner joint key. Origin is the outer key.
# Appending an axis here is the whole cost of adding one to the joint -- every
# Sankey mode is a marginal of this one table, so N axes cost N+1 key segments
# rather than N*(N-1)/2 stored matrices.
JOINT_INNER_AXES = (
    AXIS_SEVERITY, AXIS_CATEGORY, AXIS_USER, AXIS_CAPA, AXIS_MITRE,
    AXIS_YARA, AXIS_FAMILY, AXIS_VULN,
)


def tag_combo(tags):
    """Stable name for the set of tags one function carries on one axis."""
    return TAG_COMBO_SEP.join(sorted({tag_parent(t) for t in tags}))


# Joint cell layout, mirroring what a Sankey column needs on each side:
# matched and unmatched mass, in features and in function counts.
MATRIX_SLOTS = ("w_shared_a", "w_shared_b", "w_uniq_a", "w_uniq_b",
                "n_shared_a", "n_shared_b", "n_uniq_a", "n_uniq_b")


def joint_key(*combos):
    """Pack one combo name per JOINT_INNER_AXES entry into a joint inner key.

    Variadic so adding an axis is a one-line change to JOINT_INNER_AXES rather
    than a signature edit here and at the call site.
    """
    return AXIS_SEP.join(combos)


def joint_marginal(joint, axis_a, axis_b=None):
    """Collapse the stored joint down to one or two axes.

    The joint is the only crossing the backend stores; every one of the ten
    Sankey modes is a marginal of it, summed over the axes the view does not
    show. That is the whole reason it is stored as one table instead of ten
    precomputed matrices: adding a fifth axis later costs one more key segment,
    not six more matrices.

    Returns `{a_label: {b_label: [8 slots]}}`. With `axis_b` omitted the inner
    dict has the single key `""`, so a single-axis view reads the same shape.
    """
    def label(outer, inner_parts, axis):
        if axis == AXIS_ORIGIN:
            return outer
        return inner_parts[JOINT_INNER_AXES.index(axis)]

    out = defaultdict(lambda: defaultdict(lambda: [0.0] * len(MATRIX_SLOTS)))
    for outer, row in (joint or {}).items():
        for inner, cell in row.items():
            parts = str(inner).split(AXIS_SEP)
            if len(parts) != len(JOINT_INNER_AXES):
                continue
            a = label(outer, parts, axis_a)
            b = label(outer, parts, axis_b) if axis_b else ""
            acc = out[a][b]
            for i, v in enumerate(cell):
                acc[i] += v
    return {a: {b: list(c) for b, c in row.items()} for a, row in out.items()}


class AxisSplit:
    """All four axes over one pass of the same matched edges, plus their joint.

    A `TagSplit` answers each axis alone. The question none of them answers is
    the crossing -- "the 30% that matched is libc, and this much of it is
    high-severity network code" -- so a joint table is accumulated in the same
    loop. Keyed by display parent, because that is the deepest level any summary
    row is drawn at.

    Drop-in for `TagSplit` at the call sites: same `add_match` / `add_unique`.
    """

    def __init__(self, fid_tags, tag_meta=None):
        self.fid_axes = {axis: {} for axis in AXES}
        for fid, tags in (fid_tags or {}).items():
            axes = split_axes(tags, tag_meta)
            for axis, picked in axes.items():
                if picked:
                    self.fid_axes[axis][fid] = picked
        # Only origin gets an `untagged` bucket: `original_code` is a real answer
        # to "whose code is this", while "carries no severity" is the absence of
        # a finding, not a finding, and must not become a row competing with the
        # findings that were actually raised.
        self.splits = {
            axis: TagSplit(
                self.fid_axes[axis],
                untagged=TAG_UNTAGGED if axis == AXIS_ORIGIN else None,
            )
            for axis in AXES
        }
        self.joint = defaultdict(lambda: defaultdict(lambda: [0.0] * len(MATRIX_SLOTS)))

    # Named accessor kept because the origin axis is the one the pair score is
    # conventionally broken down by.
    @property
    def origin(self):
        return self.splits[AXIS_ORIGIN]

    def __bool__(self):
        return any(self.fid_axes[axis] for axis in AXES)

    def _cross(self, fid, weight, w_slot, n_slot):
        """Spread one function's mass over its (origin, severity, category, user) cell.

        A function carrying `crypto` and `network` goes to one cell named for
        both, not half to each. Splitting it evenly conserved the totals but made
        every count fractional, and left no node answering "how many functions
        are network functions" -- the flow would show 0.5 of a function under
        each tag and none under the pair it actually is.

        Only functions carrying at least one non-origin tag land here, so an
        origin row's untagged mass is what the row keeps after its cells are
        subtracted -- no bucket to store, nothing to keep in sync.

        ponytail: the origin share ignores the confidence capping
        `TagSplit.add_match` applies to a *shared* tag, so with fractional
        confidences the cells can slightly undershoot their row. Consumers
        clamp the remainder at zero; mirror the capping here if confidences
        ever stop being 1.0 in practice.
        """
        combos = [tag_combo(self.fid_axes[a].get(fid) or {}) for a in JOINT_INNER_AXES]
        if not any(combos):
            return
        inner = joint_key(*combos)
        origin = self.fid_axes[AXIS_ORIGIN].get(fid) or {TAG_UNTAGGED: 1.0}
        n_o = len(origin)
        for o, conf in origin.items():
            share = conf / n_o
            cell = self.joint[tag_parent(o)][inner]
            cell[w_slot] += weight * share
            cell[n_slot] += share

    def add_match(self, fid_a, fid_b, score, w_a, w_b):
        for split in self.splits.values():
            split.add_match(fid_a, fid_b, score, w_a, w_b)
        self._cross(fid_a, w_a, 0, 4)
        self._cross(fid_b, w_b, 1, 5)

    def add_unique(self, fid, weight, side):
        for split in self.splits.values():
            split.add_unique(fid, weight, side)
        self._cross(fid, weight, 2 if side == "a" else 3, 6 if side == "a" else 7)

    def summaries(self, total_weight_a, total_weight_b, tag_meta=None):
        """The fields a bin_sim doc stores for tags.

        `tags_summary` keeps its historical name -- it has always been the origin
        axis, and renaming it would churn the tree, table and container-sim
        renderers that already read it for exactly that.
        """
        out = {
            "tags_summary": self.splits[AXIS_ORIGIN].summary(
                total_weight_a, total_weight_b, tag_meta
            ),
            "joint": {
                p: {k: list(cell) for k, cell in row.items() if any(cell)}
                for p, row in self.joint.items()
            },
            "split_schema": SPLIT_SCHEMA,
        }
        for axis in JOINT_INNER_AXES:
            out[f"{axis}_summary"] = self.splits[axis].summary(
                total_weight_a, total_weight_b, tag_meta
            )
        return out


# The per-axis row lists a bin_sim doc stores, in AXES order. `tags_summary` is
# the origin axis under its historical name.
SUMMARY_FIELDS = (
    "tags_summary",
    "severity_summary",
    "category_summary",
    "user_summary",
    "capa_summary",
    "mitre_summary",
    "yara_summary",
    "family_summary",
    "vuln_summary",
)

# Everything `summaries()` writes, so callers that must produce an empty split do
# not have to keep their own drifting copy of the list.
EMPTY_SUMMARIES = {
    **{f: [] for f in SUMMARY_FIELDS},
    "joint": {},
    "split_schema": SPLIT_SCHEMA,
}


def tags_rev_key(collection):
    return f"{collection}:tags_rev"


def read_tags_rev(r, collection):
    """Current tag revision of a collection.

    A counter bumped by every tag write. A stored split carries the revision it
    was computed against, so the UI can say "this split predates your tagging"
    instead of silently showing stale rows -- and the resplit is only offered
    when it would actually change something.
    """
    try:
        raw = r.get(tags_rev_key(collection))
    except Exception:
        return 0
    if raw is None:
        return 0
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except (TypeError, ValueError):
        return 0


def bump_tags_rev(r, collection):
    try:
        r.incr(tags_rev_key(collection))
    except Exception:
        pass


def load_tag_meta(r, collection):
    """Read the collection's tag metadata hash.

    Values are historically a bare colour string (see TagService._ensure_tag_metadata)
    and may be a JSON object once richer metadata is written; both are accepted.
    """
    import json

    try:
        raw = r.hgetall(f"{collection}:tags_metadata") or {}
    except Exception:
        return {}
    out = {}
    for k, v in raw.items():
        tag_id = k.decode() if isinstance(k, bytes) else str(k)
        val = v.decode() if isinstance(v, bytes) else v
        if isinstance(val, str) and val.startswith("{"):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    out[tag_id] = parsed
                    continue
            except ValueError:
                pass
        out[tag_id] = {"color": val}
    return out
