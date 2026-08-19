"""The tag vocabulary, and how the old one maps onto it.

One module so the LLM prompt, the validator that rejects invented tags, and the
migration script cannot drift apart -- the prompt listing a capability the
validator strips is exactly the failure this centralises away.

Four namespaces, one per question a tag can answer (the axes that consume them
live in `bin_sim_tags.py`):

    origin:<kind>:<name>:<version>[:<func>]   whose code is this
    severity:<level>                          how bad is it
    category:<group>:<leaf>                   what does it do
    user:<slug>                               what did a human mark on it

`capa:`, `mitre:` and `mbc:` are externally standardised namespaces, recorded
verbatim rather than remapped into `category:` -- matching what the tool that
wrote them emits is their whole value. `kill-chain:` and the MAEC namespaces are
reserved for the same treatment but have no producer yet.

`misp:`, `rulezet:` and the vulnerability namespaces (`cve:`, `ghsa:`, `pysec:`)
are written by `rulezet_service` for mirrored rules; `route_source_tag()` is how
a MISP-style source tag becomes one of them. The first two feed the `family`
axis and the last three the `vuln` axis -- namespaces are sources, axes are
questions, and several sources can answer one question.
"""

import fnmatch
import re

# --- Severity ---------------------------------------------------------------
# Ordinal, so the UI can colour-ramp it. Four levels rather than five because a
# model calibrates "medium vs high" poorly enough already.
SEVERITY_LEVELS = ("none", "low", "medium", "high")

# --- Behaviour --------------------------------------------------------------
# Two levels: the graph draws the group, search and filtering keep the leaf.
# `util` is deliberate -- it is where compiler and libc plumbing lands. Without
# it that mass has nowhere to go on this axis and quietly inflates whatever else
# the model picked.
CATEGORIES = {
    "network": ("c2", "download", "upload", "p2p", "scan", "proxy", "dns", "socket"),
    "crypto": ("cipher", "hash", "key_exchange", "encoding", "random"),
    "file": ("read_write", "path", "archive", "tempfile"),
    "process": ("exec", "inject", "thread", "shell", "ipc", "privesc"),
    "persistence": ("autostart", "service", "cron", "bootloader", "registry"),
    "evasion": (
        "anti_debug",
        "anti_vm",
        "obfuscation",
        "packer",
        "rootkit",
        "log_clear",
    ),
    "recon": ("sysinfo", "proclist", "filesearch", "creds", "env"),
    # `spyware` overlaps keylog/screencap on purpose: it is the fallback when the
    # model cannot tell which, and it makes the legacy `spyware` capability map
    # across without loss. Prefer the specific leaf when it is knowable.
    "impact": ("ddos", "ransom", "wipe", "exfil", "keylog", "screencap", "spyware"),
    "util": ("init", "string", "memory", "math", "parser", "compression", "wrapper"),
}

# --- Origin -----------------------------------------------------------------
ORIGIN_KINDS = ("lib", "stdlib", "bundle")

# The origin axis is one namespace per source, not one `origin:` namespace with
# the source at segment 2. Two reasons, and neither is cosmetic: a function's
# tags are `{tag_id: weight}` with nowhere to record who found them, so the id
# has to; and it puts the library at the first level, which is what lets a
# single colour rule tell `fid:libc` from `fid:openssl` instead of needing a
# per-namespace hue depth.
#
# `lib` and `stdlib` no longer separate: the distinction never did anything --
# both carried priority 100, and nothing else read the kind. Both now take the
# namespace of whichever detector found them, which is the fact worth keeping.
#
# Only kinds that name a *source* rather than a detector's finding appear here.
# A bundle is the malware itself, so it is `malware:` whoever noticed it.
ORIGIN_KIND_NAMESPACE = {"bundle": "malware"}

# Mirrors bin_sim_tags.TAG_NAMESPACES' origin entries. `pkg:` is reserved for
# SBOM-sourced facts: purl identifies a package, which is a different claim from
# "these bytes are libc", so a detector never writes it.
ORIGIN_NAMESPACES = ("fid", "bsim", "malware", "pkg", "original")

# Kept only so `migrate_tag` can recognise the placeholder version in a legacy
# `origin:bundle:mirai:unknown`. New ids omit a version they do not have --
# depth is not fixed any more, so a filler segment bought nothing and read as a
# claim the analyzer never made.
ORIGIN_NO_VERSION = "unknown"

# File-scope only. `unpack_service` writes these onto file docs and
# `ghidra_service` strips them before they can reach a function, which is the
# behaviour to preserve: they describe the wrapper, not any function in it. A
# wrapper fact that genuinely belongs on a function goes in as
# `origin:packer:<name>:<version>` instead of a raw tag.
FILE_SCOPE_NAMESPACES = ("container", "packer")


# Written by capa, recorded verbatim from the matched rule's namespace:
# `capa:host-interaction:file-system:write`. Externally standardised, so it is
# never remapped into `category:` -- matching what capa emits is the whole value.
CAPA_NAMESPACE = "capa"

# `mitre:<technique>` (`mitre:t1027`), written by `rulezet_service` from the
# `misp-galaxy:mitre-attack-pattern` tags it recovers. Flat rather than the
# `mitre:<tactic>:<technique>` once reserved here: a technique belongs to several
# tactics at once, so a tactic segment would have to pick one arbitrarily.
# `mbc:<objective>:<behavior>` is still reserved, with no producer yet.
MITRE_NAMESPACE = "mitre"
MBC_NAMESPACE = "mbc"

# Written by the vendored YARA ruleset. Unlike capa/mitre/mbc this one is not
# recorded verbatim -- a YARA rule has no built-in namespace, so the id is built
# from the rule's own `category`/`malware`/`family` meta fields instead:
# `yara:<category>:<family>:<rule_name>`. Most of the 129k-rule rulezet mirror
# is Defender/MTB signature exports with no structured meta at all (only a
# `description` string), so `yara:unknown:unknown:<rule_name>` there is the
# correct, expected id -- not a parsing bug to chase further.
YARA_NAMESPACE = "yara"

# Written by `rulezet_service` from the MISP-style tags it produces for mirrored
# rules. `misp:` keeps a galaxy's own shape (`misp:tool:cobalt-strike`); a
# source namespace the config did not route keeps its own name
# (`runtime-packer:pe:upx`) rather than being folded under a catch-all, so an
# unmapped tag is still recorded rather than silently lost. `rulezet:` is not a
# catch-all: it holds the mirrored rule's uuid only (`rulezet:031cfb94-...`),
# written by `_match_tags` -- this is the ruleset axis. The rule *name*
# already lives in the `yara:` tag alongside it, so storing it twice would
# just be two ids for one fact.
MISP_NAMESPACE = "misp"

# Vulnerability ids, flat: `cve:cve-2021-44228`, `ghsa:ghsa-j8v8-6h6r-m6pq`.
# Flat because an id is already the whole identity -- there is no second segment
# to roll up on.
VULN_NAMESPACES = ("cve", "ghsa", "pysec")

# --- Axes -------------------------------------------------------------------
# namespace -> the question a tag in it answers. Lives here, with the
# vocabulary, because more than the split engine needs it: every view that
# groups tags by axis reads the same map, shipped to the browser by
# `color_config`. `bin_sim_tags` re-exports it as `TAG_NAMESPACES`.
#
# Several namespaces share an axis on purpose: an axis is a question, and
# `misp:tool:cobalt-strike` and `runtime-packer:pe:upx` answer the same one
# whatever taxonomy produced them. Origin has one namespace per detector, so
# `fid:` and `bsim:` can be told apart on a function that has room for nothing
# but its tag ids.
TAG_AXES = {
    "fid": "origin",
    "bsim": "origin",
    "malware": "origin",
    "pkg": "origin",
    "original": "origin",
    "origin": "origin",
    "severity": "severity",
    "category": "category",
    "user": "user",
    "capa": "capa",
    "mitre": "mitre",
    "yara": "yara",
    "rulezet": "ruleset",
    "misp": "family",
    "ms-caro-malware-full": "family",
    "runtime-packer": "family",
    "cve": "vuln",
    "ghsa": "vuln",
    "pysec": "vuln",
    # The two synthetic buckets. They are whole ids rather than namespaces, but
    # a namespace lookup on an id with no colon returns the id itself, so this
    # entry answers for them -- and any view reading the map gets what
    # `tag_axis` special-cases, instead of dropping them on the user axis.
    "original_code": "origin",
    "tag_mismatch": "origin",
}

# A tag with no known namespace (a bare `mirai` typed into the tag box) lands on
# the user axis: it came from a human, and an unrecognised tag must never be
# able to silently empty `original_code` or dilute the LLM's percentages.
DEFAULT_AXIS = "user"


# --- Colour -----------------------------------------------------------------
# A tag's colour is derived from its id, never assigned: a new namespace, a new
# library, a new capa rule all get a stable colour the day they first appear,
# with no table to update. One rule, two overrides.
#
# The rule is hue-interval subdivision. Start with the whole circle; each
# segment of the id hashes to a narrower sub-interval of the one its parent
# picked. So siblings land near each other and cousins land far: every `lib:` is
# in one arc of the wheel, `openssl` and `libc` are distinguishable corners of
# that arc, and `bundle:mirai` is nowhere near either. Segments past
# `HUE_DEPTH` stop moving the hue and step the lightness/saturation instead --
# `category:network:c2` is a shade of `category:network`, not a new colour.
#
# The two overrides: `severity:` is ordinal, so it gets the conventional ramp
# rather than a hash, and a colour a human stored on a tag always wins (the UI
# applies that one -- it is per-collection state, not vocabulary).
SEVERITY_HUES = {"none": 120, "low": 55, "medium": 30, "high": 0}

# How many segments *after* the namespace still move the hue; everything past
# them shades instead. The namespace itself is always the first band, so one
# means "namespace, then the tag's own group" -- `category:network` gets a hue
# and `category:network:c2` gets a shade of it. Two is for namespaces whose
# second segment is a kind rather than a thing: `origin:lib:libc` needs `lib`
# for the family arc *and* `libc` for its own colour.
HUE_DEPTH_DEFAULT = 1
HUE_DEPTH = {
    # Legacy ids only. `origin:lib:libc` buried the library at the second
    # segment, so one level of hue would have painted every library alike --
    # which is the bug this whole rework exists to remove. New ids put the
    # library first (`fid:libc`), so they need no entry, and this one can go
    # once no `origin:` ids remain.
    "origin": 2,
}

# --- Structure --------------------------------------------------------------
# What separates a namespace's levels, where it is not a colon, and where the
# instance tail begins. One table, one parser: `tag_levels` (colour) and
# `tag_prefixes` (index buckets) both read it, so a tag's hierarchy is the same
# fact wherever it is asked for. Two copies of this rule is what let a tree node
# take its colour from a string the index had never bucketed.
#
# ATT&CK is what forces a per-namespace entry: a sub-technique is written
# `mitre:t1027.005` because that is how ATT&CK writes it, so its second level
# hides behind a dot. A namespace keeps its source's own separators rather than
# being rewritten, so a tag stays pasteable back into the tool it came from.
TAG_SEPARATORS_DEFAULT = (":",)
TAG_SEPARATORS = {"mitre": (":", ".")}

# Everything from the first `#` is an instance, not a level: the function a
# library tag was matched on, the rule name a detection fired from. It stays
# part of the id -- searchable, filterable, displayed -- but it never becomes a
# grouping level, an index bucket, or a colour. `origin:lib:libc:2.31#memcpy`
# must not put `memcpy` in a sankey column next to `libc`.
TAG_DETAIL = "#"

# Each subdivision keeps this fraction of the interval its parent picked, per
# level. The first is narrow so unrelated groups land far apart; the second is
# wide because it is subdividing an already-small arc, and `libc` still has to
# be told from `openssl` inside it.
HUE_SHRINK = (0.3, 0.55)

# Positions within a level are quantised to slots rather than placed anywhere in
# the interval. Hashing to a continuous position means two siblings can land a
# degree apart by chance -- which they did, on the first pair anyone looks at.
# Slots make the separation a floor instead of luck: two tags are either the
# same slot or a whole slot apart.
HUE_SLOTS = 12

# Hue alone is not enough. Hashing places hues independently, so nothing stops
# three groups landing within ten degrees of each other -- and inside a family
# the interval is narrow by design, which is exactly where telling libc from
# openssl matters most. So a second hashed dimension: a tone, indexing the
# `--tagc-s{n}` / `--tagc-l{n}` pairs, which is what makes one lib read as cyan
# and its neighbour as dark blue. Taken from a different part of the hash than
# the hue so the two do not move together.
#
# ponytail: pure hash, no reserved table. Two tags can still collide outright;
# if a pair you look at often does, pin those hues in a table here rather than
# tuning the hash.
TONES = 3

# How far past HUE_DEPTH the shading goes before it stops getting lighter --
# `origin:lib:libc:2.31:memcpy` must still read as libc.
MAX_STEP = 3

# Lightness added per step past HUE_DEPTH, in percentage points.
STEP_LUM = 7


# The ids both `demo()` and `scripts/test_tag_colors.js` pin, so the Python rule
# and its JS mirror cannot drift into disagreeing about a tag's colour.
COLOR_VECTORS = (
    "severity:high",
    "severity:low",
    "origin:lib:libc",
    "origin:lib:libc:2.31:memcpy",
    "category:network",
    "capa:host-interaction:file-system:write",
    "mitre:t1027.005",
    "cve:cve-2021-44228",
    # The current shape: the library carries the hue, the version and the
    # matched symbol are shades of it, and a second library is a different hue
    # rather than a different lightness of the same one.
    "fid:libc",
    "fid:libc:2.31",
    "fid:libc:2.31#memcpy",
    "fid:openssl",
    "yara:trojan:mirai#ELF_Mirai",
)


def tag_separators(namespace):
    """The characters that separate levels inside this namespace's ids."""
    return TAG_SEPARATORS.get(namespace, TAG_SEPARATORS_DEFAULT)


def tag_in_scope(tag_id, prefix):
    """Whether a tag falls under a scope prefix, by levels rather than by text.

    A scope names levels, so it must be tested against levels: a plain
    `startswith(prefix + ":")` reads `fid:uclibc:0.9.30.1#xdrmem_getint32` as
    outside `fid:uclibc:0.9.30.1`, because the next character is the detail
    marker rather than a colon. That is how selecting a library version came
    back with nothing.

    Uses the same prefixes the index buckets under, so a scope selects in the
    UI exactly what it selects in a search.
    """
    if not tag_id or not prefix:
        return False
    body = tag_body(tag_id)[0]
    return prefix == body or prefix in tag_prefixes(tag_id)


def canonical_tag_id(tag_id):
    """A tag id in the form every consumer downstream may assume.

    One function, called by every constructor here and by the user-tag write
    path, so an id cannot enter the system in a shape the tree, the index or the
    colour rule would read differently. It normalises only what those layers
    cannot carry:

      * whitespace becomes `-`, because a space has to be quoted in a filter
        value and percent-encoded in a permalink
      * `"` is dropped, because it is how `query_syntax` quotes a literal
      * empty levels collapse, so `a::b` and `a:b` are one tag rather than two
      * case folds *the levels*, because the index buckets lowercase and two
        casings of one tag would be two vocabulary entries pointing at the same
        functions

    The detail tail keeps its case: it is a symbol, and `EVP_EncryptInit` is not
    `evp_encryptinit` in any debugger the analyst will paste it into. It is not a
    grouping level, so nothing depends on it folding.

    Everything else survives untouched. A source's own punctuation is data --
    `2.31`, `libstdc++`, `System.Net.Http` and `t1027.005` all keep their dots,
    and whether a dot is a level is `TAG_SEPARATORS`' business, not this one's.
    A bare word becomes a `user:` tag: it came from a human, and an
    unnamespaced id must not silently land on an analysis axis.
    """
    raw = " ".join(str(tag_id or "").split()).replace('"', "")
    if not raw:
        return ""
    body, detail = tag_body(raw)
    body = ":".join(p for p in body.split(":") if p)
    if not body:
        return ""
    out = body if ":" in body else f"user:{body}"
    out = out.replace(" ", "-").lower()
    return f"{out}{TAG_DETAIL}{detail.replace(' ', '-')}" if detail else out


def _split_keep(body, seps):
    """`[seg, sep, seg, sep, ...]` -- separators kept so prefixes stay literal.

    Longest separator first, so a two-character one is never split by the
    single-character one it contains.
    """
    pattern = (
        "(" + "|".join(re.escape(x) for x in sorted(seps, key=len, reverse=True)) + ")"
    )
    return re.split(pattern, body)


def tag_body(tag_id):
    """`(body, detail)` -- the id split at the first `#`, which starts the tail."""
    body, marker, detail = str(tag_id).partition(TAG_DETAIL)
    return body, (detail if marker else "")


def tag_levels(tag_id):
    """`(namespace, levels, detail)` for a tag id.

    `levels` excludes the namespace, so `category:network:c2` yields
    `("category", ["network", "c2"], "")`. Nothing here namespaces a bare tag --
    callers that want `mytag` read as `user:mytag` apply `namespaced()` first,
    because the index must keep bucketing the literal value it was handed.
    """
    body, detail = tag_body(tag_id)
    ns = body.split(":", 1)[0]
    parts = _split_keep(body, tag_separators(ns))
    segs = [p for i, p in enumerate(parts) if i % 2 == 0 and p]
    return ns, segs[1:], detail


def tag_prefixes(tag_id):
    """Ancestor prefixes of a tag id, excluding itself.

    `origin:lib:libc:2.31` -> `['origin', 'origin:lib', 'origin:lib:libc']`, and
    the detail tail contributes nothing: `lib:libc:2.31#memcpy` stops at
    `lib:libc`, so no bucket named after a function can exist. Each prefix keeps
    the original separators, so it is a literal prefix of the value rather than a
    normalized form of it.
    """
    body, _ = tag_body(tag_id)
    ns = body.split(":", 1)[0]
    parts = _split_keep(body, tag_separators(ns))
    out, prefix = [], parts[0]
    for i in range(1, len(parts) - 1, 2):
        if prefix:
            out.append(prefix)
        prefix += parts[i] + parts[i + 1]
    return out


def _hash32(text):
    """FNV-1a over UTF-16 code units.

    Code units rather than UTF-8 bytes so the JS side can produce the same
    number with `charCodeAt` -- the colour has to be identical in both, and the
    ids in play are ASCII anyway.
    """
    h = 0x811C9DC5
    for ch in str(text):
        h = ((h ^ (ord(ch) & 0xFFFF)) * 0x01000193) & 0xFFFFFFFF
    return h


def tag_style(tag_id):
    """`(hue, tone, step)` for a tag id.

    Hue in degrees, tone indexing the S/L pairs, step how far the shade is
    lightened past the tag's own colour. `hue` is None for an id with nothing to
    hash (a bare namespace), which the UI draws grey.
    """
    ns, rest, _detail = tag_levels(namespaced(tag_id))
    segs = [ns] + rest
    if not rest:
        return None, 0, 0

    depth = HUE_DEPTH.get(ns, HUE_DEPTH_DEFAULT)
    step = min(max(0, len(rest) - depth), MAX_STEP)
    # Severity is ordinal, so it keeps the conventional ramp and one flat tone;
    # its leaves would otherwise be four hashes with no order to read.
    if ns == "severity" and rest[0] in SEVERITY_HUES:
        return SEVERITY_HUES[rest[0]], 1, step

    lo, span, h = 0.0, 360.0, 0
    for i in range(min(depth, len(rest))):
        # The whole prefix, not the segment alone: it salts the hash with the
        # namespace, so two namespaces spread over the circle differently
        # instead of putting `lib` and `network` on the same hue.
        h = _hash32(":".join(segs[: i + 2]))
        width = span * HUE_SHRINK[min(i, len(HUE_SHRINK) - 1)]
        lo += (h % HUE_SLOTS) * (span - width) / (HUE_SLOTS - 1)
        span = width
    return round(lo + span / 2, 2), (h >> 20) % TONES, step


def color_config():
    """The rule's parameters, for the UI to apply the same rule client-side.

    Shipped rather than a colour per tag because the UI folds ids the backend
    never sent -- `fid:libc:2.31` up to `fid:libc` happens in the browser, and
    that folded node still needs its colour.

    Carries the namespace -> axis map too. Any view that groups tags by axis
    needs it, and a second copy living in JS is exactly the drift that let a
    tree colour a node from a string the index had never bucketed.
    """
    return {
        "tag_axes": dict(TAG_AXES),
        "tag_axis_default": DEFAULT_AXIS,
        "severity_hues": SEVERITY_HUES,
        "hue_depth": HUE_DEPTH,
        "hue_depth_default": HUE_DEPTH_DEFAULT,
        "tag_separators": {k: list(v) for k, v in TAG_SEPARATORS.items()},
        "tag_separators_default": list(TAG_SEPARATORS_DEFAULT),
        "tag_detail": TAG_DETAIL,
        "hue_shrink": list(HUE_SHRINK),
        "hue_slots": HUE_SLOTS,
        "tones": TONES,
        "max_step": MAX_STEP,
        "step_lum": STEP_LUM,
    }


def namespaced(tag_id):
    """A tag id with a namespace guaranteed: bare `mytag` -> `user:mytag`.

    A colon is the whole test. A closed list of known namespaces would have to
    grow every time the routing config names a new source taxonomy, and an
    unlisted one would be buried under `user:` instead -- which is the bug that
    put `runtime-packer:pe:upx` on the user axis. `migrate_tag` splits the same
    way, so the two cannot disagree about what is already namespaced.
    """
    raw = str(tag_id).strip()
    return raw if ":" in raw else f"user:{raw}"


def origin_tag(kind, name, version=None, func=None, detector="fid"):
    """Build `<namespace>:<name>[:<version>][#<func>]`.

    The namespace names *who said so* -- `fid:libc:2.31#memcpy`. A function's
    tags are `{tag_id: weight}`, so the id is the only per-function field there
    is: with the detector buried in a shared `origin:` namespace there would be
    no way to see where Function ID and BSim disagree, and no way to give two
    libraries two hues, because the first level would be the kind rather than
    the library.

    A missing version is a shorter id, not a placeholder segment: depth is not
    fixed any more, so `unknown` bought nothing and read as a claim.

    `kind` stays in the signature because callers speak in kinds; a bundle is
    the malware itself rather than a detector's finding, so it routes to
    `malware:`.
    """
    ns = ORIGIN_KIND_NAMESPACE.get(kind, detector)
    parts = [ns, name] + ([version] if version else [])
    tag = ":".join(str(p) for p in parts)
    return canonical_tag_id(f"{tag}{TAG_DETAIL}{func}" if func else tag)


def origin_parent(tag_id):
    """File-level origin implied by a function's origin tag, or None.

    `fid:uclibc:0.9.30.1#xdrmem_getint32` -> `fid:uclibc`: if a function is a
    known uClibc routine, the binary contains uClibc. The version is
    deliberately dropped -- one Function ID hit dates a single function, not the
    library the file was linked against, and a per-function version on the file
    document reads as a claim about the whole binary that nothing here
    established. The version stays on the function tag, where the evidence is.

    Legacy `origin:<kind>:<name>:...` ids answer with their own shape for as
    long as any survive the migration.
    """
    if not tag_id:
        return None
    parts = tag_body(tag_id)[0].split(":")
    if parts[0] == "origin":
        if len(parts) < 3 or not parts[2]:
            return None
        return f"origin:{parts[1]}:{parts[2]}"
    if parts[0] not in ORIGIN_NAMESPACES or len(parts) < 2 or not parts[1]:
        return None
    return f"{parts[0]}:{parts[1]}"


def severity_tag(level):
    return f"severity:{level}"


def category_tag(group, leaf):
    return f"category:{group}:{leaf}"


def capa_tag(rule_namespace):
    """capa rule namespace -> `capa:` tag id, or None when the rule has none.

    `host-interaction/file-system/write` -> `capa:host-interaction:file-system:write`.
    capa's own separator is `/`, but every axis in this vocabulary rolls up by
    splitting on `:`, so the path is re-punctuated on the way in rather than
    teaching the split engine a second separator.

    Rules with no namespace are capa's building blocks (`lib: true` and the
    nursery), not capabilities; they return None and are dropped by the caller.
    """
    ns = str(rule_namespace or "").strip().strip("/")
    if not ns:
        return None
    return "capa:" + ":".join(p for p in ns.split("/") if p)


def capa_meta_tags(meta):
    """capa rule meta -> the `mitre:`/`mbc:` tags the rule itself declares.

    capa rules carry their own ATT&CK and MBC mappings (`meta.attack`,
    `meta.mbc`), each entry a dict with the id already split out
    (`{"tactic": "Discovery", "technique": ..., "id": "T1082"}`). Those are the
    same externally standardised ids `mitre:`/`mbc:` already reserve, so the
    mapping is inherited rather than re-derived: capa is the producer this
    vocabulary was missing.

    `mitre:` stays flat (`mitre:t1082`) to match the rulezet producer. MBC has
    no competing producer, so it keeps the reserved two-level shape,
    `mbc:<objective>:<behavior>` -> `mbc:operating-system:environment-variable`.
    """
    out = set()
    for entry in (meta or {}).get("attack") or ():
        tid = (entry or {}).get("id") if isinstance(entry, dict) else None
        if not tid:
            m = _MITRE_ID.search(str(entry or ""))
            tid = m.group(0) if m else None
        if tid:
            out.add(f"{MITRE_NAMESPACE}:{str(tid).strip().lower()}")
    for entry in (meta or {}).get("mbc") or ():
        if not isinstance(entry, dict):
            continue
        parts = list(entry.get("parts") or ())
        objective = tag_slug(entry.get("objective") or (parts[0] if parts else ""))
        behavior = tag_slug(
            entry.get("behavior") or (parts[1] if len(parts) > 1 else "")
        )
        if objective and behavior:
            out.add(f"{MBC_NAMESPACE}:{objective}:{behavior}")
    return out


def capa_rule_hits(cdata):
    """capa `-j` document -> `(base_address, {virtual_address: {tag, ...}})`.

    Each entry of `rules[name]["matches"]` is a two-element `[address, result]`
    pair, *not* a dict -- reading it as one finds no matches at all. Only the
    address half is used: the result tree underneath says why the rule fired,
    which is not something this vocabulary records.

    The addresses are capa's own, relative to `meta.analysis.base_address`. capa
    and Ghidra pick different load bases for the same PIE image, so a caller must
    rebase them onto the disassembler's base before they name anything.
    """
    base = cdata.get("meta", {}).get("analysis", {}).get("base_address")
    base = base.get("value", 0) if isinstance(base, dict) else 0

    hits = {}
    for rule in cdata.get("rules", {}).values():
        meta = rule.get("meta", {}) or {}
        ctag = capa_tag(meta.get("namespace"))
        if not ctag:
            continue
        # The rule's own ATT&CK/MBC mappings ride along with its capa tag, on
        # the same addresses: they are claims about the same match.
        ctags = {ctag} | capa_meta_tags(meta)
        for match in rule.get("matches", []):
            if not isinstance(match, (list, tuple)) or not match:
                continue
            addr = match[0]
            # "absolute" is the only kind a static run emits. File-scope rules
            # carry "no address" and have no function to hang a tag on.
            if not isinstance(addr, dict) or addr.get("type") != "absolute":
                continue
            if "value" in addr:
                hits.setdefault(addr["value"], set()).update(ctags)
    return base, hits


def yara_tag(category, family, rule_name, namespace=YARA_NAMESPACE):
    """A matched YARA rule -> `yara:<category>:<family>#<rule_name>` tag id.

    A YARA rule carries no built-in namespace the way a capa rule does, so the
    id is assembled from the rule's own `meta.category` and `meta.malware`/
    `meta.family` fields (e.g. `category: "ransomware"`, `malware: "LOCKBIT"`,
    or a yarahub-style rule that names the family `family: "Torii"` instead of
    `malware:`). Either can be missing on a rule that carries none of these,
    and `unknown` says so.

    The rule name is the detail tail, not a level: one rule fires on one
    function and there are hundreds of thousands of them, so as a level it would
    mint an index bucket per rule and offer the sankey a column nobody can read.
    As a tail it stays searchable and displayed while grouping happens at the
    family above it.

    `namespace` picks the ruleset: `yara` for the vendored one, `rulezet` for a
    mirrored rule. Which ruleset fired is per-function evidence, and the id is
    the only per-function field there is.
    """
    cat = str(category or "unknown").strip().lower() or "unknown"
    fam = str(family or "unknown").strip().lower() or "unknown"
    return canonical_tag_id(f"{namespace}:{cat}:{fam}{TAG_DETAIL}{rule_name}")


def yara_rule_hits(matches, extra=None):
    """yara-python `Rules.match()` result -> `{file_offset: {tag, ...}}`.

    `extra` is the mirrored ruleset's tag sidecar, `{rule uuid: [tag, ...]}`;
    those tags land on exactly the offsets the rule's own tag does.

    A capa rule match names one address; a YARA rule match names every string
    it matched on, each with its own file offset, and the RL ruleset's
    `all of ($x_*)` conditions routinely span several unrelated functions (a
    resource-enumeration string here, a file-encryption string there) under one
    rule. So every instance of every matched string gets the rule's tag --
    dropping to one address per rule would silently keep only the smaller
    function.

    Offsets are raw file offsets, not addresses: a YARA scan reads the file on
    disk, not a loaded image. Turning a file offset into a Ghidra address needs
    Ghidra's own section layout (`Memory.locateAddressesForFileOffset`), so
    that step is the caller's job, not this one's -- this module stays free of
    any Ghidra import so its demo can run without a JVM.
    """
    hits = {}
    for match in matches:
        tags = _match_tags(match, extra)
        for string_match in getattr(match, "strings", None) or []:
            for instance in getattr(string_match, "instances", None) or []:
                hits.setdefault(instance.offset, set()).update(tags)
    return hits


def _match_tags(match, extra=None):
    """Every tag one match carries: its own `yara:` id, plus any sidecar tags.

    Mirrored rules are compiled with their uuid as the YARA namespace, so
    `match.namespace` is the key into the sidecar. Vendored rules use a file
    index there and simply miss, which is what should happen -- they have no
    sidecar entry.
    """
    import uuid

    tags = {_match_tag(match)}
    ns = getattr(match, "namespace", None)
    if extra:
        tags.update(extra.get(ns) or ())

    if ns:
        try:
            uuid.UUID(str(ns))
            # The uuid *is* the tag -- the rule's name is already in the
            # `yara:` tag above, so `rulezet:` only needs to say which rule.
            tags.add(f"rulezet:{ns}")
        except ValueError:
            pass
    return tags


def _match_tag(match):
    meta = getattr(match, "meta", None) or {}
    # `malware` is the vendored ruleset's convention; yarahub-mirrored rules
    # commonly use `family` instead (e.g. `family: "Torii"` with no `malware`
    # field at all) -- fall back to it rather than dropping to unknown.
    category = meta.get("category")
    family = meta.get("malware") or meta.get("family")
    if not category and not family:
        # Elastic's ruleset carries neither field, but its `threat_name` is
        # already `<os>.<category>.<family>` ("Linux.Trojan.Mirai"), so the two
        # segments it needs are read off that instead. Reading the field beats
        # rewriting 273 vendored files, which would have to be re-done by hand
        # on every re-vendor.
        parts = str(meta.get("threat_name") or "").split(".")
        if len(parts) == 3:
            category, family = parts[1], parts[2]
    return yara_tag(category, family, match.rule)


# `tag_provenance` maps a match back to the rule that minted its tag, and has to
# agree with this module on which tag that is -- re-deriving the category/family
# fallback there would be a second source of truth for tag identity.
tag_for_match = _match_tag


def yara_file_tags(matches, extra=None):
    """yara-python `Rules.match()` result -> `{tag, ...}` for the whole file.

    `yara_rule_hits()` keys on string instances, and every one of those can
    still be lost downstream: an offset Ghidra never mapped, or one that maps
    into no function and no referencing function either. A condition-only rule
    (`uint16(0) == 0x5a4d and filesize < 100KB`) has no string instances at
    all, so it never appears there in the first place.

    In all of those cases the *file* matched the rule, which is what this
    returns. Function-level attribution is a refinement on top of this, never
    the record of the match itself -- so a rule that resolves to no function
    is still visible on the file rather than silently dropped.
    """
    return {t for m in matches for t in _match_tags(m, extra)}


# --- Routing MISP-style source tags into this vocabulary --------------------
# Rulezet tags rules the way MISP does: `misp-galaxy:tool="Cobalt Strike"`,
# `tlp:clear`. Those ids are not this vocabulary's, and there are far too many of
# them to enumerate by hand, so the config routes them by *namespace* -- name a
# source namespace, say which BSimVis namespace it lands in, and every value
# under it follows. `drop` globs remove whole families (`tlp:*`) or single values
# (`tlp:white`) without needing an entry per tag.

# Used when the config says nothing. Keeps everything (unrouted namespaces land
# under `rulezet:`) and drops only the distribution markers, which say who may
# read a rule, not anything about the code it matches.
DEFAULT_TAG_MAP = {
    "misp-galaxy:mitre-attack-pattern": MITRE_NAMESPACE,
    "misp-galaxy:*": MISP_NAMESPACE,
    "cve": "cve",
    "ghsa": "ghsa",
    "pysec": "pysec",
    "*": "",
}
DEFAULT_DROPS = ("tlp:*", "pap:*")

# ATT&CK ids arrive welded onto the cluster name MISP uses:
# `Obfuscated Files or Information - T1027`. The id is the identity; the prose
# is not, and the tactic is not in the string at all (it is many-to-one per
# technique), so the tag stays flat at `mitre:t1027`.
_MITRE_ID = re.compile(r"\bT\d{4}(?:\.\d{3})?\b")


def tag_slug(text):
    """Free text -> a tag segment: `Cobalt Strike` -> `cobalt-strike`."""
    return re.sub(r"[^a-z0-9]+", "-", str(text).lower()).strip("-")


def _split_source_tag(raw):
    """`ns:pred="value"` or `ns:value` -> `(namespace, value)`."""
    raw = str(raw).strip()
    m = re.match(r'^([^:=]+):([^:=]+)=\s*"?(.*?)"?$', raw)
    if m:
        return f"{m.group(1).strip()}:{m.group(2).strip()}", m.group(3).strip()
    ns, _, value = raw.partition(":")
    return ns.strip(), value.strip()


def route_source_tag(raw, tag_map=None, drops=None):
    """A MISP-style source tag -> a BSimVis tag id, or None when dropped.

    The mapped target replaces the part of the source namespace the config
    matched; anything more specific is kept, so one `misp-galaxy:*` entry still
    tells `tool` apart from `ransomware`:

        misp-galaxy:tool="Cobalt Strike"  + {"misp-galaxy:*": "misp"}
            -> misp:tool:cobalt-strike
        misp-galaxy:mitre-attack-pattern="Obfuscated ... - T1027"
            -> mitre:t1027
    """
    raw = str(raw or "").strip()
    if not raw:
        return None
    tag_map = tag_map or DEFAULT_TAG_MAP
    drops = DEFAULT_DROPS if drops is None else drops

    low = raw.lower()
    if any(fnmatch.fnmatch(low, str(d).lower()) for d in drops):
        return None

    namespace, value = _split_source_tag(raw)
    if not value:
        return None

    # Longest matching key wins, so a specific galaxy beats `misp-galaxy:*`
    # beats `*`, whatever order the config happens to list them in.
    key = max(
        (k for k in tag_map if fnmatch.fnmatch(namespace.lower(), str(k).lower())),
        key=len,
        default=None,
    )
    if key is None:
        return None
    target = tag_map[key]
    if (
        target is False or target is None
    ):  # `"tlp" = false` reads as "drop this namespace"
        return None

    literal = str(key).split("*", 1)[0].rstrip(":")
    rest = namespace[len(literal) :].strip(":") if literal else namespace

    if target and str(target).split(":")[0] == MITRE_NAMESPACE:
        found = _MITRE_ID.search(value)
        return f"{MITRE_NAMESPACE}:{found.group(0).lower()}" if found else None

    parts = [str(target)] if target else []
    parts.extend([tag_slug(p) for p in rest.split(":") if p])
    parts.append(tag_slug(value))
    return namespaced(":".join(p for p in parts if p))


SEVERITY_TAGS = tuple(severity_tag(s) for s in SEVERITY_LEVELS)
CATEGORY_TAGS = tuple(
    category_tag(g, leaf) for g, leaves in CATEGORIES.items() for leaf in leaves
)
CATEGORY_GROUPS = tuple(CATEGORIES)


def is_taxonomy_tag(tag_id):
    """Is this one of the fixed analysis tags the LLM is allowed to invent?

    Deliberately excludes `origin:` (an analyzer's finding, not the model's) and
    `user:` (a human's), so a hallucinated library attribution cannot enter
    through the summarisation path.
    """
    t = str(tag_id).strip().lower()
    return t in SEVERITY_TAGS or t in CATEGORY_TAGS


def prompt_rules():
    """The tag half of the summarisation prompt.

    Built from the tables above rather than written out, so a leaf added to
    `CATEGORIES` reaches the model without a second edit.
    """
    groups = "; ".join(f"{g}: {', '.join(leaves)}" for g, leaves in CATEGORIES.items())
    return (
        "Then, on a final line starting with 'TAGS:', tag the function. "
        "Emit exactly one severity tag, and at most 2 category tags.\n"
        f"Severity -- format `severity:<level>`, <level> MUST be one of: "
        f"{', '.join(SEVERITY_LEVELS)}.\n"
        "Category -- format `category:<group>:<leaf>`, where <group>:<leaf> MUST "
        f"be one of: {groups}."
    )


# --- Migration off the old vocabulary ---------------------------------------
# The old id welded both questions into one tag: `flag:<risk>:<capability>`
# (and `llm:<risk>:<capability>` before that). One old tag therefore produces
# two new ones, which is the entire point of the split.
LEGACY_PREFIXES = ("flag", "llm")

LEGACY_RISK = {
    "benign": "none",
    "suspicious": "medium",
    "malicious": "high",
}

LEGACY_CAPABILITY = {
    "init": ("util", "init"),
    "string": ("util", "string"),
    "memory": ("util", "memory"),
    "math": ("util", "math"),
    "parser": ("util", "parser"),
    "compression": ("util", "compression"),
    "file_io": ("file", "read_write"),
    "crypto": ("crypto", "cipher"),
    "encoding": ("crypto", "encoding"),
    "anti_debug": ("evasion", "anti_debug"),
    "anti_vm": ("evasion", "anti_vm"),
    "obfuscation": ("evasion", "obfuscation"),
    "packer": ("evasion", "packer"),
    "c2": ("network", "c2"),
    "download": ("network", "download"),
    "network_io": ("network", "socket"),
    "p2p": ("network", "p2p"),
    "persistence": ("persistence", "autostart"),
    "registry": ("persistence", "registry"),
    "privesc": ("process", "privesc"),
    "injection": ("process", "inject"),
    "shell": ("process", "shell"),
    "ransomware": ("impact", "ransom"),
    "ddos": ("impact", "ddos"),
    "exfil": ("impact", "exfil"),
    "destruction": ("impact", "wipe"),
    "spyware": ("impact", "spyware"),
}


def modernize_tag_id(tag_id):
    """A tag id in the old shape -> the same fact in the current one.

    Separate from `migrate_tag`, which maps the *previous* vocabulary
    (`flag:`/`llm:`) onto this one. This handles the later move: the detector out
    of segment 2 and into the namespace, and per-function evidence out of a
    level and into the detail tail.

        origin:lib:libc:2.31:memcpy      -> fid:libc:2.31#memcpy
        origin:stdlib:libstdc++:11       -> fid:libstdc++:11
        origin:bundle:mirai:unknown      -> malware:mirai
        yara:trojan:mirai:ELF_Mirai      -> yara:trojan:mirai#ELF_Mirai
        rulezet:ELF_Mirai                -> rulezet:unknown#ELF_Mirai
        cve:cve-2021-44228               -> cve:2021-44228

    An id already in the current shape is returned unchanged, so the migration
    is safe to run twice -- which it will be, because a corpus is re-tagged in
    pieces and nobody tracks which pieces.

    `rulezet:` loses nothing it had: the mirrored rule's category and family
    were never in that id. They arrive on the `yara:` tag the same rule writes
    when it fires, so the migrated id says `unknown` rather than inventing one.
    """
    # Deliberately not canonicalised first: a rule name is a *level* in the old
    # id and a tail in the new one, and levels fold case. Normalising before the
    # move would lowercase `ELF_Mirai` on its way to becoming a symbol.
    raw = " ".join(str(tag_id or "").split())
    if not raw:
        return ""
    body, detail = tag_body(raw)
    parts = [p for p in body.split(":") if p]
    if not parts:
        return ""
    head = parts[0].lower()

    if head == "origin" and len(parts) >= 3:
        kind, name, rest = parts[1], parts[2], parts[3:]
        ns = ORIGIN_KIND_NAMESPACE.get(kind, "fid")
        version = rest[0] if rest and rest[0] != ORIGIN_NO_VERSION else None
        func = detail or (rest[1] if len(rest) > 1 else None)
        levels = [ns, name] + ([version] if version else [])
        out = ":".join(levels)
        return canonical_tag_id(f"{out}{TAG_DETAIL}{func}" if func else out)

    # `yara:<category>:<family>:<rule>` -- the rule name becomes the tail.
    if head in (YARA_NAMESPACE, "rulezet") and not detail:
        if head == "rulezet" and len(parts) == 2:
            return canonical_tag_id(f"rulezet:unknown{TAG_DETAIL}{parts[1]}")
        if len(parts) >= 4:
            return canonical_tag_id(
                ":".join(parts[:3]) + TAG_DETAIL + ":".join(parts[3:])
            )

    # `cve:cve-2021-44228` -- the namespace already says which registry.
    if (
        head in VULN_NAMESPACES
        and len(parts) == 2
        and parts[1].lower().startswith(head + "-")
    ):
        return canonical_tag_id(f"{head}:{parts[1][len(head) + 1:]}")

    return canonical_tag_id(raw)


def migrate_tag(tag_id):
    """Old tag id -> the list of new ids replacing it.

    Returns `[]` for a tag that should be dropped, and the id unchanged for one
    that is already in the new space or is deliberately left alone (the
    file-scope `container:`/`packer:` namespaces).

    A legacy `flag:`/`llm:` tag yields up to two ids: its severity and its
    category. Anything unrecognised is treated as human-typed and moved to
    `user:`, because an unnamespaced tag must not keep silently landing on an
    analysis axis.
    """
    raw = str(tag_id).strip()
    if not raw:
        return []
    parts = raw.split(":")
    head = parts[0].lower()

    if head in (
        "origin",
        "severity",
        "category",
        "user",
        CAPA_NAMESPACE,
        MITRE_NAMESPACE,
        MBC_NAMESPACE,
        YARA_NAMESPACE,
    ):
        # `user:flag:...` is a legacy id `namespaced()` buried under `user:`
        # after the split, not a human's word. Unbury it and migrate for real.
        if head == "user" and len(parts) > 1 and parts[1].lower() in LEGACY_PREFIXES:
            return migrate_tag(":".join(parts[1:]))
        return [raw]
    if head in FILE_SCOPE_NAMESPACES:
        return [raw]

    if head in LEGACY_PREFIXES:
        out = []
        # `flag:crypto` (no risk segment) was also produced, by the batch writer
        # prepending the namespace to a bare vocabulary tag.
        rest = [p.lower() for p in parts[1:]]
        # `flag:llm:benign:init` -- the batch writer prefixed `flag:` onto an id
        # llm_service had already namespaced `llm:`. Both layers come off, else
        # the risk segment sits behind `llm` and the severity is lost.
        while rest and rest[0] in LEGACY_PREFIXES:
            rest = rest[1:]
        if rest and rest[0] in LEGACY_RISK:
            out.append(severity_tag(LEGACY_RISK[rest[0]]))
            rest = rest[1:]
        for cap in rest:
            if cap in LEGACY_CAPABILITY:
                out.append(category_tag(*LEGACY_CAPABILITY[cap]))
        return out or [f"user:{raw}"]

    if head in ORIGIN_KINDS:
        return [migrate_origin(raw)]

    # `rulezet:` used to be the catch-all for every source namespace the routing
    # config did not name, so the taxonomy it swallowed is still in the id:
    # `rulezet:ms-caro-malware-full:malware-platform:linux` -> drop the prefix
    # and the source namespace stands on its own. A two-segment id is either a
    # mirrored rule's uuid (`rulezet:031cfb94-...`, current) or, from before
    # `_match_tags` switched to writing the uuid, a mirrored rule *name*
    # (`rulezet:ELF_Toriilike_persist`, legacy) -- both are already in the
    # shape `rulezet:` means today, so both pass through unchanged.
    if head == "rulezet" and len(parts) > 2:
        return [":".join(parts[1:])]

    # Any other namespaced id is a source taxonomy `route_source_tag` kept as
    # its own (`runtime-packer:pe:upx`, `misp:tool:cobalt-strike`). Same test as
    # `namespaced()`: one colon means someone already namespaced this, and
    # burying it under `user:` is how it ends up on the wrong axis.
    if len(parts) > 1:
        return [raw]

    return [f"user:{raw}"]


def migrate_origin(tag_id):
    """`lib:libc:2.31:memcpy` -> `origin:lib:libc:2.31:memcpy`.

    A version segment is inserted when the old id had none, so every origin id
    is `origin:kind:name:version[:func]` and rolls up at one fixed depth. The
    old `lib:` ids are `kind:name[:version[:func]]`; `bundle:` ids in practice
    never carried a version, so anything after the name is a function.
    """
    parts = str(tag_id).split(":")
    kind = parts[0].lower()
    name = parts[1] if len(parts) > 1 else ""
    tail = parts[2:]

    if kind == "bundle":
        version, func = ORIGIN_NO_VERSION, tail
    else:
        version = tail[0] if tail else ORIGIN_NO_VERSION
        func = tail[1:]
    return ":".join(["origin", kind, name, version, *func])


def demo():
    assert migrate_tag("flag:suspicious:crypto") == [
        "severity:medium",
        "category:crypto:cipher",
    ], migrate_tag("flag:suspicious:crypto")
    assert migrate_tag("llm:malicious:injection") == [
        "severity:high",
        "category:process:inject",
    ]
    assert migrate_tag("flag:benign:init") == ["severity:none", "category:util:init"]
    # Bare vocabulary tag the batch writer had prefixed, with no risk segment.
    assert migrate_tag("flag:crypto") == ["category:crypto:cipher"]
    # Unknown capability behind a known prefix must not vanish silently.
    assert migrate_tag("flag:suspicious:invented") == ["severity:medium"]
    assert migrate_tag("flag:nonsense") == ["user:flag:nonsense"]
    # Double-prefixed by the old writer disagreement.
    assert migrate_tag("flag:llm:benign:init") == [
        "severity:none",
        "category:util:init",
    ]
    # ... and the same id after `namespaced()` buried it under `user:`.
    assert migrate_tag("user:flag:llm:benign:init") == [
        "severity:none",
        "category:util:init",
    ]
    assert migrate_tag("user:flag:nonsense") == ["user:flag:nonsense"]

    assert migrate_tag("lib:libc:2.31:memcpy") == ["origin:lib:libc:2.31:memcpy"]
    assert migrate_tag("lib:uclibc") == ["origin:lib:uclibc:unknown"]
    assert migrate_tag("bundle:mirai") == ["origin:bundle:mirai:unknown"]
    assert migrate_tag("bundle:mirai:scanner") == [
        "origin:bundle:mirai:unknown:scanner"
    ]
    assert migrate_tag("stdlib:musl:1.2.4") == ["origin:stdlib:musl:1.2.4"]

    # File-scope namespaces are left exactly as they are.
    assert migrate_tag("container:apk") == ["container:apk"]
    assert migrate_tag("packer:upx") == ["packer:upx"]
    # Already migrated -> idempotent, so a re-run is safe.
    for t in ("origin:lib:libc:2.31", "severity:high", "category:network:c2", "user:x"):
        assert migrate_tag(t) == [t], t
    # Human-typed, no namespace.
    assert migrate_tag("mirai") == ["user:mirai"]
    # The old `rulezet:` catch-all comes off, and the source taxonomy it hid
    # becomes the namespace -- the tag id `route_source_tag` writes today.
    assert migrate_tag("rulezet:ms-caro-malware-full:malware-platform:linux") == [
        "ms-caro-malware-full:malware-platform:linux"
    ]
    assert migrate_tag("rulezet:runtime-packer:pe:upx") == ["runtime-packer:pe:upx"]
    # A two-segment `rulezet:` id is a mirrored rule name, which is current.
    assert migrate_tag("rulezet:ELF_Toriilike_persist") == [
        "rulezet:ELF_Toriilike_persist"
    ]
    # Namespaces this function never listed must not be buried under `user:`:
    # that is how `misp:tool:cobalt-strike` would have left the family axis on
    # the first migration run.
    for t in (
        "misp:tool:cobalt-strike",
        "ms-caro-malware-full:malware-platform:linux",
        "runtime-packer:pe:upx",
        "cve:cve-2021-44228",
    ):
        assert migrate_tag(t) == [t], t

    assert is_taxonomy_tag("severity:high")
    assert is_taxonomy_tag("category:network:c2")
    assert not is_taxonomy_tag("category:network:invented")
    assert not is_taxonomy_tag("origin:lib:libc:2.31"), "model must not invent origins"

    # The namespace names the detector, the first level names the library, and
    # the function is a detail tail rather than a fifth level.
    assert origin_tag("lib", "libc", "2.31", "memcpy") == "fid:libc:2.31#memcpy"
    assert origin_tag("lib", "libc") == "fid:libc"
    assert origin_tag("bundle", "mirai", None, "scanner") == "malware:mirai#scanner"
    assert (
        origin_tag("lib", "openssl", "3.0.2", "EVP_EncryptInit", detector="bsim")
        == "bsim:openssl:3.0.2#EVP_EncryptInit"
    ), "a second detector is a second namespace, so the two can be compared"
    # A symbol keeps its case; a level does not.
    assert origin_tag("lib", "Visual Studio", "2019", "atexit") == (
        "fid:visual-studio:2019#atexit"
    )

    # A tag the analyzer builds must roll up the way the split engine expects.
    assert origin_parent("fid:uclibc:0.9.30.1#xdrmem_getint32") == "fid:uclibc"
    assert origin_parent("malware:mirai") == "malware:mirai"
    assert origin_parent("severity:high") is None
    assert origin_parent("") is None
    # Legacy ids keep answering until the migration has run.
    assert (
        origin_parent("origin:lib:uclibc:0.9.30.1:xdrmem_getint32")
        == "origin:lib:uclibc"
    )
    assert origin_parent("origin:bundle:mirai:unknown") == "origin:bundle:mirai"

    # `canonical_tag_id` normalises only what the query and index layers cannot
    # carry, and leaves a source's own punctuation as data.
    assert canonical_tag_id("  Origin:lib:Visual Studio:2019 ") == (
        "origin:lib:visual-studio:2019"
    )
    assert canonical_tag_id('misp-galaxy:tool="Cobalt Strike"') == (
        "misp-galaxy:tool=cobalt-strike"
    )
    assert canonical_tag_id("a::b") == "a:b", "an empty level is not a level"
    assert canonical_tag_id("mirai") == "user:mirai"
    assert canonical_tag_id("mitre:T1027.005") == "mitre:t1027.005", "a dot is data"
    assert canonical_tag_id("fid:openssl:3.0.2#EVP_EncryptInit") == (
        "fid:openssl:3.0.2#EVP_EncryptInit"
    ), "a symbol keeps its case"
    assert canonical_tag_id("   ") == ""

    # The move to detector namespaces and detail tails, and it must be safe to
    # run twice -- a corpus gets migrated in pieces and nobody tracks which.
    assert modernize_tag_id("origin:lib:libc:2.31:memcpy") == "fid:libc:2.31#memcpy"
    assert modernize_tag_id("origin:stdlib:libstdc++:11") == "fid:libstdc++:11"
    assert modernize_tag_id("origin:bundle:mirai:unknown") == "malware:mirai"
    assert modernize_tag_id("yara:trojan:mirai:ELF_Mirai") == (
        "yara:trojan:mirai#ELF_Mirai"
    ), "a rule name is a symbol on its way out of being a level"
    assert modernize_tag_id("rulezet:ELF_Mirai") == "rulezet:unknown#ELF_Mirai"
    assert modernize_tag_id("cve:cve-2021-44228") == "cve:2021-44228"
    assert modernize_tag_id("category:network:c2") == "category:network:c2"
    for already in ("fid:libc:2.31#memcpy", "yara:trojan:mirai#ELF_Mirai"):
        assert modernize_tag_id(already) == already, already

    # A scope names levels, so it is tested against levels. Selecting a library
    # version came back empty because `startswith(prefix + ":")` reads the
    # detail marker as being outside the version it hangs off.
    assert tag_in_scope("fid:uclibc:0.9.30.1#xdrmem_getint32", "fid:uclibc:0.9.30.1")
    assert tag_in_scope("fid:uclibc:0.9.30.1#xdrmem_getint32", "fid:uclibc")
    assert tag_in_scope("fid:uclibc:0.9.30.1", "fid:uclibc:0.9.30.1")
    assert tag_in_scope("origin:lib:uclibc:0.9.30.1:x", "origin:lib:uclibc")
    # A sibling whose name merely starts with the same text is not in scope.
    assert not tag_in_scope("fid:uclibcplus:1.0", "fid:uclibc")
    assert not tag_in_scope("fid:musl:1.2#x", "fid:uclibc")
    assert not tag_in_scope("", "fid:uclibc")

    assert namespaced("mytag") == "user:mytag"
    assert namespaced("category:network:c2") == "category:network:c2"

    # capa ids are recorded verbatim, never remapped into `category:`.
    assert capa_tag("host-interaction/file-system/write") == (
        "capa:host-interaction:file-system:write"
    )
    assert capa_tag("communication/http/client") == "capa:communication:http:client"
    assert capa_tag(None) is None and capa_tag("") is None and capa_tag("/") is None
    assert (
        namespaced("capa:communication:http") == "capa:communication:http"
    ), "a capa tag must not be buried under user:"
    assert migrate_tag("capa:communication:http") == ["capa:communication:http"]
    assert not is_taxonomy_tag(
        "capa:communication:http"
    ), "the model must not be able to invent capa findings"

    # A capa document, shaped exactly as `capa -j` writes it: `matches` holds
    # [address, result] pairs. Reading it as a list of dicts is what silently
    # produced zero capa tags, so this is the assert that has to fail if the
    # pair-unpacking is ever "simplified" back.
    doc = {
        "meta": {
            "analysis": {"base_address": {"type": "absolute", "value": 0x2000000}}
        },
        "rules": {
            "encrypt data using RC4": {
                "meta": {"namespace": "data-manipulation/encryption/rc4"},
                "matches": [
                    [{"type": "absolute", "value": 0x2002715}, {"success": True}],
                    [{"type": "absolute", "value": 0x2002715}, {"success": True}],
                    [{"type": "absolute", "value": 0x2014000}, {"success": True}],
                ],
            },
            # No namespace -> a capa building block, never a capability tag.
            "create or open file": {
                "meta": {},
                "matches": [
                    [{"type": "absolute", "value": 0x2003000}, {"success": True}]
                ],
            },
            # File-scope rules have nowhere to land.
            "packed with UPX": {
                "meta": {"namespace": "anti-analysis/packer/upx"},
                "matches": [[{"type": "no address"}, {"success": True}]],
            },
        },
    }
    base, hits = capa_rule_hits(doc)
    assert base == 0x2000000, base
    assert hits == {
        0x2002715: {"capa:data-manipulation:encryption:rc4"},
        0x2014000: {"capa:data-manipulation:encryption:rc4"},
    }, hits
    assert capa_rule_hits({}) == (0, {})

    # capa's own ATT&CK/MBC mappings become tags, on the rule's own addresses.
    cmeta = {
        "namespace": "host-interaction/environment-variable",
        "attack": [
            {
                "parts": ["Discovery", "System Information Discovery"],
                "id": "T1082",
            }
        ],
        "mbc": [
            {"objective": "Operating System", "behavior": "Environment Variable"}
        ],
    }
    assert capa_meta_tags(cmeta) == {
        "mitre:t1082",
        "mbc:operating-system:environment-variable",
    }, capa_meta_tags(cmeta)
    assert capa_meta_tags({}) == set()
    # An entry that is a bare string (capa's YAML form) still yields the id.
    assert capa_meta_tags(
        {"attack": ["Discovery::System Information Discovery [T1082]"]}
    ) == {"mitre:t1082"}
    _, mhits = capa_rule_hits(
        {
            "meta": {"analysis": {"base_address": {"value": 0}}},
            "rules": {
                "get COMSPEC": {
                    "meta": cmeta,
                    "matches": [[{"type": "absolute", "value": 0x401880}, {}]],
                }
            },
        }
    )
    assert mhits[0x401880] == {
        "capa:host-interaction:environment-variable",
        "mitre:t1082",
        "mbc:operating-system:environment-variable",
    }, mhits

    assert yara_tag("Ransomware", "LOCKBIT", "Win32_Ransomware_LockBit") == (
        "yara:ransomware:lockbit#Win32_Ransomware_LockBit"
    )
    assert yara_tag(None, None, "no_meta_rule") == "yara:unknown:unknown#no_meta_rule"
    # yarahub-style meta: `family`, no `malware`, no `category`.
    import types

    assert (
        _match_tag(
            types.SimpleNamespace(
                rule="ELF_Toriilike_persist", meta={"family": "Torii"}
            )
        )
        == "yara:unknown:torii#ELF_Toriilike_persist"
    )
    assert (
        namespaced("yara:ransomware:lockbit#x") == "yara:ransomware:lockbit#x"
    ), "a yara tag must not be buried under user:"

    # Shaped like yara-python's own Match/StringMatch/StringMatchInstance, not a
    # dict -- `.strings[i].instances[j].offset` is the real access path, and
    # this is the assert that has to fail if that walk is ever "simplified"
    # into reading `match.strings` as flat offsets.
    class _Instance:
        def __init__(self, offset):
            self.offset = offset

    class _StringMatch:
        def __init__(self, offsets):
            self.instances = [_Instance(o) for o in offsets]

    class _Match:
        def __init__(self, rule, meta, offsets_by_string):
            self.rule = rule
            self.meta = meta
            self.strings = [_StringMatch(o) for o in offsets_by_string]

    matches = [
        _Match(
            "Win32_Ransomware_LockBit",
            {"category": "Ransomware", "malware": "LOCKBIT"},
            [[0x1000], [0x2000, 0x2500]],
        ),
        # No meta at all -- an older or hand-written rule -- still tags, at unknown depth.
        _Match("homebrew_rule", {}, [[0x3000]]),
        # Condition-only rule: matched the file, names no string offset at all.
        _Match("Win32_Packer_Themida", {"category": "Packer"}, []),
        # Elastic: no category/malware, but `threat_name` carries both.
        _Match(
            "Linux_Trojan_Mirai_268aac0b",
            {"threat_name": "Linux.Trojan.Mirai"},
            [[0x4000]],
        ),
        # A threat_name that is not <os>.<category>.<family> stays at unknown depth
        # rather than being sliced into the wrong two segments.
        _Match("odd_shape_rule", {"threat_name": "Linux.Trojan"}, [[0x5000]]),
    ]
    hits = yara_rule_hits(matches)
    assert hits == {
        0x1000: {"yara:ransomware:lockbit#Win32_Ransomware_LockBit"},
        0x2000: {"yara:ransomware:lockbit#Win32_Ransomware_LockBit"},
        0x2500: {"yara:ransomware:lockbit#Win32_Ransomware_LockBit"},
        0x3000: {"yara:unknown:unknown#homebrew_rule"},
        0x4000: {"yara:trojan:mirai#Linux_Trojan_Mirai_268aac0b"},
        0x5000: {"yara:unknown:unknown#odd_shape_rule"},
    }, hits
    assert yara_rule_hits([]) == {}

    # The file-level set is not a rollup of the offset map: the condition-only
    # rule is in it and is absent from every value in `hits`.
    file_tags = yara_file_tags(matches)
    assert file_tags == {
        "yara:ransomware:lockbit#Win32_Ransomware_LockBit",
        "yara:unknown:unknown#homebrew_rule",
        "yara:packer:unknown#Win32_Packer_Themida",
        "yara:trojan:mirai#Linux_Trojan_Mirai_268aac0b",
        "yara:unknown:unknown#odd_shape_rule",
    }, file_tags
    assert "yara:packer:unknown#Win32_Packer_Themida" not in set().union(*hits.values())
    assert yara_file_tags([]) == set()

    # The mirrored ruleset's tags ride in a sidecar keyed by the uuid YARA
    # reports as the match namespace, because rule *names* collide freely across
    # 130k rules from different repos. Vendored matches carry a file index there
    # and must miss the sidecar cleanly.
    class _NsMatch(_Match):
        def __init__(self, rule, meta, offsets, namespace):
            super().__init__(rule, meta, offsets)
            self.namespace = namespace

    uuid_str = "12345678-1234-5678-1234-567812345678"
    mirrored = [
        _NsMatch(
            "Some_Rule",
            {"category": "trojan", "malware": "mirai"},
            [[0x7000]],
            uuid_str,
        )
    ]
    sidecar = {uuid_str: ["mitre:t1027", "cve:cve-2021-44228"]}
    # `rulezet:` carries the uuid, not the rule name -- the name is already in
    # the `yara:` tag right next to it.
    assert yara_file_tags(mirrored, sidecar) == {
        "yara:trojan:mirai#Some_Rule",
        "mitre:t1027",
        "cve:cve-2021-44228",
        f"rulezet:{uuid_str}",
    }
    assert yara_rule_hits(mirrored, sidecar) == {
        0x7000: {
            "yara:trojan:mirai#Some_Rule",
            "mitre:t1027",
            "cve:cve-2021-44228",
            f"rulezet:{uuid_str}",
        }
    }
    # No sidecar entry, and matches with no namespace at all, still work.
    assert yara_file_tags(mirrored, {"other": ["x"]}) == {
        "yara:trojan:mirai#Some_Rule",
        f"rulezet:{uuid_str}",
    }
    assert yara_file_tags(matches[:1], sidecar) == {
        "yara:ransomware:lockbit#Win32_Ransomware_LockBit"
    }

    # --- Source tag routing -------------------------------------------------
    r = route_source_tag
    assert r('misp-galaxy:tool="Cobalt Strike"') == "misp:tool:cobalt-strike"
    assert r('misp-galaxy:ransomware="LockBit"') == "misp:ransomware:lockbit"
    # A specific galaxy beats the wildcard no matter the dict order.
    assert r('misp-galaxy:mitre-attack-pattern="Obfuscated Files - T1027"') == (
        "mitre:t1027"
    )
    assert r('misp-galaxy:mitre-attack-pattern="Indicator Removal - T1027.005"') == (
        "mitre:t1027.005"
    )
    # An attack-pattern cluster with no technique id is not an ATT&CK fact.
    assert r('misp-galaxy:mitre-attack-pattern="Some Prose"') is None
    # Unrouted namespaces are kept, not lost -- the whole source path survives.
    assert r('runtime-packer:pe="upx"') == "runtime-packer:pe:upx"
    # ms-caro is 97% of the sidecar, and its two predicates are the reason it
    # stays: `malware-type` and `malware-platform` are different questions, and
    # the family tree nests on that segment rather than flattening to the leaf.
    assert r('ms-caro-malware-full:malware-type="Trojan"') == (
        "ms-caro-malware-full:malware-type:trojan"
    )
    assert r('ms-caro-malware-full:malware-platform="Linux"') == (
        "ms-caro-malware-full:malware-platform:linux"
    )
    assert r("cve:CVE-2021-44228") == "cve:cve-2021-44228"
    assert r("ghsa:GHSA-j8v8-6h6r-m6pq") == "ghsa:ghsa-j8v8-6h6r-m6pq"
    # Distribution markers: the family and a single value.
    assert r("tlp:clear") is None and r("tlp:white") is None and r("pap:clear") is None
    assert r("tlp:red", drops=["tlp:white"]) == "tlp:red"
    # `false` as a target reads as "drop this namespace".
    assert r('misp-galaxy:tool="Netcat"', {"misp-galaxy:*": False}) is None
    # No catch-all configured -> an unrouted tag is dropped rather than guessed.
    assert r('runtime-packer:pe="upx"', {"cve": "cve"}) is None
    assert r("") is None and r(None) is None
    # A routed tag must never end up buried under `user:`.
    for src in ('misp-galaxy:tool="X"', "cve:CVE-2021-1", 'misp-galaxy:x="y"'):
        assert not route_source_tag(src).startswith("user:"), src
    assert tag_slug("Cobalt Strike 4.0!") == "cobalt-strike-4-0"

    # --- Colour -------------------------------------------------------------
    # Severity keeps the ordinal ramp rather than a hash, and one flat tone so
    # the four levels differ only in the way that carries the order.
    assert tag_style("severity:high")[0] == 0
    assert tag_style("severity:none")[0] == 120
    assert len({tag_style(f"severity:{lv}")[1] for lv in SEVERITY_LEVELS}) == 1
    # A family shares an arc: every lib sits closer to another lib than to the
    # stdlib arc, because `origin:lib` picks the interval they all subdivide.
    libs = [tag_style(f"origin:lib:{n}")[0] for n in ("libc", "openssl", "zlib")]
    other = tag_style("origin:stdlib:libstdc++")[0]
    assert max(libs) - min(libs) < min(abs(other - h) for h in libs), (libs, other)
    # ...and are still told apart inside it, by slot or by tone.
    assert len({tag_style(f"origin:lib:{n}")[:2] for n in ("libc", "openssl")}) == 2
    # Past HUE_DEPTH the hue stops moving and the shade steps: a leaf is a shade
    # of its group, and a version is a shade of its library.
    assert tag_style("category:network:c2")[0] == tag_style("category:network")[0]
    assert tag_style("category:network:c2")[2] == 1
    assert tag_style("origin:lib:libc:2.31:memcpy")[2] == 2
    assert tag_style("origin:lib:libc:2.31:memcpy:x:y:z")[2] == MAX_STEP
    # ATT&CK splits on the dot, so a sub-technique is a shade of its technique.
    assert tag_style("mitre:t1027.005")[0] == tag_style("mitre:t1027")[0]
    assert tag_style("mitre:t1027.005")[2] == 1
    assert tag_style("mitre:t1027")[0] != tag_style("mitre:t1059")[0]
    # A flat namespace has no family to group by: one colour per id, no shade.
    assert tag_style("cve:cve-2021-44228")[2] == 0
    assert tag_style("cve:cve-2021-44228")[:2] != tag_style("cve:cve-2021-1")[:2]
    # Same id, same colour, every run -- the whole point of deriving it.
    assert tag_style("capa:host-interaction:file-system:write") == tag_style(
        "capa:host-interaction:file-system:write"
    )
    # A bare tag is a user tag, and a bare namespace has nothing to hash.
    assert tag_style("bookmark") == tag_style("user:bookmark")
    assert tag_style("user:") == (None, 0, 0)
    # The vectors `scripts/test_tag_colors.js` asserts the JS mirror against.
    # Both sides must agree exactly or the same tag is two colours.
    assert [tag_style(t) for t in COLOR_VECTORS] == [
        (0, 1, 0),
        (55, 1, 0),
        (75.52, 0, 0),
        (75.52, 0, 2),
        (76.91, 0, 0),
        (122.73, 1, 2),
        (237.27, 0, 1),
        (99.82, 0, 0),
        (99.82, 1, 0),
        (99.82, 1, 1),
        (99.82, 1, 1),
        (54.0, 0, 0),
        (54.0, 0, 1),
    ], [tag_style(t) for t in COLOR_VECTORS]

    # The bug this rework exists to remove: two libraries must differ in hue,
    # not merely in lightness, and a library must keep one hue at every depth it
    # is displayed at -- as a card, as a tree node, as a version, as a symbol.
    libc = [tag_style(f"fid:libc{s}")[0] for s in ("", ":2.31", ":2.31#memcpy")]
    assert len(set(libc)) == 1, libc
    assert tag_style("fid:libc")[0] != tag_style("fid:openssl")[0]
    assert tag_style("fid:libc")[0] != tag_style("fid:visual-studio")[0]
    # A detail tail changes nothing about the colour: it is not a level.
    assert tag_style("fid:libc:2.31#memcpy") == tag_style("fid:libc:2.31#malloc")

    rules = prompt_rules()
    assert "severity:<level>" in rules and "key_exchange" in rules
    print("tag_taxonomy demo OK")


if __name__ == "__main__":
    demo()
