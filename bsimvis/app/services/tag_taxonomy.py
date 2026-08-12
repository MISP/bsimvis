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
a MISP-style source tag becomes one of them.
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
    "evasion": ("anti_debug", "anti_vm", "obfuscation", "packer", "rootkit", "log_clear"),
    "recon": ("sysinfo", "proclist", "filesearch", "creds", "env"),
    # `spyware` overlaps keylog/screencap on purpose: it is the fallback when the
    # model cannot tell which, and it makes the legacy `spyware` capability map
    # across without loss. Prefer the specific leaf when it is knowable.
    "impact": ("ddos", "ransom", "wipe", "exfil", "keylog", "screencap", "spyware"),
    "util": ("init", "string", "memory", "math", "parser", "compression", "wrapper"),
}

# --- Origin -----------------------------------------------------------------
ORIGIN_KINDS = ("lib", "stdlib", "bundle")

# Bundles have no natural version but carry this placeholder anyway, so origin
# ids are a uniform `origin:kind:name:version[:func]` and roll up at one depth.
# Mirrors bin_sim_tags.ORIGIN_NO_VERSION; kept here so the migration does not
# have to import the split engine.
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
# from the rule's own `category`/`malware` meta fields instead:
# `yara:<category>:<family>:<rule_name>`.
YARA_NAMESPACE = "yara"

# Written by `rulezet_service` from the MISP-style tags it produces for mirrored
# rules. `misp:` keeps a galaxy's own shape (`misp:tool:cobalt-strike`), and
# `rulezet:` is the catch-all for source namespaces the config did not route
# anywhere in particular -- so an unmapped tag is still recorded rather than
# silently lost.
MISP_NAMESPACE = "misp"
RULEZET_NAMESPACE = "rulezet"

# Vulnerability ids, flat: `cve:cve-2021-44228`, `ghsa:ghsa-j8v8-6h6r-m6pq`.
# Flat because an id is already the whole identity -- there is no second segment
# to roll up on.
VULN_NAMESPACES = ("cve", "ghsa", "pysec")

# The namespaces a tag id may lead with. Anything else is human-typed and gets
# moved under `user:` -- an unnamespaced tag must never land on an analysis axis.
KNOWN_NAMESPACES = (
    "origin",
    "severity",
    "category",
    "user",
    CAPA_NAMESPACE,
    MITRE_NAMESPACE,
    MBC_NAMESPACE,
    YARA_NAMESPACE,
    MISP_NAMESPACE,
    RULEZET_NAMESPACE,
) + VULN_NAMESPACES + FILE_SCOPE_NAMESPACES


def namespaced(tag_id):
    """A tag id with a namespace guaranteed: bare `mytag` -> `user:mytag`."""
    raw = str(tag_id).strip()
    head = raw.split(":", 1)[0].lower()
    return raw if head in KNOWN_NAMESPACES else f"user:{raw}"


def origin_tag(kind, name, version=None, func=None):
    """Build `origin:<kind>:<name>:<version>[:<func>]`.

    The version segment is always present -- `unknown` when the analyzer did not
    establish one -- so every origin id rolls up at one fixed depth instead of
    needing a per-kind rule.
    """
    parts = ["origin", kind, name, version or ORIGIN_NO_VERSION]
    if func:
        parts.append(func)
    return ":".join(parts)


def origin_parent(tag_id):
    """File-level origin implied by a function's origin tag, or None.

    `origin:lib:uclibc:0.9.30.1:xdrmem_getint32` -> `origin:lib:uclibc`: if a
    function is a known uClibc routine, the binary contains uClibc. The version
    is deliberately dropped -- one Function ID hit dates a single function, not
    the library the file was linked against, and a per-function version on the
    file document reads as a claim about the whole binary that nothing here
    established. The version stays on the function tag, where the evidence is.
    """
    if not tag_id:
        return None
    parts = str(tag_id).split(":")
    if len(parts) < 3 or parts[0] != "origin" or not parts[2]:
        return None
    return f"origin:{parts[1]}:{parts[2]}"


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
        ctag = capa_tag(rule.get("meta", {}).get("namespace"))
        if not ctag:
            continue
        for match in rule.get("matches", []):
            if not isinstance(match, (list, tuple)) or not match:
                continue
            addr = match[0]
            # "absolute" is the only kind a static run emits. File-scope rules
            # carry "no address" and have no function to hang a tag on.
            if not isinstance(addr, dict) or addr.get("type") != "absolute":
                continue
            if "value" in addr:
                hits.setdefault(addr["value"], set()).add(ctag)
    return base, hits


def yara_tag(category, family, rule_name):
    """A matched YARA rule -> `yara:<category>:<family>:<rule_name>` tag id.

    A YARA rule carries no built-in namespace the way a capa rule does, so the
    id is assembled from the vendored ruleset's own `meta.category` and
    `meta.malware` fields (e.g. `category: "ransomware"`, `malware: "LOCKBIT"`).
    Either can be missing on a rule that predates that convention; `unknown`
    keeps the id at a fixed four-segment depth rather than needing a per-rule
    rule for how many segments it has.
    """
    cat = str(category or "unknown").strip().lower() or "unknown"
    fam = str(family or "unknown").strip().lower() or "unknown"
    return f"yara:{cat}:{fam}:{rule_name}"


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
    tags = {_match_tag(match)}
    if extra:
        tags.update(extra.get(getattr(match, "namespace", None)) or ())
    return tags


def _match_tag(match):
    meta = getattr(match, "meta", None) or {}
    category, family = meta.get("category"), meta.get("malware")
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
    "*": RULEZET_NAMESPACE,
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
    if not target:  # `"tlp" = false` reads as "drop this namespace"
        return None

    literal = str(key).split("*", 1)[0].rstrip(":")
    rest = namespace[len(literal):].strip(":") if literal else namespace

    if str(target).split(":")[0] == MITRE_NAMESPACE:
        found = _MITRE_ID.search(value)
        return f"{MITRE_NAMESPACE}:{found.group(0).lower()}" if found else None

    parts = [str(target)] + [tag_slug(p) for p in rest.split(":") if p]
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

    if head in ("origin", "severity", "category", "user", CAPA_NAMESPACE,
                MITRE_NAMESPACE, MBC_NAMESPACE, YARA_NAMESPACE):
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
        "severity:medium", "category:crypto:cipher"], migrate_tag("flag:suspicious:crypto")
    assert migrate_tag("llm:malicious:injection") == [
        "severity:high", "category:process:inject"]
    assert migrate_tag("flag:benign:init") == ["severity:none", "category:util:init"]
    # Bare vocabulary tag the batch writer had prefixed, with no risk segment.
    assert migrate_tag("flag:crypto") == ["category:crypto:cipher"]
    # Unknown capability behind a known prefix must not vanish silently.
    assert migrate_tag("flag:suspicious:invented") == ["severity:medium"]
    assert migrate_tag("flag:nonsense") == ["user:flag:nonsense"]
    # Double-prefixed by the old writer disagreement.
    assert migrate_tag("flag:llm:benign:init") == ["severity:none", "category:util:init"]
    # ... and the same id after `namespaced()` buried it under `user:`.
    assert migrate_tag("user:flag:llm:benign:init") == [
        "severity:none", "category:util:init"]
    assert migrate_tag("user:flag:nonsense") == ["user:flag:nonsense"]

    assert migrate_tag("lib:libc:2.31:memcpy") == ["origin:lib:libc:2.31:memcpy"]
    assert migrate_tag("lib:uclibc") == ["origin:lib:uclibc:unknown"]
    assert migrate_tag("bundle:mirai") == ["origin:bundle:mirai:unknown"]
    assert migrate_tag("bundle:mirai:scanner") == ["origin:bundle:mirai:unknown:scanner"]
    assert migrate_tag("stdlib:musl:1.2.4") == ["origin:stdlib:musl:1.2.4"]

    # File-scope namespaces are left exactly as they are.
    assert migrate_tag("container:apk") == ["container:apk"]
    assert migrate_tag("packer:upx") == ["packer:upx"]
    # Already migrated -> idempotent, so a re-run is safe.
    for t in ("origin:lib:libc:2.31", "severity:high", "category:network:c2", "user:x"):
        assert migrate_tag(t) == [t], t
    # Human-typed, no namespace.
    assert migrate_tag("mirai") == ["user:mirai"]

    assert is_taxonomy_tag("severity:high")
    assert is_taxonomy_tag("category:network:c2")
    assert not is_taxonomy_tag("category:network:invented")
    assert not is_taxonomy_tag("origin:lib:libc:2.31"), "model must not invent origins"

    assert origin_tag("lib", "libc", "2.31", "memcpy") == "origin:lib:libc:2.31:memcpy"
    assert origin_tag("lib", "libc") == "origin:lib:libc:unknown"
    assert origin_tag("bundle", "mirai", None, "scanner") == "origin:bundle:mirai:unknown:scanner"
    # A tag the analyzer builds must roll up the way the split engine expects.
    assert origin_parent("origin:lib:uclibc:0.9.30.1:xdrmem_getint32") == "origin:lib:uclibc"
    assert origin_parent("origin:bundle:mirai:unknown") == "origin:bundle:mirai"
    assert origin_parent("severity:high") is None
    assert origin_parent("") is None

    assert namespaced("mytag") == "user:mytag"
    assert namespaced("category:network:c2") == "category:network:c2"

    # capa ids are recorded verbatim, never remapped into `category:`.
    assert capa_tag("host-interaction/file-system/write") == (
        "capa:host-interaction:file-system:write")
    assert capa_tag("communication/http/client") == "capa:communication:http:client"
    assert capa_tag(None) is None and capa_tag("") is None and capa_tag("/") is None
    assert namespaced("capa:communication:http") == "capa:communication:http", (
        "a capa tag must not be buried under user:")
    assert migrate_tag("capa:communication:http") == ["capa:communication:http"]
    assert not is_taxonomy_tag("capa:communication:http"), (
        "the model must not be able to invent capa findings")

    # A capa document, shaped exactly as `capa -j` writes it: `matches` holds
    # [address, result] pairs. Reading it as a list of dicts is what silently
    # produced zero capa tags, so this is the assert that has to fail if the
    # pair-unpacking is ever "simplified" back.
    doc = {
        "meta": {"analysis": {"base_address": {"type": "absolute", "value": 0x2000000}}},
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
            "create or open file": {"meta": {}, "matches": [
                [{"type": "absolute", "value": 0x2003000}, {"success": True}]]},
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

    assert yara_tag("Ransomware", "LOCKBIT", "Win32_Ransomware_LockBit") == (
        "yara:ransomware:lockbit:Win32_Ransomware_LockBit")
    assert yara_tag(None, None, "no_meta_rule") == "yara:unknown:unknown:no_meta_rule"
    assert namespaced("yara:ransomware:lockbit:x") == "yara:ransomware:lockbit:x", (
        "a yara tag must not be buried under user:")

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
        _Match("Win32_Ransomware_LockBit", {"category": "Ransomware", "malware": "LOCKBIT"},
               [[0x1000], [0x2000, 0x2500]]),
        # No meta at all -- an older or hand-written rule -- still tags, at unknown depth.
        _Match("homebrew_rule", {}, [[0x3000]]),
        # Condition-only rule: matched the file, names no string offset at all.
        _Match("Win32_Packer_Themida", {"category": "Packer"}, []),
        # Elastic: no category/malware, but `threat_name` carries both.
        _Match("Linux_Trojan_Mirai_268aac0b", {"threat_name": "Linux.Trojan.Mirai"},
               [[0x4000]]),
        # A threat_name that is not <os>.<category>.<family> stays at unknown depth
        # rather than being sliced into the wrong two segments.
        _Match("odd_shape_rule", {"threat_name": "Linux.Trojan"}, [[0x5000]]),
    ]
    hits = yara_rule_hits(matches)
    assert hits == {
        0x1000: {"yara:ransomware:lockbit:Win32_Ransomware_LockBit"},
        0x2000: {"yara:ransomware:lockbit:Win32_Ransomware_LockBit"},
        0x2500: {"yara:ransomware:lockbit:Win32_Ransomware_LockBit"},
        0x3000: {"yara:unknown:unknown:homebrew_rule"},
        0x4000: {"yara:trojan:mirai:Linux_Trojan_Mirai_268aac0b"},
        0x5000: {"yara:unknown:unknown:odd_shape_rule"},
    }, hits
    assert yara_rule_hits([]) == {}

    # The file-level set is not a rollup of the offset map: the condition-only
    # rule is in it and is absent from every value in `hits`.
    file_tags = yara_file_tags(matches)
    assert file_tags == {
        "yara:ransomware:lockbit:Win32_Ransomware_LockBit",
        "yara:unknown:unknown:homebrew_rule",
        "yara:packer:unknown:Win32_Packer_Themida",
        "yara:trojan:mirai:Linux_Trojan_Mirai_268aac0b",
        "yara:unknown:unknown:odd_shape_rule",
    }, file_tags
    assert "yara:packer:unknown:Win32_Packer_Themida" not in set().union(*hits.values())
    assert yara_file_tags([]) == set()

    # The mirrored ruleset's tags ride in a sidecar keyed by the uuid YARA
    # reports as the match namespace, because rule *names* collide freely across
    # 130k rules from different repos. Vendored matches carry a file index there
    # and must miss the sidecar cleanly.
    class _NsMatch(_Match):
        def __init__(self, rule, meta, offsets, namespace):
            super().__init__(rule, meta, offsets)
            self.namespace = namespace

    mirrored = [_NsMatch("Some_Rule", {"category": "trojan", "malware": "mirai"},
                         [[0x7000]], "abc-uuid")]
    sidecar = {"abc-uuid": ["mitre:t1027", "cve:cve-2021-44228"]}
    assert yara_file_tags(mirrored, sidecar) == {
        "yara:trojan:mirai:Some_Rule", "mitre:t1027", "cve:cve-2021-44228"}
    assert yara_rule_hits(mirrored, sidecar) == {
        0x7000: {"yara:trojan:mirai:Some_Rule", "mitre:t1027", "cve:cve-2021-44228"}}
    # No sidecar entry, and matches with no namespace at all, still work.
    assert yara_file_tags(mirrored, {"other": ["x"]}) == {"yara:trojan:mirai:Some_Rule"}
    assert yara_file_tags(matches[:1], sidecar) == {
        "yara:ransomware:lockbit:Win32_Ransomware_LockBit"}

    # --- Source tag routing -------------------------------------------------
    r = route_source_tag
    assert r('misp-galaxy:tool="Cobalt Strike"') == "misp:tool:cobalt-strike"
    assert r('misp-galaxy:ransomware="LockBit"') == "misp:ransomware:lockbit"
    # A specific galaxy beats the wildcard no matter the dict order.
    assert r('misp-galaxy:mitre-attack-pattern="Obfuscated Files - T1027"') == (
        "mitre:t1027")
    assert r('misp-galaxy:mitre-attack-pattern="Indicator Removal - T1027.005"') == (
        "mitre:t1027.005")
    # An attack-pattern cluster with no technique id is not an ATT&CK fact.
    assert r('misp-galaxy:mitre-attack-pattern="Some Prose"') is None
    # Unrouted namespaces are kept, not lost -- the whole source path survives.
    assert r('runtime-packer:pe="upx"') == "rulezet:runtime-packer:pe:upx"
    assert r('ms-caro-malware-full:malware-type="Trojan"') == (
        "rulezet:ms-caro-malware-full:malware-type:trojan")
    assert r("cve:CVE-2021-44228") == "cve:cve-2021-44228"
    assert r("ghsa:GHSA-j8v8-6h6r-m6pq") == "ghsa:ghsa-j8v8-6h6r-m6pq"
    # Distribution markers: the family and a single value.
    assert r("tlp:clear") is None and r("tlp:white") is None and r("pap:clear") is None
    assert r("tlp:red", drops=["tlp:white"]) == "rulezet:tlp:red"
    # `false` as a target reads as "drop this namespace".
    assert r('misp-galaxy:tool="Netcat"', {"misp-galaxy:*": False}) is None
    # No catch-all configured -> an unrouted tag is dropped rather than guessed.
    assert r('runtime-packer:pe="upx"', {"cve": "cve"}) is None
    assert r("") is None and r(None) is None
    # A routed tag must never end up buried under `user:`.
    for src in ('misp-galaxy:tool="X"', "cve:CVE-2021-1", 'misp-galaxy:x="y"'):
        assert not route_source_tag(src).startswith("user:"), src
    assert tag_slug("Cobalt Strike 4.0!") == "cobalt-strike-4-0"

    rules = prompt_rules()
    assert "severity:<level>" in rules and "key_exchange" in rules
    print("tag_taxonomy demo OK")


if __name__ == "__main__":
    demo()
