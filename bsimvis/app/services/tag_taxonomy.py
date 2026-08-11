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
"""

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

# Reserved the same way as capa, ahead of a producer: `mitre:<tactic>:<technique>`
# and `mbc:<objective>:<behavior>`, recorded verbatim once something writes them.
MITRE_NAMESPACE = "mitre"
MBC_NAMESPACE = "mbc"

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
) + FILE_SCOPE_NAMESPACES


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

    if head in ("origin", "severity", "category", "user", CAPA_NAMESPACE):
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

    rules = prompt_rules()
    assert "severity:<level>" in rules and "key_exchange" in rules
    print("tag_taxonomy demo OK")


if __name__ == "__main__":
    demo()
