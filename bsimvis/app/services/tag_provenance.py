"""Where a tag came from: the rule that minted it, and a link to its source.

Tags stay flat strings everywhere else in the system -- the index buckets, the
search filters, the axis splits and the propagation cascade all key on the
string and nothing else. This module is a *side table* hanging off that string,
read only when a user clicks a tag. Nothing here is on the search path, and
nothing here changes what a tag is.

Two ways a tag gets its provenance, picked per source by whether the tag id
already determines the answer:

  * **Derived** -- `capa:host-interaction:file-system:write` *is* the path of a
    directory in the capa-rules repo, so the URL is a string transform with
    nothing to store. Same shape is what a future `spdx:`/FunctionID mapping
    wants.
  * **Stored** -- a `yara:` tag names a rule, and a rule name says nothing about
    where the rule lives (589 vendored files, and names collide freely across
    the 130k-rule mirror). So the scan records it: the worker has the match
    object, which carries the file path or the rulezet uuid in its namespace.

Records are a list per tag, not a single dict, because the same rule name really
does occur in more than one file -- the mirror's rule names are not unique and
the vendored set has no rule saying they must be. One record is the normal case.

The store is global rather than per-collection: "which file is rule X in" is a
fact about the ruleset, identical for every collection that ever matched it.
"""

import json
from urllib.parse import urlencode

# One hash, tag -> JSON list of records. Not namespaced per collection: see the
# module docstring.
KEY = "global:tag_provenance"

CAPA_RULES_REPO = "https://github.com/mandiant/capa-rules/tree/master"


def _redis():
    from bsimvis.app.services.redis_client import get_redis

    return get_redis()


# --- Derived (no storage) ---------------------------------------------------


def capa_provenance(tag):
    """`capa:a:b:c` -> the capa-rules directory that namespace is.

    capa tags are built from a rule's *namespace*, not its name
    (`tag_taxonomy.capa_tag`), and a namespace is literally a directory path in
    the capa-rules repo. So one rule never owns a capa tag -- the directory
    does, and that is what this links to.
    """
    parts = [p for p in str(tag or "").split(":") if p]
    if len(parts) < 2 or parts[0] != "capa":
        return None
    return {
        "source": "capa",
        "name": "/".join(parts[1:]),
        "url": f"{CAPA_RULES_REPO}/" + "/".join(parts[1:]),
    }


def derived(tag):
    """Provenance computable from the tag id alone, or None."""
    return capa_provenance(tag)


# --- Stored (recorded at scan time) -----------------------------------------


def rulezet_url(title):
    """Deep link to one rule on rulezet.org.

    rulezet has **no per-rule permalink** -- its web UI is a JS table over
    `get_rules_page_filter` and there is no `/rule/<uuid>` route to link to
    (checked against rulezet-core's own blueprint). The exact-title filter is
    the closest thing that resolves to a single rule, and `rules_list` forwards
    its whole query string into that filter, so this is a real deep link rather
    than a search box the user still has to drive.
    """
    from bsimvis.app.services.rulezet_service import DEFAULT_URL, cfg

    base = (cfg("url") or DEFAULT_URL).rstrip("/")
    query = urlencode(
        {"search": title, "search_field": "title", "exact_match": "true"}
    )
    return f"{base}/rule/rules_list?{query}"


def _mirror_meta():
    """`{uuid: {title, author, ...}}` written by `rulezet sync`, or `{}`."""
    from bsimvis.app.services.rulezet_service import paths

    p = paths()["rules_meta"]
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (ValueError, OSError):
        return {}


def _vendored_path(ns):
    """Rule file path relative to the ruleset root (`elastic/Foo.yar`).

    The namespace is an absolute path because that is what `yara.compile` was
    handed. Relative is what identifies the rule -- the absolute prefix is this
    deployment's install directory, which is noise in the UI and not something
    to hand out.
    """
    if not ns:
        return None
    from pathlib import Path

    from bsimvis.app.services.yara_service import rules_dir

    try:
        return str(Path(ns).relative_to(rules_dir()))
    except ValueError:
        return ns


def _is_mirror_rule(ns):
    """Is this namespace a mirrored rule's uuid rather than a vendored path?"""
    if not ns or "/" in ns:
        return False
    from bsimvis.app.services.rulezet_service import paths

    try:
        return (paths()["rules"] / f"{ns}.yara").exists()
    except OSError:
        return False


def _match_record(match, mirror_meta):
    """One YARA match -> its provenance record.

    `match.namespace` is the join key for both rulesets, by construction: the
    mirror compiles with the rule's uuid there (`rulezet_service.compile_mirror`)
    and the vendored set with the rule file's path (`yara_service.compiled_rules`).
    A uuid hits the sync sidecar; anything else is a vendored path.
    """
    ns = str(getattr(match, "namespace", "") or "")
    meta = getattr(match, "meta", None) or {}

    # A mirror rule is identified by its uuid file existing, not by the sidecar
    # having an entry: mirrors synced before `rules_meta.json` existed have the
    # rules but no metadata, and calling those a vendored file path would put a
    # bare uuid in the UI's "File" row.
    entry = mirror_meta.get(ns)
    if entry is None and _is_mirror_rule(ns):
        entry = {}
    if entry is not None:
        title = entry.get("title")
        return {
            "source": "rulezet",
            "id": ns,
            "name": match.rule,
            "title": title or match.rule,
            "author": entry.get("author") or meta.get("author"),
            "license": entry.get("license"),
            "upstream": entry.get("github_path"),
            # No title means a mirror synced before the sidecar existed. The
            # uuid still identifies the rule, but rulezet's only single-rule
            # deep link is by exact title, so there is no honest link to give.
            "url": rulezet_url(title) if title else None,
        }

    return {
        "source": "yara",
        "name": match.rule,
        "path": _vendored_path(ns),
        "author": meta.get("author"),
        "description": meta.get("description"),
        # `reference` is the vendored convention; Elastic's rules use neither.
        "url": meta.get("reference") or meta.get("url") or None,
    }


def yara_provenance(matches):
    """`{tag: [record, ...]}` for every rule that matched.

    Only the rule's *own* `yara:` tag is mapped. The mirror's sidecar tags
    (`mitre:`, `cve:`) ride along on the same match but are taxonomy, not rule
    identity -- thousands of rules carry `mitre:t1027`, so pointing it at
    whichever one happened to fire would be a lie.
    """
    from bsimvis.app.services.tag_taxonomy import tag_for_match

    meta = _mirror_meta() if matches else {}
    out = {}
    for match in matches:
        out.setdefault(tag_for_match(match), []).append(_match_record(match, meta))
    return out


def _key(record):
    return (record.get("source"), record.get("id"), record.get("path"),
            record.get("name"))


def record(provenance, r=None):
    """Merge `{tag: [record, ...]}` into the store. Returns tags touched.

    Merged rather than overwritten because two files can carry the same rule
    name, and a scan only ever sees the ones that matched *this* sample -- a
    write that replaced the list would make provenance flap between samples.
    """
    if not provenance:
        return 0
    r = r or _redis()
    touched = 0
    for tag, records in provenance.items():
        raw = r.hget(KEY, tag)
        try:
            existing = json.loads(raw) if raw else []
        except ValueError:
            existing = []
        merged = {_key(rec): rec for rec in existing}
        before = len(merged)
        for rec in records:
            merged.setdefault(_key(rec), rec)
        if len(merged) != before:
            r.hset(KEY, tag, json.dumps(list(merged.values())))
            touched += 1
    return touched


def lookup(tags, r=None):
    """`{tag: [record, ...]}` for the tags that have any. Stored plus derived.

    Tags with no provenance are simply absent, so a caller can hand this the
    whole tag list of an entity and render links for whatever comes back.
    """
    tags = [t for t in (tags or []) if t]
    if not tags:
        return {}

    out = {}
    try:
        raw = (r or _redis()).hmget(KEY, tags)
    except Exception:
        raw = [None] * len(tags)

    for tag, value in zip(tags, raw):
        records = []
        if value:
            try:
                records = json.loads(value)
            except ValueError:
                records = []
        d = derived(tag)
        if d and not any(_key(rec) == _key(d) for rec in records):
            records.append(d)
        if records:
            out[tag] = records
    return out


def demo():
    """Check the two joins that only fail for real: namespace -> record."""

    class M:
        def __init__(self, rule, namespace, meta=None):
            self.rule, self.namespace, self.meta = rule, namespace, meta or {}

    uuid = "11111111-2222-3333-4444-555555555555"
    mirror_meta = {uuid: {"title": "Some Rule Title", "author": "someone",
                          "license": "MIT"}}

    rec = _match_record(M("Mirror_Rule", uuid), mirror_meta)
    assert rec["source"] == "rulezet" and rec["id"] == uuid, rec
    assert "search=Some+Rule+Title" in rec["url"], rec["url"]
    assert "exact_match=true" in rec["url"], rec["url"]

    rec = _match_record(
        M("Vendored_Rule", "/data/yara_rules/apt/x.yar", {"author": "florian"}),
        mirror_meta,
    )
    assert rec["source"] == "yara", rec
    assert rec["path"] == "/data/yara_rules/apt/x.yar", rec
    assert rec["author"] == "florian", rec

    # A mirror synced before the sidecar existed: still a rulezet rule (the
    # uuid file is there), just with no title to build a deep link from.
    import tempfile
    from pathlib import Path

    from bsimvis.app.services import rulezet_service

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "rules").mkdir()
        (root / "rules" / f"{uuid}.yara").write_text("rule X { condition: true }")
        original = rulezet_service.mirror_dir
        rulezet_service.mirror_dir = lambda: root
        try:
            rec = _match_record(M("Mirror_Rule", uuid), {})
            assert rec["source"] == "rulezet" and rec["id"] == uuid, rec
            assert rec["url"] is None, rec
        finally:
            rulezet_service.mirror_dir = original

    # A capa tag needs no store at all -- the id is the path.
    d = derived("capa:host-interaction:file-system:write")
    assert d["url"].endswith("/host-interaction/file-system/write"), d
    assert derived("yara:trojan:mirai:Some_Rule") is None

    # Sidecar tags are deliberately not mapped; only the rule's own tag is.
    prov = yara_provenance([M("Mirror_Rule", uuid)])
    assert list(prov) == ["yara:unknown:unknown:Mirror_Rule"], list(prov)

    # Merge keeps both files when one rule name lives in two of them.
    store = {}

    class FakeRedis:
        def hget(self, _k, f):
            return store.get(f)

        def hset(self, _k, f, v):
            store[f] = v

        def hmget(self, _k, fs):
            return [store.get(f) for f in fs]

    fake = FakeRedis()
    record({"yara:a:b:R": [{"source": "yara", "path": "/one.yar", "name": "R"}]}, fake)
    record({"yara:a:b:R": [{"source": "yara", "path": "/two.yar", "name": "R"}]}, fake)
    got = lookup(["yara:a:b:R"], fake)["yara:a:b:R"]
    assert len(got) == 2, got
    # Re-recording the same rule does not grow the list.
    assert record(
        {"yara:a:b:R": [{"source": "yara", "path": "/one.yar", "name": "R"}]}, fake
    ) == 0
    assert lookup(["nothing:here"], fake) == {}

    print("tag_provenance demo OK")


if __name__ == "__main__":
    demo()
