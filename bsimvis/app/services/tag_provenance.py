"""Which rule produced a tag, and where that rule came from.

Tags stay flat strings everywhere else in the system -- the index buckets, the
search filters, the axis splits and the propagation cascade all key on the
string and nothing else. Nothing here is on the search path, and nothing here
changes what a tag is.

The data is **normalised**, because it has to be: a tag like
`ms-caro-malware-full:malware-platform:linux` is carried by tens of
thousands of rules, and a broad mirror is ~214k rules. So a rule's metadata is
stored exactly once, and everything else refers to it by id:

    global:rule_meta            rule id  -> one JSON row per rule
    global:tag_rules:<tag>      set of rule ids that can emit that tag
    <collection>:rule_hits      entity id -> JSON list of rule ids that fired

That split is what the two read paths need. "Why is this tag on this file?"
reads the entity's own hit list -- a handful of ids. "What is this tag, across
the whole ruleset?" reads the tag index, which is large and therefore paged.
Neither one ever stores a rule's metadata against a tag.

Rule ids are one flat namespace so a single lookup answers all three analysers:

  * rulezet  -- the rule's uuid, exactly as rulezet issues it.
  * vendored -- `yara:<path under the ruleset>#<rule name>`. A rule name alone
    is not unique across 589 files, and neither is a file.
  * capa     -- `capa:<namespace>`. A capa tag is built from the namespace
    rather than the rule name (`tag_taxonomy.capa_tag`), so the namespace is
    what the tag actually identifies.

A future FunctionID/SPDX source adds a prefix here and nothing else changes.
"""

import json
from urllib.parse import urlencode

RULE_META_KEY = "global:rule_meta"
TAG_RULES_PREFIX = "global:tag_rules:"

CAPA_RULES_REPO = "https://github.com/mandiant/capa-rules/tree/master"


def _redis():
    from bsimvis.app.services.redis_client import get_redis

    return get_redis()


def hits_key(collection):
    return f"{collection}:rule_hits"


# --- Rule ids ---------------------------------------------------------------


def yara_rule_id(path, rule_name):
    """`yara:elastic/Foo.yar#Rule_Name` -- file *and* name, since neither alone
    identifies a vendored rule."""
    return f"yara:{path}#{rule_name}"


def capa_rule_id(namespace):
    return "capa:" + str(namespace or "").strip("/")


def _vendored_path(ns):
    """Rule file path relative to the ruleset root (`elastic/Foo.yar`).

    `match.namespace` is an absolute path because that is what `yara.compile`
    was handed. The absolute prefix is this deployment's install directory --
    noise in the UI, and not something to hand out.
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
    """Is this namespace a mirrored rule's uuid rather than a vendored path?

    Asked of the filesystem rather than of the metadata table: a mirror synced
    before metadata existed has the rule files and no rows, and those are still
    rulezet rules.
    """
    if not ns or "/" in ns:
        return False
    from bsimvis.app.services.rulezet_service import paths

    try:
        return (paths()["rules"] / f"{ns}.yara").exists()
    except OSError:
        return False


# --- Metadata rows ----------------------------------------------------------


def rulezet_url(title):
    """Deep link to one rule on rulezet.org.

    rulezet has **no per-rule permalink** -- its rule list is a JS table over
    `get_rules_page_filter` and no `/rule/<uuid>` route exists (checked against
    rulezet-core's own blueprint). The exact-title filter is the closest thing
    that resolves to a single rule, and `rules_list` forwards its whole query
    string into that filter, so this is a real deep link.
    """
    from bsimvis.app.services.rulezet_service import DEFAULT_URL, cfg

    base = (cfg("url") or DEFAULT_URL).rstrip("/")
    query = urlencode({"search": title, "search_field": "title", "exact_match": "true"})
    return f"{base}/rule/rules_list?{query}"


def rule_url(rid, row):
    """The link for a rule row, rebuilt on read rather than stored.

    A URL is derivable from the id and the title, so storing it would be one
    more copy of the same string across ~214k rows for nothing.
    """
    source = row.get("source")
    if source == "capa":
        return f"{CAPA_RULES_REPO}/" + rid.split(":", 1)[1]
    if source == "rulezet":
        title = row.get("title")
        # Without a title there is no honest link: rulezet's only single-rule
        # URL is by exact title. The uuid still identifies the rule.
        return rulezet_url(title) if title else None
    return row.get("upstream") or None


def rulezet_row(rule, tags=None):
    """A rulezet API rule dict -> the row stored for it.

    Only fields a user reading provenance needs. Notably not the rule text: it
    is already on disk as `rules/<uuid>.yara`, and 214k copies of it in the
    database would dwarf everything else here.
    """
    row = {
        "source": "rulezet",
        "title": rule.get("title"),
        "author": rule.get("author"),
        "license": rule.get("license"),
        "upstream": rule.get("github_path"),
        "format": rule.get("format"),
    }
    if tags:
        row["tags"] = sorted(tags)
    return {k: v for k, v in row.items() if v}


def match_rule_id(match):
    """The rule id one match is stored under -- mirror uuid, or vendored id."""
    ns = str(getattr(match, "namespace", "") or "")
    if _is_mirror_rule(ns):
        return ns
    return yara_rule_id(_vendored_path(ns), match.rule)


def match_offsets(matches):
    """`{file offset: {rule id, ...}}` -- which rules fired at each offset.

    Attributing an offset to a function needs Ghidra's memory map, so that stays
    the caller's job (`_funcs_by_offset`). This only carries the ids, and it
    keys them the way `match_rows` keys its rows, so a function's hit list can
    never name a rule the table cannot describe.
    """
    out = {}
    for match in matches:
        rid = match_rule_id(match)
        for string_match in getattr(match, "strings", None) or []:
            for instance in getattr(string_match, "instances", None) or []:
                out.setdefault(instance.offset, set()).add(rid)
    return out


def match_rows(matches, extra=None):
    """yara-python matches -> `{rule id: row}` for the rules that fired.

    `extra` is the mirror's tag sidecar (`{uuid: [tag, ...]}`), so a mirrored
    rule's row records every tag it emits -- which is what lets the read path
    group an entity's hits by tag without a second index.
    """
    from bsimvis.app.services.tag_taxonomy import tag_for_match

    rows = {}
    for match in matches:
        ns = str(getattr(match, "namespace", "") or "")
        meta = getattr(match, "meta", None) or {}
        own_tag = tag_for_match(match)

        if _is_mirror_rule(ns):
            tags = set(extra.get(ns) or ()) if extra else set()
            tags.add(own_tag)
            import uuid

            try:
                uuid.UUID(ns)
                tags.add(f"rulezet:{match.rule}")
            except ValueError:
                pass
            rows[ns] = {
                "source": "rulezet",
                "name": match.rule,
                "author": meta.get("author"),
                "tags": sorted(tags),
            }
        else:
            path = _vendored_path(ns)
            rows[match_rule_id(match)] = {
                "source": "yara",
                "name": match.rule,
                "path": path,
                "author": meta.get("author"),
                "upstream": meta.get("reference") or meta.get("url"),
                "tags": [own_tag],
            }
    return {rid: {k: v for k, v in row.items() if v} for rid, row in rows.items()}


def capa_rows(cdata):
    """capa `-j` document -> `{rule id: row}`.

    Keyed by namespace, not rule name: the tag is built from the namespace, so
    that is the thing a user clicking the tag is asking about. Several rules
    share one namespace, and their names are collected into the row.
    """
    from bsimvis.app.services.tag_taxonomy import capa_tag

    rows = {}
    for rule in (cdata or {}).get("rules", {}).values():
        meta = rule.get("meta", {}) or {}
        tag = capa_tag(meta.get("namespace"))
        if not tag:
            continue
        row = rows.setdefault(
            capa_rule_id(meta.get("namespace")),
            {
                "source": "capa",
                "name": meta.get("namespace"),
                "tags": [tag],
                "rules": [],
            },
        )
        name = meta.get("name")
        if name and name not in row["rules"]:
            row["rules"].append(name)
    return rows


# --- Writes -----------------------------------------------------------------


def put_rules(rows, r=None):
    """Store rule rows and index the tags they emit. Returns rows written.

    Rows already present are left alone: the mirror's own sync knows more about
    a rulezet rule (title, license, upstream) than a scan-time match ever does,
    so a match must not overwrite it with the little it has.
    """
    if not rows:
        return 0
    r = r or _redis()
    ids = list(rows)
    existing = r.hmget(RULE_META_KEY, ids)

    written = 0
    for rid, current in zip(ids, existing):
        row = rows[rid]
        if current:
            try:
                stored = json.loads(current)
            except ValueError:
                stored = {}
            old_tags = set(stored.get("tags") or [])
            new_tags = set(row.get("tags") or [])
            if new_tags - old_tags:
                stored["tags"] = sorted(old_tags | new_tags)
                r.hset(RULE_META_KEY, rid, json.dumps(stored))
                for tag in new_tags - old_tags:
                    r.sadd(TAG_RULES_PREFIX + tag, rid)
                written += 1
            continue

        r.hset(RULE_META_KEY, rid, json.dumps(row))
        for tag in row.get("tags") or []:
            r.sadd(TAG_RULES_PREFIX + tag, rid)
        written += 1
    return written


def put_rules_bulk(rows, r=None, overwrite=True):
    """Sync's write path: authoritative rows, pipelined.

    Unlike `put_rules` this *does* overwrite -- it is the sync speaking, which
    is the source of truth for a rulezet rule.
    """
    if not rows:
        return 0
    r = r or _redis()
    pipe = r.pipeline()
    for rid, row in rows.items():
        if not overwrite:
            pipe.hsetnx(RULE_META_KEY, rid, json.dumps(row))
        else:
            pipe.hset(RULE_META_KEY, rid, json.dumps(row))
        for tag in row.get("tags") or []:
            pipe.sadd(TAG_RULES_PREFIX + tag, rid)
    pipe.execute()
    return len(rows)


def record_hits(collection, entity_id, rule_ids, r=None):
    """Remember which rules fired on one entity (file or function)."""
    if not (collection and entity_id and rule_ids):
        return False
    r = r or _redis()
    r.hset(hits_key(collection), entity_id, json.dumps(sorted(set(rule_ids))))
    return True


def record_hits_bulk(collection, hits, r=None):
    """`{entity id: [rule id, ...]}` for a whole program, in one pipeline."""
    hits = {k: v for k, v in (hits or {}).items() if v}
    if not (collection and hits):
        return 0
    r = r or _redis()
    pipe = r.pipeline()
    key = hits_key(collection)
    for entity_id, rule_ids in hits.items():
        pipe.hset(key, entity_id, json.dumps(sorted(set(rule_ids))))
    pipe.execute()
    return len(hits)


# --- Reads ------------------------------------------------------------------


def rule_meta(rule_ids, r=None):
    """`{rule id: row}` with the url rebuilt. Unknown ids are simply absent."""
    rule_ids = [i for i in dict.fromkeys(rule_ids or []) if i]
    if not rule_ids:
        return {}
    raw = (r or _redis()).hmget(RULE_META_KEY, rule_ids)

    out = {}
    for rid, value in zip(rule_ids, raw):
        row = None
        if value:
            try:
                row = json.loads(value)
            except ValueError:
                row = None
        if row is None:
            # A rule seen in a hit list but never described. Still worth
            # answering with: the id says what it is and where it came from.
            row = _row_from_id(rid)
        row["url"] = rule_url(rid, row)
        out[rid] = row
    return out


def _row_from_id(rid):
    """Best-effort row for an id with no stored metadata."""
    if rid.startswith("capa:"):
        return {"source": "capa", "name": rid.split(":", 1)[1]}
    if rid.startswith("yara:"):
        rest = rid.split(":", 1)[1]
        path, _, name = rest.partition("#")
        return {"source": "yara", "name": name or None, "path": path}
    return {"source": "rulezet"}


def match_provenance(collection, entity_ids, r=None):
    """Endpoint A: why do these entities carry the tags they carry?

    Returns `{entity id: {tag: [rule id, ...]}}` plus one shared `rules` table,
    so a rule matched by twenty functions is described once in the response
    rather than twenty times.
    """
    entity_ids = [e for e in dict.fromkeys(entity_ids or []) if e]
    if not (collection and entity_ids):
        return {}, {}
    r = r or _redis()

    raw = r.hmget(hits_key(collection), entity_ids)
    per_entity, all_ids = {}, []
    for entity_id, value in zip(entity_ids, raw):
        try:
            ids = json.loads(value) if value else []
        except ValueError:
            ids = []
        if ids:
            per_entity[entity_id] = ids
            all_ids.extend(ids)

    rules = rule_meta(all_ids, r)

    out = {}
    for entity_id, ids in per_entity.items():
        by_tag = {}
        for rid in ids:
            for tag in rules.get(rid, {}).get("tags") or []:
                by_tag.setdefault(tag, []).append(rid)
        out[entity_id] = {t: sorted(set(v)) for t, v in sorted(by_tag.items())}
    return out, rules


def tag_rules(tag, offset=0, limit=50, r=None):
    """Endpoint B: every rule that can emit a tag, paged.

    Paged because it has to be -- a MISP platform tag is carried by tens of
    thousands of mirror rules, and the count is often the interesting half of
    the answer on its own.
    """
    if not tag:
        return 0, {}
    r = r or _redis()
    key = TAG_RULES_PREFIX + tag

    total = r.scard(key)
    if not total:
        return 0, {}

    ids = sorted(i.decode() if isinstance(i, bytes) else i for i in r.smembers(key))[
        offset : offset + limit
    ]
    return total, rule_meta(ids, r)


def demo():
    """Cover the joins that only fail for real: id shape, grouping, paging."""

    class M:
        def __init__(self, rule, namespace, meta=None):
            self.rule, self.namespace, self.meta = rule, namespace, meta or {}

    class FakeRedis:
        def __init__(self):
            self.h, self.s = {}, {}

        def hset(self, k, f, v):
            self.h.setdefault(k, {})[f] = v

        def hsetnx(self, k, f, v):
            self.h.setdefault(k, {}).setdefault(f, v)

        def hget(self, k, f):
            return self.h.get(k, {}).get(f)

        def hmget(self, k, fs):
            return [self.h.get(k, {}).get(f) for f in fs]

        def sadd(self, k, *v):
            self.s.setdefault(k, set()).update(v)

        def scard(self, k):
            return len(self.s.get(k, ()))

        def smembers(self, k):
            return set(self.s.get(k, ()))

        def pipeline(self):
            return self

        def execute(self):
            return []

    r = FakeRedis()
    uuid = "11111111-2222-3333-4444-555555555555"

    # Sync's authoritative rows, then the tag index they build.
    put_rules_bulk(
        {
            uuid: rulezet_row(
                {"title": "Some Rule Title", "author": "someone", "license": "MIT"},
                tags=["platform:linux", "rulezet:Mirror_Rule", "yara:trojan:x:Mirror_Rule"],
            )
        },
        r,
    )
    total, rules = tag_rules("platform:linux", r=r)
    assert total == 1 and uuid in rules, (total, rules)
    assert "search=Some+Rule+Title" in rules[uuid]["url"], rules[uuid]
    assert "content" not in rules[uuid] and "url" not in r.h[RULE_META_KEY][uuid]

    # A scan-time row must not clobber what the sync knows.
    put_rules({uuid: {"source": "rulezet", "name": "Mirror_Rule"}}, r)
    assert json.loads(r.h[RULE_META_KEY][uuid])["title"] == "Some Rule Title"

    # Vendored ids carry file *and* rule name; capa ids are the namespace.
    rows = match_rows([M("R", "/rules/elastic/a.yar", {"author": "Elastic"})])
    (vid,) = rows

    # Per-offset ids use the *same* keys as the rows, or a function's hit list
    # names rules the table cannot describe. Two rules on one offset is the
    # normal case, not an edge case.
    class S:
        def __init__(self, *offsets):
            self.instances = [type("I", (), {"offset": o})() for o in offsets]

    m1 = M("R", "/rules/elastic/a.yar")
    m1.strings = [S(0x100, 0x200)]
    m2 = M("Q", "/rules/elastic/b.yar")
    m2.strings = [S(0x200)]
    offsets = match_offsets([m1, m2])
    assert offsets == {
        0x100: {"yara:/rules/elastic/a.yar#R"},
        0x200: {"yara:/rules/elastic/a.yar#R", "yara:/rules/elastic/b.yar#Q"},
    }, offsets
    assert match_rule_id(m1) in rows, (match_rule_id(m1), list(rows))
    # A condition-only rule has no string instances: file level or nothing.
    assert match_offsets([M("C", "/rules/x.yar")]) == {}

    assert vid == "yara:/rules/elastic/a.yar#R", vid
    assert rows[vid]["author"] == "Elastic", rows[vid]
    crows = capa_rows(
        {
            "rules": {
                "x": {
                    "meta": {
                        "namespace": "host-interaction/file-system",
                        "name": "write file",
                    }
                }
            }
        }
    )
    assert "capa:host-interaction/file-system" in crows, crows

    put_rules(rows, r)
    put_rules(crows, r)

    # Endpoint A groups an entity's hits by tag, with one shared rule table.
    record_hits_bulk(
        "coll",
        {"f1": [uuid, vid], "f2": [vid]},
        r,
    )
    by_entity, table = match_provenance("coll", ["f1", "f2", "f3"], r)
    assert set(by_entity) == {"f1", "f2"}, by_entity
    assert by_entity["f1"]["platform:linux"] == [uuid], by_entity["f1"]
    assert by_entity["f2"] == {"yara:unknown:unknown:R": [vid]}, by_entity["f2"]
    assert set(table) == {uuid, vid}, table
    assert table[vid]["path"] == "/rules/elastic/a.yar", table[vid]

    # capa's url is derived from the id, never stored.
    cmeta = rule_meta(["capa:host-interaction/file-system"], r)
    assert cmeta["capa:host-interaction/file-system"]["url"].endswith(
        "/host-interaction/file-system"
    ), cmeta

    # An id in a hit list with no row still answers.
    assert (
        rule_meta(["capa:nursery/thing"], r)["capa:nursery/thing"]["source"] == "capa"
    )

    # Paging: the count is the answer for a broad tag, not the id list.
    put_rules_bulk(
        {
            f"u{i}": {"source": "rulezet", "tags": ["platform:linux"]}
            for i in range(120)
        },
        r,
    )
    total, page = tag_rules("platform:linux", offset=0, limit=10, r=r)
    assert total == 121 and len(page) == 10, (total, len(page))

    print("tag_provenance demo OK")


if __name__ == "__main__":
    demo()
