"""Strip the dead `rulezet:` catch-all off the mirror's stored tag ids.

`rulezet:` used to be the routing fallback, so every source namespace the config
did not name was swallowed by it:

    rulezet:ms-caro-malware-full:malware-platform:linux
        -> ms-caro-malware-full:malware-platform:linux
    rulezet:runtime-packer:pe:upx  ->  runtime-packer:pe:upx
    rulezet:ELF_Toriilike_persist  ->  unchanged (a rule *name*, which is what
                                       `rulezet:` means now -- the ruleset axis)

The mapping is `tag_taxonomy.migrate_tag`, so this script only moves storage,
and it is idempotent: an already-migrated id maps to itself.

Why a migration and not a re-sync: the routing runs at **sync** time and its
result is frozen in the `tags.json` sidecar, and `rulezet_service._merge_tags`
is union-only by design -- it must not drop the tags `index_tags()` writes,
which no bulk sync can rediscover. So a re-sync would *add* the new ids and keep
the old ones, double-tagging every file, at the price of a full API sync.

What it rewrites:

  * `data/rulezet/tags.json` -- the tag sidecar, the source of every mirror tag
    a scan applies. Backed up to `tags.json.bak` on the first apply.
  * `global:rule_meta` -- the `tags` list on each rule row (provenance)
  * `global:tag_rules:<tag>` -- the tag -> rule-ids index, merged into the new
    key server-side and the old key dropped

What it does NOT rewrite: the tags already on files and functions in a
collection. Those live in the per-collection docs and index buckets, and
`scripts/migrate_tag_taxonomy.py` already knows how to move them (it drives the
real indexer, so the ancestor expansion cannot drift). Run this script first,
then that one per collection:

    uv run python scripts/migrate_rulezet_tags.py            # dry run
    uv run python scripts/migrate_rulezet_tags.py --apply
    uv run python scripts/migrate_tag_taxonomy.py --collection testMirai9
"""

import argparse
import json
import logging

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_provenance import RULE_META_KEY, TAG_RULES_PREFIX
from bsimvis.app.services.tag_taxonomy import migrate_tag

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("migrate_rulezet_tags")

BATCH = 500


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def migrate_tags(tags):
    """Tag list -> migrated tag list, sorted and deduped."""
    out = set()
    for tag in tags or []:
        out.update(migrate_tag(tag))
    return sorted(out)


def migrate_sidecar(apply=False):
    """Rewrite `tags.json`. Returns (rules touched, distinct old ids seen)."""
    from bsimvis.app.services.rulezet_service import paths

    p = paths()["tags"]
    if not p.exists():
        log.info("no tag sidecar at %s -- nothing to migrate", p)
        return 0, set()

    data = json.loads(p.read_text())
    touched, old_ids = 0, set()
    for uuid, tags in data.items():
        new = migrate_tags(tags)
        if new != sorted(set(tags or [])):
            old_ids.update(t for t in tags if t not in new)
            touched += 1
            data[uuid] = new

    log.info(
        "sidecar: %d/%d rules change, %d distinct old ids", touched, len(data), len(old_ids)
    )
    if apply and touched:
        backup = p.with_suffix(".json.bak")
        if not backup.exists():
            backup.write_text(json.dumps(json.loads(p.read_text())))
            log.info("sidecar: backed up to %s", backup)
        p.write_text(json.dumps(data))
    return touched, old_ids


def migrate_rule_meta(r, apply=False):
    """Rewrite the `tags` list on every rule row. Returns rows changed."""
    changed, seen, pending = 0, 0, {}
    cursor = 0
    while True:
        cursor, batch = r.hscan(RULE_META_KEY, cursor, count=BATCH)
        for rid, raw in batch.items():
            seen += 1
            try:
                row = json.loads(raw)
            except ValueError:
                continue
            tags = row.get("tags")
            if not tags:
                continue
            new = migrate_tags(tags)
            if new == sorted(set(tags)):
                continue
            row["tags"] = new
            pending[_s(rid)] = json.dumps(row)
            changed += 1
        if pending and apply:
            r.hset(RULE_META_KEY, mapping=pending)
            pending = {}
        if cursor == 0:
            break
    if pending and apply:
        r.hset(RULE_META_KEY, mapping=pending)
    log.info("rule_meta: %d/%d rows change", changed, seen)
    return changed


def migrate_tag_index(r, apply=False):
    """Merge each old `tag_rules` key into its new one. Returns keys moved.

    Merged with SUNIONSTORE rather than by reading the members back: a broad
    mirror tag holds tens of thousands of rule ids, and the union is the
    server's job.
    """
    moved = 0
    cursor = 0
    pattern = TAG_RULES_PREFIX + "rulezet:*"
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=BATCH)
        for key in keys:
            key = _s(key)
            old_tag = key[len(TAG_RULES_PREFIX) :]
            new = migrate_tag(old_tag)
            if new == [old_tag]:
                continue  # a rule name -- already the current form
            for new_tag in new:
                new_key = TAG_RULES_PREFIX + new_tag
                log.info("tag_rules: %s -> %s (%d ids)", old_tag, new_tag, r.scard(key))
                if apply:
                    r.sunionstore(new_key, [new_key, key])
            if apply:
                r.delete(key)
            moved += 1
        if cursor == 0:
            break
    log.info("tag_rules: %d keys moved", moved)
    return moved


def demo():
    """The rules that only fail for real: which ids move, and idempotence."""
    assert migrate_tags(["rulezet:ms-caro-malware-full:malware-platform:linux"]) == [
        "ms-caro-malware-full:malware-platform:linux"
    ]
    # A rule name is the current form and must survive untouched, or the whole
    # ruleset axis empties itself on the first run.
    assert migrate_tags(["rulezet:ELF_Toriilike_persist"]) == [
        "rulezet:ELF_Toriilike_persist"
    ]
    # Namespaces that were already routed are left alone.
    kept = ["cve:cve-2021-44228", "misp:tool:cobalt-strike", "yara:trojan:mirai:R"]
    assert migrate_tags(kept) == sorted(kept)
    # Idempotent: a re-run after an interruption is a no-op.
    once = migrate_tags(["rulezet:runtime-packer:pe:upx", "rulezet:Some_Rule"])
    assert migrate_tags(once) == once, once

    class FakeRedis:
        """Enough of the API for the two storage passes."""

        def __init__(self):
            self.h, self.s = {}, {}

        def hscan(self, k, cursor, count=None):
            return 0, dict(self.h.get(k, {}))

        def hset(self, k, mapping=None):
            self.h.setdefault(k, {}).update(mapping or {})

        def scan(self, cursor, match=None, count=None):
            prefix = match.rstrip("*")
            return 0, [k for k in list(self.s) if k.startswith(prefix)]

        def scard(self, k):
            return len(self.s.get(k, ()))

        def sunionstore(self, dest, keys):
            out = set()
            for k in keys:
                out |= self.s.get(k, set())
            self.s[dest] = out

        def delete(self, k):
            self.s.pop(k, None)

    r = FakeRedis()
    old = "rulezet:ms-caro-malware-full:malware-platform:linux"
    r.h[RULE_META_KEY] = {
        "u1": json.dumps({"source": "rulezet", "tags": [old, "rulezet:Some_Rule"]})
    }
    r.s[TAG_RULES_PREFIX + old] = {"u1", "u2"}
    # The new key may already exist from a post-fix sync -- the merge must add
    # to it, not replace it.
    r.s[TAG_RULES_PREFIX + "ms-caro-malware-full:malware-platform:linux"] = {"u3"}

    assert migrate_rule_meta(r, apply=True) == 1
    assert json.loads(r.h[RULE_META_KEY]["u1"])["tags"] == [
        "ms-caro-malware-full:malware-platform:linux",
        "rulezet:Some_Rule",
    ], r.h[RULE_META_KEY]["u1"]
    assert migrate_tag_index(r, apply=True) == 1
    assert r.s[TAG_RULES_PREFIX + "ms-caro-malware-full:malware-platform:linux"] == {
        "u1",
        "u2",
        "u3",
    }
    assert TAG_RULES_PREFIX + old not in r.s
    assert migrate_rule_meta(r, apply=True) == 0, "second pass must be a no-op"
    assert migrate_tag_index(r, apply=True) == 0, "second pass must be a no-op"
    print("migrate_rulezet_tags demo OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write (default: dry run)")
    ap.add_argument("--demo", action="store_true", help="run the rule checks and exit")
    args = ap.parse_args()

    if args.demo:
        demo()
        return

    if not args.apply:
        log.info("DRY RUN -- nothing is written. Re-run with --apply.")

    migrate_sidecar(apply=args.apply)
    r = get_redis()
    migrate_rule_meta(r, apply=args.apply)
    migrate_tag_index(r, apply=args.apply)

    if args.apply:
        log.info(
            "done. Per-collection file/function tags still carry old ids -- run "
            "scripts/migrate_tag_taxonomy.py --collection <name> for each."
        )


if __name__ == "__main__":
    main()
