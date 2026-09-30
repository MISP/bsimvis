"""Re-derive boilerplate tags from stored function names; no Ghidra, no re-analysis.

Boilerplate tags are file-gated (plain libc names only tag in a static-libc file),
so this walks file by file. Other tag namespaces are left alone.

Preview: ``uv run python scripts/backfill_boilerplate_tags.py --collection main``
Apply:   ``uv run python scripts/backfill_boilerplate_tags.py --collection main --apply``
One file: add ``--md5 <md5>``. After applying, rebuild the file similarities.
"""

import argparse
import json
from itertools import islice

from backfill_function_id_tags import BATCH, metas, sync_file

from bsimvis.app.services.bin_sim_tags import bump_tags_rev
from bsimvis.app.services.boilerplate_tag_service import boilerplate_tags_for_names
from bsimvis.app.services.index_service import _unindex_tag, save_function
from bsimvis.app.services.redis_client import get_redis

PREFIX = "boilerplate:"


def file_metas(r, collection, md5):
    ids = r.sscan_iter(f"{collection}:idx:file:functions:{md5}", count=BATCH)
    found = []
    while batch := list(islice(ids, BATCH)):
        found.extend(metas(r, batch))
    return found


def backfill_file(r, collection, md5, apply):
    found = file_metas(r, collection, md5)
    wanted = boilerplate_tags_for_names(m.get("function_name") for _, m in found)
    changes = {}
    for fid, meta in found:
        old = list(meta.get("tags") or [])
        tag = wanted.get(meta.get("function_name"))
        new = [t for t in old if not str(t).startswith(PREFIX)] + ([tag] if tag else [])
        if new == old:
            continue
        changes[fid] = new
        if apply:
            meta["tags"] = new
            pipe = r.pipeline(transaction=False)
            _unindex_tag(pipe, collection, "func", "tags", old, fid)
            pipe.set(f"{fid}:meta", json.dumps(meta))
            save_function(pipe, collection, md5, fid.rsplit(":", 1)[-1], meta)
            pipe.execute()
    file_changed = sync_file(r, collection, md5, changes, apply, PREFIX)
    return changes, file_changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", action="append", required=True)
    parser.add_argument("--md5", action="append", help="limit to these files")
    parser.add_argument("--apply", action="store_true", help="write (default: preview)")
    args = parser.parse_args()
    r = get_redis()
    for collection in args.collection:
        md5s = args.md5 or [
            i.rsplit(":", 1)[-1] for i in r.sscan_iter(f"{collection}:all_files")
        ]
        funcs = files = 0
        for md5 in md5s:
            changes, file_changed = backfill_file(r, collection, md5, args.apply)
            funcs += len(changes)
            files += bool(file_changed)
        if args.apply and funcs:
            bump_tags_rev(r, collection)
            for pool in r.smembers(f"{collection}:pools"):
                bump_tags_rev(r, f"global:pool:{pool}")
        verb = "updated" if args.apply else "would update"
        print(f"{collection}: {verb} {funcs} function(s), {files} file(s)")


if __name__ == "__main__":
    main()
