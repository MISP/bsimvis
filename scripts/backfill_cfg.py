"""Queue a re-analysis for files whose functions have no CFG.

`cfg`, `tok_block` and the per-feature `block_index` come from the live Ghidra
decompiler (`ghidra_service.stream_bsim_data`), so they cannot be rebuilt from
stored data. This finds files whose functions lack `cfg` in `{fid}:source` and
queues a plain GHIDRA_ANALYZE with `skip_sim` and every optional module skipped
(tags, YARA, capa already exist; similarities are untouched), one lane unit per
file, the same job the scan-commit top-up queues.

Files whose raw bytes are gone cannot be re-analysed and are reported.

    uv run python scripts/backfill_cfg.py --collection main --dry-run
    uv run python scripts/backfill_cfg.py --collection main --limit 1
    uv run python scripts/backfill_cfg.py --all
"""

import argparse
import json
import logging

from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_raw_redis, get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def has_cfg(r, collection, md5):
    """True when any function of the file has a cfg. None: nothing to inspect."""
    ids = list(r.sscan_iter(f"{collection}:idx:file:functions:{md5}", count=50))
    if not ids:
        return None
    # ponytail: a file is all-or-nothing (one analysis run), so a few probes
    # decide it. A decompile that failed has no cfg either, so probe several.
    for fid in ids[:20]:
        fid = _s(fid)
        fid = fid[: -len(":meta")] if fid.endswith(":meta") else fid
        raw = r.get(f"{fid}:source")
        if not raw:
            continue
        src = json.loads(raw)
        if isinstance(src, list):
            src = src[0] if src else {}
        if src.get("cfg"):
            return True
    return False


def backfill(collection, js, dry_run=False, limit=None):
    r, raw = get_redis(), get_raw_redis()
    queued = have = no_raw = empty = 0
    for base in sorted(_s(b) for b in r.smembers(f"{collection}:all_files")):
        md5 = base.rsplit(":", 1)[-1]
        state = has_cfg(r, collection, md5)
        if state is None:
            empty += 1
            continue
        if state:
            have += 1
            continue
        raw_id = f"{collection}:file:{md5}:raw"
        if not raw.exists(raw_id):
            no_raw += 1
            logging.warning(f"[!] {md5}: raw binary missing, cannot re-analyse")
            continue
        if limit is not None and queued >= limit:
            break
        meta = json.loads(r.get(f"{base}:meta") or "{}")
        queued += 1
        if dry_run:
            continue
        job = js.create_job(
            JobType.GHIDRA_ANALYZE,
            {
                "collection": collection,
                "raw_file_id": raw_id,
                "file_md5": md5,
                "file_name": meta.get("file_name", "unknown"),
                "batch_uuid": meta.get("batch_uuid"),
                "batch_name": meta.get("batch_name"),
                "skip_sim": True,
                "skip_function_id": True,
                "skip_capa": True,
                "skip_yara": True,
                "skip_rulezet": True,
                "skip_boilerplate": True,
            },
            enqueue=False,
        )
        js.submit_to_lane(collection, job)
    logging.info(
        f"{collection}: {'would queue' if dry_run else 'queued'} {queued}, "
        f"already have cfg {have}, no raw {no_raw}, no functions {empty}"
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, queue nothing")
    ap.add_argument("--limit", type=int, help="queue at most N files per collection")
    args = ap.parse_args()

    if args.all:
        collections = sorted(_s(c) for c in get_redis().smembers("global:collections"))
    elif args.collection:
        collections = args.collection
    else:
        ap.error("pass --collection NAME (repeatable) or --all")

    js = JobService()
    for c in collections:
        backfill(c, js, dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
