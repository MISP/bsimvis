"""Backfill the `entry:process` tag and `entry_point` onto already-analysed files.

Files analysed before entry-point tagging existed have neither. This reads each
file's stored `{coll}:file:{md5}:raw` bytes, finds the header entry point with
LIEF and tags the stored function whose body contains it. Pure Kvrocks pass: no
Ghidra, no re-ingest. Files without stored raw bytes are skipped, and so is an
entry stub that the min_func_len filter never stored.

Idempotent -- a file that already has `entry_point` is left alone.

    uv run python scripts/backfill_entrypoints.py --collection main
"""

import argparse
import logging

from bsimvis.app.services.entrypoint_service import backfill_entrypoints
from bsimvis.app.services.redis_client import get_raw_redis, get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--collection", required=True)
    args = ap.parse_args()

    r, r_raw = get_redis(), get_raw_redis()
    cursor, seen, tagged = "0", 0, 0
    while True:
        cursor, n, t = backfill_entrypoints(r, r_raw, args.collection, cursor)
        seen, tagged = seen + n, tagged + t
        if cursor == "0":
            break
    logging.info(f"[+] Tagged the entry point on {tagged}/{seen} files.")


if __name__ == "__main__":
    main()
