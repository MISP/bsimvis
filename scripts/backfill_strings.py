"""Backfill strings onto collections analysed before string extraction existed.

Ghidra defined the strings at analysis time and that data was never stored, but
the decompiled source every function keeps still holds the literals it uses
(`"Sum: %d\\n"` is a token). This reads those tokens and writes them with the
same code path the analyze job uses (`string_service.index_file_strings`), so
the Strings pages, pivots, `strings=` search and the N-diff Strings facet all
work. Pure Kvrocks pass: no Ghidra, no re-ingest, no binary needed.

What it cannot recover: strings the decompiler shows as `DAT_...` or never
prints, strings no function uses, the real address/section, and YARA hits on
strings. Backfilled rows carry section `decompiler` and the address
`decomp::{id}`, so a later real analysis can tell them apart and replace them.

A file that already has strings is skipped (never overwrite a real extraction)
unless --force.

    uv run python scripts/backfill_strings.py --collection main --dry-run
    uv run python scripts/backfill_strings.py --collection main
    uv run python scripts/backfill_strings.py --all
"""

import argparse
import json
import logging
import re

from tqdm import tqdm

from bsimvis.app.services import string_service
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.redis_client import get_redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

# Sources are large token lists, so fetch them a slice at a time.
BATCH = 100
SECTION = "decompiler"
_ESC = re.compile(r"\\(x[0-9a-fA-F]{2}|[0-7]{1,3}|.)", re.S)
_SIMPLE = {"n": 10, "t": 9, "r": 13, "a": 7, "b": 8, "f": 12, "v": 11}


def _s(v):
    return v.decode() if isinstance(v, bytes) else v


def unescape(body):
    """C escapes of a decompiler literal -> the string it spells."""
    out = bytearray()
    pos = 0
    for m in _ESC.finditer(body):
        out += body[pos : m.start()].encode("utf-8")
        e = m.group(1)
        if e[0] == "x":
            out.append(int(e[1:], 16))
        elif e[0] in "01234567":
            out.append(int(e, 8) & 0xFF)
        else:
            out.append(_SIMPLE.get(e, ord(e[0]) if len(e) == 1 else 63))
        pos = m.end()
    out += body[pos:].encode("utf-8")
    return out.decode("utf-8", "replace")


def literals(source, min_len):
    """{value: encoding} of the string literals in one function's source JSON."""
    found = {}
    for tok in source.get("c_tokens", []):
        t = tok.get("t")
        if tok.get("type") != "variable" or not isinstance(t, str):
            continue
        wide = t.startswith('L"')
        if not (t.endswith('"') and (t.startswith('"') or wide)):
            continue
        value = unescape(t[2:-1] if wide else t[1:-1])
        if len(value) < min_len:
            continue
        enc = "utf-16" if wide else ("ascii" if value.isascii() else "utf-8")
        found.setdefault(value, enc)
    return found


def backfill_file(r, coll, md5, entries, min_len, cap, dry_run):
    """Index one file's strings from the sources of its functions. -> count."""
    table = {}  # value -> [encoding, set of function entry addrs]
    for start in range(0, len(entries), BATCH):
        chunk = entries[start : start + BATCH]
        pipe = r.pipeline(transaction=False)
        for entry in chunk:
            pipe.get(f"{coll}:func:{md5}:{entry}:source")
        for entry, raw in zip(chunk, pipe.execute()):
            if not raw:
                continue
            try:
                source = json.loads(_s(raw))
            except ValueError:
                logging.warning(f"    {coll}:{md5}:{entry}: unreadable source")
                continue
            if isinstance(source, list):
                source = source[0] if source else {}
            for value, enc in literals(source, min_len).items():
                table.setdefault(value, [enc, set()])[1].add(entry)
    values = sorted(table)[:cap]
    if dry_run or not values:
        return len(values)
    records = []
    for value in values:
        enc, funcs = table[value]
        sid = string_service.string_id(value)
        rec = string_service.make_record(
            value, f"decomp::{sid}", enc, SECTION, funcs=funcs
        )
        records.append(rec)
    return string_service.index_file_strings(r, coll, md5, records)


def backfill(collection, dry_run=False, force=False):
    r = get_redis()
    min_len = int(config_service.get("strings.min_length", 4))
    cap = int(config_service.get("strings.max_per_file", 10000))

    # all_functions holds "{coll}:func:{md5}:{addr}"; group the addrs by file.
    files = {}
    for f in r.smembers(f"{collection}:all_functions"):
        base = _s(f)
        base = base[: -len(":meta")] if base.endswith(":meta") else base
        parts = base.split(":")
        files.setdefault(parts[-2], []).append(parts[-1])
    if not files:
        logging.warning(f"[!] No functions in collection {collection}")
        return 0

    done = skipped = total = 0
    for md5, entries in tqdm(sorted(files.items()), desc=collection, unit="file"):
        if not force and r.hlen(string_service.file_key(collection, md5)):
            skipped += 1
            continue
        total += backfill_file(
            r, collection, md5, sorted(entries), min_len, cap, dry_run
        )
        done += 1

    verb = "would write" if dry_run else "wrote"
    logging.info(
        f"[+] {collection}: {verb} {total} strings for {done} files "
        f"({skipped} skipped, already have strings)"
    )
    return total


def demo():
    assert unescape(r"Sum: %d\n") == "Sum: %d\n"
    assert unescape(r"a\x41\101\"\\") == 'aAA"\\'
    assert unescape(r"\xe2\x82\xac") == "€"
    src = {
        "c_tokens": [
            {"type": "variable", "t": '"apple"'},
            {"type": "variable", "t": '"ab"'},
            {"type": "variable", "t": 'L"wide str"'},
            {"type": "variable", "t": "DAT_001234"},
            {"type": "text", "t": '"not a variable"'},
        ]
    }
    assert literals(src, 4) == {"apple": "ascii", "wide str": "utf-16"}
    print("backfill_strings demo ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument(
        "--force", action="store_true", help="also replace files that have strings"
    )
    ap.add_argument("--demo", action="store_true", help="run the self-check and exit")
    args = ap.parse_args()

    if args.demo:
        return demo()
    if args.all:
        collections = sorted(_s(c) for c in get_redis().smembers("global:collections"))
    elif args.collection:
        collections = args.collection
    else:
        ap.error("pass --collection NAME (repeatable) or --all")

    for c in collections:
        backfill(c, dry_run=args.dry_run, force=args.force)


if __name__ == "__main__":
    main()
