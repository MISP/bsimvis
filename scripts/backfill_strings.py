"""Backfill strings onto collections analysed before string extraction existed.

Ghidra defined the strings at analysis time and that data was never stored, but
the decompiled source every function keeps still holds the literals it uses
(`"Sum: %d\\n"` is a token). This reads those tokens and writes them with the
same code path the analyze job uses (`string_service.index_file_strings`), so
the Strings pages, pivots, `strings=` search and the N-diff Strings facet all
work. Pure Kvrocks pass: no Ghidra, no re-ingest, no binary needed.

What it cannot recover: strings the decompiler shows as `DAT_...` or never
prints, and strings no function uses. A decompiler token carries the address of
the instruction that uses a literal, not of the literal itself, so a row starts
with section `decompiler` and the address `decomp::{id}`.

--locate gives those rows a real address: it finds the literal's bytes in the
stored `{coll}:file:{md5}:raw` and maps the file offset to a Ghidra address with
LIEF (the YARA workbench's `Locator`), taking the LIEF section name. The first
NUL-terminated occurrence wins, else the first one. A literal that is not in the
file (built at runtime, packed sample) keeps its `decomp::` row. `funcs` stays
"functions whose source prints it", not Ghidra's xrefs. With --locate, files that
already have a backfilled table are relocated in place (rows with a real address
are never touched), and new backfills are located as they are written.

A file that already has strings is skipped (never overwrite a real extraction)
unless --force.

    uv run python scripts/backfill_strings.py --collection main --dry-run
    uv run python scripts/backfill_strings.py --collection main
    uv run python scripts/backfill_strings.py --all
    uv run python scripts/backfill_strings.py --collection main --locate
"""

import argparse
import json
import logging
import re

from tqdm import tqdm

from bsimvis.app.services import string_service
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.redis_client import get_raw_redis, get_redis

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


def find_offset(raw, value, enc):
    """File offset of `value` in `raw`, or None. Prefers a NUL-terminated copy."""
    try:
        b = value.encode("utf-16-le" if enc == "utf-16" else "utf-8")
    except UnicodeEncodeError:
        return None
    nul = b"\0\0" if enc == "utf-16" else b"\0"
    for needle in (b + nul, b):
        off = raw.find(needle)
        if off != -1:
            return off
    return None


def locate(r, coll, md5, records):
    """Give `decomp::` records a real address and section in place. -> count."""
    from bsimvis.app.services.yara_edit_service import Locator

    raw = get_raw_redis().get(f"{coll}:file:{md5}:raw")
    if not raw:
        return 0
    loc = Locator(r, coll, md5, raw)
    loc._load()
    if loc.binary is None:
        return 0
    taken = {rec["addr"] for rec in records if not rec["addr"].startswith("decomp::")}
    n = 0
    for rec in records:
        if not rec["addr"].startswith("decomp::"):
            continue
        off = find_offset(raw, rec["value"], rec.get("encoding"))
        va = None if off is None else loc.to_ghidra(off)
        if va is None or f"{va:08x}" in taken:
            continue
        rec["addr"] = f"{va:08x}"
        taken.add(rec["addr"])
        try:
            rec["section"] = loc.binary.section_from_offset(off).name or rec["section"]
        except Exception:
            pass
        n += 1
    return n


def relocate_file(r, coll, md5, dry_run):
    """Locate an existing backfilled table and rewrite it. -> rows located."""
    table = r.hgetall(string_service.file_key(coll, md5))
    records = [json.loads(_s(v)) for k, v in table.items() if v]
    if not any(rec["addr"].startswith("decomp::") for rec in records):
        return 0
    n = locate(r, coll, md5, records)
    if n and not dry_run:
        string_service.index_file_strings(r, coll, md5, records)
    return n


def backfill_file(r, coll, md5, entries, min_len, cap, dry_run, locate_rows=False):
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
    if locate_rows:
        locate(r, coll, md5, records)
    return string_service.index_file_strings(r, coll, md5, records)


def backfill(collection, dry_run=False, force=False, locate_rows=False):
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

    done = skipped = total = located = 0
    for md5, entries in tqdm(sorted(files.items()), desc=collection, unit="file"):
        if not force and r.hlen(string_service.file_key(collection, md5)):
            if locate_rows:
                located += relocate_file(r, collection, md5, dry_run)
            skipped += 1
            continue
        total += backfill_file(
            r, collection, md5, sorted(entries), min_len, cap, dry_run, locate_rows
        )
        done += 1
    if locate_rows:
        verb = "would locate" if dry_run else "located"
        logging.info(f"[+] {collection}: {verb} {located} existing rows")

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
    raw = b"xx/proc/%d/mapsYY\0/proc/%d/maps\0w\0i\0d\0e\0\0\0"
    assert find_offset(raw, "/proc/%d/maps", "ascii") == 18  # NUL-terminated copy
    assert find_offset(raw, "wide", "utf-16") == 32
    assert find_offset(raw, "absent", "ascii") is None
    print("backfill_strings demo ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--collection", action="append", help="repeatable")
    ap.add_argument("--all", action="store_true", help="every known collection")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument(
        "--force", action="store_true", help="also replace files that have strings"
    )
    ap.add_argument(
        "--locate",
        action="store_true",
        help="give decomp:: rows a real address from the stored file bytes",
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
        backfill(c, dry_run=args.dry_run, force=args.force, locate_rows=args.locate)


if __name__ == "__main__":
    main()
