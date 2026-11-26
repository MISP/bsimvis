"""Download samples from MalwareBazaar or MWDB, then optionally upload them.

  bsimvis fetch malwarebazaar -o samples/ tag:Mirai -n 20 -c main
  bsimvis fetch mwdb -o samples/ hashes.txt
  echo '"md5a","md5b"' | bsimvis fetch mb -o samples/ -   (mb = malwarebazaar)

Inputs (any mix): `tag:NAME`, a file path, `-` for stdin, or raw text. Hashes
(md5 or sha256) are pulled out by regex, so any separator or quoting works.
Keys come from the environment or .env, never from the command line:
MB_API_KEY (abuse.ch Auth-Key), MWDB_API_KEY, MWDB_URL (default mwdb.cert.pl).

Every sample gets a row in `<out>/metadata.csv` (the `bsimvis upload --metadata`
format: md5, file names, family, first_seen / last_seen), so `-c COLLECTION`
uploads them with their in-the-wild dates.
"""

import csv
import hashlib
import io
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pyzipper
import requests

HASH_RE = re.compile(
    r"(?<![0-9a-fA-F])(?:[0-9a-fA-F]{64}|[0-9a-fA-F]{32})(?![0-9a-fA-F])"
)
MB_URL = "https://mb-api.abuse.ch/api/v1/"
META_COLS = ["HASH", "names", "avtype", "first_seen", "last_seen"]
SOURCE_TAG = {"malwarebazaar": "source:malwarebazaar", "mwdb": "source:mwdb"}


def parse_inputs(items):
    """Split inputs into (hashes, tags). Order kept, duplicates dropped."""
    hashes, tags = {}, []
    for item in items:
        if item.startswith("tag:"):
            tags.append(item[4:].strip().strip("\"'"))
            continue
        if item == "-":
            item = sys.stdin.read()
        elif os.path.isfile(item):
            item = Path(item).read_text()
        for h in HASH_RE.findall(item):
            hashes[h.lower()] = None
    return list(hashes), tags


def _call(fn, *a, **kw):
    """One HTTP call, retried with backoff on 429 / 5xx (both APIs rate-limit)."""
    for wait in (5, 15, 45, 90, None):
        r = fn(*a, **kw)
        if wait is None or (r.status_code != 429 and r.status_code < 500):
            r.raise_for_status()
            return r
        print(f"HTTP {r.status_code}, retrying in {wait}s", file=sys.stderr)
        time.sleep(wait)


def _cell(v):
    """One CSV cell: `|` is the delimiter and `,` splits lists on upload."""
    return re.sub(r"[|,\r\n]", "_", str(v or "").strip()) or "-"


class Bazaar:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["Auth-Key"] = os.environ["MB_API_KEY"]

    def _post(self, **data):
        return _call(self.s.post, MB_URL, data=data, timeout=120)

    def tag(self, name, limit=None):
        n = min(limit or 1000, 1000)
        j = self._post(query="get_taginfo", tag=name, limit=n).json()
        if j.get("query_status") == "ok":
            return [d["sha256_hash"] for d in j["data"]]
        # Big tags (Mirai, ...) time out server-side; the family signature does not.
        # The public CSV dumps that used to back this are gone (HTTP 410).
        print(
            f"tag {name}: {j.get('data') or j.get('query_status')}, trying signature",
            file=sys.stderr,
        )
        j = self._post(query="get_siginfo", signature=name, limit=n).json()
        if j.get("query_status") == "ok":
            return [d["sha256_hash"] for d in j["data"]]
        raise RuntimeError(f"tag {name}: {j}")

    def info(self, h):
        j = self._post(query="get_info", hash=h).json()
        if j.get("query_status") != "ok":
            raise RuntimeError(f"unknown hash ({j.get('query_status')})")
        d = j["data"][0]
        return {
            "names": d.get("file_name"),
            "avtype": d.get("signature"),
            "first_seen": d.get("first_seen"),  # "YYYY-MM-DD HH:MM:SS", UTC
            "last_seen": d.get("last_seen"),
        }, d["sha256_hash"]

    def fetch(self, h):
        """-> (file name, bytes, metadata row)."""
        meta, sha = self.info(h)  # get_file only takes sha256
        r = self._post(query="get_file", sha256_hash=sha)
        if r.headers.get("content-type", "").startswith("application/json"):
            raise RuntimeError(r.json().get("query_status"))
        with pyzipper.AESZipFile(io.BytesIO(r.content)) as z:
            z.setpassword(b"infected")
            return sha, z.read(z.namelist()[0]), meta


class Mwdb:
    def __init__(self):
        self.base = os.environ.get("MWDB_URL", "https://mwdb.cert.pl").rstrip("/")
        self.s = requests.Session()
        self.s.headers["Authorization"] = "Bearer " + os.environ["MWDB_API_KEY"]

    def _get(self, path, **params):
        return _call(self.s.get, f"{self.base}/api/{path}", params=params, timeout=300)

    def tag(self, name, limit=None):
        out, older = [], None
        while True:  # keyset pagination: `older_than` = last id seen
            params = {"query": f'tag:"{name}"'}
            if older:
                params["older_than"] = older
            files = self._get("file", **params).json()["files"]
            if not files or (limit and len(out) >= limit):
                return out
            out += [f["sha256"] for f in files]
            older = files[-1]["id"]

    def info(self, h):
        d = self._get(f"file/{h}").json()
        # Plain tags are families (`mirai`); `yara:`, `runnable:`, `feed:` ... are not.
        fam = [t["tag"] for t in d.get("tags", []) if ":" not in t["tag"]]
        return {
            "names": d.get("file_name"),
            "avtype": fam[0] if fam else None,
            # MWDB has no sighting dates: its upload time is the first sighting.
            "first_seen": d.get("upload_time"),
            "last_seen": None,
        }, d["sha256"]

    def fetch(self, h):
        meta, sha = self.info(h)
        return sha, self._get(f"file/{sha}/download").content, meta


def read_meta(path):
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return {r["HASH"]: r for r in csv.DictReader(f, delimiter="|")}


def write_meta(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, META_COLS, delimiter="|", lineterminator="\n")
        w.writeheader()
        w.writerows(rows.values())


def meta_row(md5, meta):
    return {"HASH": md5, **{k: _cell(meta.get(k)) for k in META_COLS[1:]}}


def upload(files, args, meta_csv, host):
    """Hand the files to `bsimvis upload`, in batches to stay under argv limits."""
    base = [sys.executable, "-m", "bsimvis.cli.main", "upload", "-H", host]
    base += ["-c", args.collection, "--metadata", str(meta_csv)]
    for t in [SOURCE_TAG[args.source]] + args.tag:
        base += ["-t", t]
    if args.skip_sim:
        base.append("--skip-sim")
    for i in range(0, len(files), 200):
        part = files[i : i + 200]
        print(f"uploading {i + 1}..{i + len(part)}/{len(files)}", file=sys.stderr)
        r = subprocess.run(base + [str(f) for f in part])
        if r.returncode:
            return r.returncode
    return 0


def run_fetch(api_host, args):
    if args.source == "mb":
        args.source = "malwarebazaar"
    if ":" not in api_host:
        api_host += ":5000"
    key = "MB_API_KEY" if args.source == "malwarebazaar" else "MWDB_API_KEY"
    if not os.environ.get(key):
        print(f"{key} not set (put it in .env)", file=sys.stderr)
        return 2
    client = Bazaar() if args.source == "malwarebazaar" else Mwdb()

    hashes, tags = parse_inputs(args.inputs)
    for t in tags:
        found = client.tag(t, args.limit)[: args.limit]
        print(f"tag:{t} -> {len(found)} samples", file=sys.stderr)
        hashes += [h for h in found if h not in hashes]
    if args.list:
        print("\n".join(hashes))
        return 0

    args.out.mkdir(parents=True, exist_ok=True)
    meta_csv = args.out / "metadata.csv"
    rows = read_meta(meta_csv)
    on_disk = {}  # md5 and sha256 of every sample already downloaded
    for p in args.out.iterdir():
        if p.is_file() and p != meta_csv:
            data = p.read_bytes()
            on_disk[hashlib.md5(data).hexdigest()] = p
            on_disk[hashlib.sha256(data).hexdigest()] = p
    failed, files = 0, []
    for i, h in enumerate(hashes, 1):
        have = on_disk.get(h)
        try:
            if have:
                data, name = have.read_bytes(), have.name
                md5 = hashlib.md5(data).hexdigest()
                if md5 not in rows:  # downloaded by an older run: no metadata yet
                    rows[md5] = meta_row(md5, client.info(h)[0])
            else:
                name, data, meta = client.fetch(h)
                md5 = hashlib.md5(data).hexdigest()
                (args.out / name).write_bytes(data)
                rows[md5] = meta_row(md5, meta)
                print(f"[{i}/{len(hashes)}] {name} {len(data)}B", file=sys.stderr)
            if args.out / name not in files:  # md5 and sha256 of one sample
                files.append(args.out / name)
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(hashes)}] {h} FAILED: {e}", file=sys.stderr)
    write_meta(meta_csv, rows)
    print(
        f"{len(files)} samples, {failed} failed, metadata: {meta_csv}", file=sys.stderr
    )
    if args.collection and files:
        rc = upload(files, args, meta_csv, api_host)
        if rc:
            return rc
    return 1 if failed else 0


def add_parser(subparsers):
    p = subparsers.add_parser(
        "fetch", help="Download samples from MalwareBazaar or MWDB (and upload them)"
    )
    p.add_argument("source", choices=["malwarebazaar", "mb", "mwdb"])
    p.add_argument("inputs", nargs="+", help="tag:NAME, hash list file, - or hashes")
    p.add_argument("-o", "--out", required=True, type=Path)
    p.add_argument("--list", action="store_true", help="print hashes, download nothing")
    p.add_argument("-c", "--collection", help="then `bsimvis upload` into it")
    p.add_argument(
        "-t", "--tag", action="append", default=[], help="extra upload tag (repeatable)"
    )
    p.add_argument("-n", "--limit", type=int, help="max samples per tag")
    p.add_argument(
        "-H", "--host", dest="hosts", action="append", help="BSimVis API host[:port]"
    )
    p.add_argument("--skip-sim", action="store_true", help="upload with --skip-sim")


def _selfcheck():
    m, s = "a" * 32, "B" * 64
    text = f'"{m}", {s};\n{m}\t{"c" * 33}'  # 33 hex chars: not a hash
    got, tags = parse_inputs([text, "tag:Mirai", 'tag:"Gafgyt"'])
    assert got == [m, s.lower()] and tags == ["Mirai", "Gafgyt"], (got, tags)
    row = meta_row(m, {"names": "a|b,c.elf", "first_seen": "2026-11-10 01:02:03"})
    assert row["names"] == "a_b_c.elf" and row["last_seen"] == "-", row


if __name__ == "__main__":
    _selfcheck()
    print("ok")
