"""Download samples from MalwareBazaar or MWDB into a directory.

  uv run scripts/fetch_samples.py mb   -o samples/ tag:Mirai
  uv run scripts/fetch_samples.py mwdb -o samples/ hashes.txt
  echo '"md5a","md5b"' | uv run scripts/fetch_samples.py mb -o samples/ -

Inputs (any mix): `tag:NAME`, a file path, `-` for stdin, or raw text. Hashes
(md5 or sha256) are pulled out by regex, so any separator or quoting works.
Keys (env or .env): MB_API_KEY (abuse.ch Auth-Key), MWDB_API_KEY, MWDB_URL (default mwdb.cert.pl).
Add `-c COLLECTION [-t TAG ...]` to also run `bsimvis upload` on the samples.
"""

import argparse
import csv
import io
import time
import zipfile
import os
import re
import subprocess
import sys
from pathlib import Path

import pyzipper
import requests
from dotenv import load_dotenv

HASH_RE = re.compile(
    r"(?<![0-9a-fA-F])(?:[0-9a-fA-F]{64}|[0-9a-fA-F]{32})(?![0-9a-fA-F])"
)
MB_URL = "https://mb-api.abuse.ch/api/v1/"
MB_DUMP_URL = "https://bazaar.abuse.ch/export/csv/full/"


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


class Bazaar:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers["Auth-Key"] = os.environ["MB_API_KEY"]

    def _post(self, **data):
        r = self.s.post(MB_URL, data=data, timeout=120)
        r.raise_for_status()
        return r

    def tag(self, name, limit=None):
        j = self._post(query="get_taginfo", tag=name, limit=1000).json()
        if j.get("query_status") == "ok":
            return [d["sha256_hash"] for d in j["data"]]
        if "exceeded" in str(j.get("data")):  # big tags time out server-side
            print(f"tag {name}: API timeout, using bulk dump", file=sys.stderr)
            return self.dump_signature(name)
        raise RuntimeError(f"tag {name}: {j}")

    def dump_signature(self, name):
        """Full public CSV dump, filtered on `signature` (family). It has no tags column."""
        cache = Path.home() / ".cache/bsimvis/mb_full.zip"
        if not cache.exists() or time.time() - cache.stat().st_mtime > 86400:
            cache.parent.mkdir(parents=True, exist_ok=True)
            print("downloading MalwareBazaar full dump (~220MB)", file=sys.stderr)
            with requests.get(MB_DUMP_URL, stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(cache, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
        with zipfile.ZipFile(cache) as z, z.open(z.namelist()[0]) as f:
            lines = (l.decode() for l in f if not l.startswith(b"#"))
            rows = csv.reader(lines, skipinitialspace=True)
            return [r[1] for r in rows if len(r) > 8 and r[8].lower() == name.lower()]

    def fetch(self, h):
        if len(h) == 32:  # get_file only takes sha256
            j = self._post(query="get_info", hash=h).json()
            if j.get("query_status") != "ok":
                raise RuntimeError(f"unknown md5 ({j.get('query_status')})")
            h = j["data"][0]["sha256_hash"]
        r = self._post(query="get_file", sha256_hash=h)
        if r.headers.get("content-type", "").startswith("application/json"):
            raise RuntimeError(r.json().get("query_status"))
        with pyzipper.AESZipFile(io.BytesIO(r.content)) as z:
            z.setpassword(b"infected")
            return h, z.read(z.namelist()[0])


class Mwdb:
    def __init__(self):
        self.base = os.environ.get("MWDB_URL", "https://mwdb.cert.pl").rstrip("/")
        self.s = requests.Session()
        self.s.headers["Authorization"] = "Bearer " + os.environ["MWDB_API_KEY"]

    def _get(self, path, **params):
        r = self.s.get(f"{self.base}/api/{path}", params=params, timeout=300)
        r.raise_for_status()
        return r

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

    def fetch(self, h):  # /file/<any hash> resolves to the sample
        return h, self._get(f"file/{h}/download").content


def upload(files, collection, tags, host=None):
    """Hand the files to `bsimvis upload`, in batches to stay under argv limits."""
    exe = Path(sys.executable).parent / "bsimvis"
    base = [str(exe), "upload", "-c", collection]
    for t in tags:
        base += ["-t", t]
    if host:
        base += ["-H", host]
    for i in range(0, len(files), 200):
        print(
            f"uploading {i + 1}..{i + len(files[i:i + 200])}/{len(files)}",
            file=sys.stderr,
        )
        subprocess.run(base + [str(f) for f in files[i : i + 200]], check=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("source", choices=["mb", "mwdb"])
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("-o", "--out", required=True, type=Path)
    ap.add_argument(
        "--list", action="store_true", help="print hashes, download nothing"
    )
    ap.add_argument(
        "-c", "--collection", help="then `bsimvis upload` into this collection"
    )
    ap.add_argument(
        "-t", "--tag", action="append", default=[], help="upload tag (repeatable)"
    )
    ap.add_argument("-H", "--host", help="bsimvis host:port for the upload")
    ap.add_argument("-n", "--limit", type=int, help="max samples per tag")
    args = ap.parse_args()
    load_dotenv()  # MB_API_KEY etc. from .env

    key = "MB_API_KEY" if args.source == "mb" else "MWDB_API_KEY"
    if key not in os.environ:
        sys.exit(f"{key} not set")
    client = Bazaar() if args.source == "mb" else Mwdb()

    hashes, tags = parse_inputs(args.inputs)
    for t in tags:
        found = client.tag(t, args.limit)[: args.limit]
        print(f"tag:{t} -> {len(found)} samples", file=sys.stderr)
        hashes += [h for h in found if h not in hashes]
    if args.list:
        print("\n".join(hashes))
        return

    args.out.mkdir(parents=True, exist_ok=True)
    failed, files = 0, []
    for i, h in enumerate(hashes, 1):
        have = next(args.out.glob(f"{h}*"), None)
        if have:
            files.append(have)
            continue
        try:
            name, data = client.fetch(h)
            (args.out / name).write_bytes(data)
            files.append(args.out / name)
            print(f"[{i}/{len(hashes)}] {name} {len(data)}B", file=sys.stderr)
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(hashes)}] {h} FAILED: {e}", file=sys.stderr)
    if args.collection and files:
        upload(files, args.collection, args.tag, args.host)
    sys.exit(1 if failed else 0)


def _selfcheck():
    m, s = "a" * 32, "B" * 64
    text = f'"{m}", {s};\n{m}\t{"c" * 33}'  # 33 hex chars: not a hash
    got, tags = parse_inputs([text, "tag:Mirai", 'tag:"Gafgyt"'])
    assert got == [m, s.lower()] and tags == ["Mirai", "Gafgyt"], (got, tags)


if __name__ == "__main__":
    _selfcheck()
    main()
