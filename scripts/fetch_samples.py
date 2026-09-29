"""Download samples from MalwareBazaar or MWDB into a directory.

  uv run scripts/fetch_samples.py mb   -o samples/ tag:Mirai
  uv run scripts/fetch_samples.py mwdb -o samples/ hashes.txt
  echo '"md5a","md5b"' | uv run scripts/fetch_samples.py mb -o samples/ -

Inputs (any mix): `tag:NAME`, a file path, `-` for stdin, or raw text. Hashes
(md5 or sha256) are pulled out by regex, so any separator or quoting works.
Keys: MB_API_KEY (abuse.ch Auth-Key), MWDB_API_KEY, MWDB_URL (default mwdb.cert.pl).
Then: `bsimvis upload` the directory.
"""

import argparse
import io
import os
import re
import sys
from pathlib import Path

import pyzipper
import requests

HASH_RE = re.compile(
    r"(?<![0-9a-fA-F])(?:[0-9a-fA-F]{64}|[0-9a-fA-F]{32})(?![0-9a-fA-F])"
)
MB_URL = "https://mb-api.abuse.ch/api/v1/"


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

    def tag(self, name):
        j = self._post(query="get_taginfo", tag=name, limit=1000).json()
        if j.get("query_status") != "ok":
            raise RuntimeError(f"tag {name}: {j.get('query_status')}")
        return [d["sha256_hash"] for d in j["data"]]

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

    def tag(self, name):
        out, older = [], None
        while True:  # keyset pagination: `older_than` = last id seen
            params = {"query": f'tag:"{name}"'}
            if older:
                params["older_than"] = older
            files = self._get("file", **params).json()["files"]
            if not files:
                return out
            out += [f["sha256"] for f in files]
            older = files[-1]["id"]

    def fetch(self, h):  # /file/<any hash> resolves to the sample
        return h, self._get(f"file/{h}/download").content


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("source", choices=["mb", "mwdb"])
    ap.add_argument("inputs", nargs="+")
    ap.add_argument("-o", "--out", required=True, type=Path)
    ap.add_argument(
        "--list", action="store_true", help="print hashes, download nothing"
    )
    args = ap.parse_args()

    key = "MB_API_KEY" if args.source == "mb" else "MWDB_API_KEY"
    if key not in os.environ:
        sys.exit(f"{key} not set")
    client = Bazaar() if args.source == "mb" else Mwdb()

    hashes, tags = parse_inputs(args.inputs)
    for t in tags:
        found = client.tag(t)
        print(f"tag:{t} -> {len(found)} samples", file=sys.stderr)
        hashes += [h for h in found if h not in hashes]
    if args.list:
        print("\n".join(hashes))
        return

    args.out.mkdir(parents=True, exist_ok=True)
    failed = 0
    for i, h in enumerate(hashes, 1):
        if any(args.out.glob(f"{h}*")):
            continue
        try:
            name, data = client.fetch(h)
            (args.out / name).write_bytes(data)
            print(f"[{i}/{len(hashes)}] {name} {len(data)}B", file=sys.stderr)
        except Exception as e:
            failed += 1
            print(f"[{i}/{len(hashes)}] {h} FAILED: {e}", file=sys.stderr)
    sys.exit(1 if failed else 0)


def _selfcheck():
    m, s = "a" * 32, "B" * 64
    text = f'"{m}", {s};\n{m}\t{"c" * 33}'  # 33 hex chars: not a hash
    got, tags = parse_inputs([text, "tag:Mirai", 'tag:"Gafgyt"'])
    assert got == [m, s.lower()] and tags == ["Mirai", "Gafgyt"], (got, tags)


if __name__ == "__main__":
    _selfcheck()
    main()
