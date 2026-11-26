"""MalwareBazaar and MWDB clients, and the per-file lookup cache.

Every call here sends a hash to a third party. Nothing in the app calls them on
its own: only an explicit user action (a Look up button, a bulk lookup job, the
`bsimvis fetch` CLI). An instance may hold TLP:RED samples.

Cache: `{coll}:file:{md5}:ext:{source}` = JSON {source, found, fetched_at, tags,
family, first_seen, last_seen, names, url, error}. Imported tags are `mb:<tag>`
/ `mwdb:<tag>` user tags written by the "ext" writer.
"""

import json
import os
import re
import sys
import time

import requests

MB_URL = "https://mb-api.abuse.ch/api/v1/"
SOURCES = ("mb", "mwdb")
TAG_PREFIX = {"mb": "mb", "mwdb": "mwdb"}
KEY_ENV = {"mb": "MB_API_KEY", "mwdb": "MWDB_API_KEY"}


def _call(fn, *a, retries=(5, 15, 45, 90), **kw):
    """One HTTP call, retried with backoff on 429 / 5xx (both APIs rate-limit)."""
    for wait in (*retries, None):
        r = fn(*a, **kw)
        if wait is None or (r.status_code != 429 and r.status_code < 500):
            r.raise_for_status()
            return r
        print(f"HTTP {r.status_code}, retrying in {wait}s", file=sys.stderr)
        time.sleep(wait)


def mwdb_base():
    return os.environ.get("MWDB_URL", "https://mwdb.cert.pl").rstrip("/")


class Bazaar:
    def __init__(self, retries=(5, 15, 45, 90)):
        self.s = requests.Session()
        self.s.headers["Auth-Key"] = os.environ["MB_API_KEY"]
        self.retries = retries

    def _post(self, **data):
        return _call(self.s.post, MB_URL, data=data, timeout=120, retries=self.retries)

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

    def raw(self, h):
        """The full get_info record, or None when MB does not know the hash."""
        j = self._post(query="get_info", hash=h).json()
        if j.get("query_status") in ("hash_not_found", "no_results"):
            return None
        if j.get("query_status") != "ok":
            raise RuntimeError(f"MalwareBazaar: {j.get('query_status')}")
        return j["data"][0]

    def info(self, h):
        d = self.raw(h)
        if d is None:
            raise RuntimeError("unknown hash (hash_not_found)")
        return {
            "names": d.get("file_name"),
            "avtype": d.get("signature"),
            "first_seen": d.get("first_seen"),  # "YYYY-MM-DD HH:MM:SS", UTC
            "last_seen": d.get("last_seen"),
        }, d["sha256_hash"]

    def fetch(self, h):
        """-> (sha256, bytes, metadata row)."""
        import io

        import pyzipper

        meta, sha = self.info(h)  # get_file only takes sha256
        r = self._post(query="get_file", sha256_hash=sha)
        if r.headers.get("content-type", "").startswith("application/json"):
            raise RuntimeError(r.json().get("query_status"))
        with pyzipper.AESZipFile(io.BytesIO(r.content)) as z:
            z.setpassword(b"infected")
            return sha, z.read(z.namelist()[0]), meta


class Mwdb:
    def __init__(self, retries=(5, 15, 45, 90)):
        self.base = mwdb_base()
        self.s = requests.Session()
        self.s.headers["Authorization"] = "Bearer " + os.environ["MWDB_API_KEY"]
        self.retries = retries

    def _get(self, path, **params):
        return _call(
            self.s.get,
            f"{self.base}/api/{path}",
            params=params,
            timeout=300,
            retries=self.retries,
        )

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

    def raw(self, h):
        """The file record, or None when MWDB does not know the hash."""
        try:
            return self._get(f"file/{h}").json()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

    def info(self, h):
        d = self.raw(h)
        if d is None:
            raise RuntimeError("unknown hash (404)")
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


def _tag_value(t):
    return re.sub(r"\s+", "-", str(t).strip().lower())


def summarize(source, d):
    """A raw API record -> the cached card fields (pure, checked in demo())."""
    if d is None:
        return {"found": False, "tags": []}
    if source == "mb":
        tags = list(d.get("tags") or [])
        if d.get("signature"):
            tags.insert(0, d["signature"])
        return {
            "found": True,
            "family": d.get("signature"),
            "names": [d["file_name"]] if d.get("file_name") else [],
            "first_seen": d.get("first_seen"),
            "last_seen": d.get("last_seen"),
            "file_type": d.get("file_type"),
            "url": f"https://bazaar.abuse.ch/sample/{d.get('sha256_hash')}/",
            "tags": list(dict.fromkeys(_tag_value(t) for t in tags if t)),
        }
    tags = [t["tag"] for t in d.get("tags") or [] if t.get("tag")]
    fam = [t for t in tags if ":" not in t]
    return {
        "found": True,
        "family": fam[0] if fam else None,
        "names": [d["file_name"]] if d.get("file_name") else [],
        "first_seen": d.get("upload_time"),
        "last_seen": None,
        "file_type": d.get("file_type"),
        "url": f"{mwdb_base()}/file/{d.get('sha256')}",
        "tags": list(dict.fromkeys(_tag_value(t) for t in tags)),
    }


def cache_key(collection, md5, source):
    return f"{collection}:file:{md5}:ext:{source}"


def configured():
    return {s: bool(os.environ.get(KEY_ENV[s])) for s in SOURCES}


def lookup(r, collection, md5, source, sha256=None, retries=(5, 15)):
    """Ask `source` about one file, cache and return the card. Sends the hash out."""
    if source not in SOURCES:
        raise ValueError(f"unknown source {source!r}")
    if not os.environ.get(KEY_ENV[source]):
        raise RuntimeError(f"{KEY_ENV[source]} is not set on the server")
    client = (Bazaar if source == "mb" else Mwdb)(retries=retries)
    doc = {"source": source, "fetched_at": int(time.time() * 1000)}
    try:
        doc.update(summarize(source, client.raw(sha256 or md5)))
    except Exception as e:
        doc.update({"found": False, "tags": [], "error": str(e)[:300]})
    r.set(cache_key(collection, md5, source), json.dumps(doc))
    return doc


def cached(r, collection, md5):
    """{source: card} of what earlier lookups stored. No network."""
    pipe = r.pipeline(transaction=False)
    for s in SOURCES:
        pipe.get(cache_key(collection, md5, s))
    return {s: json.loads(v) for s, v in zip(SOURCES, pipe.execute()) if v}


def import_tags(collection, md5, source, card, tag_service):
    """Write a card's tags as `mb:<tag>` / `mwdb:<tag>` on the file. -> tags added."""
    added = []
    for t in card.get("tags") or []:
        tag = f"{TAG_PREFIX[source]}:{t}"
        if tag_service.add_user_tag(
            collection, "file", f"{collection}:file:{md5}", tag, writer="ext"
        ):
            added.append(tag)
    return added


def demo():
    mb = summarize(
        "mb",
        {
            "sha256_hash": "ab" * 32,
            "signature": "Mirai",
            "tags": ["mirai", "elf", "Botnet arm"],
            "file_name": "x.elf",
            "first_seen": "2026-10-02 10:00:00",
        },
    )
    assert mb["tags"] == ["mirai", "elf", "botnet-arm"], mb
    assert mb["url"].endswith("/sample/" + "ab" * 32 + "/") and mb["family"] == "Mirai"
    mw = summarize(
        "mwdb",
        {"sha256": "cd" * 32, "tags": [{"tag": "yara:win_x"}, {"tag": "mirai"}]},
    )
    assert mw["family"] == "mirai" and mw["tags"] == ["yara:win_x", "mirai"], mw
    assert summarize("mb", None) == {"found": False, "tags": []}
    print("ext_intel_service: ok")


if __name__ == "__main__":
    demo()
