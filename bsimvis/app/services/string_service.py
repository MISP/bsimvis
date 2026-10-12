"""Strings: Ghidra-defined string literals, stored once per file and
cross-referenced to the functions that use them.

Layout (all under the origin collection; a pool reads its members' keys):

    {coll}:file:{md5}:strings              Hash  addr -> JSON string record
    {coll}:func:{md5}:{entry}:strings      Set   string addrs the function references
    {coll}:idx:string:value:{id}           Set   func ids referencing the string
    {coll}:idx:string:file:{id}            Set   file ids holding the string
    {coll}:reg:string:value / :file        Set   the bucket keys above
    {coll}:idx:func:strings:{lower value}  Set   func ids  (func search `strings=`)
    {coll}:idx:file:strings:{lower value}  Set   file ids  (file search `strings=`)
    {coll}:reg:func:strings / reg:file:strings

`id` is sha1(value)[:16]: the same literal gets the same id in every file, which
is what makes the cross-file pivot a single set lookup. The record key is the
string's address, unique within a file.

String record: {id, value, encoding, addr, section, length, tags, rules,
user_tags, funcs}. `tags`/`rules` come from the analyzers (YARA); `user_tags`
are analyst tags. `funcs` are the entry addresses of referencing functions.
"""

import fnmatch
import hashlib
import json

from bsimvis.app.services.index_service import get_pool_id
from bsimvis.app.services.query_syntax import resolve_targets

MAX_VALUE_LEN = 1024  # stored value is cut here; `length` keeps the real size
MAX_BUCKET_LEN = 128  # search-bucket key is the lowered value cut to this


def file_key(coll, md5):
    return f"{coll}:file:{md5}:strings"


def func_key(coll, md5, addr):
    return f"{coll}:func:{md5}:{addr}:strings"


def string_id(value):
    return hashlib.sha1(value.encode("utf-8", "replace")).hexdigest()[:16]


def _bucket(value):
    return value.lower()[:MAX_BUCKET_LEN]


def _txt(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def _origins(r, collection):
    """Real collections behind `collection` (itself, or a pool's members)."""
    pool_id = get_pool_id(collection)
    if not pool_id:
        return [collection]
    return sorted(_txt(c) for c in r.smembers(f"global:pool:{pool_id}:collections"))


def _loads(raw):
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def make_record(value, addr, encoding, section, tags=None, rules=None, funcs=None):
    """Build one record from a raw extracted string."""
    return {
        "id": string_id(value),
        "value": value[:MAX_VALUE_LEN],
        "encoding": encoding,
        "addr": addr,
        "section": section,
        "length": len(value),
        "tags": sorted(tags or []),
        "rules": sorted(rules or []),
        "user_tags": [],
        "funcs": sorted(set(funcs or [])),
    }


# --------------------------------------------------------------------- write


def _index_ops(pipe, coll, md5, rec, add):
    """Queue the add/remove of one record's inverted-index entries."""
    sid, low = rec["id"], _bucket(rec["value"])
    fid_set = f"{coll}:idx:string:value:{sid}"
    file_set = f"{coll}:idx:string:file:{sid}"
    func_bucket = f"{coll}:idx:func:strings:{low}"
    file_bucket = f"{coll}:idx:file:strings:{low}"
    file_id = f"{coll}:file:{md5}"
    op = pipe.sadd if add else pipe.srem
    touched = []
    for entry in rec.get("funcs", []):
        fid = f"{coll}:func:{md5}:{entry}"
        op(fid_set, fid)
        op(func_bucket, fid)
        op(func_key(coll, md5, entry), rec["addr"])
    op(file_set, file_id)
    op(file_bucket, file_id)
    regs = [
        (f"{coll}:reg:string:value", fid_set),
        (f"{coll}:reg:string:file", file_set),
        (f"{coll}:reg:func:strings", func_bucket),
        (f"{coll}:reg:file:strings", file_bucket),
    ]
    if add:
        for reg, key in regs:
            pipe.sadd(reg, key)
    else:
        touched = regs
    return touched


def unindex_file_strings(r, coll, md5):
    """Drop a file's string table and every index entry that points at it."""
    table = r.hgetall(file_key(coll, md5))
    if not table:
        return 0
    touched = []
    pipe = r.pipeline(transaction=False)
    for raw in table.values():
        rec = _loads(raw)
        if rec:
            touched += _index_ops(pipe, coll, md5, rec, add=False)
            for entry in rec.get("funcs", []):
                pipe.delete(func_key(coll, md5, entry))
    pipe.delete(file_key(coll, md5))
    pipe.execute()
    # An emptied bucket vanishes in Redis; leave no dangling registry entry.
    touched = list(dict.fromkeys(touched))
    sizes = r.pipeline(transaction=False)
    for _, key in touched:
        sizes.exists(key)
    pipe = r.pipeline(transaction=False)
    for (reg, key), alive in zip(touched, sizes.execute()):
        if not alive:
            pipe.srem(reg, key)
    pipe.execute()
    return len(table)


def index_file_strings(r, coll, md5, strings):
    """Replace a file's strings with `strings` (a list of records)."""
    unindex_file_strings(r, coll, md5)
    pipe = r.pipeline(transaction=False)
    n = 0
    for rec in strings:
        pipe.hset(file_key(coll, md5), rec["addr"], json.dumps(rec))
        _index_ops(pipe, coll, md5, rec, add=True)
        n += 1
        if n % 500 == 0:
            pipe.execute()
    pipe.execute()
    return n


def set_user_tag(r, coll, md5, addr, tag, add=True):
    """Add or remove an analyst tag on one string. Returns the record."""
    raw = r.hget(file_key(coll, md5), addr)
    rec = _loads(raw)
    if not rec:
        return None
    tags = set(rec.get("user_tags") or [])
    tags.add(tag) if add else tags.discard(tag)
    rec["user_tags"] = sorted(tags)
    r.hset(file_key(coll, md5), addr, json.dumps(rec))
    return rec


# ---------------------------------------------------------------------- read


def function_strings(r, collection, md5, addr):
    """Strings one function references: [{id, value, encoding, addr, section,
    length, tags, rules, user_tags}] ordered by address."""
    out = []
    for coll in _origins(r, collection):
        addrs = sorted(_txt(a) for a in r.smembers(func_key(coll, md5, addr)))
        if not addrs:
            continue
        for raw in r.hmget(file_key(coll, md5), addrs):
            rec = _loads(raw)
            if rec:
                rec.pop("funcs", None)
                out.append(rec)
        break
    return out


def _is_id(token):
    return len(token) == 16 and all(c in "0123456789abcdef" for c in token)


def string_functions(r, collection, value_or_id, limit=100, offset=0):
    """Function ids (`{coll}:func:{md5}:{addr}`) referencing a string, across a
    collection or every member of a pool. Takes a string id or a literal value."""
    token = str(value_or_id)
    ids = []
    for coll in _origins(r, collection):
        key = None
        if _is_id(token) and r.exists(f"{coll}:idx:string:value:{token}"):
            key = f"{coll}:idx:string:value:{token}"
        elif r.exists(f"{coll}:idx:string:value:{string_id(token)}"):
            key = f"{coll}:idx:string:value:{string_id(token)}"
        else:
            key = f"{coll}:idx:func:strings:{_bucket(token)}"
        ids += [_txt(f) for f in r.smembers(key)]
    ids = sorted(set(ids))
    return ids[offset : offset + limit] if limit else ids[offset:]


def string_files(r, collection, value_or_id):
    """File ids (`{coll}:file:{md5}`) that contain a string."""
    token = str(value_or_id)
    out = set()
    for coll in _origins(r, collection):
        sid = token if _is_id(token) else string_id(token)
        out |= {_txt(f) for f in r.smembers(f"{coll}:idx:string:file:{sid}")}
        out |= {
            _txt(f) for f in r.smembers(f"{coll}:idx:file:strings:{_bucket(token)}")
        }
    return sorted(out)


SORTS = {
    "addr": lambda s: int(s["addr"], 16) if _hex(s["addr"]) else 0,
    "length": lambda s: s["length"],
    "value": lambda s: s["value"].lower(),
    "refs": lambda s: len(s.get("funcs") or []),
    "encoding": lambda s: s["encoding"],
    "section": lambda s: s.get("section") or "",
}


def _hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def file_strings(
    r,
    collection,
    md5,
    q=None,
    encoding=None,
    section=None,
    tag=None,
    yara=None,
    referenced=None,
    min_length=0,
    sort_by="addr",
    sort_order="asc",
    offset=0,
    limit=100,
):
    """One file's string table, filtered, sorted and paged server-side.

    `q` is a case-insensitive substring (`*` makes it a glob), `tag` matches an
    analyzer or user tag by substring, `yara` keeps only strings a rule hit,
    `referenced` keeps only strings some function references."""
    rows = []
    for coll in _origins(r, collection):
        table = r.hgetall(file_key(coll, md5))
        if table:
            rows = [x for x in (_loads(v) for v in table.values()) if x]
            break

    needle = (q or "").lower()
    tag = (tag or "").lower()

    def keep(s):
        if needle:
            hay = s["value"].lower()
            if "*" in needle:
                if not fnmatch.fnmatchcase(hay, needle):
                    return False
            elif needle not in hay:
                return False
        if encoding and s["encoding"] != encoding:
            return False
        if section and s.get("section") != section:
            return False
        if tag and not any(
            tag in t.lower() for t in (s.get("tags") or []) + (s.get("user_tags") or [])
        ):
            return False
        if yara is not None and bool(s.get("rules")) != yara:
            return False
        if referenced is not None and bool(s.get("funcs")) != referenced:
            return False
        return s["length"] >= min_length

    rows = [s for s in rows if keep(s)]
    rows.sort(key=SORTS.get(sort_by, SORTS["addr"]), reverse=sort_order == "desc")
    total = len(rows)
    page = rows[offset : offset + limit] if limit else rows[offset:]
    return {"total": total, "offset": offset, "limit": limit, "strings": page}


def search_strings(r, collection, q, limit=50, offset=0):
    """Distinct string values matching `q` across all files of a collection or
    pool, with how many files and functions carry each."""
    seen = {}
    for coll in _origins(r, collection):
        targets, _, _ = resolve_targets(
            r, coll, "file", "strings", q, default_kind="substring"
        )
        for low in targets:
            row = seen.setdefault(low, {"value": low, "files": 0, "functions": 0})
            row["files"] += r.scard(f"{coll}:idx:file:strings:{low}")
            row["functions"] += r.scard(f"{coll}:idx:func:strings:{low}")
    rows = sorted(seen.values(), key=lambda x: (-x["files"], x["value"]))
    return {
        "total": len(rows),
        "offset": offset,
        "limit": limit,
        "strings": rows[offset : offset + limit],
    }
