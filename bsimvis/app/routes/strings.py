"""Strings endpoints: a file's string table, per-function strings, the
string -> functions pivot, cross-file value search and analyst tagging.

Full-text search over function strings is the existing function search with
`strings=<text>` (index_config field `strings`); there is no second query path.
"""

import json

from flask import request

from bsimvis.app.services import string_service, tag_provenance
from bsimvis.app.services.redis_client import get_redis


def _collection():
    pool = request.args.get("pool")
    if pool:
        return f"global:pool:{pool}"
    return request.args.get("collection")


def _flag(name):
    """Tri-state boolean query arg: None when absent."""
    v = request.args.get(name)
    return None if v is None else v.lower() in ("1", "true", "yes")


def _with_rules(r, collection, rows):
    """Rule metadata for the YARA rules referenced by `rows`."""
    ids = [i for s in rows for i in s.get("rules") or []]
    return tag_provenance.rule_meta(ids, r) if ids else {}


def get_file_strings():
    collection, md5 = _collection(), request.args.get("md5")
    if not collection or not md5:
        return {"error": "collection and md5 are required"}, 400
    try:
        offset = max(0, int(request.args.get("offset", 0)))
        limit = max(1, min(int(request.args.get("limit", 100)), 5000))
        min_length = int(request.args.get("min_length", 0))
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400
    r = get_redis()
    page = string_service.file_strings(
        r,
        collection,
        md5,
        q=request.args.get("q"),
        encoding=request.args.get("encoding"),
        section=request.args.get("section"),
        tag=request.args.get("tag"),
        yara=_flag("yara"),
        referenced=_flag("referenced"),
        min_length=min_length,
        sort_by=request.args.get("sort_by", "addr"),
        sort_order=request.args.get("sort_order", "asc"),
        offset=offset,
        limit=limit,
    )
    page["rules"] = _with_rules(r, collection, page["strings"])
    return page


def get_function_strings():
    collection = _collection()
    md5, addr = request.args.get("md5"), request.args.get("addr")
    if not collection or not md5 or not addr:
        return {"error": "collection, md5 and addr are required"}, 400
    r = get_redis()
    rows = string_service.function_strings(r, collection, md5, addr)
    return {"strings": rows, "rules": _with_rules(r, collection, rows)}


def get_string_functions():
    """Functions that reference a string (value or id), with display fields."""
    collection, token = _collection(), request.args.get("value") or request.args.get(
        "id"
    )
    if not collection or not token:
        return {"error": "collection and value (or id) are required"}, 400
    try:
        offset = max(0, int(request.args.get("offset", 0)))
        limit = max(1, min(int(request.args.get("limit", 100)), 1000))
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400
    r = get_redis()
    everything = string_service.string_functions(r, collection, token, limit=0)
    ids = everything[offset : offset + limit]
    pipe = r.pipeline(transaction=False)
    for fid in ids:
        pipe.get(f"{fid}:meta")
    out = []
    for fid, raw in zip(ids, pipe.execute()):
        try:
            doc = json.loads(raw) if raw else {}
        except ValueError:
            doc = {}
        coll, _, rest = fid.partition(":func:")
        md5, _, addr = rest.partition(":")
        out.append(
            {
                "id": fid,
                "collection": coll,
                "file_md5": md5,
                "entrypoint_address": addr,
                "function_name": doc.get("function_name"),
                "file_name": doc.get("file_name"),
            }
        )
    return {
        "total": len(everything),
        "offset": offset,
        "limit": limit,
        "functions": out,
        "files": string_service.string_files(r, collection, token)[:200],
    }


def search_strings():
    """Distinct strings across the collection/pool: aggregated, paged, filtered
    and sorted server-side."""
    collection = _collection()
    if not collection:
        return {"error": "collection or pool is required"}, 400
    try:
        offset = max(0, int(request.args.get("offset", 0)))
        limit = max(1, min(int(request.args.get("limit", 100)), 5000))
        min_length = int(request.args.get("min_length", 0))
    except ValueError:
        return {"error": "Invalid numeric parameter"}, 400
    return string_service.list_strings(
        get_redis(),
        collection,
        q=request.args.get("q"),
        encoding=request.args.get("encoding"),
        section=request.args.get("section"),
        tag=request.args.get("tag"),
        yara=_flag("yara"),
        referenced=_flag("referenced"),
        min_length=min_length,
        sort_by=request.args.get("sort_by", "files"),
        sort_order=request.args.get("sort_order", "desc"),
        offset=offset,
        limit=limit,
    )
