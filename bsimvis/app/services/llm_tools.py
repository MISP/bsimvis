"""Tool layer for LLM-driven analysis.

Thin, mostly read-only wrappers around existing services/routes, exposed as
Ollama-style function-call tools. Both the interactive chat agent
(`llm_chat_service`) and the context-aware batch orchestrator
(`analysis_orchestrator`) share this one implementation of "how do we look up
a function / its neighbours / its tags" instead of each re-deriving it.

Wrappers call the existing Flask view functions in-process via
`test_request_context` rather than re-implementing their filter/query logic
(the same trick `routes/llm.py:_resolve_filters_to_ids` already uses) -- so a
schema or filter-syntax change in those views does not have to be mirrored
here.

Two different callers, two different Flask situations: the chat agent always
runs inside a live HTTP request (an app context already exists), but the
batch orchestrator runs inside `worker.py` -- a plain Python process with no
Flask app at all. `current_app` only resolves inside an *already active*
context, so it works for the former and raises for the latter. `_context_app`
reuses the live app when there is one and lazily builds a standalone instance
(cached per worker process) when there is not.
"""

import json
import logging
from urllib.parse import parse_qs

from flask import current_app, has_app_context
from werkzeug.datastructures import MultiDict

from bsimvis.app.services.function_service import fetch_function_data
from bsimvis.app.services.redis_client import get_redis

logger = logging.getLogger(__name__)

_standalone_app = None


def _context_app():
    """A Flask app to build a `test_request_context` on, whether or not one
    is already running this call (see module docstring)."""
    if has_app_context():
        return current_app._get_current_object()
    global _standalone_app
    if _standalone_app is None:
        from bsimvis.app import create_app

        _standalone_app = create_app()
    return _standalone_app


def parse_func_id(func_id):
    """(collection, md5, addr) from any of the id shapes used across the app."""
    fid = func_id
    if ":func:" in fid:
        prefix, rest = (fid[4:] if fid.startswith("idx:") else fid).split(":func:", 1)
        parts = rest.split(":")
        return prefix, parts[0], parts[1]
    if ":function:" in fid:
        prefix, rest = (fid[4:] if fid.startswith("idx:") else fid).split(
            ":function:", 1
        )
        parts = rest.split(":")
        return prefix, parts[0], parts[1]
    parts = fid.split(":")
    if len(parts) < 4:
        raise ValueError(f"Invalid function id: {func_id}")
    if parts[0] == "idx":
        return parts[1], parts[3], parts[4]
    return parts[0], parts[2], parts[3]


def get_function(func_id):
    """Full context for one function: decompiled code, name, tags, notes."""
    from bsimvis.app.routes.llm import get_code_for_llm
    from bsimvis.app.services.note_service import note_service

    code_res, err = get_code_for_llm(func_id)
    if err:
        return {"error": err}

    collection, md5, addr = parse_func_id(func_id)
    _, _, meta, _ = fetch_function_data(collection, md5, addr, meta_only=True)
    meta = meta or {}
    notes = note_service.get_notes(collection, func_id) or []

    return {
        "func_id": func_id,
        "func_name": code_res["func_name"],
        "code": code_res["code"],
        "namespace": meta.get("namespace"),
        "return_type": meta.get("return_type"),
        "file_name": meta.get("file_name"),
        "file_md5": md5,
        # `tags`: static/system findings written at ingest (capa:, yara:,
        # origin:lib:, ...). `user_tags`: human- and LLM-writable.
        "tags": meta.get("tags") or [],
        "user_tags": meta.get("user_tags") or [],
        "notes": [n.get("text") for n in notes if n.get("text")],
    }


def get_call_graph(func_id):
    """Direct callers/callees of one function (names + ids only, no code)."""
    from bsimvis.app.routes.function_code import get_function_call_graph

    with _context_app().test_request_context(
        "/api/function/call_graph", query_string={"id": func_id}
    ):
        result = get_function_call_graph()
    if isinstance(result, tuple):
        return {"error": (result[0] or {}).get("detail", "call graph lookup failed")}
    return result


def get_function_relations(func_ids, collection, algo="unweighted_cosine", min_score=0.85):
    """Call edges and similarity edges among an arbitrary set of function ids.

    The bulk equivalent of `get_call_graph` for a whole working set at once --
    what the orchestrator uses to discover which functions in a batch call
    each other.
    """
    from bsimvis.app.routes.function_code import get_function_relations as _relations

    qs = {
        "ids": ",".join(func_ids),
        "collection": collection,
        "algo": algo,
        "min_score": str(min_score),
    }
    with _context_app().test_request_context("/api/function/relations", query_string=qs):
        result = _relations()
    if isinstance(result, tuple):
        return {"error": (result[0] or {}).get("detail", "relations lookup failed")}
    return result


def get_similar_functions(collection, md5, address, min_score=0.9, limit=10, pool=None):
    """Nearest neighbours of one function by BSim similarity score."""
    from bsimvis.app.routes.search_similarity import similarity_search

    qs = {
        "collection": collection,
        "md5": md5,
        "address": address,
        "min_score": str(min_score),
        "limit": str(limit),
    }
    if pool:
        qs["pool"] = pool
    with _context_app().test_request_context("/api/similarity/search", query_string=qs):
        result = similarity_search()
    if isinstance(result, tuple):
        return {"error": (result[0] or {}).get("error", "similarity search failed")}
    return {
        "pairs": result.get("pairs") or result.get("items") or result.get("results") or [],
        "total": result.get("total"),
    }


def search_functions(collection, filters_qs="", limit=25):
    """Function search using the same filter query string the search UI sends
    (e.g. `tag=capa:crypto&name=decrypt`)."""
    from bsimvis.app.routes.search_function import search_functions as _search

    args = MultiDict(parse_qs(filters_qs, keep_blank_values=True))
    args.setlist("collection", [collection])
    args.setlist("limit", [str(limit)])
    args.setlist("offset", ["0"])
    with _context_app().test_request_context(
        "/api/function/search", query_string=args.to_dict(flat=False)
    ):
        result = _search()
    if isinstance(result, tuple):
        return {"error": (result[0] or {}).get("error", "search failed")}
    funcs = result.get("functions") or []
    return {
        "total": result.get("total", len(funcs)),
        "functions": [
            {
                "func_id": f.get("function_id") or f.get("id"),
                "name": f.get("function_name") or f.get("name"),
                "tags": f.get("tags") or [],
                "user_tags": f.get("user_tags") or [],
            }
            for f in funcs
        ],
    }


def get_file_info(collection, file_md5):
    """Binary-level metadata: filetype, AV classification, YARA/capa hits at
    the file level, and cluster memberships -- ingest-time findings, not a
    fresh scan."""
    r = get_redis()
    raw = r.get(f"{collection}:file:{file_md5}:meta")
    if not raw:
        return {"error": "File not found"}
    file_meta = json.loads(raw) if not isinstance(raw, dict) else raw
    if isinstance(file_meta, str):
        file_meta = json.loads(file_meta)

    cluster_ids_raw = r.smembers(f"{collection}:file:{file_md5}:bin_clusters") or []
    cluster_ids = [c.decode() if isinstance(c, bytes) else c for c in cluster_ids_raw]

    return {
        "file_md5": file_md5,
        "file_name": file_meta.get("file_name"),
        "filetype": file_meta.get("filetype"),
        "avtype": file_meta.get("avtype"),
        "yara": file_meta.get("yara"),
        "cc_ip": file_meta.get("cc_ip"),
        "function_count": file_meta.get("function_count"),
        "cluster_ids": cluster_ids,
    }


def get_cluster_info(collection, cluster_id, algo="unweighted_cosine"):
    """Metadata + member distribution for a binary cluster (name, cohesion,
    yara/avtype/filename distributions, bookmarks/tags already on it)."""
    r = get_redis()
    raw = r.get(f"{collection}:bin_cluster:{algo}:{cluster_id}:meta")
    if not raw:
        return {"error": "Cluster not found"}
    meta = json.loads(raw) if not isinstance(raw, dict) else raw
    if isinstance(meta, str):
        meta = json.loads(meta)
    return meta


# --- tool schemas (Ollama / OpenAI function-calling format) ----------------

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_function",
            "description": (
                "Fetch full context for one function by id: decompiled code, "
                "name, existing tags, and analyst notes. Use this before "
                "judging any single function."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "func_id": {
                        "type": "string",
                        "description": "Function id, e.g. 'main:func:<md5>:<addr>'",
                    }
                },
                "required": ["func_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_call_graph",
            "description": (
                "Direct callers and callees of one function (names/ids only, "
                "no code). Use to understand a function's role before or "
                "after reading it -- e.g. is this only ever called by a "
                "single suspicious wrapper."
            ),
            "parameters": {
                "type": "object",
                "properties": {"func_id": {"type": "string"}},
                "required": ["func_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_similar_functions",
            "description": (
                "BSim nearest neighbours of one function above a similarity "
                "threshold. Use to check whether a function is near-identical "
                "to known library/stdlib code (legitimate) versus unique to "
                "this binary (custom, worth closer inspection)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "collection": {"type": "string"},
                    "md5": {"type": "string", "description": "File md5"},
                    "address": {"type": "string", "description": "Function entry address"},
                    "min_score": {"type": "number", "default": 0.9},
                    "limit": {"type": "integer", "default": 10},
                },
                "required": ["collection", "md5", "address"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_functions",
            "description": (
                "Search functions in a collection by tag/name/namespace, e.g. "
                "to find every function tagged capa:crypto, or every function "
                "named like 'decrypt*'. filters_qs uses the same query-string "
                "syntax as the app's function search (tag=, name=, namespace=)."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "collection": {"type": "string"},
                    "filters_qs": {
                        "type": "string",
                        "description": "e.g. 'tag=capa:crypto&name=decrypt'",
                    },
                    "limit": {"type": "integer", "default": 25},
                },
                "required": ["collection"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_file_info",
            "description": (
                "Binary-level metadata for a file already in the collection: "
                "filetype, AV classification, YARA hits, cluster memberships. "
                "Ingest-time findings, not a fresh scan."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "collection": {"type": "string"},
                    "file_md5": {"type": "string"},
                },
                "required": ["collection", "file_md5"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_cluster_info",
            "description": (
                "Metadata for a binary cluster: name, cohesion score, member "
                "count, and yara/avtype/filename distributions across members. "
                "Use to interpret what a function's or file's cluster "
                "membership implies."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "collection": {"type": "string"},
                    "cluster_id": {"type": "string"},
                    "algo": {"type": "string", "default": "unweighted_cosine"},
                },
                "required": ["collection", "cluster_id"],
            },
        },
    },
]

DISPATCH = {
    "get_function": lambda a: get_function(a["func_id"]),
    "get_call_graph": lambda a: get_call_graph(a["func_id"]),
    "get_similar_functions": lambda a: get_similar_functions(
        a["collection"],
        a["md5"],
        a["address"],
        min_score=a.get("min_score", 0.9),
        limit=a.get("limit", 10),
    ),
    "search_functions": lambda a: search_functions(
        a["collection"], a.get("filters_qs", ""), a.get("limit", 25)
    ),
    "get_file_info": lambda a: get_file_info(a["collection"], a["file_md5"]),
    "get_cluster_info": lambda a: get_cluster_info(
        a["collection"], a["cluster_id"], a.get("algo", "unweighted_cosine")
    ),
}


def call_tool(name, arguments):
    """Runs one tool call and returns a JSON-serialisable result.

    Never raises: a bad tool call (unknown name, missing arg, lookup miss) is
    fed back to the model as an error string so it can retry or explain
    instead of aborting the whole conversation.
    """
    fn = DISPATCH.get(name)
    if not fn:
        return {"error": f"Unknown tool: {name}"}
    try:
        return fn(arguments or {})
    except Exception as e:
        logger.warning(f"llm_tools: tool {name} failed: {e}")
        return {"error": str(e)}
