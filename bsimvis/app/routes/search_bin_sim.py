import json
import logging
import time

from flask import request
from bsimvis.app.services import lineage_service
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import normalize_tags, enrich_pool_data

DEFAULT_LIMIT = 50

# ?containers= values: both members / at least one / neither (plain files only).
CONTAINER_MODES = ("both", "any", "none")

# Collection sort field -> ZSET index suffix (None = no zset, must sort via docs).
# Mirrors the numeric indexes written by _index_bin_sim_pair.
SORT_ZSET_MAP = {
    "score": "score",
    "score_code": "score_code",
    "score_library": "score_library",
    "score_content": "score_content",
    "coverage": "coverage_a",
    "shared_clusters": "shared_clusters",
    "functions_count": "functions_count_a",
    "computed_at": "computed_at",
    "architecture": None,
}


def _dec(x):
    return x.decode() if isinstance(x, bytes) else str(x)


def _bucket_union(r, collection, fields, val):
    """SIDs whose bucket (in any of `fields`) contains `val` as a substring.
    Mirrors get_field_matches in search_similarity: SSCAN the registry for
    matching buckets, then SUNION their members."""
    val_l = val.lower()
    buckets = []
    for field in fields:
        reg = f"{collection}:reg:bin_sim:{field}"
        try:
            for b in r.sscan_iter(reg, match=f"*{val_l}*", count=1000):
                bs = _dec(b)
                if val_l in bs.lower():
                    buckets.append(bs)
        except Exception as e:
            logging.warning(f"bin_sim registry SSCAN failed for {reg}: {e}")
    out = set()
    if buckets:
        pipe = r.pipeline(transaction=False)
        for b in buckets:
            pipe.smembers(b)
        for res in pipe.execute():
            if res:
                out.update(_dec(x) for x in res)
    return out


def _file_tag_union(r, collection, val, fields=("tags", "user_tags"), is_pool=False):
    """SIDs whose A or B binary carries a file tag matching `val` (substring).

    The bin_sim file_tags_* buckets are a build-time snapshot of the file doc, so
    they go stale the moment a tag is added or removed. Tag filters therefore
    resolve through the file-level tag index — which the tag service keeps current
    in collections and mirrors into every pool — and map file -> pairs through
    bin_sim:involves. The denormalized fields stay, but for display only.
    """
    val_l = val.lower()
    buckets = []
    for field in fields:
        reg = f"{collection}:reg:file:{field}"
        try:
            for b in r.sscan_iter(reg, match=f"*{val_l}*", count=1000):
                bs = _dec(b)
                # bucket key is {ns}:idx:file:{field}:{tag} — match the tag only,
                # so a value colliding with the prefix can't drag in every bucket.
                if val_l in bs.rsplit(":", 1)[-1].lower():
                    buckets.append(bs)
        except Exception as e:
            logging.warning(f"file tag registry SSCAN failed for {reg}: {e}")
    if not buckets:
        return set()

    file_ids = set()
    pipe = r.pipeline(transaction=False)
    for b in buckets:
        pipe.smembers(b)
    for res in pipe.execute():
        if res:
            file_ids.update(_dec(x) for x in res)

    pipe = r.pipeline(transaction=False)
    queried = False
    for fid in file_ids:
        parts = fid.split(":")
        if len(parts) < 3 or parts[1] != "file":
            continue
        f_coll, md5 = parts[0], parts[2]
        # Pool involves keys are qualified by origin collection; the same md5 can
        # appear in two member collections.
        pipe.smembers(
            f"{collection}:bin_sim:involves:{f_coll}:{md5}"
            if is_pool
            else f"{collection}:bin_sim:involves:{md5}"
        )
        queried = True
    if not queried:
        return set()
    out = set()
    for res in pipe.execute():
        if res:
            out.update(_dec(x) for x in res)
    return out


def _split_sid(sid, algo_marker, collection, is_pool=False):
    """Parse a pair SID into (coll_a, m_a, coll_b, m_b), or None if it isn't one.
    Collection: {coll}:bin_sim:{algo}:{m_a}::{m_b}
    Pool:       global:pool:{id}:bin_sim:{algo}:{coll_a}:{m_a}::{coll_b}:{m_b}"""
    try:
        rest = sid.split(algo_marker, 1)[1]
    except (IndexError, ValueError):
        return None
    parts = rest.split("::", 1)
    part_a = parts[0]
    part_b = parts[1] if len(parts) > 1 else ""
    if is_pool:
        coll_a, m_a = part_a.rsplit(":", 1) if ":" in part_a else ("", part_a)
        coll_b, m_b = part_b.rsplit(":", 1) if ":" in part_b else ("", part_b)
    else:
        coll_a = coll_b = collection
        m_a, m_b = part_a, part_b
    return coll_a, m_a, coll_b, m_b


def _container_pred(r, collection, algo_marker, mode, is_pool=False):
    """Predicate sid -> keep, for the container-membership filter. None if off.

    Reads the live `{coll}:lineage:containers` set rather than a bin_sim index,
    so marking a file as a container takes effect without rebuilding pairs.
    ponytail: one SMEMBERS per involved collection, cached for the request.
    """
    if mode not in CONTAINER_MODES:
        return None
    cache = {}

    def members(coll):
        if coll not in cache:
            cache[coll] = lineage_service.container_md5s(coll, r) if coll else set()
        return cache[coll]

    def keep(sid):
        parsed = _split_sid(sid, algo_marker, collection, is_pool)
        if not parsed:
            return False
        coll_a, m_a, coll_b, m_b = parsed
        a = m_a in members(coll_a)
        b = m_b in members(coll_b)
        if mode == "both":
            return a and b
        if mode == "any":
            return a or b
        return not a and not b

    return keep


def _znum(r, collection, field, lo, hi):
    """SIDs in the numeric ZSET index for `field` within [lo, hi] (None = open)."""
    key = f"{collection}:idx:bin_sim:{field}"
    return set(
        _dec(x)
        for x in r.zrangebyscore(
            key, lo if lo is not None else "-inf", hi if hi is not None else "+inf"
        )
    )


def _collection_page(r, collection, algo, f, is_pool=False):
    """Index-backed candidate resolution + server-side sort + pagination. Serves
    both collections and (once reindexed) pools; `collection` is the index-key
    prefix ({collection} or global:pool:{id}). Returns (paged_light, total). Only
    the returned page is enriched by the caller."""
    algo_marker = f":bin_sim:{algo}:"
    sort_zset_field = SORT_ZSET_MAP.get(f["sort_by"], "score")
    reverse = f["sort_order"] != "asc"

    candidates = None  # None == unconstrained (all sims for this algo)

    def restrict(s):
        nonlocal candidates
        candidates = s if candidates is None else (candidates & s)

    # --- Set-bucket (substring) filters, a/b unioned ---
    if f["file_name"]:
        restrict(
            _bucket_union(
                r,
                collection,
                [
                    "file_name_a",
                    "file_name_b",
                    "file_parent_file_name_a",
                    "file_parent_file_name_b",
                    "file_related_file_name_a",
                    "file_related_file_name_b",
                ],
                f["file_name"],
            )
        )
    if f["arch"]:
        restrict(
            _bucket_union(
                r, collection, ["architecture_a", "architecture_b"], f["arch"]
            )
        )
    if f["md5"]:
        restrict(
            _bucket_union(
                r,
                collection,
                [
                    "md5_a",
                    "md5_b",
                    "file_parent_md5_a",
                    "file_parent_md5_b",
                    "file_related_md5_a",
                    "file_related_md5_b",
                ],
                f["md5"],
            )
        )
    for tf in f["file_tag"]:
        restrict(_file_tag_union(r, collection, tf, is_pool=is_pool))
    for word in f["q"].split():
        if word:
            restrict(
                _bucket_union(
                    r,
                    collection,
                    ["file_name_a", "file_name_b", "md5_a", "md5_b"],
                    word,
                )
                | _file_tag_union(r, collection, word, is_pool=is_pool)
            )

    # --- Numeric range filters via ZSET indexes (a/b union semantics) ---
    if f["min_score"] is not None:
        restrict(_znum(r, collection, "score", f["min_score"], None))
    if f["max_score"] is not None:
        restrict(_znum(r, collection, "score", None, f["max_score"]))
    # coverage: min keeps if max(a,b)>=min (union); max keeps if min(a,b)<=max (union)
    if f["min_cov"] is not None:
        restrict(
            _znum(r, collection, "coverage_a", f["min_cov"], None)
            | _znum(r, collection, "coverage_b", f["min_cov"], None)
        )
    if f["max_cov"] is not None:
        restrict(
            _znum(r, collection, "coverage_a", None, f["max_cov"])
            | _znum(r, collection, "coverage_b", None, f["max_cov"])
        )
    if f["min_shared"] is not None:
        restrict(_znum(r, collection, "shared_clusters", f["min_shared"], None))
    if f["max_shared"] is not None:
        restrict(_znum(r, collection, "shared_clusters", None, f["max_shared"]))
    # functions_count: both sides must pass. A pair is only interesting when both
    # binaries carry enough functions — a union lets a 3-function stub ride along
    # with a big partner and swamp the page.
    if f["min_funcs"] is not None:
        restrict(
            _znum(r, collection, "functions_count_a", f["min_funcs"], None)
            & _znum(r, collection, "functions_count_b", f["min_funcs"], None)
        )
    if f["max_funcs"] is not None:
        restrict(
            _znum(r, collection, "functions_count_a", None, f["max_funcs"])
            & _znum(r, collection, "functions_count_b", None, f["max_funcs"])
        )

    # --- Exclusions (subtract) ---
    excl = []
    if f["exclude_file_tag"]:
        excl.append((("tags", "user_tags"), f["exclude_file_tag"]))
    if f["exclude_file_static_tag"]:
        excl.append((("tags",), f["exclude_file_static_tag"]))
    if f["exclude_file_user_tag"]:
        excl.append((("user_tags",), f["exclude_file_user_tag"]))
    if excl:
        if candidates is None:
            # exclusion-only query: base is the full built set (SIDs, not docs)
            # ponytail: O(N) SMEMBERS only when the sole filter is an exclusion; fine at this rarity.
            candidates = set(
                _dec(x) for x in r.smembers(f"{collection}:bin_sim:built:{algo}")
            )
        for fields, vals in excl:
            for v in vals:
                candidates -= _file_tag_union(
                    r, collection, v, fields=fields, is_pool=is_pool
                )

    # --- Sort + paginate. The sort ZSET already holds SIDs in sorted order, so we
    # never fetch per-candidate scores or sort in Python. ---
    # ponytail: assumes one bin_sim algo per collection (weighted variants are score
    # fields, not separate builds), so idx:bin_sim:{field} == this algo's members.
    zkey = f"{collection}:idx:bin_sim:{sort_zset_field}" if sort_zset_field else None
    offset, limit = f["offset"], f["limit"]

    # Container membership isn't indexed per pair; it's a predicate on the SID's
    # two md5s, so it rules out the count-only fast path below.
    keep = _container_pred(r, collection, algo_marker, f["containers"], is_pool)

    if candidates is None and zkey and keep is None:
        # Fast path: page straight off the sorted ZSET (O(offset+limit)).
        total = r.zcard(zkey)
        page_raw = (
            r.zrevrange(zkey, offset, offset + limit - 1)
            if reverse
            else r.zrange(zkey, offset, offset + limit - 1)
        )
        page_sids = [_dec(s) for s in page_raw]
    elif zkey:
        # Filtered: walk the sorted ZSET once, keep only candidates. One ZRANGE +
        # in-memory intersect, no per-candidate round trips, no Python sort.
        # ponytail: fine for ~1e4 pairs; if bin_sim ever hits millions, push this
        # producer walk into a Lua script like search_similarity.lua.
        ordered = r.zrevrange(zkey, 0, -1) if reverse else r.zrange(zkey, 0, -1)
        total = 0
        page_sids = []
        for s in ordered:
            s = _dec(s)
            if candidates is not None and s not in candidates:
                continue
            if keep is not None and not keep(s):
                continue
            if offset <= total < offset + limit:
                page_sids.append(s)
            total += 1
    else:
        # architecture sort: no ZSET, rank candidate docs by architecture_a.
        if candidates is None:
            # ponytail: O(N) doc load only for the (rare) unfiltered non-numeric sort.
            candidates = set(
                _dec(x) for x in r.smembers(f"{collection}:bin_sim:built:{algo}")
            )
        cand = [s for s in candidates if algo_marker in s and (keep is None or keep(s))]
        total = len(cand)
        pipe = r.pipeline(transaction=False)
        for s in cand:
            pipe.get(s)
        arch = []
        for s, raw in zip(cand, pipe.execute()):
            a = ""
            if raw:
                try:
                    d = json.loads(raw) if not isinstance(raw, dict) else raw
                    if isinstance(d, str):
                        d = json.loads(d)
                    a = (d.get("architecture_a") or "").lower()
                except Exception:
                    pass
            arch.append((s, a))
        arch.sort(key=lambda x: x[1], reverse=reverse)
        page_sids = [s for s, _ in arch[offset : offset + limit]]

    # --- Build light docs for the page + fetch metadata for its md5s only ---
    paged_light, page_md5s = _light_from_sids(
        page_sids, algo_marker, collection, is_pool
    )
    file_meta_cache, file_funcs_count = _fetch_meta(r, page_md5s)
    return paged_light, total, file_meta_cache, file_funcs_count


def _light_from_sids(page_sids, algo_marker, collection, is_pool=False):
    """Parse SIDs into light docs."""
    paged_light = []
    page_md5s = set()
    for sid in page_sids:
        parsed = _split_sid(sid, algo_marker, collection, is_pool)
        if not parsed:
            continue
        coll_a, m_a, coll_b, m_b = parsed
        page_md5s.add((coll_a, m_a))
        page_md5s.add((coll_b, m_b))
        paged_light.append(
            {
                "sid": sid,
                "m_a": m_a,
                "m_b": m_b,
                "coll_a": coll_a,
                "coll_b": coll_b,
                "doc": None,
            }
        )
    return paged_light, page_md5s


def _fetch_meta(r, md5_pairs):
    """Batch fetch file meta + function count for a set of (collection, md5)."""
    file_meta_cache = {}
    file_funcs_count = {}
    md5_list = list(md5_pairs)
    if not md5_list:
        return file_meta_cache, file_funcs_count
    pipe = r.pipeline(transaction=False)
    for coll, md5 in md5_list:
        pipe.get(f"{coll}:file:{md5}:meta")
        pipe.scard(f"{coll}:idx:file:functions:{md5}")
    results = pipe.execute()
    for i, (coll, md5) in enumerate(md5_list):
        res = results[2 * i]
        func_count = results[2 * i + 1]
        if res:
            m = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(m, str):
                m = json.loads(m)
            file_meta_cache[(coll, md5)] = m if isinstance(m, dict) else {}
        else:
            file_meta_cache[(coll, md5)] = {}
        file_funcs_count[(coll, md5)] = func_count or 0
    return file_meta_cache, file_funcs_count


def _pool_page(r, pool_id, algo, f):
    """Legacy in-memory path for pools (which have no ZSET indexes). Materializes
    all pair docs, then filters/sorts/paginates in Python.
    ponytail: O(N) by necessity — pools lack the secondary indexes collections have.
    If pool sizes grow into the tens of thousands, index them like collections."""
    if f["md5"]:
        cursor = 0
        matching = []
        while True:
            cursor, keys = r.scan(
                cursor=cursor,
                match=f"global:pool:{pool_id}:bin_sim:involves:*:{f['md5']}*",
                count=1000,
            )
            matching.extend(keys)
            if cursor == 0:
                break
        sids = set()
        if matching:
            pipe = r.pipeline(transaction=False)
            for k in matching:
                pipe.smembers(k)
            for res in pipe.execute():
                sids.update(res)
    else:
        sids = r.smembers(f"global:pool:{pool_id}:bin_sim:built:{algo}")

    candidates = [_dec(s) for s in sids]
    algo_marker = f":bin_sim:{algo}:"
    candidates = [s for s in candidates if algo_marker in s]
    if not candidates:
        return [], 0, {}, {}

    keep = _container_pred(
        r, f"global:pool:{pool_id}", algo_marker, f["containers"], is_pool=True
    )

    light_docs = []
    unique_md5s = set()
    for sid in candidates:
        if keep is not None and not keep(sid):
            continue
        parsed = _split_sid(sid, algo_marker, "", is_pool=True)
        if not parsed:
            continue
        coll_a, m_a, coll_b, m_b = parsed
        unique_md5s.add((coll_a, m_a))
        unique_md5s.add((coll_b, m_b))
        light_docs.append(
            {
                "sid": sid,
                "m_a": m_a,
                "m_b": m_b,
                "coll_a": coll_a,
                "coll_b": coll_b,
                "score": 0.0,
                "coverage_a": 0.0,
                "coverage_b": 0.0,
                "shared_clusters": 0,
                "doc": None,
            }
        )

    file_meta_cache, file_funcs_count = _fetch_meta(r, unique_md5s)

    # Pools store metrics only in the doc — fetch them to sort/filter.
    pipe = r.pipeline(transaction=False)
    for ld in light_docs:
        pipe.get(ld["sid"])
    for ld, raw in zip(light_docs, pipe.execute()):
        if not raw:
            continue
        doc = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(doc, str):
            doc = json.loads(doc)
        ld["score"] = float(doc.get("score", 0.0))
        # None-valued fields (a pair with no library/content data) sort as 0,
        # same as the ZSET-backed collection path never adding an unset field.
        ld["score_code"] = float(doc.get("score_code") or 0.0)
        ld["score_library"] = float(doc.get("score_library") or 0.0)
        ld["score_content"] = float(doc.get("score_content") or 0.0)
        ld["coverage_a"] = float(doc.get("coverage_a", 0.0))
        ld["coverage_b"] = float(doc.get("coverage_b", 0.0))
        ld["shared_clusters"] = int(doc.get("shared_clusters", 0))
        ld["doc"] = doc

    filtered = []
    for ld in light_docs:
        coll_a, m_a = ld["coll_a"], ld["m_a"]
        coll_b, m_b = ld["coll_b"], ld["m_b"]
        meta_a = file_meta_cache.get((coll_a, m_a), {})
        meta_b = file_meta_cache.get((coll_b, m_b), {})
        enrich_pool_data(meta_a, pool_id)
        enrich_pool_data(meta_b, pool_id)
        enrich_pool_data(ld, pool_id)

        name_a = meta_a.get("file_name", m_a)
        name_b = meta_b.get("file_name", m_b)
        arch_a = meta_a.get("language_id", "")
        arch_b = meta_b.get("language_id", "")
        tags_a = meta_a.get("tags", []) + meta_a.get("user_tags", [])
        tags_b = meta_b.get("tags", []) + meta_b.get("user_tags", [])
        funcs_a = file_funcs_count.get((coll_a, m_a), 0)
        funcs_b = file_funcs_count.get((coll_b, m_b), 0)
        ld["functions_count_a"] = funcs_a
        ld["functions_count_b"] = funcs_b
        ld["architecture_a"] = arch_a

        if (
            f["file_name"]
            and f["file_name"] not in name_a.lower()
            and f["file_name"] not in name_b.lower()
        ):
            continue
        if f["md5"] and f["md5"] not in m_a.lower() and f["md5"] not in m_b.lower():
            continue
        if f["q"]:
            hay = " ".join([name_a, name_b, m_a, m_b] + tags_a + tags_b).lower()
            if not all(w in hay for w in f["q"].split()):
                continue
        if f["min_score"] is not None and ld.get("score", 0) < f["min_score"]:
            continue
        if f["max_score"] is not None and ld.get("score", 0) > f["max_score"]:
            continue
        if (
            f["min_cov"] is not None
            and max(ld["coverage_a"], ld["coverage_b"]) < f["min_cov"]
        ):
            continue
        if (
            f["max_cov"] is not None
            and min(ld["coverage_a"], ld["coverage_b"]) > f["max_cov"]
        ):
            continue
        if f["min_shared"] is not None and ld["shared_clusters"] < f["min_shared"]:
            continue
        if f["max_shared"] is not None and ld["shared_clusters"] > f["max_shared"]:
            continue
        if (
            f["arch"]
            and f["arch"] not in arch_a.lower()
            and f["arch"] not in arch_b.lower()
        ):
            continue
        # both sides must pass, same as the indexed path
        if f["min_funcs"] is not None and min(funcs_a, funcs_b) < f["min_funcs"]:
            continue
        if f["max_funcs"] is not None and max(funcs_a, funcs_b) > f["max_funcs"]:
            continue
        if f["file_tag"]:
            combined = set(t.lower() for t in tags_a + tags_b)
            if not all(tf in combined for tf in f["file_tag"]):
                continue
        if f["exclude_file_tag"]:
            combined = set(t.lower() for t in tags_a + tags_b)
            if any(tf in combined for tf in f["exclude_file_tag"]):
                continue
        if f["exclude_file_static_tag"]:
            static = set(
                t.lower() for t in meta_a.get("tags", []) + meta_b.get("tags", [])
            )
            if any(tf in static for tf in f["exclude_file_static_tag"]):
                continue
        if f["exclude_file_user_tag"]:
            usr = set(
                t.lower()
                for t in meta_a.get("user_tags", []) + meta_b.get("user_tags", [])
            )
            if any(tf in usr for tf in f["exclude_file_user_tag"]):
                continue
        filtered.append(ld)

    total = len(filtered)
    sort_zset_field = SORT_ZSET_MAP.get(f["sort_by"], "score")

    def key(d):
        if sort_zset_field:
            return d.get(sort_zset_field, 0)
        if f["sort_by"] == "architecture":
            return d.get("architecture_a", "")
        if f["sort_by"] == "functions_count":
            return d.get("functions_count_a", 0)
        return 0

    filtered.sort(key=key, reverse=(f["sort_order"] != "asc"))
    paged = filtered[f["offset"] : f["offset"] + f["limit"]]
    return paged, total, file_meta_cache, file_funcs_count


def search_bin_sims():
    """Search binary similarity pairs. Collections use ZSET/bucket indexes to
    filter+sort+paginate server-side and enrich only the page; pools fall back to
    an in-memory scan (no indexes)."""
    try:
        t_start = time.perf_counter()
        r = get_redis()

        pool_id = request.args.get("pool")
        collection = request.args.get("collection")
        if not collection and not pool_id:
            return {"error": "No collection or pool specified"}, 400

        algo = request.args.get("algo", "unweighted_cosine")
        is_pool = pool_id is not None

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", DEFAULT_LIMIT))
        except ValueError:
            return {"error": "offset and limit must be integers"}, 400

        def parse_float(v):
            try:
                return float(v) if v is not None and v.strip() != "" else None
            except (ValueError, AttributeError):
                return None

        def tags(name):
            return [t.strip().lower() for t in request.args.getlist(name) if t.strip()]

        f = {
            "offset": offset,
            "limit": limit,
            "sort_by": (
                request.args.get("sort_by") or request.args.get("sort") or "score"
            ).strip(),
            "sort_order": request.args.get("sort_order", "desc"),
            "min_score": parse_float(request.args.get("min_score")),
            "max_score": parse_float(request.args.get("max_score")),
            "min_cov": parse_float(request.args.get("min_coverage")),
            "max_cov": parse_float(request.args.get("max_coverage")),
            "min_shared": parse_float(request.args.get("min_shared")),
            "max_shared": parse_float(request.args.get("max_shared")),
            "min_funcs": parse_float(request.args.get("min_funcs")),
            "max_funcs": parse_float(request.args.get("max_funcs")),
            # `view` is the hard File/Container partition (no edge ever crosses
            # node types); it overrides the older `containers` both/any/none
            # filter rather than composing with it, so there is no way back to
            # a mixed page once a view is chosen.
            "containers": (
                {"file": "none", "container": "both"}.get(
                    request.args.get("view", "").strip().lower()
                )
                or request.args.get("containers", "").strip().lower()
                or "none"
            ),
            "arch": request.args.get("arch", "").strip().lower(),
            "md5": request.args.get("md5", "").strip().lower(),
            "file_name": request.args.get("file_name", "").strip().lower(),
            "q": request.args.get("q", "").strip().lower(),
            "file_tag": tags("file_tag"),
            "exclude_file_tag": tags("exclude_file_tag"),
            "exclude_file_static_tag": tags("exclude_file_static_tag"),
            "exclude_file_user_tag": tags("exclude_file_user_tag"),
        }

        # Similarity-level tag filters (not indexed for bin_sim; applied on the page)
        sim_tag_filters = tags("tag")
        exclude_sim_tag_filters = tags("exclude_tag")
        exclude_sim_static_tag_filters = tags("exclude_static_tag")
        exclude_sim_user_tag_filters = tags("exclude_user_tag")

        t0 = time.perf_counter()
        if is_pool:
            prefix = f"global:pool:{pool_id}"
            if r.exists(f"{prefix}:idx:bin_sim:score"):
                paged_light, total, file_meta_cache, file_funcs_count = (
                    _collection_page(r, prefix, algo, f, is_pool=True)
                )
            else:
                # Not reindexed yet -> legacy O(N) scan. Run reindex_pool_bin_sim to speed up.
                paged_light, total, file_meta_cache, file_funcs_count = _pool_page(
                    r, pool_id, algo, f
                )
        else:
            paged_light, total, file_meta_cache, file_funcs_count = _collection_page(
                r, collection, algo, f
            )
        t1 = time.perf_counter()

        # Batch-fetch sim docs for any page row that doesn't already carry one
        # (index-backed collection/pool paths defer the doc; legacy _pool_page preloads).
        missing = [ld for ld in paged_light if not ld.get("doc")]
        if missing:
            pipe = r.pipeline(transaction=False)
            for ld in missing:
                pipe.get(ld["sid"])
            for ld, raw in zip(missing, pipe.execute()):
                if raw:
                    d = json.loads(raw) if not isinstance(raw, dict) else raw
                    ld["doc"] = json.loads(d) if isinstance(d, str) else d

        # --- Enrich only the page ---
        final_docs = []
        for ld in paged_light:
            sid = ld["sid"]
            doc = ld["doc"]
            if not doc:
                continue

            doc["_id"] = sid
            doc.pop("diff", None)

            m_a = doc.get("md5_a") or ld["m_a"]
            m_b = doc.get("md5_b") or ld["m_b"]
            coll_a = ld["coll_a"]
            coll_b = ld["coll_b"]
            doc["md5_a"] = m_a
            doc["md5_b"] = m_b
            doc["coll_a"] = coll_a
            doc["coll_b"] = coll_b

            meta_a = file_meta_cache.get((coll_a, m_a), {})
            meta_b = file_meta_cache.get((coll_b, m_b), {})
            if pool_id:
                enrich_pool_data(meta_a, pool_id)
                enrich_pool_data(meta_b, pool_id)
                enrich_pool_data(doc, pool_id)

            doc["file_name_a"] = meta_a.get("file_name", m_a)
            doc["file_name_b"] = meta_b.get("file_name", m_b)
            doc["file_parent_file_name_a"] = meta_a.get("parent_file_name")
            doc["file_parent_file_name_b"] = meta_b.get("parent_file_name")
            doc["file_related_file_name_a"] = meta_a.get("related_file_name")
            doc["file_related_file_name_b"] = meta_b.get("related_file_name")
            doc["file_parent_md5_a"] = meta_a.get("parent_md5")
            doc["file_parent_md5_b"] = meta_b.get("parent_md5")
            doc["file_related_md5_a"] = meta_a.get("related_md5")
            doc["file_related_md5_b"] = meta_b.get("related_md5")
            doc["file_tags_a"] = meta_a.get("tags", [])
            doc["file_tags_b"] = meta_b.get("tags", [])
            doc["file_user_tags_a"] = meta_a.get("user_tags", [])
            doc["file_user_tags_b"] = meta_b.get("user_tags", [])
            doc["architecture_a"] = doc.get("architecture_a") or meta_a.get(
                "language_id", ""
            )
            doc["architecture_b"] = doc.get("architecture_b") or meta_b.get(
                "language_id", ""
            )
            doc["functions_count_a"] = doc.get(
                "functions_count_a"
            ) or file_funcs_count.get((coll_a, m_a), 0)
            doc["functions_count_b"] = doc.get(
                "functions_count_b"
            ) or file_funcs_count.get((coll_b, m_b), 0)
            doc["compiler_a"] = meta_a.get("compiler") or meta_a.get("compiler_id", "")
            doc["compiler_b"] = meta_b.get("compiler") or meta_b.get("compiler_id", "")
            doc["entry_date_a"] = meta_a.get("entry_date", 0)
            doc["entry_date_b"] = meta_b.get("entry_date", 0)

            normalize_tags(doc)
            normalize_tags(
                doc,
                tag_fields=[
                    "file_tags_a",
                    "file_user_tags_a",
                    "file_tags_b",
                    "file_user_tags_b",
                ],
            )

            # sim-level tag filters (page-local; adjusts total like the legacy path)
            if (
                sim_tag_filters
                or exclude_sim_tag_filters
                or exclude_sim_static_tag_filters
                or exclude_sim_user_tag_filters
            ):
                sim_tags = set(
                    t.lower() for t in doc.get("tags", []) + doc.get("user_tags", [])
                )
                sim_static = set(t.lower() for t in doc.get("tags", []))
                sim_user = set(t.lower() for t in doc.get("user_tags", []))
                if sim_tag_filters and not all(
                    tf in sim_tags for tf in sim_tag_filters
                ):
                    total -= 1
                    continue
                if any(tf in sim_tags for tf in exclude_sim_tag_filters):
                    total -= 1
                    continue
                if any(tf in sim_static for tf in exclude_sim_static_tag_filters):
                    total -= 1
                    continue
                if any(tf in sim_user for tf in exclude_sim_user_tag_filters):
                    total -= 1
                    continue

            final_docs.append(doc)

        logging.info(
            f"BIN_SIM SEARCH | {'pool' if is_pool else 'coll'} | query:{t1-t0:.3f}s "
            f"| enrich:{time.perf_counter()-t1:.3f}s | TOTAL:{time.perf_counter()-t_start:.3f}s "
            f"| total={total} sort={f['sort_by']}"
        )
        return {"total": total, "offset": offset, "limit": limit, "results": final_docs}
    except Exception as e:
        logging.error(f"Error in search_bin_sims: {e}", exc_info=True)
        return {"error": str(e)}, 500
