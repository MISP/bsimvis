import os
import time
import json
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services import lineage_service
from bsimvis.app.services.bin_sim_tags import (
    AxisSplit,
    code_library_split,
    score_pair,
    merge_tag_fields,
    load_tag_meta,
    read_tags_rev,
)


BIN_SIM_TAG_FIELDS = (
    "md5_a",
    "md5_b",
    "algo",
    "file_name_a",
    "file_tags_a",
    "file_user_tags_a",
    "architecture_a",
    "file_name_b",
    "file_tags_b",
    "file_user_tags_b",
    "architecture_b",
)

BIN_SIM_NUM_FIELDS = (
    "score",
    "score_code",
    "score_library",
    "score_content",
    "coverage_a",
    "coverage_b",
    "shared_clusters",
    "computed_at",
    "functions_count_a",
    "functions_count_b",
)


def pair_score_upper_bound(num_ub, min_sum, weight_a, weight_b):
    """Exact upper bound on `score_pair(...)["score"]` for one binary pair.

    `score_pair` greedily matches a subset M of the pair's candidate edges and
    returns

        num = sum over M of  s * max(w_a, w_b)
        den = W(A) + W(B) - sum over M of min(w_a, w_b)
        score = num / den

    (`den` is `sum_weights`: matched functions contribute the larger of the two
    weights once, unmatched ones contribute their own -- which is the same as
    the two totals minus the smaller weight of every matched edge.)

    Every term is >= 0 and M only ever matches a function once, so any
    over-count of the numerator and any over-count of what is subtracted from
    the denominator both push the ratio up. `sum over M of min(w_a, w_b) <=
    min(W(A), W(B))`, so clamping with that keeps `den` positive. The result is
    therefore >= the true score: a pair whose bound falls under the threshold
    cannot reach it, and dropping it loses nothing. `leftovers()` clamps a zero
    weight up to 1.0 while a matched function keeps its raw weight, so the real
    denominator is only ever larger than the one bounded here -- still safe.
    """
    capped = min(min_sum, min(weight_a, weight_b))
    den = weight_a + weight_b - capped
    if den <= 0:
        return 1.0
    return min(1.0, num_ub / den)


def _zadd_score_split(pipe, base, algo, sid, scores):
    """Write a pair's sort ZSETs. `base` is `{collection}:bin_sim` or
    `global:pool:{id}:bin_sim` -- the two builders and the resplit path share it
    so a new score type is added in one place.

    `score_library` is None when the pair carries no library mass at all --
    absence, not a zero score -- so it is removed rather than stored as 0,
    keeping "sort by Library" a list of pairs that actually have one.
    """
    pipe.zadd(f"{base}:score:{algo}", {sid: scores["score"]})
    pipe.zadd(f"{base}:score_code:{algo}", {sid: scores["score_code"]})
    if scores.get("score_library") is not None:
        pipe.zadd(f"{base}:score_library:{algo}", {sid: scores["score_library"]})
    else:
        pipe.zrem(f"{base}:score_library:{algo}", sid)


def bin_sim_rev_key(collection):
    return f"{collection}:bin_sim_rev"


def read_bin_sim_rev(r, collection):
    """Generation counter of a collection's bin_sim pair docs.

    Bumped by every pair-doc rewrite. /api/diff memoizes the hydrated doc and
    used to revalidate it against `tags_rev` alone -- which a rebuild never
    touches -- so after a bin-sim rebuild it kept serving the previous split
    while every other reader (`call_graph?retain=`, the container rollup, which
    all go through `load_pair`) already saw the new one.
    """
    try:
        raw = r.get(bin_sim_rev_key(collection))
    except Exception:
        return 0
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except (AttributeError, TypeError, ValueError):
        return 0


def _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a=None, file_meta_b=None):
    """Write secondary indexes for a bin_sim pair doc."""
    # Every pair-doc write in the codebase passes through here, so this is the
    # one place the generation above can be moved from. See read_bin_sim_rev.
    pipe.incr(bin_sim_rev_key(collection))

    def tag_index(field, value):
        if value is None:
            return
        values = value if isinstance(value, list) else [value]
        for v in values:
            if v is None or v == "":
                continue
            bucket_key = f"{collection}:idx:bin_sim:{field}:{str(v).lower()}"
            pipe.sadd(bucket_key, sid)
            registry_key = f"{collection}:reg:bin_sim:{field}"
            pipe.sadd(registry_key, bucket_key)

    def num_index(field, value):
        if value is None:
            return
        try:
            pipe.zadd(f"{collection}:idx:bin_sim:{field}", {sid: float(value)})
        except (ValueError, TypeError):
            pass

    # Tag indexes from doc
    tag_index("md5_a", doc.get("md5_a"))
    tag_index("md5_b", doc.get("md5_b"))
    tag_index("algo", doc.get("algo"))

    # Denormalized file metadata
    if file_meta_a:
        tag_index("file_name_a", file_meta_a.get("file_name"))
        tag_index("file_tags_a", file_meta_a.get("tags"))
        tag_index("file_user_tags_a", file_meta_a.get("user_tags"))
        tag_index("architecture_a", file_meta_a.get("language_id"))
    if file_meta_b:
        tag_index("file_name_b", file_meta_b.get("file_name"))
        tag_index("file_tags_b", file_meta_b.get("tags"))
        tag_index("file_user_tags_b", file_meta_b.get("user_tags"))
        tag_index("architecture_b", file_meta_b.get("language_id"))

    # Numeric indexes
    for field in BIN_SIM_NUM_FIELDS:
        num_index(field, doc.get(field))

    pipe.sadd(f"{collection}:all_bin_sims", sid)


def _unindex_bin_sim_pair(
    pipe, collection, sid, doc, file_meta_a=None, file_meta_b=None
):
    """Remove secondary indexes for a bin_sim pair doc."""

    def tag_unindex(field, value):
        if value is None:
            return
        values = value if isinstance(value, list) else [value]
        for v in values:
            if v is None or v == "":
                continue
            bucket_key = f"{collection}:idx:bin_sim:{field}:{str(v).lower()}"
            pipe.srem(bucket_key, sid)

    tag_unindex("md5_a", doc.get("md5_a"))
    tag_unindex("md5_b", doc.get("md5_b"))
    tag_unindex("algo", doc.get("algo"))
    if file_meta_a:
        tag_unindex("file_name_a", file_meta_a.get("file_name"))
        tag_unindex("file_tags_a", file_meta_a.get("tags"))
        tag_unindex("file_user_tags_a", file_meta_a.get("user_tags"))
        tag_unindex("architecture_a", file_meta_a.get("language_id"))
    if file_meta_b:
        tag_unindex("file_name_b", file_meta_b.get("file_name"))
        tag_unindex("file_tags_b", file_meta_b.get("tags"))
        tag_unindex("file_user_tags_b", file_meta_b.get("user_tags"))
        tag_unindex("architecture_b", file_meta_b.get("language_id"))

    for num_field in BIN_SIM_NUM_FIELDS:
        pipe.zrem(f"{collection}:idx:bin_sim:{num_field}", sid)

    pipe.srem(f"{collection}:all_bin_sims", sid)


def _origin_coll(collection):
    """Pool bin_sim keys are qualified by the origin collection name.

    A request carrying `pool` reaches the route with `collection` already
    rewritten to `global:pool:{id}:col:{name}` (app/__init__.py
    normalize_pool_params), so every pair lookup has to map it back or it
    builds `...:involves:global:pool:{id}:col:{name}:{md5}` and finds nothing.
    """
    return collection.split(":col:")[-1] if collection else collection


class BinSimService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def find_pair_sid(
        self,
        collection,
        md5_a,
        md5_b,
        coll_b=None,
        pool_id=None,
        algo="unweighted_cosine",
    ):
        """Resolve one stored pair without hydrating its function rows."""
        coll_b = coll_b or collection
        if pool_id:
            coll_a, coll_b = _origin_coll(collection), _origin_coll(coll_b)
            pipe = self.r.pipeline(transaction=False)
            pipe.smembers(f"global:pool:{pool_id}:bin_sim:involves:{coll_a}:{md5_a}")
            pipe.smembers(f"global:pool:{pool_id}:bin_sim:involves:{coll_b}:{md5_b}")
            res_a, res_b = pipe.execute()
            a = {x.decode() if isinstance(x, bytes) else x for x in (res_a or set())}
            b = {x.decode() if isinstance(x, bytes) else x for x in (res_b or set())}
            return next(
                (sid for sid in a & b if f":bin_sim:{algo}:" in sid),
                None,
            )
        md5_a, md5_b = sorted((md5_a, md5_b))
        return f"{collection}:bin_sim:{algo}:{md5_a}::{md5_b}"

    def load_pair(
        self,
        collection,
        md5_a,
        md5_b,
        coll_b=None,
        pool_id=None,
        algo="unweighted_cosine",
    ):
        sid = self.find_pair_sid(collection, md5_a, md5_b, coll_b, pool_id, algo)
        raw = self.r.get(sid) if sid else None
        if not raw:
            return sid, None
        pair = json.loads(raw) if not isinstance(raw, dict) else raw
        if isinstance(pair, str):
            pair = json.loads(pair)
        return sid, pair

    def unique_functions_for_pair(
        self,
        collection,
        file_md5,
        reference_md5,
        reference_collection=None,
        pool_id=None,
        algo="unweighted_cosine",
    ):
        """Return the stored unique side for file_md5 in one exact pair."""
        reference_collection = reference_collection or collection
        sid, pair = self.load_pair(
            collection, file_md5, reference_md5, reference_collection, pool_id, algo
        )
        if not pair:
            return sid, None, None
        if pair.get("is_container_pair"):
            return sid, pair, None
        asked = (_origin_coll(collection), file_md5)
        stored_a = (pair.get("coll_a") or asked[0], pair.get("md5_a"))
        table = "unique_to_a" if stored_a == asked else "unique_to_b"
        return (
            sid,
            pair,
            {
                row.get("func_id")
                for row in (pair.get("diff") or {}).get(table, [])
                if row.get("func_id")
            },
        )

    def max_file_entrypoint(self, collection, md5):
        """Read the highest address from the file's function-ID index."""
        maximum = None
        for raw in self.r.smembers(f"{collection}:idx:file:functions:{md5}") or ():
            fid = raw.decode() if isinstance(raw, bytes) else raw
            try:
                address = int(fid.rsplit(":", 1)[-1], 16)
            except (AttributeError, ValueError):
                continue
            maximum = address if maximum is None else max(maximum, address)
        return maximum

    # ---- edge-join helpers -------------------------------------------------
    #
    # build_bin_sim used to rediscover function similarity itself: every file
    # pair got its two tf matrices built and a full |A| x |B| cosine run over
    # them. That redid, once per pair, work the collection had already done
    # once per function -- a binary in 179 pairs had its functions re-cosined
    # 179 times -- and it forced the pair list to be enumerated up front, which
    # is the N^2 nobody wanted. The function-sim build (BUILD_SIM, LSH +
    # inverted index) already writes every edge above `similarity.min_score`
    # and indexes it per file, so the file pairs can be *read* out of that
    # graph instead: walk one file's edges, group them by the file on the other
    # end, and a pair that shares no function never materialises at all.

    def _file_funcs(self, collection, md5, cache):
        """Canonical `{collection}:func:{md5}:{addr}` ids of one file."""
        if md5 not in cache:
            raw = self.r.smembers(f"{collection}:idx:file:functions:{md5}") or ()
            cache[md5] = set(
                (x.decode() if isinstance(x, bytes) else str(x)).replace(":meta", "")
                for x in raw
            )
        return cache[md5]

    def _func_weights(self, collection, fids, cache):
        """`bsim_features_count` per function, from the numeric index.

        The weight is already a ZSET score (`_index_num` at ingestion), so
        weighting a function costs a ZSCORE rather than a meta-document read --
        which is what lets the threshold below be applied before any metadata
        is fetched. A function missing from the index reads as 1.0, the same
        default `meta.get("bsim_features_count", 1.0)` gave, so stored scores
        don't move.
        """
        idx = f"{collection}:idx:func:bsim_features_count"
        miss = [f for f in fids if f not in cache]
        batch = int(os.getenv("BIN_SIM_LOAD_BATCH", 5000))
        for start in range(0, len(miss), batch):
            window = miss[start : start + batch]
            pipe = self.r.pipeline(transaction=False)
            for fid in window:
                pipe.zscore(idx, fid)
            for fid, raw in zip(window, pipe.execute()):
                cache[fid] = 1.0 if raw is None else float(raw)
        return cache

    def _func_tags(self, collection, md5, fids, cache):
        """`{fid: tags}` for the tagged functions of one file.

        Only the tagged ones are kept, which is what makes caching this for a
        whole collection cheap -- most functions carry no tags, and the meta
        documents themselves are never held past the batch that reads them.
        """
        if md5 in cache:
            return cache[md5]
        tags_by_fid = {}
        fids = sorted(fids)
        batch = int(os.getenv("BIN_SIM_LOAD_BATCH", 5000))
        for start in range(0, len(fids), batch):
            window = fids[start : start + batch]
            pipe = self.r.pipeline(transaction=False)
            for fid in window:
                pipe.get(f"{fid}:meta")
            for fid, raw in zip(window, pipe.execute()):
                if not raw:
                    continue
                meta = raw
                if not isinstance(meta, dict):
                    try:
                        meta = json.loads(meta)
                    except (TypeError, ValueError):
                        continue
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except ValueError:
                        continue
                if not isinstance(meta, dict):
                    continue
                tags = merge_tag_fields(meta)
                if tags:
                    tags_by_fid[fid] = tags
        cache[md5] = tags_by_fid
        return tags_by_fid

    def _edges_by_partner(self, collection, algo, md5, canonical, keep, min_score):
        """Every function-sim edge of `md5`, grouped by the file on the far end.

        `{collection}:sim:involves:file:{md5}` is written by every function-sim
        persist (including `_hash_match_small`, which is how sub-`min_features`
        functions get an edge at all), and the sid carries both function ids:
        `{collection}:sim:{algo}:{md5_a}:{addr_a}::{md5_b}:{addr_b}`. So the
        partner file, both function ids and the orientation all come out of the
        key itself; only the similarity needs a lookup, and that is one
        pipelined ZSCORE per edge against the sim score ZSET.

        Edges are re-filtered against `min_score` because `similarity.min_score`
        can have been lowered since the edges were baked. `canonical` maps a
        lowercased md5 back to the spelling the function-id index uses, so the
        ids built here always match `_file_funcs`; `keep` decides which
        partners this walk is responsible for.
        """
        r = self.r
        prefix = f"{collection}:sim:{algo}:"
        func_prefix = f"{collection}:func:"
        mine_md5 = canonical.get(md5.lower(), md5)

        sids, oriented = [], []
        for raw in r.smembers(f"{collection}:sim:involves:file:{md5}") or ():
            sid = raw.decode() if isinstance(raw, bytes) else str(raw)
            if not sid.startswith(prefix):
                continue  # another algo's edge
            left, sep, right = sid[len(prefix) :].partition("::")
            if not sep:
                continue
            # Each side is `{md5}:{addr}` -- or the whole function id when the
            # persist could not strip the collection prefix. The address is the
            # last segment either way, and the md5 the one before it.
            md5_1, _, addr_1 = left.rpartition(":")
            md5_2, _, addr_2 = right.rpartition(":")
            md5_1 = md5_1.rpartition(":")[-1]
            md5_2 = md5_2.rpartition(":")[-1]
            if not addr_1 or not addr_2 or not md5_1 or not md5_2:
                continue
            low_1, low_2 = md5_1.lower(), md5_2.lower()
            if low_1 == low_2:
                continue  # same-binary duplicate: not a file-diff signal
            if low_1 == mine_md5.lower():
                partner_low, mine_addr, other_addr = low_2, addr_1, addr_2
            elif low_2 == mine_md5.lower():
                partner_low, mine_addr, other_addr = low_1, addr_2, addr_1
            else:
                continue  # stale involves entry, neither side is this file
            partner = canonical.get(partner_low)
            if partner is None or not keep(partner):
                continue
            sids.append(sid)
            oriented.append(
                (
                    partner,
                    f"{func_prefix}{mine_md5}:{mine_addr}",
                    f"{func_prefix}{partner}:{other_addr}",
                )
            )

        by_partner = {}
        missing = 0
        if not sids:
            return by_partner, missing
        score_key = f"{collection}:sim:score:{algo}"
        batch = int(os.getenv("BIN_SIM_LOAD_BATCH", 5000))
        for start in range(0, len(sids), batch):
            pipe = r.pipeline(transaction=False)
            for sid in sids[start : start + batch]:
                pipe.zscore(score_key, sid)
            for (partner, fid_mine, fid_other), raw in zip(
                oriented[start : start + batch], pipe.execute()
            ):
                if raw is None:
                    # An involves entry whose score ZSET member is gone. The two
                    # are written in one pipeline, so this means a half-cleared
                    # sim -- counted rather than dropped in silence.
                    missing += 1
                    continue
                score = float(raw)
                if score < min_score:
                    continue
                by_partner.setdefault(partner, []).append((fid_mine, fid_other, score))
        return by_partner, missing

    def _file_was_sim_built(self, collection, algo, fids, sample=32):
        """Did a BUILD_SIM ever cover this file's functions?

        `built:functions:{algo}` is marked per function by the function-sim
        build, so it is the one signal that separates "this file has no stored
        edges because nothing matched it" from "this file has no stored edges
        because nobody built any" -- an empty `sim:involves:file:{md5}` looks
        identical in both cases. A sample is enough: the build marks every
        function of a file it processes, including the ones it finds nothing for.
        """
        if not fids:
            return True  # nothing to discover either way
        window = sorted(fids)[:sample]
        pipe = self.r.pipeline(transaction=False)
        for fid in window:
            pipe.sismember(f"{collection}:built:functions:{algo}", fid)
        return any(pipe.execute())

    def _discovered_edges_by_partner(
        self, sim_service, collection, algo, md5, canonical, keep, min_score
    ):
        """`_edges_by_partner`'s shape, from edges computed now instead of read.

        Delegates to `discover_file_edges`, which runs the function-sim discovery
        against the feature posting lists and funcid buckets ingestion wrote and
        persists nothing. Partner files are resolved through `canonical` and
        filtered by `keep` exactly as the stored path does, so which side of a
        pair owns a pair does not depend on where its edges came from.

        `sim_service` is built once per build_bin_sim call and carries the read
        caches (posting lists, norms, feature counts) across every file of the
        walk -- which is what keeps the cost proportional to the collection's
        features instead of to its files. It must not outlive the build: those
        caches are only valid while ingestion is not moving underneath them.
        """
        from bsimvis.app.services.similarity_service import _fid_md5

        edges = sim_service.discover_file_edges(
            collection, md5, algo=algo, min_score=min_score
        )

        by_partner = {}
        for fid_mine, fid_other, score in edges:
            partner = canonical.get(_fid_md5(fid_other).lower())
            if partner is None or not keep(partner):
                continue
            by_partner.setdefault(partner, []).append((fid_mine, fid_other, score))
        return by_partner

    def _exact_edges_by_partner(
        self, sim_service, collection, md5, fids, canonical, keep
    ):
        """Candidate partners for one file, from exact function matches alone.

        `{coll}:bsimhash:{hash}` is written at ingestion for every function.
        So "which files share an identical BSim vector with this one"
        is one bucket read per function -- one lookup, against the ~50 feature
        posting lists per function a BSim discovery walks, which is the whole
        reason this scales where discovery does not.

        Returns `{partner_md5: [(fid_mine, fid_other, 1.0), ...]}`.
        """
        from bsimvis.app.services.similarity_service import _fid_md5

        by_partner = {}
        for fid_mine, fid_other, score in sim_service._exact_bsim_edges(
            collection, sorted(fids)
        ):
            partner = canonical.get(_fid_md5(fid_other).lower())
            if partner is None or not keep(partner):
                continue
            by_partner.setdefault(partner, []).append((fid_mine, fid_other, score))
        return by_partner

    def _file_matrix(self, collection, md5, fids, feature_to_idx, cache):
        """L2-normalised sparse tf matrix of one file, rows in `fids` order.

        Rows are normalised on the way in so a pair's cosine is one sparse
        matrix product with no per-pair normalisation, and `feature_to_idx` is
        shared across the build so any two files' matrices are column-compatible
        (they are widened to the current feature count before multiplying).
        """
        import numpy as np
        import scipy.sparse as sp

        cached = cache.get(md5)
        if cached is not None:
            return cached

        rows, cols, data = [], [], []
        batch = int(os.getenv("BIN_SIM_LOAD_BATCH", 5000))
        for start in range(0, len(fids), batch):
            window = fids[start : start + batch]
            pipe = self.r.pipeline(transaction=False)
            for fid in window:
                pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            for n, vec in enumerate(pipe.execute()):
                row = start + n
                norm = 0.0
                for _feat, tf in vec or ():
                    norm += float(tf) * float(tf)
                norm = norm**0.5
                if norm <= 0:
                    continue
                for feat_hash, tf in vec:
                    key = feat_hash.decode() if isinstance(feat_hash, bytes) else feat_hash
                    idx = feature_to_idx.get(key)
                    if idx is None:
                        idx = len(feature_to_idx)
                        feature_to_idx[key] = idx
                    rows.append(row)
                    cols.append(idx)
                    data.append(float(tf) / norm)

        mat = sp.csr_matrix(
            (
                np.asarray(data, dtype=float),
                (np.asarray(rows, dtype=np.int32), np.asarray(cols, dtype=np.int32)),
            ),
            shape=(len(fids), max(len(feature_to_idx), 1)),
        )
        cache[md5] = mat
        # Bounded: the walk touches one anchor and its candidates at a time, so a
        # small cache absorbs the repeats without holding the collection.
        limit = int(os.getenv("BIN_SIM_MATRIX_CACHE", 24))
        while len(cache) > limit:
            cache.pop(next(iter(cache)))
        return mat

    def _refine_pair_edges(self, mat_a, fids_a, mat_b, fids_b, threshold):
        """Function pairs above `threshold` between two files, sparsely.

        Both matrices are already row-normalised, so the cosine is `A · Bt` --
        and taking that product sparsely is the point: the old builder called
        sklearn's cosine_similarity, which materialises a dense |A| x |B| matrix
        (32MB for two 2000-function files) for every pair before thresholding
        it. Here the product only holds entries for function pairs that actually
        share a feature, so the cost follows the real overlap.
        """
        import numpy as np

        if mat_a.nnz == 0 or mat_b.nnz == 0:
            return []
        width = max(mat_a.shape[1], mat_b.shape[1])
        if mat_a.shape[1] != width:
            mat_a.resize((mat_a.shape[0], width))
        if mat_b.shape[1] != width:
            mat_b.resize((mat_b.shape[0], width))

        product = (mat_a @ mat_b.T).tocoo()
        keep = product.data >= threshold
        return [
            (fids_a[int(i)], fids_b[int(j)], float(s))
            for i, j, s in zip(
                product.row[keep], product.col[keep], np.clip(product.data[keep], 0, 1)
            )
        ]

    def build_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5_a=None,
        md5_b=None,
        min_cohesion=0.5,
        batch_uuid=None,
        pairs_key=None,
        offset=0,
        min_pair_score=None,
        job_service=None,
        job_id=None,
    ):
        """Build binary similarity diff docs and scores from the collection's
        function-sim edges, one anchor file at a time.

        The walk is over *files*, not pairs: each file's edges are taken once and
        grouped by the file on the other end, so a pair is visited exactly once
        (at whichever of its two files comes first in the walk) and a pair with
        no shared function is never visited at all. `pairs_key` holds that file
        walk for a chunked build and `offset` is an index into it.

        Where a file's edges come from is `bin_sim.edge_source`:

        `stored` reads the function-sim docs a BUILD_SIM baked
        (`sim:involves:file:{md5}`) -- cheapest, and the only option that needs
        no computation, but a collection uploaded with --skip-sim has none.

        `discover` computes them now and writes none, straight off the feature
        posting lists and funcid buckets that ingestion already wrote. That is
        the fast-bin-sim path: no function-sim graph to build, store or keep in
        sync, at the cost of redoing the discovery on every rebuild.

        `auto` (the default) picks per file: stored when that file's functions
        are in `built:functions:{algo}`, discover when they are not. A mixed
        collection -- some files similarity-built, some uploaded --skip-sim --
        therefore needs no flag, and neither does either pure case.
        """
        r = self.r

        from bsimvis.app.services.config_service import config_service

        # A pair that scores 0.01 is storage, an index entry and a bin_cluster
        # edge for a file pair nobody will ever open. An upper bound skips the
        # scoring of pairs that cannot reach this, and the score itself is
        # re-checked after matching because that bound is loose. 0 stores every
        # pair that shares a single function, which is what filled the index
        # with 1% pairs.
        if min_pair_score is None:
            min_pair_score = config_service.get("bin_sim.min_pair_score", 0.05)
        min_pair_score = float(min_pair_score or 0)

        # Edges below the function threshold are ignored even if an older,
        # looser build left them in the graph. Collection-sticky: a collection
        # keeps the min_score it was first built with, so reading raw config
        # here would drop edges the graph legitimately holds.
        from bsimvis.app.services.collection_config import get_collection_param

        func_sim_threshold = float(
            get_collection_param(
                collection,
                "min_score",
                config_service.get("similarity.min_score", 0.9),
            )
        )

        edge_source = str(config_service.get("bin_sim.edge_source", "auto")).lower()
        if edge_source not in ("auto", "stored", "discover", "tiered"):
            edge_source = "auto"
        # Tier 1 proposes a pair only when the two files share identical
        # functions carrying at least this share of the smaller file's weight.
        # "At least one shared function" is far too weak a filter on a corpus of
        # statically linked binaries -- one identical libc stub would drag every
        # pair into tier 2 and put the N^2 straight back.
        candidate_ratio = float(
            config_service.get("bin_sim.candidate_min_exact_ratio", 0.02)
        )

        exact_pair = bool(md5_a and md5_b)
        if exact_pair:
            # An explicit request for one pair is answered even when the pair
            # scores nothing: callers (/api/bin_sim/diff, the LLM pair
            # analysis) ask for a specific doc and need an answer, not a
            # threshold decision made on their behalf.
            min_pair_score = 0.0

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Starting Binary Similarity Build for collection {collection} (algo: {algo})",
            )

        # 1. Which files this build is responsible for walking.
        all_files_key = f"{collection}:all_files"
        collection_binaries = []
        for d in r.smembers(all_files_key) or ():
            k = d.decode() if isinstance(d, bytes) else str(d)
            if k.endswith(":meta"):
                continue
            parts = k.split(":")
            if len(parts) >= 3:
                collection_binaries.append(parts[2])
        collection_binaries = sorted(set(collection_binaries))

        containers = set(lineage_service.container_md5s(collection, r) or ())
        collection_binaries = [m for m in collection_binaries if m not in containers]
        # Function ids are built from these spellings, so an edge's md5 is
        # mapped back to one before any key is assembled from it.
        canonical = {m.lower(): m for m in collection_binaries}

        if exact_pair:
            # Every key below is built from the spelling the function-id index
            # uses, so a caller's md5 is mapped onto it first.
            md5_a = canonical.get(md5_a.lower(), md5_a)
            md5_b = canonical.get(md5_b.lower(), md5_b)
            if md5_a in containers or md5_b in containers:
                # Containers hold no code of their own; their score is rolled up
                # from their children by container_sim_service.
                if job_service and job_id:
                    job_service.add_log(
                        job_id, "Container pair: nothing to compare directly."
                    )
                    job_service.update_progress(job_id, 100)
                return True
            # One anchor is enough for one pair -- walking both ends would just
            # compute and overwrite the same doc twice.
            walk = [min(md5_a, md5_b)]
        elif batch_uuid:
            func_keys = r.smembers(f"{collection}:batch:{batch_uuid}:functions") or ()
            batch_binaries = set()
            for k in func_keys:
                k = k.decode() if isinstance(k, bytes) else str(k)
                parts = k.split(":")
                if len(parts) >= 3:
                    batch_binaries.add(parts[-2])
            walk = sorted(
                canonical[m.lower()]
                for m in batch_binaries
                if m not in containers and m.lower() in canonical
            )
        else:
            walk = list(collection_binaries)

        if len(collection_binaries) < 2 or not walk:
            if job_service and job_id:
                job_service.add_log(job_id, "Not enough binaries to compare.")
                job_service.update_progress(job_id, 100)
            return True

        # Chunking walks files, not pairs. Memory is bounded by one anchor
        # file's edges plus the weights of the files it touches, so the chunk
        # size is a latency/restart knob rather than the memory ceiling it had
        # to be when every pair in the chunk held two tf matrices.
        CHUNK_SIZE = int(os.getenv("BIN_SIM_FILE_CHUNK", 200))
        if offset == 0:
            if len(walk) > CHUNK_SIZE:
                pairs_key = f"{collection}:bin_sim_jobs:{job_id}:files"
                r.set(
                    pairs_key,
                    json.dumps(walk),
                    ex=int(os.getenv("BIN_SIM_PAIRS_TTL", 604800)),
                )
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"[*] {len(walk)} files to walk, stored at {pairs_key} -- run "
                        f"a later chunk with that pairs_key and an offset.",
                    )
            else:
                pairs_key = None
        else:
            if not pairs_key:
                return True
            raw = r.get(pairs_key)
            if not raw:
                # Expired or cleared. Returning quietly here is what let a build
                # that outran the TTL report every remaining chunk as a success.
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"[!] file walk key {pairs_key} is gone (expired or cleared). "
                        f"Nothing to resume at offset {offset} -- rerun from offset 0.",
                    )
                return True
            walk = json.loads(raw)

        total_files = len(walk)
        if offset >= total_files:
            if pairs_key:
                r.delete(pairs_key)
            return True

        # A pair belongs to whichever of its files the walk reaches first, so
        # membership is tested against the whole walk -- not this chunk.
        walk_rank = {m: i for i, m in enumerate(walk)}
        chunk = walk[offset : offset + CHUNK_SIZE]

        # Splice next chunk if needed
        if offset + CHUNK_SIZE < total_files:
            from bsimvis.app.services.job_service import JobType

            next_payload = {
                "collection": collection,
                "algo": algo,
                "md5_a": md5_a,
                "md5_b": md5_b,
                "min_cohesion": min_cohesion,
                "batch_uuid": batch_uuid,
                "pairs_key": pairs_key,
                "offset": offset + CHUNK_SIZE,
                "min_pair_score": min_pair_score,
            }
            if job_service and job_id:
                # job:* hashes live on job_service's queue redis, not bin_sim_service's
                # data redis (self.r/`r` here) -- looking this up on `r` always misses,
                # parent_id silently falls back to job_id itself (a leaf task with no
                # task_ids), and splice_tasks no-ops. That's why chunked builds always
                # stopped after exactly one CHUNK_SIZE chunk.
                spliced = job_service.splice_tasks(
                    parent_id=job_service.r.hget(f"job:{job_id}", "parent_id")
                    or job_id,
                    after_id=job_id,
                    new_tids=[(JobType.BUILD_BIN_SIM.value, next_payload)],
                )
                if not spliced:
                    # `parent_id` is "" on a job created outside a pipeline, so
                    # the fallback above points at the job itself -- a leaf with
                    # no task_ids for splice_tasks to extend. The chain ends
                    # here whatever the file total says.
                    job_service.add_log(
                        job_id,
                        f"[!] Could not queue the next chunk: this job has no "
                        f"parent pipeline to splice into. {offset + CHUNK_SIZE} "
                        f"of {total_files} files remain. Continue with "
                        f"pairs_key={pairs_key} offset={offset + CHUNK_SIZE}.",
                    )
        elif pairs_key:
            r.delete(pairs_key)

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Joining function-sim edges for files {offset} to "
                f"{offset + len(chunk)} of {total_files}...",
            )

        tag_meta_cache = load_tag_meta(r, collection)
        tags_rev = read_tags_rev(r, collection)

        funcs_cache = {}
        weight_cache = {}
        tags_cache = {}
        file_meta_cache = {}
        pair_scores = {}
        processed = 0
        pruned = 0
        dropped = 0
        no_edges = 0
        missing_scores = 0
        discovered = 0
        refined = 0
        not_candidates = 0
        sim_service = None
        matrix_cache = {}
        feature_to_idx = {}

        def file_meta(md5):
            if md5 not in file_meta_cache:
                raw = r.get(f"{collection}:file:{md5}:meta")
                meta = {}
                if raw:
                    meta = raw if isinstance(raw, dict) else json.loads(raw)
                    if isinstance(meta, str):
                        meta = json.loads(meta)
                file_meta_cache[md5] = meta if isinstance(meta, dict) else {}
            return file_meta_cache[md5]

        pipe = r.pipeline(transaction=False)
        for walked, m_a in enumerate(chunk):
            if exact_pair:
                other = max(md5_a, md5_b)

                def keep(partner, _other=other):
                    return partner == _other

            else:

                def keep(partner, _anchor=m_a):
                    # Whoever comes first in the walk owns the pair; a partner
                    # outside the walk (an older file on an incremental build)
                    # is always this anchor's responsibility.
                    rank = walk_rank.get(partner)
                    return rank is None or rank > walk_rank[_anchor]

            funcs_a = self._file_funcs(collection, m_a, funcs_cache)

            use_stored = edge_source == "stored" or (
                edge_source == "auto"
                and (
                    # Any stored edge is direct evidence the graph covers this
                    # file. Only when it has none does it matter whether that
                    # means "built, nothing matched" or "never built".
                    r.scard(f"{collection}:sim:involves:file:{m_a}") > 0
                    or self._file_was_sim_built(collection, algo, funcs_a)
                )
            )
            if use_stored:
                by_partner, missing = self._edges_by_partner(
                    collection, algo, m_a, canonical, keep, func_sim_threshold
                )
                missing_scores += missing
                if not by_partner:
                    no_edges += 1
            else:
                if sim_service is None:
                    from bsimvis.app.services.similarity_service import (
                        SimilarityService,
                    )

                    # One per build: its read caches are only valid for as long
                    # as ingestion is not writing, and they are what makes the
                    # walk share one fetch of each posting list.
                    sim_service = SimilarityService(r)
                if edge_source == "tiered":
                    by_partner = self._exact_edges_by_partner(
                        sim_service, collection, m_a, funcs_a, canonical, keep
                    )
                else:
                    by_partner = self._discovered_edges_by_partner(
                        sim_service,
                        collection,
                        algo,
                        m_a,
                        canonical,
                        keep,
                        func_sim_threshold,
                    )
                discovered += 1
            if exact_pair:
                by_partner.setdefault(other, [])

            self._func_weights(collection, sorted(funcs_a), weight_cache)
            weight_a = sum(weight_cache.get(f, 1.0) for f in funcs_a)

            for m_b, anchor_edges in by_partner.items():
                funcs_b = self._file_funcs(collection, m_b, funcs_cache)
                self._func_weights(collection, sorted(funcs_b), weight_cache)
                weight_b = sum(weight_cache.get(f, 1.0) for f in funcs_b)

                if edge_source == "tiered" and not exact_pair:
                    # Tier 1 found identical functions; tier 2 only runs when
                    # enough of the smaller file is in them. The exact edges are
                    # real matches, so their weight is a lower bound on what the
                    # pair shares -- a pair under the ratio would need the fuzzy
                    # 0.9-1.0 band to carry it on its own, which is the recall
                    # this trade knowingly gives up for a cost that scales.
                    exact_mass = sum(
                        weight_cache.get(fid, 1.0)
                        for fid in {e[0] for e in anchor_edges}
                    )
                    floor = min(weight_a, weight_b)
                    if floor > 0 and exact_mass / floor < candidate_ratio:
                        not_candidates += 1
                        continue

                    # Tier 2: the 0.9-1.0 band, for this candidate pair only.
                    mat_a = self._file_matrix(
                        collection,
                        m_a,
                        sorted(funcs_a),
                        feature_to_idx,
                        matrix_cache,
                    )
                    mat_b = self._file_matrix(
                        collection,
                        m_b,
                        sorted(funcs_b),
                        feature_to_idx,
                        matrix_cache,
                    )
                    fuzzy = self._refine_pair_edges(
                        mat_a,
                        sorted(funcs_a),
                        mat_b,
                        sorted(funcs_b),
                        func_sim_threshold,
                    )
                    if fuzzy:
                        # Exact edges win a duplicate: they are 1.0 by
                        # construction and the cosine can only tie them.
                        merged = {(u, v): s for u, v, s in fuzzy}
                        merged.update({(u, v): s for u, v, s in anchor_edges})
                        anchor_edges = [(u, v, s) for (u, v), s in merged.items()]
                    refined += 1

                # A pair is stored under its sorted md5s, and score_pair's diff
                # is oriented a->b, so the edges are flipped here rather than
                # scored twice.
                if m_a <= m_b:
                    key_a, key_b = m_a, m_b
                    edges = anchor_edges
                    pair_funcs_a, pair_funcs_b = funcs_a, funcs_b
                else:
                    key_a, key_b = m_b, m_a
                    edges = [(b, a, s) for a, b, s in anchor_edges]
                    pair_funcs_a, pair_funcs_b = funcs_b, funcs_a

                # Bound the pair from its edge list before fetching a single
                # tag: greedy matching uses each function once, so summing
                # `s * max(w_a, w_b)` over *every* edge can only overshoot the
                # numerator, and the denominator can lose at most
                # `sum over edges of min(w_a, w_b)`.
                if min_pair_score:
                    num_ub = 0.0
                    min_ub = 0.0
                    for fid_a, fid_b, score in edges:
                        w_a = weight_cache.get(fid_a, 1.0)
                        w_b = weight_cache.get(fid_b, 1.0)
                        num_ub += score * max(w_a, w_b)
                        min_ub += min(w_a, w_b)
                    if (
                        pair_score_upper_bound(num_ub, min_ub, weight_a, weight_b)
                        < min_pair_score
                    ):
                        pruned += 1
                        continue

                fid_tags = dict(
                    self._func_tags(collection, m_a, funcs_a, tags_cache)
                )
                fid_tags.update(self._func_tags(collection, m_b, funcs_b, tags_cache))

                def _feat(fid):
                    return weight_cache.get(fid, 1.0)

                common = score_pair(
                    edges,
                    pair_funcs_a,
                    pair_funcs_b,
                    _feat,
                    fid_tags,
                    tag_meta_cache,
                )

                if min_pair_score and common["score"] < min_pair_score:
                    dropped += 1
                    continue

                file_meta_a = file_meta(key_a)
                file_meta_b = file_meta(key_b)
                sid = f"{collection}:bin_sim:{algo}:{key_a}::{key_b}"
                pair_scores[(key_a, key_b)] = common["score"]

                doc = {
                    "md5_a": key_a,
                    "md5_b": key_b,
                    "algo": algo,
                    "architecture_a": file_meta_a.get("language_id", ""),
                    "architecture_b": file_meta_b.get("language_id", ""),
                    "functions_count_a": len(
                        self._file_funcs(collection, key_a, funcs_cache)
                    ),
                    "functions_count_b": len(
                        self._file_funcs(collection, key_b, funcs_cache)
                    ),
                    "computed_at": int(time.time() * 1000),
                    # Bumped by every tag write, so a stored split can be told apart
                    # from the tag state it was computed against without rebuilding.
                    "tags_rev": tags_rev,
                    # score / score_code / score_library / coverage / cluster counts /
                    # tag summaries / diff -- shared with the pool builder.
                    **common,
                }

                pipe.set(sid, json.dumps(doc))
                # `algo` is a provenance tag (which function similarity the clusters came
                # from), not a choice of file score. The sort score is always the
                # unweighted cohesion mean so it means the same thing in every namespace
                # and matches the pool-level score. The other aggregates stay in `doc`.
                _zadd_score_split(pipe, f"{collection}:bin_sim", algo, sid, common)
                pipe.sadd(f"{collection}:bin_sim:involves:{key_a}", sid)
                pipe.sadd(f"{collection}:bin_sim:involves:{key_b}", sid)
                pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)

                # Secondary indexes
                _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a, file_meta_b)

                processed += 1
                if processed % 100 == 0:
                    pipe.execute()
                    pipe = r.pipeline(transaction=False)

            if job_service and job_id and chunk:
                job_service.update_progress(
                    job_id,
                    10 + int((walked + 1) / len(chunk) * 80),
                    f"Walked {walked + 1}/{len(chunk)} files, {processed} pairs stored",
                )

        pipe.execute()

        if job_service and job_id and discovered:
            job_service.add_log(
                job_id,
                f"[*] {discovered}/{len(chunk)} files had their edges computed "
                f"on the fly (no function-sim docs written); "
                f"{len(chunk) - discovered} read stored edges.",
            )
        if job_service and job_id and edge_source == "tiered":
            job_service.add_log(
                job_id,
                f"[*] tier 1 (exact function matches) proposed "
                f"{refined + not_candidates} pairs, {not_candidates} of them "
                f"under candidate_min_exact_ratio={candidate_ratio}; "
                f"{refined} were refined against the 0.9-1.0 band.",
            )
        if no_edges and job_service and job_id:
            job_service.add_log(
                job_id,
                f"[!] {no_edges}/{len(chunk)} walked files were similarity-built "
                f"for algo {algo} but have no stored function-sim edges, so they "
                f"form no pair. Rebuild their function similarities, or set "
                f"bin_sim.edge_source = discover to compute edges per build.",
            )
        if missing_scores and job_service and job_id:
            job_service.add_log(
                job_id,
                f"[!] {missing_scores} function-sim edges are indexed against a "
                f"file but absent from {collection}:sim:score:{algo}. Their pairs "
                f"are scored without them -- rebuild the function similarities.",
            )

        if not exact_pair:
            # A filtered collection is missing pairs on purpose. Record the
            # cutoff so a reader can tell "no pair below 0.05 exists" from
            # "nothing was ever built" -- written even at 0, so the marker
            # never outlives the filter that set it.
            r.set(f"{collection}:bin_sim:min_pair_score:{algo}", min_pair_score)
        if min_pair_score:
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] min_pair_score={min_pair_score}: {pruned} pairs skipped on "
                    f"the bound, {dropped} dropped on their real score, "
                    f"{processed} stored",
                )

        # Containers were kept out of the walk above because they hold no code
        # of their own. Roll the child pairs it just wrote up the containment
        # edges, so an APK can be compared as a whole.
        from bsimvis.app.services import container_sim_service

        container_sim_service.build_container_sims(
            collection,
            algo,
            pair_scores,
            r,
            job_service=job_service,
            job_id=job_id,
        )

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Completed binary similarity build for {processed} pairs."
            )

        return True

    def clear_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5=None,
        job_service=None,
        job_id=None,
    ):
        """
        Clears binary similarity scores.
        If md5 is provided, clears only pairs involving that md5.
        """
        r = self.r
        from bsimvis.app.services.cluster_common import clear_hier_state

        clear_hier_state(r, f"{collection}:bin_cluster:hier:{algo}")
        clear_hier_state(r, f"{collection}:bin_cluster:hier:{algo}:container")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Clearing binary similarities (md5: {md5 or 'ALL'})"
            )

        if md5:
            involves_key = f"{collection}:bin_sim:involves:{md5}"
            sids = [
                s.decode() if isinstance(s, bytes) else str(s)
                for s in (r.smembers(involves_key) or ())
            ]
            if sids:
                # Read the docs before deleting them: the secondary indexes are
                # keyed by the fields they denormalise, so a sid dropped without
                # its doc stays in every index bucket it was ever filed under.
                reader = r.pipeline(transaction=False)
                for sid in sids:
                    reader.get(sid)
                raw_docs = reader.execute()

                meta_cache = {}

                def _meta(m):
                    if m not in meta_cache:
                        raw = r.get(f"{collection}:file:{m}:meta")
                        try:
                            meta_cache[m] = json.loads(raw) if raw else {}
                        except (ValueError, TypeError):
                            meta_cache[m] = {}
                    return meta_cache[m]

                pipe = r.pipeline(transaction=False)
                for sid, raw in zip(sids, raw_docs):
                    try:
                        doc = json.loads(raw) if raw else {}
                    except (ValueError, TypeError):
                        doc = {}

                    pipe.delete(sid)
                    pipe.zrem(f"{collection}:bin_sim:score:{algo}", sid)
                    pipe.zrem(f"{collection}:bin_sim:score_code:{algo}", sid)
                    pipe.zrem(f"{collection}:bin_sim:score_library:{algo}", sid)
                    pipe.zrem(f"{collection}:bin_sim:score_content:{algo}", sid)
                    pipe.srem(f"{collection}:bin_sim:built:{algo}", sid)

                    m_a, m_b = doc.get("md5_a"), doc.get("md5_b")
                    if not (m_a and m_b):
                        tail = sid.split(f"{collection}:bin_sim:{algo}:")
                        if len(tail) == 2 and "::" in tail[1]:
                            m_a, m_b = tail[1].split("::", 1)
                    other_md5 = m_b if m_a == md5 else m_a
                    if other_md5:
                        pipe.srem(f"{collection}:bin_sim:involves:{other_md5}", sid)
                    if doc:
                        _unindex_bin_sim_pair(
                            pipe, collection, sid, doc, _meta(m_a), _meta(m_b)
                        )

                pipe.delete(involves_key)
                pipe.execute()
        else:
            patterns = [
                f"{collection}:bin_sim:{algo}:*",
                f"{collection}:bin_sim:involves:*",
            ]

            for pattern in patterns:
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                    if keys:
                        r.delete(*keys)
                    if cursor == 0:
                        break

            r.delete(f"{collection}:bin_sim:built:{algo}")
            r.delete(f"{collection}:bin_sim:min_pair_score:{algo}")
            r.delete(f"{collection}:all_bin_sims")

            # Actual secondary indexes live under idx:bin_sim:* / reg:bin_sim:*
            # (written by _index_bin_sim_pair), not the bin_sim:score:{algo}-style
            # keys above. Those were never populated by the writer, so clearing
            # them was a no-op that left every idx:/reg: entry orphaned across
            # every clear+rebuild cycle. ponytail: one algo per collection
            # (assumed elsewhere in this codebase too), so wipe the whole index
            # rather than filtering per-sid.
            for field in BIN_SIM_NUM_FIELDS:
                r.delete(f"{collection}:idx:bin_sim:{field}")
            for field in BIN_SIM_TAG_FIELDS:
                reg_key = f"{collection}:reg:bin_sim:{field}"
                buckets = r.smembers(reg_key)
                if buckets:
                    bucket_keys = [
                        b.decode() if isinstance(b, bytes) else b for b in buckets
                    ]
                    r.delete(*bucket_keys)
                r.delete(reg_key)

        if job_service and job_id:
            job_service.update_progress(job_id, 100, "Cleared binary similarities.")

        return True

    def reindex_bin_sim(
        self, collection, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """
        Rebuilds secondary indexes for all existing bin_sim pairs in the collection.
        Use after deploy or when indexes are missing.
        """
        r = self.r
        if job_service and job_id:
            job_service.add_log(
                job_id, f"[*] Reindexing bin_sim pairs for collection {collection}"
            )

        built_key = f"{collection}:bin_sim:built:{algo}"
        sids = list(r.smembers(built_key))
        if not sids:
            if job_service and job_id:
                job_service.add_log(job_id, "No bin_sim docs found to reindex.")
                job_service.update_progress(job_id, 100)
            return True

        sids = [s.decode() if isinstance(s, bytes) else s for s in sids]
        total = len(sids)

        # Pre-fetch all file meta we might need
        md5s = set()
        for sid in sids:
            try:
                rest = sid.split(f"bin_sim:{algo}:")[1]
                m_a, m_b = rest.split("::")
                md5s.update([m_a, m_b])
            except Exception:
                pass

        file_meta_cache = {}
        md5_list = list(md5s)
        if md5_list:
            pipe_meta = r.pipeline(transaction=False)
            for md5 in md5_list:
                pipe_meta.get(f"{collection}:file:{md5}:meta")
            for md5, res in zip(md5_list, pipe_meta.execute()):
                if res:
                    m = json.loads(res) if not isinstance(res, dict) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    file_meta_cache[md5] = m if isinstance(m, dict) else {}
                else:
                    file_meta_cache[md5] = {}

        # Fetch all docs and reindex
        pipe = r.pipeline(transaction=False)
        for sid in sids:
            pipe.get(sid)
        docs = pipe.execute()

        pipe = r.pipeline(transaction=False)
        for i, (sid, res) in enumerate(zip(sids, docs)):
            if not res:
                continue
            doc = json.loads(res) if not isinstance(res, dict) else res
            if isinstance(doc, str):
                doc = json.loads(doc)
            m_a = doc.get("md5_a", "")
            m_b = doc.get("md5_b", "")
            _index_bin_sim_pair(
                pipe,
                collection,
                sid,
                doc,
                file_meta_cache.get(m_a),
                file_meta_cache.get(m_b),
            )
            if (i + 1) % 200 == 0:
                pipe.execute()
                pipe = r.pipeline(transaction=False)
                if job_service and job_id:
                    job_service.update_progress(job_id, int((i + 1) / total * 100))

        pipe.execute()

        if job_service and job_id:
            job_service.update_progress(
                job_id, 100, f"Reindexed {total} bin_sim pairs."
            )
        return True

    def resplit_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5=None,
        sid=None,
        job_service=None,
        job_id=None,
    ):
        """Recompute the tag split of stored pairs from their persisted diff.

        Tags never enter the pair score -- only its split -- so re-tagging does
        not need the build pipeline (BSim queries, greedy matching, clustering)
        run again. Everything `AxisSplit` consumes is already in the doc: the
        matched edges, the leftovers, and the function ids to read tags from.
        This is what the "refresh split" button behind LLM tagging calls.
        """
        r = self.r
        built_key = f"{collection}:bin_sim:built:{algo}"
        sids = [
            s.decode() if isinstance(s, bytes) else s for s in r.smembers(built_key)
        ]
        # Tagging touches the functions of particular binaries, so only pairs
        # naming one of them can change; everything else would be rewritten to
        # an identical value. `md5` takes one or several -- the pair view sends
        # both of its sides. Omit it and the whole collection is resplit.
        if sid is not None:
            sids = [sid] if sid in set(sids) else []
        else:
            wanted = [md5] if isinstance(md5, str) else list(md5 or ())
            if wanted:
                sids = [s for s in sids if any(m and m in s for m in wanted)]
        total = len(sids)
        if not total:
            if job_service and job_id:
                job_service.update_progress(job_id, 100, "No bin_sim docs to resplit.")
            return True

        if job_service and job_id:
            job_service.add_log(job_id, f"[*] Resplitting {total} bin_sim pairs")

        tag_meta = load_tag_meta(r, collection)
        rev = read_tags_rev(r, collection)
        # fid -> tags, kept across pairs: the same libc function shows up in
        # every pair of the collection and is worth reading once.
        fid_tags = {}
        feat = {}
        done = 0

        for start in range(0, total, 200):
            chunk = sids[start : start + 200]
            pipe = r.pipeline(transaction=False)
            for sid in chunk:
                pipe.get(sid)
            docs = []
            for sid, raw in zip(chunk, pipe.execute()):
                if not raw:
                    continue
                doc = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
                if isinstance(doc, str):
                    doc = json.loads(doc)
                if isinstance(doc, dict):
                    docs.append((sid, doc))

            wanted = set()
            for _, doc in docs:
                diff = doc.get("diff") or {}
                for m in diff.get("matched") or []:
                    wanted.add(m.get("func_a"))
                    wanted.add(m.get("func_b"))
                for u in (diff.get("unique_to_a") or []) + (
                    diff.get("unique_to_b") or []
                ):
                    wanted.add(u.get("func_id"))
            missing = [f for f in wanted if f and f not in feat]
            if missing:
                pipe = r.pipeline(transaction=False)
                for fid in missing:
                    pipe.get(f"{fid}:meta")
                for fid, res in zip(missing, pipe.execute()):
                    m = {}
                    if res:
                        m = json.loads(res.decode() if isinstance(res, bytes) else res)
                        if isinstance(m, str):
                            try:
                                m = json.loads(m)
                            except ValueError:
                                m = {}
                    m = m if isinstance(m, dict) else {}
                    try:
                        feat[fid] = float(m.get("bsim_features_count", 1.0) or 1.0)
                    except (TypeError, ValueError):
                        feat[fid] = 1.0
                    tags = merge_tag_fields(m)
                    if tags:
                        fid_tags[fid] = tags

            pipe = r.pipeline(transaction=False)
            for sid, doc in docs:
                diff = doc.get("diff") or {}
                split = AxisSplit(fid_tags, tag_meta)
                total_a = total_b = 0.0
                for m in diff.get("matched") or []:
                    fa, fb = m.get("func_a"), m.get("func_b")
                    wa, wb = feat.get(fa, 1.0), feat.get(fb, 1.0)
                    split.add_match(fa, fb, m.get("similarity", 0.0), wa, wb)
                    total_a += wa
                    total_b += wb
                for side, rows in (
                    ("a", diff.get("unique_to_a") or []),
                    ("b", diff.get("unique_to_b") or []),
                ):
                    for u in rows:
                        w = feat.get(u.get("func_id"), 1.0) or 1.0
                        split.add_unique(u.get("func_id"), w, side)
                        if side == "a":
                            total_a += w
                        else:
                            total_b += w
                doc.update(split.summaries(total_a, total_b, tag_meta))
                doc["tags_rev"] = rev
                matched = diff.get("matched") or []
                u_a = diff.get("unique_to_a") or []
                u_b = diff.get("unique_to_b") or []
                score_library, score_code = code_library_split(
                    matched, u_a, u_b, fid_tags
                )
                doc["score_library"] = score_library
                doc["score_code"] = score_code
                pipe.set(sid, json.dumps(doc))
                _zadd_score_split(pipe, f"{collection}:bin_sim", algo, sid, doc)
                # Search sorts off the `idx:` ZSETs, not the ones above, so a
                # resplit that skipped this left "sort by Code" on pre-resplit
                # values (or, for a pair that never had one, off the list).
                _index_bin_sim_pair(pipe, collection, sid, doc)
            pipe.execute()

            done += len(chunk)
            if job_service and job_id:
                job_service.update_progress(job_id, int(done / total * 100))

        if job_service and job_id:
            job_service.update_progress(job_id, 100, f"Resplit {total} bin_sim pairs.")
        return True


bin_sim_service = BinSimService()
