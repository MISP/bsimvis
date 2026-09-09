import os
import time
import json
import logging
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


def candidate_rows(mass, norm2, threshold):
    """Which functions on one side could still form an edge into the other.

    `cosine_similarity` scores an edge `<a,b> / (||a|| * ||b||)`. Restricting
    the dot product to the features `b` actually has can only grow it, and
    Cauchy-Schwarz over those coordinates gives

        <a,b> <= || a restricted to features(B) || * ||b||

    so `cos(a, b) <= || a restricted to features(B) || / ||a||` for *every* b in
    B at once. A function whose mass inside B's feature set falls below
    `threshold * ||a||` can therefore not reach the edge threshold against any
    function of B, and contributes nothing to the pair's numerator.

    `mass` is that restricted squared norm per row, `norm2` the full squared
    norm per row; both come straight from the sparse matrix the builder already
    has. Returns the boolean row mask of the survivors, which is what the
    numerator bound is built from. A zero-norm row (no features at all) has no
    cosine edge to lose and stays in -- an exact funcid match can still pair it.
    """
    return mass >= (threshold * threshold) * norm2


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

    def _discover_binaries(self, collection, target_md5, min_cohesion=0.1):
        """
        Finds candidate similar binaries using Rare Feature Anchoring.
        """
        func_set_key = f"{collection}:idx:file:functions:{target_md5}"
        fids = self.r.smembers(func_set_key)
        if not fids:
            return []
        
        # 1. Collect all features for target binary
        pipe = self.r.pipeline(transaction=False)
        for fid in fids:
            fid_str = fid.decode() if isinstance(fid, bytes) else str(fid)
            fid_str = fid_str.replace(":meta", "")
            pipe.zrange(f"{fid_str}:vec:tf", 0, -1)
        
        results = pipe.execute()
        
        unique_features = set()
        for res in results:
            for f_hash in res:
                f_hash_str = f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash)
                unique_features.add(f_hash_str)
                
        if not unique_features:
            return []
            
        # 2. Sort features by rarity (ZCARD)
        pipe = self.r.pipeline(transaction=False)
        for f_hash in unique_features:
            pipe.zcard(f"{collection}:feature:{f_hash}:functions")
        
        sizes = pipe.execute()
        # Filter out features that only appear in this binary (size <= 1)
        # as they cannot help us find other similar binaries.
        # Also filter out extremely common features to avoid blowing up the pipeline.
        valid_features = [(f, s) for f, s in zip(unique_features, sizes) if 1 < s < 1000]
        feature_sizes = sorted(valid_features, key=lambda x: x[1])
        
        # 3. Use more features for discovery to catch low-similarity file pairs
        # Taking only 25% misses pairs that share only semi-common library functions.
        num_anchors = max(10, int(len(valid_features) * 1.0))
        if num_anchors > 5000:
            num_anchors = 5000
        anchors = [f_hash for f_hash, size in feature_sizes[:num_anchors]]
        
        # 4. Query reverse index for anchors (ZSET)
        pipe = self.r.pipeline(transaction=False)
        for f_hash in anchors:
            pipe.zrange(f"{collection}:feature:{f_hash}:functions", 0, -1)
            
        anchor_results = pipe.execute()
        
        # 5. Vote
        from collections import defaultdict
        votes = defaultdict(int)
        for res in anchor_results:
            for fid in res:
                fid_str = fid.decode() if isinstance(fid, bytes) else str(fid)
                parts = fid_str.split(":")
                if len(parts) >= 4:
                    cand_md5 = parts[-2]
                    if cand_md5 != target_md5:
                        votes[cand_md5] += 1
                        
        # 6. Filter by min votes (at least 2 for noise reduction)
        min_votes = 2
        return [md5 for md5, count in votes.items() if count >= min_votes]

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
        """
        Builds binary similarity diff docs and scores for pairs of binaries.
        Uses a cluster-first greedy sweep algorithm.
        """
        r = self.r
        start_time = time.time()

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Starting Binary Similarity Build for collection {collection} (algo: {algo})",
            )

        # 1. Fetch all files (binaries)
        file_keys = []
        if md5_a and md5_b:
            binaries = [md5_a, md5_b]
        else:
            all_files_key = f"{collection}:all_files"
            file_keys = [
                d.decode() if isinstance(d, bytes) else str(d)
                for d in r.smembers(all_files_key)
            ]
            binaries = []
            for k in file_keys:
                if k.endswith(":meta"):
                    continue
                parts = k.split(":")
                if len(parts) >= 3:
                    binaries.append(parts[2])
            binaries = list(set(binaries))

        containers = lineage_service.container_md5s(collection, r)
        if containers:
            binaries = [m for m in binaries if m not in containers]

        num_binaries = len(binaries)
        if num_binaries < 2:
            if job_service and job_id:
                job_service.add_log(job_id, "Not enough binaries to compare.")
                job_service.update_progress(job_id, 100)
            return True

        # Generate Pairs and Chunking
        # Memory per chunk tracks the number of distinct binaries the chunk
        # touches, not the number of pairs: every function of every binary in
        # it gets its meta and tf vector pulled into this process. Pairs are
        # sorted, so a chunk is a few `a` values against a wide spread of `b`
        # values and touches nearly the whole collection whatever its size.
        # Lower this when the collection is too big to hold at once.
        CHUNK_SIZE = int(os.getenv("BIN_SIM_CHUNK_SIZE", 10000))
        if offset == 0:
            if md5_a and md5_b:
                if md5_a < md5_b:
                    pairs = [(md5_a, md5_b)]
                else:
                    pairs = [(md5_b, md5_a)]
            else:
                pairs = []
                if batch_uuid:
                    func_keys = r.smembers(f"{collection}:batch:{batch_uuid}:functions")
                    batch_binaries = set(
                        (k.decode() if isinstance(k, bytes) else k).split(":")[-2]
                        for k in func_keys
                        if len(k.split(":")) >= 3
                    )
                    batch_binaries = list(batch_binaries - set(containers))
                    # Fast discovery using Rare Feature Anchoring
                    seen_pairs = set()
                    for target_md5 in batch_binaries:
                        cands = self._discover_binaries(collection, target_md5, min_cohesion)
                        for cand in cands:
                            if cand in containers: continue
                            if cand == target_md5: continue
                            b1, b2 = (target_md5, cand) if target_md5 < cand else (cand, target_md5)
                            if (b1, b2) not in seen_pairs:
                                seen_pairs.add((b1, b2))
                                pairs.append((b1, b2))
                else:
                    # Fast discovery using Rare Feature Anchoring
                    seen_pairs = set()
                    for target_md5 in binaries:
                        cands = self._discover_binaries(collection, target_md5, min_cohesion)
                        for cand in cands:
                            if cand in containers: continue
                            if cand == target_md5: continue
                            b1, b2 = (target_md5, cand) if target_md5 < cand else (cand, target_md5)
                            if (b1, b2) not in seen_pairs:
                                seen_pairs.add((b1, b2))
                                pairs.append((b1, b2))
                pairs.sort()
                # Pairs are deterministic for chunking

            if len(pairs) > CHUNK_SIZE:
                pairs_key = f"{collection}:bin_sim_jobs:{job_id}:pairs"
                # Store all pairs as JSON
                r.set(
                    pairs_key,
                    json.dumps(pairs),
                    ex=int(os.getenv("BIN_SIM_PAIRS_TTL", 604800)),
                )
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"[*] {len(pairs)} pairs stored at {pairs_key} -- run a "
                        f"later chunk with that pairs_key and an offset.",
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
                        f"[!] pairs key {pairs_key} is gone (expired or cleared). "
                        f"Nothing to resume at offset {offset} -- rerun from offset 0.",
                    )
                return True
            pairs = json.loads(raw)

        total_pairs = len(pairs)
        if offset >= total_pairs:
            if pairs_key:
                r.delete(pairs_key)
            return True

        chunk_pairs = pairs[offset : offset + CHUNK_SIZE]

        # Splice next chunk if needed
        if offset + CHUNK_SIZE < total_pairs:
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
                    # here whatever the pair total says.
                    job_service.add_log(
                        job_id,
                        f"[!] Could not queue the next chunk: this job has no "
                        f"parent pipeline to splice into. {offset + CHUNK_SIZE} "
                        f"of {total_pairs} pairs remain. Continue with "
                        f"pairs_key={pairs_key} offset={offset + CHUNK_SIZE}.",
                    )
        elif pairs_key:
            r.delete(pairs_key)

        pairs = chunk_pairs

        # Re-derive binaries list so we only fetch metadata for binaries in this chunk
        chunk_binaries = set()
        for p in pairs:
            chunk_binaries.update(p)
        binaries = list(chunk_binaries)
        num_binaries = len(binaries)

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Computing similarities for pairs {offset} to {offset+len(pairs)} out of {total_pairs}...",
            )
        # 2. Load each binary's function IDs + counts (needed below for coverage,
        # bsim_features_count, and the diff doc's functions_count_a/b).
        binary_fids = {}
        binary_func_counts = {}
        for md5 in binaries:
            func_set_key = f"{collection}:idx:file:functions:{md5}"
            raw_ids = r.smembers(func_set_key)
            binary_func_counts[md5] = len(raw_ids)
            fids = [
                (
                    fid.decode().replace(":meta", "")
                    if isinstance(fid, bytes)
                    else str(fid).replace(":meta", "")
                )
                for fid in raw_ids
            ]
            binary_fids[md5] = set(fids)

        # 3. Load metadata, vectors and funcid hashes -- one binary at a time.
        #
        # This was a single un-batched pipeline over every function in the
        # chunk whose replies were then kept whole: a parsed meta dict and a
        # list of (feature, tf) tuples per function, for every function of
        # every binary the chunk touches. On a collection of 1.5M functions
        # that is ~10GB of Python objects, held for the whole chunk, so that
        # the code below could read one number out of each meta and fold each
        # vector into a sparse row.
        #
        # Nothing downstream wants the raw replies, so each binary is folded
        # into its matrix as it arrives and only what is actually read
        # survives: a weight, the tags, and the funcid row lists. Feature
        # columns are numbered as they are first seen and every matrix widened
        # to the final count afterwards, which keeps this one pass over the
        # data rather than one to collect features and another to build.
        import numpy as np
        import scipy.sparse as sp

        func_weight = {}
        fid_tags = {}
        binary_ordered_fids = {}
        binary_matrices = {}
        binary_hash_rows = {}
        feature_to_idx = {}

        total_functions = sum(len(f) for f in binary_fids.values())
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"[*] Loading metadata and vectors for {total_functions} functions "
                f"across {len(binaries)} binaries...",
            )

        LOAD_BATCH = int(os.getenv("BIN_SIM_LOAD_BATCH", 5000))
        loaded = 0
        for md5 in binaries:
            # Sorted so a rebuild numbers the rows the same way twice running.
            fids = sorted(binary_fids[md5])
            binary_ordered_fids[md5] = fids
            rows, cols, data = [], [], []
            hash_rows = {}

            for start_i in range(0, len(fids), LOAD_BATCH):
                batch = fids[start_i : start_i + LOAD_BATCH]
                pipe_load = r.pipeline(transaction=False)
                for fid in batch:
                    pipe_load.get(f"{fid}:meta")
                    pipe_load.get(f"{fid}:funcid")
                    pipe_load.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
                results = pipe_load.execute()

                for n, fid in enumerate(batch):
                    row = start_i + n
                    res_meta = results[n * 3]
                    res_funcid = results[n * 3 + 1]
                    res_vec = results[n * 3 + 2]

                    meta = {}
                    if res_meta:
                        meta = (
                            json.loads(res_meta)
                            if not isinstance(res_meta, dict)
                            else res_meta
                        )
                        if isinstance(meta, str):
                            try:
                                meta = json.loads(meta)
                            except ValueError:
                                meta = {}
                        if not isinstance(meta, dict):
                            meta = {}

                    # The only two things any caller reads out of a function's
                    # meta. Normalising the tags here also keeps that off the
                    # per-matched-edge path, as it was before.
                    func_weight[fid] = float(meta.get("bsim_features_count", 1.0))
                    tags = merge_tag_fields(meta)
                    if tags:
                        fid_tags[fid] = tags

                    if res_funcid:
                        digest = (
                            res_funcid.decode()
                            if isinstance(res_funcid, bytes)
                            else res_funcid
                        )
                        hash_rows.setdefault(digest, []).append(row)

                    for feat_hash, tf in res_vec or ():
                        idx = feature_to_idx.get(feat_hash)
                        if idx is None:
                            idx = len(feature_to_idx)
                            feature_to_idx[feat_hash] = idx
                        rows.append(row)
                        cols.append(idx)
                        data.append(float(tf))

                results = None
                loaded += len(batch)

            binary_hash_rows[md5] = hash_rows
            # Built at the width known so far; widened to the final count below.
            # Every column index here came from feature_to_idx, so none of them
            # can fall outside it.
            binary_matrices[md5] = sp.csr_matrix(
                (
                    np.asarray(data, dtype=float),
                    (
                        np.asarray(rows, dtype=np.int32),
                        np.asarray(cols, dtype=np.int32),
                    ),
                ),
                shape=(len(fids), max(len(feature_to_idx), 1)),
            )
            rows = cols = data = None

            if job_service and job_id and total_functions:
                job_service.update_progress(
                    job_id,
                    int(loaded / total_functions * 10),
                    f"Loaded {loaded}/{total_functions} functions",
                )

        num_features = max(len(feature_to_idx), 1)
        for mat in binary_matrices.values():
            mat.resize((mat.shape[0], num_features))

        tag_meta_cache = load_tag_meta(r, collection) if fid_tags else {}
        tags_rev = read_tags_rev(r, collection)

        # 5. Process Pairs (Direct in-memory Similarity Matching)
        processed = 0
        pipe = r.pipeline(transaction=False)
        pair_scores = {}

        # Pre-fetch file metadata for all binaries (for indexing)
        file_meta_cache = {}
        pipe_meta = r.pipeline(transaction=False)
        for md5 in binaries:
            pipe_meta.get(f"{collection}:file:{md5}:meta")
        meta_results = pipe_meta.execute()
        for md5, res in zip(binaries, meta_results):
            if res:
                m = json.loads(res) if not isinstance(res, dict) else res
                if isinstance(m, str):
                    m = json.loads(m)
                file_meta_cache[md5] = m if isinstance(m, dict) else {}
            else:
                file_meta_cache[md5] = {}
                
        from sklearn.metrics.pairwise import cosine_similarity

        # Per-binary state for the pruning bound, built only when a threshold
        # was asked for -- `sq` is a second copy of every matrix, which is not
        # worth carrying for a build that is going to score every pair anyway.
        # It holds the squared tf values so one sparse mat-vec against another
        # binary's feature mask yields every row's restricted squared norm at
        # once; `norm2` is the same row sums unrestricted. Raw weights (no 1.0
        # clamp) because a smaller denominator is the safe side of the bound.
        binary_sq = {}
        binary_norm2 = {}
        binary_row_weights = {}
        binary_total_weight = {}
        binary_feature_cols = {}
        feature_mask = None
        pruned = 0
        if min_pair_score:
            for md5 in binaries:
                mat = binary_matrices[md5]
                sq = mat.copy()
                sq.data = sq.data * sq.data
                binary_sq[md5] = sq
                binary_norm2[md5] = np.asarray(sq.sum(axis=1)).ravel()
                weights = np.array(
                    [func_weight.get(fid, 1.0) for fid in binary_ordered_fids[md5]],
                    dtype=float,
                )
                binary_row_weights[md5] = weights
                binary_total_weight[md5] = float(weights.sum())
                binary_feature_cols[md5] = np.unique(mat.indices)

            # Scratch mask over the chunk's global feature space, refilled per
            # pair rather than reallocated (num_features runs to millions).
            feature_mask = np.zeros(num_features, dtype=float)

        from bsimvis.app.services.config_service import config_service
        # Get threshold dynamically like similarity_service does, default 0.9 if not provided
        min_score_val = min_cohesion if min_cohesion is not None else config_service.get("similarity.min_score", 0.9)
        # Use 0.9 as strict cutoff for edges, min_cohesion is for binary cohesion.
        # Function sim threshold should be config's min_score.
        func_sim_threshold = config_service.get("similarity.min_score", 0.9)

        for m_a, m_b in pairs:
            file_meta_a = file_meta_cache.get(m_a, {})
            file_meta_b = file_meta_cache.get(m_b, {})

            fids_a = binary_ordered_fids[m_a]
            fids_b = binary_ordered_fids[m_b]

            # Prune before the O(|A| x |B|) cosine, not after. The bound below
            # is exact: it can only overshoot the score `score_pair` would have
            # returned, so a pair it puts under `min_pair_score` could not have
            # reached the threshold and is not worth computing. Costs one
            # sparse mat-vec per side, against the |A| x |B| product it saves.
            if min_pair_score:
                cols_b = binary_feature_cols[m_b]
                feature_mask[cols_b] = 1.0
                cand_a = candidate_rows(
                    binary_sq[m_a].dot(feature_mask),
                    binary_norm2[m_a],
                    func_sim_threshold,
                )
                feature_mask[cols_b] = 0.0

                cols_a = binary_feature_cols[m_a]
                feature_mask[cols_a] = 1.0
                cand_b = candidate_rows(
                    binary_sq[m_b].dot(feature_mask),
                    binary_norm2[m_b],
                    func_sim_threshold,
                )
                feature_mask[cols_a] = 0.0

                hash_rows_a = binary_hash_rows[m_a]
                hash_rows_b = binary_hash_rows[m_b]
                for h in hash_rows_a.keys() & hash_rows_b.keys():
                    cand_a[hash_rows_a[h]] = True
                    cand_b[hash_rows_b[h]] = True

                weight_a = binary_total_weight[m_a]
                weight_b = binary_total_weight[m_b]
                # Greedy matching uses each function once, so the numerator's
                # `sum of s * max(w_a, w_b)` is at most the two candidate sides
                # added up; `min(W(A), W(B))` is the most the denominator can
                # lose. Both are the loosest-safe direction.
                bound = pair_score_upper_bound(
                    float(binary_row_weights[m_a][cand_a].sum())
                    + float(binary_row_weights[m_b][cand_b].sum()),
                    min(weight_a, weight_b),
                    weight_a,
                    weight_b,
                )
                if bound < min_pair_score:
                    pruned += 1
                    continue

            edges = []
            
            # Vector similarity using cached sparse matrices
            mat_a = binary_matrices[m_a]
            mat_b = binary_matrices[m_b]
            if mat_a.nnz > 0 and mat_b.nnz > 0:
                sim_matrix = cosine_similarity(mat_a, mat_b)
                rows, cols = np.where(sim_matrix >= func_sim_threshold)
                for r_idx, c_idx in zip(rows, cols):
                    score = float(sim_matrix[r_idx, c_idx])
                    edges.append((fids_a[r_idx], fids_b[c_idx], score))
            
            # Exact Hash Matches for small functions (or identical large ones, though vectors will catch large ones)
            # Find common exact hashes
            hash_rows_a = binary_hash_rows[m_a]
            for digest, rows_b in binary_hash_rows[m_b].items():
                for row_a in hash_rows_a.get(digest, ()):
                    fid_a = fids_a[row_a]
                    for row_b in rows_b:
                        edges.append((fid_a, fids_b[row_b], 1.0))
                        
            # Remove duplicate edges and keep the highest score if any overlaps
            unique_edges = {}
            for u, v, score in edges:
                key = (u, v)
                if key not in unique_edges or score > unique_edges[key]:
                    unique_edges[key] = score
            edges = [(u, v, s) for (u, v), s in unique_edges.items()]

            all_funcs_a_total = binary_fids[m_a]
            all_funcs_b_total = binary_fids[m_b]

            def _feat(fid):
                return func_weight.get(fid, 1.0)

            common = score_pair(
                edges,
                all_funcs_a_total,
                all_funcs_b_total,
                _feat,
                fid_tags,
                tag_meta_cache,
            )

            sid = f"{collection}:bin_sim:{algo}:{m_a}::{m_b}"
            pair_scores[(m_a, m_b)] = common["score"]

            doc = {
                "md5_a": m_a,
                "md5_b": m_b,
                "algo": algo,
                "architecture_a": file_meta_a.get("language_id", ""),
                "architecture_b": file_meta_b.get("language_id", ""),
                "functions_count_a": binary_func_counts.get(m_a, 0),
                "functions_count_b": binary_func_counts.get(m_b, 0),
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
            pipe.sadd(f"{collection}:bin_sim:involves:{m_a}", sid)
            pipe.sadd(f"{collection}:bin_sim:involves:{m_b}", sid)
            pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)

            # Secondary indexes
            _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a, file_meta_b)

            processed += 1

            if processed % 100 == 0:
                pipe.execute()
                if job_service and job_id:
                    pct = 10 + int(processed / len(pairs) * 80)
                    job_service.update_progress(
                        job_id, pct, f"Processed {processed}/{len(pairs)} pairs"
                    )

        pipe.execute()

        if min_pair_score:
            # A pruned collection is missing pairs on purpose. Record the cutoff
            # so a reader can tell "no pair below 0.4 exists" from "nothing was
            # ever built", and log what the bound bought.
            r.set(f"{collection}:bin_sim:min_pair_score:{algo}", min_pair_score)
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] Pruned {pruned}/{len(pairs)} pairs that cannot reach "
                    f"min_pair_score={min_pair_score}",
                )

        # Containers were kept out of the sweep above because they hold no code
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
