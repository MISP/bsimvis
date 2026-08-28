import os
import time
import json
import logging
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services import lineage_service
from bsimvis.app.services.bin_sim_tags import (
    AxisSplit,
    EMPTY_SUMMARIES,
    code_library_split,
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


def _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a=None, file_meta_b=None):
    """Write secondary indexes for a bin_sim pair doc."""

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
            pipe = self.r.pipeline(transaction=False)
            pipe.smembers(
                f"global:pool:{pool_id}:bin_sim:involves:{collection}:{md5_a}"
            )
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
        stored_a = (pair.get("coll_a") or collection, pair.get("md5_a"))
        table = "unique_to_a" if stored_a == (collection, file_md5) else "unique_to_b"
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

    def build_bin_sim(
        self,
        collection,
        algo="unweighted_cosine",
        md5_a=None,
        md5_b=None,
        min_cohesion=0.5,
        batch_uuid=None,
        job_service=None,
        job_id=None,
    ):
        """
        Builds binary similarity diff docs and scores for pairs of binaries.
        Uses a cluster-first greedy sweep algorithm.

        Streamed and resumable, same shape as feature_service.enrich_features
        (job-system-rework-plan.md §6/§7.2): once there's more than one chunk
        of pairs, they're stored under pairs_key as a Redis LIST and consumed
        CHUNK_SIZE at a time with LPOP -- the list itself is the checkpoint,
        so a hard-kill mid-run loses at most one chunk, and re-running the
        same job_id resumes from whatever's still in the list instead of
        restarting from zero. Replaces the old self-splice (a new
        BUILD_BIN_SIM job per chunk via JobService.splice_tasks), which only
        ever ran one chunk in practice because the splice looked up parent_id
        on the wrong Redis connection and silently no-op'd.
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

        # Readiness gate: never persist a bin_sim doc computed against a
        # binary whose own similarity discovery hasn't run yet -- that would
        # silently write a score with one side effectively empty (wrong, not
        # just incomplete) whenever another file is still mid-analysis while
        # this one's batch finishes. {collection}:built:functions:{algo} is
        # exactly where similarity_service.build_batch marks a function done
        # once its discovery pass has run. A binary that isn't ready yet is
        # simply skipped this round -- whichever of a pair finishes analysis
        # LAST will find the other side already ready and complete the pair
        # then, so nothing is permanently lost, only deferred until correct.
        built_functions = set(
            f.decode() if isinstance(f, bytes) else f
            for f in r.smembers(f"{collection}:built:functions:{algo}")
        )
        ready_binaries = []
        not_ready = []
        for md5 in binaries:
            fids = r.smembers(f"{collection}:idx:file:functions:{md5}")
            fids = {f.decode() if isinstance(f, bytes) else f for f in fids}
            if fids and not fids.issubset(built_functions):
                not_ready.append(md5)
            else:
                ready_binaries.append(md5)
        if not_ready:
            msg = (
                f"[*] Skipping {len(not_ready)} binaries whose similarity "
                f"discovery hasn't finished yet -- will pair once ready: "
                f"{not_ready[:10]}{'...' if len(not_ready) > 10 else ''}"
            )
            logging.info(msg)
            if job_service and job_id:
                job_service.add_log(job_id, msg)
        binaries = ready_binaries

        num_binaries = len(binaries)
        if num_binaries < 2:
            if job_service and job_id:
                job_service.add_log(job_id, "Not enough binaries to compare.")
                job_service.update_progress(job_id, 100)
            return True

        # Generate pairs once, up front.
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
                # pairs between batch and all binaries
                for b1 in batch_binaries:
                    for b2 in binaries:
                        if b1 == b2:
                            continue
                        if b1 < b2:
                            pairs.append((b1, b2))
                        else:
                            pairs.append((b2, b1))
                pairs = list(set(pairs))
            else:
                for i in range(len(binaries)):
                    for j in range(i + 1, len(binaries)):
                        b1, b2 = binaries[i], binaries[j]
                        if b1 < b2:
                            pairs.append((b1, b2))
                        else:
                            pairs.append((b2, b1))
            # Sort pairs for determinism
            pairs.sort()

        total_pairs = len(pairs)
        if total_pairs == 0:
            if job_service and job_id:
                job_service.update_progress(job_id, 100, "No new binary pairs to compare.")
            return True

        CHUNK_SIZE = int(os.getenv("BIN_SIM_CHUNK_SIZE", 100))
        # Only worth the round-trips (and the resumability they buy) once
        # there's more than one chunk to do -- the common md5_a/md5_b or
        # small-batch call stays a single in-memory pass, same cost as before.
        pairs_key = (
            f"{collection}:bin_sim_jobs:{job_id}:pairs"
            if job_id and total_pairs > CHUNK_SIZE
            else None
        )
        if pairs_key:
            existing_len = r.llen(pairs_key)
            if existing_len:
                # A previous attempt was hard-killed mid-run: the list is
                # exactly what's left, so pick up from there.
                total_pairs = int(r.get(f"{pairs_key}:total") or existing_len)
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"[*] Resuming binary similarity build: "
                        f"{existing_len}/{total_pairs} pairs left.",
                    )
            else:
                r.rpush(pairs_key, *[json.dumps(p) for p in pairs])
                r.expire(pairs_key, 86400)
                r.set(f"{pairs_key}:total", total_pairs, ex=86400)

        processed_overall = total_pairs - (r.llen(pairs_key) if pairs_key else 0)
        done_with_single_pass = False

        while True:
            # Checked before popping the next chunk, not after: whatever's
            # already been LPOP'd off pairs_key stays checkpointed, and the
            # remainder is left in the list for `restart` (§2) to resume from
            # exactly here -- the same reason this loop is resumable at all.
            if job_service and job_id and job_service.is_cancelled(job_id):
                job_service.add_log(job_id, "Cancelled.")
                return False
            if pairs_key:
                raw_chunk = r.lpop(pairs_key, CHUNK_SIZE)
                if not raw_chunk:
                    break
                chunk_pairs = [tuple(json.loads(p)) for p in raw_chunk]
            else:
                if done_with_single_pass:
                    break
                chunk_pairs = pairs
                done_with_single_pass = True

            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"[*] Computing similarities for pairs "
                    f"{processed_overall} to {processed_overall + len(chunk_pairs)} "
                    f"out of {total_pairs}...",
                )

            # Re-derive binaries list so we only fetch metadata for binaries in this chunk
            chunk_binaries = set()
            for p in chunk_pairs:
                chunk_binaries.update(p)
            binaries = list(chunk_binaries)
            num_binaries = len(binaries)
            pairs = chunk_pairs
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

            # 3. Load function metadata (for bsim_features_count & names)
            func_meta_cache = {}
            all_unique_fids = set()
            for fids_set in binary_fids.values():
                all_unique_fids.update(fids_set)

            if all_unique_fids:
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"[*] Loading metadata for {len(all_unique_fids)} functions...",
                    )
                fids_list = list(all_unique_fids)
                pipe = r.pipeline(transaction=False)
                for fid in fids_list:
                    pipe.get(f"{fid}:meta")
                meta_results = pipe.execute()
                for fid, res in zip(fids_list, meta_results):
                    if res:
                        m = json.loads(res) if not isinstance(res, dict) else res
                        if isinstance(m, str):
                            try:
                                m = json.loads(m)
                            except ValueError:
                                pass
                        func_meta_cache[fid] = m if isinstance(m, dict) else {}

            # Normalize each function's tags once here, not once per matched edge.
            fid_tags = {}
            for fid, m in func_meta_cache.items():
                tags = merge_tag_fields(m)
                if tags:
                    fid_tags[fid] = tags

            tag_meta_cache = load_tag_meta(r, collection) if fid_tags else {}
            tags_rev = read_tags_rev(r, collection)

            # 5. Process Pairs (Direct Similarity Matching with Bipartite Greedy Selection)
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

            # ponytail: Determine if this collection is a pool or a normal collection
            is_pool = collection.startswith("global:pool:") or collection.startswith(
                "pool:"
            )
            if is_pool:
                from bsimvis.app.services.index_service import get_pool_id

                pool_id = get_pool_id(collection)
                involves_file_prefix = f"global:pool:{pool_id}:sim:involves:file:"
            else:
                involves_file_prefix = f"{collection}:sim:involves:file:"

            for m_a, m_b in pairs:
                file_meta_a = file_meta_cache.get(m_a, {})
                file_meta_b = file_meta_cache.get(m_b, {})

                # ponytail: Use Kvrocks SINTER to fetch similarities involving both files without temporary keys
                involves_a = f"{involves_file_prefix}{m_a}"
                involves_b = f"{involves_file_prefix}{m_b}"
                sim_keys = [
                    k.decode() if isinstance(k, bytes) else str(k)
                    for k in r.sinter(involves_a, involves_b)
                ]

                # Fetch similarity documents in parallel
                sim_docs = []
                if sim_keys:
                    pipe_sim = r.pipeline(transaction=False)
                    for k in sim_keys:
                        pipe_sim.get(k)
                    sim_res = pipe_sim.execute()
                    for res in sim_res:
                        if res:
                            sim_docs.append(
                                json.loads(res.decode() if isinstance(res, bytes) else res)
                            )

                # Filter/extract edges
                edges = []
                for doc in sim_docs:
                    fid1 = doc.get("id1")
                    fid2 = doc.get("id2")
                    score = doc.get("score", 0.0)
                    if fid1 and fid2:
                        # ponytail: Extract exact MD5 out of function IDs (casing-insensitive)
                        parts1 = fid1.split(":")
                        parts2 = fid2.split(":")
                        if len(parts1) >= 2 and len(parts2) >= 2:
                            m1 = parts1[-2].lower()
                            m2 = parts2[-2].lower()
                            m_a_clean = m_a.lower()
                            m_b_clean = m_b.lower()
                            if m1 == m_a_clean and m2 == m_b_clean:
                                edges.append((fid1, fid2, score))
                            elif m1 == m_b_clean and m2 == m_a_clean:
                                edges.append((fid2, fid1, score))

                # Sort edges by score descending (greedy match prioritizes best matches), using function IDs as deterministic tie-breakers
                edges.sort(key=lambda x: (-x[2], x[0], x[1]))

                assigned_a = set()
                assigned_b = set()
                diff_matched = []

                sum_weighted_cohesion = 0.0
                sum_weights = 0.0

                tag_split = AxisSplit(fid_tags, tag_meta_cache)

                # Match greedily
                for fid_a, fid_b, score in edges:
                    if fid_a not in assigned_a and fid_b not in assigned_b:
                        assigned_a.add(fid_a)
                        assigned_b.add(fid_b)

                        f_features_a = float(
                            func_meta_cache.get(fid_a, {}).get("bsim_features_count", 1.0)
                        )
                        f_features_b = float(
                            func_meta_cache.get(fid_b, {}).get("bsim_features_count", 1.0)
                        )
                        f_features = max(f_features_a, f_features_b)

                        # Slim doc: persist only the stable/expensive triple. Cluster tag +
                        # rarity are derived live at read (get_bin_sim) from current cluster
                        # meta, so a cluster rebuild can't leave them stale. [[Change 1]]
                        diff_matched.append(
                            {
                                "similarity": score,
                                "avg_features": f_features,
                                "func_a": fid_a,
                                "func_b": fid_b,
                            }
                        )

                        sum_weighted_cohesion += score * f_features
                        sum_weights += f_features

                        tag_split.add_match(fid_a, fid_b, score, f_features_a, f_features_b)

                all_funcs_a_total = binary_fids[m_a]
                all_funcs_b_total = binary_fids[m_b]

                unassigned_a = all_funcs_a_total - assigned_a
                unassigned_b = all_funcs_b_total - assigned_b

                unique_to_a = []
                for fid in sorted(list(unassigned_a)):
                    f_features = float(
                        func_meta_cache.get(fid, {}).get("bsim_features_count", 1.0)
                    )
                    if f_features <= 0:
                        f_features = 1.0

                    # Slim: cluster tag + rarity derived at read. [[Change 1]]
                    unique_to_a.append(
                        {
                            "func_id": fid,
                            "avg_features": f_features,
                        }
                    )
                    sum_weights += f_features
                    tag_split.add_unique(fid, f_features, "a")

                unique_to_b = []
                for fid in sorted(list(unassigned_b)):
                    f_features = float(
                        func_meta_cache.get(fid, {}).get("bsim_features_count", 1.0)
                    )
                    if f_features <= 0:
                        f_features = 1.0

                    # Slim: cluster tag + rarity derived at read. [[Change 1]]
                    unique_to_b.append(
                        {
                            "func_id": fid,
                            "avg_features": f_features,
                        }
                    )
                    sum_weights += f_features
                    tag_split.add_unique(fid, f_features, "b")

                score_unweighted = (
                    sum_weighted_cohesion / sum_weights if sum_weights > 0 else 0.0
                )

                cov_a = (
                    len(assigned_a) / len(all_funcs_a_total) if all_funcs_a_total else 0.0
                )
                cov_b = (
                    len(assigned_b) / len(all_funcs_b_total) if all_funcs_b_total else 0.0
                )

                # Coverage is against each binary's whole mass, so "libc covers 40% of A"
                # means 40% of everything A contains, matched or not.
                def _total_weight(fids):
                    return sum(
                        float(func_meta_cache.get(f, {}).get("bsim_features_count", 1.0))
                        or 1.0
                        for f in fids
                    )

                tag_fields = (
                    tag_split.summaries(
                        _total_weight(all_funcs_a_total),
                        _total_weight(all_funcs_b_total),
                        tag_meta_cache,
                    )
                    if fid_tags
                    else dict(EMPTY_SUMMARIES)
                )

                sid = f"{collection}:bin_sim:{algo}:{m_a}::{m_b}"
                pair_scores[(m_a, m_b)] = score_unweighted

                # Code/Library is the same weighted-cosine formula as `score`,
                # restricted per category -- not a re-average of `tags_summary`.
                score_library, score_code = code_library_split(
                    diff_matched, unique_to_a, unique_to_b, fid_tags
                )

                doc = {
                    "md5_a": m_a,
                    "md5_b": m_b,
                    "algo": algo,
                    "architecture_a": file_meta_a.get("language_id", ""),
                    "architecture_b": file_meta_b.get("language_id", ""),
                    "functions_count_a": binary_func_counts.get(m_a, 0),
                    "functions_count_b": binary_func_counts.get(m_b, 0),
                    "score": score_unweighted,
                    "score_code": score_code,
                    "score_library": score_library,
                    "coverage_a": cov_a,
                    "coverage_b": cov_b,
                    "shared_clusters": len(diff_matched),
                    "unique_clusters_a": len(unique_to_a),
                    "unique_clusters_b": len(unique_to_b),
                    "unclustered_a": len(unique_to_a),
                    "unclustered_b": len(unique_to_b),
                    "computed_at": int(time.time() * 1000),
                    # Bumped by every tag write, so a stored split can be told apart
                    # from the tag state it was computed against without rebuilding.
                    "tags_rev": tags_rev,
                    **tag_fields,
                    "diff": {
                        "matched": diff_matched,
                        "unique_to_a": unique_to_a,
                        "unique_to_b": unique_to_b,
                        "unclustered_a": [],
                        "unclustered_b": [],
                    },
                }

                pipe.set(sid, json.dumps(doc))
                # `algo` is a provenance tag (which function similarity the clusters came
                # from), not a choice of file score. The sort score is always the
                # unweighted cohesion mean so it means the same thing in every namespace
                # and matches the pool-level score. The other aggregates stay in `doc`.
                final_bin_score = score_unweighted

                pipe.zadd(f"{collection}:bin_sim:score:{algo}", {sid: final_bin_score})
                pipe.zadd(f"{collection}:bin_sim:score_code:{algo}", {sid: score_code})
                if score_library is not None:
                    pipe.zadd(f"{collection}:bin_sim:score_library:{algo}", {sid: score_library})
                pipe.sadd(f"{collection}:bin_sim:involves:{m_a}", sid)
                pipe.sadd(f"{collection}:bin_sim:involves:{m_b}", sid)
                pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)

                # Secondary indexes
                _index_bin_sim_pair(pipe, collection, sid, doc, file_meta_a, file_meta_b)

                processed += 1

                if processed % 100 == 0:
                    pipe.execute()
                    if job_service and job_id:
                        pct = min(99, int((processed_overall + processed) / total_pairs * 100))
                        job_service.update_progress(
                            job_id,
                            pct,
                            f"Processed {processed_overall + processed}/{total_pairs} pairs",
                            processed=processed_overall + processed,
                            total=total_pairs,
                        )

            pipe.execute()

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

            processed_overall += len(chunk_pairs)
            if job_service and job_id:
                pct = min(99, int(processed_overall / total_pairs * 100))
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Processed {processed_overall}/{total_pairs} pairs",
                    processed=processed_overall,
                    total=total_pairs,
                )

        if pairs_key:
            r.delete(pairs_key)
            r.delete(f"{pairs_key}:total")

        if job_service and job_id:
            job_service.update_progress(
                job_id,
                100,
                f"Completed binary similarity build for {processed_overall} pairs.",
                processed=processed_overall,
                total=total_pairs,
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
                pipe.zadd(f"{collection}:bin_sim:score_code:{algo}", {sid: score_code})
                if score_library is not None:
                    pipe.zadd(
                        f"{collection}:bin_sim:score_library:{algo}",
                        {sid: score_library},
                    )
                else:
                    pipe.zrem(f"{collection}:bin_sim:score_library:{algo}", sid)
            pipe.execute()

            done += len(chunk)
            if job_service and job_id:
                job_service.update_progress(job_id, int(done / total * 100))

        if job_service and job_id:
            job_service.update_progress(job_id, 100, f"Resplit {total} bin_sim pairs.")
        return True


bin_sim_service = BinSimService()
