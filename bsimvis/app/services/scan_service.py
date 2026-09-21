"""Fast scan mode: analyse a file, compare it, write nothing to Kvrocks.

A scan answers "does this binary look like anything we already have?" without
ingesting the sample. It runs the same Ghidra analysis every upload runs, keeps
the result in Redis under a TTL, and computes the match by *reading* an existing
collection's indexes.

Nothing here writes a Kvrocks key. That is possible because discovery is already
vector-in: `SimilarityService.build_discovery_args` turns an in-memory feature
vector into the whole target side of `_discover`, which reads only the
collection's posting lists. The scan's functions get synthetic ids
(`scan:{id}:func:{md5}:{addr}`) that collide with nothing, so self-matches
survive when the same md5 is already in scope -- a 1.0 row is a useful check
that the scan reproduces the stored analysis.

The file score is the canonical one: the same `greedy_match` + `score_pair` the
bin_sim builder persists, so a scan row and a stored bin_sim doc are the same
number.
"""

import json
import time
import uuid

from bsimvis.app.services.bin_sim_service import (
    discover_edges,
    load_vectors,
    stored_discovery,
    stored_unweighted_match,
)
from bsimvis.app.services.bin_sim_tags import (
    greedy_match,
    load_tag_meta,
    merge_tag_fields,
    normalize_tags,
    read_tags_rev,
    score_pair,
)
from bsimvis.app.services.cluster_utils import (
    bin_cluster_ns,
    fetch_bin_cluster_meta_all_axes,
    pick_best_cluster,
)
from bsimvis.app.services.collection_config import (
    assert_signature_settings_match,
    get_collection_params,
)
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.redis_client import (
    get_queue_redis,
    get_raw_queue_redis,
    get_raw_redis,
    get_redis,
)
from bsimvis.app.services.similarity_service import SimilarityService
from bsimvis.similarity import registry

BIN_AXES = ("overall", "code", "library", "content")


def cache_ttl():
    return int(config_service.get("scan.cache_ttl", 86400))


def max_cached_bytes():
    return int(config_service.get("scan.max_cached_bytes", 268435456))


def default_top_files():
    return int(config_service.get("scan.top_files", 20))


def default_modules():
    """Extra tagging modules a scan runs, from `scan.modules`."""
    mods = config_service.get("scan.modules", [])
    return list(mods) if isinstance(mods, (list, tuple)) else []


def scan_modules(enable=(), disable=()):
    """Scan taggers, with FunctionID matching the upload default."""
    upload = config_service.get("analysis_modules.enabled", []) or []
    mods = set(default_modules()) | (set(upload) & {"FunctionID"}) | set(enable)
    return sorted(mods - set(disable))


def _decode(raw):
    if not raw:
        return {}
    value = json.loads(raw) if not isinstance(raw, dict) else raw
    return json.loads(value) if isinstance(value, str) else value


def _s(v):
    return v.decode() if isinstance(v, bytes) else str(v)


class ScanService:
    """Owns the `scan:{id}:*` cache and the read-only matching pass."""

    def __init__(self):
        self.r = get_redis()  # Kvrocks: read only, always
        self.q = get_queue_redis()  # the scan cache
        self.q_raw = get_raw_queue_redis()  # the sample's bytes
        self.sim = SimilarityService(self.r)

    # ------------------------------------------------------------------ cache

    def key(self, scan_id, *parts):
        return ":".join(["scan", scan_id, *parts])

    def create(self, raw_bytes, file_name, file_md5, scopes, params):
        """Park the sample and its request, return the new scan id."""
        scan_id = uuid.uuid4().hex[:12]
        ttl = cache_ttl()
        doc = {
            "scan_id": scan_id,
            "status": "pending",
            "file_name": file_name,
            "file_md5": file_md5,
            "size": len(raw_bytes),
            "scopes": scopes,
            "params": params,
            "created_at": int(time.time() * 1000),
            "warnings": [],
        }
        self.q_raw.setex(self.key(scan_id, "raw"), ttl, raw_bytes)
        self.q.setex(self.key(scan_id, "doc"), ttl, json.dumps(doc))
        return scan_id

    def get(self, scan_id):
        return _decode(self.q.get(self.key(scan_id, "doc"))) or None

    def update(self, scan_id, **fields):
        doc = self.get(scan_id)
        if not doc:
            return None
        doc.update(fields)
        self.q.setex(self.key(scan_id, "doc"), cache_ttl(), json.dumps(doc))
        return doc

    def raw(self, scan_id):
        return self.q_raw.get(self.key(scan_id, "raw"))

    def list_scans(self):
        """Returns a list of all active scan documents from Redis."""
        keys = self.q.keys("scan:*:doc")
        if not keys:
            return []

        docs = []
        for key in keys:
            doc = self.q.get(key)
            if doc:
                try:
                    parsed = _decode(doc)
                    if parsed:
                        docs.append(parsed)
                except Exception:
                    pass

        # Sort newest first
        docs.sort(key=lambda d: d.get("created_at", 0), reverse=True)
        return docs

    def delete(self, scan_id):
        """Drop one scan's cache. Every key carries the scan id, so this is a
        bounded DEL list built from the doc, never a scan of the keyspace."""
        doc = self.get(scan_id)
        keys = [
            self.key(scan_id, "raw"),
            self.key(scan_id, "doc"),
            self.key(scan_id, "meta"),
        ]
        keys += [
            self.key(scan_id, "chunk", str(i))
            for i in range(int((doc or {}).get("chunk_count", 0)))
        ]
        for scope in (doc or {}).get("scanned", []):
            for row in scope.get("files", []):
                keys.append(
                    self.key(scan_id, "rows", scope["collection"], row["file_md5"])
                )
        if keys:
            self.q.delete(*keys)
        return bool(doc)

    def store_meta(self, scan_id, file_meta, modules):
        self.q.setex(
            self.key(scan_id, "meta"),
            cache_ttl(),
            json.dumps({"file_meta": file_meta, "modules": modules}),
        )

    def store_chunk(self, scan_id, index, functions):
        self.q.setex(
            self.key(scan_id, "chunk", str(index)), cache_ttl(), json.dumps(functions)
        )

    def load_functions(self, scan_id):
        """Every cached function of a scan, in chunk order."""
        doc = self.get(scan_id) or {}
        out = []
        for i in range(int(doc.get("chunk_count", 0))):
            out.extend(_decode(self.q.get(self.key(scan_id, "chunk", str(i)))) or [])
        return out

    # ----------------------------------------------------------------- commit

    def commit(self, scan_id, collection, batch_name=None, skip_sim=False, topup=True):
        """Promote a cached scan into a real collection without re-running Ghidra.

        A cached chunk is byte-identical to what `upload_chunk` writes, so this
        copies the chunks into Kvrocks and hands them to the same
        INDEX_META -> INDEX_FUNCTIONS -> INDEX_FEATURES -> BUILD_SIM pipeline the
        upload path builds. The analysis itself is never repeated.

        Ghidra does run once more when the scan ran lean and modules the
        instance normally enables are missing: a JVM-free tag pass is not
        possible (YARA offset mapping needs the live program). That top-up is a
        plain GHIDRA_ANALYZE with `skip_sim`, queued behind the commit on the
        same lane, so the file is queryable long before it lands.

        A collection that does not exist yet is created by the ingest, exactly
        as an upload into a new collection creates it.
        """
        from bsimvis.app.routes.cluster import build_rebuild_all_tasks
        from bsimvis.app.services.job_service import JobService, JobType

        doc = self.get(scan_id)
        if not doc:
            return {"error": "Scan not found or expired"}, 404
        if not doc.get("chunk_count"):
            return {"error": f"Scan {scan_id} has no cached analysis to commit"}, 409
        md5 = doc.get("file_md5")
        if self.r.sismember(f"{collection}:all_files", f"{collection}:file:{md5}"):
            return {"error": f"File {md5} is already in '{collection}'"}, 409

        raw = self.raw(scan_id)
        if raw is None:
            return {"error": "The scanned bytes expired from the cache"}, 410

        cached = _decode(self.q.get(self.key(scan_id, "meta")))
        file_meta = dict(cached.get("file_meta") or {})
        batch_uuid = str(uuid.uuid4())
        file_meta.update(
            {
                "file_md5": md5,
                "file_name": doc.get("file_name") or file_meta.get("file_name", ""),
                "batch_uuid": batch_uuid,
                "batch_name": batch_name or f"scan {scan_id}",
                "collection": collection,
            }
        )

        get_raw_redis().set(f"{collection}:file:{md5}:raw", raw)

        job_service = JobService()
        chunk_jobs, num_functions, total_features = [], 0, 0
        for index in range(int(doc["chunk_count"])):
            functions = _decode(self.q.get(self.key(scan_id, "chunk", str(index))))
            if not functions:
                continue
            chunk_id = f"{collection}:file:{md5}:chunk_data:{index}"
            self.r.set(chunk_id, json.dumps(functions))
            job_id = job_service.create_job(
                JobType.INDEX_FUNCTIONS,
                {
                    "collection": collection,
                    "chunk_id": chunk_id,
                    "file_meta": file_meta,
                    "file_md5": md5,
                    "batch_uuid": batch_uuid,
                },
                enqueue=False,
            )
            # Same continuation push upload_chunk uses: batches from different
            # jobs interleave instead of one commit starving the fleet.
            job_service.enqueue_job(job_id, is_continuation=True)
            chunk_jobs.append(job_id)
            num_functions += len(functions)
            total_features += sum(
                f.get("function_metadata", {}).get("bsim_features_count", 0)
                for f in functions
            )

        params = self._params_for(collection, doc.get("params") or {})
        tasks = [
            (
                JobType.INDEX_META,
                {
                    "collection": collection,
                    "file_id": None,
                    "file_meta": file_meta,
                    "num_functions": num_functions,
                    "total_features": total_features,
                },
            )
        ]
        if chunk_jobs:
            tasks.append(job_service.create_group(chunk_jobs, enqueue=False))
        tasks.append((JobType.INDEX_FEATURES, {"collection": collection, "md5": md5}))
        if not skip_sim:
            tasks.append(
                (
                    JobType.BUILD_SIM,
                    {
                        "collection": collection,
                        "file_id": None,
                        "md5": md5,
                        "algo": params["algo"],
                        "top_k": params["top_k"],
                        "min_score": params["min_score"],
                        "min_features": params["min_features"],
                    },
                )
            )
            tasks.append(
                (
                    JobType.INDEX_SIM,
                    {"collection": collection, "md5": md5, "algo": params["algo"]},
                )
            )
        tasks += build_rebuild_all_tasks(
            collection,
            params["algo"],
            skip_sim=skip_sim,
            data={"batch_uuid": batch_uuid},
        )
        pipeline_id = job_service.submit_to_lane(collection, tasks)

        topup_job = None
        missing = sorted(
            set(config_service.get("analysis_modules.enabled", []))
            - set(cached.get("modules") or [])
        )
        if topup and missing:
            topup_job = job_service.submit_to_lane(
                collection,
                job_service.create_job(
                    JobType.GHIDRA_ANALYZE,
                    {
                        "collection": collection,
                        "raw_file_id": f"{collection}:file:{md5}:raw",
                        "file_md5": md5,
                        "file_name": file_meta["file_name"],
                        "batch_uuid": batch_uuid,
                        "batch_name": file_meta["batch_name"],
                        "profile": (doc.get("params") or {}).get("profile", "fast"),
                        "skip_sim": True,
                        "skip_function_id": "FunctionID" not in missing,
                        "skip_capa": "capa" not in missing,
                        "skip_yara": "yara" not in missing,
                        "skip_rulezet": "rulezet" not in missing,
                        "skip_boilerplate": "boilerplate" not in missing,
                    },
                    enqueue=False,
                ),
            )

        self.update(
            scan_id,
            committed={
                "collection": collection,
                "batch_uuid": batch_uuid,
                "pipeline_id": pipeline_id,
                "topup_job": topup_job,
                "topup_modules": missing if topup_job else [],
                "at": int(time.time() * 1000),
            },
        )
        return {
            "status": "committed",
            "scan_id": scan_id,
            "collection": collection,
            "file_md5": md5,
            "batch_uuid": batch_uuid,
            "batch_name": file_meta["batch_name"],
            "pipeline_id": pipeline_id,
            "chunks": len(chunk_jobs),
            "function_count": num_functions,
            "topup_job": topup_job,
            "topup_modules": missing if topup_job else [],
        }

    # ----------------------------------------------------------------- scopes

    def resolve_scopes(self, collections=None, pools=None, use_all=False):
        """`([collection, ...], [warning, ...])` for one scan request.

        A pool is a named union of collections and its posting lists live in its
        members, so a pool scope expands to its members rather than reaching for
        the pool's own similarity namespace.
        """
        warnings = []
        out = []
        if use_all:
            out.extend(sorted(_s(c) for c in self.r.smembers("global:collections")))
        for coll in collections or []:
            if self.r.exists(f"global:collection:{coll}:meta"):
                out.append(coll)
            else:
                warnings.append(f"Unknown collection '{coll}', skipped.")
        for pool_id in pools or []:
            from bsimvis.app.services.pool_service import PoolService

            pool = PoolService(self.r).get_pool(pool_id)
            if not pool:
                warnings.append(f"Unknown pool '{pool_id}', skipped.")
                continue
            out.extend(pool.get("collections") or [])
        seen, deduped = set(), []
        for coll in out:
            if coll not in seen:
                seen.add(coll)
                deduped.append(coll)
        return deduped, warnings

    def _params_for(self, collection, params):
        """Locked collection params, unless the caller supplied their own.

        `min_features` / `min_score` are sticky per collection, so a scan that
        takes them from the collection reports a score comparable with what that
        collection's own UI shows. A caller-supplied value wins everywhere.
        """
        locked = get_collection_params(collection)
        return {
            "algo": params.get("algo")
            or config_service.get("similarity.algo", registry.DEFAULT_ALGO),
            "min_score": (
                params["min_score"]
                if params.get("min_score") is not None
                else locked["min_score"]["value"]
            ),
            "min_features": (
                params["min_features"]
                if params.get("min_features") is not None
                else locked["min_features"]["value"]
            ),
            "top_k": params.get("top_k")
            or config_service.get("similarity.top_k", 1000),
        }

    # --------------------------------------------------------------- matching

    def _targets(self, scan_id, functions, scan_md5):
        """Cached functions -> the shape discovery and scoring both need."""
        targets = []
        for func in functions:
            meta = func.get("function_metadata", {}) or {}
            feats = func.get("function_features", {}) or {}
            addr = meta.get("entrypoint_address")
            if not addr:
                continue
            tf = [
                (item.get("hash"), item.get("tf", 1))
                for item in (feats.get("bsim_features_tf") or [])
                if item.get("hash")
            ]
            targets.append(
                {
                    "fid": f"scan:{scan_id}:func:{scan_md5}:{addr}",
                    "addr": addr,
                    "name": meta.get("function_name", ""),
                    "features": tf,
                    "feat_count": float(
                        feats.get("bsim_features_count")
                        or meta.get("bsim_features_count")
                        or len(tf)
                    ),
                    "funcid": meta.get("function_id_hash"),
                    "tags": merge_tag_fields(meta),
                }
            )
        return targets

    def _hash_match(self, collection, targets_small):
        """Exact FunctionID-hash matches for functions under `min_features`.

        BSim is false-positive-prone on tiny functions, so the build path gives
        them a deterministic 1.0 from their exact Ghidra FunctionID hash instead.
        `SimilarityService._hash_match_small` does the same thing but parses a
        collection fid out of its input, which a scan fid is not, so the read
        half is repeated here. It writes nothing either way.
        """
        by_hash = {}
        for target in targets_small:
            if target["funcid"]:
                by_hash.setdefault(target["funcid"], []).append(target)
        if not by_hash:
            return {}

        hashes = list(by_hash)
        pipe = self.r.pipeline(transaction=False)
        for fid_hash in hashes:
            pipe.smembers(f"{collection}:funcid:{fid_hash}")
        out = {}
        for fid_hash, mates in zip(hashes, pipe.execute()):
            mate_ids = [_s(m) for m in (mates or set())]
            if not mate_ids:
                continue
            for target in by_hash[fid_hash]:
                out[target["fid"]] = [
                    {"id": m, "score": 1.0, "c_total": 0.0} for m in mate_ids
                ]
        return out

    def _discover_scope(self, collection, targets, params):
        """`{scan fid: [candidate, ...]}` for one collection. Read-only."""
        algo = params["algo"]
        min_features = float(params["min_features"] or 0)
        results = {}
        small = []

        for target in targets:
            if not target["features"] or len(target["features"]) < min_features:
                small.append(target)
                continue
            _total, args = self.sim.build_discovery_args(
                target["fid"],
                collection,
                algo,
                params["min_score"],
                params["top_k"],
                min_features,
                target["features"],
            )
            raw = self.sim._discover(args)
            candidates = []
            for k in range(0, len(raw or []), 3):
                candidates.append(
                    {
                        "id": _s(raw[k]),
                        "score": float(raw[k + 1]),
                        "c_total": float(raw[k + 2]),
                    }
                )
            if candidates:
                results[target["fid"]] = candidates

        results.update(self._hash_match(collection, small))
        return results

    def _rank_files(self, candidates_by_fid, targets_by_fid, top_files):
        """Candidate files ranked by matched feature mass.

        The mass is the scan side's own feature count, which is free -- weighing
        it against the candidate's would need a second read per candidate, and
        this only decides which files get the exact score.
        """
        mass, hits = {}, {}
        for fid, candidates in candidates_by_fid.items():
            weight = targets_by_fid[fid]["feat_count"] or 1.0
            for cand in candidates:
                parts = cand["id"].split(":")
                if len(parts) < 4:
                    continue
                md5 = parts[-2]
                mass[md5] = mass.get(md5, 0.0) + cand["score"] * weight
                # Scan functions, not candidate edges: one function can have
                # many candidates in the same file.
                hits.setdefault(md5, set()).add(fid)
        ranked = sorted(mass.items(), key=lambda kv: (-kv[1], kv[0]))
        return ranked[:top_files], len(ranked), {k: len(v) for k, v in hits.items()}

    def _score_file(self, collection, md5, targets, candidates_by_fid, params):
        """The canonical BSimVis file score for one candidate file.

        Same greedy assignment and `score_pair` settings the bin_sim builder
        persists, so this number and a stored bin_sim doc's are the same.
        """
        fids_b = {
            _s(f).removesuffix(":meta")
            for f in self.r.smembers(f"{collection}:idx:file:functions:{md5}")
        }
        if not fids_b:
            return None

        fid_list = list(fids_b)
        pipe = self.r.pipeline(transaction=False)
        for fid in fid_list:
            pipe.get(f"{fid}:meta")
        pipe.get(f"{collection}:file:{md5}:meta")
        results = pipe.execute()
        meta_b = {fid: _decode(raw) for fid, raw in zip(fid_list, results[:-1])}
        file_meta_b = _decode(results[-1])

        edges = []
        for target in targets:
            for cand in candidates_by_fid.get(target["fid"], []):
                if cand["id"] in fids_b:
                    edges.append((target["fid"], cand["id"], cand["score"]))

        fids_a = {t["fid"] for t in targets}
        feat_a = {t["fid"]: t["feat_count"] for t in targets}

        discovery = stored_discovery()
        if discovery:
            edges.extend(
                self._discover_leftovers(
                    targets, fids_b, edges, params["algo"], discovery
                )
            )
        unweighted = stored_unweighted_match()

        def feat(fid):
            if fid in feat_a:
                return float(feat_a[fid] or 1.0)
            return float(meta_b.get(fid, {}).get("bsim_features_count", 1.0))

        fid_tags = {
            t["fid"]: normalize_tags(tags) for t in targets if (tags := t["tags"])
        }
        fid_tags.update(
            {
                fid: tags
                for fid, value in meta_b.items()
                if (tags := normalize_tags(merge_tag_fields(value)))
            }
        )
        common = score_pair(
            edges,
            fids_a,
            fids_b,
            feat,
            fid_tags,
            load_tag_meta(self.r, collection) if fid_tags else {},
            unweighted=unweighted,
        )
        self._enrich_clusters(
            collection,
            common["diff"],
            meta_b,
            params,
            {t["fid"]: t["name"] for t in targets},
        )
        return {
            "collection": collection,
            "file_md5": md5,
            "file_name": file_meta_b.get("file_name", ""),
            "architecture": file_meta_b.get("language_id", ""),
            "functions_count": len(fids_b),
            "algo": params["algo"],
            # Same provenance a stored bin_sim doc carries: a score built under
            # the other setting is not comparable with this one.
            "unweighted_match": unweighted,
            "discovery": bool(discovery),
            "tags_rev": read_tags_rev(self.r, collection),
            **common,
        }

    def _discover_leftovers(self, targets, fids_b, edges, algo, discovery):
        """Re-match functions the initial greedy pass did not place."""
        min_score, max_df = discovery
        _, matched_a, matched_b = greedy_match(edges)
        left_a = {t["fid"] for t in targets} - matched_a
        left_b = set(fids_b) - matched_b
        if not left_a or not left_b:
            return []
        vectors = {
            t["fid"]: {h: float(tf) for h, tf in t["features"]}
            for t in targets
            if t["fid"] in left_a and t["features"]
        }
        vectors.update(load_vectors(self.r, left_b))
        return discover_edges(vectors, left_a, left_b, algo, min_score, max_df=max_df)

    def _enrich_clusters(self, collection, diff, meta_b, params, names_a):
        """Attach, per matched row, the cluster the scan function would land in.

        The nearest cluster is the collection-side partner's best cluster; a
        real build would only actually merge the two at `clustering.uf_threshold`,
        so the row reports both. `_enrich_diff_clusters` in routes/bin_sim.py
        cannot be reused as-is: it keys matched rows off the cluster the two
        functions *share*, and a scan function is in no cluster at all.
        """
        threshold = float(config_service.get("clustering.uf_threshold", 0.98))
        matched = diff.get("matched", [])
        if not matched:
            return

        fids = [row["func_b"] for row in matched]
        pipe = self.r.pipeline(transaction=False)
        for fid in fids:
            pipe.smembers(f"{fid}:clusters")
        fid_labels, all_labels = {}, set()
        for fid, res in zip(fids, pipe.execute()):
            labels = {_s(c) for c in (res or set())}
            fid_labels[fid] = labels
            all_labels |= labels

        cluster_meta = {}
        if all_labels:
            labels = list(all_labels)
            pipe = self.r.pipeline(transaction=False)
            for label in labels:
                pipe.get(f"{collection}:cluster:{params['algo']}:{label}:meta")
            for label, res in zip(labels, pipe.execute()):
                meta = _decode(res)
                if isinstance(meta, dict) and meta:
                    cluster_meta[label] = meta

        for row in matched:
            best = pick_best_cluster(fid_labels.get(row["func_b"], set()), cluster_meta)
            row["cluster_id"] = best.get("cluster_id", "") if best else ""
            row["cluster_uuid"] = best.get("cluster_uuid", "") if best else ""
            row["cluster_name"] = best.get("cluster_name", "") if best else ""
            row["cohesion"] = float(best.get("cohesion_score", 0.0)) if best else 0.0
            row["cluster_member_count"] = (
                int(best.get("member_count", 0)) if best else 0
            )
            row["is_clustered"] = best is not None
            # Nearest is not membership: a real build unions the two only above
            # the same threshold clustering itself uses.
            row["would_join"] = bool(best) and row["similarity"] >= threshold
            row["function_name_a"] = names_a.get(row["func_a"], "")
            row["function_name_b"] = meta_b.get(row["func_b"], {}).get(
                "function_name", ""
            )

    def _bin_clusters(self, collection, scored, params):
        """Binary clusters the scanned file would join, per axis.

        Binary clustering unions two files above `clustering.bin_uf_threshold`,
        so the answer is the clusters of every scored file that clears it.
        """
        threshold = float(config_service.get("clustering.bin_uf_threshold", 0.1))
        joiners = [row for row in scored if float(row.get("score", 0.0)) >= threshold]
        if not joiners:
            return {axis: [] for axis in BIN_AXES}

        pipe = self.r.pipeline(transaction=False)
        for row in joiners:
            for axis in BIN_AXES:
                axis_algo = (
                    params["algo"] if axis == "overall" else f"{params['algo']}:{axis}"
                )
                pipe.smembers(
                    f"{collection}:file:{row['file_md5']}:bin_clusters:"
                    f"{bin_cluster_ns(axis_algo, False)}"
                )
        raws = pipe.execute()

        entries = []
        for index in range(len(joiners)):
            window = raws[index * len(BIN_AXES) : (index + 1) * len(BIN_AXES)]
            entries.append(
                (
                    {
                        axis: [_s(c) for c in (raw or set())]
                        for axis, raw in zip(BIN_AXES, window)
                    },
                    False,
                )
            )

        meta_by_uuid, uuids_by_axis = fetch_bin_cluster_meta_all_axes(
            self.r, collection, entries, algo=params["algo"]
        )
        out = {axis: {} for axis in BIN_AXES}
        for row, per_axis in zip(joiners, uuids_by_axis):
            for axis, uuids in per_axis.items():
                if axis not in out:
                    continue
                for cluster_uuid in uuids:
                    meta = meta_by_uuid.get(cluster_uuid) or {}
                    entry = out[axis].setdefault(
                        cluster_uuid,
                        {
                            "cluster_uuid": cluster_uuid,
                            "cluster_id": meta.get("cluster_id", ""),
                            "cluster_name": meta.get("cluster_name", ""),
                            "cohesion_score": meta.get("cohesion_score", 0.0),
                            "member_count": meta.get("member_count", 0),
                            "function_count_stats": meta.get(
                                "function_count_stats", {}
                            ),
                            "yara_distribution": meta.get("yara_distribution", []),
                            "avtype_distribution": meta.get("avtype_distribution", []),
                            "filetype_distribution": meta.get(
                                "filetype_distribution", []
                            ),
                            "ccip_distribution": meta.get("ccip_distribution", []),
                            "filename_distribution": meta.get(
                                "filename_distribution", []
                            ),
                            "md5_distribution": meta.get("md5_distribution", []),
                            "via": [],
                        },
                    )
                    entry["via"].append(
                        {"file_md5": row["file_md5"], "score": row["score"]}
                    )
        return {axis: list(v.values()) for axis, v in out.items()}

    def run_match(self, scan_id, job_service=None, job_id=None):
        """Compare a cached scan against its scopes. Writes no Kvrocks key."""
        doc = self.get(scan_id)
        if not doc:
            raise RuntimeError(f"Scan {scan_id} expired or unknown")

        params = doc.get("params") or {}
        top_files = int(params.get("top_files") or default_top_files())
        functions = self.load_functions(scan_id)
        scan_md5 = doc.get("file_md5") or "unknown_md5"
        targets = self._targets(scan_id, functions, scan_md5)
        targets_by_fid = {t["fid"]: t for t in targets}
        warnings = list(doc.get("warnings") or [])

        scanned = []
        for index, collection in enumerate(doc.get("scopes") or []):
            if job_service and job_id:
                job_service.update_progress(
                    job_id,
                    10 + int(80 * index / max(1, len(doc["scopes"]))),
                    f"Matching against {collection}",
                )
            scope_params = self._params_for(collection, params)
            try:
                assert_signature_settings_match(collection)
            except ValueError as e:
                # Feature hashes from a different signature mask do not
                # intersect, so this would silently score zero. Say so and
                # carry on -- the other scopes are still worth reporting.
                warnings.append(f"{collection}: {e}")

            candidates = self._discover_scope(collection, targets, scope_params)
            ranked, total_files, hits = self._rank_files(
                candidates, targets_by_fid, top_files
            )

            scored = []
            for md5, mass in ranked:
                row = self._score_file(
                    collection, md5, targets, candidates, scope_params
                )
                if not row:
                    continue
                row["matched_mass"] = round(mass, 4)
                row["matched_functions"] = hits.get(md5, 0)
                # The rows are the bulk of the report; keep them addressable
                # instead of inlining them into the summary.
                self.q.setex(
                    self.key(scan_id, "rows", collection, md5),
                    cache_ttl(),
                    json.dumps(row.pop("diff")),
                )
                scored.append(row)

            scored.sort(key=lambda r: (-float(r.get("score", 0.0)), r["file_md5"]))
            scanned.append(
                {
                    "collection": collection,
                    "params": scope_params,
                    "files_touched": total_files,
                    "files_scored": len(scored),
                    "files": scored,
                    "bin_clusters": self._bin_clusters(
                        collection, scored, scope_params
                    ),
                }
            )

        already = [
            s["collection"]
            for s in scanned
            if any(f["file_md5"] == scan_md5 for f in s["files"])
        ]
        return self.update(
            scan_id,
            status="completed",
            scanned=scanned,
            warnings=warnings,
            already_present=already,
            function_count=len(targets),
            completed_at=int(time.time() * 1000),
        )

    # ------------------------------------------------------------------- rows

    def rows(
        self,
        scan_id,
        collection,
        md5,
        table="matched",
        offset=0,
        limit=50,
        sort_col=None,
        sort_dir="desc",
    ):
        """One page of a scored pair's diff, filtered and sorted server-side."""
        diff = _decode(self.q.get(self.key(scan_id, "rows", collection, md5)))
        if not diff:
            return None

        tables = (
            ["matched", "unique_to_a", "unique_to_b"] if table == "all" else [table]
        )
        out = {}
        for name in tables:
            rows = list(diff.get(name) or [])
            if sort_col:
                rows.sort(
                    key=lambda r: (
                        _sort_key(r.get(sort_col)),
                        r.get("func_a") or r.get("func_id") or "",
                    ),
                    reverse=(sort_dir or "desc").lower() == "desc",
                )
            out[name] = {
                "total": len(rows),
                "offset": offset,
                "limit": limit,
                "rows": rows[offset : offset + limit],
            }
        return out


def _sort_key(value):
    """Numbers sort as numbers, everything else as a string."""
    if isinstance(value, (int, float)):
        return (0, value, "")
    if value is None:
        return (1, 0.0, "")
    try:
        return (0, float(value), "")
    except (TypeError, ValueError):
        return (2, 0.0, str(value))


_scan_service = None


def get_scan_service():
    """Lazy singleton: constructing one opens Redis pools."""
    global _scan_service
    if _scan_service is None:
        _scan_service = ScanService()
    return _scan_service
