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

from bsimvis.app.services.bin_sim_tags import (
    load_tag_meta,
    merge_tag_fields,
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
    """Tagging modules a scan runs. Empty = BSim + FunctionID only."""
    mods = config_service.get("scan.modules", [])
    return list(mods) if isinstance(mods, (list, tuple)) else []


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

    def delete(self, scan_id):
        """Drop one scan's cache. Every key carries the scan id, so this is a
        bounded DEL list built from the doc, never a scan of the keyspace."""
        doc = self.get(scan_id)
        keys = [self.key(scan_id, "raw"), self.key(scan_id, "doc")]
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
                    "tags": meta.get("tags") or {},
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
                hits[md5] = hits.get(md5, 0) + 1
        ranked = sorted(mass.items(), key=lambda kv: (-kv[1], kv[0]))
        return ranked[:top_files], len(ranked), hits

    def _score_file(self, collection, md5, targets, candidates_by_fid, params):
        """The canonical BSimVis file score for one candidate file.

        Same greedy assignment and feature-mass-weighted mean the bin_sim
        builder persists (`bin_sim_tags.score_pair`), so this number and a
        stored bin_sim doc's are the same number.
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

        def feat(fid):
            if fid in feat_a:
                return float(feat_a[fid] or 1.0)
            return float(meta_b.get(fid, {}).get("bsim_features_count", 1.0))

        fid_tags = {t["fid"]: tags for t in targets if (tags := t["tags"])}
        fid_tags.update(
            {
                fid: tags
                for fid, value in meta_b.items()
                if (tags := merge_tag_fields(value))
            }
        )
        common = score_pair(
            edges,
            fids_a,
            fids_b,
            feat,
            fid_tags,
            load_tag_meta(self.r, collection) if fid_tags else {},
        )
        self._enrich_clusters(collection, common["diff"], meta_b, params)
        return {
            "collection": collection,
            "file_md5": md5,
            "file_name": file_meta_b.get("file_name", ""),
            "architecture": file_meta_b.get("language_id", ""),
            "functions_count": len(fids_b),
            "algo": params["algo"],
            "tags_rev": read_tags_rev(self.r, collection),
            **common,
        }

    def _enrich_clusters(self, collection, diff, meta_b, params):
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
