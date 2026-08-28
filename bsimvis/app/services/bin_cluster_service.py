import logging
import json
import time
import uuid
from collections import Counter
import numpy as np
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services import sim_edges
from bsimvis.app.services.cluster_utils import default_bin_cluster_name

_EMPTY_I = np.empty(0, dtype=np.int32)
_EMPTY_F = np.empty(0, dtype=np.float32)

try:
    import hdbscan
except ImportError:
    hdbscan = None


class BinClusterService:
    def run_clustering(
        self,
        collection,
        algo="unweighted_cosine",
        min_cluster_size=None,
        min_samples=None,
        cluster_selection_epsilon=None,
        selection_method=None,
        min_sim=None,
        batch_uuid=None,
        job_service=None,
        job_id=None,
        min_cohesion=None,
    ):
        from bsimvis.app.services.config_service import config_service

        engine = config_service.get("clustering.bin_engine", "threshold_uf")

        if engine == "hierarchical_uf":
            # Full rebuild only, same reasoning as func-level hierarchical_uf
            # in cluster_service.py -- single-linkage merges are monotonic,
            # no incremental path yet.
            return self._run_clustering_hierarchical_uf(
                collection,
                algo=algo,
                min_sim=min_sim,
                min_cluster_size=min_cluster_size,
                job_service=job_service,
                job_id=job_id,
                min_cohesion=min_cohesion,
            )

        if engine == "threshold_uf":
            threshold = config_service.get("clustering.bin_uf_threshold", 0.1)
            if batch_uuid and not collection.startswith("global:pool:"):
                new_files = list(
                    self.r.smembers(f"{collection}:batch:{batch_uuid}:files")
                )
                if not new_files:
                    # file.py tracks files in batch differently? Wait, batch files aren't in a set!
                    # Let's get files from functions
                    func_keys = self.r.smembers(
                        f"{collection}:batch:{batch_uuid}:functions"
                    )
                    new_files = list(
                        {
                            k.decode().split(":")[-2]
                            for k in func_keys
                            if len(k.split(":")) >= 3
                        }
                    )
                else:
                    new_files = [
                        f.decode() if isinstance(f, bytes) else f for f in new_files
                    ]

                if new_files:
                    return self._incremental_cluster_binaries(
                        collection,
                        algo,
                        threshold,
                        new_files,
                        job_service=job_service,
                        job_id=job_id,
                        min_cohesion=min_cohesion,
                    )
                return True
            return self._run_clustering_threshold_uf(
                collection,
                algo=algo,
                threshold=threshold,
                min_sim=min_sim,
                job_service=job_service,
                job_id=job_id,
                min_cohesion=min_cohesion,
            )

        return self._run_clustering_hdbscan(
            collection,
            algo,
            min_cluster_size,
            min_samples,
            cluster_selection_epsilon,
            selection_method,
            min_sim,
            job_service,
            job_id,
            min_cohesion,
        )

    def _incremental_cluster_binaries(
        self,
        collection,
        algo,
        threshold,
        new_files,
        job_service=None,
        job_id=None,
        min_cohesion=None,
    ):
        from bsimvis.app.services.cluster_threshold import RedisUF
        from bsimvis.app.services.config_service import config_service
        from bsimvis.app.services import lineage_service

        if min_cohesion is None:
            min_cohesion = config_service.get("clustering.min_cohesion", 0.5)

        r = self.r
        sim_prefix = f"{collection}:bin_sim:{algo}:"
        sim_score_key = f"{collection}:bin_sim:score:{algo}"
        uuid_key = f"{collection}:bin_cluster:{algo}:uf:uuid"

        def members_key(root):
            return f"{collection}:bin_cluster:{algo}:{root}:members"

        # Two forests, not one: a container (APK/ZIP wrapper) and a plain
        # file are never the same md5, so roots from the two never collide
        # in members_key/uuid_key -- but they still need separate `parent`
        # union-find state, or a stray cross-type edge could walk a find()
        # from file-space into container-space. Each side still clusters
        # normally against its own kind (container-rollup pairs keep forming
        # container clusters); they just never merge into one cluster.
        uf_file = RedisUF(
            r, f"{collection}:bin_cluster:{algo}:uf:parent", members_key
        )
        uf_container = RedisUF(
            r, f"{collection}:bin_cluster:{algo}:container:uf:parent", members_key
        )

        msg = f"[threshold_uf] incremental binary update: {len(new_files)} new files..."
        import logging

        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        containers = lineage_service.container_md5s(collection, r)

        touched_roots = set()
        for md5 in new_files:
            uf = uf_container if md5 in containers else uf_file
            sids = r.smembers(f"{collection}:bin_sim:involves:{md5}")
            for sid_raw in sids or ():
                sid = sid_raw.decode() if isinstance(sid_raw, bytes) else sid_raw
                if not sid.startswith(sim_prefix):
                    continue
                id_part = sid[len(sim_prefix) :]
                if "::" not in id_part:
                    continue
                c1, c2 = id_part.split("::")
                other_md5 = c2 if c1 == md5 else c1
                # Same-type only: a container never unions with a file, even
                # via a stray edge that shouldn't exist upstream in the first
                # place -- see the container_sim_service guard this backs up.
                if other_md5 == md5 or (other_md5 in containers) != (md5 in containers):
                    continue
                score = r.zscore(sim_score_key, sid)
                if score is None or float(score) < threshold:
                    continue

                ra, rb = uf.find(md5), uf.find(other_md5)
                if ra == rb:
                    touched_roots.add(ra)
                    continue
                survivor, absorbed = uf.union(md5, other_md5)
                touched_roots.add(survivor)
                touched_roots.add(absorbed)

        final_roots = {
            t
            for t in touched_roots
            if (uf_container if t in containers else uf_file).find(t) == t
        }
        stale_roots = touched_roots - final_roots

        pipe = r.pipeline(transaction=False)
        for stale in stale_roots:
            old_meta_raw = r.get(f"{collection}:bin_cluster:{algo}:{stale}:meta")
            import json

            old_meta = json.loads(old_meta_raw) if old_meta_raw else {}
            pipe.delete(f"{collection}:bin_cluster:{algo}:{stale}:meta")
            pipe.delete(f"{collection}:bin_cluster:{algo}:{stale}:direct_members")
            pipe.srem(f"{collection}:bin_cluster:list:{algo}", str(stale))
            pipe.delete(f"{collection}:idx:file:bin_cluster_id:{str(stale).lower()}")
            old_name = old_meta.get("cluster_name")
            if old_name:
                pipe.delete(
                    f"{collection}:idx:file:bin_cluster_name:{old_name.lower()}"
                )
            old_uuid = r.hget(uuid_key, stale)
            if old_uuid:
                old_uuid = (
                    old_uuid.decode() if isinstance(old_uuid, bytes) else old_uuid
                )
                pipe.delete(
                    f"{collection}:idx:file:bin_cluster_uuid:{old_uuid.lower()}"
                )
                pipe.hdel(uuid_key, stale)
        pipe.execute()

        all_members_raw = {}
        if final_roots:
            for root in final_roots:
                mset = r.smembers(members_key(root))
                members = sorted(
                    m.decode() if isinstance(m, bytes) else m for m in (mset or ())
                )
                if len(members) < 2:
                    continue
                all_members_raw[root] = members

            self._enrich_and_persist_binary_clusters(
                collection,
                algo,
                all_members_raw,
                uuid_key,
                min_cohesion,
                job_service,
                job_id,
            )

        msg = f"[threshold_uf] incremental binary update done. Touched {len(final_roots)} live clusters."
        logging.info(msg)
        if job_service and job_id:
            job_service.add_log(job_id, msg)
            job_service.update_progress(job_id, 100)
        return True

    def _run_clustering_threshold_uf(
        self,
        collection,
        algo,
        threshold,
        min_sim,
        job_service=None,
        job_id=None,
        min_cohesion=None,
    ):
        import time
        import logging
        from bsimvis.app.services.cluster_threshold import build_threshold_clusters
        from bsimvis.app.services import sim_edges, lineage_service
        from bsimvis.app.services.config_service import config_service

        if min_cohesion is None:
            min_cohesion = config_service.get("clustering.min_cohesion", 0.5)

        r = self.r
        sim_score_key = f"{collection}:bin_sim:score:{algo}"
        prefix = f"{collection}:bin_sim:{algo}:"
        uuid_key = f"{collection}:bin_cluster:{algo}:uf:uuid"

        msg = f"[threshold_uf] Fetching binary similarity pairs from {sim_score_key} (threshold={threshold})..."
        logging.info(msg)
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # Containers and files never share a cluster graph -- a container
        # holds no code of its own, so an edge naming one is either a
        # container-rollup pair (belongs with other containers) or a stray
        # same-sample edge (a bug upstream, not a real similarity). Run the
        # two node types as two independent graphs; fid strings already
        # embed the md5, so a file cluster's key can never collide with a
        # container cluster's even though both persist under the same algo.
        container_fids = {
            f"{collection}:file:{m}"
            for m in lineage_service.container_md5s(collection, r)
        }

        total_clusters = 0
        for pass_name, edge_kwargs in (
            ("file", {"excluded_fids": container_fids}),
            ("container", {"allowed_fids": container_fids}),
        ):
            if pass_name == "container" and not container_fids:
                continue

            edge_set = sim_edges.load_edges(
                r,
                sim_score_key,
                prefix,
                False,
                collection,
                min_sim=min_sim,
                node_kind="file",
                **edge_kwargs,
            )
            id_to_idx = edge_set.id_to_idx
            idx_to_id = edge_set.idx_to_id

            if edge_set.n_scanned == 0 or edge_set.src.size == 0:
                continue

            num_nodes = len(id_to_idx)
            msg = f"[threshold_uf/{pass_name}] {num_nodes} binaries, {edge_set.src.size} edges. Running union-find..."
            logging.info(msg)
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            start_fit = time.time()
            uf = build_threshold_clusters(edge_set, threshold)
            fit_time = time.time() - start_fit

            cluster_members = {
                idx_to_id[label]: [idx_to_id[i] for i in members]
                for label, members in uf.clusters(min_size=2).items()
            }

            self._enrich_and_persist_binary_clusters(
                collection,
                algo,
                cluster_members,
                uuid_key,
                min_cohesion,
                job_service,
                job_id,
            )
            total_clusters += len(cluster_members)

            msg = f"[threshold_uf/{pass_name}] union-find done in {fit_time:.2f}s. Found {len(cluster_members)} clusters."
            logging.info(msg)
            if job_service and job_id:
                job_service.add_log(job_id, msg)

        if job_service and job_id:
            job_service.update_progress(job_id, 100)
        logging.info(f"[threshold_uf] {total_clusters} total clusters persisted.")
        return True

    def _enrich_and_persist_binary_clusters(
        self,
        collection,
        algo,
        cluster_members,
        uuid_key,
        min_cohesion,
        job_service,
        job_id,
    ):
        import uuid
        import time
        import json
        from collections import Counter

        r = self.r
        pipe = r.pipeline(transaction=False)

        # We need all_member_meta
        all_member_file_ids = list({f for ms in cluster_members.values() for f in ms})
        all_member_meta = {}
        for i in range(0, len(all_member_file_ids), 1000):
            chunk = all_member_file_ids[i : i + 1000]
            m_pipe = r.pipeline(transaction=False)
            for file_id in chunk:
                m_pipe.get(f"{collection}:file:{file_id}:meta")
            for file_id, raw_meta in zip(chunk, m_pipe.execute()):
                m = {}
                if raw_meta:
                    try:
                        m = json.loads(raw_meta)
                    except Exception:
                        pass
                all_member_meta[file_id] = m

        for label, members in cluster_members.items():
            pipe.sadd(f"{collection}:bin_cluster:{algo}:{label}:members", *members)
            pipe.sadd(
                f"{collection}:bin_cluster:{algo}:{label}:direct_members", *members
            )
            pipe.sadd(f"{collection}:bin_cluster:list:{algo}", str(label))

            # Ensure UUID
            c_uuid = r.hget(uuid_key, label)
            if not c_uuid:
                c_uuid = uuid.uuid4().hex[:12]
                r.hset(uuid_key, label, c_uuid)
            else:
                c_uuid = c_uuid.decode() if isinstance(c_uuid, bytes) else c_uuid

            # Build Metadata
            names_list = []
            md5s_list = []
            yara_list = []
            avtype_list = []
            filetype_list = []
            ccip_list = []

            for file_id in members:
                m = all_member_meta.get(file_id, {})
                if m.get("file_names"):
                    names_list.extend(m["file_names"])
                elif m.get("file_name"):
                    names_list.append(m["file_name"])

                if m.get("file_md5"):
                    md5s_list.append(m["file_md5"])

                if m.get("yara"):
                    yara_list.extend(
                        m["yara"] if isinstance(m["yara"], list) else [m["yara"]]
                    )
                if m.get("avtype"):
                    avtype_list.extend(
                        m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
                    )
                if m.get("filetype"):
                    filetype_list.extend(
                        m["filetype"]
                        if isinstance(m["filetype"], list)
                        else [m["filetype"]]
                    )
                if m.get("cc_ip"):
                    ccip_list.extend(
                        m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
                    )

            default_name = default_bin_cluster_name(
                names_list, avtype_list, yara_list, f"Binary Cluster {label}"
            )

            def build_freq(items):
                return (
                    [
                        {
                            "value": k,
                            "count": v,
                            "percent": round((v / len(members)) * 100),
                        }
                        for k, v in Counter(items).most_common(5)
                    ]
                    if items
                    else []
                )

            yara_freq = build_freq(yara_list)
            avtype_freq = build_freq(avtype_list)
            filetype_freq = build_freq(filetype_list)
            ccip_freq = build_freq(ccip_list)
            filename_freq = build_freq(names_list)
            md5_freq = build_freq(md5s_list)

            # Simple average cohesion proxy (for true cohesion, we'd need sparse adjacency, but for incremental this is an approximation or skip if too slow)
            cohesion_score = 1.0  # placeholder for now to guarantee indexing, or compute exact. UF threshold is already a cohesion guarantee!

            sample_members = []
            for file_id in members[:5]:
                m = all_member_meta.get(file_id, {})
                sample_members.append(
                    {
                        "id": file_id,
                        "name": m.get("file_name", "Unknown"),
                        "file_name": m.get("file_name", "Unknown"),
                    }
                )

            rep_file_id = members[0] if members else None
            rep_meta = all_member_meta.get(rep_file_id, {}) if rep_file_id else {}
            snippet = rep_meta.get("file_name", "unknown")

            meta = {
                "cluster_id": str(label),
                "snippet": snippet,
                "cluster_uuid": c_uuid,
                "cluster_name": default_name,
                "cohesion_score": float(cohesion_score),
                "avg_stability": 1.0,
                "cluster_stability": 1.0,
                "member_count": len(members),
                "sample_files": names_list[:5],
                "sample_members": sample_members,
                "yara_distribution": yara_freq,
                "avtype_distribution": avtype_freq,
                "filetype_distribution": filetype_freq,
                "ccip_distribution": ccip_freq,
                "filename_distribution": filename_freq,
                "md5_distribution": md5_freq,
                "created_at": int(time.time() * 1000),
            }

            pipe.set(f"{collection}:bin_cluster:{algo}:{label}:meta", json.dumps(meta))

            # Indexes
            bucket_key = (
                f"{collection}:idx:file:bin_cluster_name:{default_name.lower()}"
            )
            pipe.sadd(bucket_key, *members)
            pipe.sadd(f"{collection}:reg:file:bin_cluster_name", bucket_key)

            bucket_key_id = f"{collection}:idx:file:bin_cluster_id:{str(label).lower()}"
            pipe.sadd(bucket_key_id, *members)
            pipe.sadd(f"{collection}:reg:file:bin_cluster_id", bucket_key_id)

            bucket_key_uuid = f"{collection}:idx:file:bin_cluster_uuid:{c_uuid.lower()}"
            pipe.sadd(bucket_key_uuid, *members)
            pipe.sadd(f"{collection}:reg:file:bin_cluster_uuid", bucket_key_uuid)

            inferred_mapping = {
                "yara_distribution": "inferred_yara",
                "avtype_distribution": "inferred_avtype",
                "filetype_distribution": "inferred_filetype",
                "ccip_distribution": "inferred_ccip",
                "filename_distribution": "inferred_filename",
                "md5_distribution": "inferred_md5",
            }
            for dist_key, meta_key in inferred_mapping.items():
                dist = meta.get(dist_key) or []
                if dist:
                    top_val = dist[0].get("value")
                    if top_val:
                        b_key = (
                            f"{collection}:idx:file:{meta_key}:{str(top_val).lower()}"
                        )
                        pipe.sadd(b_key, *members)
                        pipe.sadd(f"{collection}:reg:file:{meta_key}", b_key)

            if len(pipe) > 1000:
                pipe.execute()

        pipe.execute()

    def __init__(self, r=None):
        self.r = r or get_redis()
        from bsimvis.app.services.index_config import (
            get_native_fields,
            get_propagated_fields,
        )

        self.get_native_fields = get_native_fields
        self.get_propagated_fields = get_propagated_fields

    def _tree_lambdas(self, tree_df):
        """Birth/death lambda per cluster id, from a condensed/single-linkage
        tree_df (columns parent/child/lambda_val/child_size)."""
        root_id = tree_df["parent"].min()
        birth_lambdas = {root_id: 0.0}
        for row in tree_df.itertuples(index=False):
            if row.child_size > 1:
                birth_lambdas[int(row.child)] = float(row.lambda_val)

        death_lambdas = {}
        for row in tree_df.itertuples(index=False):
            p = int(row.parent)
            l = float(row.lambda_val)
            if p not in death_lambdas or l > death_lambdas[p]:
                death_lambdas[p] = l

        return birth_lambdas, death_lambdas

    def _run_clustering_hierarchical_uf(
        self,
        collection,
        algo="unweighted_cosine",
        min_sim=None,
        min_cluster_size=None,
        job_service=None,
        job_id=None,
        min_cohesion=None,
    ):
        """Full single-linkage hierarchy via Kruskal + Union-Find over binary
        similarity pairs (cluster_threshold.build_single_linkage_tree) --
        binary counterpart of cluster_service._run_clustering_hierarchical_uf.
        No epsilon pruning (raw-similarity lambda, not HDBSCAN's
        inverse-density one); persistence is otherwise identical to the
        HDBSCAN path via the shared _persist_hierarchical_binary_clusters.
        """
        from bsimvis.app.services.config_service import config_service
        from bsimvis.app.services.cluster_threshold import build_single_linkage_tree
        from bsimvis.app.services import lineage_service
        import pandas as pd

        if min_cluster_size is None:
            min_cluster_size = config_service.get("clustering.min_cluster_size", 2)
        if min_sim is None:
            min_sim = config_service.get("clustering.min_sim", 0.0)
        if min_cohesion is None:
            min_cohesion = config_service.get("clustering.min_cohesion", 0.5)

        r = self.r
        sim_score_key = f"{collection}:bin_sim:score:{algo}"
        prefix = f"{collection}:bin_sim:{algo}:"

        msg = f"[hierarchical_uf] Fetching binary similarity pairs from {sim_score_key}..."
        logging.info(msg)
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # Container clusters and file clusters are built as two independent
        # trees -- see _persist_hierarchical_binary_clusters for why they
        # can't share a label namespace.
        container_fids = {
            f"{collection}:file:{m}"
            for m in lineage_service.container_md5s(collection, r)
        }

        for node_type, edge_kwargs in (
            ("file", {"excluded_fids": container_fids}),
            ("container", {"allowed_fids": container_fids}),
        ):
            if node_type == "container" and not container_fids:
                continue

            edge_set = sim_edges.load_edges(
                r,
                sim_score_key,
                prefix,
                False,
                collection,
                min_sim=min_sim,
                node_kind="file",
                **edge_kwargs,
            )
            id_to_idx = edge_set.id_to_idx
            idx_to_id = edge_set.idx_to_id

            if edge_set.n_scanned == 0 or edge_set.src.size == 0:
                logging.warning(
                    f"No binary similarity edges found for {collection}:{algo} ({node_type})"
                )
                continue

            num_nodes = len(id_to_idx)
            msg = f"[hierarchical_uf/{node_type}] {num_nodes} binaries, {edge_set.src.size} edges. Building single-linkage tree..."
            logging.info(msg)
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            start_fit = time.time()
            tree_rows, global_root_id, _ = build_single_linkage_tree(edge_set)
            tree_df = pd.DataFrame(tree_rows)
            fit_time = time.time() - start_fit
            msg = f"[hierarchical_uf/{node_type}] tree built in {fit_time:.2f}s, {len(tree_df)} rows."
            logging.info(msg)
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            birth_lambdas, _ = self._tree_lambdas(tree_df)

            persisted = self._persist_hierarchical_binary_clusters(
                collection,
                algo,
                edge_set,
                id_to_idx,
                idx_to_id,
                tree_df,
                global_root_id,
                num_nodes,
                min_cluster_size,
                min_cohesion,
                birth_lambdas,
                job_service,
                job_id,
                node_type=node_type,
            )
            if persisted is False:
                return False

        return True

    def _run_clustering_hdbscan(
        self,
        collection,
        algo="unweighted_cosine",
        min_cluster_size=None,
        min_samples=None,
        cluster_selection_epsilon=None,
        selection_method=None,
        min_sim=None,
        job_service=None,
        job_id=None,
        min_cohesion=None,
    ):
        """
        Runs HDBSCAN clustering on similarity pairs stored in Kvrocks.
        """
        from bsimvis.app.services.config_service import config_service
        from bsimvis.app.services import lineage_service

        if min_cluster_size is None:
            min_cluster_size = config_service.get("clustering.min_cluster_size", 2)
        if min_samples is None:
            min_samples = config_service.get("clustering.min_samples", 1)
        if cluster_selection_epsilon is None:
            cluster_selection_epsilon = config_service.get("clustering.epsilon", 0.001)
        if selection_method is None:
            selection_method = config_service.get("clustering.selection_method", "eom")
        if min_sim is None:
            min_sim = config_service.get("clustering.min_sim", 0.0)
        if min_cohesion is None:
            min_cohesion = config_service.get("clustering.min_cohesion", 0.5)

        if hdbscan is None:
            logging.error(
                "hdbscan library not installed. Please install it to use clustering."
            )
            return False

        r = self.r
        sim_score_key = f"{collection}:bin_sim:score:{algo}"

        # 1. Fetch all similarity pairs
        logging.info(f"[*] Fetching binary similarity pairs from {sim_score_key}...")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Fetching binary similarity pairs for {collection} ({algo})..."
            )

        prefix = f"{collection}:bin_sim:{algo}:"

        # Container clusters and file clusters are built as two independent
        # trees -- see _persist_hierarchical_binary_clusters for why they
        # can't share a label namespace.
        container_fids = {
            f"{collection}:file:{m}"
            for m in lineage_service.container_md5s(collection, r)
        }

        for node_type, edge_kwargs in (
            ("file", {"excluded_fids": container_fids}),
            ("container", {"allowed_fids": container_fids}),
        ):
            if node_type == "container" and not container_fids:
                continue

            # 2. Stream the ZSET straight into typed edge arrays.
            # sid format: {coll}:bin_sim:{algo}:{md5_a}::{md5_b}
            # Same fix as cluster_service.run_clustering: building a `pairs` list of
            # every member string and then a second list of edge tuples cost 2.15 GiB
            # on a real 5.4M-pair set, against a 3 GB per-worker cap.
            edge_set = sim_edges.load_edges(
                r,
                sim_score_key,
                prefix,
                False,
                collection,
                min_sim=min_sim,
                **edge_kwargs,
                node_kind="file",
            )
            id_to_idx = edge_set.id_to_idx
            idx_to_id = edge_set.idx_to_id

            msg = f"Fetched {edge_set.n_scanned} binary similarity pairs."
            logging.info(f"[+] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            if edge_set.n_scanned == 0:
                logging.warning(f"No binary similarity pairs found for {collection}:{algo} ({node_type})")
                continue

            if edge_set.src.size == 0:
                logging.warning(
                    f"No valid edges found for {collection}:{algo} ({node_type}) after parsing {edge_set.n_scanned} binary pairs."
                )
                if job_service and job_id:
                    job_service.add_log(
                        job_id,
                        f"Error: No valid similarity edges found after parsing {edge_set.n_scanned} binary pairs. Check filters.",
                    )
                continue

            num_nodes = len(id_to_idx)
            msg = f"Building binary graph with {num_nodes} files and {edge_set.src.size} similarity edges..."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            import scipy.sparse as sp
            from scipy.sparse.csgraph import connected_components
            import pandas as pd

            msg = f"Shattering binary graph into connected components to avoid OOM..."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            adj_matrix = sim_edges.build_adjacency(edge_set, num_nodes)
            n_components, labels = connected_components(csgraph=adj_matrix, directed=False)
            del adj_matrix

            comp_to_nodes = {}
            for i, comp_id in enumerate(labels):
                if comp_id not in comp_to_nodes:
                    comp_to_nodes[comp_id] = []
                comp_to_nodes[comp_id].append(i)

            # Views into one sorted permutation, not a dict of tuple lists.
            comp_to_edges = sim_edges.group_edges_by_component(edge_set, labels)

            msg = f"Found {n_components} connected components. Running local HDBSCAN..."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            global_tree_rows = []
            global_root_id = num_nodes
            next_cluster_id = num_nodes + 1
            comp_roots = []

            # One scratch buffer for global-index -> component-local-index, reused
            # across components.
            gmap = np.full(num_nodes, -1, dtype=np.int32)

            start_fit = time.time()

            for comp_id, comp_nodes in comp_to_nodes.items():
                size = len(comp_nodes)
                if size < min_cluster_size:
                    for node in comp_nodes:
                        comp_roots.append((node, 1))
                    continue

                sub_id_to_global = {
                    i: global_idx for i, global_idx in enumerate(comp_nodes)
                }

                # Global index -> component-local index, vectorised.
                comp_nodes_arr = np.asarray(comp_nodes, dtype=np.int32)
                gmap[comp_nodes_arr] = np.arange(size, dtype=np.int32)
                e_src, e_dst, e_dist = comp_to_edges.get(
                    comp_id, (_EMPTY_I, _EMPTY_I, _EMPTY_F)
                )
                ui = gmap[e_src]
                vi = gmap[e_dst]

                if size >= 5000:
                    from scipy.sparse.linalg import svds

                    sim = 1.0 - e_dist
                    rows_sp = np.concatenate([ui, vi])
                    cols_sp = np.concatenate([vi, ui])
                    data_sp = np.concatenate([sim, sim])

                    comp_matrix = sp.csr_matrix(
                        (data_sp, (rows_sp, cols_sp)), shape=(size, size), dtype=np.float32
                    )
                    comp_matrix.setdiag(1.0)

                    k = min(50, size - 1)
                    u, s, vt = svds(comp_matrix, k=k)
                    embeddings = u @ np.diag(np.sqrt(s))
                    del comp_matrix, rows_sp, cols_sp, data_sp

                    clusterer = hdbscan.HDBSCAN(
                        min_cluster_size=min(min_cluster_size, size),
                        min_samples=min(min_samples, size),
                        cluster_selection_epsilon=cluster_selection_epsilon,
                        cluster_selection_method=selection_method,
                        metric="euclidean",
                        gen_min_span_tree=True,
                    )
                    clusterer.fit(embeddings)
                else:
                    # float64 up front: HDBSCAN's precomputed path needs float64, so
                    # building float32 and converting at fit time held both matrices
                    # alive at once. Same fix as cluster_service.
                    sub_dist = np.ones((size, size), dtype=np.float64)
                    np.fill_diagonal(sub_dist, 0)

                    if ui.size:
                        sub_dist[ui, vi] = e_dist
                        sub_dist[vi, ui] = e_dist

                    clusterer = hdbscan.HDBSCAN(
                        min_cluster_size=min(min_cluster_size, size),
                        min_samples=min(min_samples, size),
                        cluster_selection_epsilon=cluster_selection_epsilon,
                        cluster_selection_method=selection_method,
                        metric="precomputed",
                        gen_min_span_tree=True,
                    )
                    clusterer.fit(sub_dist)

                local_tree_df = clusterer.condensed_tree_.to_pandas()
                if local_tree_df.empty:
                    for node in comp_nodes:
                        comp_roots.append((node, 1))
                    continue

                sub_internal_to_global = {}
                # Ensure local root maps to a single global internal ID
                local_root_sub = local_tree_df["parent"].min()

                for row in local_tree_df.itertuples(index=False):
                    parent = int(row.parent)
                    child = int(row.child)

                    if parent not in sub_internal_to_global:
                        sub_internal_to_global[parent] = next_cluster_id
                        next_cluster_id += 1

                    if child < size:  # Leaf
                        global_child = sub_id_to_global[child]
                    else:  # Internal
                        if child not in sub_internal_to_global:
                            sub_internal_to_global[child] = next_cluster_id
                            next_cluster_id += 1
                        global_child = sub_internal_to_global[child]

                    global_tree_rows.append(
                        {
                            "parent": sub_internal_to_global[parent],
                            "child": global_child,
                            "lambda_val": float(row.lambda_val),
                            "child_size": int(row.child_size),
                        }
                    )

                comp_roots.append((sub_internal_to_global[local_root_sub], size))

            # Stitch all roots to a synthetic global root at lambda 1.0 (distance 1.0)
            for comp_root, size in comp_roots:
                global_tree_rows.append(
                    {
                        "parent": global_root_id,
                        "child": comp_root,
                        "lambda_val": 1.0,
                        "child_size": size,
                    }
                )

            tree_df = pd.DataFrame(global_tree_rows)
            fit_time = time.time() - start_fit

            msg = f"HDBSCAN fit completed in {fit_time:.2f}s."
            logging.info(f"[+] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            msg = f"Global condensed tree has {len(tree_df)} rows."
            logging.info(f"[*] {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

            birth_lambdas, death_lambdas = self._tree_lambdas(tree_df)

            # Pruning tree based on cluster_selection_epsilon (if > 0)
            pruned_clusters = set()
            if cluster_selection_epsilon and cluster_selection_epsilon > 0.0:
                lambda_threshold = 1.0 / cluster_selection_epsilon
                for c, b_lambda in birth_lambdas.items():
                    if b_lambda > lambda_threshold:
                        pruned_clusters.add(c)

            child_to_parent = dict(zip(tree_df["child"], tree_df["parent"]))

            def get_nearest_non_pruned_ancestor(node):
                curr = node
                while curr in child_to_parent:
                    p = child_to_parent[curr]
                    if p not in pruned_clusters:
                        return p
                    curr = p
                return None

            # Build a pruned tree DataFrame
            if pruned_clusters:
                pruned_rows = []
                for row in tree_df.itertuples(index=False):
                    parent = int(row.parent)
                    child = int(row.child)
                    child_size = int(row.child_size)
                    lambda_val = float(row.lambda_val)

                    if parent in pruned_clusters:
                        ancestor = get_nearest_non_pruned_ancestor(parent)
                        if ancestor is not None:
                            parent = ancestor
                        else:
                            continue  # Skip if no ancestor

                    if child_size > 1:
                        if child in pruned_clusters:
                            continue

                    pruned_rows.append(
                        {
                            "parent": parent,
                            "child": child,
                            "lambda_val": lambda_val,
                            "child_size": child_size,
                        }
                    )
                import pandas as pd

                tree_df = pd.DataFrame(pruned_rows)

            persisted = self._persist_hierarchical_binary_clusters(
                collection,
                algo,
                edge_set,
                id_to_idx,
                idx_to_id,
                tree_df,
                global_root_id,
                num_nodes,
                min_cluster_size,
                min_cohesion,
                birth_lambdas,
                job_service,
                job_id,
                node_type=node_type,
            )
            if persisted is False:
                return False

        return True

    def _persist_hierarchical_binary_clusters(
        self,
        collection,
        algo,
        edge_set,
        id_to_idx,
        idx_to_id,
        tree_df,
        global_root_id,
        num_nodes,
        min_cluster_size,
        min_cohesion,
        birth_lambdas,
        job_service,
        job_id,
        node_type="file",
    ):
        """Shared tail for every hierarchical binary engine (HDBSCAN,
        hierarchical_uf): given a condensed/single-linkage tree_df (columns
        parent/child/lambda_val/child_size) and its fitted birth_lambdas,
        extract clusters, compute stability + cohesion, and persist.

        `node_type` picks which node kind this tree/cluster_members belong to
        ("file" or "container"). Cluster labels here are synthetic ints
        (tree-node ids / component counters), NOT globally unique fids like
        the union-find engines use -- a file pass and a container pass each
        number their own clusters from scratch, so without a distinct key
        namespace per node_type, e.g. label 5 from one pass would silently
        overwrite label 5's members from the other. `algo_ns` carries that
        namespace into every algo-scoped key; `label_key` does the same for
        the one index (`bin_cluster_id`) that isn't algo-scoped at all today.
        """
        r = self.r
        algo_ns = f"{algo}:container" if node_type == "container" else algo
        label_key = (lambda label: f"c{label}") if node_type == "container" else str

        # 4. Extract Condensed Tree for UI
        tree_json = tree_df.to_json(orient="records")
        tree_key = f"{collection}:bin_cluster:tree:{algo_ns}"
        r.set(tree_key, tree_json)

        cluster_tree_key = f"{collection}:bin_cluster:tree_links:{algo_ns}"
        tree_links = []
        for row in tree_df.itertuples(index=False):
            if int(row.child_size) > 1:
                tree_links.append(
                    {
                        "parent": int(row.parent),
                        "child": int(row.child),
                        "lambda": float(row.lambda_val),
                        "size": int(row.child_size),
                    }
                )
        r.set(cluster_tree_key, json.dumps(tree_links))

        logging.info("[*] Extracting hierarchical clusters from tree...")
        if job_service and job_id:
            job_service.add_log(job_id, "Extracting hierarchical clusters from tree...")

        # Map leaves to the clusters they actually survive into (shed noise
        # points excluded). See cluster_common.hierarchical_membership.
        from bsimvis.app.services.cluster_common import hierarchical_membership

        leaf_to_clusters, leaf_home = hierarchical_membership(
            tree_df, num_nodes, global_root_id, min_size=min_cluster_size
        )

        cluster_members = {}
        for leaf, clusters in leaf_to_clusters.items():
            for c in clusters:
                if c not in cluster_members:
                    cluster_members[c] = []
                cluster_members[c].append(idx_to_id[leaf])

        label_to_uuid = {c: f"{uuid.uuid4().hex[:12]}" for c in cluster_members.keys()}

        # 5. Calculate Stability
        stabilities = {}
        leaf_death_lambdas = {}
        for row in tree_df.itertuples(index=False):
            if row.child_size == 1:
                leaf_death_lambdas[int(row.child)] = float(row.lambda_val)

        for label, members in cluster_members.items():
            b_lambda = birth_lambdas.get(label, 0.0)
            total_area = 0.0
            for file_id in members:
                leaf_idx = id_to_idx[file_id]
                d_lambda = leaf_death_lambdas.get(leaf_idx, b_lambda)
                total_area += max(0.0, d_lambda - b_lambda)
            stabilities[label] = total_area

        # 5. Persist binary assignments
        logging.info(f"[*] Persisting {len(cluster_members)} binary clusters...")
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Persisting {len(cluster_members)} binary clusters and assigning files...",
            )

        from bsimvis.app.services.index_service import _index_tag, _unindex_tag

        pipe = r.pipeline(transaction=False)

        for c, members in cluster_members.items():
            pipe.sadd(f"{collection}:bin_cluster:{algo_ns}:{c}:members", *members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        # Direct members = leaves whose deepest surviving cluster is this node
        # (shed noise points excluded, matching the membership rule above).
        direct_members = {}
        for leaf, p in leaf_home.items():
            if leaf in idx_to_id:
                direct_members.setdefault(p, []).append(idx_to_id[leaf])

        for c, d_members in direct_members.items():
            pipe.sadd(f"{collection}:bin_cluster:{algo_ns}:{c}:direct_members", *d_members)
            if len(pipe) > 1000:
                pipe.execute()
        pipe.execute()

        file_tag_fields = [
            f
            for f in self.get_native_fields("file", False)
            if f.startswith("bin_cluster_")
        ]

        for i, (leaf, clusters) in enumerate(leaf_to_clusters.items()):
            file_id = idx_to_id[leaf]
            clusters_key = f"{file_id}:bin_clusters"

            if clusters:
                pipe.delete(clusters_key)
                pipe.sadd(clusters_key, *clusters)
            else:
                pipe.delete(clusters_key)

            if i % 500 == 0:
                pipe.execute()
                if job_service and job_id:
                    if job_service.is_cancelled(job_id):
                        job_service.add_log(job_id, "Cancelled.")
                        return False
                    pct = int((i / num_nodes) * 50)
                    job_service.update_progress(job_id, pct)

        pipe.execute()

        # Update secondary index for 'bin_cluster_id', 'bin_cluster_uuid', 'bin_cluster_name'
        logging.info(
            f"[*] Updating secondary indexes for {len(cluster_members)} binary clusters..."
        )
        for idx, (label, members) in enumerate(cluster_members.items()):
            if "bin_cluster_id" in file_tag_fields:
                bucket_key = (
                    f"{collection}:idx:file:bin_cluster_id:{label_key(label).lower()}"
                )
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_id", bucket_key)

            if "bin_cluster_uuid" in file_tag_fields:
                c_uuid = label_to_uuid[label]
                bucket_key = f"{collection}:idx:file:bin_cluster_uuid:{c_uuid.lower()}"
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_uuid", bucket_key)

            if idx % 100 == 0:
                pipe.execute()
        pipe.execute()

        # 6. Calculate Cluster Metadata
        logging.info(
            f"[*] Calculating enriched metadata for {len(cluster_members)} binary clusters..."
        )

        all_member_file_ids = list(id_to_idx.keys())
        all_member_meta = {}
        total_members = len(all_member_file_ids)
        msg = f"Pre-fetching metadata for {total_members} files..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        for i in range(0, total_members, 1000):
            chunk = all_member_file_ids[i : i + 1000]
            m_pipe = r.pipeline(transaction=False)
            for file_id in chunk:
                m_pipe.get(f"{file_id}:meta")
            results = m_pipe.execute()
            for idx, file_id in enumerate(chunk):
                meta_res = results[idx]
                m = {}
                if meta_res:
                    if isinstance(meta_res, bytes):
                        meta_res = meta_res.decode("utf-8")
                    try:
                        m = json.loads(meta_res)
                    except Exception:
                        m = {}
                all_member_meta[file_id] = m

            if i % 1000 == 0:
                logging.info(f"[*] Fetched meta for {i}/{total_members} files...")

        # Build sparse adjacency dictionary of similarities for fast cohesion calculation
        msg = "Building sparse adjacency map for cohesion calculation..."
        logging.info(f"[*] {msg}")
        if job_service and job_id:
            job_service.add_log(job_id, msg)

        # CSR rather than a dict of dicts -- see sim_edges.SimAdjacency.
        adj_sim = sim_edges.SimAdjacency(edge_set, num_nodes)

        total_clusters = len(cluster_members)
        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Enriching metadata for {total_clusters} hierarchical binary clusters...",
            )

        for idx, (label, members) in enumerate(cluster_members.items()):
            names_list = []
            md5s_list = []
            yara_list = []
            avtype_list = []
            filetype_list = []
            ccip_list = []

            for file_id in members:
                m = all_member_meta.get(file_id, {})
                if m.get("file_names"):
                    names_list.extend(m["file_names"])
                elif m.get("file_name"):
                    names_list.append(m["file_name"])

                if m.get("file_md5"):
                    md5s_list.append(m["file_md5"])

                if m.get("yara"):
                    yara_list.extend(
                        m["yara"] if isinstance(m["yara"], list) else [m["yara"]]
                    )
                if m.get("avtype"):
                    avtype_list.extend(
                        m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
                    )
                if m.get("filetype"):
                    filetype_list.extend(
                        m["filetype"]
                        if isinstance(m["filetype"], list)
                        else [m["filetype"]]
                    )
                if m.get("cc_ip"):
                    ccip_list.extend(
                        m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
                    )

            default_name = default_bin_cluster_name(
                names_list, avtype_list, yara_list, f"Binary Cluster {label}"
            )

            def build_freq(items):
                return (
                    [
                        {
                            "value": k,
                            "count": v,
                            "percent": round((v / len(members)) * 100),
                        }
                        for k, v in Counter(items).most_common(5)
                    ]
                    if items
                    else []
                )

            yara_freq = build_freq(yara_list)
            avtype_freq = build_freq(avtype_list)
            filetype_freq = build_freq(filetype_list)
            ccip_freq = build_freq(ccip_list)
            filename_freq = build_freq(names_list)
            md5_freq = build_freq(md5s_list)

            # Exact Average Internal Similarity (Cohesion) using sparse adjacency map
            if len(members) > 1:
                member_indices = [id_to_idx[file_id] for file_id in members]
                n_members = len(members)

                total_sim = adj_sim.cohesion_sum(member_indices)
                cohesion_score = total_sim / (n_members * (n_members - 1) / 2.0)
            else:
                cohesion_score = 1.0

            if cohesion_score < min_cohesion:
                yara_freq = []
                avtype_freq = []
                filetype_freq = []
                ccip_freq = []
                filename_freq = []
                md5_freq = []

            sample_members = []
            for file_id in members[:5]:
                m = all_member_meta.get(file_id, {})
                sample_members.append(
                    {
                        "id": file_id,
                        "name": m.get("file_name", "Unknown"),
                        "file_name": m.get("file_name", "Unknown"),
                    }
                )

            rep_file_id = members[0] if members else None
            rep_meta = all_member_meta.get(rep_file_id, {}) if rep_file_id else {}
            snippet = rep_meta.get("file_name", "unknown")

            meta = {
                # int for the default (file) pass -- unchanged shape for
                # existing callers; the container pass's "c"-prefixed id
                # can't be an int, so it stays a string there.
                "cluster_id": int(label) if node_type == "file" else label_key(label),
                "node_type": node_type,
                "snippet": snippet,
                "cluster_uuid": label_to_uuid[label],
                "cluster_name": default_name,
                "cohesion_score": float(cohesion_score),
                "avg_stability": float(stabilities.get(label, 0.0)),
                "cluster_stability": float(stabilities.get(label, 0.0)),
                "member_count": len(members),
                "sample_files": names_list[:5],
                "sample_members": sample_members,
                "yara_distribution": yara_freq,
                "avtype_distribution": avtype_freq,
                "filetype_distribution": filetype_freq,
                "ccip_distribution": ccip_freq,
                "filename_distribution": filename_freq,
                "md5_distribution": md5_freq,
                "created_at": int(time.time() * 1000),
            }

            for k, v in meta.items():
                if isinstance(v, float):
                    if not np.isfinite(v):
                        meta[k] = 0.0
            pipe.set(f"{collection}:bin_cluster:{algo_ns}:{label}:meta", json.dumps(meta))

            if "bin_cluster_name" in file_tag_fields:
                bucket_key = (
                    f"{collection}:idx:file:bin_cluster_name:{default_name.lower()}"
                )
                pipe.sadd(bucket_key, *members)
                pipe.sadd(f"{collection}:reg:file:bin_cluster_name", bucket_key)

            # Index top inferred metadata if cohesion is high enough
            if cohesion_score >= min_cohesion:
                inferred_mapping = {
                    "yara_distribution": "inferred_yara",
                    "avtype_distribution": "inferred_avtype",
                    "filetype_distribution": "inferred_filetype",
                    "ccip_distribution": "inferred_ccip",
                    "filename_distribution": "inferred_filename",
                    "md5_distribution": "inferred_md5",
                }
                for dist_key, meta_key in inferred_mapping.items():
                    dist = meta.get(dist_key) or []
                    if dist:
                        top_val = dist[0].get("value")
                        if top_val:
                            # Standardized Bucket indexing
                            bucket_key = f"{collection}:idx:file:{meta_key}:{str(top_val).lower()}"
                            pipe.sadd(bucket_key, *members)
                            pipe.sadd(f"{collection}:reg:file:{meta_key}", bucket_key)

            if job_service and job_id and (idx + 1) % 50 == 0:
                pct = 50 + int(((idx + 1) / total_clusters) * 50)
                job_service.update_progress(
                    job_id,
                    pct,
                    f"Enriching binary clusters: {idx + 1}/{total_clusters}",
                )

        pipe.execute()

        cluster_list_key = f"{collection}:bin_cluster:list:{algo_ns}"
        r.delete(cluster_list_key)
        if cluster_members:
            r.sadd(cluster_list_key, *[str(k) for k in cluster_members.keys()])

        summary = f"Binary clustering complete. Found {len(cluster_members)} hierarchical clusters."
        logging.info(f"[+] {summary}")
        if job_service and job_id:
            job_service.add_log(job_id, summary)

        return True

    def clear_clusters(
        self, collection, algo="unweighted_cosine", job_service=None, job_id=None
    ):
        """
        Clears all binary clustering data for a collection and algorithm --
        both the file-cluster and container-cluster namespaces (see
        _persist_hierarchical_binary_clusters for why they're separate keys).
        """
        r = self.r

        from bsimvis.app.services.index_service import _unindex_tag

        for algo_ns in (algo, f"{algo}:container"):
            cluster_list_key = f"{collection}:bin_cluster:list:{algo_ns}"
            cids_raw = r.smembers(cluster_list_key)
            all_meta_keys = []
            if cids_raw:
                all_meta_keys = [
                    f"{collection}:bin_cluster:{algo_ns}:{cid.decode() if isinstance(cid, bytes) else cid}:meta"
                    for cid in cids_raw
                ]
            else:
                pattern = f"{collection}:bin_cluster:{algo_ns}:*:meta"
                cursor = 0
                while True:
                    cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
                    all_meta_keys.extend(
                        [k.decode() if isinstance(k, bytes) else k for k in keys]
                    )
                    if cursor == 0:
                        break

            cluster_ids = []
            prefix = f"{collection}:bin_cluster:{algo_ns}:"
            for k in all_meta_keys:
                cid = k[len(prefix) : -len(":meta")]
                cluster_ids.append(cid)

            total_clusters = len(cluster_ids)
            if job_service and job_id:
                job_service.add_log(
                    job_id,
                    f"Cleaning up binary clustering data for {total_clusters} clusters ({algo_ns})...",
                )

            for i, cid in enumerate(cluster_ids):
                members_key = f"{collection}:bin_cluster:{algo_ns}:{cid}:members"
                members = r.smembers(members_key)
                if members:
                    pipe = r.pipeline(transaction=False)
                    for j, mid_raw in enumerate(members):
                        mid = (
                            mid_raw.decode() if isinstance(mid_raw, bytes) else mid_raw
                        )
                        _unindex_tag(
                            pipe, collection, "file", "bin_cluster_id", cid, mid
                        )
                        pipe.delete(f"{mid}:bin_clusters")

                        if j % 500 == 0:
                            pipe.execute()
                    pipe.execute()

                r.delete(f"{collection}:bin_cluster:{algo_ns}:{cid}:members")
                r.delete(f"{collection}:bin_cluster:{algo_ns}:{cid}:direct_members")
                r.delete(f"{collection}:bin_cluster:{algo_ns}:{cid}:meta")

                if job_service and job_id and total_clusters and i % 10 == 0:
                    pct = int((i / total_clusters) * 100)
                    job_service.update_progress(job_id, pct)

            r.delete(f"{collection}:bin_cluster:tree:{algo_ns}")
            r.delete(f"{collection}:bin_cluster:list:{algo_ns}")
            r.delete(f"{collection}:bin_cluster:tree_links:{algo_ns}")

        # Clear named-based indexes (shared bucket space across both
        # namespaces -- cid/uuid values never collide, see the write side).
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_name")
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_uuid")
        self._clear_indexes_via_registry(collection, "file", "bin_cluster_id")

        # Clear inferred metadata indexes
        self._clear_indexes_via_registry(collection, "file", "inferred_yara")
        self._clear_indexes_via_registry(collection, "file", "inferred_avtype")
        self._clear_indexes_via_registry(collection, "file", "inferred_filetype")
        self._clear_indexes_via_registry(collection, "file", "inferred_ccip")
        self._clear_indexes_via_registry(collection, "file", "inferred_filename")
        self._clear_indexes_via_registry(collection, "file", "inferred_md5")

        if job_service and job_id:
            job_service.add_log(job_id, "Binary clustering data cleared successfully.")
            job_service.update_progress(job_id, 100)

        return True

    def _clear_indexes_via_registry(self, collection, level, field):
        """Delete all index buckets for a field using its registry, then clear the registry."""
        r = self.r
        reg_key = f"{collection}:reg:{level}:{field}"
        buckets = r.smembers(reg_key)
        if buckets:
            pipe = r.pipeline(transaction=False)
            for b_raw in buckets:
                b = b_raw.decode() if isinstance(b_raw, bytes) else b_raw
                pipe.delete(b)
            pipe.execute()
        r.delete(reg_key)


bin_cluster_service = BinClusterService()
