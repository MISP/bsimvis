"""Small helpers for address-scoped similarity maintenance."""

import json

from bsimvis.app.services.index_service import delete_similarity


class MaintenanceService:
    def __init__(self, redis_client, similarity_service):
        self.r = redis_client
        self.similarity_service = similarity_service

    def _functions(self, collection, md5=None, batch_uuid=None, address=None):
        selected = None

        def narrow(values):
            nonlocal selected
            values = {v.decode() if isinstance(v, bytes) else v for v in values}
            selected = values if selected is None else selected & values

        if md5:
            narrow(self.r.smembers(f"{collection}:idx:file:functions:{md5}"))
        if batch_uuid:
            narrow(self.r.smembers(f"{collection}:batch:{batch_uuid}:functions"))
        if address:
            narrow(
                self.r.smembers(
                    f"{collection}:idx:func:entrypoint_address:{str(address).lower()}"
                )
            )
        return sorted(selected or ())

    def build(self, collection, md5=None, batch_uuid=None, address=None, **kwargs):
        for fid in self._functions(collection, md5, batch_uuid, address):
            self.similarity_service.build_function(collection, fid, **kwargs)
        return True

    def clear(self, collection, md5=None, batch_uuid=None, address=None, algo=None):
        function_ids = self._functions(collection, md5, batch_uuid, address)
        prefix = f"{collection}:func:"
        sids = set()
        for fid in function_ids:
            clean = fid[len(prefix) :] if fid.startswith(prefix) else fid
            sids.update(self.r.smembers(f"{collection}:sim:involves:func:{clean}"))
        reader = self.r.pipeline(transaction=False)
        sids = [sid.decode() if isinstance(sid, bytes) else sid for sid in sids]
        for sid in sids:
            reader.get(sid)
        pipe = self.r.pipeline(transaction=False)
        for sid, raw in zip(sids, reader.execute()):
            try:
                doc = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                doc = None
            if not doc or (algo and doc.get("algo", "unweighted_cosine") != algo):
                continue
            current_algo = doc.get("algo", "unweighted_cosine")
            for fid in (doc.get("id1"), doc.get("id2")):
                if fid:
                    clean = fid[len(prefix) :] if fid.startswith(prefix) else fid
                    pipe.srem(f"{collection}:sim:involves:func:{clean}", sid)
            for file_md5 in (doc.get("md5_1"), doc.get("md5_2")):
                if file_md5:
                    pipe.srem(f"{collection}:sim:involves:file:{file_md5}", sid)
            pipe.zrem(f"{collection}:sim:score:{current_algo}", sid)
            delete_similarity(pipe, collection, sid, doc)
            pipe.delete(sid)
        for fid in function_ids:
            pipe.srem(
                f"{collection}:built:functions:{algo or 'unweighted_cosine'}", fid
            )
        pipe.execute()
        return True

    def remove_files(self, collection, payload, job_service=None, job_id=None):
        """Delete analyzed data for selected files while retaining uploaded raw bytes."""
        from bsimvis.app.services.index_service import (
            delete_file,
            delete_function,
            delete_similarity,
        )
        from bsimvis.app.services.lineage_service import children, parents, forget
        from bsimvis.app.services.bin_sim_service import _unindex_bin_sim_pair
        from bsimvis.app.services.feature_service import FeatureService

        def txt(value):
            return value.decode() if isinstance(value, bytes) else str(value)

        job_files_key = f"{collection}:remove_job:{job_id}:files" if job_id else None
        selected = set(payload.get("md5s") or ())
        if job_files_key:
            selected.update(txt(v) for v in self.r.smembers(job_files_key))
        if payload.get("batch_uuid"):
            selected.update(
                txt(v)
                for v in self.r.smembers(
                    f"{collection}:batch:{payload['batch_uuid']}:files"
                )
            )
        # Include descendants only when every known parent is also in the removal set.
        changed = True
        while changed:
            changed = False
            for md5 in tuple(selected):
                for child in children(collection, md5, self.r):
                    child_md5 = child["md5"]
                    if child_md5 in selected:
                        continue
                    if parents(collection, child_md5, self.r) and all(
                        edge["md5"] in selected
                        for edge in parents(collection, child_md5, self.r)
                    ):
                        selected.add(child_md5)
                        changed = True
        if not selected:
            return True
        if job_files_key:
            self.r.sadd(job_files_key, *selected)

        # Refuse to race live ingestion jobs for any selected file.
        for jid in self.r.lrange(f"jobs:collection:{collection}", 0, -1):
            jid = txt(jid)
            job = self.r.hgetall(f"job:{jid}")
            if not job:
                continue
            data = {txt(k): txt(v) for k, v in job.items()}
            if data.get("status") not in {"pending", "running"}:
                continue
            if data.get("type") not in {
                "file_data_ingest",
                "ghidra_analyze",
                "idx_meta",
                "idx_functions",
                "idx_features",
                "build_sim",
                "index_sim",
            }:
                continue
            try:
                task = json.loads(data.get("payload") or "{}")
            except ValueError:
                continue
            task_md5s = set(task.get("md5s") or ())
            task_md5s.update(v for v in (task.get("md5"), task.get("file_md5")) if v)
            if task_md5s & selected or task.get("batch_uuid") == payload.get(
                "batch_uuid"
            ):
                raise ValueError(
                    "Removal refused while target files have active ingest jobs"
                )

        bin_algos = set()
        for key in self.r.scan_iter(match=f"{collection}:bin_sim:score:*"):
            bin_algos.add(txt(key).split(f"{collection}:bin_sim:score:", 1)[1])
        from bsimvis.app.services import container_sim_service

        for md5 in selected:
            for algo in bin_algos:
                container_sim_service.clear_for(collection, md5, algo=algo, r=self.r)

        total = len(selected)
        for number, md5 in enumerate(sorted(selected), 1):
            marker_key = f"{collection}:remove:{job_id or 'manual'}:{md5}"
            try:
                marker = json.loads(self.r.get(marker_key) or "{}")
            except (ValueError, TypeError):
                marker = {}
            file_base = f"{collection}:file:{md5}"
            raw_meta = self.r.get(f"{file_base}:meta")
            try:
                file_meta = json.loads(raw_meta) if raw_meta else {}
            except (ValueError, TypeError):
                file_meta = {}
            function_ids = [
                txt(fid)
                for fid in self.r.smembers(f"{collection}:idx:file:functions:{md5}")
            ]
            if not marker:
                marker = {
                    "batch_uuid": file_meta.get("batch_uuid"),
                    "function_count": len(function_ids),
                    "was_indexed": bool(
                        self.r.sismember(f"{collection}:all_files", file_base)
                    ),
                }
                self.r.set(marker_key, json.dumps(marker))
            batch_uuid = marker.get("batch_uuid") or payload.get("batch_uuid")
            # Remove every persisted function pair by its reverse index and stored algorithm.
            sim_ids = set()
            for fid in function_ids:
                clean = fid.removeprefix(f"{collection}:func:")
                sim_ids.update(
                    txt(sid)
                    for sid in self.r.smembers(
                        f"{collection}:sim:involves:func:{clean}"
                    )
                )
            for sid in sim_ids:
                raw = self.r.get(sid)
                try:
                    doc = json.loads(raw) if raw else {}
                except (ValueError, TypeError):
                    doc = {}
                if not doc:
                    continue
                pipe = self.r.pipeline(transaction=False)
                for fid in (doc.get("id1"), doc.get("id2")):
                    if fid:
                        pipe.srem(
                            f"{collection}:sim:involves:func:{fid.removeprefix(f'{collection}:func:')}",
                            sid,
                        )
                for other in (doc.get("md5_1"), doc.get("md5_2")):
                    if other:
                        pipe.srem(f"{collection}:sim:involves:file:{other}", sid)
                sim_algo = doc.get("algo", "unweighted_cosine")
                pipe.zrem(f"{collection}:sim:score:{sim_algo}", sid)
                for fid in (doc.get("id1"), doc.get("id2")):
                    if fid:
                        pipe.srem(f"{collection}:built:functions:{sim_algo}", fid)
                func_meta = []
                file_meta = []
                for fid in (doc.get("id1"), doc.get("id2")):
                    try:
                        func_meta.append(json.loads(self.r.get(f"{fid}:meta") or "{}"))
                    except (ValueError, TypeError):
                        func_meta.append({})
                for other in (doc.get("md5_1"), doc.get("md5_2")):
                    try:
                        file_meta.append(
                            json.loads(
                                self.r.get(f"{collection}:file:{other}:meta") or "{}"
                            )
                        )
                    except (ValueError, TypeError):
                        file_meta.append({})
                delete_similarity(pipe, collection, sid, doc, *func_meta, *file_meta)
                pipe.delete(sid)
                pipe.execute()

            # Binary pair docs carry their own algorithm namespace.
            bin_ids = [
                txt(sid)
                for sid in self.r.smembers(f"{collection}:bin_sim:involves:{md5}")
            ]
            for sid in bin_ids:
                raw = self.r.get(sid)
                try:
                    doc = json.loads(raw) if raw else {}
                except (ValueError, TypeError):
                    doc = {}
                if not doc:
                    continue
                other = (
                    doc.get("md5_b") if doc.get("md5_a") == md5 else doc.get("md5_a")
                )
                pipe = self.r.pipeline(transaction=False)
                if other:
                    pipe.srem(f"{collection}:bin_sim:involves:{other}", sid)
                algo = doc.get("algo", "unweighted_cosine")
                for axis in ("", "_code", "_library", "_content"):
                    pipe.zrem(f"{collection}:bin_sim:score{axis}:{algo}", sid)
                pipe.srem(f"{collection}:bin_sim:built:{algo}", sid)
                ma, mb = [], []
                for value in (doc.get("md5_a"), doc.get("md5_b")):
                    try:
                        ma.append(
                            json.loads(
                                self.r.get(f"{collection}:file:{value}:meta") or "{}"
                            )
                        )
                    except (ValueError, TypeError):
                        ma.append({})
                _unindex_bin_sim_pair(pipe, collection, sid, doc, *ma)
                pipe.delete(sid)
                pipe.execute()

            # delete_file updates all file indexes and containment state before metadata is removed.
            if self.r.exists(f"{file_base}:meta") and self.r.sismember(
                f"{collection}:all_files", file_base
            ):
                delete_file(self.r, collection, md5)
            elif not self.r.exists(f"{file_base}:meta"):
                forget(collection, md5, self.r)
            # Preserve raw sample bytes; clear only analyzed file/function keys.
            keys = [
                txt(k)
                for k in self.r.scan_iter(match=f"{file_base}:*")
                if not txt(k).endswith(":raw")
            ]
            if keys:
                self.r.delete(*keys)
            if function_ids:
                FeatureService(self.r).clear_features(collection, file_md5=md5)
            for fid in function_ids:
                # delete_function reads metadata to remove all secondary indexes.
                delete_function(self.r, collection, md5, fid.rsplit(":", 1)[-1])
                for key in self.r.scan_iter(match=f"{fid}:*"):
                    self.r.delete(key)
                for batch_functions in self.r.scan_iter(
                    match=f"{collection}:batch:*:functions"
                ):
                    self.r.srem(batch_functions, fid)
            # Drop empty feature documents from reverse postings.
            for key in list(
                self.r.scan_iter(match=f"{collection}:feature:*:functions")
            ):
                if self.r.zcard(key) == 0:
                    stem = txt(key)[: -len(":functions")]
                    self.r.delete(key, f"{stem}:meta", f"{stem}:global_meta")
            # The batch membership index also covers legacy files without metadata.
            was_batch_member = False
            if not batch_uuid:
                for batch_key in self.r.scan_iter(match=f"{collection}:batch:*:files"):
                    if self.r.sismember(batch_key, md5):
                        batch_uuid = txt(batch_key).split(":")[2]
                        was_batch_member = True
                        break
            else:
                batch_files_key = f"{collection}:batch:{batch_uuid}:files"
                was_batch_member = bool(self.r.sismember(batch_files_key, md5))
            if batch_uuid:
                bkey = f"{collection}:batch:{batch_uuid}"
                try:
                    batch = json.loads(self.r.get(bkey) or "{}")
                except (ValueError, TypeError):
                    batch = {}
                batch_files_key = f"{collection}:batch:{batch_uuid}:files"
                if not marker.get("batch_counted"):
                    if batch:
                        batch["total_files"] = max(
                            0,
                            int(batch.get("total_files", 0))
                            - (1 if was_batch_member else 0),
                        )
                        batch["total_functions"] = max(
                            0,
                            int(batch.get("total_functions", 0))
                            - (
                                marker.get("function_count", 0)
                                if was_batch_member
                                else 0
                            ),
                        )
                    marker["batch_counted"] = True
                    pipe = self.r.pipeline(transaction=True)
                    pipe.set(marker_key, json.dumps(marker))
                    if batch:
                        pipe.set(bkey, json.dumps(batch))
                    pipe.srem(batch_files_key, md5)
                    pipe.execute()
                else:
                    self.r.srem(batch_files_key, md5)
                if self.r.scard(f"{collection}:batch:{batch_uuid}:files") == 0:
                    self.r.srem(f"{collection}:all_batches", batch_uuid)
                    self.r.delete(
                        f"{collection}:batch:{batch_uuid}",
                        f"{collection}:batch:{batch_uuid}:files",
                        f"{collection}:batch:{batch_uuid}:functions",
                    )
                    global_key = f"global:batch:{batch_uuid}"
                    try:
                        global_meta = json.loads(self.r.get(global_key) or "{}")
                    except (ValueError, TypeError):
                        global_meta = {}
                    global_meta.get("collections", {}).pop(collection, None)
                    if global_meta and global_meta["collections"]:
                        self.r.set(global_key, json.dumps(global_meta))
                    else:
                        self.r.delete(global_key)
                        self.r.srem("global:batches", batch_uuid)
            self.r.delete(marker_key)
            if job_service and job_id:
                job_service.update_progress(job_id, int(number * 100 / total))

        # Container scores depend on child pairs and lineage, so rebuild those
        # canonical aggregates after every selected descendant has been removed.
        for algo in bin_algos:
            container_sim_service.build_container_sims(collection, algo=algo, r=self.r)

        # Full clustering rebuilds consume only the surviving stored edges.
        from bsimvis.app.services.cluster_service import cluster_service
        from bsimvis.app.services.bin_cluster_service import bin_cluster_service

        for key in self.r.scan_iter(match=f"{collection}:sim:score:*"):
            algo = txt(key).split(f"{collection}:sim:score:", 1)[1]
            cluster_service.clear_clustering(collection, algo)
            cluster_service.run_clustering(collection, algo)
        for axis, score_axis in (
            ("overall", "score"),
            ("code", "score_code"),
            ("library", "score_library"),
            ("content", "score_content"),
        ):
            for key in self.r.scan_iter(match=f"{collection}:bin_sim:{score_axis}:*"):
                algo = txt(key).split(f"{collection}:bin_sim:{score_axis}:", 1)[1]
                bin_cluster_service.clear_clusters(collection, algo, axis=axis)
                bin_cluster_service.run_clustering(collection, algo, axis=axis)
        if job_files_key:
            self.r.delete(job_files_key)
        return True
