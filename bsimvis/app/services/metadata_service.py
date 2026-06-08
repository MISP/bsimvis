import json
import logging
from collections import Counter
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_config import INDEX_CONFIG, NUM_FIELDS
from bsimvis.app.services.index_service import (
    _index_tag,
    _unindex_tag,
    _index_num,
    _unindex_num,
    save_similarity,
    delete_similarity,
)
from bsimvis.app.services.bin_sim_service import (
    _index_bin_sim_pair,
    _unindex_bin_sim_pair,
)


class MetadataService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def propagate_metadata(self, collection, updates, job_service=None, job_id=None):
        r = self.r
        if not updates:
            logging.info("No updates passed to propagate_metadata.")
            return True

        total_files = len(updates)
        logging.info(f"[*] Starting metadata propagation for {total_files} files in collection '{collection}'...")

        # Check active algorithms directly using exists (O(1) operations) to avoid scanning millions of keys
        algos = set()
        for candidate_algo in ["unweighted_cosine", "jaccard", "milvus_sparse"]:
            if r.exists(f"{collection}:bin_cluster:list:{candidate_algo}"):
                algos.add(candidate_algo)

        if not algos:
            algos.add("unweighted_cosine")

        affected_clusters_to_recalculate = set()

        for idx_file, (md5, file_updates) in enumerate(updates.items()):
            if job_service and job_id:
                pct = int((idx_file) / total_files * 100)
                job_service.update_progress(
                    job_id, pct, f"Propagating metadata for file {md5} ({idx_file+1}/{total_files})"
                )

            file_base_id = f"{collection}:file:{md5}"
            file_meta_key = f"{file_base_id}:meta"

            # 1. Fetch current file metadata
            old_meta_raw = r.json().get(file_meta_key, "$")
            if not old_meta_raw:
                logging.warning(f"[-] File metadata not found for {md5} in collection '{collection}'")
                continue

            old_meta = old_meta_raw[0] if isinstance(old_meta_raw, list) else old_meta_raw
            if isinstance(old_meta, str):
                old_meta = json.loads(old_meta)

            old_meta_copy = dict(old_meta)

            # 2. Merge updates
            list_fields = [
                "tags",
                "user_tags",
                "first_seen",
                "last_seen",
                "filetype",
                "avtype",
                "yara",
                "cc_ip",
                "file_names",
            ]
            new_meta = dict(old_meta)
            changed_fields = []

            for field, val in file_updates.items():
                if field in list_fields:
                    if val is None or val == "" or val == "-":
                        cleaned_val = []
                    elif isinstance(val, str):
                        cleaned_val = [v.strip() for v in val.split(",") if v.strip()]
                    elif isinstance(val, list):
                        cleaned_val = [
                            str(v).strip()
                            for v in val
                            if str(v).strip() and str(v).strip() != "-"
                        ]
                    else:
                        cleaned_val = [str(val).strip()]
                else:
                    cleaned_val = val

                old_val = old_meta.get(field)
                if old_val != cleaned_val:
                    new_meta[field] = cleaned_val
                    changed_fields.append(field)

            if not changed_fields:
                logging.info(f"[*] No changes detected for file {md5}")
                continue

            if "file_names" in changed_fields:
                names = new_meta.get("file_names", [])
                if names:
                    new_meta["file_name"] = names[0]
                    changed_fields.append("file_name")

            # 3. Store merged file meta
            r.json().set(file_meta_key, "$", new_meta)

            # 4. Update file-level secondary indexes
            pipe = r.pipeline()
            file_config = INDEX_CONFIG.get("file", {})
            for field in changed_fields:
                if field not in file_config:
                    continue
                old_val = old_meta_copy.get(field)
                new_val = new_meta.get(field)

                if field in NUM_FIELDS:
                    _unindex_num(pipe, collection, "file", field, file_base_id)
                    _index_num(pipe, collection, "file", field, new_val, file_base_id)
                else:
                    _unindex_tag(pipe, collection, "file", field, old_val, file_base_id)
                    _index_tag(pipe, collection, "file", field, new_val, file_base_id)

            pipe.execute()

            # 5. Propagate copied fields to all functions of the file
            funcs_key = f"{collection}:idx:file:functions:{md5}"
            func_ids = [
                fid.decode() if isinstance(fid, bytes) else str(fid)
                for fid in r.smembers(funcs_key)
            ]

            fields_to_propagate = [
                f for f in changed_fields if f in file_config and "func" in file_config[f]
            ]

            if func_ids and fields_to_propagate:
                logging.info(f"[*] Propagating {fields_to_propagate} to {len(func_ids)} functions of {md5}")
                chunk_size = 500
                for i in range(0, len(func_ids), chunk_size):
                    chunk = func_ids[i : i + chunk_size]
                    get_pipe = r.pipeline()
                    for func_id in chunk:
                        get_pipe.json().get(f"{func_id}:meta", "$")
                    meta_results = get_pipe.execute()

                    write_pipe = r.pipeline()
                    any_changes = False

                    for func_id, fmeta_raw in zip(chunk, meta_results):
                        if not fmeta_raw:
                            continue
                        old_fmeta = fmeta_raw[0] if isinstance(fmeta_raw, list) else fmeta_raw
                        if isinstance(old_fmeta, str):
                            old_fmeta = json.loads(old_fmeta)

                        new_fmeta = dict(old_fmeta)
                        f_changed = []
                        for f in fields_to_propagate:
                            new_fmeta[f] = new_meta.get(f)
                            old_val = old_fmeta.get(f)
                            new_val = new_meta.get(f)
                            if old_val != new_val:
                                f_changed.append(f)
                                if f in NUM_FIELDS:
                                    _unindex_num(write_pipe, collection, "func", f, func_id)
                                    _index_num(write_pipe, collection, "func", f, new_val, func_id)
                                else:
                                    _unindex_tag(write_pipe, collection, "func", f, old_val, func_id)
                                    _index_tag(write_pipe, collection, "func", f, new_val, func_id)

                        if f_changed:
                            write_pipe.json().set(f"{func_id}:meta", "$", new_fmeta)
                            any_changes = True

                    if any_changes:
                        write_pipe.execute()

            # 6. Propagate to function similarity records
            sim_involves_key = f"{collection}:sim:involves:file:{md5}"
            sim_ids = [
                sid.decode() if isinstance(sid, bytes) else str(sid)
                for sid in r.smembers(sim_involves_key)
            ]
            if sim_ids:
                logging.info(f"[*] Propagating metadata to {len(sim_ids)} function similarity records for {md5}")
                sim_docs_raw = []
                chunk_size = 1000
                for i in range(0, len(sim_ids), chunk_size):
                    chunk = sim_ids[i : i + chunk_size]
                    sim_pipe = r.pipeline()
                    for sid in chunk:
                        sim_pipe.json().get(sid, "$")
                    sim_docs_raw.extend(sim_pipe.execute())

                valid_sims = []
                func_ids_needed = set()
                file_ids_needed = set()

                for sid, raw_doc in zip(sim_ids, sim_docs_raw):
                    if not raw_doc:
                        continue
                    doc = raw_doc[0] if isinstance(raw_doc, list) else raw_doc
                    if isinstance(doc, str):
                        doc = json.loads(doc)
                    valid_sims.append((sid, doc))
                    func_ids_needed.add(doc["id1"])
                    func_ids_needed.add(doc["id2"])
                    file_ids_needed.add(f"{collection}:file:{doc['md5_1']}")
                    file_ids_needed.add(f"{collection}:file:{doc['md5_2']}")

                func_ids_list = list(func_ids_needed)
                file_ids_list = list(file_ids_needed)
                func_meta_map = {}
                file_meta_map = {}

                # Chunked fetch func metadata
                for i in range(0, len(func_ids_list), chunk_size):
                    chunk = func_ids_list[i : i + chunk_size]
                    meta_pipe = r.pipeline()
                    for fid in chunk:
                        meta_pipe.json().get(f"{fid}:meta", "$")
                    meta_results = meta_pipe.execute()
                    for fid, res in zip(chunk, meta_results):
                        m = res[0] if isinstance(res, list) and res else {}
                        if isinstance(m, str):
                            m = json.loads(m)
                        func_meta_map[fid] = m

                # Chunked fetch file metadata
                for i in range(0, len(file_ids_list), chunk_size):
                    chunk = file_ids_list[i : i + chunk_size]
                    meta_pipe = r.pipeline()
                    for fid in chunk:
                        meta_pipe.json().get(f"{fid}:meta", "$")
                    meta_results = meta_pipe.execute()
                    for fid, res in zip(chunk, meta_results):
                        m = res[0] if isinstance(res, list) and res else {}
                        if isinstance(m, str):
                            m = json.loads(m)
                        file_meta_map[fid] = m

                # Chunked delete & save similarities
                for chunk_idx in range(0, len(valid_sims), 500):
                    sim_chunk = valid_sims[chunk_idx : chunk_idx + 500]
                    idx_pipe = r.pipeline()
                    for sid, doc in sim_chunk:
                        fmeta1 = func_meta_map.get(doc["id1"])
                        fmeta2 = func_meta_map.get(doc["id2"])

                        file_id1 = f"{collection}:file:{doc['md5_1']}"
                        file_id2 = f"{collection}:file:{doc['md5_2']}"

                        fmeta1_doc = file_meta_map.get(file_id1, {})
                        fmeta2_doc = file_meta_map.get(file_id2, {})

                        if doc["md5_1"] == md5:
                            fmeta1_old = old_meta_copy
                            fmeta1_new = new_meta
                        else:
                            fmeta1_old = fmeta1_doc
                            fmeta1_new = fmeta1_doc

                        if doc["md5_2"] == md5:
                            fmeta2_old = old_meta_copy
                            fmeta2_new = new_meta
                        else:
                            fmeta2_old = fmeta2_doc
                            fmeta2_new = fmeta2_doc

                        delete_similarity(
                            idx_pipe,
                            collection,
                            sid,
                            doc,
                            fmeta1,
                            fmeta2,
                            fmeta1_old,
                            fmeta2_old,
                        )
                        save_similarity(
                            idx_pipe,
                            collection,
                            sid,
                            doc,
                            fmeta1,
                            fmeta2,
                            fmeta1_new,
                            fmeta2_new,
                        )
                    idx_pipe.execute()

            # 7. Propagate to binary similarity records
            binsim_involves_a = [
                sid.decode() if isinstance(sid, bytes) else str(sid)
                for sid in r.smembers(f"{collection}:idx:bin_sim:md5_a:{md5}")
            ]
            binsim_involves_b = [
                sid.decode() if isinstance(sid, bytes) else str(sid)
                for sid in r.smembers(f"{collection}:idx:bin_sim:md5_b:{md5}")
            ]
            binsim_ids = list(set(binsim_involves_a + binsim_involves_b))
            if binsim_ids:
                logging.info(f"[*] Propagating metadata to {len(binsim_ids)} binary similarity records for {md5}")
                bs_docs_raw = []
                chunk_size = 1000
                for i in range(0, len(binsim_ids), chunk_size):
                    chunk = binsim_ids[i : i + chunk_size]
                    bs_pipe = r.pipeline()
                    for sid in chunk:
                        bs_pipe.json().get(sid, "$")
                    bs_docs_raw.extend(bs_pipe.execute())

                valid_bs = []
                file_ids_needed = set()
                for sid, raw_doc in zip(binsim_ids, bs_docs_raw):
                    if not raw_doc:
                        continue
                    doc = raw_doc[0] if isinstance(raw_doc, list) else raw_doc
                    if isinstance(doc, str):
                        doc = json.loads(doc)
                    valid_bs.append((sid, doc))
                    file_ids_needed.add(f"{collection}:file:{doc['md5_a']}")
                    file_ids_needed.add(f"{collection}:file:{doc['md5_b']}")

                file_ids_list = list(file_ids_needed)
                file_meta_map = {}
                for i in range(0, len(file_ids_list), chunk_size):
                    chunk = file_ids_list[i : i + chunk_size]
                    meta_pipe = r.pipeline()
                    for fid in chunk:
                        meta_pipe.json().get(f"{fid}:meta", "$")
                    meta_results = meta_pipe.execute()
                    for fid, res in zip(chunk, meta_results):
                        m = res[0] if isinstance(res, list) and res else {}
                        if isinstance(m, str):
                            m = json.loads(m)
                        file_meta_map[fid] = m

                for chunk_idx in range(0, len(valid_bs), 500):
                    bs_chunk = valid_bs[chunk_idx : chunk_idx + 500]
                    bs_idx_pipe = r.pipeline()
                    for sid, doc in bs_chunk:
                        file_id_a = f"{collection}:file:{doc['md5_a']}"
                        file_id_b = f"{collection}:file:{doc['md5_b']}"

                        meta_a_doc = file_meta_map.get(file_id_a, {})
                        meta_b_doc = file_meta_map.get(file_id_b, {})

                        if doc["md5_a"] == md5:
                            meta_a_old = old_meta_copy
                            meta_a_new = new_meta
                        else:
                            meta_a_old = meta_a_doc
                            meta_a_new = meta_a_doc

                        if doc["md5_b"] == md5:
                            meta_b_old = old_meta_copy
                            meta_b_new = new_meta
                        else:
                            meta_b_old = meta_b_doc
                            meta_b_new = meta_b_doc

                        _unindex_bin_sim_pair(
                            bs_idx_pipe, collection, sid, doc, meta_a_old, meta_b_old
                        )
                        _index_bin_sim_pair(
                            bs_idx_pipe, collection, sid, doc, meta_a_new, meta_b_new
                        )
                    bs_idx_pipe.execute()

            # 8. Track affected binary clusters for recalculation at the end
            bin_clusters_key = f"{file_base_id}:bin_clusters"
            affected_clusters = [
                cid.decode() if isinstance(cid, bytes) else str(cid)
                for cid in r.smembers(bin_clusters_key)
            ]
            for algo in algos:
                for cid in affected_clusters:
                    affected_clusters_to_recalculate.add((algo, cid))

        # 9. Recalculate affected binary clusters exactly once per cluster
        if affected_clusters_to_recalculate:
            logging.info(f"[*] Recalculating statistics for {len(affected_clusters_to_recalculate)} affected binary clusters...")
            for algo, cid in affected_clusters_to_recalculate:
                meta_key = f"{collection}:bin_cluster:{algo}:{cid}:meta"
                if not r.exists(meta_key):
                    continue

                old_cluster_meta_raw = r.json().get(meta_key, "$")
                if not old_cluster_meta_raw:
                    continue
                old_cm = (
                    old_cluster_meta_raw[0]
                    if isinstance(old_cluster_meta_raw, list)
                    else old_cluster_meta_raw
                )
                if isinstance(old_cm, str):
                    old_cm = json.loads(old_cm)

                members_key = f"{collection}:bin_cluster:{algo}:{cid}:members"
                members = [
                    m.decode() if isinstance(m, bytes) else str(m)
                    for m in r.smembers(members_key)
                ]

                if not members:
                    continue

                c_meta_pipe = r.pipeline()
                for mid in members:
                    c_meta_pipe.json().get(f"{mid}:meta", "$")
                c_meta_results = c_meta_pipe.execute()

                names = []
                yara_list = []
                avtype_list = []
                filetype_list = []
                ccip_list = []

                for mid, res in zip(members, c_meta_results):
                    if not res:
                        continue
                    m = res[0] if isinstance(res, list) else res
                    if isinstance(m, str):
                        m = json.loads(m)
                    if m.get("file_name"):
                        names.append(m["file_name"])
                    if m.get("yara"):
                        yara_list.extend(
                            m["yara"]
                            if isinstance(m["yara"], list)
                            else [m["yara"]]
                        )
                    if m.get("avtype"):
                        avtype_list.extend(
                            m["avtype"]
                            if isinstance(m["avtype"], list)
                            else [m["avtype"]]
                        )
                    if m.get("filetype"):
                        filetype_list.extend(
                            m["filetype"]
                            if isinstance(m["filetype"], list)
                            else [m["filetype"]]
                        )
                    if m.get("cc_ip"):
                        ccip_list.extend(
                            m["cc_ip"]
                            if isinstance(m["cc_ip"], list)
                            else [m["cc_ip"]]
                        )

                def build_freq(items):
                    return (
                        [
                            {
                                "value": k,
                                "count": v,
                                "percent": round((v / len(items)) * 100),
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

                snippet = names[0] if names else "unknown"
                new_default_name = (
                    Counter(names).most_common(1)[0][0]
                    if names
                    else f"Binary Cluster {cid}"
                )

                new_cm = dict(old_cm)
                new_cm["snippet"] = snippet
                new_cm["yara_distribution"] = yara_freq
                new_cm["avtype_distribution"] = avtype_freq
                new_cm["filetype_distribution"] = filetype_freq
                new_cm["ccip_distribution"] = ccip_freq

                is_custom = old_cm.get("is_custom_name", False)
                old_name = old_cm.get("cluster_name")
                name_changed = False

                if not is_custom and old_name != new_default_name:
                    new_cm["cluster_name"] = new_default_name
                    name_changed = True

                r.json().set(meta_key, "$", new_cm)

                if name_changed:
                    prop_pipe = r.pipeline()
                    for mid in members:
                        if old_name:
                            _unindex_tag(
                                prop_pipe,
                                collection,
                                "file",
                                "bin_cluster_name",
                                old_name,
                                mid,
                            )
                        _index_tag(
                            prop_pipe,
                            collection,
                            "file",
                            "bin_cluster_name",
                            new_default_name,
                            mid,
                        )
                        prop_pipe.json().set(
                            f"{mid}:meta",
                            "$.bin_cluster_name",
                            new_default_name,
                        )
                    prop_pipe.execute()

        logging.info(f"[+] Propagation complete for {total_files} files.")
        return True
