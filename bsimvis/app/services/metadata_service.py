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
from bsimvis.app.services.config_service import config_service


# `upload --metadata` matches CSV rows by md5, but unpacking only happens on the
# server: the md5 of an archive member or a UPX-unpacked payload does not exist
# until after the upload. So the whole map is staged once per batch and each blob
# looks itself up as it is ingested, instead of the client guessing which rows
# will be needed. Expires on its own -- a batch that never finishes must not leave
# the CSV sitting in the datastore forever.
STAGED_METADATA_TTL = 7 * 24 * 3600


def _staged_key(batch_uuid):
    return f"metadata_staged:{batch_uuid}"


def stage_metadata(batch_uuid, updates, r=None):
    """Store a batch's md5 -> metadata map for the ingest path to resolve."""
    r = r or get_redis()
    if not batch_uuid or not updates:
        return 0
    r.hset(
        _staged_key(batch_uuid),
        mapping={str(k): json.dumps(v) for k, v in updates.items()},
    )
    r.expire(_staged_key(batch_uuid), STAGED_METADATA_TTL)
    return len(updates)


def staged_metadata(batch_uuid, file_md5, r=None):
    """This blob's own staged CSV row, or None."""
    if not batch_uuid or not file_md5:
        return None
    r = r or get_redis()
    raw = r.hget(_staged_key(batch_uuid), str(file_md5))
    if not raw:
        return None
    try:
        return json.loads(raw.decode() if isinstance(raw, bytes) else raw)
    except ValueError:
        return None


class MetadataService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def propagate_metadata(self, collection, updates, job_service=None, job_id=None):
        r = self.r
        if not updates:
            logging.info("No updates passed to propagate_metadata.")
            return True

        total_files = len(updates)
        logging.info(
            f"[*] Starting metadata propagation for {total_files} files in collection '{collection}'..."
        )

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
                    job_id,
                    pct,
                    f"Propagating metadata for file {md5} ({idx_file+1}/{total_files})",
                )

            file_base_id = f"{collection}:file:{md5}"
            file_meta_key = f"{file_base_id}:meta"

            # 1. Fetch current file metadata
            old_meta_raw = r.get(file_meta_key)
            if not old_meta_raw:
                logging.warning(
                    f"[-] File metadata not found for {md5} in collection '{collection}'"
                )
                continue

            old_meta = json.loads(old_meta_raw) if old_meta_raw else None
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
            r.set(file_meta_key, json.dumps(new_meta))

            # 4. Update file-level secondary indexes
            pipe = r.pipeline(transaction=False)
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
                f
                for f in changed_fields
                if f in file_config and "func" in file_config[f]
            ]

            if func_ids and fields_to_propagate:
                logging.info(
                    f"[*] Propagating {fields_to_propagate} to {len(func_ids)} functions of {md5}"
                )
                chunk_size = 500
                for i in range(0, len(func_ids), chunk_size):
                    chunk = func_ids[i : i + chunk_size]
                    get_pipe = r.pipeline(transaction=False)
                    for func_id in chunk:
                        get_pipe.get(f"{func_id}:meta")
                    meta_results = get_pipe.execute()

                    write_pipe = r.pipeline(transaction=False)
                    any_changes = False

                    for func_id, fmeta_raw in zip(chunk, meta_results):
                        if not fmeta_raw:
                            continue
                        old_fmeta = json.loads(fmeta_raw) if fmeta_raw else None
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
                                    _unindex_num(
                                        write_pipe, collection, "func", f, func_id
                                    )
                                    _index_num(
                                        write_pipe,
                                        collection,
                                        "func",
                                        f,
                                        new_val,
                                        func_id,
                                    )
                                else:
                                    _unindex_tag(
                                        write_pipe,
                                        collection,
                                        "func",
                                        f,
                                        old_val,
                                        func_id,
                                    )
                                    _index_tag(
                                        write_pipe,
                                        collection,
                                        "func",
                                        f,
                                        new_val,
                                        func_id,
                                    )

                        if f_changed:
                            write_pipe.set(f"{func_id}:meta", json.dumps(new_fmeta))
                            any_changes = True

                    if any_changes:
                        write_pipe.execute()

            # 6 & 7. Propagation to function and binary similarity records bypassed for performance
            pass

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
            logging.info(
                f"[*] Recalculating statistics for {len(affected_clusters_to_recalculate)} affected binary clusters..."
            )
            for algo, cid in affected_clusters_to_recalculate:
                meta_key = f"{collection}:bin_cluster:{algo}:{cid}:meta"
                if not r.exists(meta_key):
                    continue

                old_cluster_meta_raw = r.get(meta_key)
                if not old_cluster_meta_raw:
                    continue
                old_cm = (
                    json.loads(old_cluster_meta_raw) if old_cluster_meta_raw else None
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

                c_meta_pipe = r.pipeline(transaction=False)
                for mid in members:
                    c_meta_pipe.get(f"{mid}:meta")
                c_meta_results = c_meta_pipe.execute()

                names = []
                yara_list = []
                avtype_list = []
                filetype_list = []
                ccip_list = []
                member_metas = {}

                for mid, res in zip(members, c_meta_results):
                    if not res:
                        continue
                    m = json.loads(res) if res else {}
                    member_metas[mid] = m
                    if isinstance(m, str):
                        m = json.loads(m)
                    if m.get("file_name"):
                        names.append(m["file_name"])
                    if m.get("yara"):
                        yara_list.extend(
                            m["yara"] if isinstance(m["yara"], list) else [m["yara"]]
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
                            m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
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

                min_cohesion = config_service.get("clustering.min_cohesion", 0.5)
                cohesion_score = old_cm.get("cohesion_score", 1.0)
                if cohesion_score < min_cohesion:
                    yara_freq = []
                    avtype_freq = []
                    filetype_freq = []
                    ccip_freq = []

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

                r.set(meta_key, json.dumps(new_cm))

                if name_changed:
                    prop_pipe = r.pipeline(transaction=False)
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
                        mid_meta = member_metas.get(mid, {})
                        mid_meta["bin_cluster_name"] = new_default_name
                        prop_pipe.set(f"{mid}:meta", json.dumps(mid_meta))
                    prop_pipe.execute()

        logging.info(f"[+] Propagation complete for {total_files} files.")
        return True
