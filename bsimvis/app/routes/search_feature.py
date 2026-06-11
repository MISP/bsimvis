import json
import logging

from flask import request
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.index_service import parse_timestamp


def _scan_feature_keys(r, collection, feature_prefix, offset, limit, sort_by):
    """Scans or ZRanges features based on sort criteria."""
    if sort_by == "tf":
        zset_key = f"{collection}:features:by_tf"
        if feature_prefix:
            cursor = 0
            all_matches = []
            while True:
                cursor, matches = r.zscan(
                    zset_key, cursor=cursor, match=f"{feature_prefix}*", count=1000
                )
                all_matches.extend(matches)
                if cursor == 0 or len(all_matches) > 5000:
                    break
            all_matches.sort(key=lambda x: x[1], reverse=True)
            page = all_matches[offset : offset + limit]
            return [
                {
                    "hash": h,
                    "tf_score": s,
                    "frequency": r.zcard(f"{collection}:feature:{h}:functions"),
                }
                for h, s in page
            ], len(all_matches)
        else:
            total = r.zcard(zset_key)
            page = r.zrevrange(zset_key, offset, offset + limit - 1, withscores=True)
            return [
                {
                    "hash": h,
                    "tf_score": s,
                    "frequency": r.zcard(f"{collection}:feature:{h}:functions"),
                }
                for h, s in page
            ], total
    else:
        match_pattern = f"{collection}:feature:{feature_prefix}*:functions"
        feature_list = []
        cursor = 0
        total_found = 0
        current_idx = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=match_pattern, count=1000)
            for key in keys:
                if current_idx >= offset and len(feature_list) < limit:
                    # Key is collection}:feature:{hash}:functions
                    prefix = f"{collection}:feature:"
                    suffix = ":functions"
                    if key.startswith(prefix) and key.endswith(suffix):
                        fh = key[len(prefix) : -len(suffix)]
                        feature_list.append({"hash": fh, "frequency": r.zcard(key)})
                    else:
                        # Fallback for unexpected formats
                        parts = key.split(":")
                        if len(parts) >= 4:
                            fh = parts[3]
                            feature_list.append({"hash": fh, "frequency": r.zcard(key)})
                current_idx += 1
                total_found += 1
            if cursor == 0 or (
                len(feature_list) >= limit and total_found > offset + limit + 1000
            ):
                break

        if feature_list:
            zset_key = f"{collection}:features:by_tf"
            pipe = r.pipeline()
            for f in feature_list:
                pipe.zscore(zset_key, f["hash"])
            scores = pipe.execute()
            for i, f in enumerate(feature_list):
                f["tf_score"] = scores[i] if scores[i] is not None else 0

        return feature_list, total_found


def _enrich_feature_context(r, collection, feature_list):
    """Enriches feature list with Pcode and C-code context."""
    if not feature_list:
        return feature_list

    try:
        pipe = r.pipeline()
        for f in feature_list:
            pipe.execute_command("HVALS", f"{collection}:feature:{f['hash']}:meta")
        first_metas_raw = pipe.execute()

        first_metas = []
        for res in first_metas_raw:
            if res and isinstance(res, list) and len(res) > 0:
                first_metas.append([json.loads(res[0])])
            else:
                first_metas.append([])

        pipe = r.pipeline()
        for i, meta_pkg in enumerate(first_metas):
            fm = meta_pkg[0] if meta_pkg else None
            f = feature_list[i]
            if fm:
                fid = fm.get("function_id", "")
                parts = fid.split(":")
                md5, addr = "N/A", "N/A"
                if len(parts) >= 4:
                    if parts[0] == "idx":
                        md5 = parts[3]
                        addr = parts[4]
                    else:
                        md5 = parts[2]
                        addr = parts[3]

                f["context"] = {
                    "type": fm.get("type", "N/A"),
                    "op": fm.get("pcode_op", "N/A"),
                    "pcode_full": fm.get("pcode_op_full"),
                    "func_id": fid,
                    "seq": fm.get("seq"),
                    "line_idxs": fm.get("line_idx", []),
                    "md5": md5,
                    "addr": addr,
                    "name": fm.get("function_name", addr),
                    "c_code": None,
                }
                func_id = fm.get("function_id")
                if func_id and f["context"]["line_idxs"]:
                    pipe.json().get(f"{func_id}:source", "$")
                    f["_line_idx"] = f["context"]["line_idxs"][0]
                else:
                    pipe.execute_command("ECHO", "no_source")

                if not f["context"]["pcode_full"] and func_id:
                    pipe.json().get(f"{func_id}:vec:meta", "$")
                else:
                    pipe.execute_command("ECHO", "no_meta_fallback")
            else:
                f["context"] = {
                    "c_code": None,
                    "pcode_full": "N/A",
                    "type": "N/A",
                    "op": "N/A",
                }
                pipe.execute_command("ECHO", "no_fm_source")
                pipe.execute_command("ECHO", "no_fm_meta")

        second_results = pipe.execute()
        for i, f in enumerate(feature_list):
            if "context" not in f:
                continue
            source_data = second_results[i * 2]
            if isinstance(source_data, list) and source_data:
                source_data = source_data[0]

            vec_meta = second_results[i * 2 + 1]
            if isinstance(vec_meta, list) and vec_meta:
                vec_meta = vec_meta[0]

            if source_data and isinstance(source_data, dict) and "_line_idx" in f:
                target_line = int(f["_line_idx"])
                try:
                    c_tokens = source_data.get("c_tokens", [])
                    line_tokens = [
                        t for t in c_tokens if int(t.get("line", -1)) == target_line
                    ]
                    if line_tokens:
                        f["context"]["c_code"] = [
                            {
                                "type": t.get("type"),
                                "text": str(t.get("t", t.get("text", ""))),
                            }
                            for t in line_tokens
                        ]
                except Exception as e:
                    logging.error(
                        f"Error extracting tokens for feature {f['hash']}: {e}"
                    )

            if (
                vec_meta
                and isinstance(vec_meta, list)
                and not f["context"]["pcode_full"]
            ):
                for feat in vec_meta:
                    if feat.get("hash") == f["hash"]:
                        f["context"]["pcode_full"] = feat.get("pcode_op_full", "N/A")
                        break

            if not f["context"].get("pcode_full"):
                f["context"]["pcode_full"] = "N/A"
            f.pop("_line_idx", None)

    except Exception as e:
        logging.error(f"Error in feature context enrichment: {e}")
        for f in feature_list:
            if "context" not in f:
                f["context"] = {
                    "c_code": None,
                    "pcode_full": "N/A",
                    "type": "N/A",
                    "op": "N/A",
                }

    return feature_list


def query_features_advanced(r, collection, filters):
    # Start with all features as candidates
    all_features_key = f"{collection}:all_features"
    candidates = {
        d.decode() if isinstance(d, bytes) else str(d)
        for d in r.smembers(all_features_key)
    }

    def get_field_matches(field_name, search_val):
        registry_key = f"{collection}:reg:feature:{field_name}"
        val_lower = search_val.lower()
        matching_buckets = []
        try:
            for bucket in r.sscan_iter(
                registry_key, match=f"*{val_lower}*", count=1000
            ):
                bucket_str = (
                    bucket.decode() if isinstance(bucket, bytes) else str(bucket)
                )
                if val_lower in bucket_str.lower():
                    matching_buckets.append(bucket_str)
        except Exception as e:
            logging.warning(f"Registry SSCAN failed for {registry_key}: {e}")

        field_candidates = set()
        if matching_buckets:
            if len(matching_buckets) == 1:
                field_candidates = {
                    t.decode() if isinstance(t, bytes) else str(t)
                    for t in r.smembers(matching_buckets[0])
                }
            else:
                pipe = r.pipeline()
                for b in matching_buckets:
                    pipe.smembers(b)
                for res in pipe.execute():
                    if res:
                        field_candidates.update(
                            t.decode() if isinstance(t, bytes) else str(t) for t in res
                        )
        return field_candidates

    # 1. Global query q
    search_q = filters.get("q", "").lower().strip()
    if search_q:
        search_fields = ["hash", "type", "op"]
        for word in [w for w in search_q.split() if w.strip()]:
            word_matches = set()
            for field in search_fields:
                word_matches.update(get_field_matches(field, word))
            candidates.intersection_update(word_matches)

    # 2. Specific tag filters
    for field in ["hash", "type", "op"]:
        val = filters.get(field, "").strip()
        if val:
            candidates.intersection_update(get_field_matches(field, val))

    # 3. Numeric range filters
    # Frequency
    min_freq = filters.get("min_frequency")
    max_freq = filters.get("max_frequency")
    if (min_freq is not None) or (max_freq is not None):
        try:
            fmin = float(min_freq) if min_freq is not None else "-inf"
            fmax = float(max_freq) if max_freq is not None else "+inf"
            freq_matches = {
                t.decode() if isinstance(t, bytes) else str(t)
                for t in r.zrangebyscore(
                    f"{collection}:idx:feature:frequency", fmin, fmax
                )
            }
            candidates.intersection_update(freq_matches)
        except (ValueError, TypeError):
            pass

    # TF score
    min_tf = filters.get("min_tf_score")
    max_tf = filters.get("max_tf_score")
    if (min_tf is not None) or (max_tf is not None):
        try:
            tmin = float(min_tf) if min_tf is not None else "-inf"
            tmax = float(max_tf) if max_tf is not None else "+inf"
            tf_matches = {
                t.decode() if isinstance(t, bytes) else str(t)
                for t in r.zrangebyscore(
                    f"{collection}:idx:feature:tf_score", tmin, tmax
                )
            }
            candidates.intersection_update(tf_matches)
        except (ValueError, TypeError):
            pass

    total = len(candidates)
    candidate_list = list(candidates)

    # 4. Sorting
    sort_by = filters.get("sort_by", "tf_score")
    sort_order = filters.get("sort_order", "desc")
    reverse_sort = sort_order == "desc"

    if sort_by in ["frequency", "tf_score"]:
        zset_key = f"{collection}:idx:feature:{sort_by}"
        pipe = r.pipeline()
        for doc_id in candidate_list:
            pipe.zscore(zset_key, doc_id)
        scores = pipe.execute()
        scored_candidates = [
            (doc_id, float(score) if score is not None else 0.0)
            for doc_id, score in zip(candidate_list, scores)
        ]
        scored_candidates.sort(key=lambda x: x[1], reverse=reverse_sort)
        candidate_list = [x[0] for x in scored_candidates]
    elif sort_by in ["hash", "type", "op"]:
        if sort_by == "hash":
            candidate_list.sort(key=lambda x: x.split(":")[-1], reverse=reverse_sort)
        else:
            pipe = r.pipeline()
            for doc_id in candidate_list:
                f_hash = doc_id.split(":")[-1]
                pipe.json().get(
                    f"{collection}:feature:{f_hash}:global_meta", f"$.{sort_by}"
                )
            sort_vals = pipe.execute()

            def get_sort_val(v):
                if isinstance(v, list) and v:
                    v = v[0]
                return str(v or "").lower()

            sorted_candidates = [
                (doc_id, get_sort_val(val))
                for doc_id, val in zip(candidate_list, sort_vals)
            ]
            sorted_candidates.sort(key=lambda x: x[1], reverse=reverse_sort)
            candidate_list = [x[0] for x in sorted_candidates]
    else:
        # Default: alphabetical by hash
        candidate_list.sort(key=lambda x: x.split(":")[-1], reverse=reverse_sort)

    # 5. Pagination
    offset = filters.get("offset", 0)
    limit = filters.get("limit", 20)
    page_ids = candidate_list[offset : offset + limit]

    # 6. Enrichment
    page_hashes = [doc_id.split(":")[-1] for doc_id in page_ids]

    pipe = r.pipeline()
    for fh in page_hashes:
        pipe.json().get(f"{collection}:feature:{fh}:global_meta", "$")
    metas_raw = pipe.execute()

    features = []
    for fh, m_list in zip(page_hashes, metas_raw):
        meta = m_list[0] if (isinstance(m_list, list) and m_list) else None
        if not meta:
            # Fallback dynamic enrichment
            meta = {
                "hash": fh,
                "feature_id": f"{collection}:feature:{fh}",
                "type": "N/A",
                "op": "N/A",
                "frequency": r.zcard(f"{collection}:feature:{fh}:functions"),
                "tf_score": r.zscore(f"{collection}:features:by_tf", fh) or 0.0,
                "context": {
                    "type": "N/A",
                    "op": "N/A",
                    "pcode_full": "N/A",
                    "c_code": None,
                },
            }
        features.append(meta)

    return features, total


def search_features():
    try:
        r = get_redis()
        collection = request.args.get("collection")
        if not collection:
            return {"error": "No collection specified"}, 400

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", 20))
        except ValueError:
            return {"error": "offset and limit must be integers"}, 400

        format_arg = request.args.get("format")
        if format_arg in ("csv", "json"):
            offset = 0
            limit = 100000

        # Build filters dictionary
        # toggleSort writes sort_by/sort_order; also support legacy sort/order
        filters = {
            "q": request.args.get("q", ""),
            "hash": request.args.get("hash", ""),
            "type": request.args.get("type", ""),
            "op": request.args.get("op", ""),
            "sort_by": request.args.get("sort_by")
            or request.args.get("sort", "tf_score"),
            "sort_order": request.args.get("sort_order")
            or request.args.get("order", "desc"),
            "offset": offset,
            "limit": limit,
        }

        # Range filters
        for num_f in ["min_frequency", "max_frequency", "min_tf_score", "max_tf_score"]:
            val = request.args.get(num_f)
            if val is not None and val != "":
                filters[num_f] = val

        feature_list, total_found = query_features_advanced(r, collection, filters)

        response_data = {
            "total": total_found,
            "offset": offset,
            "limit": limit,
            "features": feature_list,
        }
        if format_arg == "csv":
            from bsimvis.app.services.export_service import export_to_csv

            return export_to_csv(feature_list, "features")
        elif format_arg == "json":
            from bsimvis.app.services.export_service import export_to_json

            return export_to_json(response_data, "features")
        else:
            return response_data
    except Exception as e:
        logging.error(f"Error in search_features: {e}", exc_info=True)
        return {"error": str(e)}, 500


def get_feature_details(f_hash):
    try:
        r = get_redis()
        collection = request.args.get("collection")
        if not collection:
            return {"error": "No collection specified"}, 400

        try:
            offset = int(request.args.get("offset", 0))
            limit = int(request.args.get("limit", 1000))
        except ValueError:
            return {"error": "offset and limit must be integers"}, 400

        func_ids = r.zrange(f"{collection}:feature:{f_hash}:functions", 0, -1)
        raw_meta_vals = r.hvals(f"{collection}:feature:{f_hash}:meta")
        meta_data = []
        if raw_meta_vals:
            for v in raw_meta_vals:
                m = json.loads(v)
                if "entry_date" in m:
                    m["entry_date"] = parse_timestamp(m["entry_date"])
                meta_data.append(m)

        total_occurrences = len(meta_data)
        paginated_meta = meta_data[offset : offset + limit]

        # Augment missing fields
        pipe = r.pipeline()
        augment_indices = []
        for i, occ in enumerate(paginated_meta):
            if (
                "pcode_op_full" not in occ
                or "tf" not in occ
                or "pcode_block" not in occ
                or "seq" not in occ
            ):
                func_id = occ.get("function_id")
                if func_id:
                    pipe.json().get(f"{func_id}:vec:meta", "$")
                    pipe.zscore(f"{func_id}:vec:tf", f_hash)
                    augment_indices.append(i)

        if augment_indices:
            try:
                extra_results = pipe.execute()
                for i, idx in enumerate(augment_indices):
                    occ = paginated_meta[idx]
                    vec_meta = extra_results[i * 2]
                    if isinstance(vec_meta, list) and vec_meta and len(vec_meta) == 1:
                        vec_meta = vec_meta[0]
                    tf_score = extra_results[i * 2 + 1]
                    if vec_meta:
                        for feat in vec_meta:
                            if feat.get("hash") == f_hash:
                                occ["pcode_op_full"] = feat.get("pcode_op_full", "N/A")
                                occ["pcode_block"] = feat.get("pcode_block", {})
                                occ["seq"] = feat.get("seq")
                                break
                    occ["tf"] = int(tf_score) if tf_score is not None else 0
            except Exception:
                pass

        for occ in paginated_meta:
            col = occ.get("collection", collection)
            md5 = occ.get("file_md5")
            addr = occ.get("entrypoint_address")
            b_uuid = occ.get("batch_uuid")
            if "function_id" not in occ and col and md5 and addr:
                occ["function_id"] = f"{col}:func:{md5}:{addr}"
            if "file_id" not in occ and col and md5:
                occ["file_id"] = f"{col}:file:{md5}"
            if "batch_id" not in occ and col and b_uuid:
                occ["batch_id"] = f"{col}:batch:{b_uuid}"

        return {
            "hash": f_hash,
            "occurrence_count": len(func_ids),
            "total_occurrences": total_occurrences,
            "offset": offset,
            "limit": limit,
            "associated_functions": list(func_ids),
            "occurrences": paginated_meta,
        }
    except Exception as e:
        logging.error(f"Error in get_feature_details: {e}", exc_info=True)
        return {"error": str(e)}, 500
