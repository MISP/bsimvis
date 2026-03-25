from flask import Blueprint, jsonify, request
from bsimvis.app.services.redis_client import get_redis
from redis.commands.search.query import Query
import json
import logging
import re
from bsimvis.app.routes.function_code import render_single_function

search_bp = Blueprint('search', __name__)

DEFAULT_PAGING_LIMIT = 100

def escape_tag_value(value):
    """Escape all RediSearch special characters for Tag fields."""
    if not value:
        return ""
    # RediSearch special characters for Tag fields (Dialect 2)
    # , . / : ; ' " ! @ # $ % ^ & * ( ) - + = ~ [ ] { } | \ < > ? and space
    # Any non-alphanumeric character should ideally be escaped. 
    return re.sub(r"([,.\\/ \-@:;!#$%^&*()=+~[\]{}|<>?])", r"\\\1", str(value))

# TODO : This is temporary
def normalize_tags(data):
    """Ensure the 'tags' field is always a list, splitting strings if needed."""
    tags = data.get('tags')
    if isinstance(tags, str):
        if not tags:
            data['tags'] = []
        else:
            # Kvrocks returns TAG fields as comma-separated strings
            data['tags'] = [t.strip() for t in tags.split(',')]
    elif tags is None:
        data['tags'] = []
    return data

@search_bp.route("/api/collection/search")
def search_collections():
    r = get_redis()
    
    # 1. Get the list of names from the SET
    collection_names = r.smembers("global:collections")
    
    results = []
    # 2. Use a pipeline to fetch all HASH metadata in one trip
    pipe = r.pipeline()
    for name in collection_names:
        pipe.hgetall(f"global:collection:{name}:meta")
        
    metas = pipe.execute()


    # 3. Format for the frontend
    for name, meta in zip(collection_names, metas):
        results.append({
            "name": name,
            "total_files": int(meta.get("total_files", 0)),
            "total_functions": int(meta.get("total_functions", 0)),
            "last_updated": meta.get("last_updated", "N/A")
        })
    
    return jsonify(results)

@search_bp.route("/api/batch/search")
def search_batches():
    r = get_redis()
    
    target_collection = request.args.get('collection')
    
    if not target_collection:
        return jsonify({"error": "No collection specified"})
    # 1. Get Batch UUIDs from the SET
    # (If we implement the global:batches set as seen in your CLI)
    batch_uuids = r.smembers(f"global:batches")
    
    results = []
    pipe = r.pipeline()
    
    # 2. Fetch the JSON documents
    # Note: Since your keys are '{collection}:batch:{uuid}', we need the collection name.
    # For now, we'll assume the collection is known or we iterate through collections.
    # If target_collection is not provided, we might need a global batch summary hash 
    # to avoid "guessing" the collection prefix.
    
    
    for uuid in batch_uuids:
        # We try to get the JSON from the predicted key path
        # If you have multiple collections, we'd use the global:batch_summaries HASH instead
        # For this POC, let's look at the specific collection path:
        if target_collection:
            pipe.json().get(f"{target_collection}:batch:{uuid}", "$")
        else:
            # Fallback: find the first collection this batch belongs to 
            # (Requires the global:batch_summaries hash we discussed earlier)
            pipe.hget("global:batch_summaries", uuid)

    raw_data = pipe.execute()

    for item in raw_data:
        if item:
            # Kvrocks JSON.GET with $ returns a list [doc]
            data = item[0] if isinstance(item, list) and item else item
            # If it's a string from hget, parse it.
            data = json.loads(data) if isinstance(data, str) else data
            col = data.get('collection') or target_collection
            b_uuid = data.get('batch_uuid') or data.get('batch_uuid')
            if col and b_uuid and 'batch_id' not in data:
                data['batch_id'] = f"{col}:batch:{b_uuid}"
            results.append(data)
    
                
    # Sort by newest first
    results.sort(key=lambda x: x.get('last_updated', ''), reverse=True)
    return jsonify(results)



@search_bp.route("/api/file/search")
def search_files():
    r = get_redis()
    
    # 1. Get Pagination Parameters (Defaults: Start at 0, fetch 20)
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', DEFAULT_PAGING_LIMIT))
    except ValueError:
        return jsonify({"error": "Offset and limit must be integers"}), 400

    # 2. Get Filters
    collection = request.args.get('collection')
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    filters = {
        "batch_uuid": request.args.get('batch_uuid'),
        "language_id": request.args.get('language_id'),
        "file_md5": request.args.get('file_md5'),
        "file_name": request.args.get('file_name'),
    }

    tags = request.args.getlist('tag')
    

    # 3. Build Query (Standardized naming - no spaces and quoted TAGs for Kvrocks)
    query_parts = [
        f"@collection:{{{collection}}}",
        "@type:{file}"
    ]

    for tag in tags:
        query_parts.append(f'@tags:{{"{tag}"}}')
    
    # Optional filters
    if filters["batch_uuid"]:
        query_parts.append(f'@batch_uuid:{{"{filters["batch_uuid"]}"}}')
    
    if filters["language_id"]:
        # Handles complex Ghidra strings like 'AARCH64:LE:64:v8A'
        query_parts.append(f'@language_id:{{"{filters["language_id"]}"}}')
    
    if filters["file_md5"]:
        query_parts.append(f'@file_md5:{{"{filters["file_md5"]}"}}')
    
    if filters["file_name"]:
        # Text field with wildcards for partial match
        query_parts.append(f'@file_name:*{filters["file_name"]}*')

    
    search_str = " ".join(query_parts)

    if search_str == "":
        search_str = "*"

    logging.info(f"[*] Searching for {search_str} in collection '{collection}'")
    # 4. Execute with Paging and Sorting
    # We sort by entry_date DESC so the newest uploads appear first
    query = (
        Query(search_str)
        .dialect(2)
        .paging(offset, limit)
        #.sort_by("entry_date", asc=False)
    )

    try:
        results = r.ft("idx:files").search(query)
        
        files_list = []
        for doc in results.docs:
            if hasattr(doc, 'json'):
                data = json.loads(doc.json)
            else:
                # Fallback for systems (like Kvrocks) that return flat fields
                data = doc.__dict__

            col = data.get('collection', collection)
            md5 = data.get('file_md5') or data.get('file_md5')
            b_uuid = data.get('batch_uuid') or data.get('batch_uuid')
            
            if col and md5 and 'file_id' not in data:
                data['file_id'] = f"{col}:file:{md5}"
            if col and b_uuid and 'batch_id' not in data:
                data['batch_id'] = f"{col}:batch:{b_uuid}"
            
            normalize_tags(data)
            files_list.append(data)

        # 5. Return Data + Metadata
        return jsonify({
            "total": results.total,      # Total matches in DB
            "offset": offset,            # Current starting point
            "limit": limit,              # Batch size
            "files": files_list
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@search_bp.route("/api/function/search")
def search_functions():
    r = get_redis()
    
    # 1. Get Pagination Parameters (Defaults: Start at 0, fetch 20)
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', DEFAULT_PAGING_LIMIT))
    except ValueError:
        return jsonify({"error": "Offset and limit must be integers"}), 400

    # 2. Get Filters
    collection = request.args.get('collection')
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    filters = {
        "batch_uuid": request.args.get('batch_uuid'),
        "language_id": request.args.get('language_id'),
        "file_md5": request.args.get('file_md5'),
        "file_name": request.args.get('file_name'),
        "tag": request.args.get('tag'),
        "decompiler_id": request.args.get('decompiler_id'),
        "function_name": request.args.get('function_name'),
        "return_type": request.args.get('return_type'),
        "calling_convention": request.args.get('calling_convention'),
        "entrypoint_address": request.args.get('entrypoint_address'),
    }

    # 3. Build Query (Standardized naming - no spaces and quoted TAGs for Kvrocks)
    query_parts = [
        f"@collection:{{{collection}}}",
        "@type:{function}"
    ]

    tags = request.args.getlist('tag')
    for tag in tags:
        query_parts.append(f'@tags:{{"{tag}"}}')
    
    # Optional filters
    if filters["batch_uuid"]:
        query_parts.append(f'@batch_uuid:{{"{filters["batch_uuid"]}"}}')
    
    if filters["language_id"]:
        # Handles complex Ghidra strings like 'AARCH64:LE:64:v8A'
        query_parts.append(f'@language_id:{{"{filters["language_id"]}"}}')
    
    if filters["file_md5"]:
        query_parts.append(f'@file_md5:{{"{filters["file_md5"]}"}}')
    
    if filters["file_name"]:
        # Text field with wildcards for partial match
        query_parts.append(f'@file_name:*{filters["file_name"]}*')

    if filters["decompiler_id"]:
        query_parts.append(f'@decompiler_id:{{"{filters["decompiler_id"]}"}}')
    
    if filters["function_name"]:
        # TextField, use quotes for phrase
        query_parts.append(f'@function_name:"{filters["function_name"]}"')
    
    if filters["return_type"]:
        # TextField
        query_parts.append(f'@return_type:"{filters["return_type"]}"')
    
    if filters["calling_convention"]:
        # TextField
        query_parts.append(f'@calling_convention:"{filters["calling_convention"]}"')
    
    if filters["entrypoint_address"]:
        # TextField
        query_parts.append(f'@entrypoint_address:"{filters["entrypoint_address"]}"')

    search_str = " ".join(query_parts)

    if search_str == "":
        search_str = "*"

    logging.info(f"[*] Searching for '{search_str}' in collection '{collection}'")
    # 4. Execute with Paging and Sorting
    # We sort by entry_date DESC so the newest uploads appear first
    query = (
        Query(search_str)
        .dialect(2)
        .paging(offset, limit)
        #.sort_by("entry_date", asc=False)
    )

    try:
        results = r.ft("idx:functions").search(query)

        functions_list = []
        for doc in results.docs:
            if hasattr(doc, 'json'):
                data = json.loads(doc.json)
            else:
                # Fallback for systems (like Kvrocks) that return flat fields
                data = doc.__dict__

            col = data.get('collection', collection)
            md5 = data.get('file_md5') or data.get('file_md5')
            addr = data.get('entrypoint_address') or data.get('entrypoint_address')
            b_uuid = data.get('batch_uuid') or data.get('batch_uuid')

            if col and md5 and addr and 'function_id' not in data:
                data['function_id'] = f"{col}:function:{md5}:{addr}"
            if col and md5 and 'file_id' not in data:
                data['file_id'] = f"{col}:file:{md5}"
            if col and b_uuid and 'batch_id' not in data:
                data['batch_id'] = f"{col}:batch:{b_uuid}"
            
            normalize_tags(data)
            functions_list.append(data)

        # 5. Return Data + Metadata
        return jsonify({
            "total": results.total,      # Total matches in DB
            "offset": offset,            # Current starting point
            "limit": limit,              # Batch size
            "functions": functions_list
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500



def _scan_feature_keys(r, collection, feature_prefix, offset, limit, sort_by):
    """Scans or Zranges features based on sort criteria."""
    if sort_by == 'tf':
        zset_key = f"{collection}:features:by_tf"
        if feature_prefix:
            cursor = 0
            all_matches = []
            while True:
                cursor, matches = r.zscan(zset_key, cursor=cursor, match=f"{feature_prefix}*", count=1000)
                all_matches.extend(matches)
                if cursor == 0 or len(all_matches) > 5000: break
            
            all_matches.sort(key=lambda x: x[1], reverse=True)
            page = all_matches[offset : offset + limit]
            return [{"hash": h, "tf_score": s, "frequency": r.scard(f"{collection}:feature:{h}:functions")} for h, s in page], len(all_matches)
        else:
            total = r.zcard(zset_key)
            page = r.zrevrange(zset_key, offset, offset + limit - 1, withscores=True)
            return [{"hash": h, "tf_score": s, "frequency": r.scard(f"{collection}:feature:{h}:functions")} for h, s in page], total
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
                    parts = key.split(':')
                    if len(parts) >= 3:
                        fh = parts[2]
                        feature_list.append({"hash": fh, "frequency": r.scard(key)})
                current_idx += 1
                total_found += 1
            if cursor == 0 or (len(feature_list) >= limit and total_found > offset + limit + 1000):
                break

        if feature_list:
            zset_key = f"{collection}:features:by_tf"
            pipe = r.pipeline()
            for f in feature_list:
                pipe.zscore(zset_key, f['hash'])
            scores = pipe.execute()
            for i, f in enumerate(feature_list):
                f['tf_score'] = scores[i] if scores[i] is not None else 0

        return feature_list, total_found

def _enrich_feature_context(r, collection, feature_list):
    """Enriches feature list with Pcode and C-code context in two stages."""
    if not feature_list: return feature_list

    try:
        # 1. Fetch first meta entry for each feature
        pipe = r.pipeline()
        for f in feature_list:
            pipe.json().get(f"{collection}:feature:{f['hash']}:meta", '$[0]')
        first_metas = pipe.execute()
        
        # 2. Prepare for stage 2 (Source and Fallback Meta)
        pipe = r.pipeline()
        for i, meta_pkg in enumerate(first_metas):
            fm = meta_pkg[0] if meta_pkg and len(meta_pkg) > 0 else None
            f = feature_list[i]
            if fm:
                parts = fm.get("function_id", "").split(':')
                f['context'] = {
                    "type": fm.get("type", "N/A"),
                    "op": fm.get("pcode_op", "N/A"),
                    "pcode_full": fm.get("pcode_op_full"),
                    "func_id": fm.get("function_id"),
                    "line_idxs": fm.get("line_idx", []),
                    "md5": parts[2] if len(parts) >= 3 else "N/A",
                    "addr": parts[3] if len(parts) >= 4 else "N/A",
                    "name": fm.get("function_name", parts[3] if len(parts) >= 4 else "N/A"),
                    "c_code": None
                }
                func_id = fm.get("function_id")
                if func_id and f['context']['line_idxs']:
                    pipe.json().get(f"{func_id}:source")
                    f['_line_idx'] = f['context']['line_idxs'][0]
                else:
                    pipe.execute_command('ECHO', 'no_source')
                
                if not f['context']['pcode_full'] and func_id:
                    pipe.json().get(f"{func_id}:vec:meta")
                else:
                    pipe.execute_command('ECHO', 'no_meta_fallback')
            else:
                f['context'] = {"c_code": None, "pcode_full": "N/A", "type": "N/A", "op": "N/A"}
                pipe.execute_command('ECHO', 'no_fm_source')
                pipe.execute_command('ECHO', 'no_fm_meta')

        # 3. Execute Stage 2 and Merge
        second_results = pipe.execute()
        for i, f in enumerate(feature_list):
            if 'context' not in f: continue
            source_data = second_results[i*2]
            vec_meta = second_results[i*2 + 1]
            
            if source_data and isinstance(source_data, dict) and '_line_idx' in f:
                target_line = int(f['_line_idx'])
                try:
                    c_tokens = source_data.get('c_tokens', [])
                    line_tokens = [t for t in c_tokens if int(t.get('line', -1)) == target_line]
                    if line_tokens:
                        f['context']["c_code"] = [
                            {"type": t.get("type"), "text": str(t.get("t", t.get("text", "")))} for t in line_tokens
                        ]
                except Exception as e:
                    logging.error(f"Error extracting tokens for feature {f['hash']}: {e}")
            
            if vec_meta and isinstance(vec_meta, list) and not f['context']['pcode_full']:
                for feat in vec_meta:
                    if feat.get('hash') == f['hash']:
                        f['context']['pcode_full'] = feat.get('pcode_op_full', 'N/A')
                        break
            
            if not f['context'].get('pcode_full'): f['context']['pcode_full'] = "N/A"
            if '_line_idx' in f: del f['_line_idx']

    except Exception as e:
        logging.error(f"Error in batch fetch context enrichment: {e}")
        for f in feature_list:
            if 'context' not in f:
                f['context'] = {"c_code": None, "pcode_full": "N/A", "type": "N/A", "op": "N/A"}
    
    return feature_list

@search_bp.route("/api/feature/search")
def search_features():
    r = get_redis()
    collection = request.args.get('collection')
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 20))
    except ValueError:
        return jsonify({"error": "Offset and limit must be integers"}), 400

    feature_prefix = request.args.get('hash', '')
    sort_by = request.args.get('sort', 'default')
    
    # 1. Fetch feature list
    feature_list, total_found = _scan_feature_keys(r, collection, feature_prefix, offset, limit, sort_by)
    
    # 2. Enrich with context
    feature_list = _enrich_feature_context(r, collection, feature_list)
    for f in feature_list:
        if 'feature_id' not in f:
            f['feature_id'] = f"{collection}:feature:{f['hash']}"

    return jsonify({
        "total_estimated": total_found,
        "offset": offset,
        "limit": limit,
        "features": feature_list
    })

@search_bp.route("/api/feature/details/<f_hash>")
def get_feature_details(f_hash):
    r = get_redis()
    collection = request.args.get('collection')
    if not collection:
        return jsonify({"error": "No collection specified"}), 400

    # Pagination
    try:
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', 1000)) # High default for backward compatibility
    except ValueError:
        return jsonify({"error": "Offset and limit must be integers"}), 400

    # 1. Get all functions containing this feature
    func_ids = r.smembers(f"{collection}:feature:{f_hash}:functions")
    
    # 2. Get the specific occurrences metadata (the JSON array we built in the rebuilder)
    meta_data = r.json().get(f"{collection}:feature:{f_hash}:meta", "$") or []
    
    if isinstance(meta_data, list) and meta_data and len(meta_data) == 1: meta_data = meta_data[0]

    total_occurrences = len(meta_data)
    paginated_meta = meta_data[offset : offset + limit]

    # 3. Augment with Pcode Context and TF if missing (avoiding full re-index)
    pipe = r.pipeline()
    augment_indices = []
    for i, occ in enumerate(paginated_meta):
        if 'pcode_op_full' not in occ or 'tf' not in occ or 'pcode_block' not in occ:
            func_id = occ.get('function_id')
            if func_id:
                pipe.json().get(f"{func_id}:vec:meta")
                pipe.zscore(f"{func_id}:vec:tf", f_hash)
                augment_indices.append(i)
    
    if augment_indices:
        try:
            extra_results = pipe.execute()
            for i, idx in enumerate(augment_indices):
                occ = paginated_meta[idx]
                vec_meta = extra_results[i*2]
                tf_score = extra_results[i*2 + 1]
                
                if vec_meta:
                    # Find this specific feature in the function's vector meta
                    for feat in vec_meta:
                        if feat.get('hash') == f_hash:
                            occ['pcode_op_full'] = feat.get('pcode_op_full', 'N/A')
                            occ['pcode_block'] = feat.get('pcode_block', {})
                            break
                
                occ['tf'] = int(tf_score) if tf_score is not None else 0
        except Exception:
            pass # Fallback to existing data if augmentation fails

    for occ in paginated_meta:
        col = occ.get('collection', collection)
        md5 = occ.get('file_md5') or occ.get('file_md5')
        addr = occ.get('entrypoint_address') or occ.get('entrypoint_address')
        b_uuid = occ.get('batch_uuid') or occ.get('batch_uuid')
        
        if 'function_id' not in occ and col and md5 and addr:
            occ['function_id'] = f"{col}:function:{md5}:{addr}"
        if 'file_id' not in occ and col and md5:
            occ['file_id'] = f"{col}:file:{md5}"
        if 'batch_id' not in occ and col and b_uuid:
            occ['batch_id'] = f"{col}:batch:{b_uuid}"

    return jsonify({
        "hash": f_hash,
        "occurrence_count": len(func_ids),
        "total_occurrences": total_occurrences,
        "offset": offset,
        "limit": limit,
        "associated_functions": list(func_ids),
        "occurrences": paginated_meta
    })

@search_bp.route("/api/similarity/search", methods=["GET"])
def similarity_search_api():
    col = request.args.get('collection')
    algo = request.args.get('algo', 'unweighted_cosine')
    
    try:
        threshold = float(request.args.get('threshold', 0.1))
        max_score = float(request.args.get('max_score', 1.0))
        offset = int(request.args.get('offset', 0))
        limit = int(request.args.get('limit', DEFAULT_PAGING_LIMIT))
        min_features = int(request.args.get('min_features', 0))
    except ValueError:
        return jsonify({"detail": "Invalid threshold, max_score, offset, limit, or min_features parameters"}), 400

    md5_filters = request.args.getlist('md5')
    is_cross_binary = request.args.get('cross_binary', 'false').lower() == 'true'

    if not col:
        return jsonify({"detail": "Missing collection"}), 400

    try:
        r = get_redis()
        
        # Build RediSearch Query (Dialect 2)
        query_parts = [
            f"@collection:{{{col}}}",
            f"@algo:{{{algo}}}",
            f"@score:[{threshold} {max_score}]"
        ]
        
        if min_features > 0:
            query_parts.append(f"@feat_count1:[{min_features} +inf]")
            query_parts.append(f"@feat_count2:[{min_features} +inf]")
            
        if is_cross_binary:
            query_parts.append("@is_cross_binary:{true}")
            
        if md5_filters:
            md5_str = " | ".join([f'"{m}"' for m in md5_filters])
            query_parts.append(f"(@md5_1:{{{md5_str}}} | @md5_2:{{{md5_str}}})")
            
        search_str = " ".join(query_parts)
        logging.info(f"[*] Similarity Search Query: {search_str}")

        query = (
            Query(search_str)
            .dialect(2)
            .paging(offset, limit)
            .sort_by("score", asc=False)
        )

        try:
            results = r.ft("idx:similarities").search(query)
            
            enriched_pairs = []
            for doc in results.docs:
                if hasattr(doc, 'json'):
                    data = json.loads(doc.json)
                else:
                    # Fallback for systems that return flat fields
                    data = doc.__dict__
                
                # Cleanup internal fields
                if 'id' in data: del data['id']
                if 'payload' in data: del data['payload']
                
                # Format to match expectations of the Similarity frontend component
                enriched_pairs.append({
                    "id1": data.get("id1"),
                    "id2": data.get("id2"),
                    "name1": data.get("name1", data.get("id1", ":").split(':')[-1]),
                    "name2": data.get("name2", data.get("id2", ":").split(':')[-1]),
                    "score": float(data.get("score", 0)),
                    "meta1": {
                        "file_md5": data.get("md5_1"),
                        "tags": data.get("tags1", []),
                        "batch_uuid": data.get("batch_uuid1"),
                        "language_id": data.get("language_id1"),
                        "bsim_features_count": data.get("feat_count1")
                    },
                    "meta2": {
                        "file_md5": data.get("md5_2"),
                        "tags": data.get("tags2", []),
                        "batch_uuid": data.get("batch_uuid2"),
                        "language_id": data.get("language_id2"),
                        "bsim_features_count": data.get("feat_count2")
                    }
                })

            return jsonify({
                "collection": col,
                "algo": algo,
                "threshold": threshold,
                "total": results.total,
                "offset": offset,
                "limit": limit,
                "pairs": enriched_pairs
            })
        except Exception as se:
            logging.error(f"RediSearch Error: {se}")
            return jsonify({"detail": f"Search execution failed: {str(se)}"}), 500

    except Exception as e:
        return jsonify({"detail": f"Error searching similarities: {str(e)}"}), 500