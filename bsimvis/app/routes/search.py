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
            "total-files": int(meta.get("total-files", 0)),
            "total-functions": int(meta.get("total-functions", 0)),
            "last-updated": meta.get("last-updated", "N/A")
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
            pipe.json().get(f"{target_collection}:batch:{uuid}")
        else:
            # Fallback: find the first collection this batch belongs to 
            # (Requires the global:batch_summaries hash we discussed earlier)
            pipe.hget("global:batch_summaries", uuid)

    raw_data = pipe.execute()

    for item in raw_data:
        if item:
            # If it's a string from hget, parse it. If it's a dict from json().get, use as is.
            data = json.loads(item) if isinstance(item, str) else item
            results.append(data)
    
                
    # Sort by newest first
    results.sort(key=lambda x: x.get('last-updated', ''), reverse=True)
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
    

    # 3. Build Query (Standardized naming)
    query_parts = [f"@collection:{{ {escape_tag_value(collection)} }}"]

    for tag in tags:
        query_parts.append(f'@tag:{{ {escape_tag_value(tag)} }}')
    
    # Optional filters
    if filters["batch_uuid"]:
        query_parts.append(f'@batch_uuid:{{ {escape_tag_value(filters["batch_uuid"])} }}')
    
    if filters["language_id"]:
        # Handles complex Ghidra strings like 'AARCH64:LE:64:v8A'
        query_parts.append(f'@language_id:{{ {escape_tag_value(filters["language_id"])} }}')
    
    if filters["file_md5"]:
        query_parts.append(f'@file_md5:{{ {escape_tag_value(filters["file_md5"])} }}')
    
    if filters["file_name"]:
        # Text field with wildcards for partial match
        query_parts.append(f'@file_name:*{filters["file_name"]}*')

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
        .sort_by("entry_date", asc=False)
    )

    try:
        results = r.ft("idx:files").search(query)
        
        # 5. Return Data + Metadata
        return jsonify({
            "total": results.total,      # Total matches in DB
            "offset": offset,            # Current starting point
            "limit": limit,              # Batch size
            "files": [json.loads(doc.json) for doc in results.docs]
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
        "is_thunk": request.args.get('is_thunk'),
        "return_type": request.args.get('return_type'),
        "calling_convention": request.args.get('calling_convention'),
        "entrypoint_address": request.args.get('entrypoint_address'),
    }

    # 3. Build Query (Standardized naming)
    query_parts = [f"@collection:{{ {escape_tag_value(collection)} }}"]

    tags = request.args.getlist('tag')
    for tag in tags:
        query_parts.append(f'@tag:{{ {escape_tag_value(tag)} }}')
    
    # Optional filters
    if filters["batch_uuid"]:
        query_parts.append(f'@batch_uuid:{{ {escape_tag_value(filters["batch_uuid"])} }}')
    
    if filters["language_id"]:
        # Handles complex Ghidra strings like 'AARCH64:LE:64:v8A'
        query_parts.append(f'@language_id:{{ {escape_tag_value(filters["language_id"])} }}')
    
    if filters["file_md5"]:
        query_parts.append(f'@file_md5:{{ {escape_tag_value(filters["file_md5"])} }}')
    
    if filters["file_name"]:
        # Text field with wildcards for partial match
        query_parts.append(f'@file_name:*{filters["file_name"]}*')

    if filters["decompiler_id"]:
        query_parts.append(f'@decompiler_id:{{ {escape_tag_value(filters["decompiler_id"])} }}')
    
    if filters["function_name"]:
        # TextField, use quotes for phrase
        query_parts.append(f'@function_name:"{filters["function_name"]}"')
    
    if filters["is_thunk"]:
        query_parts.append(f'@is_thunk:{{ {escape_tag_value(filters["is_thunk"])} }}')
    
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
        .sort_by("entry_date", asc=False)
    )

    try:
        results = r.ft("idx:functions").search(query)

        # 5. Return Data + Metadata
        return jsonify({
            "total": results.total,      # Total matches in DB
            "offset": offset,            # Current starting point
            "limit": limit,              # Batch size
            "functions": [json.loads(doc.json) for doc in results.docs]
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
                parts = fm.get("function-id", "").split(':')
                f['context'] = {
                    "type": fm.get("type", "N/A"),
                    "op": fm.get("pcode-op", "N/A"),
                    "pcode_full": fm.get("pcode-op-full"),
                    "func_id": fm.get("function-id"),
                    "line_idxs": fm.get("line-idx", []),
                    "md5": parts[2] if len(parts) >= 3 else "N/A",
                    "addr": parts[3] if len(parts) >= 4 else "N/A",
                    "name": fm.get("function-name", parts[3] if len(parts) >= 4 else "N/A"),
                    "c_code": None
                }
                func_id = fm.get("function-id")
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
                    c_tokens = source_data.get('c-tokens', [])
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
                        f['context']['pcode_full'] = feat.get('pcode-op-full', 'N/A')
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
    meta_data = r.json().get(f"{collection}:feature:{f_hash}:meta") or []
    
    total_occurrences = len(meta_data)
    paginated_meta = meta_data[offset : offset + limit]

    # 3. Augment with Pcode Context and TF if missing (avoiding full re-index)
    pipe = r.pipeline()
    augment_indices = []
    for i, occ in enumerate(paginated_meta):
        if 'pcode-op-full' not in occ or 'tf' not in occ:
            func_id = occ.get('function-id')
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
                            occ['pcode-op-full'] = feat.get('pcode-op-full', 'N/A')
                            break
                
                occ['tf'] = int(tf_score) if tf_score is not None else 0
        except Exception:
            pass # Fallback to existing data if augmentation fails

    return jsonify({
        "hash": f_hash,
        "occurrence_count": len(func_ids),
        "total_occurrences": total_occurrences,
        "offset": offset,
        "limit": limit,
        "associated_functions": list(func_ids),
        "occurrences": paginated_meta
    })