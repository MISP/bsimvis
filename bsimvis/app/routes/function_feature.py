from flask import Blueprint, request, jsonify
from bsimvis.app.services.function_service import fetch_function_data
from bsimvis.app.routes.function_code import render_single_function

function_feature_bp = Blueprint('function_feature', __name__)

@function_feature_bp.route("/api/function/features", methods=["GET"])
def get_function_features():
    """
    Returns a list of all raw features for a function, enriched with the 
    lines of C code (tokens) they are associated with.
    """
    func_id = request.args.get('id')
    if not func_id:
        return jsonify({"detail": "Missing function id"}), 400
        
    try:
        parts = func_id.split(':')
        if len(parts) < 4:
            return jsonify({"detail": "Invalid ID format"}), 400
            
        collection = parts[0]
        md5 = parts[2]
        addr = parts[3]
        
        source, features, meta, tf_map = fetch_function_data(collection, md5, addr)
        
        if not source:
            return jsonify({"detail": "Function not found"}), 404
            
        # 1. Get the rendered rows (lines with tokens) to maintain consistent format
        rows, tips = render_single_function(source, features, tf_map)
        
        # 2. Build index mapping global_idx to the entire row object (for context)
        token_to_row = {}
        for row in rows:
            for token in row['tokens']:
                token_to_row[token['global_idx']] = row

        # 3. Process features and attach context lines
        rich_features = []
        for feat in (features or []):
            t_idxs = feat.get('addr_to_token_idx', [])
            # Handle both list and single value (Ghidra export varies)
            if not isinstance(t_idxs, list):
                t_idxs = [t_idxs] if t_idxs is not None else []
                
            # Find unique lines associated with this feature
            context_lines = []
            seen_line_idxs = set()
            for tidx in t_idxs:
                try:
                    row = token_to_row.get(int(tidx))
                    if row and row['line_idx'] not in seen_line_idxs:
                        # Return the full row object (which includes tokens for highlighting)
                        context_lines.append(row)
                        seen_line_idxs.add(row['line_idx'])
                except (ValueError, TypeError):
                    continue
            
            # Create a rich feature object
            enriched_feat = feat.copy()
            enriched_feat['context_lines'] = context_lines
            enriched_feat['tf'] = tf_map.get(feat['hash'], 0)
            
            rich_features.append(enriched_feat)

        if meta:
            if 'function_id' not in meta:
                meta['function_id'] = f"{collection}:function:{md5}:{addr}"
            if 'file_id' not in meta:
                meta['file_id'] = f"{collection}:file:{md5}"
            if 'batch_id' not in meta and meta.get('batch_uuid'):
                meta['batch_id'] = f"{collection}:batch:{meta['batch_uuid']}"

        return jsonify({
            "id": func_id,
            "meta": meta or {},
            "features": rich_features,
            "tips": tips
        })
        
    except Exception as e:
        import traceback
        logging_err = f"Feature API error: {str(e)}\n{traceback.format_exc()}"
        print(logging_err)
        return jsonify({"detail": str(e)}), 500
