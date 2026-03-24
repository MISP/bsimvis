from flask import Blueprint, request, jsonify
from bsimvis.app.services.function_service import fetch_function_data, get_feature_map
import traceback
function_code_bp = Blueprint('function_code', __name__)

def render_single_function(source, features, tf_map):
    """
    Renders the semantic tokens for a single function without any diffing logic.
    """
    f_map = get_feature_map(features)
    tokens = source.get('c_tokens', [])
    if not tokens:
        return []
        
    max_line = max(t['line'] for t in tokens)
    lines_dict = {i: [] for i in range(max_line + 1)}
    for idx, t in enumerate(tokens):
        lines_dict[t['line']].append((idx, t))
        
    addr_map = source.get('line_to_addr', {})
    
    rows = []
    tips = {}
    
    for i in range(max_line + 1):
        line_tokens = lines_dict.get(i, [])
        tokens_json = []
        
        for global_idx, token in line_tokens:
            token_features = f_map.get(global_idx, [])
            hash_list = [f['hash'] for f in token_features]
            
            # For single view, we mark everything as "unique" if it has features 
            # Or just leave it neutral. Let's use diff-unique class for consistency.
            diff_class = "diff-unique" if token_features else ""
            
            tip_features = []
            if token_features:
                for f in token_features:
                    tip_features.append([
                        f['hash'], f.get('pcode_op'), f.get('pcode_op_full'), 
                        f.get('type'), f.get('seq'), f.get('addr'), f.get('line_idx'),
                        tf_map.get(f['hash'], "N/A"), "#66d9ef" # cyan for neutral base
                    ])
                tips[global_idx] = [token.get('type'), token.get('seq'), tip_features]
            
            tokens_json.append({
                "type": token.get("type"),
                "has_features": bool(token_features),
                "diff_class": diff_class,
                "hash_list": hash_list,
                "global_idx": global_idx,
                "text": token['t']
            })
            
        addr = addr_map.get(str(i), [""])[0] if addr_map else ""
        
        rows.append({
            "line_idx": i + 1,
            "address": addr,
            "tokens": tokens_json
        })
            
    return rows, tips

@function_code_bp.route("/api/function/code", methods=["GET"])
def get_function_code():
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
            
        rows, tips = render_single_function(source, features, tf_map)
        
        # Ensure MD5 and Decompiler ID are in meta if missing, but otherwise keep full meta
        if meta:
            meta['file_md5'] = md5
            if 'decompiler_id' not in meta:
                meta['decompiler_id'] = source.get('metadata', {}).get('decompiler_id', 'unknown')
            
            if 'function_id' not in meta:
                meta['function_id'] = f"{collection}:function:{md5}:{addr}"
            if 'file_id' not in meta:
                meta['file_id'] = f"{collection}:file:{md5}"
            if 'batch_id' not in meta and meta.get('batch_uuid'):
                meta['batch_id'] = f"{collection}:batch:{meta['batch_uuid']}"

        return jsonify({
            "rows": rows,
            "tips": tips,
            "meta": meta or {}
        })
    except Exception as e:
        # Capture the full stack trace as a string
        error_traceback = traceback.format_exc()
        
        # Log it to your console/file so you don't lose it
        print(error_traceback) 

        return jsonify({
            "detail": str(e),
            "type": e.__class__.__name__,
            "traceback": error_traceback  # Optional: only for development
        }), 500
