import json
from bsimvis.app.services.redis_client import get_redis

def get_enriched_nodes(collection, md5, addr):
    """
    Fetches and enriches callers and callees for a given function.
    """
    try:
        r = get_redis()
        base_func_key = f"{collection}:func:{md5}:{addr}"

        caller_ids_bytes = r.smembers(f"{base_func_key}:callers") or []
        callee_ids_bytes = r.smembers(f"{base_func_key}:callees") or []

        caller_ids = [cid.decode() if isinstance(cid, bytes) else cid for cid in caller_ids_bytes if cid]
        callee_ids = [cid.decode() if isinstance(cid, bytes) else cid for cid in callee_ids_bytes if cid]

        # Pipeline to fetch names for internal functions
        all_ids = list(set(caller_ids + callee_ids))
        all_ids = [fid for fid in all_ids if fid]
        pipe = r.pipeline()
        for fid in all_ids:
            if not fid.startswith("ext:"):
                pipe.json().get(f"{fid}:meta", "$")
            else:
                pipe.exists("dummy") # keep pipeline aligned
        
        meta_results = pipe.execute()
        meta_map = {}
        for fid, raw_meta in zip(all_ids, meta_results):
            if fid.startswith("ext:"):
                continue
            if raw_meta:
                meta = raw_meta[0] if isinstance(raw_meta, list) else raw_meta
                if isinstance(meta, str):
                    meta = json.loads(meta)
                if meta:
                    meta_map[fid] = {
                        "name": meta.get("function_name"),
                        "entrypoint": meta.get("entrypoint_address"),
                        "namespace": meta.get("namespace"),
                        "return_type": meta.get("return_type"),
                        "parameters": meta.get("parameters"),
                        "is_external": False
                    }

        def build_node_info(fid):
            if fid.startswith("ext:"):
                name = fid.split(":", 1)[1]
                return {
                    "id": fid,
                    "name": name,
                    "entrypoint": None,
                    "is_external": True
                }
            info = meta_map.get(fid)
            if info:
                return {
                    "id": fid,
                    "name": info["name"],
                    "entrypoint": info["entrypoint"],
                    "namespace": info.get("namespace"),
                    "return_type": info.get("return_type"),
                    "parameters": info.get("parameters"),
                    "is_external": False
                }
            # Fallback
            addr_part = fid.split(":")[-1]
            return {
                "id": fid,
                "name": f"func_{addr_part}",
                "entrypoint": addr_part,
                "is_external": False
            }

        return {
            "callers": [build_node_info(cid) for cid in caller_ids],
            "callees": [build_node_info(cid) for cid in callee_ids]
        }
    except Exception as e:
        print(f"Error enriching nodes: {e}")
        return {"callers": [], "callees": []}
