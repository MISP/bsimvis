from bsimvis.app.services.redis_client import get_redis

def fetch_function_data(collection, md5, addr):
    """
    Fetches raw function data from Redis: source, BSim vectors, and metadata.
    Returns: (source_dict, features_list, meta_dict, tf_map)
    """
    try:
        r = get_redis()
        pipe = r.pipeline()
        pipe.json().get(f"{collection}:function:{md5}:{addr}:source", "$")
        pipe.json().get(f"{collection}:function:{md5}:{addr}:vec:meta", "$")
        pipe.json().get(f"{collection}:function:{md5}:{addr}:meta", "$")

        tf_key = f"{collection}:function:{md5}:{addr}:vec:tf"
        pipe.zrange(tf_key, 0, -1, withscores=True)

        source, features, meta, tf_raw = pipe.execute()
        
        # Unwrap Dialect 2 / JSON.GET $ results (Kvrocks returns a list of results for path '$')
        if isinstance(source, list) and source and len(source) == 1: source = source[0]
        if isinstance(features, list) and features and len(features) == 1: features = features[0]
        if isinstance(meta, list) and meta and len(meta) == 1: meta = meta[0]

        tf_map = {member: int(float(score)) for member, score in tf_raw} if tf_raw else {}

        return source, features, meta, tf_map
    except Exception as e:
        import traceback
        print(f"Error fetching function data: {str(e)}")
        print(traceback.format_exc())
        return None, None, None, str(e)

def get_feature_map(features):
    """
    Maps global token indices to lists of BSim features that cover them.
    """
    f_map = {}
    for f in (features or []):
        t_idxs = f.get('addr_to_token_idx', [])
        if isinstance(t_idxs, (int, str)): 
            t_idxs = [t_idxs]
        for t in t_idxs:
            try:
                f_map.setdefault(int(t), []).append(f)
            except (ValueError, TypeError):
                continue
    return f_map
