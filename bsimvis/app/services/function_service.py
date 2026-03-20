from bsimvis.app.services.redis_client import get_redis

def fetch_function_data(collection, md5, addr):
    """
    Fetches raw function data from Redis: source, BSim vectors, and metadata.
    Returns: (source_dict, features_list, meta_dict, tf_map)
    """
    try:
        r = get_redis()
        pipe = r.pipeline()
        pipe.json().get(f"{collection}:function:{md5}:{addr}:source")
        pipe.json().get(f"{collection}:function:{md5}:{addr}:vec:meta")
        pipe.json().get(f"{collection}:function:{md5}:{addr}:meta")

        tf_key = f"{collection}:function:{md5}:{addr}:vec:tf"
        pipe.zrange(tf_key, 0, -1, withscores=True)

        source, features, meta, tf_raw = pipe.execute()
        tf_map = {member: int(score) for member, score in tf_raw} if tf_raw else {}

        return source, features, meta, tf_map
    except Exception as e:
        return None, None, None, str(e)

def get_feature_map(features):
    """
    Maps global token indices to lists of BSim features that cover them.
    """
    f_map = {}
    for f in (features or []):
        t_idxs = f.get('addr-to-token-idx', [])
        if isinstance(t_idxs, (int, str)): 
            t_idxs = [t_idxs]
        for t in t_idxs:
            try:
                f_map.setdefault(int(t), []).append(f)
            except (ValueError, TypeError):
                continue
    return f_map
