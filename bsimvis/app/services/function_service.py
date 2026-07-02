from bsimvis.app.services.redis_client import get_redis


def fetch_function_data(collection, md5, addr):
    """
    Fetches raw function data from Redis: source, BSim vectors, and metadata.
    Returns: (source_dict, features_list, meta_dict, tf_map)
    """
    try:
        sub_collection = collection
        if collection:
            if collection.startswith("global:pool:"):
                parts = collection.split(":")
                if len(parts) >= 5 and parts[3] == "col":
                    sub_collection = parts[4]
                elif len(parts) >= 3:
                    sub_collection = parts[2]
            elif collection.startswith("pool:"):
                parts = collection.split(":")
                if len(parts) >= 4 and parts[2] == "col":
                    sub_collection = parts[3]
                elif len(parts) >= 2:
                    sub_collection = parts[1]

        r = get_redis()
        pipe = r.pipeline(transaction=False)
        pipe.get(f"{sub_collection}:func:{md5}:{addr}:source")
        pipe.get(f"{sub_collection}:func:{md5}:{addr}:vec:meta")
        pipe.get(f"{sub_collection}:func:{md5}:{addr}:meta")

        tf_key = f"{sub_collection}:func:{md5}:{addr}:vec:tf"
        pipe.zrange(tf_key, 0, -1, withscores=True)

        source_raw, features_raw, meta_raw, tf_raw = pipe.execute()

        import json

        source = json.loads(source_raw) if source_raw else None
        if isinstance(source, list) and len(source) == 1:
            source = source[0]

        features = json.loads(features_raw) if features_raw else None
        if isinstance(features, list) and len(features) == 1:
            features = features[0]

        meta = json.loads(meta_raw) if meta_raw else None
        if isinstance(meta, list) and len(meta) == 1:
            meta = meta[0]

        tf_map = (
            {member: int(float(score)) for member, score in tf_raw} if tf_raw else {}
        )

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
    for f in features or []:
        t_idxs = f.get("addr_to_token_idx", [])
        if isinstance(t_idxs, (int, str)):
            t_idxs = [t_idxs]
        for t in t_idxs:
            try:
                f_map.setdefault(int(t), []).append(f)
            except (ValueError, TypeError):
                continue
    return f_map
