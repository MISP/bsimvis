"""Score-axis indexes for function similarity pairs."""

import json

from bsimvis.app.services.bin_sim_tags import is_library_tag


def is_library_function(meta):
    if not meta:
        return False
    for field in ("tags", "user_tags"):
        tags = meta.get(field) or []
        if isinstance(tags, str):
            tags = [tags]
        if any(is_library_tag(tag) for tag in tags):
            return True
    return False


def score_axis_key(namespace, axis, algo=None):
    suffix = f":{algo}" if algo else ""
    return f"{namespace}:sim:score_axis:{axis}{suffix}"


def ready_key(namespace, algo=None):
    suffix = f":{algo}" if algo else ""
    return f"{namespace}:sim:score_axes_ready{suffix}"


def pair_axis(meta1, meta2):
    return (
        "library"
        if is_library_function(meta1) or is_library_function(meta2)
        else "code"
    )


def add_pair(pipe, namespace, sid, score, axis, algo=None):
    other = "library" if axis == "code" else "code"
    pipe.zrem(score_axis_key(namespace, other, algo), sid)
    pipe.zadd(score_axis_key(namespace, axis, algo), {sid: score})


def refresh_function(r, collection, function_id, pools=()):
    """Reindex existing collection and pool pairs after a function tag change."""
    clean_id = function_id.removeprefix(f"{collection}:func:")
    namespaces = [(collection, False)] + [
        (f"global:pool:{pool_id}", True) for pool_id in pools
    ]
    for namespace, is_pool in namespaces:
        involves_id = function_id if is_pool else clean_id
        sids = r.smembers(f"{namespace}:sim:involves:func:{involves_id}")
        sids = [sid.decode() if isinstance(sid, bytes) else sid for sid in sids]
        for start in range(0, len(sids), 200):
            batch = sids[start : start + 200]
            docs_raw = r.mget(batch)
            docs = []
            fids = set()
            for sid, raw in zip(batch, docs_raw):
                if not raw:
                    continue
                doc = json.loads(raw) if not isinstance(raw, dict) else raw
                docs.append((sid, doc))
                fids.update((doc.get("id1"), doc.get("id2")))
            fids.discard(None)
            fids = list(fids)
            meta_raw = r.mget([f"{fid}:meta" for fid in fids]) if fids else []
            metas = {
                fid: (json.loads(raw) if raw else {})
                for fid, raw in zip(fids, meta_raw)
            }
            pipe = r.pipeline(transaction=False)
            score_keys = []
            for sid, doc in docs:
                algo = doc.get("algo")
                score_key = (
                    f"{namespace}:sim:score"
                    if is_pool
                    else f"{namespace}:sim:score:{algo}"
                )
                score_keys.append((sid, doc, score_key))
                pipe.zscore(score_key, sid)
            scores = pipe.execute()
            pipe = r.pipeline(transaction=False)
            for (sid, doc, _), score in zip(score_keys, scores):
                algo = doc.get("algo")
                target = pair_axis(metas.get(doc.get("id1")), metas.get(doc.get("id2")))
                for axis in ("code", "library"):
                    key = score_axis_key(namespace, axis, None if is_pool else algo)
                    if axis == target and score is not None:
                        pipe.zadd(key, {sid: score})
                    else:
                        pipe.zrem(key, sid)
            pipe.execute()
