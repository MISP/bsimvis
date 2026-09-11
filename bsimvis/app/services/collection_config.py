"""Collection-sticky similarity parameters.

min_features / min_score are locked per collection on first sim build and read
back everywhere after — so the BSim-vs-hash split (and therefore the canonical
file-similarity score) is stable regardless of what later jobs pass. The API is
unchanged: a request value only *initializes* the collection; it is ignored once
the collection is locked.
"""

from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.config_service import config_service

_META = "global:collection:{coll}:meta"


def _coerce(value, default):
    """Return value as the same type as default (Redis hashes store strings)."""
    if isinstance(default, bool):
        return str(value).lower() in ("1", "true")
    if isinstance(default, int):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default
    if isinstance(default, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default
    return value


def get_collection_param(collection, name, default=None):
    v = get_redis().hget(_META.format(coll=collection), name)
    if v is None:
        return default
    return _coerce(v.decode() if isinstance(v, bytes) else v, default)


def resolve_and_lock(collection, name, requested):
    """Lock the param on first call (requested value, else config default), then
    always return the locked value. `similarity.<name>` supplies the default."""
    default = config_service.get(f"similarity.{name}", 0)
    value = requested if requested is not None else default
    get_redis().hsetnx(_META.format(coll=collection), name, value)
    return get_collection_param(collection, name, default)


# Only these are settable through the API — the meta hash also holds counters
# (total_files, total_functions, ...) a client must never be able to write.
LOCKED_PARAMS = ("min_features", "min_score")


def get_collection_params(collection):
    """`{name: {value, default, locked}}` for every sticky similarity param."""
    meta = get_redis().hgetall(_META.format(coll=collection)) or {}
    meta = {
        (k.decode() if isinstance(k, bytes) else k): (
            v.decode() if isinstance(v, bytes) else v
        )
        for k, v in meta.items()
    }
    out = {}
    for name in LOCKED_PARAMS:
        default = config_service.get(f"similarity.{name}", 0)
        raw = meta.get(name)
        out[name] = {
            "value": _coerce(raw, default) if raw is not None else default,
            "default": default,
            "locked": raw is not None,
        }
    return out


def set_collection_param(collection, name, value):
    """Overwrite a locked param (resolve_and_lock only ever initializes one).
    Already-built similarity edges are NOT recomputed — rebuild after changing."""
    if name not in LOCKED_PARAMS:
        raise ValueError(f"unknown collection param: {name}")
    default = config_service.get(f"similarity.{name}", 0)
    value = _coerce(value, default)
    get_redis().hset(_META.format(coll=collection), name, value)
    return value
