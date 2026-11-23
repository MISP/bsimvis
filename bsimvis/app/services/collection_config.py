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
LOCKED_PARAMS = ("min_features", "min_score", "algo")


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
        default = config_service.get(
            f"similarity.{name}",
            "unweighted_cosine" if name == "algo" else 0,
        )
        raw = meta.get(name)
        if raw is None and name == "algo":
            value = resolve_collection_algo(collection)
        else:
            value = _coerce(raw, default) if raw is not None else default
        out[name] = {
            "value": value,
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
    if name == "algo":
        from bsimvis.app.services.similarity_service import assert_buildable_algo

        assert_buildable_algo(value)
        get_redis().hset(_META.format(coll=collection), name, value)
        return value
    value = _coerce(value, default)
    get_redis().hset(_META.format(coll=collection), name, value)
    return value


def _built_algo(collection, r=None):
    """The algo a collection's stored function edges were built with, or None.

    Collections built before `algo` was locked have no lock: without this, a
    change of `similarity.algo` would point them at an algo they never built.
    The configured default wins when it has edges too.
    """
    from bsimvis.similarity import registry

    r = r or get_redis()
    default = config_service.get("similarity.algo", "unweighted_cosine")
    names = [default] + [n for n in registry.names(buildable=True) if n != default]
    pipe = r.pipeline(transaction=False)
    for n in names:
        pipe.exists(f"{collection}:sim:score:{n}")
    return next((n for n, ok in zip(names, pipe.execute()) if ok), None)


def resolve_collection_algo(collection, requested=None):
    default = config_service.get("similarity.algo", "unweighted_cosine")
    configured = get_collection_param(collection, "algo", None)
    return configured or _built_algo(collection) or requested or default


def lock_collection_algo(collection, requested=None):
    """Lock the algo on a collection's first build, like min_score / min_features.

    The lock is what that build resolves to (an older collection's built algo,
    else the request, else the config default); later requests are ignored.
    Change it with set_collection_param, then rebuild.
    """
    algo = resolve_collection_algo(collection, requested)
    get_redis().hsetnx(_META.format(coll=collection), "algo", algo)
    return get_collection_param(collection, "algo", algo)


SIGNATURE_SETTINGS_FIELD = "signature_settings"


def lock_signature_settings(collection, settings):
    """Record the signature-settings mask on first ingest; return the locked value."""
    get_redis().hsetnx(
        _META.format(coll=collection), SIGNATURE_SETTINGS_FIELD, int(settings)
    )
    return get_collection_param(collection, SIGNATURE_SETTINGS_FIELD, int(settings))


def get_signature_settings(collection):
    """The mask this collection's features were extracted with, or None if unrecorded."""
    return get_collection_param(collection, SIGNATURE_SETTINGS_FIELD, None)


def assert_signature_settings_match(collection, settings=None):
    """Raise ValueError if `collection` was extracted under a different mask.

    `settings` defaults to the active configured mask. A collection with no
    recorded mask is treated as compatible (legacy data, nothing to check).
    """
    if settings is None:
        # imported here: bsim_profiles imports config_service, module-level would cycle
        from bsimvis.app.services import bsim_profiles

        settings = bsim_profiles.get_signature_settings()

    recorded = get_signature_settings(collection)
    if recorded is None:
        return
    recorded = int(recorded)
    if recorded != int(settings):
        raise ValueError(
            f"Collection '{collection}' was extracted with signature settings "
            f"{recorded:#x}, but {int(settings):#x} is active. Feature hashes from "
            f"different masks are not comparable; re-decompile one of them to compare."
        )


def demo():
    """Algo lock and the fallback for collections built before the lock (no Redis)."""

    class FakeRedis:
        def __init__(self, keys=()):
            self.keys, self.h, self.q = set(keys), {}, []

        def hget(self, k, f):
            return self.h.get(k, {}).get(f)

        def hsetnx(self, k, f, v):
            self.h.setdefault(k, {}).setdefault(f, v)

        def pipeline(self, transaction=False):
            return self

        def exists(self, k):
            self.q.append(k in self.keys)

        def execute(self):
            out, self.q = self.q, []
            return out

    global get_redis
    real, conf = get_redis, config_service.get
    config_service.get = lambda k, d=None: (
        "weighted_cosine" if k == "similarity.algo" else d
    )
    try:
        # built before the lock with unweighted_cosine: it keeps that algo
        fake = FakeRedis({"old:sim:score:unweighted_cosine"})
        get_redis = lambda: fake
        assert resolve_collection_algo("old") == "unweighted_cosine"
        assert lock_collection_algo("old", "jaccard") == "unweighted_cosine"
        # a new collection locks the default on its first build, then ignores requests
        assert resolve_collection_algo("new", "jaccard") == "jaccard"
        assert lock_collection_algo("new") == "weighted_cosine"
        assert lock_collection_algo("new", "jaccard") == "weighted_cosine"
        assert resolve_collection_algo("new", "jaccard") == "weighted_cosine"
    finally:
        get_redis, config_service.get = real, conf
    print("collection_config self-check: OK")


if __name__ == "__main__":
    demo()
