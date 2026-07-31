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
