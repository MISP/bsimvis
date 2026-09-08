from __future__ import annotations


def calculate_vicr(
    *,
    incremental_seconds: float,
    checkpoint_seconds: float,
    recovery_seconds: float,
    full_rebuild_seconds: float,
    exact_refresh_every: int,
    recovery_every: int,
) -> dict:
    if full_rebuild_seconds <= 0:
        raise ValueError("full_rebuild_seconds must be positive")
    if exact_refresh_every <= 0:
        raise ValueError("exact_refresh_every must be positive")
    if recovery_every <= 0:
        raise ValueError("recovery_every must be positive")
    costs = (incremental_seconds, checkpoint_seconds, recovery_seconds)
    if any(cost < 0 for cost in costs):
        raise ValueError("measured costs must not be negative")

    amortized_recovery = recovery_seconds / recovery_every
    amortized_exact_refresh = full_rebuild_seconds / exact_refresh_every
    numerator = (
        incremental_seconds
        + checkpoint_seconds
        + amortized_recovery
        + amortized_exact_refresh
    )
    ratio = numerator / full_rebuild_seconds
    return {
        "vicr": ratio,
        "reciprocal_speedup": 1.0 / ratio,
        "amortized_recovery_seconds": amortized_recovery,
        "amortized_exact_refresh_seconds": amortized_exact_refresh,
        "exact_refresh_every": exact_refresh_every,
        "recovery_every": recovery_every,
    }


def vicr_publication_gate(
    *,
    exact_products_match: bool,
    replacement_passed: bool,
    deletion_passed: bool,
    interruption_preserves_active_generation: bool,
    deferred_quantiles_labelled: bool,
) -> dict:
    checks = {
        "exact_products_match": exact_products_match,
        "replacement_passed": replacement_passed,
        "deletion_passed": deletion_passed,
        "interruption_preserves_active_generation": interruption_preserves_active_generation,
        "deferred_quantiles_labelled": deferred_quantiles_labelled,
    }
    return {
        "publishable": all(checks.values()),
        "checks": checks,
        "failed_checks": [name for name, passed in checks.items() if not passed],
    }
