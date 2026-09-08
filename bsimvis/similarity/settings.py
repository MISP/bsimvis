import os

DEFAULT_EXACT_BLOCK_MAX_FUNCTIONS = 5000


def configured_exact_block_max_functions():
    """Return the largest collection routed through the exact block builder."""
    return max(
        0,
        int(
            os.getenv(
                "BSIMVIS_EXACT_BLOCK_MAX_FUNCTIONS",
                str(DEFAULT_EXACT_BLOCK_MAX_FUNCTIONS),
            )
        ),
    )


def safe_similarity_generations_enabled():
    """Use replacement generations unless explicitly disabled."""
    configured = os.getenv("BSIMVIS_SAFE_SIMILARITY_GENERATIONS")
    if configured is not None:
        return configured.strip().lower() in {"1", "true", "yes"}
    return configured_exact_block_max_functions() > 0
