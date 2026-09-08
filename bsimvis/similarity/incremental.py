from __future__ import annotations

import time
from typing import Sequence

from .backends import ExactSimilarityBackend, SparseVector
from .compact import CompactSimilarityStore


def plan_deletion_strategy(
    existing_summaries: Sequence[dict],
    deletion_indices: Sequence[int],
    *,
    max_delta_density: float = 0.10,
    max_affected_target_ratio: float = 0.20,
) -> dict:
    """Choose an exact overlay or full rebuild before creating a generation."""
    function_count = len(existing_summaries)
    deletions = sorted(set(int(index) for index in deletion_indices))
    if (
        not deletions
        or len(deletions) >= function_count
        or any(index < 0 or index >= function_count for index in deletions)
    ):
        raise ValueError("deletion_indices must leave at least one valid vector")
    deletion_set = set(deletions)
    edges = _indexed_edges_from_summaries(existing_summaries, function_count)
    affected_targets = 0
    for target, summary in enumerate(existing_summaries):
        if target in deletion_set:
            continue
        candidates = {
            int(candidate)
            for field in ("nearest", "distant")
            for candidate, _score in summary[field]
        }
        affected_targets += int(bool(candidates.intersection(deletion_set)))
    deleted_edges = sum(
        int(left in deletion_set or right in deletion_set) for left, right in edges
    )
    bounded_width = max(
        1,
        max(
            (
                len(summary.get("nearest", ())) + len(summary.get("distant", ()))
                for summary in existing_summaries
            ),
            default=1,
        ),
    )
    estimated_delta_items = min(
        2 * max(1, len(edges)),
        deleted_edges + affected_targets * bounded_width,
    )
    survivors = function_count - len(deletions)
    estimated_delta_density = estimated_delta_items / max(1, len(edges))
    affected_target_ratio = affected_targets / max(1, survivors)
    strategy = (
        "incremental_overlay"
        if estimated_delta_density <= max(0.0, float(max_delta_density))
        and affected_target_ratio <= max(0.0, float(max_affected_target_ratio))
        else "full_rebuild"
    )
    return {
        "strategy": strategy,
        "deletion_count": len(deletions),
        "active_edge_count": len(edges),
        "affected_target_count": affected_targets,
        "affected_target_ratio": affected_target_ratio,
        "deleted_edge_count": deleted_edges,
        "estimated_delta_items": estimated_delta_items,
        "estimated_delta_density": estimated_delta_density,
        "max_delta_density": float(max_delta_density),
        "max_affected_target_ratio": float(max_affected_target_ratio),
    }


def execute_routed_deletion(
    existing_summaries: Sequence[dict],
    deletion_indices: Sequence[int],
    *,
    incremental_writer,
    full_rebuild_writer,
    max_delta_density: float = 0.10,
    max_affected_target_ratio: float = 0.20,
) -> dict:
    """Execute only the exact writer selected by the preflight cost estimate."""
    plan = plan_deletion_strategy(
        existing_summaries,
        deletion_indices,
        max_delta_density=max_delta_density,
        max_affected_target_ratio=max_affected_target_ratio,
    )
    writer = (
        incremental_writer
        if plan["strategy"] == "incremental_overlay"
        else full_rebuild_writer
    )
    result = writer()
    return {**result, "deletion_route": plan}


def merge_ranked(existing, added, limit, *, descending):
    by_candidate = {int(candidate): float(score) for candidate, score in existing}
    by_candidate.update({int(candidate): float(score) for candidate, score in added})
    direction = -1 if descending else 1
    return sorted(
        by_candidate.items(), key=lambda item: (direction * item[1], item[0])
    )[: max(0, limit)]


def combine_moments(left: dict, right: dict) -> tuple[int, float, float]:
    left_count = int(left["count"])
    right_count = int(right["count"])
    count = left_count + right_count
    if count == 0:
        return 0, 0.0, 0.0
    delta = float(right["mean"]) - float(left["mean"])
    mean = (
        left_count * float(left["mean"]) + right_count * float(right["mean"])
    ) / count
    sum_squared = (
        left_count * float(left["variance"])
        + right_count * float(right["variance"])
        + delta * delta * left_count * right_count / count
    )
    return count, mean, sum_squared / count


def replace_moment_observations(summary, removed, added):
    count = int(summary["count"])
    if len(removed) != len(added):
        raise ValueError("replacement observations must preserve the count")
    total = float(summary["mean"]) * count
    total_squared = (float(summary["variance"]) + float(summary["mean"]) ** 2) * count
    total += sum(added) - sum(removed)
    total_squared += sum(value * value for value in added) - sum(
        value * value for value in removed
    )
    mean = total / count if count else 0.0
    variance = max(0.0, total_squared / count - mean * mean) if count else 0.0
    return count, mean, variance


def remove_moment_observations(summary, removed):
    original_count = int(summary["count"])
    count = original_count - len(removed)
    if count < 0:
        raise ValueError("cannot remove more observations than the summary contains")
    total = float(summary["mean"]) * original_count - sum(removed)
    total_squared = (
        float(summary["variance"]) + float(summary["mean"]) ** 2
    ) * original_count - sum(value * value for value in removed)
    mean = total / count if count else 0.0
    variance = max(0.0, total_squared / count - mean * mean) if count else 0.0
    return count, mean, variance


def update_analytics_for_append(
    backend: ExactSimilarityBackend,
    vectors: Sequence[SparseVector],
    base_count: int,
    existing_summaries: Sequence[dict],
    algorithm: str,
    *,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
) -> list[dict]:
    if not 0 <= base_count <= len(vectors):
        raise ValueError("base_count is out of range")
    if len(existing_summaries) != base_count:
        raise ValueError("existing_summaries must contain one record per base vector")
    if base_count == len(vectors):
        return [dict(summary) for summary in existing_summaries]

    old_indices = list(range(base_count))
    new_indices = list(range(base_count, len(vectors)))
    old_deltas = backend.summarize_target_block(
        vectors,
        old_indices,
        algorithm,
        candidate_indices=new_indices,
        workers=workers,
        nearest_k=nearest_k,
        distant_k=distant_k,
    )
    updated = []
    for existing, delta in zip(existing_summaries, old_deltas):
        count, mean, variance = combine_moments(existing, delta)
        updated.append(
            {
                "nearest": merge_ranked(
                    existing["nearest"],
                    delta["nearest"],
                    nearest_k,
                    descending=True,
                ),
                "distant": merge_ranked(
                    existing["distant"],
                    delta["distant"],
                    distant_k,
                    descending=False,
                ),
                "count": count,
                "mean": mean,
                "variance": variance,
                "minimum": min(float(existing["minimum"]), float(delta["minimum"])),
                "maximum": max(float(existing["maximum"]), float(delta["maximum"])),
                "quantiles": None,
                "quantiles_status": "pending_exact_refresh",
            }
        )

    new_summaries = backend.summarize_target_block(
        vectors,
        new_indices,
        algorithm,
        candidate_indices=range(len(vectors)),
        workers=workers,
        nearest_k=nearest_k,
        distant_k=distant_k,
    )
    for summary in new_summaries:
        summary["quantiles_status"] = "exact"
    return updated + new_summaries


def update_analytics_for_replacements(
    backend: ExactSimilarityBackend,
    old_vectors: Sequence[SparseVector],
    new_vectors: Sequence[SparseVector],
    replacement_indices: Sequence[int],
    existing_summaries: Sequence[dict],
    algorithm: str,
    *,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
) -> tuple[list[dict], dict]:
    if len(old_vectors) != len(new_vectors):
        raise ValueError("replacement must preserve the vector count")
    if len(existing_summaries) != len(old_vectors):
        raise ValueError("existing_summaries must contain one record per vector")
    replacements = sorted(set(int(index) for index in replacement_indices))
    if not replacements or any(
        index < 0 or index >= len(old_vectors) for index in replacements
    ):
        raise ValueError("replacement_indices must contain valid vector indices")

    pairs = [
        (target, replacement)
        for target in range(len(old_vectors))
        if target not in replacements
        for replacement in replacements
    ]
    old_scores = backend.score_pairs(old_vectors, pairs, algorithm, workers=workers)
    new_scores = backend.score_pairs(new_vectors, pairs, algorithm, workers=workers)
    observations = {}
    for pair, old_score, new_score in zip(pairs, old_scores, new_scores):
        observations.setdefault(pair[0], []).append((pair[1], old_score, new_score))

    replacement_set = set(replacements)
    fallback = set(replacements)
    repaired_distant = {}

    def exact_zero_distant(target):
        candidates = []
        target_vector = new_vectors[target]
        for candidate, vector in enumerate(new_vectors):
            if candidate == target:
                continue
            common = set(target_vector).intersection(vector)
            if algorithm == "unweighted_cosine":
                zero = not common or not any(
                    target_vector[feature] * vector[feature] for feature in common
                )
            elif algorithm == "jaccard":
                zero = not common or not any(
                    min(target_vector[feature], vector[feature]) for feature in common
                )
            else:
                return None
            if zero:
                candidates.append((candidate, 0.0))
                if len(candidates) == distant_k:
                    return candidates
        return None

    for target, changed in observations.items():
        summary = existing_summaries[target]
        changed_scores = {
            candidate: (float(old_score), float(new_score))
            for candidate, old_score, new_score in changed
        }
        nearest_candidates = {candidate for candidate, _score in summary["nearest"]}
        distant_candidates = {candidate for candidate, _score in summary["distant"]}
        changed_nearest = replacement_set.intersection(nearest_candidates)
        if any(
            changed_scores[candidate][0] != changed_scores[candidate][1]
            for candidate in changed_nearest
        ):
            fallback.add(target)
            continue
        changed_distant = replacement_set.intersection(distant_candidates)
        if any(
            changed_scores[candidate][0] != changed_scores[candidate][1]
            for candidate in changed_distant
        ):
            repaired = exact_zero_distant(target)
            if repaired is None:
                fallback.add(target)
                continue
            repaired_distant[target] = repaired
        for field, extreme in (
            ("distant", float(summary["minimum"])),
            ("nearest", float(summary["maximum"])),
        ):
            removed_extreme = any(old_score == extreme for _, old_score, _ in changed)
            if field == "distant" and target in repaired_distant:
                surviving_extreme = repaired_distant[target][0][1] == extreme
            else:
                surviving_extreme = any(
                    candidate not in replacement_set and float(score) == extreme
                    for candidate, score in summary[field]
                )
            if removed_extreme and not surviving_extreme:
                fallback.add(target)
                break

    refreshed = {}
    if fallback:
        refreshed_summaries = backend.summarize_target_block(
            new_vectors,
            sorted(fallback),
            algorithm,
            workers=workers,
            nearest_k=nearest_k,
            distant_k=distant_k,
        )
        refreshed = dict(zip(sorted(fallback), refreshed_summaries))
        for summary in refreshed.values():
            summary["quantiles_status"] = "exact"

    updated = []
    for target, existing in enumerate(existing_summaries):
        if target in refreshed:
            updated.append(refreshed[target])
            continue
        changed = observations.get(target, [])
        removed = [float(old_score) for _, old_score, _ in changed]
        added = [float(new_score) for _, _, new_score in changed]
        count, mean, variance = replace_moment_observations(existing, removed, added)
        nearest = [
            item for item in existing["nearest"] if item[0] not in replacement_set
        ]
        distant = repaired_distant.get(
            target,
            [item for item in existing["distant"] if item[0] not in replacement_set],
        )
        replacements_with_scores = [
            (candidate, float(new_score)) for candidate, _, new_score in changed
        ]
        updated.append(
            {
                "nearest": merge_ranked(
                    nearest,
                    replacements_with_scores,
                    nearest_k,
                    descending=True,
                ),
                "distant": (
                    distant
                    if target in repaired_distant
                    else merge_ranked(
                        distant,
                        replacements_with_scores,
                        distant_k,
                        descending=False,
                    )
                ),
                "count": count,
                "mean": mean,
                "variance": variance,
                "minimum": min(float(existing["minimum"]), *added),
                "maximum": max(float(existing["maximum"]), *added),
                "quantiles": None,
                "quantiles_status": "pending_exact_refresh",
            }
        )
    return updated, {
        "replacement_count": len(replacements),
        "fallback_target_count": len(fallback),
        "incremental_target_count": len(old_vectors) - len(fallback),
        "rescored_pairs": len(pairs),
    }


def update_analytics_for_deletions(
    backend: ExactSimilarityBackend,
    old_vectors: Sequence[SparseVector],
    deletion_indices: Sequence[int],
    existing_summaries: Sequence[dict],
    algorithm: str,
    *,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
) -> tuple[list[dict], dict]:
    if len(existing_summaries) != len(old_vectors):
        raise ValueError("existing_summaries must contain one record per vector")
    deletions = sorted(set(int(index) for index in deletion_indices))
    if (
        not deletions
        or len(deletions) >= len(old_vectors)
        or any(index < 0 or index >= len(old_vectors) for index in deletions)
    ):
        raise ValueError("deletion_indices must leave at least one valid vector")
    deletion_set = set(deletions)
    surviving_old = [
        index for index in range(len(old_vectors)) if index not in deletion_set
    ]
    old_to_new = {old: new for new, old in enumerate(surviving_old)}
    new_vectors = [old_vectors[index] for index in surviving_old]
    pairs = [(target, deleted) for target in surviving_old for deleted in deletions]
    scores = backend.score_pairs(old_vectors, pairs, algorithm, workers=workers)
    observations = {}
    for (target, deleted), score in zip(pairs, scores):
        observations.setdefault(target, []).append((deleted, float(score)))

    fallback = set()
    repaired_distant = {}

    def exact_zero_distant(old_target):
        target = old_to_new[old_target]
        target_vector = new_vectors[target]
        target_features = set(target_vector)
        candidates = []
        for candidate, vector in enumerate(new_vectors):
            if candidate == target:
                continue
            common = target_features.intersection(vector)
            if algorithm == "unweighted_cosine":
                zero = not common or not any(
                    target_vector[feature] * vector[feature] for feature in common
                )
            elif algorithm == "jaccard":
                zero = not common or not any(
                    min(target_vector[feature], vector[feature]) for feature in common
                )
            else:
                return None
            if zero:
                candidates.append((candidate, 0.0))
                if len(candidates) == distant_k:
                    return candidates
        return None

    for target in surviving_old:
        summary = existing_summaries[target]
        nearest_candidates = {candidate for candidate, _score in summary["nearest"]}
        distant_candidates = {candidate for candidate, _score in summary["distant"]}
        if deletion_set.intersection(nearest_candidates):
            fallback.add(target)
            continue
        if deletion_set.intersection(distant_candidates):
            repaired = exact_zero_distant(target)
            if repaired is None:
                fallback.add(target)
                continue
            repaired_distant[target] = repaired
        removed_scores = observations[target]
        for field, extreme in (
            ("distant", float(summary["minimum"])),
            ("nearest", float(summary["maximum"])),
        ):
            removed_extreme = any(score == extreme for _, score in removed_scores)
            if field == "distant" and target in repaired_distant:
                surviving_extreme = repaired_distant[target][0][1] == extreme
            else:
                surviving_extreme = any(
                    candidate not in deletion_set and float(score) == extreme
                    for candidate, score in summary[field]
                )
            if removed_extreme and not surviving_extreme:
                fallback.add(target)
                break

    refreshed = {}
    if fallback:
        fallback_old = sorted(fallback)
        fallback_new = [old_to_new[target] for target in fallback_old]
        refreshed_summaries = backend.summarize_target_block(
            new_vectors,
            fallback_new,
            algorithm,
            workers=workers,
            nearest_k=nearest_k,
            distant_k=distant_k,
        )
        refreshed = dict(zip(fallback_old, refreshed_summaries))
        for summary in refreshed.values():
            summary["quantiles_status"] = "exact"

    updated = []
    for target in surviving_old:
        if target in refreshed:
            updated.append(refreshed[target])
            continue
        existing = existing_summaries[target]
        removed = [score for _, score in observations[target]]
        count, mean, variance = remove_moment_observations(existing, removed)
        nearest = [
            (old_to_new[candidate], float(score))
            for candidate, score in existing["nearest"]
            if candidate not in deletion_set
        ]
        distant = repaired_distant.get(
            target,
            [
                (old_to_new[candidate], float(score))
                for candidate, score in existing["distant"]
                if candidate not in deletion_set
            ],
        )
        updated.append(
            {
                "nearest": nearest[:nearest_k],
                "distant": distant[:distant_k],
                "count": count,
                "mean": mean,
                "variance": variance,
                "minimum": float(existing["minimum"]),
                "maximum": float(existing["maximum"]),
                "quantiles": None,
                "quantiles_status": "pending_exact_refresh",
            }
        )
    return updated, {
        "deletion_count": len(deletions),
        "fallback_target_count": len(fallback),
        "incremental_target_count": len(surviving_old) - len(fallback),
        "rescored_pairs": len(pairs),
    }


def compact_edges_from_summaries(
    summaries: Sequence[dict], function_ids: Sequence[str]
) -> list[tuple[str, str, float]]:
    if len(summaries) != len(function_ids):
        raise ValueError("function_ids must contain one ID per summary")
    by_pair = _indexed_edges_from_summaries(summaries, len(function_ids))
    return [
        (str(function_ids[left]), str(function_ids[right]), score)
        for (left, right), score in sorted(by_pair.items())
    ]


def _indexed_edges_from_summaries(summaries, function_count):
    by_pair = {}
    for target, summary in enumerate(summaries):
        for field in ("nearest", "distant"):
            for candidate, score in summary[field]:
                candidate = int(candidate)
                if not 0 <= candidate < function_count:
                    raise ValueError("summary candidate index is out of range")
                if candidate == target:
                    continue
                pair = (min(target, candidate), max(target, candidate))
                rounded = round(float(score), 4)
                if pair in by_pair and by_pair[pair] != rounded:
                    raise ValueError(f"conflicting compact edge score: {pair}")
                by_pair[pair] = rounded
    return by_pair


def compact_edge_delta(existing_edges, updated_edges):
    existing = {(left, right): score for left, right, score in existing_edges}
    updated = {(left, right): score for left, right, score in updated_edges}
    additions = [
        (left, right, score)
        for (left, right), score in sorted(updated.items())
        if existing.get((left, right)) != score
    ]
    removals = [
        (left, right, score)
        for (left, right), score in sorted(existing.items())
        if updated.get((left, right)) != score
    ]
    return additions, removals


def compact_edge_delta_from_summaries(
    existing_summaries, updated_summaries, function_ids, *, native_mode="auto"
):
    if native_mode not in {"auto", "on", "off"}:
        raise ValueError("native_mode must be auto, on, or off")
    if native_mode != "off":
        try:
            from bsimvis_similarity_native import (
                compact_edge_delta_from_summaries_native,
            )
        except (ImportError, AttributeError):
            if native_mode == "on":
                raise RuntimeError("native compact delta is unavailable") from None
        else:
            existing = [
                (summary["nearest"], summary["distant"])
                for summary in existing_summaries
            ]
            updated = [
                (summary["nearest"], summary["distant"])
                for summary in updated_summaries
            ]
            additions, removals, existing_count, updated_count = (
                compact_edge_delta_from_summaries_native(existing, updated)
            )

            def render(edge):
                left, right, score = edge
                return str(function_ids[left]), str(function_ids[right]), float(score)

            return (
                [render(edge) for edge in additions],
                [render(edge) for edge in removals],
                int(existing_count),
                int(updated_count),
                "rust_native",
            )

    existing = _indexed_edges_from_summaries(
        existing_summaries, len(existing_summaries)
    )
    updated = _indexed_edges_from_summaries(updated_summaries, len(function_ids))

    def render(pair, score):
        left, right = pair
        return str(function_ids[left]), str(function_ids[right]), score

    additions = [
        render(pair, score)
        for pair, score in sorted(updated.items())
        if existing.get(pair) != score
    ]
    removals = [
        render(pair, score)
        for pair, score in sorted(existing.items())
        if updated.get(pair) != score
    ]
    return additions, removals, len(existing), len(updated), "python"


def compact_edge_delta_between_products(
    existing_summaries,
    existing_function_ids,
    updated_summaries,
    updated_function_ids,
):
    existing_edges = compact_edges_from_summaries(
        existing_summaries, existing_function_ids
    )
    updated_edges = compact_edges_from_summaries(
        updated_summaries, updated_function_ids
    )
    additions, removals = compact_edge_delta(existing_edges, updated_edges)
    return additions, removals, len(existing_edges), len(updated_edges)


def _write_compact_delta_partitions(
    redis_client,
    collection,
    storage_algorithm,
    additions,
    removals,
    partition_size,
):
    addition_store = CompactSimilarityStore(redis_client, collection, storage_algorithm)
    removal_store = CompactSimilarityStore(
        redis_client, collection, f"{storage_algorithm}.removed"
    )
    created = 0
    encoded_bytes = 0
    partition_count = 0
    for prefix, store, changed_edges in (
        ("add", addition_store, additions),
        ("remove", removal_store, removals),
    ):
        for offset in range(0, len(changed_edges), partition_size):
            result = store.write_partition(
                f"{prefix}_{offset // partition_size:06d}",
                changed_edges[offset : offset + partition_size],
            )
            created += int(result.created)
            encoded_bytes += result.encoded_bytes
            partition_count += 1
    return created, encoded_bytes, partition_count


def write_incremental_compact_working_generation(
    backend: ExactSimilarityBackend,
    redis_client,
    generation_manager,
    vectors: Sequence[SparseVector],
    base_count: int,
    existing_summaries: Sequence[dict],
    collection: str,
    algorithm: str,
    generation_id: str,
    *,
    function_ids: Sequence[str] | None = None,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
    partition_size: int = 4096,
    native_delta: str = "auto",
) -> dict:
    if partition_size <= 0:
        raise ValueError("partition_size must be positive")
    total_started = time.perf_counter()
    function_ids = list(function_ids or (str(index) for index in range(len(vectors))))
    generation = generation_manager.begin(
        collection,
        algorithm,
        {
            "compact_mode": "incremental_working",
            "update_kind": "append",
            "base_count": base_count,
            "target_count": len(vectors),
        },
        generation_id,
    )
    journal_created = generation_manager.append_journal(
        collection,
        algorithm,
        generation_id,
        "mutation",
        {"kind": "append", "base_count": base_count, "target_count": len(vectors)},
    )
    incremental_started = time.perf_counter()
    summaries = update_analytics_for_append(
        backend,
        vectors,
        base_count,
        existing_summaries,
        algorithm,
        workers=workers,
        nearest_k=nearest_k,
        distant_k=distant_k,
    )
    incremental_seconds = time.perf_counter() - incremental_started
    compact_started = time.perf_counter()
    additions, removals, base_edge_count, edge_count, delta_backend = (
        compact_edge_delta_from_summaries(
            existing_summaries,
            summaries,
            function_ids,
            native_mode=native_delta,
        )
    )
    created, encoded_bytes, partition_count = _write_compact_delta_partitions(
        redis_client,
        collection,
        generation["storage_algorithm"],
        additions,
        removals,
        partition_size,
    )
    compact_seconds = time.perf_counter() - compact_started
    checkpoint_started = time.perf_counter()
    checkpoint_created = generation_manager.checkpoint(
        collection,
        algorithm,
        generation_id,
        "compact_complete",
        items=len(additions) + len(removals),
        bytes_written=encoded_bytes,
    )
    sealed = generation_manager.seal_incremental(
        collection,
        algorithm,
        generation_id,
        output_targets=len(vectors),
        edge_count=edge_count,
        delta_items=len(additions) + len(removals),
    )
    checkpoint_seconds = time.perf_counter() - checkpoint_started
    return {
        **generation,
        "summaries": summaries,
        "edge_count": edge_count,
        "base_edge_count": base_edge_count,
        "addition_count": len(additions),
        "removal_count": len(removals),
        "delta_item_count": len(additions) + len(removals),
        "partition_count": partition_count,
        "partitions_created": created,
        "encoded_bytes": encoded_bytes,
        "delta_backend": delta_backend,
        "checkpoint_created": checkpoint_created,
        "journal_created": journal_created,
        "sealed": sealed,
        "incremental_seconds": incremental_seconds,
        "compact_seconds": compact_seconds,
        "checkpoint_seconds": checkpoint_seconds,
        "total_seconds": time.perf_counter() - total_started,
        "status": "working",
    }


def write_replacement_compact_working_generation(
    backend: ExactSimilarityBackend,
    redis_client,
    generation_manager,
    old_vectors: Sequence[SparseVector],
    new_vectors: Sequence[SparseVector],
    replacement_indices: Sequence[int],
    existing_summaries: Sequence[dict],
    collection: str,
    algorithm: str,
    generation_id: str,
    *,
    function_ids: Sequence[str] | None = None,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
    partition_size: int = 4096,
    native_delta: str = "auto",
) -> dict:
    if partition_size <= 0:
        raise ValueError("partition_size must be positive")
    total_started = time.perf_counter()
    function_ids = list(
        function_ids or (str(index) for index in range(len(new_vectors)))
    )
    generation = generation_manager.begin(
        collection,
        algorithm,
        {
            "compact_mode": "incremental_working",
            "update_kind": "replacement",
            "replacement_indices": sorted(set(int(i) for i in replacement_indices)),
            "target_count": len(new_vectors),
        },
        generation_id,
    )
    journal_created = generation_manager.append_journal(
        collection,
        algorithm,
        generation_id,
        "mutation",
        {
            "kind": "replacement",
            "indices": sorted(set(int(i) for i in replacement_indices)),
            "target_count": len(new_vectors),
        },
    )
    incremental_started = time.perf_counter()
    summaries, update_stats = update_analytics_for_replacements(
        backend,
        old_vectors,
        new_vectors,
        replacement_indices,
        existing_summaries,
        algorithm,
        workers=workers,
        nearest_k=nearest_k,
        distant_k=distant_k,
    )
    incremental_seconds = time.perf_counter() - incremental_started
    compact_started = time.perf_counter()
    additions, removals, base_edge_count, edge_count, delta_backend = (
        compact_edge_delta_from_summaries(
            existing_summaries,
            summaries,
            function_ids,
            native_mode=native_delta,
        )
    )
    created, encoded_bytes, partition_count = _write_compact_delta_partitions(
        redis_client,
        collection,
        generation["storage_algorithm"],
        additions,
        removals,
        partition_size,
    )
    compact_seconds = time.perf_counter() - compact_started
    checkpoint_started = time.perf_counter()
    checkpoint_created = generation_manager.checkpoint(
        collection,
        algorithm,
        generation_id,
        "compact_complete",
        items=len(additions) + len(removals),
        bytes_written=encoded_bytes,
    )
    sealed = generation_manager.seal_incremental(
        collection,
        algorithm,
        generation_id,
        output_targets=len(new_vectors),
        edge_count=edge_count,
        delta_items=len(additions) + len(removals),
    )
    checkpoint_seconds = time.perf_counter() - checkpoint_started
    return {
        **generation,
        **update_stats,
        "summaries": summaries,
        "edge_count": edge_count,
        "base_edge_count": base_edge_count,
        "addition_count": len(additions),
        "removal_count": len(removals),
        "delta_item_count": len(additions) + len(removals),
        "partition_count": partition_count,
        "partitions_created": created,
        "encoded_bytes": encoded_bytes,
        "delta_backend": delta_backend,
        "checkpoint_created": checkpoint_created,
        "journal_created": journal_created,
        "sealed": sealed,
        "incremental_seconds": incremental_seconds,
        "compact_seconds": compact_seconds,
        "checkpoint_seconds": checkpoint_seconds,
        "total_seconds": time.perf_counter() - total_started,
        "status": "working",
    }


def write_deletion_compact_working_generation(
    backend: ExactSimilarityBackend,
    redis_client,
    generation_manager,
    old_vectors: Sequence[SparseVector],
    deletion_indices: Sequence[int],
    existing_summaries: Sequence[dict],
    collection: str,
    algorithm: str,
    generation_id: str,
    *,
    function_ids: Sequence[str] | None = None,
    workers: int = 1,
    nearest_k: int = 20,
    distant_k: int = 20,
    partition_size: int = 4096,
) -> dict:
    if partition_size <= 0:
        raise ValueError("partition_size must be positive")
    total_started = time.perf_counter()
    function_ids = list(
        function_ids or (str(index) for index in range(len(old_vectors)))
    )
    deletions = sorted(set(int(index) for index in deletion_indices))
    deletion_set = set(deletions)
    surviving_ids = [
        function_id
        for index, function_id in enumerate(function_ids)
        if index not in deletion_set
    ]
    generation = generation_manager.begin(
        collection,
        algorithm,
        {
            "compact_mode": "incremental_working",
            "update_kind": "deletion",
            "deletion_indices": deletions,
            "target_count": len(surviving_ids),
        },
        generation_id,
    )
    journal_created = generation_manager.append_journal(
        collection,
        algorithm,
        generation_id,
        "mutation",
        {
            "kind": "deletion",
            "indices": deletions,
            "target_count": len(surviving_ids),
        },
    )
    incremental_started = time.perf_counter()
    summaries, update_stats = update_analytics_for_deletions(
        backend,
        old_vectors,
        deletions,
        existing_summaries,
        algorithm,
        workers=workers,
        nearest_k=nearest_k,
        distant_k=distant_k,
    )
    incremental_seconds = time.perf_counter() - incremental_started
    compact_started = time.perf_counter()
    additions, removals, base_edge_count, edge_count = (
        compact_edge_delta_between_products(
            existing_summaries,
            function_ids,
            summaries,
            surviving_ids,
        )
    )
    created, encoded_bytes, partition_count = _write_compact_delta_partitions(
        redis_client,
        collection,
        generation["storage_algorithm"],
        additions,
        removals,
        partition_size,
    )
    compact_seconds = time.perf_counter() - compact_started
    checkpoint_started = time.perf_counter()
    checkpoint_created = generation_manager.checkpoint(
        collection,
        algorithm,
        generation_id,
        "compact_complete",
        items=len(additions) + len(removals),
        bytes_written=encoded_bytes,
    )
    sealed = generation_manager.seal_incremental(
        collection,
        algorithm,
        generation_id,
        output_targets=len(surviving_ids),
        edge_count=edge_count,
        delta_items=len(additions) + len(removals),
    )
    checkpoint_seconds = time.perf_counter() - checkpoint_started
    return {
        **generation,
        **update_stats,
        "summaries": summaries,
        "function_ids": surviving_ids,
        "edge_count": edge_count,
        "base_edge_count": base_edge_count,
        "addition_count": len(additions),
        "removal_count": len(removals),
        "delta_item_count": len(additions) + len(removals),
        "partition_count": partition_count,
        "partitions_created": created,
        "encoded_bytes": encoded_bytes,
        "checkpoint_created": checkpoint_created,
        "journal_created": journal_created,
        "sealed": sealed,
        "incremental_seconds": incremental_seconds,
        "compact_seconds": compact_seconds,
        "checkpoint_seconds": checkpoint_seconds,
        "total_seconds": time.perf_counter() - total_started,
        "status": "working",
    }
