from __future__ import annotations

from abc import ABC, abstractmethod
from collections import OrderedDict
import logging
import math
import os
import platform
import threading
from typing import Callable, Mapping, Sequence

SparseVector = Mapping[str, float]
IndexedPair = tuple[int, int]
logger = logging.getLogger(__name__)


def _physical_memory_bytes() -> int | None:
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None


class ExactSimilarityBackend(ABC):
    """Numeric mechanism only; Python remains responsible for analysis policy."""

    name: str

    @abstractmethod
    def score_pair(
        self, vector_a: SparseVector, vector_b: SparseVector, algorithm: str
    ) -> float | None:
        pass

    def score_pairs(
        self,
        vectors: Sequence[SparseVector],
        pairs: Sequence[IndexedPair],
        algorithm: str,
        *,
        workers: int = 1,
    ) -> list[float]:
        del workers
        scores = []
        for left, right in pairs:
            score = self.score_pair(vectors[left], vectors[right], algorithm)
            if score is None:
                raise ValueError(f"Unsupported algorithm: {algorithm}")
            scores.append(score)
        return scores

    def score_all_pairs_checksum(
        self,
        vectors: Sequence[SparseVector],
        algorithm: str,
        *,
        workers: int = 1,
    ) -> tuple[int, float]:
        del workers
        checksum = 0.0
        pair_count = 0
        for left, vector_a in enumerate(vectors):
            for vector_b in vectors[left + 1 :]:
                score = self.score_pair(vector_a, vector_b, algorithm)
                if score is None:
                    raise ValueError(f"Unsupported algorithm: {algorithm}")
                checksum += score
                pair_count += 1
        return pair_count, checksum

    def select_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        top_k: int = 1000,
        min_score: float = 0.0,
    ) -> list[list[tuple[int, float]]]:
        del workers
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        selected = []
        for target_index in target_indices:
            candidates = []
            for candidate_index in eligible_candidates:
                if candidate_index == target_index:
                    continue
                candidate = vectors[candidate_index]
                score = self.score_pair(vectors[target_index], candidate, algorithm)
                if score is None:
                    raise ValueError(f"Unsupported algorithm: {algorithm}")
                if score > 0.0 and score >= min_score:
                    candidates.append((candidate_index, score))
            candidates.sort(key=lambda item: (-item[1], item[0]))
            selected.append(candidates[: max(0, top_k)])
        return selected

    def summarize_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        nearest_k: int = 20,
        distant_k: int = 20,
    ) -> list[dict]:
        del workers
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        probabilities = (0.05, 0.25, 0.5, 0.75, 0.95)
        summaries = []
        for target_index in target_indices:
            candidates = []
            for candidate_index in eligible_candidates:
                if candidate_index == target_index:
                    continue
                score = self.score_pair(
                    vectors[target_index], vectors[candidate_index], algorithm
                )
                if score is None:
                    raise ValueError(f"Unsupported algorithm: {algorithm}")
                candidates.append((candidate_index, score))
            values = sorted(score for _, score in candidates)
            count = len(values)
            mean = sum(values) / count if count else 0.0
            variance = (
                sum((value - mean) ** 2 for value in values) / count if count else 0.0
            )

            def quantile(probability):
                if not values:
                    return 0.0
                position = probability * (len(values) - 1)
                lower = math.floor(position)
                upper = math.ceil(position)
                if lower == upper:
                    return values[lower]
                fraction = position - lower
                return values[lower] * (1 - fraction) + values[upper] * fraction

            summaries.append(
                {
                    "nearest": sorted(candidates, key=lambda item: (-item[1], item[0]))[
                        : max(0, nearest_k)
                    ],
                    "distant": sorted(candidates, key=lambda item: (item[1], item[0]))[
                        : max(0, distant_k)
                    ],
                    "count": count,
                    "mean": mean,
                    "variance": variance,
                    "minimum": values[0] if values else 0.0,
                    "maximum": values[-1] if values else 0.0,
                    "quantiles": {
                        str(probability): quantile(probability)
                        for probability in probabilities
                    },
                }
            )
        return summaries

    def select_frequency_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        candidate_limit: int = 1000,
        max_posting_fraction: float = 1.0,
        top_k: int = 100,
        min_score: float = 0.0,
    ) -> list[dict]:
        del workers
        eligible = set(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        postings = {}
        for vector_index, vector in enumerate(vectors):
            for feature in vector:
                postings.setdefault(feature, []).append(vector_index)
        population = len(vectors)
        results = []
        for target_index in target_indices:
            support = {}
            for feature in vectors[target_index]:
                posting = postings[feature]
                if len(posting) / population > max_posting_fraction:
                    continue
                weight = math.log((population + 1) / (len(posting) + 1)) + 1
                for candidate in posting:
                    if candidate != target_index and candidate in eligible:
                        support[candidate] = support.get(candidate, 0.0) + weight
            ranked = sorted(
                support, key=lambda candidate: (-support[candidate], candidate)
            )
            if not ranked:
                ranked = sorted(eligible - {target_index})
            ranked = ranked[: max(0, candidate_limit)]
            selected = self.select_target_block(
                vectors,
                [target_index],
                algorithm,
                candidate_indices=ranked,
                top_k=top_k,
                min_score=min_score,
            )[0]
            results.append({"selected": selected, "candidate_count": len(ranked)})
        return results

    def select_inverted_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        max_posting_fraction: float = 1.0,
        top_k: int = 100,
        min_score: float = 0.0,
    ) -> list[dict]:
        del workers
        eligible = set(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        postings = {}
        for vector_index, vector in enumerate(vectors):
            for feature in vector:
                postings.setdefault(feature, []).append(vector_index)
        population = len(vectors)
        results = []
        for target_index in target_indices:
            candidates = set()
            for feature in vectors[target_index]:
                posting = postings[feature]
                if len(posting) / population <= max_posting_fraction:
                    candidates.update(posting)
            candidates.discard(target_index)
            candidates.intersection_update(eligible)
            selected = self.select_target_block(
                vectors,
                [target_index],
                algorithm,
                candidate_indices=sorted(candidates),
                top_k=top_k,
                min_score=min_score,
            )[0]
            results.append({"selected": selected, "candidate_count": len(candidates)})
        return results


class PythonExactBackend(ExactSimilarityBackend):
    name = "python_exact"

    def score_pair(
        self, vector_a: SparseVector, vector_b: SparseVector, algorithm: str
    ) -> float | None:
        common = set(vector_a).intersection(vector_b)
        if not common:
            return 0.0 if algorithm in {"jaccard", "unweighted_cosine"} else None

        if algorithm == "jaccard":
            intersection = sum(min(vector_a[key], vector_b[key]) for key in common)
            union = sum(vector_a.values()) + sum(vector_b.values()) - intersection
            return float(intersection / union) if union > 0 else 0.0

        if algorithm == "unweighted_cosine":
            dot_product = sum(vector_a[key] * vector_b[key] for key in common)
            norm_a = math.sqrt(sum(value**2 for value in vector_a.values()))
            norm_b = math.sqrt(sum(value**2 for value in vector_b.values()))
            return (
                float(dot_product / (norm_a * norm_b))
                if norm_a > 0 and norm_b > 0
                else 0.0
            )

        return None


class RustExactBackend(ExactSimilarityBackend):
    name = "rust_cpu"

    def __init__(self):
        try:
            import bsimvis_similarity_native
            from bsimvis_similarity_native import ExactScorer
        except ImportError as exc:
            raise RuntimeError(
                "rust_cpu backend is not installed; build native/bsimvis_similarity"
            ) from exc
        self._scorer_type = ExactScorer
        self.wgpu_supported = hasattr(bsimvis_similarity_native, "wgpu_adapter_info")
        self._snapshot = None
        self._snapshot_identity = None
        self._snapshot_limit = max(
            1, int(os.getenv("BSIMVIS_NATIVE_SNAPSHOT_GENERATIONS", "2"))
        )
        self._snapshots = OrderedDict()
        self._snapshot_hits = 0
        self._snapshot_misses = 0
        self._snapshot_evictions = 0

    def _prepare(self, vectors: Sequence[SparseVector]):
        identity = id(vectors)
        cached = self._snapshots.get(identity)
        if cached is not None and cached[0] is vectors:
            self._snapshot_hits += 1
            self._snapshots.move_to_end(identity)
            self._snapshot = cached[1]
            self._snapshot_identity = identity
        else:
            self._snapshot_misses += 1
            packed = [list(vector.items()) for vector in vectors]
            self._snapshot = self._scorer_type(packed)
            self._snapshot_identity = identity
            self._snapshots[identity] = (vectors, self._snapshot)
            self._snapshots.move_to_end(identity)
            while len(self._snapshots) > self._snapshot_limit:
                _key, (_vectors, scorer) = self._snapshots.popitem(last=False)
                if hasattr(scorer, "clear_wgpu_resident_buffers"):
                    scorer.clear_wgpu_resident_buffers()
                self._snapshot_evictions += 1
        return self._snapshot

    def snapshot_cache_stats(self):
        return {
            "entries": len(self._snapshots),
            "max_generations": self._snapshot_limit,
            "hits": self._snapshot_hits,
            "misses": self._snapshot_misses,
            "evictions": self._snapshot_evictions,
        }

    def score_pair(
        self, vector_a: SparseVector, vector_b: SparseVector, algorithm: str
    ) -> float | None:
        if algorithm not in {"jaccard", "unweighted_cosine"}:
            return None
        scorer = self._scorer_type([list(vector_a.items()), list(vector_b.items())])
        return float(scorer.score_pairs([(0, 1)], algorithm, 1)[0])

    def score_pairs(
        self,
        vectors: Sequence[SparseVector],
        pairs: Sequence[IndexedPair],
        algorithm: str,
        *,
        workers: int = 1,
    ) -> list[float]:
        scorer = self._prepare(vectors)
        return list(scorer.score_pairs(list(pairs), algorithm, workers))

    def score_all_pairs_checksum(
        self,
        vectors: Sequence[SparseVector],
        algorithm: str,
        *,
        workers: int = 1,
    ) -> tuple[int, float]:
        scorer = self._prepare(vectors)
        pair_count, checksum = scorer.score_all_pairs_checksum(algorithm, workers)
        return int(pair_count), float(checksum)

    def score_pairs_wgpu(
        self,
        vectors: Sequence[SparseVector],
        pairs: Sequence[IndexedPair],
        algorithm: str,
    ) -> list[float]:
        scorer = self._prepare(vectors)
        if not hasattr(scorer, "score_pairs_wgpu"):
            raise RuntimeError("native backend was built without the gpu feature")
        return list(scorer.score_pairs_wgpu(list(pairs), algorithm))

    def score_all_pairs_wgpu_checksum(
        self,
        vectors: Sequence[SparseVector],
        algorithm: str,
    ) -> tuple[int, float]:
        scorer = self._prepare(vectors)
        if not hasattr(scorer, "score_all_pairs_wgpu_checksum"):
            raise RuntimeError("native backend was built without the gpu feature")
        pair_count, checksum = scorer.score_all_pairs_wgpu_checksum(algorithm)
        return int(pair_count), float(checksum)

    def select_target_block_wgpu(
        self,
        vectors,
        target_indices,
        algorithm,
        *,
        candidate_indices=None,
        workers=1,
        top_k=1000,
        min_score=0.0,
        score_margin=1e-5,
    ):
        scorer = self._prepare(vectors)
        if not hasattr(scorer, "select_target_block_wgpu"):
            raise RuntimeError("native backend was built without the gpu feature")
        candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        selected = scorer.select_target_block_wgpu(
            list(target_indices),
            list(candidates),
            algorithm,
            workers,
            max(0, top_k),
            min_score,
            score_margin,
        )
        return [
            [(int(candidate), float(score)) for candidate, score in row]
            for row in selected
        ]

    def wgpu_resident_bytes(self) -> int:
        if self._snapshot is None:
            return 0
        scorer = self._snapshot
        if not hasattr(scorer, "wgpu_resident_bytes"):
            return 0
        return int(scorer.wgpu_resident_bytes())

    def clear_wgpu_resident_buffers(self) -> None:
        for _vectors, scorer in self._snapshots.values():
            if hasattr(scorer, "clear_wgpu_resident_buffers"):
                scorer.clear_wgpu_resident_buffers()

    def select_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        top_k: int = 1000,
        min_score: float = 0.0,
    ) -> list[list[tuple[int, float]]]:
        scorer = self._prepare(vectors)
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        selected = scorer.select_target_block(
            list(target_indices),
            list(eligible_candidates),
            algorithm,
            workers,
            max(0, top_k),
            min_score,
        )
        return [
            [(int(candidate), float(score)) for candidate, score in candidates]
            for candidates in selected
        ]

    def summarize_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        nearest_k: int = 20,
        distant_k: int = 20,
    ) -> list[dict]:
        scorer = self._prepare(vectors)
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        raw_summaries = scorer.summarize_target_block(
            list(target_indices),
            list(eligible_candidates),
            algorithm,
            workers,
            max(0, nearest_k),
            max(0, distant_k),
        )
        probabilities = ("0.05", "0.25", "0.5", "0.75", "0.95")
        summaries = []
        for raw in raw_summaries:
            nearest, distant, count, mean, variance, minimum, maximum, quantiles = raw
            summaries.append(
                {
                    "nearest": [
                        (int(candidate), float(score)) for candidate, score in nearest
                    ],
                    "distant": [
                        (int(candidate), float(score)) for candidate, score in distant
                    ],
                    "count": int(count),
                    "mean": float(mean),
                    "variance": float(variance),
                    "minimum": float(minimum),
                    "maximum": float(maximum),
                    "quantiles": {
                        probability: float(value)
                        for probability, value in zip(probabilities, quantiles)
                    },
                }
            )
        return summaries

    def select_frequency_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        candidate_limit: int = 1000,
        max_posting_fraction: float = 1.0,
        top_k: int = 100,
        min_score: float = 0.0,
    ) -> list[dict]:
        scorer = self._prepare(vectors)
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        raw_results = scorer.select_frequency_target_block(
            list(target_indices),
            list(eligible_candidates),
            algorithm,
            workers,
            max(0, candidate_limit),
            max_posting_fraction,
            max(0, top_k),
            min_score,
        )
        return [
            {
                "selected": [
                    (int(candidate), float(score)) for candidate, score in selected
                ],
                "candidate_count": int(candidate_count),
            }
            for selected, candidate_count in raw_results
        ]

    def select_inverted_target_block(
        self,
        vectors: Sequence[SparseVector],
        target_indices: Sequence[int],
        algorithm: str,
        *,
        candidate_indices: Sequence[int] | None = None,
        workers: int = 1,
        max_posting_fraction: float = 1.0,
        top_k: int = 100,
        min_score: float = 0.0,
    ) -> list[dict]:
        scorer = self._prepare(vectors)
        eligible_candidates = (
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        raw_results = scorer.select_inverted_target_block(
            list(target_indices),
            list(eligible_candidates),
            algorithm,
            workers,
            max_posting_fraction,
            max(0, top_k),
            min_score,
        )
        return [
            {
                "selected": [
                    (int(candidate), float(score)) for candidate, score in selected
                ],
                "candidate_count": int(candidate_count),
            }
            for selected, candidate_count in raw_results
        ]


class GpuFirstExactBackend(ExactSimilarityBackend):
    """Use WGPU for broad selection and preserve exact CPU result boundaries."""

    name = "auto_gpu_cpu"

    def __init__(
        self,
        *,
        cpu_backend: ExactSimilarityBackend | None = None,
        gpu_backend: RustExactBackend | None = None,
        gpu_enabled: bool = True,
        score_margin: float = 1e-5,
        minimum_pairs: int = 0,
        resident_buffers: bool = True,
    ):
        if cpu_backend is None:
            try:
                cpu_backend = RustExactBackend()
            except RuntimeError:
                cpu_backend = PythonExactBackend()
        if (
            gpu_enabled
            and gpu_backend is None
            and isinstance(cpu_backend, RustExactBackend)
            and cpu_backend.wgpu_supported
        ):
            gpu_backend = cpu_backend
        self.cpu_backend = cpu_backend
        self.gpu_backend = gpu_backend if gpu_enabled else None
        self.gpu_enabled = gpu_enabled
        self.score_margin = score_margin
        self.minimum_pairs = max(0, minimum_pairs)
        self.resident_buffers = resident_buffers
        self.gpu_blocks = 0
        self.gpu_skipped_blocks = 0
        self.cpu_fallback_blocks = 0
        self._gpu_failure_reason = None
        self._notices = []
        if self.gpu_enabled and self.gpu_backend is None:
            self._gpu_failure_reason = "native WGPU feature is not installed"
            self._notices.append(
                f"WGPU unavailable; continued on {self.cpu_backend.name}: "
                f"{self._gpu_failure_reason}"
            )

    @property
    def execution_policy(self) -> dict:
        apple_unified_memory = (
            platform.system() == "Darwin"
            and platform.machine().lower() in {"arm64", "aarch64"}
        )
        return {
            "requested": "wgpu_then_cpu" if self.gpu_enabled else "cpu_only",
            "cpu_backend": self.cpu_backend.name,
            "gpu_available": self.gpu_backend is not None,
            "gpu_blocks": self.gpu_blocks,
            "gpu_skipped_blocks": self.gpu_skipped_blocks,
            "cpu_fallback_blocks": self.cpu_fallback_blocks,
            "gpu_failure_reason": self._gpu_failure_reason,
            "score_margin": self.score_margin,
            "minimum_pairs": self.minimum_pairs,
            "resident_buffers": self.resident_buffers,
            "resident_bytes": (
                self.gpu_backend.wgpu_resident_bytes()
                if isinstance(self.gpu_backend, RustExactBackend)
                else 0
            ),
            "unified_memory_detected": apple_unified_memory,
            "physical_memory_bytes": _physical_memory_bytes(),
        }

    def drain_notices(self) -> list[str]:
        notices, self._notices = self._notices, []
        return notices

    def _fallback(self, reason: str) -> None:
        self.cpu_fallback_blocks += 1
        if self._gpu_failure_reason is not None:
            return
        self._gpu_failure_reason = reason
        self.gpu_backend = None
        message = f"WGPU unavailable; continued on {self.cpu_backend.name}: {reason}"
        self._notices.append(message)
        logger.warning(message)

    def score_pair(self, vector_a, vector_b, algorithm):
        return self.cpu_backend.score_pair(vector_a, vector_b, algorithm)

    def score_pairs(self, vectors, pairs, algorithm, *, workers=1):
        return self.cpu_backend.score_pairs(vectors, pairs, algorithm, workers=workers)

    def score_all_pairs_checksum(self, vectors, algorithm, *, workers=1):
        return self.cpu_backend.score_all_pairs_checksum(
            vectors, algorithm, workers=workers
        )

    def select_target_block(
        self,
        vectors,
        target_indices,
        algorithm,
        *,
        candidate_indices=None,
        workers=1,
        top_k=1000,
        min_score=0.0,
    ):
        candidates = list(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        targets = list(target_indices)
        candidate_set = set(candidates)
        pair_count = len(targets) * len(candidates) - sum(
            target in candidate_set for target in targets
        )
        if self.gpu_backend is not None and pair_count < self.minimum_pairs:
            self.gpu_skipped_blocks += 1
            return self.cpu_backend.select_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                top_k=top_k,
                min_score=min_score,
            )
        if self.gpu_backend is None:
            if self.gpu_enabled:
                self.cpu_fallback_blocks += 1
            return self.cpu_backend.select_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                top_k=top_k,
                min_score=min_score,
            )

        try:
            selected = self.gpu_backend.select_target_block_wgpu(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                top_k=top_k,
                min_score=min_score,
                score_margin=self.score_margin,
            )
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._fallback(str(exc))
            return self.cpu_backend.select_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                top_k=top_k,
                min_score=min_score,
            )

        self.gpu_blocks += 1
        if not self.resident_buffers and isinstance(self.gpu_backend, RustExactBackend):
            self.gpu_backend.clear_wgpu_resident_buffers()
        return selected

    def summarize_target_block(self, *args, **kwargs):
        return self.cpu_backend.summarize_target_block(*args, **kwargs)

    def select_frequency_target_block(self, *args, **kwargs):
        return self.cpu_backend.select_frequency_target_block(*args, **kwargs)

    def select_inverted_target_block(self, *args, **kwargs):
        return self.cpu_backend.select_inverted_target_block(*args, **kwargs)


class VariableSiliconDriveBackend(ExactSimilarityBackend):
    """Select the lowest-overhead exact backend that suits each work block."""

    name = "variable_silicon_drive"

    def __init__(
        self,
        *,
        python_backend: ExactSimilarityBackend | None = None,
        rust_factory: Callable[[], ExactSimilarityBackend] = RustExactBackend,
        rust_minimum_pairs: int = 200_000,
        gpu_enabled: bool = True,
        gpu_minimum_pairs: int = 4_000_000,
        gpu_maximum_pairs_per_dispatch: int = 15_000_000,
        score_margin: float = 1e-5,
        resident_buffers: bool = True,
    ):
        self.python_backend = python_backend or PythonExactBackend()
        self.rust_factory = rust_factory
        self.rust_minimum_pairs = max(0, rust_minimum_pairs)
        self.gpu_enabled = gpu_enabled
        self.gpu_minimum_pairs = max(self.rust_minimum_pairs, gpu_minimum_pairs)
        self.gpu_maximum_pairs_per_dispatch = max(1, gpu_maximum_pairs_per_dispatch)
        self.score_margin = score_margin
        self.resident_buffers = resident_buffers
        self._rust_backend = None
        self._gpu_backend = None
        self._native_unavailable_reason = None
        self._notices = []
        self.python_blocks = 0
        self.rust_blocks = 0
        self.gpu_blocks = 0
        self.gpu_dispatches = 0

    @property
    def execution_policy(self) -> dict:
        apple_unified_memory = (
            platform.system() == "Darwin"
            and platform.machine().lower() in {"arm64", "aarch64"}
        )
        gpu_policy = (
            getattr(self._gpu_backend, "execution_policy", {})
            if self._gpu_backend
            else {}
        )
        if not self.gpu_enabled or self._native_unavailable_reason is not None:
            gpu_available = False
        elif self._gpu_backend is not None:
            gpu_available = getattr(self._gpu_backend, "gpu_backend", None) is not None
        elif isinstance(self._rust_backend, RustExactBackend):
            gpu_available = self._rust_backend.wgpu_supported
        else:
            # Before lazy native initialization, report potential availability so
            # services can choose WGPU-friendly target block sizes.
            gpu_available = True
        return {
            "requested": self.name,
            "tiers": ["python_exact", "rust_cpu", "wgpu"],
            "python_blocks": self.python_blocks,
            "rust_blocks": self.rust_blocks,
            "gpu_blocks": self.gpu_blocks,
            "gpu_dispatches": self.gpu_dispatches,
            "rust_minimum_pairs": self.rust_minimum_pairs,
            "gpu_minimum_pairs": self.gpu_minimum_pairs,
            "gpu_maximum_pairs_per_dispatch": self.gpu_maximum_pairs_per_dispatch,
            "native_state": (
                "unavailable"
                if self._native_unavailable_reason
                else "ready" if self._rust_backend else "lazy"
            ),
            "native_unavailable_reason": self._native_unavailable_reason,
            # A potential GPU remains truthy so the service can choose an efficient
            # target block before the lazy native backend is initialized.
            "gpu_available": gpu_available,
            "gpu_execution": gpu_policy,
            "score_margin": self.score_margin,
            "resident_buffers": self.resident_buffers,
            "unified_memory_detected": apple_unified_memory,
            "physical_memory_bytes": _physical_memory_bytes(),
        }

    def drain_notices(self) -> list[str]:
        notices, self._notices = self._notices, []
        if self._gpu_backend is not None:
            notices.extend(self._gpu_backend.drain_notices())
        return notices

    @staticmethod
    def _block_pair_count(target_indices, candidate_indices) -> int:
        targets = list(target_indices)
        candidates = list(candidate_indices)
        candidate_set = set(candidates)
        return len(targets) * len(candidates) - sum(
            target in candidate_set for target in targets
        )

    def _get_rust_backend(self) -> ExactSimilarityBackend | None:
        if self._rust_backend is not None:
            return self._rust_backend
        if self._native_unavailable_reason is not None:
            return None
        try:
            self._rust_backend = self.rust_factory()
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._disable_native(exc, recomputed=False)
        return self._rust_backend

    def _disable_native(self, reason: BaseException, *, recomputed: bool) -> None:
        if self._native_unavailable_reason is not None:
            return
        if self._gpu_backend is not None and hasattr(
            self._gpu_backend, "drain_notices"
        ):
            self._notices.extend(self._gpu_backend.drain_notices())
        self._native_unavailable_reason = str(reason)
        self._rust_backend = None
        self._gpu_backend = None
        action = (
            "recomputed current work on python_exact and disabled native tiers"
            if recomputed
            else "continued on python_exact"
        )
        message = f"Native similarity unavailable; {action}: {reason}"
        self._notices.append(message)
        logger.warning(message)

    def _run_with_python_fallback(
        self,
        backend: ExactSimilarityBackend,
        native_operation: Callable[[], object],
        python_operation: Callable[[], object],
    ):
        if backend is self.python_backend:
            return python_operation()
        try:
            return native_operation()
        except BaseException as exc:
            if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                raise
            self._disable_native(exc, recomputed=True)
            return python_operation()

    def _get_gpu_backend(self) -> GpuFirstExactBackend | None:
        if not self.gpu_enabled:
            return None
        if self._gpu_backend is not None:
            return self._gpu_backend
        rust_backend = self._get_rust_backend()
        if rust_backend is None:
            return None
        gpu_backend = (
            rust_backend
            if isinstance(rust_backend, RustExactBackend)
            and rust_backend.wgpu_supported
            else None
        )
        self._gpu_backend = GpuFirstExactBackend(
            cpu_backend=rust_backend,
            gpu_backend=gpu_backend,
            gpu_enabled=True,
            score_margin=self.score_margin,
            minimum_pairs=0,
            resident_buffers=self.resident_buffers,
        )
        return self._gpu_backend

    def _cpu_backend_for(self, pair_count: int) -> ExactSimilarityBackend:
        if pair_count < self.rust_minimum_pairs:
            return self.python_backend
        return self._get_rust_backend() or self.python_backend

    def score_pair(self, vector_a, vector_b, algorithm):
        return self.python_backend.score_pair(vector_a, vector_b, algorithm)

    def score_pairs(self, vectors, pairs, algorithm, *, workers=1):
        backend = self._cpu_backend_for(len(pairs))
        return self._run_with_python_fallback(
            backend,
            lambda: backend.score_pairs(vectors, pairs, algorithm, workers=workers),
            lambda: self.python_backend.score_pairs(
                vectors, pairs, algorithm, workers=workers
            ),
        )

    def score_all_pairs_checksum(self, vectors, algorithm, *, workers=1):
        pair_count = len(vectors) * (len(vectors) - 1) // 2
        backend = self._cpu_backend_for(pair_count)
        return self._run_with_python_fallback(
            backend,
            lambda: backend.score_all_pairs_checksum(
                vectors, algorithm, workers=workers
            ),
            lambda: self.python_backend.score_all_pairs_checksum(
                vectors, algorithm, workers=workers
            ),
        )

    def select_target_block(
        self,
        vectors,
        target_indices,
        algorithm,
        *,
        candidate_indices=None,
        workers=1,
        top_k=1000,
        min_score=0.0,
    ):
        targets = list(target_indices)
        candidates = list(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        pair_count = self._block_pair_count(targets, candidates)
        if pair_count < self.rust_minimum_pairs:
            self.python_blocks += 1
            backend = self.python_backend
        elif pair_count >= self.gpu_minimum_pairs and self.gpu_enabled:
            backend = self._get_gpu_backend()
            if backend is not None and (
                not hasattr(backend, "gpu_backend")
                or getattr(backend, "gpu_backend", None) is not None
            ):
                self.gpu_blocks += 1
            else:
                backend = self._get_rust_backend()
                if backend is not None:
                    self.rust_blocks += 1
                else:
                    backend = self.python_backend
                    self.python_blocks += 1
        else:
            backend = self._get_rust_backend()
            if backend is not None:
                self.rust_blocks += 1
            else:
                backend = self.python_backend
                self.python_blocks += 1
        if backend is self._gpu_backend:
            target_chunk_size = max(
                1,
                self.gpu_maximum_pairs_per_dispatch // max(1, len(candidates)),
            )

            def select_native():
                selected = []
                for start in range(0, len(targets), target_chunk_size):
                    self.gpu_dispatches += 1
                    selected.extend(
                        backend.select_target_block(
                            vectors,
                            targets[start : start + target_chunk_size],
                            algorithm,
                            candidate_indices=candidates,
                            workers=workers,
                            top_k=top_k,
                            min_score=min_score,
                        )
                    )
                return selected

            native_operation = select_native
        else:
            native_operation = lambda: backend.select_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                top_k=top_k,
                min_score=min_score,
            )

        python_operation = lambda: self.python_backend.select_target_block(
            vectors,
            targets,
            algorithm,
            candidate_indices=candidates,
            workers=workers,
            top_k=top_k,
            min_score=min_score,
        )
        selected = self._run_with_python_fallback(
            backend, native_operation, python_operation
        )
        if backend is not self.python_backend and self._native_unavailable_reason:
            self.python_blocks += 1
        return selected

    def summarize_target_block(
        self,
        vectors,
        target_indices,
        algorithm,
        *,
        candidate_indices=None,
        workers=1,
        nearest_k=20,
        distant_k=20,
    ):
        targets = list(target_indices)
        candidates = list(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        backend = self._cpu_backend_for(self._block_pair_count(targets, candidates))
        return self._run_with_python_fallback(
            backend,
            lambda: backend.summarize_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                nearest_k=nearest_k,
                distant_k=distant_k,
            ),
            lambda: self.python_backend.summarize_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                nearest_k=nearest_k,
                distant_k=distant_k,
            ),
        )

    def select_frequency_target_block(self, *args, **kwargs):
        return self.python_backend.select_frequency_target_block(*args, **kwargs)

    def select_inverted_target_block(
        self,
        vectors,
        target_indices,
        algorithm,
        *,
        candidate_indices=None,
        workers=1,
        max_posting_fraction=1.0,
        top_k=100,
        min_score=0.0,
    ):
        targets = list(target_indices)
        candidates = list(
            range(len(vectors)) if candidate_indices is None else candidate_indices
        )
        backend = self._cpu_backend_for(self._block_pair_count(targets, candidates))
        return self._run_with_python_fallback(
            backend,
            lambda: backend.select_inverted_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                max_posting_fraction=max_posting_fraction,
                top_k=top_k,
                min_score=min_score,
            ),
            lambda: self.python_backend.select_inverted_target_block(
                vectors,
                targets,
                algorithm,
                candidate_indices=candidates,
                workers=workers,
                max_posting_fraction=max_posting_fraction,
                top_k=top_k,
                min_score=min_score,
            ),
        )


def _cpu_backend() -> ExactSimilarityBackend:
    try:
        return RustExactBackend()
    except RuntimeError:
        return PythonExactBackend()


def _new_exact_backend(name: str) -> ExactSimilarityBackend:
    normalized = name.strip().lower()
    if normalized in {"current", "python", "python_exact"}:
        return PythonExactBackend()
    if normalized in {"rust", "rust_cpu"}:
        return RustExactBackend()
    if normalized in {"auto", "variable_silicon_drive", "vsd"}:
        from .resources import detect_performance_plan

        plan = detect_performance_plan()
        mode = os.getenv("BSIMVIS_WGPU_MODE", "auto").strip().lower()
        if mode not in {"auto", "on", "off"}:
            raise ValueError("BSIMVIS_WGPU_MODE must be 'auto', 'on', or 'off'")
        margin = float(os.getenv("BSIMVIS_WGPU_SCORE_MARGIN", "0.00001"))
        resident_mode = (
            os.getenv("BSIMVIS_WGPU_RESIDENT_BUFFERS", "auto").strip().lower()
        )
        if resident_mode not in {"auto", "on", "off"}:
            raise ValueError(
                "BSIMVIS_WGPU_RESIDENT_BUFFERS must be 'auto', 'on', or 'off'"
            )
        apple_unified_memory = (
            platform.system() == "Darwin"
            and platform.machine().lower() in {"arm64", "aarch64"}
        )
        resident_buffers = resident_mode == "on" or (
            resident_mode == "auto" and apple_unified_memory
        )
        return VariableSiliconDriveBackend(
            rust_minimum_pairs=plan.rust_minimum_pairs,
            gpu_enabled=mode == "on" or (mode == "auto" and plan.gpu_available),
            score_margin=margin,
            gpu_minimum_pairs=plan.gpu_minimum_pairs if mode == "auto" else 0,
            gpu_maximum_pairs_per_dispatch=plan.gpu_maximum_pairs_per_dispatch,
            resident_buffers=resident_buffers,
        )
    if normalized in {"gpu", "gpu_first", "wgpu_auto"}:
        return GpuFirstExactBackend(cpu_backend=_cpu_backend())
    raise ValueError(f"Unknown exact similarity backend: {name}")


_BACKEND_LOCAL = threading.local()


def get_exact_backend(name: str = "python_exact") -> ExactSimilarityBackend:
    enabled = os.getenv("BSIMVIS_EXACT_BACKEND_CACHE", "1").strip().lower() not in {
        "0",
        "false",
        "off",
    }
    if not enabled:
        return _new_exact_backend(name)
    policy = (
        name.strip().lower(),
        os.getenv("BSIMVIS_WGPU_MODE", "auto"),
        os.getenv("BSIMVIS_WGPU_RESIDENT_BUFFERS", "auto"),
        os.getenv("BSIMVIS_WGPU_MIN_PAIRS_PER_BLOCK", "750000"),
        os.getenv("BSIMVIS_WGPU_MAX_PAIRS_PER_DISPATCH", "15000000"),
        os.getenv("BSIMVIS_VSD_RUST_MIN_PAIRS_PER_BLOCK", "200000"),
        os.getenv("BSIMVIS_WGPU_SCORE_MARGIN", "0.00001"),
    )
    cache = getattr(_BACKEND_LOCAL, "backends", None)
    if cache is None:
        cache = _BACKEND_LOCAL.backends = {}
    if policy not in cache:
        cache[policy] = _new_exact_backend(name)
    return cache[policy]
