from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess


@dataclass(frozen=True)
class WorkerPlan:
    logical_cpus: int
    worker_cores: int
    heavy_workers: int
    reserved_cpus: int
    ingestion_workers: int
    physical_memory_bytes: int | None
    memory_limited_workers: int
    memory_reserve_bytes: int
    estimated_worker_bytes: int
    detection_source: str
    rationale: str

    def as_dict(self):
        return asdict(self)


@dataclass(frozen=True)
class PerformancePlan:
    worker_plan: WorkerPlan
    gpu_available: bool
    gpu_name: str | None
    gpu_backend: str | None
    gpu_detection_source: str
    exact_workers: int
    rust_minimum_pairs: int
    gpu_minimum_pairs: int
    gpu_maximum_pairs_per_dispatch: int
    wgpu_target_block_size: int
    persistence_edge_batch_size: int
    feature_enrich_chunk_size: int
    feature_enrich_write_lanes: int

    def as_dict(self):
        result = asdict(self)
        result["worker_plan"] = self.worker_plan.as_dict()
        return result


@dataclass(frozen=True)
class ResourcePressurePlan:
    pressure: str
    available_memory_bytes: int | None
    exact_workers: int
    persistence_write_lanes: int
    gpu_allowed: bool
    interactive_reserve_workers: int
    fidelity_contract: str = "exact-v1"

    def as_dict(self):
        return asdict(self)


def _available_memory_bytes() -> int | None:
    try:
        return os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None


def resolve_resource_pressure(
    performance_plan: PerformancePlan,
    *,
    available_memory_bytes: int | None = None,
    pressure: str | None = None,
) -> ResourcePressurePlan:
    available = (
        _available_memory_bytes()
        if available_memory_bytes is None
        else max(0, int(available_memory_bytes))
    )
    physical = performance_plan.worker_plan.physical_memory_bytes
    if pressure is None:
        ratio = available / physical if available is not None and physical else 1.0
        pressure = (
            "critical" if ratio < 0.10 else "constrained" if ratio < 0.20 else "normal"
        )
    if pressure not in {"normal", "constrained", "critical"}:
        raise ValueError("pressure must be normal, constrained, or critical")
    workers = performance_plan.exact_workers
    if pressure == "constrained":
        workers = max(1, workers // 2)
    elif pressure == "critical":
        workers = 1
    return ResourcePressurePlan(
        pressure=pressure,
        available_memory_bytes=available,
        exact_workers=workers,
        persistence_write_lanes=(
            performance_plan.feature_enrich_write_lanes if pressure == "normal" else 1
        ),
        gpu_allowed=performance_plan.gpu_available and pressure != "critical",
        interactive_reserve_workers=max(1, performance_plan.worker_plan.reserved_cpus),
    )


def _read_positive_int(command: list[str]) -> int | None:
    try:
        value = int(
            subprocess.check_output(
                command, text=True, stderr=subprocess.DEVNULL, timeout=2
            ).strip()
        )
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    return value if value > 0 else None


def _detect_worker_cores(logical_cpus: int) -> tuple[int, str]:
    system = platform.system()
    machine = platform.machine().lower()
    if system == "Darwin":
        if machine in {"arm64", "aarch64"}:
            performance_cores = _read_positive_int(
                ["sysctl", "-n", "hw.perflevel0.physicalcpu"]
            )
            if performance_cores:
                return min(logical_cpus, performance_cores), "apple-performance-cores"
        physical_cores = _read_positive_int(["sysctl", "-n", "hw.physicalcpu"])
        if physical_cores:
            return min(logical_cpus, physical_cores), "darwin-physical-cores"
    if system == "Linux":
        try:
            rows = subprocess.check_output(
                ["lscpu", "-p=CORE,SOCKET"],
                text=True,
                stderr=subprocess.DEVNULL,
                timeout=2,
            )
            cores = {
                line.strip()
                for line in rows.splitlines()
                if line.strip() and not line.startswith("#")
            }
            if cores:
                return min(logical_cpus, len(cores)), "linux-physical-cores"
        except (OSError, subprocess.SubprocessError):
            pass
    return logical_cpus, "logical-cpu-fallback"


def _physical_memory_bytes() -> int | None:
    if platform.system() == "Darwin":
        value = _read_positive_int(["sysctl", "-n", "hw.memsize"])
        if value:
            return value
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, OSError, ValueError):
        return None


def _platform_fingerprint(logical_cpus: int) -> str:
    identity = "|".join(
        (
            platform.system(),
            platform.machine(),
            platform.release(),
            platform.processor(),
            str(logical_cpus),
        )
    )
    return hashlib.sha256(identity.encode()).hexdigest()


def _default_cache_path() -> Path:
    cache_root = Path(os.getenv("XDG_CACHE_HOME", Path.home() / ".cache")).expanduser()
    return Path(
        os.getenv(
            "BSIMVIS_WORKER_PLAN_CACHE",
            cache_root / "bsimvis" / "worker-plan-v1.json",
        )
    )


def _default_inventory_cache_path() -> Path:
    cache_root = Path(os.getenv("XDG_CACHE_HOME", Path.home() / ".cache")).expanduser()
    return Path(
        os.getenv(
            "BSIMVIS_MACHINE_INVENTORY_CACHE",
            cache_root / "bsimvis" / "machine-inventory-v1.json",
        )
    )


def _detect_gpu() -> tuple[bool, str | None, str | None, str]:
    try:
        from bsimvis_similarity_native import wgpu_adapter_info

        info = wgpu_adapter_info()
        return True, info.get("name"), info.get("backend"), "wgpu-adapter"
    except Exception as error:
        return False, None, None, f"unavailable:{type(error).__name__}"


def _cached_gpu(
    logical_cpus: int,
    physical_memory: int | None,
    cache_path: Path | None = None,
) -> tuple[bool, str | None, str | None, str]:
    path = cache_path or _default_inventory_cache_path()
    fingerprint = f"{_platform_fingerprint(logical_cpus)}:{physical_memory or 0}"
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
        entry = cached[fingerprint]
        return (
            bool(entry["gpu_available"]),
            entry.get("gpu_name"),
            entry.get("gpu_backend"),
            "cached-" + entry["gpu_detection_source"],
        )
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        cached = {}

    available, name, backend, source = _detect_gpu()
    cached[fingerprint] = {
        "gpu_available": available,
        "gpu_name": name,
        "gpu_backend": backend,
        "gpu_detection_source": source,
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(cached, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    except OSError:
        pass
    return available, name, backend, source


def _cached_worker_cores(
    logical_cpus: int, cache_path: Path | None = None
) -> tuple[int, str]:
    path = cache_path or _default_cache_path()
    fingerprint = _platform_fingerprint(logical_cpus)
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
        entry = cached.get(fingerprint, {})
        worker_cores = int(entry["worker_cores"])
        if worker_cores > 0:
            return min(logical_cpus, worker_cores), entry["detection_source"]
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
        cached = {}

    worker_cores, source = _detect_worker_cores(logical_cpus)
    cached[fingerprint] = {
        "worker_cores": worker_cores,
        "detection_source": source,
    }
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(cached, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        temporary.replace(path)
    except OSError:
        pass
    return worker_cores, source


def detect_worker_plan(
    logical_cpus: int | None = None,
    worker_cores: int | None = None,
    cache_path: Path | None = None,
    physical_memory_bytes: int | None = None,
) -> WorkerPlan:
    detected_logical = max(1, logical_cpus or os.cpu_count() or 1)
    if worker_cores is not None:
        detected_cores = max(1, min(detected_logical, worker_cores))
        source = "explicit"
    elif logical_cpus is not None:
        system = platform.system()
        machine = platform.machine().lower()
        detected_cores = (
            max(1, detected_logical - 2)
            if system == "Darwin"
            and machine in {"arm64", "aarch64"}
            and detected_logical >= 8
            else detected_logical
        )
        source = "injected-logical-cpu-plan"
    else:
        detected_cores, source = _cached_worker_cores(detected_logical, cache_path)
    physical_memory = physical_memory_bytes or _physical_memory_bytes()
    gib = 1024**3
    estimated_worker_bytes = max(
        gib, int(float(os.getenv("BSIMVIS_HEAVY_WORKER_MEMORY_GB", "4")) * gib)
    )
    configured_reserve = max(
        gib, int(float(os.getenv("BSIMVIS_SYSTEM_MEMORY_RESERVE_GB", "16")) * gib)
    )
    memory_reserve = (
        min(configured_reserve, max(gib, physical_memory // 4))
        if physical_memory
        else configured_reserve
    )
    memory_limit = (
        max(1, (physical_memory - memory_reserve) // estimated_worker_bytes)
        if physical_memory
        else detected_cores
    )
    heavy_workers = max(1, min(detected_cores, memory_limit))
    reserved = max(0, detected_logical - heavy_workers)
    ingestion = max(1, min(4, heavy_workers // 2))
    return WorkerPlan(
        logical_cpus=detected_logical,
        worker_cores=detected_cores,
        heavy_workers=heavy_workers,
        reserved_cpus=reserved,
        ingestion_workers=ingestion,
        physical_memory_bytes=physical_memory,
        memory_limited_workers=memory_limit,
        memory_reserve_bytes=memory_reserve,
        estimated_worker_bytes=estimated_worker_bytes,
        detection_source=source,
        rationale="Assign at most one heavy worker per detected core within the memory budget.",
    )


def resolve_worker_count(value: str | int | None) -> int:
    if value is None or str(value).strip().lower() == "auto":
        return detect_worker_plan().heavy_workers
    workers = int(value)
    if workers < 1:
        raise ValueError("worker count must be positive or 'auto'")
    return workers


def _env_positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def detect_performance_plan(
    *,
    worker_plan: WorkerPlan | None = None,
    gpu_info: tuple[bool, str | None, str | None, str] | None = None,
    inventory_cache_path: Path | None = None,
) -> PerformancePlan:
    """Inventory the host once and resolve conservative, overridable defaults."""
    workers = worker_plan or detect_worker_plan()
    gpu = gpu_info or _cached_gpu(
        workers.logical_cpus,
        workers.physical_memory_bytes,
        inventory_cache_path,
    )
    gpu_available, gpu_name, gpu_backend, gpu_source = gpu
    memory_gib = (workers.physical_memory_bytes or 0) / 1024**3
    apple_unified = (
        platform.system() == "Darwin"
        and platform.machine().lower() in {"arm64", "aarch64"}
        and gpu_available
    )

    if memory_gib and memory_gib < 16:
        max_gpu_pairs, target_block, persistence_batch, feature_chunk = (
            4_000_000,
            256,
            125,
            1000,
        )
    elif memory_gib and memory_gib < 32:
        max_gpu_pairs, target_block, persistence_batch, feature_chunk = (
            8_000_000,
            512,
            250,
            2500,
        )
    else:
        max_gpu_pairs, target_block, persistence_batch, feature_chunk = (
            15_000_000,
            1024,
            250,
            5000,
        )

    default_gpu_minimum = 750_000 if apple_unified else 1_500_000
    default_write_lanes = 2 if workers.physical_memory_bytes and memory_gib >= 16 else 1
    return PerformancePlan(
        worker_plan=workers,
        gpu_available=gpu_available,
        gpu_name=gpu_name,
        gpu_backend=gpu_backend,
        gpu_detection_source=gpu_source,
        exact_workers=(
            workers.heavy_workers
            if os.getenv("BSIMVIS_EXACT_WORKERS", "auto").strip().lower() == "auto"
            else resolve_worker_count(os.getenv("BSIMVIS_EXACT_WORKERS"))
        ),
        rust_minimum_pairs=_env_positive_int(
            "BSIMVIS_VSD_RUST_MIN_PAIRS_PER_BLOCK", 200_000
        ),
        gpu_minimum_pairs=_env_positive_int(
            "BSIMVIS_WGPU_MIN_PAIRS_PER_BLOCK", default_gpu_minimum
        ),
        gpu_maximum_pairs_per_dispatch=_env_positive_int(
            "BSIMVIS_WGPU_MAX_PAIRS_PER_DISPATCH", max_gpu_pairs
        ),
        wgpu_target_block_size=_env_positive_int(
            "BSIMVIS_WGPU_TARGET_BLOCK_SIZE", target_block
        ),
        persistence_edge_batch_size=_env_positive_int(
            "BSIMVIS_EXACT_PERSISTENCE_EDGE_BATCH_SIZE", persistence_batch
        ),
        feature_enrich_chunk_size=_env_positive_int(
            "BSIMVIS_FEATURE_ENRICH_CHUNK_SIZE", feature_chunk
        ),
        feature_enrich_write_lanes=_env_positive_int(
            "BSIMVIS_FEATURE_ENRICH_NATIVE_WRITE_LANES", default_write_lanes
        ),
    )
