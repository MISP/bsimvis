"""Queued maintenance for generated similarity and cluster data."""

from flask import request

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import get_redis

job_service = JobService()

_TARGETS = {
    "function_similarity",
    "function_cluster",
    "binary_similarity",
    "binary_cluster",
}
_AXES = ("overall", "code", "library", "content")


def _files(collection, batch_uuid, md5):
    if md5:
        return [md5]
    if not batch_uuid:
        return []
    return [
        value.decode() if isinstance(value, bytes) else value
        for value in get_redis().smembers(f"{collection}:batch:{batch_uuid}:files")
    ]


def maintenance():
    """Queue clear/build/rebuild work without changing source files/functions."""
    data = request.json or {}
    collection = data.get("collection")
    operation = data.get("operation")
    targets = set(data.get("targets") or [])
    batch_uuid = data.get("batch")
    md5 = data.get("md5")
    address = data.get("address")
    algo = data.get("algo", config_service.get("similarity.algo", "unweighted_cosine"))

    if not collection:
        return {"error": "collection is required"}, 400
    if operation not in {"clear", "build", "rebuild"}:
        return {"error": "operation must be clear, build, or rebuild"}, 400
    if not targets or not targets <= _TARGETS:
        return {"error": "targets must contain supported maintenance targets"}, 400
    if address and ("binary_similarity" in targets or "binary_cluster" in targets):
        return {"error": "address is only valid for function-level targets"}, 400

    scope = {
        "collection": collection,
        "md5": md5,
        "batch_uuid": batch_uuid,
        "all": not (md5 or batch_uuid or address),
    }
    if address:
        scope["address"] = address

    # A target list is intentionally optional: no filter means collection-wide
    # maintenance, matching the existing build/clear routes.
    tasks = []
    function_scope = dict(scope)
    function_scope.update(
        {
            "algo": algo,
            "top_k": data.get("top_k", config_service.get("similarity.top_k", 20)),
            "min_score": data.get(
                "min_score", config_service.get("similarity.min_score", 0.9)
            ),
            "min_features": data.get(
                "min_features", config_service.get("similarity.min_features", 0)
            ),
        }
    )

    if "function_similarity" in targets:
        if operation in {"clear", "rebuild"}:
            clear_scope = dict(function_scope)
            clear_scope["all_algorithms"] = scope["all"]
            tasks.append((JobType.CLEAR_SIM, clear_scope))
        if operation in {"build", "rebuild"}:
            tasks.append((JobType.BUILD_SIM, dict(function_scope)))
            tasks.append((JobType.INDEX_SIM, dict(function_scope)))

    # Cluster jobs intentionally do not use rebuild_all: that route also builds
    # binary similarities, which is unrelated and expensive.
    if "function_cluster" in targets:
        if operation in {"clear", "rebuild"}:
            tasks.append(
                (
                    JobType.CLEAR_CLUSTER,
                    {
                        "collection": collection,
                        "algo": algo,
                        "all_algorithms": scope["all"],
                    },
                )
            )
        if operation in {"build", "rebuild"}:
            tasks.append(
                (
                    JobType.CLUSTER_FUNCTIONS,
                    {
                        **scope,
                        "algo": algo,
                        "min_cluster_size": data.get(
                            "min_cluster_size",
                            config_service.get("clustering.min_cluster_size", 2),
                        ),
                        "min_samples": data.get(
                            "min_samples",
                            config_service.get("clustering.min_samples", 1),
                        ),
                        "epsilon": data.get(
                            "epsilon", config_service.get("clustering.epsilon", 0.1)
                        ),
                        "selection_method": data.get(
                            "selection_method",
                            config_service.get("clustering.selection_method", "eom"),
                        ),
                        "min_sim": data.get(
                            "min_sim", config_service.get("clustering.min_sim", 0.0)
                        ),
                        "min_features": data.get(
                            "min_features",
                            config_service.get("clustering.min_features", 0),
                        ),
                    },
                )
            )

    binary_files = _files(collection, batch_uuid, md5)
    binary_scope = {"collection": collection, "algo": algo}
    if batch_uuid:
        binary_scope["batch_uuid"] = batch_uuid
    if md5:
        binary_scope["md5"] = md5

    if "binary_similarity" in targets:
        if operation in {"clear", "rebuild"}:
            for file_md5 in binary_files or [None]:
                tasks.append(
                    (
                        JobType.CLEAR_BIN_SIM,
                        {
                            "collection": collection,
                            "algo": algo,
                            "md5": file_md5,
                            "all_algorithms": scope["all"],
                        },
                    )
                )
        if operation in {"build", "rebuild"}:
            tasks.append(
                (
                    JobType.BUILD_BIN_SIM,
                    {**binary_scope, "min_cohesion": data.get("min_cohesion", 0.5)},
                )
            )

    if "binary_cluster" in targets:
        if operation in {"clear", "rebuild"}:
            tasks.extend(
                (
                    JobType.CLEAR_BIN_CLUSTER,
                    {
                        "collection": collection,
                        "algo": algo,
                        "all_algorithms": scope["all"],
                        "axis": axis,
                    },
                )
                for axis in _AXES
            )
        if operation in {"build", "rebuild"}:
            tasks.extend(
                (
                    JobType.CLUSTER_BINARIES,
                    {
                        "collection": collection,
                        "algo": algo,
                        "axis": axis,
                        "min_cohesion": data.get("min_cohesion", 0.5),
                    },
                )
                for axis in _AXES
            )

    pipeline_id = job_service.submit_to_lane(collection, tasks)
    return {"job_id": pipeline_id, "pipeline_id": pipeline_id, "status": "enqueued"}
