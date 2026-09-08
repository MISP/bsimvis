from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import re
import uuid

GENERATION_PATTERN = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")


class SimilarityGenerationManager:
    _ACTIVATE_SCRIPT = """
local status = redis.call('HGET', KEYS[1], 'status')
if status ~= 'building' then return -1 end
local current = redis.call('GET', KEYS[2])
if not current then current = ARGV[2] end
if current ~= ARGV[1] then return -2 end
redis.call('HSET', KEYS[1],
  'status', 'active',
  'activated_at', ARGV[4],
  'built_targets', ARGV[5],
  'edge_count', ARGV[6])
redis.call('SET', KEYS[2], ARGV[3])
return 1
"""
    _SEAL_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'building' then return -1 end
local existing = redis.call('HGET', KEYS[1], 'sealed_output')
if existing then
  if existing == ARGV[1] then return 0 end
  return -2
end
redis.call('HSET', KEYS[1],
  'sealed_output', ARGV[1],
  'output_targets', ARGV[2],
  'edge_count', ARGV[3],
  'delta_items', ARGV[4],
  'sealed_at', ARGV[5])
return 1
"""
    _CHECKPOINT_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'building' then return -1 end
if redis.call('SADD', KEYS[2], ARGV[1]) == 0 then return 0 end
redis.call('HINCRBY', KEYS[1], 'checkpointed_partitions', 1)
redis.call('HINCRBY', KEYS[1], 'checkpointed_items', ARGV[2])
redis.call('HINCRBY', KEYS[1], 'checkpointed_bytes', ARGV[3])
redis.call('HSET', KEYS[1], 'checkpointed_at', ARGV[4])
return 1
"""
    _JOURNAL_SCRIPT = """
if redis.call('HGET', KEYS[1], 'status') ~= 'building' then return -1 end
local existing = redis.call('HGET', KEYS[2], ARGV[1])
if existing then
  if existing == ARGV[2] then return 0 end
  return -2
end
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[1], 'journal_entries', 1)
redis.call('HSET', KEYS[1],
  'journal_head', ARGV[3],
  'journaled_at', ARGV[4])
return 1
"""

    def __init__(self, redis_client):
        self.r = redis_client

    @staticmethod
    def storage_algorithm(algorithm: str, generation_id: str) -> str:
        if not GENERATION_PATTERN.fullmatch(generation_id):
            raise ValueError("invalid similarity generation ID")
        return f"{algorithm}@{generation_id}"

    @staticmethod
    def logical_algorithm(storage_algorithm: str) -> str:
        return storage_algorithm.split("@", 1)[0]

    @staticmethod
    def _active_key(collection: str, algorithm: str) -> str:
        return f"{collection}:sim:active:{algorithm}"

    @staticmethod
    def _manifest_key(collection: str, algorithm: str, generation_id: str) -> str:
        return f"{collection}:sim:generation:{algorithm}:{generation_id}"

    def resolve(self, collection: str, algorithm: str) -> str:
        return self.r.get(self._active_key(collection, algorithm)) or algorithm

    def begin(
        self, collection: str, algorithm: str, parameters=None, generation_id=None
    ):
        generation_id = generation_id or uuid.uuid4().hex
        storage_algorithm = self.storage_algorithm(algorithm, generation_id)
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        if self.r.exists(manifest_key):
            manifest = self.r.hgetall(manifest_key)
            if (
                manifest.get("status") == "building"
                and manifest.get("storage_algorithm") == storage_algorithm
            ):
                return {
                    "generation_id": generation_id,
                    "storage_algorithm": storage_algorithm,
                    "previous_storage_algorithm": manifest.get(
                        "previous_storage_algorithm", algorithm
                    ),
                    "manifest_key": manifest_key,
                }
            raise ValueError(f"similarity generation already exists: {generation_id}")
        previous = self.resolve(collection, algorithm)
        self.r.hset(
            manifest_key,
            mapping={
                "generation_id": generation_id,
                "collection": collection,
                "algorithm": algorithm,
                "storage_algorithm": storage_algorithm,
                "previous_storage_algorithm": previous,
                "status": "building",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "parameters": json.dumps(parameters or {}, sort_keys=True),
            },
        )
        self.r.sadd(f"{collection}:sim:generations:{algorithm}", generation_id)
        return {
            "generation_id": generation_id,
            "storage_algorithm": storage_algorithm,
            "previous_storage_algorithm": previous,
            "manifest_key": manifest_key,
        }

    def status(self, collection: str, algorithm: str, generation_id: str):
        manifest = self.r.hgetall(
            self._manifest_key(collection, algorithm, generation_id)
        )
        if not manifest:
            return None
        manifest["is_active"] = self.resolve(collection, algorithm) == manifest.get(
            "storage_algorithm"
        )
        return manifest

    def checkpoint(
        self,
        collection: str,
        algorithm: str,
        generation_id: str,
        partition_id: str,
        *,
        items: int,
        bytes_written: int,
    ) -> bool:
        self.storage_algorithm(algorithm, generation_id)
        if not GENERATION_PATTERN.fullmatch(partition_id):
            raise ValueError("invalid generation partition ID")
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        result = self.r.eval(
            self._CHECKPOINT_SCRIPT,
            2,
            manifest_key,
            f"{manifest_key}:checkpoint_partitions",
            partition_id,
            max(0, int(items)),
            max(0, int(bytes_written)),
            datetime.now(timezone.utc).isoformat(),
        )
        if result == -1:
            raise ValueError("similarity generation is not accepting checkpoints")
        return result == 1

    def append_journal(
        self,
        collection: str,
        algorithm: str,
        generation_id: str,
        event_id: str,
        event: dict,
    ) -> bool:
        self.storage_algorithm(algorithm, generation_id)
        if not GENERATION_PATTERN.fullmatch(event_id):
            raise ValueError("invalid generation journal event ID")
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(f"{event_id}:{payload}".encode()).hexdigest()
        result = self.r.eval(
            self._JOURNAL_SCRIPT,
            2,
            manifest_key,
            f"{manifest_key}:journal",
            event_id,
            payload,
            digest,
            datetime.now(timezone.utc).isoformat(),
        )
        if result == -1:
            raise ValueError("similarity generation is not accepting journal entries")
        if result == -2:
            raise ValueError(f"generation journal conflict: {event_id}")
        return result == 1

    def journal(
        self, collection: str, algorithm: str, generation_id: str
    ) -> list[dict]:
        self.storage_algorithm(algorithm, generation_id)
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        entries = self.r.hgetall(f"{manifest_key}:journal")
        return [
            {"event_id": event_id, **json.loads(payload)}
            for event_id, payload in sorted(entries.items())
        ]

    def seal_incremental(
        self,
        collection: str,
        algorithm: str,
        generation_id: str,
        *,
        output_targets: int,
        edge_count: int,
        delta_items: int,
    ) -> bool:
        self.storage_algorithm(algorithm, generation_id)
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        output = {
            "delta_items": max(0, int(delta_items)),
            "edge_count": max(0, int(edge_count)),
            "output_targets": max(0, int(output_targets)),
        }
        payload = json.dumps(output, sort_keys=True, separators=(",", ":"))
        result = self.r.eval(
            self._SEAL_SCRIPT,
            1,
            manifest_key,
            payload,
            output["output_targets"],
            output["edge_count"],
            output["delta_items"],
            datetime.now(timezone.utc).isoformat(),
        )
        if result == -1:
            raise ValueError("similarity generation cannot be sealed")
        if result == -2:
            raise ValueError("similarity generation sealed output conflict")
        return result == 1

    def completed_partitions(
        self, collection: str, algorithm: str, generation_id: str
    ) -> set[str]:
        self.storage_algorithm(algorithm, generation_id)
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        return set(self.r.smembers(f"{manifest_key}:checkpoint_partitions"))

    def activate(
        self, collection: str, algorithm: str, generation_id: str, expected_targets: int
    ):
        storage_algorithm = self.storage_algorithm(algorithm, generation_id)
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        manifest = self.r.hgetall(manifest_key)
        if not manifest or manifest.get("status") != "building":
            raise ValueError("similarity generation is not awaiting activation")
        parameters = json.loads(manifest.get("parameters") or "{}")
        incremental = parameters.get("compact_mode") == "incremental_working"
        sealed_compact = parameters.get("compact_mode") in {
            "incremental_working",
            "compact_full_working",
        }
        built_targets = (
            int(manifest.get("output_targets") or -1)
            if sealed_compact
            else self.r.scard(f"{collection}:built:functions:{storage_algorithm}")
        )
        if built_targets != expected_targets:
            raise ValueError(
                f"generation incomplete: {built_targets}/{expected_targets} targets"
            )
        edge_count = (
            int(manifest.get("edge_count") or -1)
            if sealed_compact
            else self.r.zcard(f"{collection}:sim:score:{storage_algorithm}")
        )
        compact_required = parameters.get("compact_mode") in {
            "dual",
            "incremental_working",
            "compact_full_working",
        }
        if compact_required:
            completed = self.completed_partitions(collection, algorithm, generation_id)
            if "compact_complete" not in completed:
                raise ValueError(
                    "compact generation incomplete: final checkpoint missing"
                )
            checkpointed_edges = int(manifest.get("checkpointed_items") or 0)
            expected_checkpoint_items = (
                int(manifest.get("delta_items") or -1) if sealed_compact else edge_count
            )
            if checkpointed_edges != expected_checkpoint_items:
                raise ValueError(
                    "compact generation incomplete: checkpoint recorded "
                    f"{checkpointed_edges}/{expected_checkpoint_items} edges"
                )
            prefixes = [f"{collection}:sim:compact:{storage_algorithm}"]
            if sealed_compact:
                if (
                    not manifest.get("sealed_output")
                    or int(manifest.get("journal_entries") or 0) < 1
                ):
                    raise ValueError(
                        "incremental generation is not sealed and journaled"
                    )
                if incremental:
                    prefixes.append(
                        f"{collection}:sim:compact:{storage_algorithm}.removed"
                    )
            partition_ids = set()
            digests = {}
            counts = {}
            for prefix in prefixes:
                ids = set(self.r.smembers(f"{prefix}:partitions"))
                partition_ids.update((prefix, item) for item in ids)
                digests.update(
                    {
                        (prefix, key): value
                        for key, value in self.r.hgetall(f"{prefix}:digests").items()
                    }
                )
                counts.update(
                    {
                        (prefix, key): value
                        for key, value in self.r.hgetall(
                            f"{prefix}:edge_counts"
                        ).items()
                    }
                )
            if expected_checkpoint_items and not partition_ids:
                raise ValueError("compact generation incomplete: no partitions")
            if not partition_ids.issubset(digests) or not partition_ids.issubset(
                counts
            ):
                raise ValueError(
                    "compact generation incomplete: partition metadata missing"
                )
            compact_edges = sum(int(counts[partition]) for partition in partition_ids)
            if compact_edges != expected_checkpoint_items:
                raise ValueError(
                    "compact generation incomplete: "
                    f"{compact_edges}/{expected_checkpoint_items} edges"
                )
        activated_at = datetime.now(timezone.utc).isoformat()
        active_key = self._active_key(collection, algorithm)
        expected_previous = manifest.get("previous_storage_algorithm") or algorithm
        result = self.r.eval(
            self._ACTIVATE_SCRIPT,
            2,
            manifest_key,
            active_key,
            expected_previous,
            algorithm,
            storage_algorithm,
            activated_at,
            built_targets,
            edge_count,
        )
        if result == -1:
            raise ValueError("similarity generation is not awaiting activation")
        if result == -2:
            raise ValueError("active similarity generation changed during build")
        return {
            "generation_id": generation_id,
            "storage_algorithm": storage_algorithm,
            "built_targets": built_targets,
            "edge_count": edge_count,
            "compact_verified": compact_required,
        }

    def rollback(self, collection: str, algorithm: str, generation_id: str):
        manifest_key = self._manifest_key(collection, algorithm, generation_id)
        manifest = self.r.hgetall(manifest_key)
        if not manifest:
            raise ValueError("similarity generation does not exist")
        previous = manifest.get("previous_storage_algorithm") or algorithm
        pipe = self.r.pipeline(transaction=True)
        if previous == algorithm:
            pipe.delete(self._active_key(collection, algorithm))
        else:
            pipe.set(self._active_key(collection, algorithm), previous)
        pipe.hset(
            manifest_key,
            mapping={
                "status": "rolled_back",
                "rolled_back_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        pipe.execute()
        return previous

    def garbage_collect_compact(
        self,
        collection: str,
        algorithm: str,
        *,
        keep_latest: int = 2,
        dry_run: bool = True,
    ):
        keep_latest = max(1, min(100, int(keep_latest)))
        generation_ids = self.r.smembers(f"{collection}:sim:generations:{algorithm}")
        generations = []
        for generation_id in generation_ids:
            manifest_key = self._manifest_key(collection, algorithm, generation_id)
            manifest = self.r.hgetall(manifest_key)
            if manifest.get("status") not in {"active", "rolled_back"}:
                continue
            generations.append(
                {
                    "generation_id": generation_id,
                    "manifest_key": manifest_key,
                    "storage_algorithm": manifest.get("storage_algorithm"),
                    "previous_storage_algorithm": manifest.get(
                        "previous_storage_algorithm"
                    ),
                    "timestamp": manifest.get("activated_at")
                    or manifest.get("rolled_back_at")
                    or manifest.get("created_at")
                    or "",
                }
            )
        generations.sort(
            key=lambda item: (item["timestamp"], item["generation_id"]), reverse=True
        )
        active_storage = self.resolve(collection, algorithm)
        active_manifest = next(
            (
                generation
                for generation in generations
                if generation["storage_algorithm"] == active_storage
            ),
            None,
        )
        protected_storage = {active_storage}
        if active_manifest and active_manifest["previous_storage_algorithm"]:
            protected_storage.add(active_manifest["previous_storage_algorithm"])
        protected_storage.update(
            generation["storage_algorithm"] for generation in generations[:keep_latest]
        )
        candidates = [
            generation
            for generation in generations
            if generation["storage_algorithm"] not in protected_storage
        ]
        plan = []
        for generation in candidates:
            prefix = f"{collection}:sim:compact:{generation['storage_algorithm']}"
            cursor = 0
            keys = []
            while True:
                cursor, batch = self.r.scan(
                    cursor=cursor, match=f"{prefix}:*", count=1000
                )
                keys.extend(batch)
                if cursor == 0:
                    break
            plan.append({**generation, "compact_keys": sorted(keys)})
        if not dry_run:
            collected_at = datetime.now(timezone.utc).isoformat()
            for generation in plan:
                for start in range(0, len(generation["compact_keys"]), 500):
                    self.r.delete(*generation["compact_keys"][start : start + 500])
                self.r.hset(
                    generation["manifest_key"],
                    mapping={
                        "status": "compact_garbage_collected",
                        "compact_garbage_collected_at": collected_at,
                        "compact_garbage_collected_keys": len(
                            generation["compact_keys"]
                        ),
                    },
                )
        return {
            "dry_run": bool(dry_run),
            "active_storage_algorithm": active_storage,
            "keep_latest": keep_latest,
            "protected_storage_algorithms": sorted(protected_storage),
            "generation_count": len(plan),
            "compact_key_count": sum(len(item["compact_keys"]) for item in plan),
            "generations": plan,
        }
