from __future__ import annotations

import base64
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import json
import math
import struct
import sys
import threading

MAGIC = b"BSC1"
NUMERIC_MAGIC = b"BSC2"
FUNCTION_TABLE_MAGIC = b"BSD1"
SCORE_SCALE = 10_000


def _encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint values must be non-negative")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while True:
        if offset >= len(data) or shift > 63:
            raise ValueError("invalid compact similarity varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, offset
        shift += 7


def encode_edges(edges) -> bytes:
    normalized = [
        (str(left), str(right), round(float(score), 4)) for left, right, score in edges
    ]
    function_ids = sorted(
        {function_id for edge in normalized for function_id in edge[:2]}
    )
    indexes = {function_id: index for index, function_id in enumerate(function_ids)}
    output = bytearray(MAGIC)
    output.extend(_encode_varint(len(function_ids)))
    for function_id in function_ids:
        encoded = function_id.encode()
        output.extend(_encode_varint(len(encoded)))
        output.extend(encoded)
    output.extend(_encode_varint(len(normalized)))
    for left, right, score in normalized:
        output.extend(_encode_varint(indexes[left]))
        output.extend(_encode_varint(indexes[right]))
        output.extend(struct.pack("<H", round(score * SCORE_SCALE)))
    return bytes(output)


def decode_edges(payload: bytes):
    if not payload.startswith(MAGIC):
        raise ValueError("unsupported compact similarity payload")
    offset = len(MAGIC)
    function_count, offset = _decode_varint(payload, offset)
    function_ids = []
    for _ in range(function_count):
        length, offset = _decode_varint(payload, offset)
        end = offset + length
        if end > len(payload):
            raise ValueError("truncated compact similarity function table")
        function_ids.append(payload[offset:end].decode())
        offset = end
    edge_count, offset = _decode_varint(payload, offset)
    edges = []
    for _ in range(edge_count):
        left, offset = _decode_varint(payload, offset)
        right, offset = _decode_varint(payload, offset)
        if (
            left >= function_count
            or right >= function_count
            or offset + 2 > len(payload)
        ):
            raise ValueError("invalid compact similarity edge")
        score = struct.unpack_from("<H", payload, offset)[0] / SCORE_SCALE
        offset += 2
        edges.append((function_ids[left], function_ids[right], score))
    if offset != len(payload):
        raise ValueError("trailing compact similarity data")
    return edges


def encode_ascii(edges) -> str:
    return base64.b85encode(encode_edges(edges)).decode()


def decode_ascii(payload: str):
    return decode_edges(base64.b85decode(payload.encode()))


def neighbors(payload: bytes, function_id: str):
    result = []
    for left, right, score in decode_edges(payload):
        if left == function_id:
            result.append((right, score))
        elif right == function_id:
            result.append((left, score))
    return sorted(result, key=lambda item: (-item[1], item[0]))


def encode_function_table(function_ids) -> tuple[bytes, tuple[str, ...]]:
    ordered = tuple(sorted(str(function_id) for function_id in function_ids))
    if len(set(ordered)) != len(ordered):
        raise ValueError("compact generation function IDs must be unique")
    payload = bytearray(FUNCTION_TABLE_MAGIC)
    payload.extend(_encode_varint(len(ordered)))
    for function_id in ordered:
        encoded = function_id.encode()
        payload.extend(_encode_varint(len(encoded)))
        payload.extend(encoded)
    return bytes(payload), ordered


def decode_function_table(payload: bytes) -> tuple[str, ...]:
    if not payload.startswith(FUNCTION_TABLE_MAGIC):
        raise ValueError("unsupported compact function table")
    offset = len(FUNCTION_TABLE_MAGIC)
    count, offset = _decode_varint(payload, offset)
    function_ids = []
    for _ in range(count):
        length, offset = _decode_varint(payload, offset)
        end = offset + length
        if end > len(payload):
            raise ValueError("truncated compact function table")
        function_ids.append(payload[offset:end].decode())
        offset = end
    if offset != len(payload):
        raise ValueError("trailing compact function table data")
    if len(set(function_ids)) != len(function_ids):
        raise ValueError("compact function table contains duplicate IDs")
    return tuple(function_ids)


def encode_numeric_edges(edges, function_indexes) -> bytes:
    by_pair = {}
    for left, right, score in edges:
        try:
            left_index = int(function_indexes[str(left)])
            right_index = int(function_indexes[str(right)])
        except KeyError as error:
            raise ValueError(f"unknown compact function ID: {error.args[0]}") from None
        if left_index == right_index:
            continue
        pair = (min(left_index, right_index), max(left_index, right_index))
        quantized = round(round(float(score), 4) * SCORE_SCALE)
        if not 0 <= quantized <= 65_535:
            raise ValueError("compact similarity score is out of range")
        if pair in by_pair and by_pair[pair] != quantized:
            raise ValueError(f"conflicting compact edge score: {pair}")
        by_pair[pair] = quantized

    canonical = sorted(by_pair.items())
    payload = bytearray(NUMERIC_MAGIC)
    payload.extend(_encode_varint(len(canonical)))
    previous_left = 0
    previous_right = None
    for (left, right), score in canonical:
        left_delta = left - previous_left
        payload.extend(_encode_varint(left_delta))
        if previous_right is not None and left_delta == 0:
            payload.extend(_encode_varint(right - previous_right - 1))
        else:
            payload.extend(_encode_varint(right - left - 1))
        payload.extend(struct.pack("<H", score))
        previous_left = left
        previous_right = right
    return bytes(payload)


def decode_numeric_edges(payload: bytes, function_ids):
    if not payload.startswith(NUMERIC_MAGIC):
        raise ValueError("unsupported numeric compact similarity payload")
    function_ids = tuple(function_ids)
    offset = len(NUMERIC_MAGIC)
    edge_count, offset = _decode_varint(payload, offset)
    edges = []
    previous_left = 0
    previous_right = None
    for _ in range(edge_count):
        left_delta, offset = _decode_varint(payload, offset)
        right_gap, offset = _decode_varint(payload, offset)
        left = previous_left + left_delta
        if previous_right is not None and left_delta == 0:
            right = previous_right + right_gap + 1
        else:
            right = left + right_gap + 1
        if right >= len(function_ids) or offset + 2 > len(payload):
            raise ValueError("invalid numeric compact similarity edge")
        score = struct.unpack_from("<H", payload, offset)[0] / SCORE_SCALE
        offset += 2
        edges.append((function_ids[left], function_ids[right], score))
        previous_left = left
        previous_right = right
    if offset != len(payload):
        raise ValueError("trailing numeric compact similarity data")
    return edges


@dataclass(frozen=True)
class CompactWriteResult:
    partition_id: str
    edge_count: int
    encoded_bytes: int
    sha256: str
    created: bool


class CompactSimilarityStore:
    """Versioned append-only compact partitions with idempotent writes."""

    def __init__(self, redis_client, collection: str, storage_algorithm: str):
        self.r = redis_client
        self.collection = collection
        self.storage_algorithm = storage_algorithm
        self.prefix = f"{collection}:sim:compact:{storage_algorithm}"

    def _partition_key(self, partition_id):
        return f"{self.prefix}:partition:{partition_id}"

    @property
    def partition_registry_key(self):
        return f"{self.prefix}:partitions"

    @property
    def digest_key(self):
        return f"{self.prefix}:digests"

    @property
    def count_key(self):
        return f"{self.prefix}:edge_counts"

    def prepare_partition(self, partition_id: str, edges):
        if not partition_id or any(character in partition_id for character in ":/\n"):
            raise ValueError("invalid compact partition ID")
        by_pair = {}
        for left, right, score in edges:
            left, right = str(left), str(right)
            if left == right:
                continue
            pair = (min(left, right), max(left, right))
            rounded = round(float(score), 4)
            if pair in by_pair and by_pair[pair] != rounded:
                raise ValueError(f"conflicting compact edge score: {pair}")
            by_pair[pair] = rounded
        canonical = [
            (left, right, by_pair[(left, right)]) for left, right in sorted(by_pair)
        ]
        encoded = encode_ascii(canonical)
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        return encoded, digest, len(canonical)

    def write_partition(self, partition_id: str, edges) -> CompactWriteResult:
        encoded, digest, edge_count = self.prepare_partition(partition_id, edges)
        partition_key = self._partition_key(partition_id)
        created = bool(self.r.set(partition_key, encoded, nx=True))
        if not created:
            current = self.r.get(partition_key)
            current = current.decode() if isinstance(current, bytes) else current
            if current != encoded:
                raise ValueError(f"compact partition conflict: {partition_id}")
        pipe = self.r.pipeline(transaction=False)
        pipe.hset(self.digest_key, partition_id, digest)
        pipe.sadd(self.partition_registry_key, partition_id)
        pipe.hset(self.count_key, partition_id, edge_count)
        pipe.execute()
        return CompactWriteResult(
            partition_id=partition_id,
            edge_count=edge_count,
            encoded_bytes=len(encoded),
            sha256=digest,
            created=created,
        )

    def read_partition(self, partition_id: str):
        payload = self.r.get(self._partition_key(partition_id))
        if payload is None:
            raise KeyError(partition_id)
        return decode_ascii(payload.decode() if isinstance(payload, bytes) else payload)

    def partition_ids(self):
        return sorted(
            item.decode() if isinstance(item, bytes) else item
            for item in self.r.smembers(self.partition_registry_key)
        )

    def read_all(self):
        edges = {}
        for partition_id in self.partition_ids():
            for left, right, score in self.read_partition(partition_id):
                edges[(left, right)] = score
        return [(left, right, score) for (left, right), score in sorted(edges.items())]

    def verify_edges(self, eager_edges):
        expected = sorted(
            (
                min(str(left), str(right)),
                max(str(left), str(right)),
                round(float(score), 4),
            )
            for left, right, score in eager_edges
        )
        actual = self.read_all()
        return {
            "compatible": actual == expected,
            "eager_edges": len(expected),
            "compact_edges": len(actual),
            "eager_sha256": hashlib.sha256(repr(expected).encode()).hexdigest(),
            "compact_sha256": hashlib.sha256(repr(actual).encode()).hexdigest(),
        }

    @staticmethod
    def expanded_json_bytes(edges):
        return len(
            json.dumps(
                [
                    {"id1": left, "id2": right, "score": round(float(score), 4)}
                    for left, right, score in edges
                ],
                separators=(",", ":"),
            ).encode()
        )


class NumericCompactSimilarityStore:
    """Binary compact partitions sharing one immutable generation ID table."""

    def __init__(self, redis_client, collection: str, storage_algorithm: str):
        self.r = redis_client
        self.collection = collection
        self.storage_algorithm = storage_algorithm
        self.prefix = f"{collection}:sim:compact2:{storage_algorithm}"
        self._function_ids = None
        self._function_indexes = None

    def _partition_key(self, partition_id):
        return f"{self.prefix}:partition:{partition_id}"

    @property
    def function_table_key(self):
        return f"{self.prefix}:function_ids"

    @property
    def partition_registry_key(self):
        return f"{self.prefix}:partitions"

    @property
    def digest_key(self):
        return f"{self.prefix}:digests"

    @property
    def count_key(self):
        return f"{self.prefix}:edge_counts"

    def write_function_ids(self, function_ids):
        payload, ordered = encode_function_table(function_ids)
        created = bool(self.r.set(self.function_table_key, payload, nx=True))
        if not created and self.r.get(self.function_table_key) != payload:
            raise ValueError("compact generation function table conflict")
        self._function_ids = ordered
        self._function_indexes = {
            function_id: index for index, function_id in enumerate(ordered)
        }
        return {
            "created": created,
            "function_count": len(ordered),
            "encoded_bytes": len(payload),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    def read_function_ids(self):
        if self._function_ids is None:
            payload = self.r.get(self.function_table_key)
            if payload is None:
                raise KeyError("compact generation function table")
            if isinstance(payload, str):
                payload = payload.encode("latin1")
            self._function_ids = decode_function_table(payload)
            self._function_indexes = {
                function_id: index
                for index, function_id in enumerate(self._function_ids)
            }
        return self._function_ids

    def prepare_partition(self, partition_id: str, edges):
        if not partition_id or any(character in partition_id for character in ":/\n"):
            raise ValueError("invalid compact partition ID")
        self.read_function_ids()
        payload = encode_numeric_edges(edges, self._function_indexes)
        digest = hashlib.sha256(payload).hexdigest()
        edge_count, _ = _decode_varint(payload, len(NUMERIC_MAGIC))
        return payload, digest, edge_count

    def write_partition(self, partition_id: str, edges) -> CompactWriteResult:
        payload, digest, edge_count = self.prepare_partition(partition_id, edges)
        partition_key = self._partition_key(partition_id)
        created = bool(self.r.set(partition_key, payload, nx=True))
        if not created and self.r.get(partition_key) != payload:
            raise ValueError(f"compact partition conflict: {partition_id}")
        pipe = self.r.pipeline(transaction=False)
        pipe.hset(self.digest_key, partition_id, digest)
        pipe.sadd(self.partition_registry_key, partition_id)
        pipe.hset(self.count_key, partition_id, edge_count)
        pipe.execute()
        return CompactWriteResult(
            partition_id=partition_id,
            edge_count=edge_count,
            encoded_bytes=len(payload),
            sha256=digest,
            created=created,
        )

    def read_partition(self, partition_id: str):
        payload = self.r.get(self._partition_key(partition_id))
        if payload is None:
            raise KeyError(partition_id)
        if isinstance(payload, str):
            payload = payload.encode("latin1")
        return decode_numeric_edges(payload, self.read_function_ids())

    def partition_ids(self):
        return sorted(
            item.decode() if isinstance(item, bytes) else item
            for item in self.r.smembers(self.partition_registry_key)
        )

    def read_all(self):
        edges = {}
        for partition_id in self.partition_ids():
            for left, right, score in self.read_partition(partition_id):
                edges[(left, right)] = score
        return [(left, right, score) for (left, right), score in sorted(edges.items())]

    def verify_edges(self, eager_edges):
        expected = sorted(
            (
                min(str(left), str(right)),
                max(str(left), str(right)),
                round(float(score), 4),
            )
            for left, right, score in eager_edges
        )
        actual = self.read_all()
        return {
            "compatible": actual == expected,
            "eager_edges": len(expected),
            "compact_edges": len(actual),
            "eager_sha256": hashlib.sha256(repr(expected).encode()).hexdigest(),
            "compact_sha256": hashlib.sha256(repr(actual).encode()).hexdigest(),
        }


class CompactSimilarityOverlayStore:
    """Exact immutable overlay with a persistent, restart-safe materialized view."""

    def __init__(
        self,
        base_store,
        addition_store,
        removal_store,
        *,
        materialize=True,
        materialized_partition_size=4096,
    ):
        self.base_store = base_store
        self.addition_store = addition_store
        self.removal_store = removal_store
        self.materialize = bool(materialize)
        self.materialized_partition_size = max(1, int(materialized_partition_size))
        self.materialized_store = CompactSimilarityStore(
            addition_store.r,
            addition_store.collection,
            f"{addition_store.storage_algorithm}.materialized",
        )
        self.materialized_complete_key = f"{self.materialized_store.prefix}:complete"

    def partition_ids(self):
        return sorted(
            set(self.base_store.partition_ids())
            | set(self.addition_store.partition_ids())
            | set(self.removal_store.partition_ids())
        )

    def read_all(self):
        if self.materialize and self.addition_store.r.get(
            self.materialized_complete_key
        ):
            return self.materialized_store.read_all()
        edges = self._merge_all()
        if self.materialize:
            for offset in range(0, len(edges), self.materialized_partition_size):
                self.materialized_store.write_partition(
                    f"materialized_{offset // self.materialized_partition_size:06d}",
                    edges[offset : offset + self.materialized_partition_size],
                )
            marker = json.dumps(
                {
                    "edge_count": len(edges),
                    "sha256": hashlib.sha256(repr(edges).encode()).hexdigest(),
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            self.addition_store.r.set(self.materialized_complete_key, marker, nx=True)
        return edges

    def _merge_all(self):
        edges = {
            (left, right): score for left, right, score in self.base_store.read_all()
        }
        for left, right, _score in self.removal_store.read_all():
            edges.pop((left, right), None)
        for left, right, score in self.addition_store.read_all():
            edges[(left, right)] = score
        return [(left, right, score) for (left, right), score in sorted(edges.items())]


def resolve_compact_store(
    redis_client,
    generation_manager,
    collection,
    algorithm,
    storage_algorithm=None,
    *,
    max_depth=32,
    materialize=True,
):
    """Resolve an immutable compact generation, including nested overlays."""
    storage_algorithm = storage_algorithm or generation_manager.resolve(
        collection, algorithm
    )
    current = storage_algorithm
    lineage = set()
    overlays = []
    while "@" in current:
        if current in lineage or len(overlays) >= max(1, int(max_depth)):
            raise ValueError("compact generation lineage is cyclic or too deep")
        lineage.add(current)
        generation_id = current.rsplit("@", 1)[1]
        manifest = generation_manager.status(collection, algorithm, generation_id) or {}
        parameters = json.loads(manifest.get("parameters") or "{}")
        if parameters.get("compact_mode") != "incremental_working":
            break
        overlays.append(current)
        current = manifest.get("previous_storage_algorithm") or algorithm
    store = CompactSimilarityStore(redis_client, collection, current)
    for overlay in reversed(overlays):
        store = CompactSimilarityOverlayStore(
            store,
            CompactSimilarityStore(redis_client, collection, overlay),
            CompactSimilarityStore(redis_client, collection, f"{overlay}.removed"),
            materialize=materialize,
        )
    return store


def compact_overlay_depth(generation_manager, collection, algorithm) -> int:
    current = generation_manager.resolve(collection, algorithm)
    seen = set()
    depth = 0
    while "@" in current:
        if current in seen:
            raise ValueError("compact generation lineage is cyclic")
        seen.add(current)
        generation_id = current.rsplit("@", 1)[1]
        manifest = generation_manager.status(collection, algorithm, generation_id) or {}
        parameters = json.loads(manifest.get("parameters") or "{}")
        if parameters.get("compact_mode") != "incremental_working":
            break
        depth += 1
        current = manifest.get("previous_storage_algorithm") or algorithm
    return depth


def compact_generation_if_needed(
    redis_client,
    generation_manager,
    collection,
    algorithm,
    generation_id,
    *,
    max_overlay_depth=3,
    partition_size=4096,
):
    """Fold an active overlay chain into one immutable compact generation."""
    depth = compact_overlay_depth(generation_manager, collection, algorithm)
    if depth < max(1, int(max_overlay_depth)):
        return {"compacted": False, "overlay_depth": depth}
    source = generation_manager.resolve(collection, algorithm)
    source_generation = source.rsplit("@", 1)[1]
    source_manifest = (
        generation_manager.status(collection, algorithm, source_generation) or {}
    )
    target_count = int(
        source_manifest.get("output_targets")
        or source_manifest.get("built_targets")
        or 0
    )
    edges = resolve_compact_store(
        redis_client,
        generation_manager,
        collection,
        algorithm,
        source,
    ).read_all()
    generation = generation_manager.begin(
        collection,
        algorithm,
        {
            "compact_mode": "compact_full_working",
            "update_kind": "overlay_compaction",
            "source_storage_algorithm": source,
            "source_overlay_depth": depth,
        },
        generation_id,
    )
    journal_created = generation_manager.append_journal(
        collection,
        algorithm,
        generation_id,
        "compaction",
        {"kind": "overlay_compaction", "source": source, "depth": depth},
    )
    store = CompactSimilarityStore(
        redis_client, collection, generation["storage_algorithm"]
    )
    partition_size = max(1, int(partition_size))
    created = 0
    encoded_bytes = 0
    for offset in range(0, len(edges), partition_size):
        result = store.write_partition(
            f"compacted_{offset // partition_size:06d}",
            edges[offset : offset + partition_size],
        )
        created += int(result.created)
        encoded_bytes += result.encoded_bytes
    checkpoint_created = generation_manager.checkpoint(
        collection,
        algorithm,
        generation_id,
        "compact_complete",
        items=len(edges),
        bytes_written=encoded_bytes,
    )
    sealed = generation_manager.seal_incremental(
        collection,
        algorithm,
        generation_id,
        output_targets=target_count,
        edge_count=len(edges),
        delta_items=len(edges),
    )
    activated = generation_manager.activate(
        collection, algorithm, generation_id, expected_targets=target_count
    )
    return {
        **generation,
        **activated,
        "compacted": True,
        "source_storage_algorithm": source,
        "source_overlay_depth": depth,
        "edge_count": len(edges),
        "encoded_bytes": encoded_bytes,
        "partitions_created": created,
        "journal_created": journal_created,
        "checkpoint_created": checkpoint_created,
        "sealed": sealed,
    }


class BoundedCompactWriter:
    """Bounded partition collector flushed by one native Tokio writer."""

    def __init__(
        self,
        redis_client,
        collection,
        storage_algorithm,
        *,
        host,
        port,
        max_partitions=64,
    ):
        self.store = CompactSimilarityStore(redis_client, collection, storage_algorithm)
        self.host = host
        self.port = int(port)
        self.max_partitions = max(1, int(max_partitions))
        self.pending = []
        self.total_partitions = 0
        self.total_edges = 0
        self.total_bytes = 0

    def submit(self, partition_id, edges):
        encoded, digest, edge_count = self.store.prepare_partition(partition_id, edges)
        self.pending.append((partition_id, digest, encoded, edge_count))
        self.total_edges += edge_count
        self.total_bytes += len(encoded)
        if len(self.pending) >= self.max_partitions:
            self.flush()
        return CompactWriteResult(
            partition_id, edge_count, len(encoded), digest, created=True
        )

    def flush(self):
        if not self.pending:
            return {"created": 0, "reused": 0, "partitions": 0}
        pending, self.pending = self.pending, []
        try:
            from bsimvis_similarity_native import (
                persist_compact_partitions_redis_native,
            )
        except ImportError:
            created = 0
            for partition_id, _digest, encoded, _edge_count in pending:
                edges = decode_ascii(encoded)
                created += int(self.store.write_partition(partition_id, edges).created)
            self.total_partitions += len(pending)
            return {
                "created": created,
                "reused": len(pending) - created,
                "partitions": len(pending),
            }
        stats = persist_compact_partitions_redis_native(
            self.host,
            self.port,
            self.store.prefix,
            pending,
        )
        self.total_partitions += len(pending)
        return stats


class CompactSimilarityQueryAdapter:
    """Exact read adapter over an immutable compact similarity generation."""

    def __init__(self, store, metadata_loader=None):
        self.store = store
        self.metadata_loader = metadata_loader
        self._edges = None
        self._adjacency = None
        self._load_lock = threading.Lock()

    def _load(self):
        if self._edges is None:
            with self._load_lock:
                if self._edges is None:
                    edges = self.store.read_all()
                    adjacency = {}
                    for left, right, score in edges:
                        adjacency.setdefault(left, []).append((right, score))
                        adjacency.setdefault(right, []).append((left, score))
                    self._edges = edges
                    self._adjacency = adjacency
        return self._edges

    def estimated_resident_bytes(self):
        if self._edges is None:
            return 0
        total = sys.getsizeof(self._edges) + sys.getsizeof(self._adjacency)
        seen_strings = set()
        for left, right, _score in self._edges:
            total += sys.getsizeof((left, right, _score))
            for function_id in (left, right):
                if function_id not in seen_strings:
                    seen_strings.add(function_id)
                    total += sys.getsizeof(function_id)
        for values in self._adjacency.values():
            total += sys.getsizeof(values)
            total += sum(sys.getsizeof(value) for value in values)
        return total

    def _matches(self, function_id, metadata_filter):
        if not metadata_filter:
            return True
        if self.metadata_loader is None:
            raise ValueError("metadata_loader is required for metadata filters")
        metadata = self.metadata_loader(function_id) or {}
        return all(
            metadata.get(field) == value for field, value in metadata_filter.items()
        )

    def _candidates(self, function_id=None, metadata_filter=None):
        self._load()
        if function_id is not None:
            candidates = self._adjacency.get(function_id, [])
            if metadata_filter and hasattr(self.metadata_loader, "load_many"):
                self.metadata_loader.load_many(
                    candidate for candidate, _score in candidates
                )
            return [
                (candidate, score)
                for candidate, score in candidates
                if self._matches(candidate, metadata_filter)
            ]
        return [
            ((left, right), score)
            for left, right, score in self._edges
            if self._matches(left, metadata_filter)
            and self._matches(right, metadata_filter)
        ]

    def nearest(self, function_id=None, *, limit=20, metadata_filter=None):
        return sorted(
            self._candidates(function_id, metadata_filter),
            key=lambda item: (-item[1], item[0]),
        )[: max(0, int(limit))]

    def distant(self, function_id=None, *, limit=20, metadata_filter=None):
        return sorted(
            self._candidates(function_id, metadata_filter),
            key=lambda item: (item[1], item[0]),
        )[: max(0, int(limit))]

    def distribution(
        self,
        function_id=None,
        *,
        metadata_filter=None,
        probabilities=(0.05, 0.25, 0.5, 0.75, 0.95),
    ):
        values = sorted(
            score for _, score in self._candidates(function_id, metadata_filter)
        )
        count = len(values)
        mean = sum(values) / count if count else 0.0
        variance = (
            sum((value - mean) ** 2 for value in values) / count if count else 0.0
        )

        def quantile(probability):
            if not values:
                return 0.0
            position = float(probability) * (count - 1)
            lower, upper = math.floor(position), math.ceil(position)
            if lower == upper:
                return values[lower]
            fraction = position - lower
            return values[lower] * (1 - fraction) + values[upper] * fraction

        return {
            "count": count,
            "mean": mean,
            "variance": variance,
            "minimum": values[0] if values else 0.0,
            "maximum": values[-1] if values else 0.0,
            "quantiles": {
                str(probability): quantile(probability) for probability in probabilities
            },
        }


class BoundedCompactQueryCache:
    """LRU budget for reloadable immutable compact query adapters."""

    def __init__(self, *, max_bytes=256 * 1024 * 1024, max_generations=4):
        self.max_bytes = max(0, int(max_bytes))
        self.max_generations = max(1, int(max_generations))
        self._adapters = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key):
        adapter = self._adapters.get(key)
        if adapter is None:
            self.misses += 1
            return None
        self.hits += 1
        self._adapters.move_to_end(key)
        return adapter

    def put(self, key, adapter):
        self._adapters[key] = adapter
        self._adapters.move_to_end(key)
        self._evict(protected=key)
        return adapter

    def touch(self, key):
        if key in self._adapters:
            self._adapters.move_to_end(key)
            self._evict(protected=None)

    @property
    def resident_bytes(self):
        return sum(
            adapter.estimated_resident_bytes() for adapter in self._adapters.values()
        )

    def _evict(self, protected):
        while self._adapters and (
            len(self._adapters) > self.max_generations
            or self.resident_bytes > self.max_bytes
        ):
            candidate = next(iter(self._adapters))
            if candidate == protected and len(self._adapters) == 1:
                break
            if candidate == protected:
                self._adapters.move_to_end(candidate)
                candidate = next(iter(self._adapters))
            self._adapters.pop(candidate)
            self.evictions += 1
        if (
            protected is None
            and len(self._adapters) == 1
            and self.resident_bytes > self.max_bytes
        ):
            self._adapters.popitem(last=False)
            self.evictions += 1

    def __len__(self):
        return len(self._adapters)

    def stats(self):
        return {
            "entries": len(self),
            "resident_bytes": self.resident_bytes,
            "max_bytes": self.max_bytes,
            "max_generations": self.max_generations,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
        }


class RedisCompactMetadataLoader:
    """Bounded pipelined function-metadata joins for compact queries."""

    def __init__(
        self,
        redis_client,
        batch_size=250,
        *,
        max_entries=50_000,
        max_bytes=128 * 1024 * 1024,
    ):
        self.r = redis_client
        self.batch_size = max(1, int(batch_size))
        self.max_entries = max(1, int(max_entries))
        self.max_bytes = max(0, int(max_bytes))
        self.cache = OrderedDict()
        self._sizes = {}
        self._resident_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self._load_lock = threading.Lock()

    @staticmethod
    def _document(raw):
        if isinstance(raw, list):
            return raw[0] if raw else {}
        return raw or {}

    @staticmethod
    def _entry_bytes(function_id, document):
        return len(function_id.encode("utf-8")) + len(
            json.dumps(document, separators=(",", ":"), default=str).encode("utf-8")
        )

    def _put(self, function_id, document):
        previous = self._sizes.pop(function_id, 0)
        self._resident_bytes -= previous
        self.cache[function_id] = document
        self.cache.move_to_end(function_id)
        size = self._entry_bytes(function_id, document)
        self._sizes[function_id] = size
        self._resident_bytes += size
        while self.cache and (
            len(self.cache) > self.max_entries
            or (self.max_bytes and self._resident_bytes > self.max_bytes)
        ):
            evicted, _ = self.cache.popitem(last=False)
            self._resident_bytes -= self._sizes.pop(evicted, 0)
            self.evictions += 1

    def stats(self):
        return {
            "entries": len(self.cache),
            "resident_bytes": self._resident_bytes,
            "max_entries": self.max_entries,
            "max_bytes": self.max_bytes,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
        }

    def load_many(self, function_ids):
        requested = list(function_ids)
        missing = sorted(set(requested) - self.cache.keys())
        self.hits += len(requested) - len(missing)
        self.misses += len(missing)
        for function_id in requested:
            if function_id in self.cache:
                self.cache.move_to_end(function_id)
        result = {
            function_id: self.cache.get(function_id, {}) for function_id in requested
        }
        if missing:
            with self._load_lock:
                missing = sorted(set(missing) - self.cache.keys())
                for start in range(0, len(missing), self.batch_size):
                    chunk = missing[start : start + self.batch_size]
                    try:
                        pipe = self.r.pipeline(transaction=False)
                    except TypeError:
                        pipe = self.r.pipeline()
                    for function_id in chunk:
                        pipe.json().get(f"{function_id}:meta", "$")
                    for function_id, raw in zip(chunk, pipe.execute()):
                        document = self._document(raw)
                        result[function_id] = document
                        self._put(function_id, document)
        return result

    def __call__(self, function_id):
        return self.load_many([function_id]).get(function_id, {})
