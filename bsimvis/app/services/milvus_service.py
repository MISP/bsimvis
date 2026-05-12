import logging
import os
import zlib
import hashlib
import math
from pymilvus import (
    connections,
    utility,
    FieldSchema,
    CollectionSchema,
    DataType,
    Collection,
)


class MilvusService:
    def __init__(self, host=None, port=None):
        self.host = host or os.getenv("MILVUS_HOST", "localhost")
        self.port = port or os.getenv("MILVUS_PORT", "19530")
        self._connected = False
        self._collections = {}

    def connect(self):
        if self._connected:
            return True
        try:
            connections.connect("default", host=self.host, port=self.port)
            self._connected = True
            logging.info(f"[*] Connected to Milvus at {self.host}:{self.port}")
            return True
        except Exception as e:
            logging.error(f"[!] Failed to connect to Milvus: {e}")
            return False

    def ensure_collection(self, collection_name, index_type="SPARSE_INVERTED_INDEX"):
        """Ensures a collection exists with the correct schema for sparse vectors."""
        self.connect()
        # Use a prefixed name to avoid collisions and indicate type
        milvus_coll_name = collection_name

        if milvus_coll_name in self._collections:
            return self._collections[milvus_coll_name]

        if utility.has_collection(milvus_coll_name):
            col = Collection(milvus_coll_name)
            col.load()
            self._collections[milvus_coll_name] = col
            return col

        # Create Schema
        fields = [
            FieldSchema(
                name="id", dtype=DataType.INT64, is_primary=True, auto_id=False
            ),
            FieldSchema(name="fid", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="vector", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(
            fields, description=f"Sparse vectors for {collection_name} ({index_type})"
        )

        col = Collection(milvus_coll_name, schema)

        # Create Index
        index_params = {
            "index_type": index_type,
            "metric_type": "IP",  # Inner Product (Cosine similarity if normalized)
            "params": {"drop_ratio_build": 0.2},
        }
        col.create_index("vector", index_params)
        col.load()

        self._collections[milvus_coll_name] = col
        logging.info(
            f"[*] Created Milvus collection: {milvus_coll_name} with {index_type}"
        )
        return col

    def _fid_to_id(self, fid):
        """Stable mapping of string fid to INT64 for Milvus PK."""
        return int(hashlib.sha256(fid.encode()).hexdigest()[:15], 16)

    def _map_vector(self, tf_dict):
        """Maps string feature hashes to uint32 indices and returns an L2-normalized dict."""
        mapped = {}
        sum_sq = 0
        for feat_hash, tf in tf_dict.items():
            idx = zlib.crc32(feat_hash.encode()) & 0xFFFFFFFF
            val = float(tf)
            mapped[idx] = val
            sum_sq += val * val

        # Normalize to unit length for Cosine Similarity via IP
        norm = math.sqrt(sum_sq)
        if norm > 0:
            for idx in mapped:
                mapped[idx] /= norm

        return mapped

    def upsert_functions(
        self,
        collection_name,
        functions_data,
        flush=False,
        index_type="SPARSE_INVERTED_INDEX",
    ):
        """
        Upserts a list of functions into Milvus.
        functions_data: list of {'fid': str, 'tf_dict': dict}
        """
        col = self.ensure_collection(collection_name, index_type=index_type)
        if not col:
            return False

        ids = []
        fids = []
        vectors = []

        for func in functions_data:
            fid = func["fid"]
            tf_dict = func["tf_dict"]

            ids.append(self._fid_to_id(fid))
            fids.append(fid)
            vectors.append(self._map_vector(tf_dict))

        if not ids:
            return True

        try:
            col.insert([ids, fids, vectors])
            if flush:
                col.flush()
            return True
        except Exception as e:
            logging.error(f"[!] Milvus Insert Error: {e}")
            return False

    def sync_collection(self, collection_name, r, job_service=None, job_id=None):
        """
        Syncs all indexed functions from Redis to Milvus for both index types.
        """
        indexed_set = f"{collection_name}:indexed:functions"
        function_ids = list(r.smembers(indexed_set))
        total = len(function_ids)

        if total == 0:
            logging.warning(
                f"No indexed functions found for collection: {collection_name}"
            )
            return True

        logging.info(
            f"[*] Syncing {total} functions to Milvus for collection: {collection_name}"
        )

        index_types = ["SPARSE_INVERTED_INDEX"]
        chunk_size = 100
        milvus_data = []

        for i, fid in enumerate(function_ids):
            fid = fid.decode() if isinstance(fid, bytes) else fid

            # Update job progress
            if job_service and job_id and (i % 50 == 0 or i == total - 1):
                pct = int((i + 1) / total * 100)
                job_service.update_progress(
                    job_id, pct, f"Syncing Milvus: {i+1}/{total}"
                )

            # Fetch TF vector
            vec_key = f"{fid}:vec:tf"
            features_raw = r.zrange(vec_key, 0, -1, withscores=True)

            if not features_raw:
                continue

            tf_dict = {
                h.decode() if isinstance(h, bytes) else h: float(s)
                for h, s in features_raw
            }
            milvus_data.append({"fid": fid, "tf_dict": tf_dict})

            if len(milvus_data) >= chunk_size:
                for itype in index_types:
                    self.upsert_functions(
                        collection_name, milvus_data, index_type=itype
                    )
                milvus_data = []

        # Final flush and log summary
        final_count = 0
        for itype in index_types:
            col = self.ensure_collection(collection_name, index_type=itype)
            if milvus_data:
                self.upsert_functions(
                    collection_name, milvus_data, flush=True, index_type=itype
                )
            if col:
                col.flush()
                final_count = col.num_entities
                logging.info(
                    f"[*] Milvus collection {col.name} now has {final_count} entities"
                )

        if final_count == 0 and total > 0:
            logging.error(
                f"[!] Sync completed but Milvus collections for {collection_name} are EMPTY!"
            )
            return False

        return True

    def search_similar(
        self,
        collection_name,
        tf_dict,
        top_k=100,
        min_score=0.0,
        index_type="SPARSE_INVERTED_INDEX",
    ):
        """Searches for similar functions in Milvus."""
        col = self.ensure_collection(collection_name, index_type=index_type)

        query_vector = self._map_vector(tf_dict)

        search_params = {"metric_type": "IP", "params": {"drop_ratio_search": 0.2}}

        try:
            results = col.search(
                data=[query_vector],
                anns_field="vector",
                param=search_params,
                limit=top_k,
                output_fields=["fid"],
            )

            candidates = []
            for hit in results[0]:
                if hit.score >= min_score:
                    candidates.append({"id": hit.entity.get("fid"), "score": hit.score})
            return candidates
        except Exception as e:
            logging.error(f"[!] Milvus Search Error: {e}")
            return None


milvus_service = MilvusService()
