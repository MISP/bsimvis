#!/usr/bin/env python3
"""§7.5 guardrail for CLUSTER_POOL_META_CHUNK_SIZE (cluster_service.py's
run_pool_bin_clustering metadata-write loop, job-system-rework-plan.md §7.1).

The real mirai3_bench corpus (12 ingested samples -> 16 files after
archive/UPX unpacking) produces only 1 pool-level binary cluster after
HDBSCAN -- mirai variants share a huge libc floor and cluster together, so
there's no way to reach "hundreds of thousands of clusters" (the scale the
chunk loop exists for) by feeding it *more* mirai samples; that would take an
unrelated, dissimilar corpus, defeating the point of using this corpus.

So this harness reproduces the loop's actual write pattern verbatim (same
Redis calls, same flush-every-N-clusters shape) against a large *synthetic*
cluster count, but every member id and every piece of metadata it writes is
real, pulled from the 16 files this run actually ingested and analyzed
(bulk-fetched the same way run_pool_bin_clustering does it). This isolates
the mechanism §7.1 added (bounded pipeline + checkpoint) from the clustering
math around it, which the ENRICH_CHUNK_SIZE precedent already exercises
(same "flush-a-pipeline-every-N-items" shape).

Usage: run once per candidate chunk size, as a separate process each time
(peak RSS is read via resource.getrusage, which is a running max for the
whole process lifetime -- must be a fresh process per trial):

    .venv/bin/python bench_cluster_pool_meta_chunk.py <redis_port> <n_clusters> <chunk_size>
"""
import json
import os
import random
import resource
import sys
import time
import uuid
from collections import Counter

import redis


def main():
    redis_port = int(sys.argv[1])
    n_clusters = int(sys.argv[2])
    chunk_size = int(sys.argv[3])

    r = redis.Redis(host="localhost", port=redis_port, decode_responses=False)

    # Real file ids from the ingested collection, same fid shape the loop
    # consumes: "<coll>:file:<md5>".
    coll = "mirai3_bench"
    file_keys = [
        k.decode() if isinstance(k, bytes) else k
        for k in r.smembers(f"{coll}:all_files")
    ]
    md5s = sorted({k.split(":")[2] for k in file_keys if not k.endswith(":meta") and len(k.split(":")) >= 3})
    if not md5s:
        print(json.dumps({"error": f"no files found under {coll}:all_files"}))
        return 1
    fids = [f"{coll}:file:{m}" for m in md5s]

    random.seed(1234)
    pool_id = "benchsynth"
    cluster_members = {}
    for label in range(n_clusters):
        size = random.choice([2, 2, 3, 3, 4])
        cluster_members[label] = random.sample(fids, min(size, len(fids)))

    t0 = time.time()

    # --- verbatim: bulk member-metadata fetch (cluster_service.py ~2963) ---
    all_member_file_ids = list({fid for members in cluster_members.values() for fid in members})
    all_member_meta = {}
    for i in range(0, len(all_member_file_ids), 1000):
        chunk = all_member_file_ids[i:i + 1000]
        m_pipe = r.pipeline(transaction=False)
        for fid in chunk:
            parts = fid.split(":")
            c, md5 = parts[0], parts[2]
            m_pipe.get(f"{c}:file:{md5}:meta")
        results = m_pipe.execute()
        for idx, fid in enumerate(chunk):
            meta_res = results[idx]
            m = {}
            if meta_res:
                try:
                    m = json.loads(meta_res)
                except Exception:
                    m = {}
            all_member_meta[fid] = m

    label_to_uuid = {c: uuid.uuid4().hex[:12] for c in cluster_members}
    cluster_list_key = f"global:pool:{pool_id}:bin_cluster:list"
    pipe = r.pipeline(transaction=False)
    pipe.delete(cluster_list_key)

    total_clusters = len(cluster_members)
    for idx, (label, members) in enumerate(cluster_members.items()):
        c_uuid = label_to_uuid[label]
        meta_key = f"global:pool:{pool_id}:bin_cluster:{c_uuid}:meta"
        members_key = f"global:pool:{pool_id}:bin_cluster:{c_uuid}:members"

        names_list, md5s_list, yara_list = [], [], []
        for fid in members:
            m = all_member_meta.get(fid, {})
            if m.get("file_name"):
                names_list.append(m["file_name"])
            if m.get("file_md5"):
                md5s_list.append(m["file_md5"])
            if m.get("yara"):
                yara_list.extend(m["yara"] if isinstance(m["yara"], list) else [m["yara"]])

        meta = {
            "id": c_uuid,
            "cluster_id": int(label),
            "member_count": len(members),
            "sample_files": names_list[:5],
            "yara_distribution": [
                {"value": k, "count": v} for k, v in Counter(yara_list).most_common(5)
            ],
        }
        pipe.sadd(cluster_list_key, c_uuid)
        pipe.set(meta_key, json.dumps(meta))
        pipe.sadd(members_key, *members)

        if (idx + 1) % chunk_size == 0 or (idx + 1) == total_clusters:
            pipe.execute()
            pipe = r.pipeline(transaction=False)

    wall = time.time() - t0
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss  # KB on Linux

    # Cleanup so repeated trials against the same isolated Redis don't leak.
    cleanup = r.pipeline(transaction=False)
    for c_uuid in label_to_uuid.values():
        cleanup.delete(f"global:pool:{pool_id}:bin_cluster:{c_uuid}:meta")
        cleanup.delete(f"global:pool:{pool_id}:bin_cluster:{c_uuid}:members")
    cleanup.delete(cluster_list_key)
    cleanup.execute()

    print(json.dumps({
        "chunk_size": chunk_size,
        "n_clusters": n_clusters,
        "wall_seconds": round(wall, 3),
        "clusters_per_sec": round(n_clusters / wall, 1) if wall else None,
        "peak_rss_mb": round(peak_rss_kb / 1024, 1),
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
