import time
import argparse
import redis
import math
from bsimvis.app.services.similarity_service import SimilarityService

def generate_mock_data(r, coll, num_funcs=1000):
    print(f"[*] Generating mock collection '{coll}' with {num_funcs} functions...")
    pipe = r.pipeline()
    
    # Common feature hashes shared by all functions
    common_features = [f"feat_common_{i}" for i in range(5)]
    
    for i in range(num_funcs):
        fid = f"{coll}:func:mock_md5:addr_{i}"
        
        # Unique features for this function
        unique_features = [f"feat_uniq_{i}_{j}" for j in range(10)]
        
        # TF vector: (feature, tf)
        # Store vector TF
        tf_key = f"{fid}:vec:tf"
        tf_dict = {}
        for f in common_features:
            tf_dict[f] = 1.0
        for f in unique_features:
            tf_dict[f] = 1.0
            
        pipe.zadd(tf_key, tf_dict)
        
        # Recalculate and set norm
        sum_sq = len(common_features) + len(unique_features)
        pipe.set(f"{fid}:vec:norm", math.sqrt(sum_sq))
        
        # Add to indexed functions
        pipe.sadd(f"{coll}:indexed:functions", fid)
        
        # Build inverted index
        for f in common_features:
            pipe.zadd(f"{coll}:feature:{f}:functions", {fid: 1.0})
        for f in unique_features:
            pipe.zadd(f"{coll}:feature:{f}:functions", {fid: 1.0})
            
        # Update feature count metadata
        pipe.zadd(f"{coll}:idx:func:bsim_features_count", {fid: len(common_features) + len(unique_features)})
        
        if i % 100 == 0:
            pipe.execute()
            pipe = r.pipeline()
            
    pipe.execute()
    print("[+] Mock collection populated successfully.")

def cleanup_mock_data(r, coll):
    print(f"[*] Cleaning up mock collection '{coll}'...")
    # Scan and delete all keys matching mock_large_coll:*
    keys = r.keys(f"{coll}:*")
    if keys:
        pipe = r.pipeline()
        for k in keys:
            pipe.delete(k)
        pipe.execute()
    print("[+] Mock collection cleaned up.")

def main():
    parser = argparse.ArgumentParser(description="Benchmark Similarity calculation speed.")
    parser.add_argument("--collection", help="Collection name to benchmark. If omitted, will use/create a mock.")
    parser.add_argument("--limit", type=int, default=50, help="Number of functions to test.")
    parser.add_argument("--port", type=int, default=6666, help="Kvrocks/Redis port.")
    parser.add_argument("--mock-size", type=int, default=1000, help="Number of functions to generate for mock test.")
    args = parser.parse_args()

    r = redis.Redis(port=args.port)
    
    coll = args.collection
    is_mock = False
    if not coll:
        coll = "mock_large_coll"
        is_mock = True
        generate_mock_data(r, coll, num_funcs=args.mock_size)

    try:
        # Fetch sample functions
        func_set = f"{coll}:indexed:functions"
        funcs = [f.decode() for f in r.srandmember(func_set, args.limit)]
        if not funcs:
            print("[-] No functions found in collection.")
            return

        print(f"[*] Starting benchmark on {len(funcs)} functions...")
        
        # Instantiate service
        sim_service = SimilarityService(r=r)
        
        # Measure time
        start_time = time.time()
        
        # We will simulate processing a chunk of functions
        temp_built_set = f"{coll}:built:benchmark_temp"
        r.delete(temp_built_set)
        
        def patched_process_chunk(collection, chunk, algo, top_k, min_score, min_features=0):
            built_set_key = temp_built_set
            
            pipe = r.pipeline()
            for fid in chunk:
                pipe.sismember(built_set_key, fid)
                pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
            results = pipe.execute()

            targets_to_build = []
            for idx, fid in enumerate(chunk):
                is_built = results[idx * 2]
                features = results[idx * 2 + 1]
                if is_built:
                    continue
                if not features or len(features) < min_features:
                    r.sadd(built_set_key, fid)
                    continue
                targets_to_build.append((fid, features))

            if not targets_to_build:
                return

            prepared_targets = []
            for fid, features_raw in targets_to_build:
                target_feat_total = 0
                target_feat_norm_sq = 0
                lua_features_args = []
                for f_hash, f_tf_raw in features_raw:
                    f_tf = float(f_tf_raw)
                    target_feat_total += f_tf
                    target_feat_norm_sq += f_tf * f_tf
                    lua_features_args.extend([f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash), str(f_tf)])

                target_feat_norm = math.sqrt(target_feat_norm_sq)
                lua_args = [
                    fid,
                    collection,
                    algo,
                    min_score,
                    target_feat_total,
                    target_feat_norm,
                    top_k,
                    min_features,
                ] + lua_features_args
                prepared_targets.append((fid, target_feat_total, lua_args))

            if prepared_targets:
                pipe = r.pipeline()
                for fid, target_feat_total, lua_args in prepared_targets:
                    sim_service._find_script(args=lua_args, client=pipe)
                    pipe.sadd(built_set_key, fid)
                pipe_results = pipe.execute()
                
                total_matches = 0
                for idx, (fid, target_feat_total, lua_args) in enumerate(prepared_targets):
                    candidates_raw = pipe_results[idx * 2]
                    if candidates_raw:
                        total_matches += len(candidates_raw) // 3

        # Run the patched loop
        chunk_size = 50
        for i in range(0, len(funcs), chunk_size):
            chunk = funcs[i:i+chunk_size]
            patched_process_chunk(coll, chunk, "unweighted_cosine", 1000, 0.3, 0)
            
        elapsed = time.time() - start_time
        r.delete(temp_built_set)
        
        print("-" * 40)
        print(f"Benchmark Results:")
        print(f"  Processed: {len(funcs)} functions")
        print(f"  Total Time: {elapsed:.2f} seconds")
        print(f"  Speed: {len(funcs)/elapsed:.2f} functions/second")
        print("-" * 40)
        
    finally:
        if is_mock:
            cleanup_mock_data(r, coll)

if __name__ == "__main__":
    main()
