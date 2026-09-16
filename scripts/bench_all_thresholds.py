import math
import time
import argparse
from dotenv import load_dotenv

load_dotenv()
from bsimvis.app.services.similarity_service import SimilarityService

def get_vectors_batch(s, fids):
    pipe = s.r.pipeline(transaction=False)
    for fid in fids:
        pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
    res = pipe.execute()
    return {fids[i]: res[i] for i in range(len(fids))}

def main():
    thresholds = [0.90, 0.85, 0.80, 0.75, 0.70, 0.60, 0.50]
    
    s = SimilarityService()
    zset_key = f"rapperbot:sim:score:unweighted_cosine"
    
    sids_with_scores = s.r.zrange(zset_key, 0, -1, withscores=True)
    if isinstance(sids_with_scores[0][0], bytes):
        sids_with_scores = [(sid.decode(), score) for sid, score in sids_with_scores]
        
    unique_fids = set()
    for sid, _ in sids_with_scores:
        try:
            parts = sid.split(':')
            empty_idx = parts.index('')
            fid1 = f"{parts[0]}:func:{parts[empty_idx-2]}:{parts[empty_idx-1]}"
            fid2 = f"{parts[0]}:func:{parts[empty_idx+1]}:{parts[empty_idx+2]}"
            unique_fids.add(fid1)
            unique_fids.add(fid2)
        except Exception:
            continue
            
    fids_list = list(unique_fids)
    print(f"Fetching vectors for {len(fids_list)} functions...")
    
    vecs = {}
    batch_size = 5000
    for i in range(0, len(fids_list), batch_size):
        batch = fids_list[i:i+batch_size]
        vecs.update(get_vectors_batch(s, batch))
        
    # Pre-parse vectors
    parsed = []
    for fid in fids_list:
        v = vecs.get(fid, [])
        if not v: continue
        
        hashes_set = {h for h, _ in v}
        d = {h: float(sc) for h, sc in v}
        norm = math.sqrt(sum(val**2 for val in d.values()))
        parsed.append((hashes_set, d, norm))
        
    n = len(parsed)
    print(f"Computing pairs: {n*(n-1)//2}")
    
    # We will accumulate for each threshold
    metrics = {t: {'TP': 0, 'FP': 0, 'FN': 0} for t in thresholds}
    
    # Also benchmark time for a subset
    t_start_bin = time.perf_counter()
    bin_comparisons = 0
    # We won't time the whole O(N^2) loop cleanly if we mix them, so we just run the logic.
    # To measure performance accurately, we do a pure loop for the first 1M pairs.
    
    benchmark_pairs = 1000000
    pairs_tested = 0
    
    time_bin = 0
    time_raw = 0
    
    for i in range(n):
        set1, d1, norm1 = parsed[i]
        len1 = len(set1)
        if len1 == 0 or norm1 == 0: continue
        
        for j in range(i+1, n):
            set2, d2, norm2 = parsed[j]
            len2 = len(set2)
            if len2 == 0 or norm2 == 0: continue
            
            # Measure pure calculation speed for first 1M pairs
            if pairs_tested < benchmark_pairs:
                # Bin time
                t0 = time.perf_counter()
                common_keys = set1.intersection(set2)
                if common_keys:
                    bin_score = len(common_keys) / math.sqrt(len1 * len2)
                else:
                    bin_score = 0.0
                t1 = time.perf_counter()
                time_bin += (t1 - t0)
                
                # Raw time
                t0 = time.perf_counter()
                common_dict = set1.intersection(set2) # intersection is needed for both in python realistic scenario
                if common_dict:
                    dot = sum(d1[k]*d2[k] for k in common_dict)
                    raw_score = dot / (norm1 * norm2)
                else:
                    raw_score = 0.0
                t1 = time.perf_counter()
                time_raw += (t1 - t0)
                pairs_tested += 1
            else:
                common_keys = set1.intersection(set2)
                if common_keys:
                    bin_score = len(common_keys) / math.sqrt(len1 * len2)
                    dot = sum(d1[k]*d2[k] for k in common_keys)
                    raw_score = dot / (norm1 * norm2)
                else:
                    bin_score = 0.0
                    raw_score = 0.0
                
            if bin_score == 0 and raw_score == 0: continue
            
            for t in thresholds:
                is_bin_match = bin_score >= t
                is_raw_match = raw_score >= t
                
                if is_bin_match and is_raw_match:
                    metrics[t]['TP'] += 1
                elif is_raw_match and not is_bin_match:
                    metrics[t]['FP'] += 1
                elif not is_raw_match and is_bin_match:
                    metrics[t]['FN'] += 1

    print("\n--- PERFORMANCE (Measured over 1,000,000 pairs) ---")
    print(f"Binary Cosine Calculation: {time_bin*1000:.2f} ms")
    print(f"Raw-TF Cosine Calculation: {time_raw*1000:.2f} ms")
    print(f"Speedup: {time_raw/time_bin:.2f}x faster")
    
    print("\n--- METRICS BY THRESHOLD ---")
    print(f"{'Threshold':<10} | {'Precision':<10} | {'Recall':<10} | {'True Positives':<15} | {'False Positives':<15} | {'Missed (FN)':<15}")
    print("-" * 85)
    
    for t in thresholds:
        m = metrics[t]
        tp = m['TP']
        fp = m['FP']
        fn = m['FN']
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        
        print(f"{t:<10} | {precision*100:>8.2f}% | {recall*100:>8.2f}% | {tp:<15} | {fp:<15} | {fn:<15}")

if __name__ == '__main__':
    main()
