import math
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
    threshold = 0.50
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
        
    print(f"Computing BOTH metrics for all {len(fids_list)*(len(fids_list)-1)//2} pairs...")
    
    # Pre-parse vectors
    parsed = []
    for fid in fids_list:
        v = vecs.get(fid, [])
        if not v: continue
        
        # for binary
        hashes_set = {h for h, _ in v}
        
        # for raw TF
        d = {h: float(sc) for h, sc in v}
        norm = math.sqrt(sum(val**2 for val in d.values()))
        
        parsed.append((fid, hashes_set, d, norm))
        
    false_positives = 0
    misses = 0
    agreements = 0
    
    # We will do O(N^2)
    # To save time, we only care about pairs that score >= 0.5 on AT LEAST ONE metric.
    n = len(parsed)
    for i in range(n):
        fid1, set1, d1, norm1 = parsed[i]
        len1 = len(set1)
        if len1 == 0 or norm1 == 0: continue
        
        for j in range(i+1, n):
            fid2, set2, d2, norm2 = parsed[j]
            len2 = len(set2)
            if len2 == 0 or norm2 == 0: continue
            
            common_keys = set1.intersection(set2)
            if not common_keys: continue
            
            # Binary
            bin_score = len(common_keys) / math.sqrt(len1 * len2)
            
            # Raw TF
            dot = sum(d1[k]*d2[k] for k in common_keys)
            raw_score = dot / (norm1 * norm2)
            
            if raw_score >= threshold and bin_score < threshold:
                false_positives += 1
            elif raw_score < threshold and bin_score >= threshold:
                misses += 1
            elif raw_score >= threshold and bin_score >= threshold:
                agreements += 1

    total_valid = false_positives + misses + agreements
    print(f"\nAt True Threshold {threshold}:")
    print(f"Total pairs scoring >= {threshold} in AT LEAST ONE metric: {total_valid}")
    print(f"True Agreements (Both >= {threshold}): {agreements} ({(agreements/total_valid*100):.2f}%)")
    print(f"Raw-TF False Positives (Raw >= {threshold}, Bin < {threshold}): {false_positives} ({(false_positives/total_valid*100):.2f}%)")
    print(f"Raw-TF Misses (Raw < {threshold}, Bin >= {threshold}): {misses} ({(misses/total_valid*100):.2f}%)")

if __name__ == '__main__':
    main()
