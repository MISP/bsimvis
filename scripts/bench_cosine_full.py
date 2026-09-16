import os
import math
import json
import argparse
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()
from bsimvis.app.services.similarity_service import SimilarityService

def parse_sid(sid):
    parts = sid.split(':')
    coll = parts[0]
    empty_idx = parts.index('')
    fid1 = f"{coll}:func:{parts[empty_idx-2]}:{parts[empty_idx-1]}"
    fid2 = f"{coll}:func:{parts[empty_idx+1]}:{parts[empty_idx+2]}"
    return fid1, fid2

def get_vectors_batch(s, fids):
    pipe = s.r.pipeline(transaction=False)
    for fid in fids:
        pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
    res = pipe.execute()
    return {fids[i]: res[i] for i in range(len(fids))}

def calc_raw_tf(v1, v2):
    d1 = {h: float(s) for h, s in v1}
    d2 = {h: float(s) for h, s in v2}
    common = set(d1.keys()).intersection(set(d2.keys()))
    if not common: return 0.0, 0.0
    dot = sum(d1[h]*d2[h] for h in common)
    n1 = math.sqrt(sum(v**2 for v in d1.values()))
    n2 = math.sqrt(sum(v**2 for v in d2.values()))
    return (dot / (n1*n2)) if n1>0 and n2>0 else 0.0, dot

def calc_binary(v1, v2):
    d1 = {h for h, _ in v1}
    d2 = {h for h, _ in v2}
    common = d1.intersection(d2)
    if not common: return 0.0
    return len(common) / math.sqrt(len(d1)*len(d2)) if (len(d1)>0 and len(d2)>0) else 0.0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collection', default='rapperbot')
    parser.add_argument('--threshold', type=float, default=0.85)
    args = parser.parse_args()

    s = SimilarityService()
    zset_key = f"{args.collection}:sim:score:unweighted_cosine"
    
    sids_with_scores = s.r.zrange(zset_key, 0, -1, withscores=True)
    if isinstance(sids_with_scores[0][0], bytes):
        sids_with_scores = [(sid.decode(), score) for sid, score in sids_with_scores]
        
    unique_fids = set()
    pairs_found = set()
    pairs_data = []
    
    for sid, score in sids_with_scores:
        try:
            fid1, fid2 = parse_sid(sid)
            unique_fids.add(fid1)
            unique_fids.add(fid2)
            pair_key = tuple(sorted([fid1, fid2]))
            pairs_found.add(pair_key)
            pairs_data.append((sid, fid1, fid2, score))
        except Exception as e:
            continue
            
    fids_list = list(unique_fids)
    
    vecs = {}
    batch_size = 5000
    for i in range(0, len(fids_list), batch_size):
        batch = fids_list[i:i+batch_size]
        vecs.update(get_vectors_batch(s, batch))
        
    results = []
    false_positives = []
    misses = []
    deltas = []
    
    # group by size ratio buckets
    size_ratio_buckets = defaultdict(list)
    # group by repeat concentration buckets
    repeat_buckets = defaultdict(list)
    
    for sid, fid1, fid2, raw_score in pairs_data:
        v1 = vecs.get(fid1, [])
        v2 = vecs.get(fid2, [])
        if not v1 or not v2: continue
        
        raw_tf, dot = calc_raw_tf(v1, v2)
        binary_cos = calc_binary(v1, v2)
        delta = binary_cos - raw_tf
        
        d1 = {h: float(sc) for h, sc in v1}
        d2 = {h: float(sc) for h, sc in v2}
        common = set(d1.keys()).intersection(set(d2.keys()))
        
        repeat_concentration = sum((d1[h]-1) + (d2[h]-1) for h in common)
        contributions = {h: d1[h]*d2[h] for h in common}
        top_contrib = sorted(contributions.items(), key=lambda x: x[1], reverse=True)[:3]
        
        size_1, size_2 = len(v1), len(v2)
        size_ratio = min(size_1, size_2) / max(size_1, size_2) if max(size_1, size_2) > 0 else 0
        
        res = {
            'sid': sid,
            'fid1': fid1,
            'fid2': fid2,
            'raw_score': raw_tf,
            'binary_score': binary_cos,
            'delta': delta,
            'feat_count_1': size_1,
            'feat_count_2': size_2,
            'shared_hashes': len(common),
            'top_contribs': top_contrib,
            'repeat_concentration': repeat_concentration,
            'size_ratio': size_ratio
        }
        results.append(res)
        deltas.append(res)
        
        # Bucketing
        sr_bucket = round(size_ratio * 10) / 10.0
        size_ratio_buckets[sr_bucket].append(delta)
        
        # Log buckets for repeat concentration
        rep_bucket = 0
        if repeat_concentration > 0:
            rep_bucket = 10 ** int(math.log10(max(1, repeat_concentration)))
        repeat_buckets[rep_bucket].append(delta)
        
        if raw_tf >= args.threshold and binary_cos < args.threshold:
            false_positives.append(res)
            
        if raw_tf < args.threshold and binary_cos >= args.threshold:
            misses.append(res)
            
    deltas_sorted = sorted(deltas, key=lambda x: x['delta'])
    max_neg_deltas = deltas_sorted[:5]
    max_pos_deltas = deltas_sorted[-5:]
    
    # Calculate blind spots
    parsed_vecs = {fid: {h for h, _ in v} for fid, v in vecs.items() if v}
    fids_parsed = list(parsed_vecs.keys())
    
    blind_spots = 0
    for i in range(len(fids_parsed)):
        fid1 = fids_parsed[i]
        v1 = parsed_vecs[fid1]
        len1 = len(v1)
        if len1 == 0: continue
        for j in range(i+1, len(fids_parsed)):
            fid2 = fids_parsed[j]
            v2 = parsed_vecs[fid2]
            len2 = len(v2)
            if len2 == 0: continue
            
            common = len(v1.intersection(v2))
            if common == 0: continue
            
            bin_cos = common / math.sqrt(len1 * len2)
            if bin_cos >= args.threshold:
                pair_key = tuple(sorted([fid1, fid2]))
                if pair_key not in pairs_found:
                    blind_spots += 1
    
    # OUTPUT
    print(f"Total functions: {len(unique_fids)}")
    print(f"Total candidate pairs examined: {len(results)}")
    
    raw_scores = [r['raw_score'] for r in results]
    bin_scores = [r['binary_score'] for r in results]
    
    mean_raw = sum(raw_scores)/len(raw_scores)
    mean_bin = sum(bin_scores)/len(bin_scores)
    var_raw = sum((x - mean_raw)**2 for x in raw_scores)
    var_bin = sum((x - mean_bin)**2 for x in bin_scores)
    corr = 0
    if var_raw > 0 and var_bin > 0:
        cov = sum((raw_scores[i] - mean_raw)*(bin_scores[i] - mean_bin) for i in range(len(raw_scores)))
        corr = cov / math.sqrt(var_raw * var_bin)
        
    print(f"Mean Raw-TF Score: {mean_raw:.4f}")
    print(f"Mean Binary Score: {mean_bin:.4f}")
    print(f"Correlation (Pearson): {corr:.4f}")
    
    print(f"Raw-TF False Positives (Raw >= {args.threshold}, Binary < {args.threshold}): {len(false_positives)} ({(len(false_positives)/len(results)*100) if results else 0:.2f}%)")
    print(f"Raw-TF Misses (Raw < {args.threshold}, Binary >= {args.threshold}): {len(misses)} ({(len(misses)/len(results)*100) if results else 0:.2f}%)")
    print(f"Candidate-Discovery Blind Spots (Binary >= {args.threshold} but NOT in candidates): {blind_spots}")
    
    print("\nLargest Negative Deltas (Raw heavily overestimated):")
    for d in max_neg_deltas:
        print(f"  Delta {d['delta']:.4f} | Raw: {d['raw_score']:.4f} -> Bin: {d['binary_score']:.4f} | RepConc: {d['repeat_concentration']}")
        
    print("\nLargest Positive Deltas (Raw heavily underestimated):")
    for d in reversed(max_pos_deltas):
        print(f"  Delta {d['delta']:.4f} | Raw: {d['raw_score']:.4f} -> Bin: {d['binary_score']:.4f} | RepConc: {d['repeat_concentration']}")

    print("\nScore Changes by Size Ratio:")
    for b in sorted(size_ratio_buckets.keys()):
        avg_d = sum(size_ratio_buckets[b]) / len(size_ratio_buckets[b])
        print(f"  Ratio ~{b:.1f}: {len(size_ratio_buckets[b])} pairs, Avg Delta: {avg_d:.4f}")
        
    print("\nScore Changes by Repeat Concentration:")
    for b in sorted(repeat_buckets.keys()):
        avg_d = sum(repeat_buckets[b]) / len(repeat_buckets[b])
        print(f"  RepConc ~{b}: {len(repeat_buckets[b])} pairs, Avg Delta: {avg_d:.4f}")

    out_file = "cosine_benchmark_sample.json"
    with open(out_file, "w") as f:
        sample = false_positives[:10] + misses[:10] + max_neg_deltas + max_pos_deltas
        json.dump(sample, f, indent=2)

if __name__ == '__main__':
    main()
