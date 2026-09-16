import json
import math
from dotenv import load_dotenv
load_dotenv()
from bsimvis.app.services.bin_sim_service import BinSimService
from bsimvis.app.services.similarity_service import SimilarityService

def get_vectors_batch(s, fids):
    pipe = s.r.pipeline(transaction=False)
    for fid in fids:
        pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)
    res = pipe.execute()
    return {fids[i]: res[i] for i in range(len(fids))}

def calc_binary(v1, v2):
    d1 = {h for h, _ in v1}
    d2 = {h for h, _ in v2}
    common = d1.intersection(d2)
    if not common: return 0.0
    return len(common) / math.sqrt(len(d1)*len(d2)) if (len(d1)>0 and len(d2)>0) else 0.0

def main():
    bs = BinSimService()
    ss = SimilarityService()
    keys = bs.r.keys("rapperbot:bin_sim:unweighted_cosine:*")
    
    file_pairs = []
    for k in keys:
        try:
            data = json.loads(bs.r.get(k))
            if data.get('is_container_pair'): continue
            
            fa = data.get('functions_count_a', 0)
            fb = data.get('functions_count_b', 0)
            
            if fa >= 20 and fb >= 20:
                file_pairs.append((k, data))
        except Exception as e:
            continue
            
    print(f"Found {len(file_pairs)} file pairs with >= 20 functions.")
    
    results = []
    for k, data in file_pairs:
        matched = data.get('diff', {}).get('matched', [])
        if not matched: continue
        
        # We need to get vectors for all matched function pairs
        fids_to_fetch = set()
        pairs = []
        for m in matched:
            fid_a = m.get('id_a') or m.get('id1') # wait, how are function ids stored in matched?
            # Let's check keys
            if 'func_a' in m:
                fid_a = m['func_a']
                fid_b = m['func_b']
            elif 'id_a' in m:
                fid_a = f"rapperbot:func:{m['md5_a']}:{m['address_a']}" if 'address_a' in m else None
                fid_b = f"rapperbot:func:{m['md5_b']}:{m['address_b']}" if 'address_b' in m else None
                
            if fid_a and fid_b:
                fids_to_fetch.add(fid_a)
                fids_to_fetch.add(fid_b)
                pairs.append((fid_a, fid_b, m.get('similarity', 0)))
                
        if not pairs: continue
        
        vecs = get_vectors_batch(ss, list(fids_to_fetch))
        
        raw_sum = 0
        bin_sum = 0
        valid_pairs = 0
        
        for fa, fb, sim in pairs:
            v1 = vecs.get(fa, [])
            v2 = vecs.get(fb, [])
            if not v1 or not v2: continue
            
            bin_score = calc_binary(v1, v2)
            raw_sum += sim
            bin_sum += bin_score
            valid_pairs += 1
            
        if valid_pairs < 10: continue
        
        avg_raw = raw_sum / valid_pairs
        avg_bin = bin_sum / valid_pairs
        delta = avg_bin - avg_raw
        
        results.append({
            'md5_a': data['md5_a'],
            'md5_b': data['md5_b'],
            'file_score': data.get('score'),
            'fa': fa,
            'fb': fb,
            'matched_count': valid_pairs,
            'avg_raw_sim': avg_raw,
            'avg_bin_sim': avg_bin,
            'delta': delta
        })
        
    results.sort(key=lambda x: x['delta'], reverse=True)
    
    print("\n--- Extreme Underestimated Examples (Largest Positive Delta) ---")
    for r in results[:2]:
        print(r)
        
    print("\n--- Moderate Examples (Middle Delta) ---")
    if len(results) > 0:
        mid_idx = len(results) // 2
        for r in results[mid_idx:mid_idx+2]:
            print(r)

if __name__ == '__main__':
    main()
