import re

with open("bsimvis/app/services/similarity_service.py", "r") as f:
    code = f.read()

# 1. Update _discover_find min_shared setup
old_1 = """        min_shared_norm_sq = 0.0
        if algo == "unweighted_cosine":
            min_shared_norm_sq = (threshold * target_norm) ** 2"""

new_1 = """        min_shared_features = 0.0
        target_len = len(target_features)
        if algo == "unweighted_cosine":
            min_shared_features = (threshold ** 2) * target_len"""

code = code.replace(old_1, new_1)

# 2. Update loop early exit
old_2 = """            if algo == "unweighted_cosine":
                if remaining_norm_sq < min_shared_norm_sq:
                    can_add_new = False"""

new_2 = """            if algo == "unweighted_cosine":
                if (len(features_sorted) - i) < min_shared_features:
                    can_add_new = False"""
                    
code = code.replace(old_2, new_2)

# 3. Update dict init inside loop
old_3 = """                    if not is_existing:
                        intersection_counts[func_id] = 0.0
                        if algo == "unweighted_cosine":
                            shared_target_norm_sq[func_id] = 0.0"""

new_3 = """                    if not is_existing:
                        intersection_counts[func_id] = 0.0
                        num_candidates += 1"""
# wait, there's `num_candidates += 1` inside `if not is_existing:`. Let me do regex.

code = re.sub(
    r'                    if not is_existing:\n                        intersection_counts\[func_id\] = 0\.0\n                        if algo == "unweighted_cosine":\n                            shared_target_norm_sq\[func_id\] = 0\.0\n                        num_candidates \+= 1',
    r'                    if not is_existing:\n                        intersection_counts[func_id] = 0.0\n                        num_candidates += 1',
    code
)

# 4. Update the actual increment
old_4 = """                    if algo == "jaccard":
                        intersection_counts[func_id] += min(feat["tf"], cand_tf)
                    elif algo == "unweighted_cosine":
                        intersection_counts[func_id] += feat["tf"] * cand_tf
                        shared_target_norm_sq[func_id] += target_tf_sq"""

new_4 = """                    if algo == "jaccard":
                        intersection_counts[func_id] += min(feat["tf"], cand_tf)
                    elif algo == "unweighted_cosine":
                        intersection_counts[func_id] += 1"""
                        
code = code.replace(old_4, new_4)

# 5. Update Phase 1 filter bound
old_5 = """            elif algo == "unweighted_cosine":
                if shared_target_norm_sq.get(cid, 0) < min_shared_norm_sq:
                    continue"""

new_5 = """            elif algo == "unweighted_cosine":
                if intersect < min_shared_features:
                    continue"""
                    
code = code.replace(old_5, new_5)

# 6. Update candidate score calc
old_6 = """        else:  # unweighted_cosine — norm only fetched for phase-2 survivors
            need_norm = []
            for cid, cand_total in zip(kept, totals):
                if cand_total < min_features or cand_total <= 0:
                    continue
                intersect = intersection_counts[cid]
                denom = threshold * target_norm
                max_cand_total = (intersect / denom) ** 2 if denom > 0 else 0
                if cand_total <= max_cand_total:
                    need_norm.append((cid, intersect, cand_total))
            if need_norm:
                norms = self._norms([cid for cid, _, _ in need_norm])
                for (cid, intersect, cand_total), cand_norm in zip(need_norm, norms):
                    score = (
                        intersect / (target_norm * cand_norm)
                        if (target_norm > 0 and cand_norm > 0)
                        else 0
                    )
                    if score >= threshold and score > 0:
                        candidate_list.append((cid, score, cand_total))"""

new_6 = """        else:  # unweighted_cosine — we can compute exact binary cosine right here
            for cid, cand_total in zip(kept, totals):
                if cand_total < min_features or cand_total <= 0:
                    continue
                intersect = intersection_counts[cid]
                import math
                score = (
                    intersect / math.sqrt(target_len * cand_total)
                    if (target_len > 0 and cand_total > 0)
                    else 0
                )
                if score >= threshold and score > 0:
                    candidate_list.append((cid, score, cand_total))"""
                    
code = code.replace(old_6, new_6)

# 7. Update calculate_exact_score
old_7 = """            elif algo == "unweighted_cosine":
                # TF-weighted Cosine: sum(a*b) / (sqrt(sum(a^2)) * sqrt(sum(b^2)))
                dot_product = sum(d1[h] * d2[h] for h in common)
                norm1 = math.sqrt(sum(v**2 for v in d1.values()))
                norm2 = math.sqrt(sum(v**2 for v in d2.values()))
                return (
                    float(dot_product / (norm1 * norm2))
                    if (norm1 > 0 and norm2 > 0)
                    else 0.0
                )"""

new_7 = """            elif algo == "unweighted_cosine":
                # True Binary Unweighted Cosine
                dot_product = len(common)
                norm1 = math.sqrt(len(d1))
                norm2 = math.sqrt(len(d2))
                return (
                    float(dot_product / (norm1 * norm2))
                    if (norm1 > 0 and norm2 > 0)
                    else 0.0
                )"""

code = code.replace(old_7, new_7)

with open("bsimvis/app/services/similarity_service.py", "w") as f:
    f.write(code)
print("Updated similarity_service.py")
