with open("bsimvis/app/services/similarity_service.py", "r") as f:
    content = f.read()

old_code = """            if discovery_results:
                written = self._persist_and_index_batch(
                    collection,
                    algo,
                    discovery_results,
                    min_features=min_features,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
                total_sims = written or 0
            else:
                total_sims = 0"""

new_code = """            if discovery_results:
                written = self._persist_and_index_batch(
                    collection,
                    algo,
                    discovery_results,
                    min_features=min_features,
                    index_depth=index_depth,
                    skip_write=skip_write,
                )
                total_sims = written or 0
            else:
                total_sims = 0
                
            small_fids = [f for f, count in func_feat_counts.items() if count < min_features and f in target_func_set]
            if small_fids and not skip_write:
                written_small = self._hash_match_small(collection, algo, small_fids, index_depth)
                total_sims += written_small or 0"""

content = content.replace(old_code, new_code)
with open("bsimvis/app/services/similarity_service.py", "w") as f:
    f.write(content)
