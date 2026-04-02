import requests
import json
import time
import argparse

def run_sim(host, port, args):
    """
    Thin client for similarity operations via API.
    """
    api_url = f"http://{host}:{port}/api/similarity"
    coll = args.collection

    if args.action == "status":
        params = {"collection": coll, "algo": args.algo or "unweighted_cosine"}
        md5_val = args.md5[0] if (args.md5 and isinstance(args.md5, list)) else args.md5
        if md5_val: params["md5"] = md5_val
        if args.batch: params["batch"] = args.batch
        
        try:
            resp = requests.get(f"{api_url}/status", params=params)
            resp.raise_for_status()
            data = resp.json()
            print(f"[*] Similarity Build Status for: {coll} ({data.get('algo')})")
            target_str = f"Batch {args.batch}" if args.batch else (f"MD5 {md5_val}" if md5_val else "Collection")
            print(f"    Target:      {target_str}")
            print(f"    Total:       {data.get('total')}")
            print(f"    Built:       {data.get('built')}")
            print(f"    Missing:     {data.get('unbuilt', data.get('missing'))}")
            print(f"    Completion:  {data.get('ratio'):.1f}%")
        except Exception as e:
            print(f"[!] Error getting similarity status: {e}")

    elif args.action == "list":
        # Check if user wants list by md5 or batch (default)
        if hasattr(args, 'md5') and args.md5:
            params = {"collection": coll, "algo": args.algo or "unweighted_cosine", "by": "md5"}
            header = f"{'File MD5':<34} | {'File Name':<36} | {'Funcs':<8} | {'Built':<8} | {'Ratio':<6}"
            format_str = "{file_md5:<34} | {name:<36} | {total:<8} | {built:<8} | {ratio:>5.1f}%"
        else:
            params = {"collection": coll, "algo": args.algo or "unweighted_cosine", "by": "batch"}
            header = f"{'Batch UUID':<40} | {'Name':<30} | {'Src Funcs':<10} | {'Built':<8} | {'Ratio':<6}"
            format_str = "{batch_uuid:<40} | {name:<30} | {total:<10} | {built:<8} | {ratio:>5.1f}%"
        
        try:
            resp = requests.get(f"{api_url}/batches", params=params)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            
            print(f"\n[*] Similarity Build Status for Collection: {coll}")
            print(header)
            print("-" * len(header))
            
            for res in results:
                # Truncate strings to fit
                if "name" in res:
                    res["name"] = str(res["name"])[:30] if "batch_uuid" in res else str(res["name"])[:36]
                print(format_str.format(**res))
        except Exception as e:
            print(f"[!] Error listing: {e}")

    elif args.action == "scores":
        params = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "limit": getattr(args, 'top_k', 20) or 20
        }
        md5_val = args.md5[0] if (args.md5 and isinstance(args.md5, list)) else args.md5
        if md5_val: params["md5"] = md5_val
        elif args.batch: params["batch"] = args.batch
            
        try:
            resp = requests.get(f"{api_url}/list", params=params)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            print(f"[*] Found {data.get('total')} similarities (showing first {len(results)})")
            for res in results:
                print(f"  {res.get('score'):.4f} | {res.get('name1')} <-> {res.get('name2')}")
        except Exception as e:
            print(f"[!] Error listing similarity scores: {e}")

    elif args.action == "build":
        targets = []
        if args.md5: targets.extend([{"md5": m} for m in args.md5])
        if args.batch: targets.append({"batch": args.batch})
        
        for target in targets:
            payload = {
                "collection": coll, "algo": args.algo or "unweighted_cosine",
                "top_k": args.top_k, "min_score": args.min_score, **target
            }
            try:
                print(f"[*] Enqueuing build job for {target}...")
                resp = requests.post(f"{api_url}/build", json=payload)
                resp.raise_for_status()
                print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
            except Exception as e:
                print(f"[!] Build failed for {target}: {e}")

    elif args.action == "rebuild":
        targets = []
        if args.md5: targets.extend([{"md5": m} for m in args.md5])
        if args.batch: targets.append({"batch": args.batch})
        
        for target in targets:
            payload = {
                "collection": coll, "algo": args.algo or "unweighted_cosine",
                "top_k": args.top_k, "min_score": args.min_score, **target
            }
            try:
                print(f"[*] Enqueuing REBUILD pipeline for {target}...")
                resp = requests.post(f"{api_url}/rebuild", json=payload)
                resp.raise_for_status()
                print(f"[+] Success! Pipeline ID: {resp.json().get('pipeline_id')}")
            except Exception as e:
                print(f"[!] Rebuild failed for {target}: {e}")

    elif args.action == "clear":
        targets = []
        if args.md5: targets.extend([{"md5": m} for m in args.md5])
        if args.batch: targets.append({"batch": args.batch})
        
        for target in targets:
            payload = { "collection": coll, "algo": args.algo or "unweighted_cosine", **target }
            try:
                print(f"[*] Enqueuing clear job for {target}...")
                resp = requests.post(f"{api_url}/clear", json=payload)
                resp.raise_for_status()
                print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
            except Exception as e:
                print(f"[!] Clear failed for {target}: {e}")
