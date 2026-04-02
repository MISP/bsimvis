import requests
import json
import time
import sys
import os

def run_features(host, port, args):
    """
    Thin client for feature indexing via API.
    """
    api_url = f"http://{host}:{port}/api/features"
    coll = args.collection

    if args.action == "status":
        params = {"collection": coll}
        if args.batch: params["batch"] = args.batch
        if args.md5: params["md5"] = args.md5
        
        try:
            resp = requests.get(f"{api_url}/status", params=params)
            resp.raise_for_status()
            data = resp.json()
            print(f"[*] Index Status for {coll}:")
            target_str = f"Batch {args.batch}" if args.batch else (f"MD5 {args.md5}" if args.md5 else "Collection")
            print(f"    - Target:          {target_str}")
            print(f"    - Total functions: {data.get('total')}")
            print(f"    - Indexed:         {data.get('indexed')}")
            print(f"    - Missing:         {data.get('unindexed')}")
            print(f"    - Completion:      {data.get('ratio'):.1f}%")
        except Exception as e:
            print(f"[!] Error getting status: {e}")

    elif args.action == "list":
        # Check if user wants list by md5 or batch (default)
        if hasattr(args, 'md5') and args.md5:
            endpoint = f"{api_url}/files"
            header = f"{'File MD5':<34} | {'File Name':<36} | {'Funcs':<8} | {'Indexed':<8} | {'Ratio':<6}"
            format_str = "{file_md5:<34} | {name:<36} | {total:<8} | {indexed:<8} | {ratio:>5.1f}%"
        else:
            endpoint = f"{api_url}/status"
            header = f"{'Batch UUID':<40} | {'Name':<30} | {'Src Funcs':<10} | {'Indexed':<8} | {'Ratio':<6}"
            format_str = "{batch_uuid:<40} | {name:<30} | {total:<10} | {indexed:<8} | {ratio:>5.1f}%"

        params = {"collection": coll, "details": "true"}
        if args.batch: params["batch"] = args.batch
        
        try:
            resp = requests.get(endpoint, params=params)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            
            print(f"\n[*] Indexing Status for Collection: {coll}")
            print(header)
            print("-" * len(header))
            
            for res in results:
                # Truncate strings to fit
                if "name" in res:
                    res["name"] = str(res["name"])[:30] if "batch_uuid" in res else str(res["name"])[:36]
                print(format_str.format(**res))
        except Exception as e:
            print(f"[!] Error listing: {e}")

    elif args.action == "build":
        payload = {"collection": coll}
        if args.batch: payload["batch"] = args.batch
        if args.md5: payload["md5"] = args.md5
        
        try:
            print(f"[*] Enqueuing build job for {coll}...")
            resp = requests.post(f"{api_url}/index", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Build failed: {e}")

    elif args.action == "clear":
        payload = {"collection": coll}
        if args.batch: payload["batch"] = args.batch
        if args.md5: payload["md5"] = args.md5
        
        try:
            print(f"[*] Enqueuing clear job for {coll}...")
            resp = requests.post(f"{api_url}/clear", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Clear failed: {e}")

    elif args.action == "reindex":
        print("[!] Reindex (secondary indexing) not yet exposed via Job API.")
