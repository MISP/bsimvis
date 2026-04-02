import requests
import json
import os
import sys

def format_bytes(b):
    if b < 1:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"

def run_index_status(host, port, args):
    """
    Thin client for index status via API.
    """
    api_url = f"http://{host}:{port}/api/index/status"
    params = {
        "collection": args.collection,
        "details": "true" if args.details else "false"
    }

    try:
        resp = requests.get(api_url, params=params)
        resp.raise_for_status()
        data = resp.json()

        if not args.details:
            print(f"\n[*] Index Status for Collection: {args.collection}")
            print(f"    - Files           : {data.get('num_files')}")
            print(f"    - Functions       : {data.get('num_functions')} ({data.get('num_indexed')} indexed / {data.get('num_missing')} missing)")
            print(f"    - Unique Features : {data.get('num_features')}")
            if data.get("num_sim_meta", 0) > 0:
                print(f"    - Sim Metadata    : {data.get('num_sim_meta')}")
            print(f"\n[i] Use --details for comprehensive space and size analysis.")
        else:
            summary = data.get("summary", {})
            print(f"\n[*] Index Status for Collection: {args.collection}")
            print(f"    - Files           : {summary.get('num_files')}")
            print(f"    - Functions       : {summary.get('num_functions')} ({summary.get('num_indexed')} indexed / {summary.get('num_missing')} missing)")
            print(f"    - Unique Features : {summary.get('num_features')}")
            
            print(f"\n[+] Detailed Index Analysis (Approximate):")
            print(f"    {'Component':<20} | {'Avg Size':<12} | {'Total Est.':<12}")
            print("-" * 60)
            
            total_est = 0
            for comp in data.get("components", []):
                total_est += comp.get("total_size", 0)
                print(f"    {comp['name']:<20} | {format_bytes(comp['avg_size']):<12} | {format_bytes(comp['total_size']):<12}")
            
            print("-" * 60)
            print(f"    {'ESTIMATED TOTAL':<20} | {'':<12} | {format_bytes(total_est):<12}")

    except Exception as e:
        print(f"[!] Error getting index status: {e}")
