import requests
import json


def run_binsim(host, port, args):
    """
    Thin client for binary similarity operations via API.
    """
    api_url = f"http://{host}:{port}/api/bin_sim"
    coll = args.collection

    if args.action == "build":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "md5_a": args.md5_a,
            "md5_b": args.md5_b,
            "min_cohesion": args.min_cohesion,
        }
        try:
            print(f"[*] Enqueuing Binary Similarity build job for {coll}...")
            resp = requests.post(f"{api_url}/build", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Build failed: {e}")

    elif args.action == "rebuild":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "md5_a": args.md5_a,
            "md5_b": args.md5_b,
            "min_cohesion": args.min_cohesion,
        }
        try:
            print(f"[*] Enqueuing Binary Similarity rebuild pipeline for {coll}...")
            resp = requests.post(f"{api_url}/rebuild", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Pipeline ID: {resp.json().get('pipeline_id')}")
        except Exception as e:
            print(f"[!] Rebuild failed: {e}")

    elif args.action == "clear":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "md5": args.md5,
        }
        try:
            print(f"[*] Enqueuing Binary Similarity clear job for {coll}...")
            resp = requests.post(f"{api_url}/clear", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Clear failed: {e}")

    elif args.action == "list":
        params = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "md5": args.md5,
            "limit": args.limit,
            "offset": args.offset,
        }
        try:
            resp = requests.get(f"{api_url}/list", params=params)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            print(f"[*] Similar binaries for {args.md5} (Total: {data.get('total')}):")
            print(f"{'Binary MD5':<35} | {'Score':<10} | {'Shared Clusters'}")
            print("-" * 75)
            for res in results:
                other_md5 = res.get("md5_b") if res.get("md5_a") == args.md5 else res.get("md5_a")
                print(f"{other_md5:<35} | {res.get('score_collection_weighted', 0.0):.4f}     | {res.get('shared_clusters')}")
        except Exception as e:
            print(f"[!] Error fetching similar binaries: {e}")
            
    elif args.action == "diff":
        params = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "md5_a": args.md5_a,
            "md5_b": args.md5_b,
        }
        try:
            resp = requests.get(f"{api_url}/diff", params=params)
            resp.raise_for_status()
            data = resp.json()
            print(json.dumps(data, indent=2))
        except Exception as e:
            print(f"[!] Error fetching diff: {e}")
