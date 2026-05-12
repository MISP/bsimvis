import requests
import json


def run_cluster(host, port, args):
    """
    Thin client for clustering operations via API.
    """
    api_url = f"http://{host}:{port}/api/cluster"
    coll = args.collection

    if args.action == "build":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "min_cluster_size": args.min_cluster_size,
            "min_samples": args.min_samples,
            "epsilon": args.epsilon,
            "selection_method": "leaf" if args.leaf_method else "eom",
            "min_sim": args.min_sim,
        }
        try:
            print(f"[*] Enqueuing HDBSCAN clustering job for {coll}...")
            resp = requests.post(f"{api_url}/build", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Clustering failed: {e}")

    elif args.action == "rebuild":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
            "min_cluster_size": args.min_cluster_size,
            "min_samples": args.min_samples,
            "epsilon": args.epsilon,
            "selection_method": "leaf" if args.leaf_method else "eom",
            "min_sim": args.min_sim,
        }
        try:
            print(f"[*] Enqueuing cluster REBUILD pipeline for {coll}...")
            resp = requests.post(f"{api_url}/rebuild", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Pipeline ID: {resp.json().get('pipeline_id')}")
        except Exception as e:
            print(f"[!] Rebuild failed: {e}")

    elif args.action == "clear":
        payload = {
            "collection": coll,
            "algo": args.algo or "unweighted_cosine",
        }
        try:
            print(f"[*] Enqueuing cluster clear job for {coll}...")
            resp = requests.post(f"{api_url}/clear", json=payload)
            resp.raise_for_status()
            print(f"[+] Success! Job ID: {resp.json().get('job_id')}")
        except Exception as e:
            print(f"[!] Clear failed: {e}")

    elif args.action == "list":
        if args.cluster_id:
            # Show members of a specific cluster
            params = {
                "collection": coll,
                "algo": args.algo or "unweighted_cosine",
                "cluster_id": args.cluster_id,
                "limit": args.limit,
                "offset": args.offset,
            }
            try:
                resp = requests.get(f"{api_url}/members", params=params)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                print(
                    f"[*] Members for {args.cluster_id} (Total: {data.get('total')}):"
                )
                print(f"{'Function ID':<60} | {'Name':<30}")
                print("-" * 95)
                for res in results:
                    name = res.get("meta", {}).get("name", "N/A")
                    print(f"{res.get('id'):<60} | {name:<30}")
            except Exception as e:
                print(f"[!] Error fetching cluster members: {e}")
        else:
            # List all clusters
            params = {"collection": coll, "algo": args.algo or "unweighted_cosine"}
            try:
                resp = requests.get(f"{api_url}/list", params=params)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                print(f"[*] Discovered Clusters for {coll} ({data.get('algo')}):")
                print(f"{'Cluster ID':<20} | {'Size':<10}")
                print("-" * 35)
                for res in results:
                    print(f"{res.get('cluster_id'):<20} | {res.get('count'):<10}")
            except Exception as e:
                print(f"[!] Error fetching clusters: {e}")
