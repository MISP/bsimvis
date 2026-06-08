import requests
import sys


def run_collection(host, port, args):
    """
    Thin client for collection operations via API.
    """
    api_url = f"http://{host}:{port}/api/collection"
    coll = args.collection

    if args.action == "delete":
        # Interactive confirmation prompt to prevent accidental deletions
        print(f"⚠️  WARNING: You are about to permanently delete all data for collection '{coll}'.")
        print("This action is destructive and cannot be undone.")
        try:
            confirm = input(f"Are you sure you want to proceed? [y/N]: ").strip().lower()
        except KeyboardInterrupt:
            print("\n[*] Aborted.")
            sys.exit(0)

        if confirm not in ("y", "yes"):
            print("[*] Aborted.")
            sys.exit(0)

        try:
            print(f"[*] Enqueuing delete job for collection '{coll}'...")
            resp = requests.post(f"{api_url}/delete", json={"collection": coll})
            resp.raise_for_status()
            res = resp.json()
            print(f"[+] Success! Job ID: {res.get('job_id')}")
        except Exception as e:
            print(f"[!] Delete collection failed: {e}")

    elif args.action == "clean":
        try:
            print(f"[*] Enqueuing clean job for collection '{coll}'...")
            resp = requests.post(f"{api_url}/clean", json={"collection": coll})
            resp.raise_for_status()
            res = resp.json()
            print(f"[+] Success! Job ID: {res.get('job_id')}")
        except Exception as e:
            print(f"[!] Clean collection failed: {e}")

