import requests
import json
import time
import sys

API_BASE = "http://localhost:5000/api"

def run_job(host, port, args):
    if args.action == "list":
        list_jobs(args.limit)
    elif args.action == "status":
        job_status(args.job_id, args.watch, args.logs)
    elif args.action == "cancel":
        cancel_job(args.job_id)

def list_jobs(limit):
    try:
        resp = requests.get(f"{API_BASE}/jobs", params={"limit": limit})
        resp.raise_for_status()
        jobs = resp.json()
        
        print(f"{'ID':<30} | {'TYPE':<15} | {'STATUS':<10} | {'PROGRESS':<8} | {'CREATED'}")
        print("-" * 85)
        for j in jobs:
            created = time.strftime('%Y-%m-%d %H:%M', time.localtime(j['created_at']/1000))
            print(f"{j['id']:<30} | {j['type']:<15} | {j['status']:<10} | {j['progress']:>7}% | {created}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)

def job_status(job_id, watch, logs):
    try:
        while True:
            resp = requests.get(f"{API_BASE}/jobs/{job_id}")
            resp.raise_for_status()
            job = resp.json()
            
            if not watch:
                print(json.dumps(job, indent=2))
                return

            # Watch mode
            print("\033[H\033[J", end="") # Clear screen
            print(f"Job: {job['id']} ({job['type']})")
            print(f"Status: {job['status']}")
            print(f"Progress: {job['progress']}%")
            print("-" * 40)
            
            if "sub_tasks" in job:
                print("Sub-tasks:")
                for st in job["sub_tasks"]:
                    print(f"  - {st['type']:<15}: {st['status']:<10} ({st['progress']}%)")
                print("-" * 40)

            if logs and job.get("logs"):
                print("Recent Logs:")
                for log in reversed(job["logs"][:10]):
                    print(f"  {log}")

            if job["status"] in ["completed", "failed", "cancelled"]:
                print("\nJob finished.")
                break
                
            time.sleep(2)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)

def cancel_job(job_id):
    try:
        resp = requests.post(f"{API_BASE}/jobs/{job_id}/cancel")
        resp.raise_for_status()
        print(f"Job {job_id} cancellation requested.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
