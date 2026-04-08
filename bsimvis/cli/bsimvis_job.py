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
    elif args.action == "perf":
        job_perf(args.job_id, args.top)

def job_perf(job_id, top_n=10):
    try:
        resp = requests.get(f"{API_BASE}/jobs/{job_id}")
        resp.raise_for_status()
        job = resp.json()

        print(f"\n=== PERFORMANCE REPORT: {job_id} ===")
        print(f"Type:   {job.get('type')}")
        print(f"Status: {job.get('status')}")
        print("-" * 85)

        all_ops = []

        # Handle Pipeline/Sub-tasks
        if "sub_tasks" in job and job["sub_tasks"]:
            print(f"{'Sub-Task Type':<20} | {'Total (s)':>10} | {'Python':>10} | {'DB':>10} | {'Lua':>10}")
            print("-" * 85)
            
            agg_total = 0.0
            agg_python = 0.0
            agg_db = 0.0
            agg_lua = 0.0
            
            for st in job["sub_tasks"]:
                t = float(st.get("perf_total", 0))
                p = float(st.get("perf_python", 0))
                d = float(st.get("perf_db", 0))
                l = float(st.get("perf_lua", 0))
                
                agg_total += t
                agg_python += p
                agg_db += d
                agg_lua += l
                
                print(f"{st['type']:<20} | {t:>10.4f} | {p:>10.4f} | {d:>10.4f} | {l:>10.4f}")
                
                # Fetch full details for each sub-task to aggregate operations
                try:
                    st_resp = requests.get(f"{API_BASE}/jobs/{st['id']}")
                    if st_resp.status_code == 200:
                        st_full = st_resp.json()
                        details = st_full.get("perf_details", [])
                        for op in details:
                            op["task_type"] = st["type"]
                            all_ops.append(op)
                except:
                    pass
            
            print("-" * 85)
            print(f"{'GRAND TOTAL':<20} | {agg_total:>10.4f} | {agg_python:>10.4f} | {agg_db:>10.4f} | {agg_lua:>10.4f}")
            
            if agg_total > 0:
                print(f"{'PERCENTAGE':<20} | {'100%':>10} | {agg_python/agg_total*100:>9.1f}% | {agg_db/agg_total*100:>9.1f}% | {agg_lua/agg_total*100:>9.1f}%")

        else:
            # Single job stats
            t = float(job.get("perf_total", 0))
            p = float(job.get("perf_python", 0))
            d = float(job.get("perf_db", 0))
            l = float(job.get("perf_lua", 0))
            
            if t == 0 and job.get("status") == "pending":
                print("Job is still pending. No performance data available yet.")
                return

            print(f"Total Time:    {t:.4f} s")
            print(f"Pure Python:   {p:.4f} s ({p/t*100 if t>0 else 0:.1f}%)")
            print(f"DB Queries:    {d:.4f} s ({d/t*100 if t>0 else 0:.1f}%)")
            print(f"Lua Scripts:   {l:.4f} s ({l/t*100 if t>0 else 0:.1f}%)")
            print(f"Operations:    {job.get('perf_ops', 'N/A')}")
            
            details = job.get("perf_details", [])
            for op in details:
                op["task_type"] = job["type"]
                all_ops.append(op)
        
        # Display Top Demanding Commands
        if all_ops:
            print(f"\nTOP {top_n} MOST DEMANDING COMMANDS")
            print("-" * 85)
            print(f"{'Task Source':<15} | {'Command':<25} | {'Duration (s)':>12} | {'Category':<15}")
            print("-" * 85)
            
            # Sort by execution time descending
            sorted_ops = sorted(all_ops, key=lambda x: x.get("time", 0), reverse=True)
            for op in sorted_ops[:top_n]:
                duration = op.get("time", 0)
                print(f"{op.get('task_type', 'N/A'):<15} | {op.get('op', 'N/A'):<25} | {duration:>12.6f} | {op.get('cat', 'N/A'):<15}")

    except Exception as e:
        print(f"Error fetching performance stats: {e}", file=sys.stderr)

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
            if not job_id:
                # GLOBAL STATS MODE
                resp = requests.get(f"{API_BASE}/jobs/stats")
                resp.raise_for_status()
                stats = resp.json()
                
                print("\033[H\033[J", end="") # Clear screen
                print("=== GLOBAL JOB STATUS ===")
                print(f"Active Workers: {stats['active_workers']}")
                print(f"Pending Jobs:   {stats['pending_jobs']}")
                print(f"Total Speed:    {stats['total_speed']} fn/s")
                print(f"Avg Speed:      {stats['avg_speed']} fn/s")
                print(f"Items Left:     {stats['remaining_items']}")
                print(f"Est. Global Time: {stats['global_eta']}s")
                print("-" * 30)
                
                if not watch:
                    return
                time.sleep(2)
                continue

            # INDIVIDUAL JOB MODE
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
            
            if "speed" in job:
                print(f"Speed: {job['speed']} fn/s")
            if "eta" in job:
                print(f"ETA: {job['eta']}s")
            
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
