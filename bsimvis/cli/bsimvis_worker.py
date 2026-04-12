import subprocess
import time
import sys
import signal
import os
import json
from bsimvis.app.services.redis_client import get_queue_redis
from bsimvis.app.services.job_service import JobStatus

def run_worker(host, port, args):
    if args.action == "start":
        # 1. Reset all running jobs as requested
        rescue_jobs()
        
        # 2. Start the workers
        start_workers(args.count)

def rescue_jobs():
    """Moves jobs from 'processing' back to 'pending' if workers are being restarted."""
    r = get_queue_redis()
    processing_jobs = r.lrange("jobs:processing", 0, -1)
    
    if not processing_jobs:
        return

    print(f"[*] Found {len(processing_jobs)} jobs in processing queue. Rescuing...")
    
    for job_id in processing_jobs:
        # 1. Update status to pending
        timestamp = int(time.time() * 1000)
        r.hset(f"job:{job_id}", mapping={
            "status": JobStatus.PENDING.value,
            "updated_at": timestamp
        })
        
        # 2. Add log entry
        log_entry = f"[{timestamp}] Job rescued from stale worker processing queue and returned to pending."
        r.lpush(f"job_log:{job_id}", log_entry)
        
        # 3. Move back to pending queue
        # Use a transaction or pipeline for atomicity
        pipe = r.pipeline()
        pipe.lpush("jobs:pending", job_id)
        pipe.lrem("jobs:processing", 1, job_id)
        pipe.execute()
        
        print(f"  [+] Rescued job: {job_id}")

    print("[*] Job rescue complete.")

def start_workers(count):
    processes = []
    print(f"[*] Starting {count} workers...")
    
    # Get the project root directory
    # bsimvis/cli/bsimvis_worker_cli.py -> bsimvis/
    cli_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(cli_dir)
    worker_script = os.path.join(project_root, "worker.py")
    
    if not os.path.exists(worker_script):
        # Maybe we are in a different structure
        worker_script = "bsimvis/worker.py"

    try:
        # processes dict: name -> subprocess.Popen object
        worker_map = {}
        
        for i in range(count):
            name = f"worker-{i+1}"
            cmd = ["uv", "run", worker_script, "--name", name]
            print(f"  [+] Spawning {name}: {' '.join(cmd)}")
            
            p = subprocess.Popen(cmd)
            worker_map[name] = p
        
        print(f"[*] {count} workers are running. Press Ctrl+C to stop all.")
        
        # Keep alive and monitor
        while True:
            time.sleep(2)
            # Check if any process died
            for name, p in list(worker_map.items()):
                exit_code = p.poll()
                if exit_code is not None:
                    print(f"[!] Worker {name} (PID {p.pid}) exited with code {exit_code}")
                    
                    # AUTO-RESTART
                    print(f"[*] Restarting {name} in 2 seconds...")
                    time.sleep(2)
                    
                    cmd = ["uv", "run", worker_script, "--name", name]
                    new_p = subprocess.Popen(cmd)
                    worker_map[name] = new_p
                    print(f"  [+] {name} restarted with PID {new_p.pid}")
    
    except KeyboardInterrupt:
        print("\n[*] Stopping all workers...")
        for name, p in worker_map.items():
            p.terminate()
        
        # Wait for them to finish
        for name, p in worker_map.items():
            p.wait()
        print("[*] All workers stopped.")
    except Exception as e:
        print(f"[!] Error in supervisor: {e}")
        for name, p in worker_map.items():
            p.terminate()
        sys.exit(1)
