import subprocess
import time
import sys
import signal
import os

def run_worker(host, port, args):
    if args.action == "start":
        start_workers(args.count)

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
        for i in range(count):
            name = f"worker-{i+1}"
            cmd = ["uv", "run", worker_script, "--name", name]
            print(f"  [+] Spawning {name}: {' '.join(cmd)}")
            
            p = subprocess.Popen(cmd)
            processes.append(p)
        
        print(f"[*] {count} workers are running. Press Ctrl+C to stop all.")
        
        # Keep alive and monitor
        while True:
            time.sleep(1)
            # Check if any process died
            for p in processes:
                if p.poll() is not None:
                    print(f"[!] Worker process {p.pid} exited with code {p.returncode}")
                    # In a production environment, we might want to restart it
    
    except KeyboardInterrupt:
        print("\n[*] Stopping all workers...")
        for p in processes:
            p.terminate()
        
        # Wait for them to finish
        for p in processes:
            p.wait()
        print("[*] All workers stopped.")
    except Exception as e:
        print(f"[!] Error: {e}")
        for p in processes:
            p.terminate()
        sys.exit(1)
