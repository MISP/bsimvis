import subprocess
import time
import sys
import signal
import os
import json
from dotenv import load_dotenv
from bsimvis.app.services.job_service import JobService

# Load environment variables
load_dotenv()


def run_worker(host, port, args):
    if args.action == "start":
        # 1. Recover anything a dead worker was holding
        rescue_jobs()

        # 2. Start the workers
        start_workers(args.count)


def rescue_jobs():
    """Requeues jobs whose lease expired, i.e. whose worker died.

    This used to sweep everything in jobs:processing back to pending, including
    jobs that live workers were still running -- which then executed twice. The
    lease reaper can tell the two apart, so only genuinely stranded jobs move.
    """
    requeued, failed, cleaned = JobService().reap_expired()
    if requeued or failed or cleaned:
        print(
            f"[*] Recovered: {requeued} requeued, {failed} failed (attempt limit), "
            f"{cleaned} stale entries cleared."
        )


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
                    print(
                        f"[!] Worker {name} (PID {p.pid}) exited with code {exit_code}"
                    )

                    # AUTO-RESTART
                    print(f"[*] Restarting {name} in 2 seconds...")
                    time.sleep(2)

                    cmd = ["uv", "run", worker_script, "--name", name]
                    new_p = subprocess.Popen(cmd)
                    worker_map[name] = new_p
                    print(f"  [+] {name} restarted with PID {new_p.pid}")

    except (KeyboardInterrupt, SystemExit):
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
