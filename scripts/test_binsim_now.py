"""End-to-end checks for scripts/binsim_now.py, on a throwaway Redis it starts itself.

binsim_now reaches directly into the live job queue, so what matters is not that
it works but that it cannot hurt a running fleet. Each check below is one way it
could:

  1. starting BinSim while a binary is still being analyzed,
  2. blocking forever on a job whose worker died (status stays "running"),
  3. leaving the fleet paused,
  4. disturbing queue entries that belong to someone else,
  5. running a second pass on top of one already going.

Run: uv run scripts/test_binsim_now.py
"""

import json
import os
import socket
import subprocess
import sys
import threading
import time

PORT = 7999
COLL = "TestColl"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port(start):
    for p in range(start, start + 50):
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", p)) != 0:
                return p
    raise RuntimeError("no free port")


def run(*args):
    return subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "binsim_now.py"), COLL,
         "--redis", f"127.0.0.1:{PORT}", "--algo", "unweighted_cosine", *args],
        capture_output=True, text=True, cwd=ROOT,
    )


def main():
    global PORT
    PORT = free_port(PORT)
    tmp = os.path.join(os.getenv("TMPDIR", "/tmp"), f"binsim_now_test_{PORT}")
    os.makedirs(tmp, exist_ok=True)
    subprocess.run(["redis-server", "--port", str(PORT), "--save", "", "--appendonly", "no",
                    "--daemonize", "yes", "--dir", tmp, "--logfile", os.path.join(tmp, "r.log")],
                   check=True)
    try:
        _checks()
    finally:
        subprocess.run(["redis-cli", "-p", str(PORT), "shutdown", "nosave"],
                       capture_output=True)


def _checks():
    sys.path.insert(0, os.path.join(ROOT, "scripts"))
    from bsimvis.app.services.redis_client import init_redis

    init_redis(host="127.0.0.1", redis_port=PORT)
    from bsimvis.app.services.job_service import JobService
    import binsim_now as b

    for _ in range(20):
        try:
            JobService().r.ping()
            break
        except Exception:
            time.sleep(0.2)
    js = JobService()
    r = js.r
    r.flushdb()

    def mkjob(jid, jtype, status, leased=True):
        r.hset(f"job:{jid}", mapping={
            "id": jid, "type": jtype, "status": status, "collection": COLL,
            "payload": json.dumps({"collection": COLL})})
        if status == "running":
            r.rpush("jobs:processing", jid)
            r.sadd("jobs:idx:status:running", jid)
            if leased:
                r.zadd("jobs:leased", {jid: time.time() + 60})

    mkjob("live1", "ghidra_analyze", "running")
    mkjob("dead1", "ghidra_analyze", "running", leased=False)
    r.rpush("jobs:pending", "backlog1", "backlog2")
    before_pending = r.lrange("jobs:pending", 0, -1)

    # 2. a job whose lease expired is a corpse the reaper owns, not something to wait on
    assert [t for _, t in b.running_for(r, COLL)] == ["ghidra_analyze"], "corpse counted as in-flight"

    def release():
        time.sleep(3)
        r.hset("job:live1", "status", "completed")
        r.lrem("jobs:processing", 0, "live1")

    threading.Thread(target=release, daemon=True).start()
    t0 = time.time()
    out = run("--drain-timeout", "60")
    elapsed = time.time() - t0
    assert out.returncode == 0, out.stderr
    # 1. it really waited for the analysis to finish
    assert elapsed >= 3, f"did not wait for the in-flight job ({elapsed:.1f}s)"
    assert "paused the fleet" in out.stdout and "fleet resumed" in out.stdout, out.stdout
    # 3 + 4
    assert not r.exists("jobs:paused"), "fleet left paused"
    assert r.lrange("jobs:pending", 0, -1) == before_pending, "existing queue disturbed"
    assert not r.keys(f"lane:{COLL}:*"), "wrote lane state"

    high = r.lrange("jobs:pending:high", 0, -1)
    assert len(high) == 1, high
    first = r.hgetall(f"job:{high[0]}")
    assert first["type"] == "build_bin_sim", first
    pipe_id = first["parent_id"]
    tids = json.loads(r.hget(f"job:{pipe_id}", "task_ids"))
    stages = [r.hget(f"job:{t}", "type") for t in tids]
    assert stages == ["build_bin_sim", "cluster_binaries", "index_sim", "cluster_functions"], stages
    assert not any(s.startswith("clear_") for s in stages), stages
    # knobs we do not pin stay absent, so the worker reads the server's own config
    assert "min_cluster_size" not in json.loads(r.hget(f"job:{tids[3]}", "payload"))

    # 5. refuses while one of its own stages is running
    r.hset(f"job:{high[0]}", "status", "running")
    r.sadd("jobs:idx:status:running", high[0])
    out2 = run()
    assert out2.returncode == 1 and "refusing" in out2.stderr, (out2.returncode, out2.stderr)
    assert not r.exists("jobs:paused")

    # 3 again, the path that matters most: a drain that never completes must
    # still hand the fleet back, and must queue nothing.
    r.flushdb()
    mkjob("stuck", "ghidra_analyze", "running")
    out3 = run("--drain-timeout", "2")
    assert out3.returncode == 1, out3.stdout
    assert "timed out" in out3.stderr, out3.stderr
    assert not r.exists("jobs:paused"), "timeout path left the fleet paused"
    assert r.llen("jobs:pending:high") == 0, "queued work after aborting"

    print("binsim_now checks OK")


if __name__ == "__main__":
    main()
