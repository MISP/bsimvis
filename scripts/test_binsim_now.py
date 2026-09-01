"""End-to-end checks for scripts/binsim_now.py, on a throwaway Redis it starts itself.

binsim_now reaches directly into the live job queue, so what matters is not that
it works but that it cannot hurt a running fleet. Each check below is one way it
could:

  1. starting BinSim while a binary is still being analyzed,
  2. blocking forever on a job whose worker died (status stays "running"),
  3. leaving the fleet paused, or holding queued jobs hostage after a failure,
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
    r = JobService().r
    STASH = b.stash_key(COLL)

    def mkjob(jid, jtype, status, coll=COLL, leased=True):
        r.hset(f"job:{jid}", mapping={
            "id": jid, "type": jtype, "status": status, "collection": coll,
            "payload": json.dumps({"collection": coll})})
        if status == "running":
            r.rpush("jobs:processing", jid)
            r.sadd("jobs:idx:status:running", jid)
            if leased:
                r.zadd("jobs:leased", {jid: time.time() + 60})

    def release_after(jid, seconds):
        def go():
            time.sleep(seconds)
            r.hset(f"job:{jid}", "status", "completed")
            r.lrem("jobs:processing", 0, jid)
        threading.Thread(target=go, daemon=True).start()

    def stage_names(first_id):
        pipe_id = r.hget(f"job:{first_id}", "parent_id")
        tids = json.loads(r.hget(f"job:{pipe_id}", "task_ids"))
        return [r.hget(f"job:{t}", "type") for t in tids], tids

    # --- 2. a lease that expired is a corpse the reaper owns, not something to
    # wait on: its status stays "running" for hours after the worker died.
    r.flushdb()
    mkjob("live1", "ghidra_analyze", "running")
    mkjob("dead1", "ghidra_analyze", "running", leased=False)
    assert [t for _, t in b.running_for(r, COLL)] == ["ghidra_analyze"], "corpse counted"

    # --- default (stash) path -------------------------------------------------
    # Only this collection's *queued analyses* may be held back. Another
    # collection's work, and this collection's in-flight indexing chain, stay.
    mkjob("other1", "ghidra_analyze", "pending", coll="OtherColl")
    mkjob("mine_a", "ghidra_analyze", "pending")
    mkjob("idxf", "index_features", "pending")
    mkjob("mine_b", "ghidra_analyze", "pending")
    r.rpush("jobs:pending", "other1", "mine_a", "idxf", "mine_b")

    release_after("live1", 3)
    t0 = time.time()
    out = run("--drain-timeout", "60")
    elapsed = time.time() - t0
    assert out.returncode == 0, out.stderr
    # 1. it waited for the analysis that was already under way
    assert elapsed >= 3, f"did not wait for the in-flight job ({elapsed:.1f}s)"
    assert "held back 2 queued analysis job(s)" in out.stdout, out.stdout
    # 3. the blunt instrument is not used unless asked for
    assert not r.exists("jobs:paused"), "stash path must not pause the fleet"
    assert "paused the fleet" not in out.stdout

    # 4. everything is back, nothing of anyone else's was ever taken. The two
    # held analyses return ahead of the untouched entries, which is the point:
    # BinSim is on the high queue by then, so they queue behind it either way.
    assert r.lrange("jobs:pending", 0, -1) == ["mine_a", "mine_b", "other1", "idxf"], \
        r.lrange("jobs:pending", 0, -1)
    assert not r.exists(STASH), "stash key outlived the run"

    high = r.lrange("jobs:pending:high", 0, -1)
    assert len(high) == 1, high
    assert r.hget(f"job:{high[0]}", "type") == "build_bin_sim"
    stages, tids = stage_names(high[0])
    assert stages == ["build_bin_sim", "cluster_binaries", "index_sim", "cluster_functions"], stages
    assert not any(s.startswith("clear_") for s in stages), stages
    # knobs we do not pin stay absent, so the worker reads the server's own config
    assert "min_cluster_size" not in json.loads(r.hget(f"job:{tids[3]}", "payload"))

    # --- 5. refuses while one of its own stages is running --------------------
    r.hset(f"job:{high[0]}", "status", "running")
    r.sadd("jobs:idx:status:running", high[0])
    out2 = run()
    assert out2.returncode == 1 and "refusing" in out2.stderr, (out2.returncode, out2.stderr)

    # --- 3. a drain that never finishes must hand everything back -------------
    r.flushdb()
    mkjob("stuck", "ghidra_analyze", "running")
    mkjob("q1", "ghidra_analyze", "pending")
    r.rpush("jobs:pending", "q1")
    out3 = run("--drain-timeout", "2")
    assert out3.returncode == 1 and "timed out" in out3.stderr, out3.stderr
    assert r.lrange("jobs:pending", 0, -1) == ["q1"], "held jobs not returned after abort"
    assert not r.exists(STASH)
    assert r.llen("jobs:pending:high") == 0, "queued work after aborting"

    # --- a stash left behind by a killed run is recoverable, and blocks -------
    r.flushdb()
    r.rpush(STASH, "orphan1", "orphan2")
    out4 = run()
    assert out4.returncode == 1 and "--restore" in out4.stderr, out4.stderr
    assert r.lrange(STASH, 0, -1) == ["orphan1", "orphan2"], "refusal must not consume it"
    out5 = run("--restore")
    assert out5.returncode == 0, out5.stderr
    assert r.lrange("jobs:pending", 0, -1) == ["orphan1", "orphan2"], r.lrange("jobs:pending", 0, -1)
    assert not r.exists(STASH)

    # --- --pause still works, and still always unpauses -----------------------
    r.flushdb()
    mkjob("live2", "ghidra_analyze", "running")
    release_after("live2", 2)
    out6 = run("--pause", "--drain-timeout", "60")
    assert out6.returncode == 0, out6.stderr
    assert "paused the fleet" in out6.stdout and "fleet resumed" in out6.stdout
    assert not r.exists("jobs:paused"), "fleet left paused"

    print("binsim_now checks OK")


if __name__ == "__main__":
    main()
