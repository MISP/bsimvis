"""GhidraProject must not leak a 'File System Listener' thread per analysis.

An unclosed GhidraProject keeps its ProjectFileManager -> LocalFileSystem ->
FileSystemEventManager alive, and that class only stops its dispatch thread from
dispose(), reached via project.close(). Each leaked thread pins the project's
whole Ghidra object graph in the JVM heap, so worker RSS climbs until the host
runs out of memory.

Counts OS threads named 'File System Lis' (the kernel truncates comm to 15
chars) around repeated analyze_file() calls. Measured against the pre-fix code
this reports +1 per call; with project.close() in place it stays flat.

Needs a real JVM, so this is not part of the fast unit suite:

    .venv/bin/python test_ghidra_project_leak.py [target_binary] [rounds]
"""

import os
import sys
import time


def fs_threads():
    n = 0
    for t in os.listdir("/proc/self/task"):
        try:
            with open(f"/proc/self/task/{t}/comm") as fh:
                if fh.read().strip() == "File System Lis":
                    n += 1
        except OSError:
            pass
    return n


def main():
    target = sys.argv[1] if len(sys.argv) > 1 else "/bin/true"
    rounds = int(sys.argv[2]) if len(sys.argv) > 2 else 3

    from bsimvis.app.services.ghidra_service import ghidra_service

    ghidra_service.ensure_launcher(max_heap_mb=1536)

    print(f"target={target} rounds={rounds}", flush=True)
    baseline = fs_threads()
    print(f"baseline File System Lis threads: {baseline}", flush=True)

    counts = []
    for i in range(rounds):
        t0 = time.time()
        ghidra_service.analyze_file(target, {"profile": "fast"})
        n = fs_threads()
        counts.append(n)
        print(f"round {i+1}: {n} threads  ({time.time()-t0:.1f}s)", flush=True)

    print(f"\nbaseline={baseline} after={counts}", flush=True)
    growth = counts[-1] - baseline
    if growth == 0:
        print("PASS: no File System Listener threads leaked")
    else:
        print(f"LEAK: +{growth} threads over {rounds} analyses")
        sys.exit(1)


if __name__ == "__main__":
    main()
