"""How much of the ghidra analyze job is Function ID?

Runs the analyze job's two timed phases (auto-analysis, then the
decompile+extract stream) four ways per binary, so the FID cost splits into
the analyzer and the per-function matching we added on top of it:

    off       Function ID analyzer disabled, no FID tag extraction  (baseline)
    analyzer  analyzer on, no tag extraction        -> analyzer  = analysis delta
    bookmark  + bookmark parsing only               -> parse     = stream delta
    full      + FidQueryService hash matching       -> hashquery = stream delta

Needs a JVM, so it is not part of the fast suite:

    GHIDRA_INSTALL_DIR=$PWD/bin/ghidra_12.1_PUBLIC \
      .venv/bin/python scripts/benchmark_fid_cost.py data/test/* [--rounds 2]
"""

import argparse
import copy
import logging
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

MODES = ["off", "analyzer", "bookmark", "full"]


def run_one(gs, orig_extract, target, mode, profile):
    """Import + analyze + stream one binary in one mode. Returns timings."""
    from ghidra.base.project import GhidraProject

    # analyzer off only for the baseline; extraction level per mode
    gs.config = copy.deepcopy(gs._base_config)
    if mode == "off":
        gs.config["profiles"][profile]["analyzers"]["Function ID"] = False

    if mode == "off" or mode == "analyzer":
        patch = lambda self, func, program, q=None, s=None: []
    elif mode == "bookmark":
        patch = lambda self, func, program, q=None, s=None: orig_extract(
            self, func, program, None, None
        )
    else:
        patch = orig_extract
    type(gs)._extract_fid_tags_for_function = patch

    with tempfile.TemporaryDirectory(prefix="bench_fid_") as temp_dir:
        project = GhidraProject.createProject(temp_dir, "BenchFid", False)
        try:
            t0 = time.time()
            program = project.importProgram(Path(target).resolve())
            t_import = time.time() - t0

            t0 = time.time()
            gs.run_profile_analysis(program, profile, force_reanalysis=True)
            t_analysis = time.time() - t0

            t0 = time.time()
            gen = gs.stream_bsim_data(program, {"profile": profile}, chunk_size=999999)
            next(gen)  # file_metadata
            funcs, tagged = 0, 0
            for chunk in gen:
                for f in chunk:
                    funcs += 1
                    if any(
                        t.startswith("lib:") for t in f["function_metadata"]["tags"]
                    ):
                        tagged += 1
            t_stream = time.time() - t0
        finally:
            project.close()

    return {
        "import": t_import,
        "analysis": t_analysis,
        "stream": t_stream,
        "total": t_import + t_analysis + t_stream,
        "funcs": funcs,
        "tagged": tagged,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("targets", nargs="+")
    ap.add_argument("--profile", default="fast")
    ap.add_argument("--rounds", type=int, default=1)
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING)
    from bsimvis.app.services.ghidra_service import GhidraService, ghidra_service as gs

    gs._base_config = copy.deepcopy(gs.config)
    assert args.profile in gs._base_config.get("profiles", {}), "profile not in config"
    orig_extract = GhidraService._extract_fid_tags_for_function

    gs.ensure_launcher(max_heap_mb=4096)

    # Discard one run: the first import/analysis pays JVM class loading and
    # the FID database open, which would otherwise land on the baseline.
    print("warmup...", flush=True)
    # "bookmark", not "full": the FID service is opened by stream_bsim_data in
    # every mode, so this warms it without paying the per-function queries.
    run_one(gs, orig_extract, args.targets[0], "bookmark", args.profile)

    results = {}  # (target, mode) -> best-of-rounds timings
    for target in args.targets:
        name = Path(target).name
        for mode in MODES:
            for r in range(args.rounds):
                t = run_one(gs, orig_extract, target, mode, args.profile)
                prev = results.get((name, mode))
                # min over rounds: least noise from JIT warmup / other load
                if prev is None or t["total"] < prev["total"]:
                    results[(name, mode)] = t
                print(
                    f"  {name:<22} {mode:<9} r{r+1} "
                    f"import={t['import']:6.1f}s analysis={t['analysis']:6.1f}s "
                    f"stream={t['stream']:6.1f}s total={t['total']:6.1f}s "
                    f"funcs={t['funcs']} lib-tagged={t['tagged']}",
                    flush=True,
                )

    print(f"\n=== FID cost, profile={args.profile}, best of {args.rounds} ===")
    hdr = f"{'binary':<22} {'funcs':>6} {'base':>8} {'analyzer':>9} {'parse':>8} {'hashq':>8} {'FID tot':>9} {'overhead':>9} {'tagged':>7}"
    print(hdr)
    print("-" * len(hdr))
    for target in args.targets:
        name = Path(target).name
        off, an, bm, full = (results[(name, m)] for m in MODES)
        analyzer = an["analysis"] - off["analysis"]
        parse = bm["stream"] - an["stream"]
        hashq = full["stream"] - bm["stream"]
        fid_total = full["total"] - off["total"]
        pct = 100 * fid_total / off["total"] if off["total"] else 0
        print(
            f"{name:<22} {full['funcs']:>6} {off['total']:>7.1f}s {analyzer:>8.1f}s "
            f"{parse:>7.1f}s {hashq:>7.1f}s {fid_total:>8.1f}s {pct:>8.0f}% "
            f"{full['tagged']:>7}"
        )


if __name__ == "__main__":
    main()
