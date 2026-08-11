"""How much of the ghidra analyze job is capa and YARA?

The two are paid for very differently, so this measures them differently:

    analysis   run_profile_analysis(), the baseline the job would cost anyway
    capa wall  a full standalone capa run
    capa resid what capa actually costs the job -- it is spawned *before* the
               program is imported and waited on after analysis, so only the
               part that outlives that window is job time:
               max(0, wall - import - analysis)
    capa parse capa JSON -> function tags, serial, after the wait
    yara comp  compiling the vendored ruleset, once per worker process
    yara scan  matching the ruleset against the file, serial
    yara attr  match offsets -> function tags via xrefs, serial

`capa resid` assumes the overlapped run gets a free core. On a saturated
worker it degrades toward the full wall time, so treat it as the floor.

Needs a JVM, so it is not part of the fast suite:

    GHIDRA_INSTALL_DIR=$PWD/bin/ghidra_12.1_PUBLIC \
      .venv/bin/python scripts/benchmark_capa_yara_cost.py data/test/* [--rounds 2]
"""

import argparse
import logging
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def run_one(gs, analyzer, target, profile, capa, work):
    """Import + analyze one binary, then time capa and YARA against it."""
    from ghidra.base.project import GhidraProject
    from bsimvis.app.services.tag_taxonomy import yara_file_tags
    from bsimvis.app.services.yara_service import scan_file

    target = str(Path(target).resolve())
    t = {}

    # capa never sees the Ghidra program, so run it outside the project block
    # -- nothing here depends on the analysis having happened.
    capa_json = f"{work}/capa.json"
    if capa:
        t0 = time.time()
        with open(capa_json, "w") as out:
            rc = subprocess.call([capa, "-j", target], stdout=out, stderr=subprocess.DEVNULL)
        t["capa_wall"] = time.time() - t0
        t["capa_rc"] = rc
    else:
        t["capa_wall"], t["capa_rc"] = 0.0, None

    t0 = time.time()
    matches = scan_file(target)
    t["yara_scan"] = time.time() - t0
    t["yara_hits"] = len(yara_file_tags(matches))

    with tempfile.TemporaryDirectory(prefix="bench_cy_") as temp_dir:
        project = GhidraProject.createProject(temp_dir, "BenchCapaYara", False)
        try:
            t0 = time.time()
            program = project.importProgram(Path(target))
            t["import"] = time.time() - t0

            t0 = time.time()
            gs.run_profile_analysis(program, profile, force_reanalysis=True)
            t["analysis"] = time.time() - t0

            t0 = time.time()
            try:
                capa_tags = (
                    analyzer._capa_tags_for_program(capa_json, program)
                    if capa and t["capa_rc"] == 0
                    else {}
                )
            except Exception as e:
                logging.warning("capa parse failed: %s", e)
                capa_tags = {}
            t["capa_parse"] = time.time() - t0
            t["capa_funcs"] = len(capa_tags)

            t0 = time.time()
            yara_tags = analyzer._yara_tags_for_program(matches, program)
            t["yara_attrib"] = time.time() - t0
            t["yara_funcs"] = len(yara_tags)
        finally:
            project.close()

    # What the job pays on top of the work it would have done anyway. capa is
    # spawned before the program is even imported, so the window it hides
    # behind is import + analysis, not analysis alone.
    t["capa_resid"] = max(0.0, t["capa_wall"] - t["import"] - t["analysis"])
    t["overhead"] = t["capa_resid"] + t["capa_parse"] + t["yara_scan"] + t["yara_attrib"]
    return t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("targets", nargs="+")
    ap.add_argument("--profile", default="fast")
    ap.add_argument("--rounds", type=int, default=1)
    args = ap.parse_args()

    logging.basicConfig(level=logging.WARNING)
    from bsimvis.app.services.ghidra_service import ghidra_service as gs
    from bsimvis.app.services.unpack_service import capa_path

    assert args.profile in gs.config.get("profiles", {}), "profile not in config"
    capa = capa_path()
    if not capa:
        print("note: capa not installed -- capa columns will read 0", flush=True)

    gs.ensure_launcher(max_heap_mb=4096)
    from bsimvis.ghidra_job import GhidraAnalyzer

    analyzer = object.__new__(GhidraAnalyzer)

    # Before the warmup, or the warmup's own scan would have cached it and
    # this would read 0 -- the compile is a per-worker cost worth naming.
    from bsimvis.app.services.yara_service import compiled_rules, rules_dir

    t0 = time.time()
    rules = compiled_rules()
    yara_compile_secs = time.time() - t0
    if rules is None:
        print(f"note: no YARA rules under {rules_dir()} -- yara columns will read 0")

    work = tempfile.mkdtemp(prefix="bench_capa_yara_")
    results = {}
    print("warmup...", flush=True)
    run_one(gs, analyzer, args.targets[0], args.profile, capa, work)

    for target in args.targets:
        name = Path(target).name
        for r in range(args.rounds):
            t = run_one(gs, analyzer, target, args.profile, capa, work)
            prev = results.get(name)
            # min over rounds: least noise from JIT warmup / other load
            if prev is None or t["overhead"] < prev["overhead"]:
                results[name] = t
            print(
                f"  {name:<22} r{r+1} analysis={t['analysis']:6.1f}s "
                f"capa={t['capa_wall']:6.1f}s(rc={t['capa_rc']}) "
                f"yara scan={t['yara_scan']:5.2f}s attr={t['yara_attrib']:5.2f}s",
                flush=True,
            )

    print(f"\n=== capa/YARA cost, profile={args.profile}, best of {args.rounds} ===")
    hdr = (
        f"{'binary':<30} {'import':>7} {'analysis':>9} {'capa wall':>10} "
        f"{'capa resid':>11} {'capa parse':>11} {'yara scan':>10} {'yara attr':>10} "
        f"{'overhead':>9} {'%':>5} {'cfuncs':>7} {'yhits':>6}"
    )
    print(hdr)
    print("-" * len(hdr))
    for name, t in results.items():
        base = t["import"] + t["analysis"]
        pct = 100 * t["overhead"] / base if base else 0
        print(
            f"{name:<30} {t['import']:>6.1f}s {t['analysis']:>8.1f}s "
            f"{t['capa_wall']:>9.1f}s {t['capa_resid']:>10.1f}s "
            f"{t['capa_parse']:>10.2f}s {t['yara_scan']:>9.2f}s "
            f"{t['yara_attrib']:>9.2f}s {t['overhead']:>8.1f}s {pct:>4.0f}% "
            f"{t['capa_funcs']:>7} {t['yara_hits']:>6}"
        )
    print(
        f"\nyara ruleset compile: {yara_compile_secs:.2f}s, "
        "paid once per worker process (cached thereafter)."
    )


if __name__ == "__main__":
    main()
