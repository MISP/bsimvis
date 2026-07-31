#!/usr/bin/env python3
"""Oracle test: our BSim weighting math vs Ghidra's own WeightedLSHCosineVectorFactory.

Mirrors CompareBSimSignaturesScript.java: build the same weighted vector factory
from lshweights_nosize.xml, build LSHVectors for real functions, and check
Ghidra's (similarity, significance) against bsim_weights.WeightTable.compare().

Usage:
    GHIDRA_INSTALL_DIR=... .venv/bin/python scripts/bench/oracle_compare.py [binary]
"""

import argparse
import itertools
import os
import random
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DEFAULT_BINARY = "/home/thomas/data/versioned_c/bin/v01_linux_x64"
SIM_TOL = 1e-9
SIG_TOL = 1e-6
MIN_FEATURES = 10
COLLISION_SIM = 0.95


def build_factory(weights_path):
    """WeightedLSHCosineVectorFactory loaded from a weights XML (readWeights, ~line 152)."""
    from generic.lsh.vector import WeightedLSHCosineVectorFactory
    from ghidra.util.xml import SpecXmlUtils
    from ghidra.xml import NonThreadedXmlPullParserImpl
    from java.io import FileInputStream

    factory = WeightedLSHCosineVectorFactory()
    stream = FileInputStream(str(weights_path))
    try:
        parser = NonThreadedXmlPullParserImpl(
            stream, "Vector weights parser", SpecXmlUtils.getXmlHandler(), False
        )
        factory.readWeights(parser)
    finally:
        stream.close()
    return factory


def our_vector(decomp, func, monitor):
    """{hash: tf} exactly as ghidra_service.py extracts it."""
    sigs = decomp.debugSignatures(func, 10, monitor)
    if not sigs:
        return {}
    return dict(Counter(hex(sigs.get(i).hash & 0xFFFFFFFF)[2:] for i in range(sigs.size())))


def ghidra_vector_entries(vec):
    """{hash: tf} read back out of Ghidra's own LSHVector."""
    entries = vec.getEntries()
    out = {}
    for e in entries:
        out[hex(e.getHash() & 0xFFFFFFFF)[2:]] = int(e.getTF())
    return out


def extract(binary, factory, settings, monitor):
    """Open `binary`, return [(name, ghidra LSHVector, our {hash: tf})]."""
    import pyghidra
    from ghidra.app.decompiler import DecompInterface, DecompileOptions

    funcs = []
    with pyghidra.open_program(binary, analyze=True) as flat:
        program = flat.getCurrentProgram()
        decomp = DecompInterface()
        decomp.setOptions(DecompileOptions())
        decomp.toggleSyntaxTree(False)
        decomp.setSignatureSettings(settings)
        if not decomp.openProgram(program):
            sys.exit(f"decompiler: {decomp.getLastMessage()}")

        for func in program.getFunctionManager().getFunctions(True):
            if func.isExternal() or func.isThunk():
                continue
            name = func.getName()
            if name.startswith("FUN_"):
                continue
            sigres = decomp.generateSignatures(func, False, 10, monitor)
            if sigres is None or sigres.features is None or len(sigres.features) < MIN_FEATURES:
                continue
            gvec = factory.buildVector(sigres.features)
            ours = our_vector(decomp, func, monitor)
            if not ours:
                continue
            funcs.append((name, gvec, ours))

        decomp.closeProgram()
        decomp.dispose()
    return funcs


def check_vectors(label, funcs, failures):
    """Our dict vs the contents of Ghidra's own LSHVector (also proves the
    LSHVectors are still readable after their program context closed)."""
    mismatched = []
    for name, gvec, ours in funcs:
        theirs = ghidra_vector_entries(gvec)
        if theirs != ours:
            only_ours = {k: v for k, v in ours.items() if theirs.get(k) != v}
            only_theirs = {k: v for k, v in theirs.items() if ours.get(k) != v}
            mismatched.append((name, len(ours), len(theirs), only_ours, only_theirs))
    if mismatched:
        print(f"[{label}] VECTOR MISMATCH in {len(mismatched)}/{len(funcs)} functions:")
        for name, n_ours, n_theirs, only_ours, only_theirs in mismatched[:5]:
            print(f"  {name}: ours={n_ours} entries, ghidra={n_theirs} entries")
            print(f"    ours-differing (first 5): {dict(itertools.islice(only_ours.items(), 5))}")
            print(f"    ghidra-differing (first 5): {dict(itertools.islice(only_theirs.items(), 5))}")
        failures.append(f"[{label}] vector contents disagree")
    else:
        print(f"[{label}] vectors match: yes ({len(funcs)}/{len(funcs)}, hash+tf identical)")


def print_buckets(results):
    """Bucketed agreement table, keyed on Ghidra's similarity."""
    edges = [(i / 10, (i + 1) / 10) for i in range(10)]
    print("\nbucketed agreement (bucket by Ghidra similarity):")
    print(f"  {'bucket':>12} {'pairs':>8} {'max|dsim|':>12} {'max|dsig|':>12}")
    rows = [(f"[{lo:.1f},{hi:.1f})", lambda s, lo=lo, hi=hi: lo <= s < hi) for lo, hi in edges]
    rows.append(("== 1.0", lambda s: s >= 1.0))
    for label, pred in rows:
        sel = [r for r in results if pred(r[4])]
        if not sel:
            print(f"  {label:>12} {0:>8}            -            -")
            continue
        print(
            f"  {label:>12} {len(sel):>8} {max(r[0] for r in sel):>12.3e} "
            f"{max(r[1] for r in sel):>12.3e}"
        )


def print_name_summary(results):
    """Descriptive only: same-name (true cross-arch matches) vs different-name."""
    import statistics

    same = sorted(r[4] for r in results if r[2] == r[3])
    diff = [r for r in results if r[2] != r[3]]
    print("\ncross-binary name summary (descriptive, not part of the verdict):")
    if same:
        print(
            f"  SAME-NAME pairs: n={len(same)} min={same[0]:.4f} "
            f"median={statistics.median(same):.4f} max={same[-1]:.4f}"
        )
    else:
        print("  SAME-NAME pairs: none")
    if diff:
        sims = sorted(r[4] for r in diff)
        print(
            f"  DIFF-NAME pairs: n={len(diff)} median={statistics.median(sims):.4f} "
            f"max={sims[-1]:.4f}"
        )
        print("  top 10 different-name scorers (sim / significance):")
        for r in sorted(diff, key=lambda r: -r[4])[:10]:
            print(f"    sim={r[4]:.4f} sig={r[6]:8.3f}  {r[2]}  <->  {r[3]}")
        # Does significance separate what similarity cannot? Compare the weakest
        # true match against the strongest false one -- if the false pair scores
        # higher on similarity but lower on significance, significance is the
        # discriminator the threshold cannot be.
        same_pairs = [r for r in results if r[2] == r[3]]
        if same_pairs:
            worst_true = min(same_pairs, key=lambda r: r[4])
            best_false = max(diff, key=lambda r: r[4])
            print("\n  discrimination check:")
            print(
                f"    weakest TRUE  match: sim={worst_true[4]:.4f} "
                f"sig={worst_true[6]:8.3f}  {worst_true[2]}"
            )
            print(
                f"    strongest FALSE pair: sim={best_false[4]:.4f} "
                f"sig={best_false[6]:8.3f}  {best_false[2]} <-> {best_false[3]}"
            )
            sims = [r[4] for r in same_pairs]
            sigs = [r[6] for r in same_pairs]
            print(
                f"    TRUE  sim range [{min(sims):.4f}, {max(sims):.4f}]  "
                f"sig range [{min(sigs):.3f}, {max(sigs):.3f}]"
            )
            dsims = [r[4] for r in diff]
            dsigs = [r[6] for r in diff]
            print(
                f"    FALSE sim range [{min(dsims):.4f}, {max(dsims):.4f}]  "
                f"sig range [{min(dsigs):.3f}, {max(dsigs):.3f}]"
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("binary", nargs="?", default=DEFAULT_BINARY)
    ap.add_argument("--binary-b", default=None, help="second binary: compare full A x B")
    ap.add_argument("--weights", default=None, help="lshweights_*.xml (default: nosize)")
    ap.add_argument("--pairs", type=int, default=60)
    ap.add_argument(
        "--dump-vectors", default=None,
        help="write the extracted {name: {hash: tf}} vectors to JSON (for offline benchmarks)",
    )
    args = ap.parse_args()

    ghidra_dir = os.environ.get("GHIDRA_INSTALL_DIR")
    if not ghidra_dir:
        sys.exit("GHIDRA_INSTALL_DIR is not set")
    weights_path = args.weights or os.path.join(
        ghidra_dir, "Ghidra/Features/BSim/data/lshweights_nosize.xml"
    )

    # Import before the JVM starts: jpype's import hook shadows dotted names.
    from bsimvis.app.services import bsim_weights

    import pyghidra
    from pyghidra.launcher import HeadlessPyGhidraLauncher

    HeadlessPyGhidraLauncher(verbose=False).start()

    table = bsim_weights.WeightTable.from_file(weights_path)

    from ghidra.app.decompiler import DecompInterface, DecompileOptions
    from generic.lsh.vector import VectorCompare
    from ghidra.util.task import ConsoleTaskMonitor

    factory = build_factory(weights_path)
    settings = factory.getSettings()
    print(f"vectorFactory.getSettings() = 0x{settings:X}")
    assert settings == 0x4D, f"expected 0x4D, got 0x{settings:X}"

    monitor = ConsoleTaskMonitor()
    failures = []

    funcs = extract(args.binary, factory, settings, monitor)
    print(f"functions processed [A {os.path.basename(args.binary)}]: {len(funcs)}")
    funcs_b = None
    if args.binary_b:
        funcs_b = extract(args.binary_b, factory, settings, monitor)
        print(f"functions processed [B {os.path.basename(args.binary_b)}]: {len(funcs_b)}")

    # Also proves the LSHVectors survived their program being closed.
    if args.dump_vectors:
        import json

        dump = {"A": {name: ours for name, _, ours in funcs}}
        if funcs_b:
            dump["B"] = {name: ours for name, _, ours in funcs_b}
        with open(args.dump_vectors, "w") as fh:
            json.dump(dump, fh)
        print(f"wrote vectors to {args.dump_vectors}")

    check_vectors("A", funcs, failures)
    if funcs_b is not None:
        check_vectors("B", funcs_b, failures)

    def compare_pairs(pairs):
        out = []
        for (name_a, gvec_a, ours_a), (name_b, gvec_b, ours_b) in pairs:
            vc = VectorCompare()
            g_sim = gvec_a.compare(gvec_b, vc)
            g_sig = factory.calculateSignificance(vc)
            o_sim, o_sig = table.compare(ours_a, ours_b)
            out.append(
                (abs(g_sim - o_sim), abs(g_sig - o_sig), name_a, name_b, g_sim, o_sim, g_sig, o_sig)
            )
        return out

    if funcs_b is None:
        # Pairs: self-pairs first (identical), then all cross pairs.
        idx = [(i, i) for i in range(len(funcs))]
        cross = list(itertools.combinations(range(len(funcs)), 2))
        random.Random(0).shuffle(cross)  # spread across the function set, not just func 0
        idx += cross
        idx = idx[: max(args.pairs, len(funcs))]
        results = compare_pairs([(funcs[i], funcs[j]) for i, j in idx])
        cross_results = None
    else:
        cross_results = compare_pairs(itertools.product(funcs, funcs_b))
        internal = compare_pairs(
            [(funcs[i], funcs[j]) for i, j in itertools.combinations(range(len(funcs)), 2)]
            + [(funcs_b[i], funcs_b[j]) for i, j in itertools.combinations(range(len(funcs_b)), 2)]
        )
        results = cross_results + internal
        print(f"A x B pairs: {len(cross_results)}   A/B-internal pairs: {len(internal)}")

    # Distinct source functions that share a feature vector are indistinguishable
    # to ANY weighting scheme -- an information limit of the signature, not an
    # algorithm error. Measuring this ceiling first keeps it from being counted
    # as a false positive later.
    ceiling_funcs = funcs + (funcs_b or [])
    identical = []
    near = []
    for (name_a, _, ours_a), (name_b, _, ours_b) in itertools.combinations(ceiling_funcs, 2):
        if ours_a == ours_b:
            identical.append((name_a, name_b))
        else:
            sim, _ = table.compare(ours_a, ours_b)
            if sim >= COLLISION_SIM:
                near.append((sim, name_a, name_b))
    n_cross = len(ceiling_funcs) * (len(ceiling_funcs) - 1) // 2
    print(
        f"\nceiling: {len(identical)}/{n_cross} distinct-function pairs have "
        f"IDENTICAL feature vectors"
    )
    for a, b in identical[:10]:
        print(f"  identical: {a} <-> {b}")
    print(
        f"ceiling: {len(near)}/{n_cross} further pairs score >= {COLLISION_SIM} "
        f"without being identical"
    )
    for sim, a, b in sorted(near, reverse=True)[:10]:
        print(f"  {sim:.4f}: {a} <-> {b}")

    print(f"\npairs compared: {len(results)}")
    print_buckets(results)
    if cross_results is not None:
        print_name_summary(cross_results)

    max_sim = max(r[0] for r in results)
    max_sig = max(r[1] for r in results)
    print(f"max |sim diff| = {max_sim:.3e}   (tol {SIM_TOL:.0e})")
    print(f"max |sig diff| = {max_sig:.3e}   (tol {SIG_TOL:.0e})")

    print("worst 5 pairs by similarity difference:")
    for d_sim, d_sig, a, b, gs, os_, gg, og in sorted(results, reverse=True)[:5]:
        print(f"  {a} vs {b}: dsim={d_sim:.3e} ghidra={gs!r} ours={os_!r}")
        print(f"      dsig={d_sig:.3e} ghidra={gg!r} ours={og!r}")

    print("worst 5 pairs by significance difference:")
    for d_sim, d_sig, a, b, gs, os_, gg, og in sorted(results, key=lambda r: -r[1])[:5]:
        print(f"  {a} vs {b}: dsig={d_sig:.3e} ghidra={gg!r} ours={og!r} (sim {gs!r}/{os_!r})")

    if max_sim > SIM_TOL:
        failures.append(f"similarity diff {max_sim:.3e} > {SIM_TOL:.0e}")
    if max_sig > SIG_TOL:
        failures.append(f"significance diff {max_sig:.3e} > {SIG_TOL:.0e}")

    if failures:
        print("ORACLE FAILED: " + "; ".join(failures))
        return 1
    print("ORACLE PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
