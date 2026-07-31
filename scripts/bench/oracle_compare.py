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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("binary", nargs="?", default=DEFAULT_BINARY)
    ap.add_argument("--weights", default=None, help="lshweights_*.xml (default: nosize)")
    ap.add_argument("--pairs", type=int, default=60)
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

    with pyghidra.open_program(args.binary, analyze=True) as flat:
        program = flat.getCurrentProgram()
        decomp = DecompInterface()
        decomp.setOptions(DecompileOptions())
        decomp.toggleSyntaxTree(False)
        decomp.setSignatureSettings(settings)
        if not decomp.openProgram(program):
            sys.exit(f"decompiler: {decomp.getLastMessage()}")

        funcs = []
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

        print(f"functions processed: {len(funcs)}")

        # Cross-check: our dict vs the contents of Ghidra's own LSHVector.
        mismatched = []
        for name, gvec, ours in funcs:
            theirs = ghidra_vector_entries(gvec)
            if theirs != ours:
                only_ours = {k: v for k, v in ours.items() if theirs.get(k) != v}
                only_theirs = {k: v for k, v in theirs.items() if ours.get(k) != v}
                mismatched.append((name, len(ours), len(theirs), only_ours, only_theirs))
        if mismatched:
            print(f"VECTOR MISMATCH in {len(mismatched)}/{len(funcs)} functions:")
            for name, n_ours, n_theirs, only_ours, only_theirs in mismatched[:5]:
                print(f"  {name}: ours={n_ours} entries, ghidra={n_theirs} entries")
                print(f"    ours-differing (first 5): {dict(itertools.islice(only_ours.items(), 5))}")
                print(f"    ghidra-differing (first 5): {dict(itertools.islice(only_theirs.items(), 5))}")
            failures.append("vector contents disagree")
        else:
            print(f"vectors match: yes ({len(funcs)}/{len(funcs)} functions, hash+tf identical)")

        # Pairs: self-pairs first (identical), then all cross pairs.
        pairs = [(i, i) for i in range(len(funcs))]
        cross = list(itertools.combinations(range(len(funcs)), 2))
        random.Random(0).shuffle(cross)  # spread across the function set, not just func 0
        pairs += cross
        pairs = pairs[: max(args.pairs, len(funcs))]

        results = []
        for i, j in pairs:
            name_a, gvec_a, ours_a = funcs[i]
            name_b, gvec_b, ours_b = funcs[j]
            vc = VectorCompare()
            g_sim = gvec_a.compare(gvec_b, vc)
            g_sig = factory.calculateSignificance(vc)
            o_sim, o_sig = table.compare(ours_a, ours_b)
            results.append(
                (abs(g_sim - o_sim), abs(g_sig - o_sig), name_a, name_b, g_sim, o_sim, g_sig, o_sig)
            )

        # Distinct source functions that share a feature vector are indistinguishable
        # to ANY weighting scheme -- an information limit of the signature, not an
        # algorithm error. Measuring this ceiling first keeps it from being counted
        # as a false positive later.
        identical = []
        near = []
        for i, j in itertools.combinations(range(len(funcs)), 2):
            name_a, gvec_a, ours_a = funcs[i]
            name_b, _, ours_b = funcs[j]
            if ours_a == ours_b:
                identical.append((name_a, name_b))
            else:
                sim, _ = table.compare(ours_a, ours_b)
                if sim >= COLLISION_SIM:
                    near.append((sim, name_a, name_b))
        n_cross = len(funcs) * (len(funcs) - 1) // 2
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

        decomp.closeProgram()
        decomp.dispose()

    print(f"pairs compared: {len(results)}")
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
