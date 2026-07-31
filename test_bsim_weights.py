"""BSim signature-settings profiles and feature weighting.

The weighting math is transcribed from Ghidra's reference C implementation
(Features/BSim/src/lshvector/c/weights.c); these tests pin the parts that are
easy to get subtly wrong and that would otherwise fail silently -- above all the
min-tf dot product rule, which agrees with the naive one on almost every pair.

Tests needing Ghidra's shipped weight tables are skipped when no install is
present (set GHIDRA_INSTALL_DIR, or have bin/ghidra_*).
"""

import math
import os
from pathlib import Path

from bsimvis.app.services import bsim_profiles, bsim_weights
from bsimvis.app.services.bsim_profiles import ProfileError

EPS = 1e-12


class Skip(Exception):
    """Raised by a test that cannot run in this environment."""


def _shipped_table():
    """Path to Ghidra's lshweights_nosize.xml, or None if no install is present."""
    root = os.environ.get("GHIDRA_INSTALL_DIR")
    candidates = [Path(root)] if root else []
    candidates += sorted(Path("bin").glob("ghidra_*"), reverse=True)
    for base in candidates:
        p = base / "Ghidra" / "Features" / "BSim" / "data" / "lshweights_nosize.xml"
        if p.is_file():
            return p
    return None


def _table():
    path = _shipped_table()
    if path is None:
        raise Skip("no Ghidra install with BSim weight tables")
    return bsim_weights.WeightTable.from_file(path)


def _raises(exc, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc as e:
        return str(e)
    raise AssertionError(f"expected {exc.__name__}")


# --- module self-checks -----------------------------------------------------


def test_module_self_checks():
    bsim_profiles.demo()
    bsim_weights.demo()


# --- signature settings mask ------------------------------------------------


def test_mask_validation_mirrors_ghidra():
    assert bsim_profiles.test_settings(0x4D)
    assert bsim_profiles.test_settings(0x49)
    # Ghidra's testSettings rejects zero outright.
    assert not bsim_profiles.test_settings(0)
    # Nothing outside 0x1CD may be set.
    assert not bsim_profiles.test_settings(0x1CD | 0x200)
    assert not bsim_profiles.test_settings(0x2)  # bit 1 is not a legal flag position
    # Bit 0 is called a check bit, but testSettings only *permits* it -- it does
    # not require it, so 0x4C (0x4D without bit 0) is a legal mask.
    assert bsim_profiles.test_settings(0x4C)
    assert bsim_profiles.VALID_SETTINGS_MASK == 0x1CD


def test_0x4d_decodes_to_the_nosize_configuration():
    assert bsim_profiles.decode_settings(0x4D) == [
        "SIG_COLLAPSE_SIZE",
        "SIG_COLLAPSE_INDNOISE",
        "SIG_DONOTUSE_CONST",
    ]
    # 0x49 is the sized configuration: the same minus SIG_COLLAPSE_SIZE.
    assert bsim_profiles.decode_settings(0x49) == [
        "SIG_COLLAPSE_INDNOISE",
        "SIG_DONOTUSE_CONST",
    ]
    assert 0x4D >> 2 == 0x13


def test_decode_rejects_illegal_mask():
    _raises(ProfileError, bsim_profiles.decode_settings, 0)


def test_algo_namespacing_roundtrip():
    assert bsim_profiles.parse_algo("weighted_cosine:nosize") == (
        "weighted_cosine",
        "nosize",
    )
    assert bsim_profiles.parse_algo("jaccard") == ("jaccard", None)
    assert (
        bsim_profiles.qualified_algo("weighted_cosine", "custom")
        == "weighted_cosine:custom"
    )


# --- weight table parsing / scoring ----------------------------------------


def test_shipped_table_parses_to_expected_shape():
    t = _table()
    assert len(t.idfweight) == bsim_weights.IDF_SIZE
    assert len(t.tfweight) == bsim_weights.TF_SIZE
    assert len(t.lookup) == 1000
    assert abs(t.scale - 1.55369941) < 1e-9
    assert abs(t.addend - 6.00980084) < 1e-9


def test_unknown_hash_gets_maximum_weight():
    """The central risk of adopting Ghidra's table on a non-x86 corpus.

    A hash missing from the 1000-entry lookup resolves to index 0, the LARGEST
    weight -- so unfamiliar boilerplate is treated as maximally rare.
    """
    t = _table()
    absent = 0xFFFFFFFF
    while absent in t.lookup:
        absent -= 1
    assert t.coeff(absent, 1) == t.idfweight[0] == max(t.idfweight)
    known = max(t.lookup, key=lambda h: t.lookup[h])
    assert t.coeff(known, 1) < t.coeff(absent, 1)


def test_identical_and_disjoint_vectors():
    t = _table()
    v = {"aabb": 2, "ccdd": 1, "eeff": 3}
    sim, _ = t.compare(v, v)
    assert abs(sim - 1.0) < EPS
    # Threshold logic and ZSET scores assume a closed [0, 1].
    assert sim <= 1.0
    assert t.compare({"aabb": 1}, {"ccdd": 1})[0] == 0.0


def test_dot_product_uses_smaller_tf_side_not_the_product():
    """weights.c:428-437 contributes min_side_coeff**2, NOT coeff_a * coeff_b.

    The two agree whenever tf_a == tf_b, so a wrong implementation passes every
    equal-tf test and only diverges on repeated features.
    """
    t = _table()
    a, b = {"aabb": 1}, {"aabb": 9}
    sim, _ = t.compare(a, b)

    len_a = t.stats(a)[0]
    len_b = t.stats(b)[0]
    w_small = t.coeff("aabb", 1)
    assert abs(sim - (w_small * w_small) / (len_a * len_b)) < EPS

    naive = t.coeff("aabb", 1) * t.coeff("aabb", 9) / (len_a * len_b)
    assert abs(sim - naive) > 1e-9


def test_tf_curve_is_applied_and_clamped():
    t = _table()
    assert t.coeff("aabb", 1) == t.idfweight[t.lookup.get(0xAABB, 0)]
    assert t.coeff("aabb", 999) == t.coeff("aabb", bsim_weights.TF_SIZE)
    # tf below 1 is nonsensical; treat it as a single occurrence rather than
    # indexing tfweight[-1] and silently picking the largest tf weight.
    assert t.coeff("aabb", 0) == t.coeff("aabb", 1)


def test_similarity_ignores_scale_but_significance_does_not():
    """sqrt(scale) multiplies every coefficient and cancels in the cosine."""
    base = _table()
    rescaled = bsim_weights.WeightTable(
        idf=[v / math.sqrt(base.scale) for v in base.idfweight],
        tf=base.tfweight,
        scale=base.scale * 4.0,
        addend=base.addend,
        weightnorm=base.weightnorm,
        probflip=(0.0, 0.0),
        probdiff=(0.0, 0.0),
        lookup=base.lookup,
    )
    a, b = {"aabb": 2, "ccdd": 1}, {"aabb": 1, "eeff": 4}
    assert abs(base.compare(a, b)[0] - rescaled.compare(a, b)[0]) < EPS


def test_significance_suppresses_boilerplate_only_matches():
    """A high cosine over few features must not carry high significance.

    This is the property that actually addresses #30: boilerplate-only matches
    self-suppress even when their similarity looks convincing.
    """
    t = _table()
    tiny_sim, tiny_sig = t.compare({"aabb": 1}, {"aabb": 1})

    rich = {f"{0xAA00 + i:04x}": 1 for i in range(40)}
    rich_sim, rich_sig = t.compare(rich, rich)

    assert abs(tiny_sim - rich_sim) < EPS  # both are perfect matches
    assert rich_sig > tiny_sig  # but the substantial one carries far more weight


def test_length_mismatch_penalizes_significance():
    t = _table()
    a = {"aabb": 1, "ccdd": 1}
    matched = t.compare(a, a)[1]
    padded = t.compare(a, dict(a, **{f"{0xBB00 + i:04x}": 1 for i in range(30)}))[1]
    assert padded < matched


def test_stats_can_be_precomputed():
    """Cached (length, hashcount) must give the same answer as recomputing."""
    t = _table()
    a, b = {"aabb": 2, "ccdd": 1}, {"aabb": 1, "eeff": 4}
    assert t.compare(a, b) == t.compare(a, b, stats_a=t.stats(a), stats_b=t.stats(b))


# --- profile resolution -----------------------------------------------------


def test_default_profile_pairs_0x4d_with_the_nosize_table():
    if _shipped_table() is None:
        raise Skip("no Ghidra install")
    p = bsim_profiles.get_profile("nosize")
    assert p.settings == 0x4D
    assert p.weights_path.name == "lshweights_nosize.xml"


def _with_profiles(profiles, fn):
    """Swap the configured profile table for the duration of `fn`."""
    original = bsim_profiles._configured_profiles
    bsim_profiles._configured_profiles = lambda: profiles
    try:
        return fn()
    finally:
        bsim_profiles._configured_profiles = original


def test_mismatched_weights_table_is_refused():
    """A table built for other signature settings must fail loudly, not score."""
    path = _shipped_table()
    if path is None:
        raise Skip("no Ghidra install")
    sized = path.parent / "lshweights_32.xml"
    if not sized.is_file():
        raise Skip("lshweights_32.xml absent")

    msg = _with_profiles(
        {"bad": {"settings": 0x4D, "weights": str(sized)}},
        lambda: _raises(ProfileError, bsim_profiles.get_profile, "bad"),
    )
    assert "0x49" in msg and "0x4d" in msg


def test_unknown_profile_name_raises():
    msg = _raises(ProfileError, bsim_profiles.get_profile, "no_such_profile_xyz")
    assert "Unknown BSim profile" in msg


def test_invalid_mask_in_profile_raises():
    msg = _with_profiles(
        {"bad": {"settings": 0x2, "weights": "whatever.xml"}},
        lambda: _raises(ProfileError, bsim_profiles.get_profile, "bad"),
    )
    assert "invalid signature settings" in msg


def test_signature_settings_falls_back_when_table_is_missing():
    """Feature extraction must not fail because a weights table is absent.

    Ingest hosts need the mask but never the weights.
    """
    settings = _with_profiles(
        {"nosize": {"settings": 0x4D, "weights": "/nonexistent/table.xml"}},
        bsim_profiles.get_signature_settings,
    )
    assert settings == 0x4D


if __name__ == "__main__":
    passed = skipped = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_"):
            continue
        try:
            fn()
        except Skip as e:
            print(f"skip  {name}  ({e})")
            skipped += 1
        else:
            print(f"ok    {name}")
            passed += 1
    print(f"all passed ({passed} run, {skipped} skipped)")
