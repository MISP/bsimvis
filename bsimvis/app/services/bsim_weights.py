"""BSim feature weighting.

Transcribed from Ghidra's reference implementation, the PostgreSQL extension in
``Ghidra/Features/BSim/src/lshvector/c/weights.c``:

* ``lsh_calc_weights`` (weights.c:231) -- per-feature coefficient and vector length
* ``lsh_compare_internal`` (weights.c:404) -- similarity *and* significance
* ``update_norms`` (weights.c:57) -- how ``scale`` is folded in

Two details are easy to get wrong and are load-bearing:

1. The ``count`` in ``<idflookup>`` is an *index* into the idf curve, not a document
   frequency. A hash absent from the table resolves to index 0, which is the
   *largest* weight -- so an unseen feature is treated as maximally rare.
2. The dot product uses the coefficient of whichever side has the smaller ``tf``,
   squared -- not ``coeff_a * coeff_b``. Because ``coeff`` already folds in the tf
   curve, this counts only the shared multiplicity. The two rules agree whenever
   ``tf_a == tf_b``, which is the common case, so a wrong implementation looks
   right until it doesn't.
"""

import math
from xml.etree import ElementTree

IDF_SIZE = 512
TF_SIZE = 64


def _as_hash(key):
    """Normalize a feature hash to int.

    Stored vectors key features by bare lowercase hex (``hex(h & 0xFFFFFFFF)[2:]``,
    ghidra_service.py), while the weights tables write ``0x``-prefixed hex.
    """
    if isinstance(key, int):
        return key
    if isinstance(key, bytes):
        key = key.decode()
    return int(key, 16)


class WeightTable:
    """A parsed lshweights_*.xml table."""

    def __init__(self, idf, tf, scale, addend, weightnorm, probflip, probdiff, lookup):
        self.scale = scale
        self.addend = addend
        # Loaded and rescaled by Ghidra but never read by lsh_compare_internal.
        # Kept for fidelity; deliberately unused.
        self.weightnorm = weightnorm
        self.lookup = lookup

        # update_norms (weights.c:62-69): the probability terms are premultiplied
        # by scale, and every idf weight by sqrt(scale).
        scale_sqrt = math.sqrt(scale)
        self.idfweight = [v * scale_sqrt for v in idf]
        self.tfweight = list(tf)
        self.probflip0, self.probflip1 = (p * scale for p in probflip)
        self.probdiff0, self.probdiff1 = (p * scale for p in probdiff)

    @classmethod
    def from_file(cls, path):
        root = ElementTree.parse(path).getroot()
        factory = root.find("weightfactory")
        if factory is None:
            raise ValueError(f"{path}: no <weightfactory> element")

        idf = [float(e.text) for e in factory.findall("idf")]
        tf = [float(e.text) for e in factory.findall("tf")]
        if len(idf) < IDF_SIZE:
            raise ValueError(f"{path}: expected >= {IDF_SIZE} <idf> entries, got {len(idf)}")
        if len(tf) < TF_SIZE:
            raise ValueError(f"{path}: expected >= {TF_SIZE} <tf> entries, got {len(tf)}")

        def scalar(tag):
            el = factory.find(tag)
            if el is None:
                raise ValueError(f"{path}: no <{tag}> element")
            return float(el.text)

        lookup = {}
        for el in root.iter("hash"):
            lookup[_as_hash(el.text.strip())] = int(el.get("count", 0))

        return cls(
            idf=idf,
            tf=tf,
            scale=float(factory.get("scale")),
            addend=float(factory.get("addend")),
            weightnorm=scalar("weightnorm"),
            probflip=(scalar("probflip0"), scalar("probflip1")),
            probdiff=(scalar("probdiff0"), scalar("probdiff1")),
            lookup=lookup,
        )

    def coeff(self, feature_hash, tf):
        """Weight of one feature occurrence set (weights.c:244-247).

        The idf curve descends with index (common features weigh less), though it
        is a fitted curve rather than a strictly monotone one -- the shipped
        nosize table has a single 1.3e-4 wobble around index 69. That is upstream
        data, not a parse bug.
        """
        idx = self.lookup.get(_as_hash(feature_hash), 0)
        tf = int(tf)
        if tf < 1:
            tf = 1
        elif tf > TF_SIZE:
            tf = TF_SIZE
        return self.idfweight[idx] * self.tfweight[tf - 1]

    def stats(self, vector):
        """(length, hashcount) for a ``{hash: tf}`` vector (weights.c:243-253)."""
        length_sq = 0.0
        hashcount = 0
        for feature_hash, tf in vector.items():
            c = self.coeff(feature_hash, tf)
            length_sq += c * c
            hashcount += int(tf)
        return math.sqrt(length_sq), hashcount

    def compare(self, vec_a, vec_b, stats_a=None, stats_b=None):
        """Return ``(similarity, significance)`` for two ``{hash: tf}`` vectors.

        Transcribes lsh_compare_internal (weights.c:404-474). Pass precomputed
        ``stats`` to skip recomputing vector lengths.
        """
        len_a, hc_a = stats_a if stats_a is not None else self.stats(vec_a)
        len_b, hc_b = stats_b if stats_b is not None else self.stats(vec_b)

        # Walk the smaller vector to keep the intersection cheap.
        if len(vec_a) > len(vec_b):
            small, large = vec_b, vec_a
        else:
            small, large = vec_a, vec_b

        dot = 0.0
        intersectcount = 0
        for feature_hash, tf_small in small.items():
            tf_large = large.get(feature_hash)
            if tf_large is None:
                continue
            t_small, t_large = int(tf_small), int(tf_large)
            # The smaller-tf side supplies the coefficient (weights.c:428-437).
            shared_tf = t_small if t_small < t_large else t_large
            w = self.coeff(feature_hash, shared_tf)
            dot += w * w
            intersectcount += shared_tf

        sim = dot / (len_a * len_b) if len_a > 0 and len_b > 0 else 0.0
        # Identical vectors land a few ULP above 1.0; callers (min_score filters,
        # ZSET thresholds) assume a closed [0, 1]. Ghidra does not clamp, but the
        # correction is far below the oracle tolerance.
        if sim > 1.0:
            sim = 1.0

        min_hc, max_hc = (hc_a, hc_b) if hc_a < hc_b else (hc_b, hc_a)
        diff = max_hc - min_hc
        numflip = min_hc - intersectcount
        if max_hc > 0:
            sig = (
                dot
                - numflip * (self.probflip0 + self.probflip1 / max_hc)
                - diff * (self.probdiff0 + self.probdiff1 / max_hc)
                + self.addend
            )
        else:
            sig = self.addend

        return sim, sig


_CACHE = {}


def load(path):
    """Load a weights table, memoized by path. Tables are static files."""
    key = str(path)
    table = _CACHE.get(key)
    if table is None:
        table = WeightTable.from_file(path)
        _CACHE[key] = table
    return table


def demo():
    """Self-check against a synthetic table with known-by-hand values."""
    import tempfile
    from pathlib import Path

    # scale=4 so sqrt(scale)=2 exactly; idf curve descends, tf curve ascends.
    idf = "\n".join(f"<idf>{1.0 - i / IDF_SIZE:.8f}</idf>" for i in range(IDF_SIZE))
    tf = "\n".join(f"<tf>{math.sqrt(i + 1):.8f}</tf>" for i in range(TF_SIZE))
    xml = f"""<weights settings="0x4d">
<weightfactory scale="4.0" addend="1.0">
{idf}
{tf}
<weightnorm>13.0</weightnorm>
<probflip0>0.5</probflip0><probflip1>1.0</probflip1>
<probdiff0>0.25</probdiff0><probdiff1>2.0</probdiff1>
</weightfactory>
<idflookup size="2">
<hash count="0">0xaaaa</hash>
<hash count="511">0xbbbb</hash>
</idflookup>
</weights>"""

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "w.xml"
        path.write_text(xml)
        t = WeightTable.from_file(path)

    # scale folding: idfweight[0] = 1.0 * sqrt(4) = 2.0
    assert abs(t.idfweight[0] - 2.0) < 1e-12, t.idfweight[0]

    # A hash absent from the lookup falls to index 0 -- the MAXIMUM weight.
    assert t.coeff("dead", 1) == t.idfweight[0]
    assert t.coeff("aaaa", 1) == t.idfweight[0]
    # A common hash (high index) must weigh less than an unknown one.
    assert t.coeff("bbbb", 1) < t.coeff("dead", 1)

    # tf curve is applied at index tf-1, and clamps at TF_SIZE.
    assert abs(t.coeff("dead", 4) - t.idfweight[0] * math.sqrt(4)) < 1e-12
    assert t.coeff("dead", 999) == t.coeff("dead", TF_SIZE)

    # Identical vectors score exactly 1.0.
    v = {"dead": 2, "beef": 1, "bbbb": 3}
    sim, _ = t.compare(v, v)
    assert abs(sim - 1.0) < 1e-12, sim

    # Disjoint vectors score 0.0.
    sim, _ = t.compare({"dead": 1}, {"beef": 1})
    assert sim == 0.0

    # THE min-tf RULE: with differing tf on a shared hash, the contribution uses
    # the smaller side's coefficient squared, NOT coeff_a * coeff_b.
    a, b = {"dead": 1}, {"dead": 9}
    sim, _ = t.compare(a, b)
    w_small = t.coeff("dead", 1)
    len_a, len_b = t.stats(a)[0], t.stats(b)[0]
    assert abs(sim - (w_small * w_small) / (len_a * len_b)) < 1e-12
    naive = t.coeff("dead", 1) * t.coeff("dead", 9) / (len_a * len_b)
    assert abs(sim - naive) > 1e-9, "min-tf rule must differ from coeff_a*coeff_b"

    # Similarity is invariant to `scale`; only significance moves with it.
    # Same curves as the parsed table (matching its 8-decimal rounding), so the
    # only difference is `scale`.
    scaled = WeightTable(
        idf=[float(f"{1.0 - i / IDF_SIZE:.8f}") for i in range(IDF_SIZE)],
        tf=[float(f"{math.sqrt(i + 1):.8f}") for i in range(TF_SIZE)],
        scale=9.0,
        addend=1.0,
        weightnorm=13.0,
        probflip=(0.5, 1.0),
        probdiff=(0.25, 2.0),
        lookup={0xAAAA: 0, 0xBBBB: 511},
    )
    x, y = {"dead": 2, "beef": 1}, {"dead": 1, "cafe": 4}
    assert abs(t.compare(x, y)[0] - scaled.compare(x, y)[0]) < 1e-12

    # Significance falls as unmatched terms accumulate, even at fixed similarity.
    sig_clean = t.compare({"dead": 1}, {"dead": 1})[1]
    sig_noisy = t.compare({"dead": 1}, {"dead": 1, "bbbb": 20})[1]
    assert sig_noisy < sig_clean, (sig_noisy, sig_clean)

    print("bsim_weights demo OK")


if __name__ == "__main__":
    demo()
