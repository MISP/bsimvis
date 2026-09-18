"""The one list of similarity algorithms.

Every algorithm name the CLI accepts, the API validates, the swagger docs
describe and the dashboard offers comes from here. Before this module the same
list was spelled out in a dozen places, so adding `binary_cosine` meant editing
eight `choices=` lists and three JS arrays -- and the API's algo lists and the
build path's own vocabulary had already drifted apart.

Two flags carry the distinctions that matter:

* ``buildable`` -- the build path (``_select_candidates``) can compute it. It
  branches on a fixed set of algorithms and does not raise on the others: an
  unknown one falls past every branch, leaves ``min_shared_norm_sq`` at 0 and
  emits unfiltered pairs. ``assert_buildable_algo`` reads this flag.
* ``exact`` -- ``calculate_exact_score`` can score one pair on demand. Every
  algorithm here can, including the ones the build path refuses.

Deliberately dependency-free: the CLI imports it to build ``choices=`` lists at
parse time, so it must not drag in flask, redis or config.
"""


class Algorithm:
    """One similarity algorithm and what can be done with it."""

    def __init__(
        self,
        name,
        label,
        icon,
        description,
        buildable=True,
        exact=True,
        significance=False,
        requires_milvus=False,
        profiled=False,
    ):
        self.name = name
        self.label = label
        self.icon = icon
        self.description = description
        self.buildable = buildable
        self.exact = exact
        # Reports a second, unbounded number alongside the score.
        self.significance = significance
        self.requires_milvus = requires_milvus
        # Name may be qualified with a weights profile (weighted_cosine:nosize).
        self.profiled = profiled

    def as_dict(self):
        return {
            "name": self.name,
            "label": self.label,
            "icon": self.icon,
            "description": self.description,
            "buildable": self.buildable,
            "exact": self.exact,
            "significance": self.significance,
            "requires_milvus": self.requires_milvus,
            "profiled": self.profiled,
        }

    def __repr__(self):
        return f"Algorithm({self.name!r})"


ALGORITHMS = (
    Algorithm(
        "unweighted_cosine",
        "Cosine",
        "fa-solid fa-arrows-left-right",
        "Cosine over the raw term-frequency vectors. 'Unweighted' means no IDF "
        "weighting, not binary: a feature occurring five times counts five times. "
        "The default.",
    ),
    Algorithm(
        "binary_cosine",
        "Binary Cosine",
        "fa-solid fa-toggle-on",
        "Cosine over the feature sets: every feature counts once however often it "
        "repeats. Insensitive to unrolled loops and repeated idioms.",
    ),
    Algorithm(
        "jaccard",
        "Jaccard",
        "fa-solid fa-object-group",
        "Generalized Jaccard (Tanimoto) over the term-frequency vectors: "
        "sum(min) / sum(max).",
    ),
    Algorithm(
        "milvus_sparse",
        "Milvus Sparse",
        "fa-solid fa-braille",
        "The same raw-TF cosine formula, served by Milvus's sparse index instead "
        "of the inverted-index walk.",
        requires_milvus=True,
    ),
    Algorithm(
        "weighted_cosine",
        "BSim Weighted",
        "fa-solid fa-scale-balanced",
        "Ghidra's own BSim scoring: features weighted by an IDF curve, so shared "
        "boilerplate counts for less than a shared rare routine. Per-pair only -- "
        "the build path has no pruning bound for it yet. Also reports significance.",
        buildable=False,
        significance=True,
        profiled=True,
    ),
)

BY_NAME = {a.name: a for a in ALGORITHMS}

DEFAULT_ALGO = "unweighted_cosine"


def base_name(algo):
    """Strip a profile qualifier: ``weighted_cosine:nosize`` -> ``weighted_cosine``.

    Mirrors ``bsim_profiles.parse_algo`` without importing it (that module pulls
    in config_service). Only profiled algorithms may carry a qualifier.
    """
    if algo and ":" in algo:
        head = algo.split(":", 1)[0]
        if head in BY_NAME and BY_NAME[head].profiled:
            return head
    return algo


def get(algo):
    """The Algorithm for `algo` (profile qualifier allowed), or None."""
    return BY_NAME.get(base_name(algo))


def names(buildable=None, milvus_enabled=True):
    """Algorithm names, newest-first-by-preference order preserved.

    `buildable=True` keeps only what the build path can compute, `False` only
    what it cannot. `milvus_enabled=False` drops algorithms needing Milvus.
    """
    out = []
    for a in ALGORITHMS:
        if buildable is not None and a.buildable is not buildable:
            continue
        if a.requires_milvus and not milvus_enabled:
            continue
        out.append(a.name)
    return out


BUILD_CHOICES = names(buildable=True)
"""What an --algo flag on a build/cluster command may be. Milvus is listed even
when disabled: the CLI cannot see the server's config at parse time."""


def describe(milvus_enabled=True):
    """The payload behind ``GET /api/similarity/algorithms``.

    Milvus-backed algorithms stay in the list when Milvus is off, marked
    ``available: false``, so a UI can grey them out rather than silently lose an
    option it had a moment ago.
    """
    out = []
    for a in ALGORITHMS:
        d = a.as_dict()
        d["available"] = milvus_enabled or not a.requires_milvus
        out.append(d)
    return out


def demo():
    """Self-check: the flags the callers actually branch on."""
    assert DEFAULT_ALGO in BY_NAME

    # The build path must accept exactly what similarity_service branches on.
    assert set(names(buildable=True)) == {
        "jaccard",
        "unweighted_cosine",
        "binary_cosine",
        "milvus_sparse",
    }, names(buildable=True)
    assert names(buildable=False) == ["weighted_cosine"]

    # Profile qualifiers resolve, and only on a profiled algorithm.
    assert base_name("weighted_cosine:nosize") == "weighted_cosine"
    assert get("weighted_cosine:nosize").significance is True
    assert base_name("jaccard:nosize") == "jaccard:nosize"
    assert get("jaccard:nosize") is None
    assert get("nonsense") is None

    # Milvus filtering drops the name from choices but not from the UI payload.
    assert "milvus_sparse" not in names(milvus_enabled=False)
    off = {d["name"]: d["available"] for d in describe(milvus_enabled=False)}
    assert off["milvus_sparse"] is False and off["jaccard"] is True

    # Every algorithm is scoreable per pair, even the unbuildable one.
    assert all(a.exact for a in ALGORITHMS)

    print("similarity registry demo OK")


if __name__ == "__main__":
    demo()
