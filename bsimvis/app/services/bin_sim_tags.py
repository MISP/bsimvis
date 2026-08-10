"""Tag-based splitting of a binary-similarity score.

`build_bin_sim` produces one cohesion score per binary pair. That single number
mixes the boring mass (libc, compiler boilerplate) with the interesting mass
(the actual malware code), so two unrelated samples that both statically link
libc look related. This module splits the same matched flow by function tag, so
the caller can report "libc contributes 25% of the match, mirai_core 60%".

Matching is done on the *full* tag id (`lib:libc:2.31:memcpy`); the summary then
rolls children up under their `lib:libc:2.31` parent, so the score is as precise
as the Function ID analyzer made it while the UI still shows one row per library
version.

Tags come from four sources -- Function ID, a vendored-code bundle, a human, and
the LLM -- answering two unrelated questions, so they are split onto two axes
(see AXIS_PROVENANCE / AXIS_FLAGS below) and crossed by `AxisSplit`. Nothing here
touches the pair score: the score is the matched edges alone, and tagging only
changes how it is broken down. That is why re-tagging is answered by a resplit
(`BinSimService.resplit_bin_sim`) rather than a rebuild.

It adds no asymptotic cost: everything here is O(1) per already-matched edge.
"""

from collections import defaultdict

# Two distinct kinds of "we can't attribute this to a library", kept apart on
# purpose: UNTAGGED means at least one side carries no tag at all (no evidence),
# MISMATCH means both sides are tagged but share nothing -- the interesting case,
# e.g. libc 2.31 matching libc 2.35, or libc matching uclibc.
TAG_UNTAGGED = "original_code"
TAG_MISMATCH = "tag_mismatch"

# --- Axes ------------------------------------------------------------------
# Tags answer two unrelated questions and must not share one pool of mass:
#
#   provenance -- "whose code is this": libc, a vendored bundle, or nobody's
#                 (original_code). Mutually exclusive, rows partition the pair.
#   flags      -- "what does this code do": suspicious, c2, crypto. A function
#                 can carry several, and carrying one says nothing about where
#                 the code came from.
#
# Splitting one flat tag space by `conf / len(tags)` mixed the two: a single
# `flag:suspicious:c2` on a libc function used to halve libc's mass, and the
# same flag on genuine original code evicted it from `original_code` entirely
# (that bucket only fills when a function has *no* tags). Routing by namespace
# keeps each axis's mass whole.
AXIS_PROVENANCE = "provenance"
AXIS_FLAGS = "flags"

# namespace prefix -> (axis, default priority). Priority only matters inside
# provenance, where a function must resolve to one origin; flags overlay and
# never compete. Library tags outrank bundle tags because Function ID matched
# actual bytes, while a bundle tag usually labels a whole binary: a statically
# linked memcpy inside a Mirai sample is still libc's, and calling it Mirai's is
# how a libc floor turns into a fake family attribution.
TAG_NAMESPACES = {
    "lib": (AXIS_PROVENANCE, 100),
    "stdlib": (AXIS_PROVENANCE, 100),
    "bundle": (AXIS_PROVENANCE, 50),
    "flag": (AXIS_FLAGS, 0),
    # Tags written by the LLM before the `flag:` namespace existed.
    "llm": (AXIS_FLAGS, 0),
}
# A tag with no known namespace (a bare `mirai` typed into the tag box) lands on
# the flags axis on purpose: an unrecognised tag must never be able to silently
# empty `original_code`. Give it a `bundle:`/`lib:` prefix to make it provenance.
DEFAULT_NAMESPACE = (AXIS_FLAGS, 0)

# Similarity is bucketed into fixed 5% bins so the UI can re-aggregate to any of
# its 5/10/20/25% split settings without the backend knowing which is selected.
TAG_BINS = 20


def normalize_tags(raw):
    """Function `tags` metadata -> {tag_id: weight}.

    Accepts the two shapes found in function meta: a plain list of tag ids, or a
    mapping of tag id to confidence. Anything else yields no tags.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            try:
                out[str(k)] = float(v)
            except (TypeError, ValueError):
                out[str(k)] = 1.0
        return out
    if isinstance(raw, (list, tuple, set)):
        return {str(k): 1.0 for k in raw if k}
    return {}


def merge_tag_fields(meta):
    """A function's tags from both fields of its metadata document.

    `tags` is what an analyzer wrote (Function ID); `user_tags` is what a human
    or the LLM added. Which axis a tag lands on is decided by its namespace, not
    by the field that carried it, so LLM findings reach the split without being
    promoted into the analyzer's namespace. The analyzer wins a duplicate id.
    """
    meta = meta or {}
    out = normalize_tags(meta.get("tags"))
    for tag_id, conf in normalize_tags(meta.get("user_tags")).items():
        out.setdefault(tag_id, conf)
    return out


def tag_namespace(tag_id):
    """(axis, default priority) for a tag id, from its leading namespace."""
    if tag_id in (TAG_UNTAGGED, TAG_MISMATCH):
        return (AXIS_PROVENANCE, 0)
    head = str(tag_id).split(":", 1)[0]
    return TAG_NAMESPACES.get(head, DEFAULT_NAMESPACE)


def tag_axis(tag_id):
    """Which question this tag answers: AXIS_PROVENANCE or AXIS_FLAGS."""
    return tag_namespace(tag_id)[0]


def tag_priority(tag_id, tag_meta=None):
    """Priority of a tag: explicit metadata beats the namespace default.

    Looked up on the full id first, then on the display parent, so setting a
    priority on `lib:libc:2.31` covers its 400 function-level children without
    touching each one.
    """
    meta = tag_meta or {}
    for key in (tag_id, tag_parent(tag_id)):
        entry = meta.get(key)
        if isinstance(entry, dict) and entry.get("priority") not in (None, ""):
            try:
                return int(entry["priority"])
            except (TypeError, ValueError):
                pass
    return tag_namespace(tag_id)[1]


# Depth a namespace rolls up to in the summary. Provenance keeps
# `type:name:version` (`lib:libc:2.31`) so versions stay separable; flags keep
# `type:name` (`flag:suspicious`) so `flag:suspicious:c2` and
# `flag:suspicious:persistence` nest under the behaviour they refine.
_PARENT_DEPTH = {AXIS_PROVENANCE: 3, AXIS_FLAGS: 2}


def tag_parent(tag_id):
    """Display parent of a tag: `lib:libc:2.31:memcpy` -> `lib:libc:2.31`,
    `flag:suspicious:c2` -> `flag:suspicious`.

    Matching uses the full id; only the summary rolls up. Synthetic buckets and
    already-short ids are their own parent.
    """
    if tag_id in (TAG_UNTAGGED, TAG_MISMATCH):
        return tag_id
    parts = str(tag_id).split(":")
    depth = _PARENT_DEPTH[tag_namespace(tag_id)[0]]
    if len(parts) > depth:
        return ":".join(parts[:depth])
    return tag_id


def split_axes(tags, tag_meta=None):
    """{tag_id: conf} -> {axis: {tag_id: conf}}.

    Provenance is resolved by priority: only the highest-priority origin on a
    function survives, so `lib:libc:2.31` + `bundle:mirai` counts once, as libc.
    Equal priorities keep the even split -- a genuine tie has no better answer
    than "half each". Flags are never filtered; overlap is the point.
    """
    prov, flags = {}, {}
    for tag_id, conf in (tags or {}).items():
        if tag_axis(tag_id) == AXIS_PROVENANCE:
            prov[tag_id] = conf
        else:
            flags[tag_id] = conf
    if len(prov) > 1:
        best = max(tag_priority(t, tag_meta) for t in prov)
        prov = {t: c for t, c in prov.items() if tag_priority(t, tag_meta) == best}
    return {AXIS_PROVENANCE: prov, AXIS_FLAGS: flags}


def parse_tag_id(tag_id):
    """Derive (type, name, version) from a `type:name[:version[:func]]` id."""
    if tag_id == TAG_UNTAGGED:
        return ("original_code", "Original Code", "")
    if tag_id == TAG_MISMATCH:
        return ("mismatch", "Tag mismatch", "")
    parts = tag_id.split(":")
    if len(parts) == 1:
        return ("user", parts[0], "")
    return (parts[0], parts[1], parts[2] if len(parts) > 2 else "")


def _bin_index(score):
    idx = int(score * TAG_BINS)
    if idx < 0:
        return 0
    if idx >= TAG_BINS:
        return TAG_BINS - 1
    return idx


class TagSplit:
    """Accumulates per-tag mass while a binary pair is being matched.

    Usage: `add_match` for every accepted greedy edge, `add_unique` for every
    function left over on either side, then `summary()` once the totals are known.
    """

    def __init__(self, fid_tags, untagged=TAG_UNTAGGED):
        # fid -> {tag_id: weight}; built once per build_bin_sim run, not per pair.
        self.fid_tags = fid_tags
        # Bucket for functions this axis knows nothing about. Provenance needs
        # one (`original_code` is a real answer to "whose code is this"); the
        # flags axis passes None, because "carries no flag" is the absence of a
        # finding, not a finding, and must not become a row competing with the
        # flags that were actually raised.
        self.untagged = untagged
        self._default = {untagged: 1.0} if untagged else {}
        self.cohesion = defaultdict(float)
        self.matched_w = defaultdict(float)
        self.matched_n = defaultdict(float)
        self.side_w = {"a": defaultdict(float), "b": defaultdict(float)}
        self.unique_w = {"a": defaultdict(float), "b": defaultdict(float)}
        self.unique_n = {"a": defaultdict(float), "b": defaultdict(float)}
        # Matched mass whose partner did not carry the same tag. Reported, not
        # rebucketed -- see add_match.
        self.mismatch_w = {"a": defaultdict(float), "b": defaultdict(float)}
        # tag_id -> partner tag id -> weight. `mismatch_w` says a tag's mass failed
        # to agree; this says what it disagreed *with*, which is the difference
        # between "libc has 12 drifted functions" and "libc 2.31 drifted to 2.35".
        self.mismatch_pairs = defaultdict(lambda: defaultdict(float))
        # tag_id -> bin index -> [count_a, weight_a, count_b, weight_b]. Both
        # sides are tracked because a match need not be tagged the same on each.
        self.bins = defaultdict(lambda: defaultdict(lambda: [0.0, 0.0, 0.0, 0.0]))

    def add_match(self, fid_a, fid_b, score, w_a, w_b):
        """Attribute one matched pair. `w_a`/`w_b` are each side's feature counts.

        Each side is attributed to *its own* tags, exactly like `add_unique` does
        for leftovers. Requiring both sides to agree before a match counts toward
        a tag would strand every pair where only one side got recognised, and
        those stranded matches would make the tag look overwhelmingly unmatched:
        A's libc mass counted as libc when it failed to match, but as `untagged`
        when it succeeded. Disagreement is recorded per tag as `mismatch_*`
        instead of moving the mass somewhere else.
        """
        tags_a = self.fid_tags.get(fid_a) or self._default
        tags_b = self.fid_tags.get(fid_b) or self._default
        shared = set(tags_a) & set(tags_b)
        idx = _bin_index(score)

        for side, tags, weight, slot in (
            ("a", tags_a, w_a, 0),
            ("b", tags_b, w_b, 2),
        ):
            n = len(tags)
            for tag_id, conf in tags.items():
                # Confidence caps a tag's claim on its function; a shared tag is
                # only as strong as the weaker side's confidence.
                if tag_id in shared:
                    conf = min(tags_a[tag_id], tags_b[tag_id])
                frac = conf / n
                self.cohesion[tag_id] += score * weight * frac
                self.matched_w[tag_id] += weight * frac
                self.matched_n[tag_id] += frac
                self.side_w[side][tag_id] += weight * frac
                if tag_id not in shared and tag_id != self.untagged:
                    self.mismatch_w[side][tag_id] += weight * frac
                    # Roll the partner up to its display parent: the useful fact
                    # is "drifted to libc 2.35", not which of its 400 functions
                    # this particular edge landed on.
                    others = tags_b if side == "a" else tags_a
                    for partner in others:
                        if partner in shared or partner == self.untagged:
                            continue
                        self.mismatch_pairs[tag_id][tag_parent(partner)] += (
                            weight * frac / len(others)
                        )
                b = self.bins[tag_id][idx]
                b[slot] += frac
                b[slot + 1] += weight * frac

    def add_unique(self, fid, weight, side):
        """Attribute one unmatched function to its own tags (no peer to share with)."""
        tags = self.fid_tags.get(fid) or self._default
        n = len(tags)
        for tag_id, w in tags.items():
            frac = w / n
            self.unique_w[side][tag_id] += weight * frac
            self.unique_n[side][tag_id] += frac

    def summary(self, total_weight_a, total_weight_b, tag_meta=None):
        """Roll up to one row per display parent, with children nested.

        The side totals are each binary's full mass, so coverage answers "how
        much of binary A is this tag". `contribution_pct` is against the mass
        this split itself accounted for, so the rows sum to 100.
        All `*_pct` fields are 0-100.
        """
        tag_meta = tag_meta or {}
        total_weight = sum(self.matched_w.values())
        tag_ids = (
            set(self.matched_w) | set(self.unique_w["a"]) | set(self.unique_w["b"])
        )

        parents = defaultdict(list)
        for tag_id in tag_ids:
            parents[tag_parent(tag_id)].append(tag_id)

        out = []
        for parent, children in parents.items():
            rows = [
                self._row(t, total_weight, total_weight_a, total_weight_b, tag_meta)
                for t in children
            ]
            row = self._row(
                parent,
                total_weight,
                total_weight_a,
                total_weight_b,
                tag_meta,
                merge=rows,
            )
            # A parent that is just its single child adds a pointless nesting level.
            row["children"] = (
                sorted(rows, key=lambda x: -x["matched_weight"])
                if children != [parent]
                else []
            )
            out.append(row)
        return sorted(out, key=lambda x: -x["matched_weight"])

    def _row(self, tag_id, total_w, total_a, total_b, tag_meta, merge=None):
        if merge is None:
            matched_w = self.matched_w.get(tag_id, 0.0)
            mm_a = self.mismatch_w["a"].get(tag_id, 0.0)
            mm_b = self.mismatch_w["b"].get(tag_id, 0.0)
            matched_n = self.matched_n.get(tag_id, 0.0)
            cohesion = self.cohesion.get(tag_id, 0.0)
            w_a = self.side_w["a"].get(tag_id, 0.0)
            w_b = self.side_w["b"].get(tag_id, 0.0)
            uw_a = self.unique_w["a"].get(tag_id, 0.0)
            uw_b = self.unique_w["b"].get(tag_id, 0.0)
            un_a = self.unique_n["a"].get(tag_id, 0.0)
            un_b = self.unique_n["b"].get(tag_id, 0.0)
            bins = {str(k): list(v) for k, v in self.bins.get(tag_id, {}).items()}
            drift = dict(self.mismatch_pairs.get(tag_id, {}))
        else:
            matched_w = sum(r["matched_weight"] for r in merge)
            matched_n = sum(r["matched_count"] for r in merge)
            cohesion = sum(r["score"] * r["matched_weight"] for r in merge)
            w_a = sum(r["weight_a"] for r in merge)
            w_b = sum(r["weight_b"] for r in merge)
            uw_a = sum(r["unique_weight_a"] for r in merge)
            uw_b = sum(r["unique_weight_b"] for r in merge)
            un_a = sum(r["unique_count_a"] for r in merge)
            un_b = sum(r["unique_count_b"] for r in merge)
            mm_a = sum(r["mismatch_weight_a"] for r in merge)
            mm_b = sum(r["mismatch_weight_b"] for r in merge)
            bins = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
            drift = defaultdict(float)
            for r in merge:
                for k, v in r["bins"].items():
                    acc = bins[k]
                    for i in range(4):
                        acc[i] += v[i]
                for partner, w in r["drift"].items():
                    drift[partner] += w
            bins = dict(bins)
            drift = dict(drift)

        t_type, t_name, t_version = parse_tag_id(tag_id)
        meta = tag_meta.get(tag_id) or {}
        return {
            "tag_id": tag_id,
            "type": meta.get("type") or t_type,
            "name": meta.get("name") or t_name,
            "version": meta.get("version") or t_version,
            "source": meta.get("source", "system"),
            "color": meta.get("color", ""),
            "score": cohesion / matched_w if matched_w > 0 else 0.0,
            "contribution_pct": 100.0 * matched_w / total_w if total_w > 0 else 0.0,
            "matched_weight": matched_w,
            "matched_count": matched_n,
            "weight_a": w_a,
            "weight_b": w_b,
            "unique_weight_a": uw_a,
            "unique_weight_b": uw_b,
            "mismatch_weight_a": mm_a,
            "mismatch_weight_b": mm_b,
            "unique_count_a": un_a,
            "unique_count_b": un_b,
            "coverage_pct_a": 100.0 * (w_a + uw_a) / total_a if total_a > 0 else 0.0,
            "coverage_pct_b": 100.0 * (w_b + uw_b) / total_b if total_b > 0 else 0.0,
            "bins": bins,
            # partner tag id -> weight that failed to agree with it. Empty for
            # every tag that matched cleanly.
            "drift": drift,
        }


# Joint-matrix cell layout, mirroring what a Sankey column needs on each side:
# matched and unmatched mass, in features and in function counts.
MATRIX_SLOTS = ("w_shared_a", "w_shared_b", "w_uniq_a", "w_uniq_b",
                "n_shared_a", "n_shared_b", "n_uniq_a", "n_uniq_b")


class AxisSplit:
    """Both axes over one pass of the same matched edges, plus their crossing.

    A `TagSplit` answers each axis alone. The question neither answers is the
    crossing -- "the 30% that matched is libc, and this much of it is flagged
    suspicious" -- so a joint (provenance parent x flag parent) matrix is
    accumulated in the same loop. Keyed by display parent, because that is the
    deepest level any summary row is drawn at.

    Drop-in for `TagSplit` at the call sites: same `add_match` / `add_unique`.
    """

    def __init__(self, fid_tags, tag_meta=None):
        prov, flags = {}, {}
        for fid, tags in (fid_tags or {}).items():
            axes = split_axes(tags, tag_meta)
            if axes[AXIS_PROVENANCE]:
                prov[fid] = axes[AXIS_PROVENANCE]
            if axes[AXIS_FLAGS]:
                flags[fid] = axes[AXIS_FLAGS]
        self.fid_prov = prov
        self.fid_flags = flags
        self.provenance = TagSplit(prov)
        self.flags = TagSplit(flags, untagged=None)
        self.matrix = defaultdict(lambda: defaultdict(lambda: [0.0] * len(MATRIX_SLOTS)))

    def __bool__(self):
        return bool(self.fid_prov or self.fid_flags)

    def _cross(self, fid, weight, w_slot, n_slot):
        """Spread one function's mass over its (provenance, flag) cells.

        Only flagged functions land here, so a provenance row's unflagged mass
        is what the row keeps after its cells are subtracted -- no bucket to
        store, nothing to keep in sync.

        ponytail: the provenance share ignores the confidence capping
        `TagSplit.add_match` applies to a *shared* tag, so with fractional
        confidences the cells can slightly undershoot their row. Consumers
        clamp the remainder at zero; mirror the capping here if confidences
        ever stop being 1.0 in practice.
        """
        flags = self.fid_flags.get(fid)
        if not flags:
            return
        prov = self.fid_prov.get(fid) or {TAG_UNTAGGED: 1.0}
        n_p, n_f = len(prov), len(flags)
        for p, conf in prov.items():
            share = conf / n_p
            for f in flags:
                cell = self.matrix[tag_parent(p)][tag_parent(f)]
                cell[w_slot] += weight * share / n_f
                cell[n_slot] += share / n_f

    def add_match(self, fid_a, fid_b, score, w_a, w_b):
        self.provenance.add_match(fid_a, fid_b, score, w_a, w_b)
        self.flags.add_match(fid_a, fid_b, score, w_a, w_b)
        self._cross(fid_a, w_a, 0, 4)
        self._cross(fid_b, w_b, 1, 5)

    def add_unique(self, fid, weight, side):
        self.provenance.add_unique(fid, weight, side)
        self.flags.add_unique(fid, weight, side)
        self._cross(fid, weight, 2 if side == "a" else 3, 6 if side == "a" else 7)

    def summaries(self, total_weight_a, total_weight_b, tag_meta=None):
        """The three fields a bin_sim doc stores for tags."""
        return {
            "tags_summary": self.provenance.summary(
                total_weight_a, total_weight_b, tag_meta
            ),
            "flags_summary": self.flags.summary(
                total_weight_a, total_weight_b, tag_meta
            ),
            "flag_matrix": {
                p: {f: list(cell) for f, cell in row.items() if any(cell)}
                for p, row in self.matrix.items()
            },
        }


def tags_rev_key(collection):
    return f"{collection}:tags_rev"


def read_tags_rev(r, collection):
    """Current tag revision of a collection.

    A counter bumped by every tag write. A stored split carries the revision it
    was computed against, so the UI can say "this split predates your tagging"
    instead of silently showing stale rows -- and the resplit is only offered
    when it would actually change something.
    """
    try:
        raw = r.get(tags_rev_key(collection))
    except Exception:
        return 0
    if raw is None:
        return 0
    try:
        return int(raw.decode() if isinstance(raw, bytes) else raw)
    except (TypeError, ValueError):
        return 0


def bump_tags_rev(r, collection):
    try:
        r.incr(tags_rev_key(collection))
    except Exception:
        pass


def load_tag_meta(r, collection):
    """Read the collection's tag metadata hash.

    Values are historically a bare colour string (see TagService._ensure_tag_metadata)
    and may be a JSON object once richer metadata is written; both are accepted.
    """
    import json

    try:
        raw = r.hgetall(f"{collection}:tags_metadata") or {}
    except Exception:
        return {}
    out = {}
    for k, v in raw.items():
        tag_id = k.decode() if isinstance(k, bytes) else str(k)
        val = v.decode() if isinstance(v, bytes) else v
        if isinstance(val, str) and val.startswith("{"):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    out[tag_id] = parsed
                    continue
            except ValueError:
                pass
        out[tag_id] = {"color": val}
    return out
