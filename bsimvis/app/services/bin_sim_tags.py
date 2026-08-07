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

It adds no asymptotic cost: everything here is O(1) per already-matched edge.
"""

from collections import defaultdict

# Two distinct kinds of "we can't attribute this to a library", kept apart on
# purpose: UNTAGGED means at least one side carries no tag at all (no evidence),
# MISMATCH means both sides are tagged but share nothing -- the interesting case,
# e.g. libc 2.31 matching libc 2.35, or libc matching uclibc.
TAG_UNTAGGED = "original_code"
TAG_MISMATCH = "tag_mismatch"

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


def tag_parent(tag_id):
    """Display parent of a tag: `lib:libc:2.31:memcpy` -> `lib:libc:2.31`.

    Matching uses the full id; only the summary rolls up. Synthetic buckets and
    already-short ids are their own parent.
    """
    if tag_id in (TAG_UNTAGGED, TAG_MISMATCH):
        return tag_id
    parts = tag_id.split(":")
    if len(parts) > 3:
        return ":".join(parts[:3])
    return tag_id


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

    def __init__(self, fid_tags):
        # fid -> {tag_id: weight}; built once per build_bin_sim run, not per pair.
        self.fid_tags = fid_tags
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
        tags_a = self.fid_tags.get(fid_a) or {TAG_UNTAGGED: 1.0}
        tags_b = self.fid_tags.get(fid_b) or {TAG_UNTAGGED: 1.0}
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
                if tag_id not in shared and tag_id != TAG_UNTAGGED:
                    self.mismatch_w[side][tag_id] += weight * frac
                    # Roll the partner up to its display parent: the useful fact
                    # is "drifted to libc 2.35", not which of its 400 functions
                    # this particular edge landed on.
                    others = tags_b if side == "a" else tags_a
                    for partner in others:
                        if partner in shared or partner == TAG_UNTAGGED:
                            continue
                        self.mismatch_pairs[tag_id][tag_parent(partner)] += (
                            weight * frac / len(others)
                        )
                b = self.bins[tag_id][idx]
                b[slot] += frac
                b[slot + 1] += weight * frac

    def add_unique(self, fid, weight, side):
        """Attribute one unmatched function to its own tags (no peer to share with)."""
        tags = self.fid_tags.get(fid) or {TAG_UNTAGGED: 1.0}
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
