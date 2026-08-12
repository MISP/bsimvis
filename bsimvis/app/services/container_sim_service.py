"""Container similarity -- rolling child-pair scores up the containment edges.

`build_bin_sim` compares code, so it drops containers from its sweep: an APK has
no functions of its own and would score 0 against everything. That leaves the
question "do these two APKs share code?" unanswerable even though every number
needed to answer it -- the score of each `.so` against each other `.so` -- has
already been computed and stored.

This module runs as a second stage at the tail of that same build and answers it
with the formula `build_bin_sim` already uses one level down, with the units
swapped: leaf children instead of functions, function counts instead of BSim
feature counts.

    score = sum(child_score * w) / (sum(w matched) + funcs of every unmatched leaf)
    w     = max(functions in child a, functions in child b)

Children are matched greedily, best pair first, each child used once -- the same
sweep build_bin_sim runs over function pairs. The result is written as an
ordinary bin_sim pair doc under the ordinary keys, so list, search, the
scoreboard and the secondary indexes need no special case; `is_container_pair`
tells a reader that `diff.matched` holds child pairs rather than function pairs.

A file with no container above it is treated as a container of one leaf, which
is what lets a standalone `.so` pair with an APK.

Functionless children (`classes.dex`, resources) cannot enter a function-count
denominator, so the score stays a claim about analyzed code only. Their bytes
are reported separately as `unanalyzed_bytes_*` so the UI can show how much of
the file the score does not speak for.
"""

import json
import logging
import time
from collections import defaultdict

from bsimvis.app.services import lineage_service
from bsimvis.app.services.redis_client import get_redis

ALGO_DEFAULT = "unweighted_cosine"


def _txt(v):
    return v.decode() if isinstance(v, bytes) else str(v)


def _sid(collection, algo, md5_a, md5_b):
    return f"{collection}:bin_sim:{algo}:{md5_a}::{md5_b}"


def _parse_sid(sid, collection, algo):
    """('md5_a', 'md5_b') out of a pair key, or None if it is not one of ours."""
    prefix = f"{collection}:bin_sim:{algo}:"
    if not sid.startswith(prefix):
        return None
    a, sep, b = sid[len(prefix) :].partition("::")
    return (a, b) if sep and a and b else None


def _canonical(md5_a, md5_b):
    return (md5_a, md5_b) if md5_a < md5_b else (md5_b, md5_a)


# ---------------------------------------------------------------------------
# Reading one side
# ---------------------------------------------------------------------------


def leaf_info(collection, md5, r=None, containers=None):
    """Code-bearing files under `md5`, as `({leaf: functions}, {descendants})`.

    The `lineage:funcs` hash is written when a child is *indexed*, so a child
    that gained its containment edge later is missing from it -- and a child
    with no functions at all was never recorded there at all, which is exactly
    the unanalyzed mass we want to see. So the child list comes from the edges
    and the hash is consulted only for counts, with the file's own document as
    the fallback.

    A file that is not a container stands for itself, so a standalone binary and
    an APK can be handed to the same code.
    """
    r = r or get_redis()
    if containers is None:
        containers = lineage_service.container_md5s(collection, r)

    if md5 not in containers:
        return {md5: _own_function_count(collection, md5, r)}, set()

    desc = {e["md5"] for e in lineage_service.descendants(collection, md5, r)}
    # A nested container contributes its children but never itself: its own
    # document already states the subtree total (lineage_service restates it),
    # so counting it too would count those functions twice.
    leaves = desc - containers

    counts = {}
    raw = r.hgetall(lineage_service._key(collection, "funcs", md5)) or {}
    for k, v in raw.items():
        leaf = _txt(k)
        if leaf not in leaves:
            continue
        try:
            counts[leaf] = int(_txt(v))
        except ValueError:
            continue

    missing = [m for m in leaves if m not in counts]
    if missing:
        pipe = r.pipeline(transaction=False)
        for m in missing:
            pipe.get(f"{collection}:file:{m}:meta")
        for m, doc in zip(missing, pipe.execute()):
            counts[m] = _function_count_of(doc)

    return counts, desc


def _own_function_count(collection, md5, r):
    return _function_count_of(r.get(f"{collection}:file:{md5}:meta"))


def _function_count_of(raw):
    if not raw:
        return 0
    try:
        return int(json.loads(_txt(raw)).get("function_count") or 0)
    except (ValueError, TypeError, AttributeError):
        return 0


def _byte_sizes(collection, md5s, r):
    """Stored size per file. 0 means unknown -- raw bytes pruned or never kept."""
    md5s = list(md5s)
    if not md5s:
        return {}
    pipe = r.pipeline(transaction=False)
    for m in md5s:
        pipe.strlen(f"{collection}:file:{m}:raw")
    return {m: int(n or 0) for m, n in zip(md5s, pipe.execute())}


# ---------------------------------------------------------------------------
# The algorithm
# ---------------------------------------------------------------------------


def aggregate(edges, funcs_a, funcs_b, coverage=None):
    """Roll child-pair scores into one score for the two sides.

    `edges` is `(leaf_a, leaf_b, score, sid)`; `funcs_*` maps every leaf on that
    side to its function count, matched or not. `coverage` maps a sid to
    `{leaf_md5: coverage}` -- keyed by leaf rather than by position, because
    pair docs are stored in canonical md5 order and either leaf can be the `a`
    side of its own doc. A missing entry counts the pair as fully covering,
    which happens only if the pair doc vanished underneath us.

    Pure -- no redis, no keys. This is the whole formula, and `demo()` pins it.
    """
    coverage = coverage or {}
    # Best pair first, md5s as the tie-break so a rebuild picks the same match:
    # the ordering rule build_bin_sim uses over function pairs.
    ranked = sorted(
        (e for e in edges if e[2] and e[2] > 0),
        key=lambda e: (-e[2], e[0], e[1]),
    )

    taken_a, taken_b, matched = set(), set(), []
    num = den = 0.0
    cov_num_a = cov_num_b = 0.0

    for leaf_a, leaf_b, score, sid in ranked:
        if leaf_a in taken_a or leaf_b in taken_b:
            continue
        taken_a.add(leaf_a)
        taken_b.add(leaf_b)
        n_a = funcs_a.get(leaf_a, 0)
        n_b = funcs_b.get(leaf_b, 0)
        w = max(n_a, n_b)
        num += score * w
        den += w
        cov = coverage.get(sid) or {}
        cov_a = float(cov.get(leaf_a, 1.0))
        cov_b = float(cov.get(leaf_b, 1.0))
        cov_num_a += n_a * cov_a
        cov_num_b += n_b * cov_b
        matched.append(
            {
                "md5_a": leaf_a,
                "md5_b": leaf_b,
                # `similarity`, not `score`: the diff pager already filters and
                # sorts function rows on that key (bin_sim.py:717,780), and a
                # child row is the same kind of row one level up.
                "similarity": score,
                "coverage_a": cov_a,
                "coverage_b": cov_b,
                "functions_count_a": n_a,
                "functions_count_b": n_b,
                "sid": sid,
            }
        )

    unique_a = [
        {"md5": m, "functions_count": n}
        for m, n in sorted(funcs_a.items())
        if m not in taken_a
    ]
    unique_b = [
        {"md5": m, "functions_count": n}
        for m, n in sorted(funcs_b.items())
        if m not in taken_b
    ]
    den += sum(u["functions_count"] for u in unique_a)
    den += sum(u["functions_count"] for u in unique_b)

    total_a = sum(funcs_a.values())
    total_b = sum(funcs_b.values())

    return {
        "score": (num / den) if den > 0 else 0.0,
        "coverage_a": (cov_num_a / total_a) if total_a else 0.0,
        "coverage_b": (cov_num_b / total_b) if total_b else 0.0,
        "functions_count_a": total_a,
        "functions_count_b": total_b,
        "matched": matched,
        "unique_to_a": unique_a,
        "unique_to_b": unique_b,
    }


# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------


def build_container_sims(
    collection,
    algo=ALGO_DEFAULT,
    pair_scores=None,
    r=None,
    job_service=None,
    job_id=None,
):
    """Write a bin_sim pair doc for every container that shares code with something.

    `pair_scores` is the `{(md5_a, md5_b): score}` map `build_bin_sim` already
    holds in memory when its sweep ends; anything not in it is read back from
    the scoreboard, so a full sweep and a two-file incremental build take the
    same path.
    """
    r = r or get_redis()
    pair_scores = pair_scores or {}
    containers = lineage_service.container_md5s(collection, r)
    if not containers:
        return 0

    sides, desc_of = {}, {}
    for c in containers:
        sides[c], desc_of[c] = leaf_info(collection, c, r, containers)

    # A leaf reports up to every container above it, so a nested zip and the APK
    # around it each get their own row.
    owners = defaultdict(set)
    for c, leaves in sides.items():
        for leaf in leaves:
            owners[leaf].add(c)
    if not owners:
        return 0

    edges_by_pair, unknown = _collect_edges(
        collection, algo, owners, desc_of, pair_scores, r
    )
    if not edges_by_pair:
        return 0

    scores = dict(pair_scores)
    scores.update(_read_scores(collection, algo, unknown, r))

    def leaves_of(md5):
        if md5 not in sides:
            sides[md5], _ = leaf_info(collection, md5, r, containers)
        return sides[md5]

    scored_by_pair = {
        pair: [(a, b, scores.get(_canonical(a, b), 0.0), sid) for a, b, sid in edges]
        for pair, edges in edges_by_pair.items()
    }

    # Match on scores alone first, then read coverage only for the pairs that
    # won: the matched set is bounded by the smaller side's child count, far
    # fewer docs than every candidate.
    provisional = {
        pair: aggregate(scored, leaves_of(pair[0]), leaves_of(pair[1]))
        for pair, scored in scored_by_pair.items()
    }
    coverage = _read_coverage(
        collection,
        algo,
        {m["sid"] for agg in provisional.values() for m in agg["matched"]},
        r,
    )

    files = set()
    for (p, q), scored in scored_by_pair.items():
        files.update((p, q))
        files.update(leaves_of(p))
        files.update(leaves_of(q))
    meta = _load_meta(collection, files, r)
    sizes = _byte_sizes(collection, files, r)
    paths = _child_paths(collection, scored_by_pair, containers, r)

    from bsimvis.app.services.bin_sim_service import _index_bin_sim_pair

    pipe = r.pipeline(transaction=False)
    written = 0
    for (p, q), scored in scored_by_pair.items():
        agg = aggregate(scored, leaves_of(p), leaves_of(q), coverage)
        if not agg["matched"]:
            continue
        doc = _build_doc(
            algo, p, q, agg, leaves_of(p), leaves_of(q), meta, sizes, paths, containers
        )
        sid = _sid(collection, algo, p, q)
        pipe.set(sid, json.dumps(doc))
        pipe.zadd(f"{collection}:bin_sim:score:{algo}", {sid: doc["score"]})
        pipe.sadd(f"{collection}:bin_sim:involves:{p}", sid)
        pipe.sadd(f"{collection}:bin_sim:involves:{q}", sid)
        pipe.sadd(f"{collection}:bin_sim:built:{algo}", sid)
        _index_bin_sim_pair(pipe, collection, sid, doc, meta.get(p), meta.get(q))
        written += 1
        if written % 100 == 0:
            pipe.execute()
            pipe = r.pipeline(transaction=False)
    pipe.execute()

    if job_service and job_id:
        job_service.add_log(job_id, f"[*] Wrote {written} container similarity pairs")
    return written


def _collect_edges(collection, algo, owners, desc_of, pair_scores, r):
    """Every child pair that says something about two containers, grouped by pair."""
    edges_by_pair = defaultdict(list)
    unknown = set()

    leaves = list(owners)
    pipe = r.pipeline(transaction=False)
    for leaf in leaves:
        pipe.smembers(f"{collection}:bin_sim:involves:{leaf}")

    for leaf, sids in zip(leaves, pipe.execute()):
        for sid_raw in sids or ():
            sid = _txt(sid_raw)
            parsed = _parse_sid(sid, collection, algo)
            if not parsed:
                continue
            m_a, m_b = parsed
            other = m_b if m_a == leaf else m_a
            if other == leaf:
                continue
            # Both sides report up to their containers *and* stand for
            # themselves, so the same evidence is answerable at every altitude:
            # .so vs .so, .so vs APK, APK vs APK. Without the file-vs-container
            # rung, a file that lives inside a container has no container row to
            # fold its own matches under.
            for near in owners[leaf] | {leaf}:
                for far in (owners.get(other) or set()) | {other}:
                    if near == far:
                        continue
                    # Both sides standing for themselves is the function-level
                    # pair build_bin_sim already wrote; re-writing it here would
                    # replace real function rows with child rows.
                    if near == leaf and far == other:
                        continue
                    # A container against something it contains compares a file
                    # with itself; there is no similarity question there.
                    if far in desc_of.get(near, ()) or near in desc_of.get(far, ()):
                        continue
                    p, q = _canonical(near, far)
                    leaf_p, leaf_q = (leaf, other) if p == near else (other, leaf)
                    edges_by_pair[(p, q)].append((leaf_p, leaf_q, sid))
                    if parsed not in pair_scores:
                        unknown.add(sid)

    return edges_by_pair, unknown


def _read_scores(collection, algo, sids, r):
    """Child-pair scores this build did not compute itself."""
    sids = list(sids)
    if not sids:
        return {}
    pipe = r.pipeline(transaction=False)
    for sid in sids:
        pipe.zscore(f"{collection}:bin_sim:score:{algo}", sid)
    out = {}
    for sid, val in zip(sids, pipe.execute()):
        parsed = _parse_sid(sid, collection, algo)
        if parsed and val is not None:
            out[parsed] = float(val)
    return out


def _read_coverage(collection, algo, sids, r):
    """`{sid: {leaf_md5: coverage}}` for the child pairs that won their match."""
    sids = [s for s in sids if s]
    if not sids:
        return {}
    pipe = r.pipeline(transaction=False)
    for sid in sids:
        pipe.get(sid)
    out = {}
    for sid, raw in zip(sids, pipe.execute()):
        if not raw:
            continue
        try:
            doc = json.loads(_txt(raw))
        except (ValueError, TypeError):
            continue
        # Keyed by md5, so the caller never has to know which side of the stored
        # doc a given leaf landed on.
        out[sid] = {
            doc.get("md5_a"): float(doc.get("coverage_a") or 0.0),
            doc.get("md5_b"): float(doc.get("coverage_b") or 0.0),
        }
    return out


def _load_meta(collection, md5s, r):
    md5s = list(md5s)
    if not md5s:
        return {}
    pipe = r.pipeline(transaction=False)
    for m in md5s:
        pipe.get(f"{collection}:file:{m}:meta")
    out = {}
    for m, raw in zip(md5s, pipe.execute()):
        if not raw:
            continue
        try:
            out[m] = json.loads(_txt(raw))
        except (ValueError, TypeError):
            continue
    return out


def _child_paths(collection, scored_by_pair, containers, r):
    """`{container: {child: path_in_parent}}` for the containers being written."""
    out = {}
    for pair in scored_by_pair:
        for side in pair:
            if side in out or side not in containers:
                continue
            out[side] = {
                e["md5"]: e["path"]
                for e in lineage_service.descendants(collection, side, r)
            }
    return out


def _build_doc(algo, p, q, agg, funcs_p, funcs_q, meta, sizes, paths, containers):
    meta_p = meta.get(p, {})
    meta_q = meta.get(q, {})

    for row in agg["matched"]:
        row.pop("sid", None)
        row["file_name_a"] = (meta.get(row["md5_a"]) or {}).get("file_name", "")
        row["file_name_b"] = (meta.get(row["md5_b"]) or {}).get("file_name", "")
        row["path_in_parent_a"] = (paths.get(p) or {}).get(row["md5_a"], "")
        row["path_in_parent_b"] = (paths.get(q) or {}).get(row["md5_b"], "")

    for side, rows in ((p, agg["unique_to_a"]), (q, agg["unique_to_b"])):
        for row in rows:
            m = row["md5"]
            row["file_name"] = (meta.get(m) or {}).get("file_name", "")
            row["path_in_parent"] = (paths.get(side) or {}).get(m, "")
            row["bytes"] = sizes.get(m, 0)

    analyzed_p, unanalyzed_p = _mass(funcs_p, sizes)
    analyzed_q, unanalyzed_q = _mass(funcs_q, sizes)

    return {
        "md5_a": p,
        "md5_b": q,
        "algo": algo,
        "is_container_pair": True,
        "is_container_a": p in containers,
        "is_container_b": q in containers,
        "architecture_a": meta_p.get("language_id", ""),
        "architecture_b": meta_q.get("language_id", ""),
        "functions_count_a": agg["functions_count_a"],
        "functions_count_b": agg["functions_count_b"],
        "child_count_a": len(funcs_p),
        "child_count_b": len(funcs_q),
        "analyzed_bytes_a": analyzed_p,
        "unanalyzed_bytes_a": unanalyzed_p,
        "analyzed_bytes_b": analyzed_q,
        "unanalyzed_bytes_b": unanalyzed_q,
        "score": agg["score"],
        "coverage_a": agg["coverage_a"],
        "coverage_b": agg["coverage_b"],
        "shared_clusters": len(agg["matched"]),
        "unique_clusters_a": len(agg["unique_to_a"]),
        "unique_clusters_b": len(agg["unique_to_b"]),
        "unclustered_a": len(agg["unique_to_a"]),
        "unclustered_b": len(agg["unique_to_b"]),
        "computed_at": int(time.time() * 1000),
        "tags_summary": [],
        "diff": {
            "matched": agg["matched"],
            "unique_to_a": agg["unique_to_a"],
            "unique_to_b": agg["unique_to_b"],
            "unclustered_a": [],
            "unclustered_b": [],
        },
    }


def _mass(funcs, sizes):
    """(bytes the score speaks for, bytes it does not) for one side's leaves."""
    analyzed = unanalyzed = 0
    for leaf, n in funcs.items():
        size = sizes.get(leaf, 0)
        if n > 0:
            analyzed += size
        else:
            unanalyzed += size
    return analyzed, unanalyzed


def clear_for(collection, md5, algo=ALGO_DEFAULT, r=None):
    """Drop the container pair docs that a change to `md5` invalidates.

    A container's score is a statement about its children, so deleting or
    rebuilding a child leaves every container above it asserting a number it can
    no longer support.
    """
    r = r or get_redis()
    from bsimvis.app.services.bin_sim_service import _unindex_bin_sim_pair

    targets = {e["md5"] for e in lineage_service.ancestors(collection, md5, r)}
    targets &= lineage_service.container_md5s(collection, r)
    if not targets:
        return 0

    dropped = 0
    for container in targets:
        involves = f"{collection}:bin_sim:involves:{container}"
        sids = [_txt(s) for s in (r.smembers(involves) or ())]
        if not sids:
            continue
        reader = r.pipeline(transaction=False)
        for sid in sids:
            reader.get(sid)
        pipe = r.pipeline(transaction=False)
        for sid, raw in zip(sids, reader.execute()):
            try:
                doc = json.loads(_txt(raw)) if raw else {}
            except (ValueError, TypeError):
                doc = {}
            if not doc.get("is_container_pair"):
                continue
            other = (
                doc.get("md5_b") if doc.get("md5_a") == container else doc.get("md5_a")
            )
            pipe.delete(sid)
            pipe.zrem(f"{collection}:bin_sim:score:{algo}", sid)
            pipe.srem(f"{collection}:bin_sim:built:{algo}", sid)
            pipe.srem(involves, sid)
            if other:
                pipe.srem(f"{collection}:bin_sim:involves:{other}", sid)
            _unindex_bin_sim_pair(pipe, collection, sid, doc)
            dropped += 1
        pipe.execute()
    return dropped


def demo():
    """Self-check for the formula. Pure arithmetic, no kvrocks needed."""
    # One matched child, nothing unmatched: the container says exactly what the
    # child pair says, whatever the child's size.
    agg = aggregate([("a1", "b1", 0.8, "s1")], {"a1": 100}, {"b1": 100})
    assert abs(agg["score"] - 0.8) < 1e-9, agg["score"]
    assert agg["functions_count_a"] == 100

    # A big unmatched child dilutes by exactly its own mass: 0.8*100 / 300.
    agg = aggregate([("a1", "b1", 0.8, "s1")], {"a1": 100, "a2": 200}, {"b1": 100})
    assert abs(agg["score"] - (0.8 * 100) / 300) < 1e-9, agg["score"]
    assert [u["md5"] for u in agg["unique_to_a"]] == ["a2"]

    # Weight is the larger side, matching build_bin_sim's max(features).
    agg = aggregate([("a1", "b1", 1.0, "s1")], {"a1": 10}, {"b1": 90})
    assert abs(agg["score"] - 1.0) < 1e-9, agg["score"]

    # Greedy: a child already spent on a better pair cannot be reused, and the
    # loser becomes unmatched mass on both sides.
    agg = aggregate(
        [("a1", "b1", 0.9, "s1"), ("a1", "b2", 0.5, "s2")],
        {"a1": 100},
        {"b1": 100, "b2": 100},
    )
    assert [m["md5_b"] for m in agg["matched"]] == ["b1"]
    assert abs(agg["score"] - (0.9 * 100) / 200) < 1e-9, agg["score"]

    # Zero scores are not evidence and never take a child hostage.
    agg = aggregate(
        [("a1", "b1", 0.0, "s1"), ("a1", "b2", 0.4, "s2")],
        {"a1": 10},
        {"b1": 10, "b2": 10},
    )
    assert [m["md5_b"] for m in agg["matched"]] == ["b2"]

    # Coverage is the child's coverage weighted by the child's own mass, not by
    # the pair's: a fully covered tiny child cannot claim the whole container.
    agg = aggregate(
        [("a1", "b1", 0.5, "s1")],
        {"a1": 10, "a2": 90},
        {"b1": 10},
        {"s1": {"a1": 1.0, "b1": 1.0}},
    )
    assert abs(agg["coverage_a"] - 0.1) < 1e-9, agg["coverage_a"]
    assert abs(agg["coverage_b"] - 1.0) < 1e-9, agg["coverage_b"]

    # Coverage is keyed by leaf, so it survives a pair doc stored the other way
    # round -- the bug this keying exists to prevent.
    flipped = aggregate(
        [("z1", "a1", 0.5, "s1")],
        {"z1": 10},
        {"a1": 10},
        {"s1": {"a1": 0.25, "z1": 0.75}},
    )
    assert abs(flipped["coverage_a"] - 0.75) < 1e-9, flipped["coverage_a"]
    assert abs(flipped["coverage_b"] - 0.25) < 1e-9, flipped["coverage_b"]

    # Nothing on either side: a score of 0, not a division by zero.
    assert aggregate([], {}, {})["score"] == 0.0
    assert aggregate([], {"a1": 0}, {"b1": 0})["score"] == 0.0

    print("container_sim_service demo OK")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    demo()
