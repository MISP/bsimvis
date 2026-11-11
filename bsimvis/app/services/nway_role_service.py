"""Triage role of each N-way row: origin first, then call-graph shape.

Origin comes from what the lib analyzer wrote (`fid:`, `origin:lib:`,
`boilerplate:` tags): runtime, library, or the binary's own code. Shape comes
from the call graph and size alone, with thresholds taken per file:

    plumbing      stub (calls <= 1, size <= p25), wrapper (calls == 1, has a caller,
                  size <= p50) or shared helper (callers >= max(6, p95), callees <= 2)
    orchestrator  internal callees >= max(8, p95)
    entry         no internal caller (thread, signal handler, pointer table)
    compute       calls nothing, size >= p75
    leaf          calls nothing
    logic         the rest

A row takes the majority role of its functions. An analyst override is a
`role:<role>` user tag on the functions; it wins over the rules.

Measured on 33.8k functions (rapperbot, mirai, RondoDox, novispy): shape cannot
tell library from malware code, which is why origin is a separate axis; plumbing
is ~30% of a binary's own functions but ~4% of its instructions.
"""

from collections import Counter, defaultdict

from bsimvis.app.services.bin_sim_tags import is_library_tag, merge_tag_fields

ROLES = (
    (
        "orchestrator",
        "Orchestrator",
        "Calls many functions of its file: read first for the program flow.",
    ),
    (
        "entry",
        "Entry point",
        "No caller in the call graph: thread, signal handler or pointer-table entry.",
    ),
    ("logic", "Logic", "Calls and is called: the body of each feature."),
    ("compute", "Compute", "Large and calls nothing: decoders, checksums, crypto."),
    ("leaf", "Leaf helper", "Small and calls nothing."),
    (
        "plumbing",
        "Plumbing",
        "Stubs, one-call wrappers and helpers called from everywhere.",
    ),
    ("runtime", "Runtime", "C runtime start-up and teardown."),
    (
        "library",
        "Library",
        "Recognised by the library analyzer (FunctionID, stdlib reference).",
    ),
)
ROLE_IDS = tuple(k for k, _, _ in ROLES)
RANK = {k: i for i, k in enumerate(ROLE_IDS)}
TAG = "role:"
CRT = {
    "entry",
    "_start",
    "_init",
    "_fini",
    "frame_dummy",
    "__do_global_dtors_aux",
    "register_tm_clones",
    "deregister_tm_clones",
    "__libc_start_main",
    "__uClibc_main",
    "__libc_csu_init",
    "__libc_csu_fini",
    "_dl_start",
}
MAX_FIDS = 150000  # one Redis set read per function
CHUNK = 2000


def override(meta):
    """The analyst's `role:<role>` tag on a function, or None."""
    for t in merge_tag_fields(meta):
        if t.startswith(TAG) and t[len(TAG) :] in RANK:
            return t[len(TAG) :]
    return None


def origin(meta):
    tags = list(merge_tag_fields(meta))
    if meta.get("name") in CRT or any(
        t.startswith("boilerplate:runtime") for t in tags
    ):
        return "runtime"
    if any(is_library_tag(t) for t in tags):
        return "library"
    return None


def shape(i, o, x, s, th):
    """Role from callers `i`, internal callees `o`, external calls `x`, size `s`."""
    calls = o + x
    if calls <= 1 and s <= th["s25"]:
        return "plumbing"
    if calls == 1 and i >= 1 and s <= th["s50"]:
        return "plumbing"
    if o >= th["o95"]:
        return "orchestrator"
    if i >= th["i95"] and o <= 2:
        return "plumbing"
    if i == 0:
        return "entry"
    if calls == 0:
        return "compute" if s >= th["s75"] else "leaf"
    return "logic"


def _pct(vals, p):
    vals = sorted(vals)
    return vals[min(len(vals) - 1, int(len(vals) * p))] if vals else 0


def thresholds(sizes, ins, outs):
    return {
        "s25": _pct(sizes, 0.25),
        "s50": _pct(sizes, 0.5),
        "s75": _pct(sizes, 0.75),
        "o95": max(8, _pct(outs, 0.95)),
        "i95": max(6, _pct(ins, 0.95)),
    }


def _size(meta):
    return float(meta.get("instruction_count") or meta.get("bsim_features_count") or 0)


def function_roles(r, fmeta, calls=None):
    """{fid: (role, source)}, source "user" / "tag" / "graph"; {} over MAX_FIDS.

    `calls`, when given, is filled with each function's internal callees.
    """
    fids = list(fmeta)
    if len(fids) > MAX_FIDS:
        return {}
    # Edges never leave a binary and the doc holds every function of its files,
    # so callers are counted from the callee sets: one read per function.
    deg, n_in = {}, Counter()
    for at in range(0, len(fids), CHUNK):
        part = fids[at : at + CHUNK]
        pipe = r.pipeline(transaction=False)
        for f in part:
            pipe.smembers(f"{f}:callees")
        for f, callees in zip(part, pipe.execute()):
            callees = {c.decode() if isinstance(c, bytes) else c for c in callees}
            callees.discard(f)
            ext = sum(1 for c in callees if c.startswith("ext:"))
            deg[f] = (len(callees) - ext, ext)
            inner = [c for c in callees if c in fmeta]
            n_in.update(inner)
            if calls is not None:
                calls[f] = inner
    deg = {f: (n_in[f], o, x) for f, (o, x) in deg.items()}
    by_file = defaultdict(list)
    for f in fids:
        by_file[f.rsplit(":", 1)[0]].append(f)  # `{coll}:func:{md5}`
    out = {}
    for members in by_file.values():
        th = thresholds(
            [_size(fmeta[f]) for f in members],
            [deg[f][0] for f in members],
            [deg[f][1] for f in members],
        )
        for f in members:
            meta = fmeta[f]
            if own := override(meta):
                out[f] = (own, "user")
            elif kind := origin(meta):
                out[f] = (kind, "tag")
            else:
                out[f] = (shape(*deg[f], _size(meta), th), "graph")
    return out


def vote(pairs):
    """(role, source) of a set of functions: overrides first, then the majority."""
    if not pairs:
        return None, None
    user = [k for k, src in pairs if src == "user"]
    pool = user or [k for k, _ in pairs]
    count = Counter(pool)
    best = max(count, key=lambda k: (count[k], -RANK[k]))
    return best, "user" if user else "rules"


def attach(r, doc, fids_of):
    """Set `role` / `role_src` on every row of `doc` once (cached on the doc)."""
    if "_roles" not in doc:
        # requests share the cached doc: build both maps aside, publish the calls first
        calls = {}
        roles = function_roles(r, doc["fmeta"], calls) if r is not None else {}
        doc["_calls"] = calls
        doc["_roles"] = roles
    roles = doc["_roles"]
    if not roles:
        return
    for row in doc["rows"]:
        if "role" not in row:
            row["role"], row["role_src"] = vote(
                [roles[f] for f in fids_of(row) if f in roles]
            )


def unit_role(u):
    """A merged function votes over its rows' roles; a plain row has its own."""
    if "_rows" not in u:
        return u.get("role")
    return vote([(x["role"], x.get("role_src")) for x in u["_rows"] if x.get("role")])[
        0
    ]


def demo():
    th = thresholds([5, 10, 20, 40, 80, 160, 320, 640], [0] * 8, [0] * 8)
    assert shape(1, 0, 0, 5, th) == "plumbing"  # tiny stub
    assert shape(2, 1, 0, 20, th) == "plumbing"  # one-call wrapper
    assert shape(1, 9, 0, 320, th) == "orchestrator"
    assert shape(7, 1, 0, 160, th) == "plumbing"  # shared helper
    assert shape(0, 3, 0, 160, th) == "entry"
    assert shape(1, 0, 0, 640, th) == "compute"
    assert shape(1, 0, 0, 80, th) == "leaf"
    assert shape(1, 3, 1, 160, th) == "logic"
    assert override({"user_tags": ["role:compute"]}) == "compute"
    assert override({"user_tags": ["role:bogus"]}) is None
    assert origin({"name": "_start"}) == "runtime"
    assert origin({"tags": ["fid:libc"]}) == "library"
    assert vote([("leaf", "graph"), ("logic", "graph"), ("logic", "graph")]) == (
        "logic",
        "rules",
    )
    assert vote([("leaf", "graph"), ("entry", "user")]) == ("entry", "user")
    assert (
        unit_role(
            {"_rows": [{"role": "leaf"}, {"role": "orchestrator", "role_src": "user"}]}
        )
        == "orchestrator"
    )
    print("nway_role_service demo ok")


if __name__ == "__main__":
    demo()
