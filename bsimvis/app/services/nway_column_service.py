"""Metadata column groups for the N-way table.

A group is one column standing for every file that shares a value (collection,
batch, architecture, a tag namespace...) or that a saved file query matched.
`nway_diff_service.project_rows` re-expresses the rows over these groups, the
same way it does for child-cluster and twin columns.

Group shape: `{id, kind, label, members, member_count}` with `members` the file
column ids. A group of one file is a plain `file` column.
"""

import hashlib
import json
import re
from collections import Counter

from bsimvis.app.services.index_service import normalize_tags
from bsimvis.app.services.nway_diff_service import BadParams, _col_id, _decode

KEY = re.compile(r"^[\w.:-]{1,64}$")
NONE_LABEL = "(none)"
MAX_VALUES = 200  # distinct values `axes` still offers for one key
MAX_LISTED = 60  # values listed per key, most files first
MAX_VALUE_LEN = 80
PRESENCE = {"any": 1e-9, "half": 0.5, "all": 1.0}


def read_metas(r, members):
    """File meta dicts of `members` (`(collection, md5)`), `{}` for a missing one."""
    pipe = r.pipeline(transaction=False)
    for coll, md5 in members:
        pipe.get(f"{coll}:file:{md5}:meta")
    metas = []
    for raw in pipe.execute():
        meta = _decode(raw) if raw else {}
        metas.append(normalize_tags(meta if isinstance(meta, dict) else {}))
    return metas


def _scalar(v):
    return isinstance(v, (str, int, float)) and not isinstance(v, bool) or v is True


def values(meta, coll, key):
    """The values `key` takes on one file (several for a list or a tag namespace)."""
    if key == "collection":
        return [coll]
    if key.startswith("tag:"):
        ns = key[4:] + ":"
        tags = meta["tags"] + meta["user_tags"]
        return list(dict.fromkeys(t[len(ns) :] for t in tags if t.startswith(ns)))
    v = meta.get(key.removeprefix("field:"))
    items = v if isinstance(v, list) else [v]
    return [str(i) for i in items if i not in ("", None) and _scalar(i)]


def _gid(key, value):
    return "by:" + hashlib.md5(f"{key}\0{value}".encode()).hexdigest()[:8]


def _finish(groups, names):
    """Biggest group first; a group of one file is a plain file column."""
    out = []
    for g in sorted(groups, key=lambda g: (-len(g["members"]), g["label"])):
        g["member_count"] = len(g["members"])
        if g["member_count"] == 1:
            g.update(kind="file", id=g["members"][0], label=names[g["members"][0]])
        out.append(g)
    return out


def _picked(by, key, pick, drop):
    """Columns for the picked values, in the order given, plus the unpicked files as `other`."""
    groups = [
        {
            "id": _gid(key, v),
            "kind": "meta",
            "by": key,
            "value": v,
            "label": v,
            "members": by[v],
        }
        for v in dict.fromkeys(pick)
        if v in by
    ]
    rest = [m for v, ids in by.items() if v not in pick for m in ids]
    if rest and not drop:
        groups.append(
            {
                "id": _gid(key, "\0other"),
                "kind": "meta",
                "by": key,
                "value": None,
                "label": "other",
                "members": list(dict.fromkeys(rest)),
            }
        )
    if len(groups) < 2:
        raise BadParams(f"pick at least two {key} values that some files have")
    for g in groups:
        g["member_count"] = len(g["members"])
    return groups


def meta_groups(members, metas, names, key, multi="first", pick=None, drop=False):
    """Group files by `key`; raises BadParams when it leaves a single group.

    `multi=first` puts a file with several values (a tag namespace, a list) under
    its most shared one; `all` lists it under each. `pick` (a list of values) makes
    only those values columns, in that order, and the rest one `other` column, or
    nothing with `drop` (the other files then leave the set).
    """
    if not KEY.match(key or ""):
        raise BadParams("col_by must be collection, tag:NAMESPACE or a file field")
    if multi not in ("first", "all"):
        raise BadParams("col_by_multi must be first or all")
    vals = [values(m, c, key) for (c, _), m in zip(members, metas)]
    shared = Counter(v for vs in vals for v in vs)
    by = {}
    for (coll, md5), vs in zip(members, vals):
        if not vs:
            vs = [NONE_LABEL]
        elif multi == "first":
            vs = [max(vs, key=lambda v: (shared[v], v))]
        for v in vs:
            by.setdefault(v, []).append(_col_id(coll, md5))
    if pick:
        return _picked(by, key, pick, drop)
    if len(by) < 2:
        raise BadParams(f"every file has the same {key}: nothing to split")
    return _finish(
        [
            {
                "id": _gid(key, v),
                "kind": "meta",
                "by": key,
                "value": v,
                "label": v,
                "members": ids,
            }
            for v, ids in by.items()
        ],
        names,
    )


def query_groups(colset, names):
    """Groups of a saved query set (see `routes.nway.post_column_set`).

    They keep the order and labels they were given (A is the first column, even
    when it holds one file).
    """
    out = []
    for i, g in enumerate(colset["groups"]):
        members = [m for m in g["members"] if m in names]
        out.append(
            {
                "id": f"q:{i}",
                "kind": "query",
                "label": g["label"],
                "members": members,
                "member_count": len(members),
            }
        )
    return out


def axes(members, metas):
    """Keys that split these files into 2..n-1 groups, with their value counts."""
    n = len(members)
    keys = {"collection"}
    for m in metas:
        keys |= {k for k, v in m.items() if _scalar(v) and not k.startswith("_")}
        keys |= {
            "tag:" + t.split(":", 1)[0] for t in m["tags"] + m["user_tags"] if ":" in t
        }
    keys -= {"tags", "user_tags"}
    out = []
    for key in sorted(keys):
        count = Counter(
            v for (c, _), m in zip(members, metas) for v in values(m, c, key)
        )
        if 2 <= len(count) <= min(n - 1, MAX_VALUES) and all(
            len(v) <= MAX_VALUE_LEN for v in count
        ):
            top = [[v, c] for v, c in count.most_common(3)]
            every = [[v, c] for v, c in count.most_common(MAX_LISTED)]
            out.append({"key": key, "values": len(count), "top": top, "all": every})
    return sorted(out, key=lambda a: (a["values"], a["key"]))


def demo():
    members = [("c", "a" * 32), ("c", "b" * 32), ("d", "c" * 32), ("d", "e" * 32)]
    names = {f"{c}:{m}": m[:1] for c, m in members}
    metas = [
        normalize_tags({"tags": ["family:mirai", "family:gafgyt"], "arch": "x86"}),
        normalize_tags({"tags": ["family:mirai"], "arch": "x86"}),
        normalize_tags({"tags": ["family:gafgyt"], "arch": "arm"}),
        normalize_tags({"arch": "arm"}),
    ]
    g = meta_groups(members, metas, names, "collection")
    assert [x["member_count"] for x in g] == [2, 2] and g[0]["kind"] == "meta"
    g = meta_groups(members, metas, names, "tag:family")
    # mirai (2) beats gafgyt (2) on the tie by name; the tagless file is (none)
    assert {x["label"] for x in g} == {"gafgyt", "mirai", NONE_LABEL} or any(
        x["kind"] == "file" for x in g
    )
    g = meta_groups(members, metas, names, "tag:family", "all")
    assert sum(x["member_count"] for x in g) == 5
    assert {a["key"] for a in axes(members, metas)} >= {
        "collection",
        "arch",
        "tag:family",
    }
    g = meta_groups(members, metas, names, "arch", pick=["arm"])
    assert [x["label"] for x in g] == ["arm", "other"] and g[0]["member_count"] == 2
    g = meta_groups(members, metas, names, "arch", pick=["x86", "arm"], drop=True)
    assert [x["label"] for x in g] == ["x86", "arm"]
    try:
        meta_groups(members, metas, names, "arch", pick=["arm"], drop=True)
    except BadParams:
        pass
    else:
        raise AssertionError("one picked value must raise")
    try:
        meta_groups(members, metas, names, "nope")
    except BadParams:
        pass
    else:
        raise AssertionError("single group must raise")
    print("nway_column_service demo ok")


if __name__ == "__main__":
    demo()
