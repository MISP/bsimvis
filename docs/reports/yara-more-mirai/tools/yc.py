"""Tiny client for the BSimVis YARA workbench API (stdlib only).

    import sys; sys.path.insert(0, ".../tools"); import yc
    fam = yc.groups()                       # families.json groups by id
    t = yc.files(fam, "boatnet")            # -> {"label","files":[coll:md5,...]}
    n = yc.rest(fam, exclude=["boatnet"])   # every other classified file
    yc.test(rule_text, [t], [n])            # precision/recall report
"""

import json
import os
import urllib.request

BASE = os.environ.get("BSIM_API", "http://localhost:5602/api")
COLL = "more_mirai_yara"
HERE = os.path.dirname(os.path.abspath(__file__))
FAMILIES = os.path.join(HERE, "..", "families.json")


def call(path, body=None, method=None, timeout=600):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method or ("POST" if body is not None else "GET"),
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        return {"http_error": e.code, "body": e.read().decode()[:2000]}


def groups():
    """families.json groups keyed by id."""
    return {g["id"]: g for g in json.load(open(FAMILIES))["groups"]}


def files(fam, *ids, label=None):
    """A target/negative set spec from one or more group ids."""
    md5s = [m for i in ids for m in fam[i]["files"]]
    return {"label": label or "+".join(ids), "files": [f"{COLL}:{m}" for m in md5s]}


def rest(fam, exclude=(), label="rest"):
    """All classified files outside `exclude` group ids (and the unclassified)."""
    md5s = [m for i, g in fam.items() if i not in exclude for m in g["files"]]
    return {"label": label, "files": [f"{COLL}:{m}" for m in md5s]}


def test(rule, targets, negatives, **kw):
    return call(
        "/yara/test",
        {"collection": COLL, "rule": rule, "targets": targets, "negatives": negatives, **kw},
    )


def suggest(targets, negatives, min_frac=0.6):
    return call(
        "/yara/suggest",
        {"collection": COLL, "targets": targets, "negatives": negatives, "min_frac": min_frac},
    )


def hitmap(rule, targets, negatives, cap=60):
    return call(
        "/yara/hitmap",
        {"collection": COLL, "rule": rule, "targets": targets, "negatives": negatives, "cap": cap},
    )


def seed_cluster(cluster_id=None, fids=None, per_arch=16):
    body = {"collection": COLL, "per_arch": per_arch}
    body.update({"cluster_id": cluster_id} if fids is None else {"fids": fids})
    return call("/yara/seed/cluster", body)


def save(rule, note="", uuid=None):
    return call("/yara/rules", {"rule": rule, "note": note, **({"uuid": uuid} if uuid else {})})


def summary(rep):
    """One-line view of a /yara/test report."""
    if not rep.get("ok"):
        return rep
    m = rep["metrics"]
    return (
        f"tp={m['tp']} fn={m['fn']} fp={m['fp']} tn={m['tn']} "
        f"P={m['precision']} R={m['recall']} F1={m['f1']}"
    )
