"""Live YARA workbench endpoints. Logic lives in services/yara_edit_service.py;
file sets reuse the N-way column-set resolver, so a set here is whatever a
Compare group can be: searches (tags, batches, clusters, ...) or explicit files."""

import json
import re
import uuid as uuidlib

from flask import request
from flask_restx import abort

from bsimvis.app.routes.nway import resolve_group
from bsimvis.app.services import yara_edit_service as ys
from bsimvis.app.services.bin_sim_service import _origin_coll
from bsimvis.app.services.job_service import JobService, JobType
from bsimvis.app.services.redis_client import (
    get_queue_redis,
    get_raw_redis,
    get_redis,
)

job_service = JobService()
_MD5 = re.compile(r"^[0-9a-f]{32}$")


def _body():
    return request.get_json(silent=True) or {}


def _scope_collections(scope):
    if scope.get("pool"):
        from bsimvis.app.services.pool_service import pool_service

        pool = pool_service.get_pool(scope["pool"])
        if not pool:
            abort(404, "Pool not found")
        return pool.get("collections", [])
    return [scope["collection"]]


def _explicit(files, label):
    out = []
    for t in files:
        coll, _, md5 = str(t).rpartition(":")
        if not coll or not _MD5.match(md5.lower()):
            abort(400, f"{label}: want <collection>:<md5>, got {t!r}")
        out.append((_origin_coll(coll), md5.lower()))
    return out


def _rest(r, scope, cap):
    out = []
    for c in _scope_collections(scope):
        for fid in r.sscan_iter(f"{c}:all_files", count=1000):
            fid = ys._txt(fid)
            out.append((_origin_coll(c), fid.rsplit(":", 1)[-1]))
            if len(out) >= cap:
                return out
    return out


def resolve_sets(r, body, key, cap):
    """`body[key]`: list of `{label, queries | files | rest}` -> `[{label, members}]`."""
    scope = {k: body[k] for k in ("collection", "pool") if body.get(k)}
    if len(scope) != 1:
        abort(400, "give exactly one of collection / pool")
    specs = body.get(key) or []
    if not isinstance(specs, list) or not all(isinstance(g, dict) for g in specs):
        abort(400, f"{key}: a list of {{label, queries | files | rest}}")
    meta, out = {}, []
    for i, spec in enumerate(specs):
        label = str(spec.get("label") or f"{key} {i + 1}")[:60]
        if spec.get("rest"):
            found = _rest(r, scope, cap)
        elif spec.get("files"):
            found = _explicit(spec["files"], label)
        else:
            found = resolve_group(scope, spec, label, cap, meta)
        out.append({"label": label, "members": list(dict.fromkeys(found))})
    return ys.apply_pins(out, _pins(body), key[:-1])


def _pins(body):
    """`body.pins`: {"coll:md5": "target" | "negative" | "drop"} -> {(coll, md5): role}."""
    pins = body.get("pins") or {}
    if not isinstance(pins, dict) or len(pins) > 1000:
        abort(400, "pins: at most 1000 {<collection>:<md5>: role}")
    if any(v not in ("target", "negative", "drop") for v in pins.values()):
        abort(400, "pins: role is target, negative or drop")
    return dict(zip(_explicit(list(pins), "pins"), pins.values()))


def _check_rule(body):
    text = body.get("rule")
    if not isinstance(text, str) or not text.strip():
        abort(400, "rule: YARA source text required")
    if len(text) > 200_000:
        abort(413, "rule too large")
    return text


def post_validate():
    """Compile only: `{ok, errors: [{line, message}], rules}`."""
    text = _check_rule(_body())
    rules, errors = ys.compile_source(text)
    return {"ok": rules is not None, "errors": errors, "rules": ys.rule_names(text)}


def post_test():
    """Compile the rule, scan the target set and a capped sample of the negative
    set, and report precision / recall, per-group and per-string hit counts, and
    where the hits land."""
    body = _body()
    text = _check_rule(body)
    r = get_redis()
    targets = resolve_sets(r, body, "targets", ys.cfg("target_cap_files", 2000))
    if not any(g["members"] for g in targets):
        abort(400, "targets: the sets match no file")
    negatives = resolve_sets(r, body, "negatives", ys.cfg("neg_pool_cap", 20000))
    return ys.evaluate(r, get_raw_redis(), text, targets, negatives)


def get_seed_function():
    """A function's raw bytes as a YARA hex string."""
    a = request.args
    if not (a.get("collection") and _MD5.match(a.get("md5", "")) and a.get("addr")):
        abort(400, "collection, md5 and addr required")
    try:
        int(a["addr"], 16)
    except ValueError:
        abort(400, "addr must be hex")
    res = ys.function_hex(
        get_redis(),
        get_raw_redis(),
        _origin_coll(a["collection"]),
        a["md5"],
        a["addr"],
    )
    if res.get("error"):
        abort(404, res["error"])
    return res


def post_suggest():
    """Strings that most of the target files hold and the negatives do not."""
    body = _body()
    r = get_redis()
    targets = resolve_sets(r, body, "targets", 200)
    negatives = resolve_sets(r, body, "negatives", ys.cfg("neg_pool_cap", 20000))
    neg = list(dict.fromkeys(m for g in negatives for m in g["members"]))
    min_frac = max(0.0, min(1.0, float(body.get("min_frac", 0))))
    return {
        "strings": ys.suggest(r, targets, neg, min_frac),
        "consts": ys.suggest_consts(r, get_raw_redis(), targets, neg),
    }


def post_lint():
    """Style and robustness warnings for the rule source."""
    return {"warnings": ys.lint(_check_rule(_body()))}


def _sweep_hits(body):
    """A finished sweep's rule and hit files, for views that need no Must match set."""
    run_id = str(body.get("run_id", ""))
    if not re.match(r"^[0-9a-f]{12}$", run_id):
        abort(400, "bad run id")
    q = get_queue_redis()
    spec = q.get(f"yara:sweep:{run_id}:spec")
    if not spec:
        abort(404, "unknown or expired run")
    hits = sorted(ys._txt(h) for h in q.smembers(f"yara:sweep:{run_id}:hits"))
    members = []
    for fid in hits:
        coll, _, md5 = fid.rpartition(":file:")
        members.append((_origin_coll(coll), md5))
    return json.loads(spec).get("rule") or "", members


def post_hitmap():
    """Function clusters the rule's hits land in (family marker vs shared capability)."""
    body = _body()
    if body.get("run_id"):
        text, members = _sweep_hits(body)
        body = {**body, "rule": text}
        targets, negatives = [{"label": "Hit files", "members": members}], []
    else:
        text = _check_rule(body)
    if ys.compile_source(text)[0] is None:
        abort(400, "rule does not compile")
    r = get_redis()
    if not body.get("run_id"):
        targets = resolve_sets(r, body, "targets", ys.cfg("target_cap_files", 2000))
        negatives = resolve_sets(r, body, "negatives", ys.cfg("neg_pool_cap", 20000))
    cap = max(1, min(300, int(body.get("cap", 60))))
    return ys.hitmap(
        r,
        get_raw_redis(),
        text,
        targets,
        negatives,
        cap,
        float(body.get("min_cohesion", 0.5)),
    )


def post_scope():
    """File clusters to focus on: purity, what the rule does inside, greedy covers."""
    body = _body()
    if body.get("run_id"):
        return ys.scope_hits(
            get_redis(),
            _sweep_hits(body)[1],
            float(body.get("min_cohesion", 0.5)),
            max(0.0, min(1.0, float(body.get("min_purity", 0.8)))),
        )
    text = _check_rule(body)
    if ys.compile_source(text)[0] is None:
        abort(400, "rule does not compile")
    r = get_redis()
    targets = resolve_sets(r, body, "targets", ys.cfg("target_cap_files", 2000))
    negatives = resolve_sets(r, body, "negatives", ys.cfg("neg_pool_cap", 20000))
    return ys.scope(
        r,
        get_raw_redis(),
        text,
        targets,
        negatives,
        float(body.get("min_cohesion", 0.5)),
        max(0.0, min(1.0, float(body.get("min_purity", 0.8)))),
    )


def post_seed_cluster():
    """Byte-pattern seeds from a function cluster (`cluster_id` + `collection`) or
    any list of same-kind function ids (`fids`, e.g. an N-way row or lane)."""
    body = _body()
    r = get_redis()
    fids = body.get("fids")
    if not fids:
        coll, cid = body.get("collection"), body.get("cluster_id")
        if not coll or cid is None:
            abort(400, "give fids, or collection + cluster_id")
        from bsimvis.app.services.collection_config import resolve_collection_algo

        algo = resolve_collection_algo(coll)
        fids = [ys._txt(f) for f in r.smembers(f"{coll}:cluster:{algo}:{cid}:members")]
    if not fids or len(fids) > 20000:
        abort(400, "fids: 1 to 20000 function ids")
    bad = [f for f in fids if f.count(":") != 3]
    if bad:
        abort(400, f"fids: want <collection>:func:<md5>:<addr>, got {bad[0]!r}")
    return ys.seed_functions(
        r, get_raw_redis(), fids, max(2, min(64, int(body.get("per_arch", 16))))
    )


def post_draft():
    """LLM draft of a rule from the ranked suggestions."""
    from bsimvis.app.services.llm_service import LLMService

    body = _body()
    r = get_redis()
    targets = resolve_sets(r, body, "targets", 200)
    negatives = resolve_sets(r, body, "negatives", ys.cfg("neg_pool_cap", 20000))
    neg = list(dict.fromkeys(m for g in negatives for m in g["members"]))
    picks = ys.suggest(r, targets, neg, 0.8, limit=12)
    if not picks:
        abort(404, "no string is shared by the target files")
    return {"rule": LLMService().draft_yara(str(body.get("name") or "family"), picks)}


# --- registry ---------------------------------------------------------------


def list_rules():
    return {"rules": ys.list_rules(get_redis())}


def save_rule():
    body = _body()
    text = _check_rule(body)
    u = body.get("uuid")
    if u is not None:
        try:
            u = str(uuidlib.UUID(str(u)))
        except ValueError:
            abort(400, "uuid must be a UUID")
    doc = ys.save_rule(
        get_redis(), text, u, body.get("scope"), str(body.get("note") or "")[:200]
    )
    if doc.get("error"):
        abort(400, doc["error"])
    return doc


def get_rule(rule_uuid):
    r = get_redis()
    doc = ys.get_rule(r, rule_uuid, request.args.get("version", type=int))
    if not doc:
        abort(404, "unknown rule or version")
    return {**doc, "versions": ys.rule_versions(r, rule_uuid)}


def delete_rule(rule_uuid):
    if not ys.delete_rule(get_redis(), rule_uuid):
        abort(404, "unknown rule")
    return {"status": "deleted"}


def export_rule(rule_uuid):
    """The rule as a `.yar` body, uuid and version in a header comment."""
    doc = ys.get_rule(get_redis(), rule_uuid)
    if not doc:
        abort(404, "unknown rule")
    return {
        "filename": f"{doc['name']}.yar",
        "content": f"// bsimvis rule {doc['uuid']} v{doc['version']}\n{doc['text']}\n",
    }


def export_to_vendored(rule_uuid):
    """Write the rule into `data/yara_rules/custom/` for a human to commit."""
    doc = ys.get_rule(get_redis(), rule_uuid)
    if not doc:
        abort(404, "unknown rule")
    from bsimvis.app.services.yara_service import rules_dir

    folder = rules_dir() / "custom"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{doc['uuid']}.yar"
    path.write_text(f"// bsimvis rule {doc['uuid']} v{doc['version']}\n{doc['text']}\n")
    return {"path": str(path)}


# --- full sweep / apply (jobs) ----------------------------------------------


def _start_run(mode):
    body = _body()
    text = _check_rule(body)
    if ys.compile_source(text)[0] is None:
        abort(400, "rule does not compile")
    rule_uuid = body.get("uuid") if mode == "apply" else None
    if mode == "apply" and not ys.get_rule(get_redis(), str(rule_uuid)):
        abort(404, "apply needs a saved rule: save it first")
    scope = {k: body[k] for k in ("collection", "pool") if body.get(k)}
    if len(scope) != 1:
        abort(400, "give exactly one of collection / pool")
    colls = _scope_collections(scope)
    run_id = uuidlib.uuid4().hex[:12]
    q = get_queue_redis()
    q.setex(
        f"yara:sweep:{run_id}:spec",
        6 * 3600,
        json.dumps(
            {
                k: body.get(k)
                for k in ("collection", "pool", "targets", "negatives", "pins")
            }
            | {"rule": text}
        ),
    )
    total = sum(get_redis().scard(f"{c}:all_files") for c in colls)
    q.hset(
        f"yara:sweep:{run_id}:stat",
        mapping={"collections": len(colls), "mode": mode, "total": total},
    )
    q.expire(f"yara:sweep:{run_id}:stat", 6 * 3600)
    jobs = [
        job_service.create_job(
            JobType.YARA_RULE_RUN,
            {
                "collection": c,
                "rule": text,
                "mode": mode,
                "run_id": run_id,
                "rule_uuid": rule_uuid,
            },
        )
        for c in colls
    ]
    return {"run_id": run_id, "job_ids": jobs}


def post_sweep():
    """Scan every file of the collection / pool with the rule (exact numbers)."""
    return _start_run("sweep")


def post_apply():
    """Tag every matching file `yara:custom:<rule>` and clear stale tags of this rule."""
    return _start_run("apply")


def get_run(run_id):
    """Progress of a sweep / apply, with exact precision / recall against its targets."""
    if not re.match(r"^[0-9a-f]{12}$", run_id):
        abort(400, "bad run id")
    q = get_queue_redis()
    stat = {
        ys._txt(k): ys._txt(v)
        for k, v in q.hgetall(f"yara:sweep:{run_id}:stat").items()
    }
    if not stat:
        abort(404, "unknown or expired run")
    spec = json.loads(q.get(f"yara:sweep:{run_id}:spec") or "{}")
    hits = {ys._txt(h) for h in q.smembers(f"yara:sweep:{run_id}:hits")}
    # a dropped file is out of the measure: not a hit, not a negative
    drop = {f"{c}:file:{m}" for (c, m), v in _pins(spec).items() if v == "drop"}
    hits -= drop
    out = {
        "mode": stat.get("mode"),
        "done": int(stat.get("done", 0)) >= int(stat.get("collections", 1)),
        "seen": int(stat.get("seen", 0)),
        "hits": len(hits),
        "tagged": int(stat.get("changed", 0)),
    }
    out["total"] = int(stat.get("total", 0))
    if not out["done"]:
        return out

    def rows(fids):
        return [
            ys.file_label(get_redis(), (c, m))
            for c, _, m in (f.rpartition(":file:") for f in fids)
        ]

    cap = ys.LIST_CAP
    r = get_redis()
    text = spec.get("rule") or ""
    rules, _ = ys.compile_source(text)
    out["rules"] = ys.rule_names(text)
    targets = (
        resolve_sets(r, spec, "targets", ys.cfg("neg_pool_cap", 20000))
        if spec.get("targets")
        else []
    )

    def fids(group):
        return {f"{_origin_coll(c)}:file:{m}" for c, m in group["members"]}

    tset = set().union(*(fids(g) for g in targets)) if targets else set()

    # what each string hit, split into target / other files
    per = {(rule, sid): [0, 0] for rule, sid in ys.defined_strings(text)}
    raw_r = get_raw_redis()
    strs_key = f"yara:sweep:{run_id}:strs"
    if hits and rules is not None and not q.exists(strs_key):
        # a worker older than the per-string counts: tally a sample of the hit files instead
        sample = sorted(hits)[: ys.cfg("sweep_string_sample", 300)]
        out["strings_sampled"] = len(sample) < len(hits)
        found_by = {}
        for fid in sample:
            data = raw_r.get(f"{fid}:raw")
            if data:
                found_by[fid] = "|".join(
                    f"{rule}:{sid}"
                    for rule, ss in ys.scan_bytes(rules, data).items()
                    for sid in ss
                )
    else:
        found_by = ((ys._txt(f), ys._txt(v)) for f, v in q.hscan_iter(strs_key))
        found_by = dict(found_by)
    for fid, found in found_by.items():
        side = 1 if tset and fid not in tset else 0
        for key in filter(None, found.split("|")):
            rule, _, sid = key.rpartition(":")
            per.setdefault((rule, sid), [0, 0])[side] += 1
    others = max(0, out["seen"] - len(tset) - len(drop))
    out["strings"] = [
        {
            "rule": k[0],
            "id": k[1],
            "pos": v[0],
            "neg": v[1],
            "score": (ys.string_score(v[0], len(tset), v[1], others) if tset else None),
        }
        for k, v in per.items()
    ]
    out["sample"] = {"targets": len(tset) or out["seen"]}

    groups = [
        {
            "label": g["label"],
            "role": "target",
            "files": len(fids(g)),
            "hits": len(fids(g) & hits),
            "hit_rate": ys._ratio(len(fids(g) & hits), len(fids(g))),
        }
        for g in targets
    ]
    negs = spec.get("negatives") or []
    explicit = [g for g in negs if isinstance(g, dict) and not g.get("rest")]
    if explicit:
        for g in resolve_sets(
            r, {**spec, "negatives": explicit}, "negatives", cap * 400
        ):
            n = fids(g) - tset
            groups.append(
                {
                    "label": g["label"],
                    "role": "negative",
                    "files": len(n),
                    "hits": len(n & hits),
                    "hit_rate": ys._ratio(len(n & hits), len(n)),
                }
            )
    if any(isinstance(g, dict) and g.get("rest") for g in negs):
        rest = out["seen"] - len(tset)
        groups.append(
            {
                "label": "Everything else",
                "role": "negative",
                "files": rest,
                "hits": len(hits - tset),
                "hit_rate": ys._ratio(len(hits - tset), rest),
            }
        )
    groups.append(
        {
            "label": "Whole collection",
            "role": "scanned",
            "files": out["seen"],
            "hits": len(hits),
            "hit_rate": ys._ratio(len(hits), out["seen"]),
        }
    )
    out["groups"] = groups

    landed = sorted(hits & tset if tset else hits)[: ys.cfg("land_files", 20)]
    found_in = []
    for fid in landed:
        c, _, m = fid.rpartition(":file:")
        data = raw_r.get(f"{fid}:raw")
        if data and rules is not None:
            found_in.append(((c, m), ys.scan_bytes(rules, data)))
    out["landing"] = ys.locate_hits(r, raw_r, found_in)

    if not tset:
        out["hit_files"] = rows(sorted(hits)[:cap])
        return out
    tp = len(hits & tset)
    fp, fn = len(hits) - tp, len(tset) - tp
    precision, recall = ys._ratio(tp, len(hits)), ys._ratio(tp, len(tset))
    out["metrics"] = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": max(0, out["seen"] - len(tset) - len(drop) - fp),
        "precision": precision,
        "recall": recall,
        "f1": (
            round(2 * precision * recall / (precision + recall), 4)
            if precision and recall
            else None if precision is None or recall is None else 0.0
        ),
    }
    out["missed"] = rows(sorted(tset - hits)[:cap])
    out["false_hits"] = rows(sorted(hits - tset)[:cap])
    out["true_hits"] = rows(sorted(hits & tset)[:cap])
    if rules is not None:
        for key in ("false_hits", "true_hits"):
            detail = ys.explain_hits(
                r,
                raw_r,
                rules,
                [tuple(f["id"].rsplit(":", 1)) for f in out[key]],
            )
            for f in out[key]:
                f["matches"] = detail.get(f["id"], [])
    return out
