"""Quality/latency bench for the LLM summarise-and-tag pass.

Runs hand-labelled functions from the git-tracked `data/bench/` fixtures
through the real `llm_service.summarize_and_tag` and scores each verdict
against its label, alongside Ollama's token counts and wall time. Each case
also runs with its symbol replaced by `FUN_<addr>`: most real samples are
stripped, and a name like `UDPRAW` gives the answer away.

    uv run python scripts/llm_eval.py --model gemma4:e2b
    uv run python scripts/llm_eval.py --think --temperature 0.6 --only ddos
    uv run python scripts/llm_eval.py --save data/bench_results/llm_base.json
"""

import argparse
import ast
import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bsimvis.app.services.config_service import config_service  # noqa: E402
from bsimvis.app.services.llm_service import last_stats, llm_service  # noqa: E402

MIPS_BOT = "data/bench/168036e68ea46fd5dd2be5f70e248d9d.json"
COLDFIRE_BOT = "data/bench/0987f84e807a81c742e344c7cec0cb02.json"
X86_TOY = "data/bench/versioned_c/dbb7f163b18181227769d9769baf9407.json"

# severity: accepted levels ("unknown" = abstained). any_of: at least one of
# these categories must be tagged (empty = no requirement). forbid: category
# prefixes that are a wrong call for this function -- mostly the traps the
# analysis rules were written for.
CASES = [
    {
        "name": "ddos_udpraw",
        "fixture": MIPS_BOT,
        "index": 47,
        "severity": ["high"],
        "any_of": ["category:impact:ddos"],
        "forbid": ["category:network:scan", "category:network:c2"],
    },
    {
        "name": "ddos_dns",
        "fixture": MIPS_BOT,
        "index": 54,
        "severity": ["high"],
        "any_of": ["category:impact:ddos"],
        "forbid": ["category:network:scan"],
    },
    {
        "name": "ddos_http",
        "fixture": MIPS_BOT,
        "index": 41,
        "severity": ["high"],
        "any_of": ["category:impact:ddos"],
        "forbid": ["category:network:scan"],
    },
    {
        "name": "c2_connect",
        "fixture": MIPS_BOT,
        "index": 60,
        "severity": ["low", "medium", "high"],
        "any_of": ["category:network:c2", "category:network:socket"],
        "forbid": ["category:impact:", "category:network:scan"],
    },
    {
        "name": "killer_by_port",
        "fixture": MIPS_BOT,
        "index": 61,
        "severity": ["medium", "high"],
        "any_of": ["category:recon:proclist", "category:impact:", "category:evasion:"],
        "forbid": ["category:evasion:anti_vm", "category:network:c2"],
    },
    {
        "name": "killer_stripped",
        "fixture": COLDFIRE_BOT,
        "index": 26,
        "severity": ["medium", "high"],
        "any_of": ["category:recon:proclist", "category:impact:", "category:evasion:"],
        "forbid": ["category:evasion:anti_vm", "category:evasion:anti_debug"],
    },
    {
        "name": "mips_syscall_wrapper",
        "fixture": MIPS_BOT,
        "index": 249,
        "severity": ["none", "unknown"],
        "any_of": [],
        "forbid": ["category:evasion:", "category:persistence:", "category:impact:"],
    },
    {
        "name": "prng_cmwc",
        "fixture": MIPS_BOT,
        "index": 6,
        "severity": ["none", "low"],
        "any_of": ["category:crypto:random", "category:util:math"],
        "forbid": ["category:crypto:cipher", "category:evasion:"],
    },
    {
        "name": "local_ip_probe",
        "fixture": MIPS_BOT,
        "index": 9,
        "severity": ["none", "low"],
        "any_of": [
            "category:recon:sysinfo",
            "category:network:socket",
            "category:recon:env",
        ],
        "forbid": ["category:network:scan", "category:network:c2", "category:impact:"],
    },
    {
        "name": "benign_bubble_sort",
        "fixture": X86_TOY,
        "index": 5,
        "severity": ["none", "unknown"],
        "any_of": [],
        "forbid": [
            "category:network:",
            "category:evasion:",
            "category:impact:",
            "category:crypto:",
        ],
    },
    {
        "name": "benign_is_prime",
        "fixture": X86_TOY,
        "index": 19,
        "severity": ["none", "unknown"],
        "any_of": [],
        "forbid": [
            "category:network:",
            "category:evasion:",
            "category:impact:",
            "category:crypto:",
        ],
    },
]

_fixtures = {}


def load_function(path, index):
    if path not in _fixtures:
        with open(path) as f:
            _fixtures[path] = json.load(f)["functions"]
    fn = _fixtures[path][index]
    src, meta = fn["function_source"], fn["function_metadata"]
    src = ast.literal_eval(src) if isinstance(src, str) else src
    meta = ast.literal_eval(meta) if isinstance(meta, str) else meta
    return meta.get("function_name", "unknown"), "\n".join(src.get("c_lines") or [])


def strip_name(name, code, index):
    if name.startswith("FUN_"):
        return name, code
    anon = f"FUN_{index:08x}"
    return anon, re.sub(rf"\b{re.escape(name)}\b", anon, code)


def score(case, summary, tags, error):
    severity = next(
        (t.split(":", 1)[1] for t in tags if t.startswith("severity:")), "unknown"
    )
    cats = [t for t in tags if t.startswith("category:")]
    any_of = case["any_of"]
    out = {
        "parsed": error is None and bool(summary),
        "severity": severity,
        "severity_ok": severity in case["severity"],
        "category_ok": not any_of or any(c.startswith(p) for c in cats for p in any_of),
        "forbidden": [c for c in cats if any(c.startswith(p) for p in case["forbid"])],
    }
    # A failed call is never a pass, even where abstaining would be.
    out["ok"] = (
        out["parsed"]
        and out["severity_ok"]
        and out["category_ok"]
        and not out["forbidden"]
    )
    return out


def apply_overrides(args):
    llm = config_service._config.setdefault("llm", {})
    if args.model:
        llm["model"] = args.model
    if args.num_ctx:
        llm["num_ctx"] = args.num_ctx
    task = llm.setdefault("tasks", {}).setdefault("tag", {})
    if args.think is not None:
        task["think"] = args.think
    if args.temperature is not None:
        task["temperature"] = args.temperature
    if args.think_budget is not None:
        task["think_budget"] = args.think_budget


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--model")
    p.add_argument("--num-ctx", type=int)
    p.add_argument("--think", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument("--temperature", type=float)
    p.add_argument("--think-budget", type=int)
    p.add_argument("--only", help="run only cases whose name contains this")
    p.add_argument(
        "--names",
        choices=["both", "real", "stripped"],
        default="both",
        help="run with the real symbol, with FUN_<addr>, or both",
    )
    p.add_argument("--repeat", type=int, default=1)
    p.add_argument("--save", help="write per-run results as JSON")
    p.add_argument("-v", "--verbose", action="store_true", help="print summaries")
    args = p.parse_args()
    apply_overrides(args)
    llm_service._load_config()

    variants = {"both": (False, True), "real": (False,), "stripped": (True,)}[
        args.names
    ]
    cases = [c for c in CASES if not args.only or args.only in c["name"]]
    print(f"model={llm_service.model} tag={llm_service.task_options('tag')}")

    runs = []
    for case in cases:
        name, code = load_function(case["fixture"], case["index"])
        for stripped in variants:
            fname, fcode = (
                strip_name(name, code, case["index"]) if stripped else (name, code)
            )
            for _ in range(args.repeat):
                summary, tags, error = llm_service.summarize_and_tag(fname, fcode)
                stats = last_stats() or {}
                result = {
                    "case": case["name"],
                    "stripped": stripped,
                    **score(case, summary, tags, error),
                    "tags": tags,
                    "summary": summary,
                    "error": error,
                    **{
                        k: stats.get(k)
                        for k in (
                            "prompt_tokens",
                            "output_tokens",
                            "wall_s",
                            "done_reason",
                        )
                    },
                }
                runs.append(result)
                ok = result["ok"]
                print(
                    f"{'PASS' if ok else 'FAIL'} {case['name']:22} "
                    f"{'stripped' if stripped else 'named   '} "
                    f"sev={result['severity']:7} tags={','.join(t for t in tags if t.startswith('category:')) or '-'} "
                    f"in={result['prompt_tokens']} out={result['output_tokens']} {result['wall_s']}s"
                    + (f" ERR={error}" if error else "")
                    + (
                        f" forbidden={result['forbidden']}"
                        if result["forbidden"]
                        else ""
                    )
                )
                if args.verbose:
                    print(f"      {summary}")

    n = len(runs)
    if not n:
        return
    passed = sum(r["ok"] for r in runs)
    walls = [r["wall_s"] for r in runs if r["wall_s"] is not None]
    print(
        f"\n{passed}/{n} pass | parsed {sum(r['parsed'] for r in runs)}/{n} | "
        f"severity {sum(r['severity_ok'] for r in runs)}/{n} | "
        f"category {sum(r['category_ok'] for r in runs)}/{n} | "
        f"forbidden {sum(bool(r['forbidden']) for r in runs)} | "
        f"median {statistics.median(walls) if walls else '-'}s total {round(sum(walls), 1)}s"
    )
    if args.save:
        os.makedirs(os.path.dirname(args.save) or ".", exist_ok=True)
        with open(args.save, "w") as f:
            json.dump(
                {
                    "model": llm_service.model,
                    "tag": llm_service.task_options("tag"),
                    "runs": runs,
                },
                f,
                indent=2,
            )


if __name__ == "__main__":
    main()
