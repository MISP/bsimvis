"""Context-aware batch LLM tagging.

`llm_batch_service` judges every function from its own code alone -- no
caller, no callee, no idea whether it sits in a cluster of near-identical
code or is the one outlier. This module partitions a working set of
functions by call-graph locality and processes it bottom-up (callees before
callers), carrying forward a one-line summary of each already-processed
callee into its caller's prompt. A caller's prompt therefore says what its
neighbours turned out to be, without concatenating their code -- one LLM
call per function (or per mutually-recursive group), same cost as the blind
batch, but with the neighbourhood it was missing.

Mutually-recursive groups (true cycles -- A calls B calls A) have no valid
bottom-up order, so they are Tarjan-SCC'd into one combined LLM call instead:
one shared verdict for the group. A chain with no cycle (A calls B calls C)
does not need that: sequential bottom-up processing with summary injection
already gives each function its neighbours' context.

Reuses `llm_batch_service`'s already-enriched/undo bookkeeping rather than
reimplementing it -- two divergent copies of "what counts as an LLM tag, and
how overwrite removes it" is how a collection ends up with orphaned tags.
"""

import json
import logging

from bsimvis.app.services import tag_taxonomy
from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.llm_batch_service import (
    LLM_NOTE_OWNER,
    _already_enriched,
    _mark_enriched,
    _remove_llm_notes,
    _remove_llm_tags,
)
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.llm_tools import (
    get_file_info,
    get_function,
    get_function_relations,
)
from bsimvis.app.services.note_service import note_service
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_service import tag_service

DEFAULT_MAX_BATCH = 1000
SUMMARY_CONTEXT_CHARS = 220  # per-neighbour summary carried into a caller's prompt

# Agentic fallback: a unit judged from context alone can ask for a second,
# tool-using pass instead of guessing. Keeps the common case (most units) at
# one cheap call while still going deep on the ones that need it.
NEED_CONTEXT_MARKER = "NEED_MORE_CONTEXT"
MAX_AGENTIC_ITERATIONS = 4


def max_batch_size():
    return int(config_service.get("llm.batch_max", DEFAULT_MAX_BATCH))


# --- call-graph partitioning -------------------------------------------


def _tarjan_scc(node_ids, adj):
    """Iterative Tarjan SCC (no Python recursion-depth limit on a large call
    graph). Returns components in reverse-topological order: if A calls B,
    B's component comes out before A's -- exactly the bottom-up order this
    module needs, so callers never see a callee that has not run yet."""
    index_counter = [0]
    index, lowlink, on_stack = {}, {}, {}
    stack, result = [], []

    for start in node_ids:
        if start in index:
            continue
        work = [(start, iter(adj.get(start, ())))]
        index[start] = lowlink[start] = index_counter[0]
        index_counter[0] += 1
        stack.append(start)
        on_stack[start] = True

        while work:
            v, it = work[-1]
            descended = False
            for w in it:
                if w not in index:
                    index[w] = lowlink[w] = index_counter[0]
                    index_counter[0] += 1
                    stack.append(w)
                    on_stack[w] = True
                    work.append((w, iter(adj.get(w, ()))))
                    descended = True
                    break
                elif on_stack.get(w):
                    lowlink[v] = min(lowlink[v], index[w])
            if descended:
                continue
            work.pop()
            if work:
                parent = work[-1][0]
                lowlink[parent] = min(lowlink[parent], lowlink[v])
            if lowlink[v] == index[v]:
                comp = []
                while True:
                    w = stack.pop()
                    on_stack[w] = False
                    comp.append(w)
                    if w == v:
                        break
                result.append(comp)

    return result


def partition_call_graph(collection, func_ids):
    """Bottom-up processing order for `func_ids`: a list of units, each a
    list of one or more function ids (>1 only for a mutually-recursive
    group), earlier units containing no function called by a later unit's
    functions -- except within the same unit."""
    if len(func_ids) < 2:
        return [[f] for f in func_ids]

    relations = get_function_relations(func_ids, collection)
    if "error" in relations:
        logging.warning(
            f"analysis_orchestrator: relations lookup failed ({relations['error']}); "
            "falling back to one function per unit, no context propagation."
        )
        return [[f] for f in func_ids]

    adj = {}
    for edge in relations.get("call_edges", []):
        adj.setdefault(edge["from"], []).append(edge["to"])

    return _tarjan_scc(func_ids, adj)


# --- prompt context assembly --------------------------------------------


def _context_block(unit_func_ids, adj, summaries):
    """Rolling-summary context for one unit: what its already-processed
    direct callees turned out to be, if any. Summaries only, never code --
    this is what keeps a caller's prompt from growing with its subtree."""
    callee_lines = []
    seen = set()
    for fid in unit_func_ids:
        for callee in adj.get(fid, []):
            if callee in unit_func_ids or callee in seen:
                continue
            cap = summaries.get(callee)
            if not cap:
                continue
            seen.add(callee)
            callee_lines.append(f"- {cap['func_name']}: {cap['summary']}")

    if not callee_lines:
        return ""
    return (
        "Context: this function calls the following, already analysed "
        "elsewhere in this binary (use this to judge intent, e.g. a "
        "function whose only callee is a known network-send routine is "
        "itself part of a network path):\n" + "\n".join(callee_lines) + "\n"
    )


def _one_liner(summary):
    if not summary:
        return ""
    first = summary.strip().splitlines()[0]
    return first[:SUMMARY_CONTEXT_CHARS]


def _code_evidence(data):
    language = data.get("language_id") or "unknown"
    decompiler = data.get("decompiler_id") or "unknown"
    return (
        f"Target architecture (Ghidra language_id): {language}\n"
        f"Decompiler: {decompiler}\nCode:\n{data['code']}"
    )


# --- agentic fallback ----------------------------------------------------


def _needs_more_context(summary):
    return bool(summary) and summary.strip().strip("*# ").upper() == NEED_CONTEXT_MARKER


def _agentic_prompt(custom_prompt):
    base = custom_prompt or llm_service.default_prompt
    return (
        f"{base}\n\nIf this function's purpose is genuinely unclear from the code and "
        "the context given -- not just non-trivial, but actually ambiguous -- reply "
        f"with exactly '{NEED_CONTEXT_MARKER}' as your entire response and nothing else. "
        "A follow-up pass will then let you look up its call graph, BSim neighbours, and "
        "file/cluster metadata before judging it."
    )


def _agentic_summarize(collection, func_name, code_with_context, custom_prompt):
    """One-shot tool-using pass for a unit whose cheap context-only prompt came
    back NEED_MORE_CONTEXT. Same (summary, tags, err) contract as
    `llm_service.summarize_and_tag`, but the model may call `llm_tools` first --
    a standalone loop rather than `llm_chat_service`'s, since there is no chat
    session to persist here, just one verdict."""
    from ollama import Client

    from bsimvis.app.services.llm_tools import TOOLS, call_tool

    base_prompt = custom_prompt or llm_service.default_prompt
    tag_rule = tag_taxonomy.prompt_rules() + (
        "\nExample: TAGS: severity:medium, category:crypto:cipher."
    )
    system = (
        f"{base_prompt}\n\n{tag_rule}\n\n"
        f"Default collection for tool calls: '{collection}'. You have tools to look up "
        "this function's call graph, BSim similar functions, tags, and file/cluster "
        "metadata. Use whichever would change your judgment, then answer in the same "
        "TLDR/TAGS format -- do not reply with NEED_MORE_CONTEXT again."
    )
    messages = [
        {"role": "system", "content": system},
        {
            "role": "user",
            "content": f"Function Name: {func_name}\n\nCode:\n{code_with_context}",
        },
    ]

    client = Client(host=config_service.get("llm.ollama_url", "http://localhost:11434"))
    model = config_service.get("llm.model", "qwen3.6:35b")

    for _ in range(MAX_AGENTIC_ITERATIONS):
        try:
            response = client.chat(
                model=model,
                messages=messages,
                tools=TOOLS,
                stream=False,
                think=False,
                options={"num_predict": -1, "temperature": 0.2},
            )
        except Exception as e:
            return None, [], str(e)

        msg = response.get("message", {})
        content = msg.get("content", "") or ""
        raw_calls = [
            tc.model_dump() if hasattr(tc, "model_dump") else tc
            for tc in (msg.get("tool_calls") or [])
        ]
        if not raw_calls:
            summary, tags = llm_service._split_summary_tags(content)
            return summary, tags, None

        messages.append(
            {"role": "assistant", "content": content, "tool_calls": raw_calls}
        )
        for tc in raw_calls:
            fn = tc.get("function", {})
            name = fn.get("name")
            args = fn.get("arguments") or {}
            if isinstance(args, str):
                try:
                    args = json.loads(args)
                except Exception:
                    args = {}
            result = call_tool(name, args)
            messages.append({"role": "tool", "content": json.dumps(result)[:8000]})

    return None, [], "agentic fallback stopped after too many tool calls without a final answer"


# --- per-collection redis bookkeeping (shared shape with llm_batch_service) --


def _resolve_doc_id(func_id):
    return func_id.replace(":function:", ":func:") + (
        "" if func_id.endswith(":meta") else ":meta"
    )


class AnalysisOrchestrator:
    def __init__(self, r=None):
        self.r = r or get_redis()
        self._vocab_seen = set()

    def _result_key(self, job_id):
        return f"llm_contextual_batch:{job_id}:results"

    def _record(self, job_id, func_id, state, detail=None):
        self.r.hset(
            self._result_key(job_id),
            func_id,
            json.dumps({"state": state, "detail": detail}),
        )

    def get_results(self, job_id):
        raw = self.r.hgetall(self._result_key(job_id)) or {}
        out = {}
        for k, v in raw.items():
            k = k.decode() if isinstance(k, bytes) else k
            try:
                out[k] = json.loads(v)
            except Exception:
                out[k] = {"state": "unknown"}
        return out

    # --- one unit -------------------------------------------------------

    def _write_result(
        self, collection, func_id, func_name, summary, tags, actions, overwrite
    ):
        do_notes = "notes" in actions and summary
        do_tags = "tags" in actions and tags

        if do_notes:
            if overwrite:
                _remove_llm_notes(collection, func_id)
            note_service.add_note(collection, func_id, summary, owner=LLM_NOTE_OWNER)
            _mark_enriched(self.r, collection, func_id, "notes")

        applied = []
        if do_tags:
            if overwrite:
                _remove_llm_tags(collection, func_id)
            known = self._vocab_seen
            for t in tags:
                marked = tag_taxonomy.namespaced(t)
                if marked.lower() not in known:
                    tag_service.create_tag(collection, marked, llm=True)
                    known.add(marked.lower())
                if tag_service.add_user_tag(collection, "function", func_id, marked):
                    applied.append(marked)
            _mark_enriched(self.r, collection, func_id, "tags")

        return applied

    def _process_singleton(
        self,
        collection,
        func_id,
        adj,
        summaries,
        actions,
        overwrite,
        custom_prompt,
        agentic=False,
    ):
        data = get_function(func_id)
        if "error" in data:
            return "failed", data["error"]

        context = _context_block([func_id], adj, summaries)
        evidence = _code_evidence(data)
        code_with_context = f"{context}\n{evidence}" if context else evidence

        prompt = _agentic_prompt(custom_prompt) if agentic else custom_prompt
        summary, tags, err = llm_service.summarize_and_tag(
            data["func_name"], code_with_context, custom_prompt=prompt
        )
        if not err and agentic and _needs_more_context(summary):
            summary, tags, err = _agentic_summarize(
                collection, data["func_name"], code_with_context, custom_prompt
            )
        if err:
            return "failed", err
        if not summary and not tags:
            return "failed", "empty LLM response"

        summaries[func_id] = {"func_name": data["func_name"], "summary": _one_liner(summary)}
        applied = self._write_result(
            collection, func_id, data["func_name"], summary, tags, actions, overwrite
        )
        return "done", ", ".join(applied) if applied else None

    def _process_scc(
        self,
        collection,
        unit,
        adj,
        summaries,
        actions,
        overwrite,
        custom_prompt,
        agentic=False,
    ):
        """A mutually-recursive group: no valid bottom-up order inside it, so
        one combined LLM call sees every member's code and returns one shared
        verdict, applied to each member.

        ponytail: coarse -- a large SCC still gets one verdict for all
        members rather than per-function attribution. Upgrade to per-function
        blocks (ask the model to label each with a delimiter, parse per
        block) if a group this coarse turns out to matter in practice; most
        SCCs in compiled C/C++ are small (mutual helper pairs, dispatch
        loops), where one verdict for the group is the right grain anyway.
        """
        members = []
        for fid in unit:
            data = get_function(fid)
            if "error" not in data:
                members.append(data)
        if not members:
            return {fid: ("failed", "no member function resolved") for fid in unit}

        context = _context_block(unit, adj, summaries)
        combined_code = "\n\n".join(
            f"// --- {m['func_name']} ({m['func_id']}) ---\n{_code_evidence(m)}"
            for m in members
        )
        header = (
            f"{context}\nThe following {len(members)} functions call each other "
            "(mutual recursion / a dispatch cycle) and must be judged as one unit:\n"
        )

        combined_name = " + ".join(m["func_name"] for m in members)
        prompt = _agentic_prompt(custom_prompt) if agentic else custom_prompt
        summary, tags, err = llm_service.summarize_and_tag(
            combined_name,
            header + combined_code,
            custom_prompt=prompt,
        )
        if not err and agentic and _needs_more_context(summary):
            summary, tags, err = _agentic_summarize(
                collection, combined_name, header + combined_code, custom_prompt
            )
        if err or (not summary and not tags):
            detail = err or "empty LLM response"
            return {m["func_id"]: ("failed", detail) for m in members}

        out = {}
        for m in members:
            summaries[m["func_id"]] = {
                "func_name": m["func_name"],
                "summary": _one_liner(summary),
            }
            applied = self._write_result(
                collection, m["func_id"], m["func_name"], summary, tags, actions, overwrite
            )
            out[m["func_id"]] = ("done", ", ".join(applied) if applied else None)
        return out

    # --- run -------------------------------------------------------------

    def run_contextual_batch(
        self,
        collection,
        func_ids,
        actions=None,
        overwrite=False,
        custom_prompt=None,
        job_service=None,
        job_id=None,
        unit_max_size=None,
        agentic=False,
        summaries_out=None,
    ):
        actions = [a for a in (actions or []) if a in ("notes", "tags")] or ["notes", "tags"]
        total = len(func_ids)
        if not total:
            if job_service and job_id:
                job_service.add_log(job_id, "No functions matched the selection.")
            return False

        if "tags" in actions:
            self._vocab_seen = {
                t.lower() for t in tag_service.get_llm_vocabulary(collection) or ()
            }

        units = partition_call_graph(collection, func_ids)

        relations = (
            get_function_relations(func_ids, collection) if len(func_ids) > 1 else {"call_edges": []}
        )
        adj = {}
        for edge in relations.get("call_edges", []):
            adj.setdefault(edge["from"], []).append(edge["to"])

        if job_service and job_id:
            scc_count = sum(1 for u in units if len(u) > 1)
            job_service.add_log(
                job_id,
                f"Contextual LLM batch over {total} functions in {len(units)} units "
                f"({scc_count} mutually-recursive groups) | actions={','.join(actions)} "
                f"| overwrite={overwrite}",
            )

        summaries = summaries_out if summaries_out is not None else {}
        counters = {"done": 0, "skipped": 0, "failed": 0}
        processed = 0

        for unit in units:
            if job_service and job_id and job_service.is_cancelled(job_id):
                if job_service:
                    job_service.add_log(job_id, "Cancelled.")
                return False

            if not overwrite and all(
                _already_enriched(self.r, collection, fid, a)
                for fid in unit
                for a in actions
            ):
                for fid in unit:
                    self._record(job_id, fid, "skipped", "already enriched") if job_id else None
                counters["skipped"] += len(unit)
                processed += len(unit)
                continue

            try:
                if len(unit) == 1:
                    fid = unit[0]
                    state, detail = self._process_singleton(
                        collection,
                        fid,
                        adj,
                        summaries,
                        actions,
                        overwrite,
                        custom_prompt,
                        agentic,
                    )
                    results = {fid: (state, detail)}
                else:
                    results = self._process_scc(
                        collection,
                        unit,
                        adj,
                        summaries,
                        actions,
                        overwrite,
                        custom_prompt,
                        agentic,
                    )
            except Exception as e:
                logging.error(f"analysis_orchestrator: unit {unit} failed: {e}")
                results = {fid: ("failed", str(e)) for fid in unit}

            for fid, (state, detail) in results.items():
                counters[state] = counters.get(state, 0) + 1
                processed += 1
                if job_id:
                    self._record(job_id, fid, state, detail)

            if job_service and job_id:
                job_service.update_progress(
                    job_id,
                    int(processed * 100 / total),
                    f"{processed}/{total} | unit of {len(unit)} -> "
                    f"{', '.join(f'{f}:{s}' for f, (s, _) in results.items())}",
                )

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"Contextual LLM batch finished: {counters['done']} done, "
                f"{counters['skipped']} skipped, {counters['failed']} failed.",
            )
        return True

    # --- whole-file analysis ---------------------------------------------

    def run_file_analysis(
        self,
        collection,
        file_md5,
        func_ids,
        actions=None,
        overwrite=False,
        custom_prompt=None,
        job_service=None,
        job_id=None,
    ):
        """The context-aware batch (agentic this time, see `agentic=True`)
        over one file's functions, followed by one closing call that folds
        every function's summary into a whole-file report saved as a file
        note -- the deliverable a per-function pass alone can't produce."""
        summaries = {}
        ok = self.run_contextual_batch(
            collection,
            func_ids,
            actions=actions,
            overwrite=overwrite,
            custom_prompt=custom_prompt,
            job_service=job_service,
            job_id=job_id,
            agentic=True,
            summaries_out=summaries,
        )
        if ok and summaries:
            self._write_file_report(
                collection, file_md5, summaries, overwrite, job_service, job_id
            )
        return ok

    def _write_file_report(
        self, collection, file_md5, summaries, overwrite, job_service, job_id
    ):
        file_info = get_file_info(collection, file_md5)
        if "error" in file_info:
            if job_service and job_id:
                job_service.add_log(job_id, f"File report skipped: {file_info['error']}")
            return

        report = llm_service.chat(
            [{"role": "user", "content": _file_report_prompt(file_info, summaries)}]
        )
        if not report or report.startswith("Error:"):
            if job_service and job_id:
                job_service.add_log(job_id, f"File report generation failed: {report}")
            return

        file_id = f"{collection}:file:{file_md5}"
        if overwrite:
            for n in note_service.get_file_notes(collection, file_id) or []:
                if n.get("owner") == LLM_NOTE_OWNER:
                    note_service.remove_file_note(collection, file_id, n.get("id"))
        note_service.add_file_note(collection, file_id, report, owner=LLM_NOTE_OWNER)
        if job_service and job_id:
            job_service.add_log(job_id, "Whole-file report written as a file note.")


def _file_report_prompt(file_info, summaries):
    lines = [
        "You are a senior malware analyst. Synthesize a whole-file report from "
        "per-function findings already produced for this binary -- do not "
        "re-derive them, just interpret what they add up to.",
        "",
        f"File: {file_info.get('file_name')} (md5={file_info.get('file_md5')})",
    ]
    for key, label in [
        ("filetype", "Filetype"),
        ("avtype", "AV classification"),
        ("yara", "YARA matches"),
        ("cc_ip", "C2 IPs"),
    ]:
        val = file_info.get(key)
        if val:
            lines.append(f"{label}: {val}")
    lines.append(
        f"Functions analysed: {len(summaries)} of {file_info.get('function_count')}"
    )
    lines.append("")
    lines.append("Per-function findings:")
    for s in summaries.values():
        lines.append(f"- {s['func_name']}: {s['summary']}")
    lines.append("")
    lines.append("Write a structured whole-file report:")
    lines.append("**OVERVIEW**: [2-4 sentences: what this file is/does overall]")
    lines.append(
        "**CAPABILITIES**: [bullet list of concrete capabilities observed across functions]"
    )
    lines.append(
        "**NOTABLE FUNCTIONS**: [the few functions most worth an analyst's attention, and why]"
    )
    lines.append(
        "**ASSESSMENT**: [benign / suspicious / malicious -- and why, citing the evidence above]"
    )
    return "\n".join(lines)


analysis_orchestrator = AnalysisOrchestrator()


def _selfcheck():
    """Partitioning correctness, stubbed I/O -- Tarjan order and the
    singleton/SCC split, not the LLM or storage."""

    # A simple chain: no cycles, every unit a singleton, callees before callers.
    adj = {"a": ["b"], "b": ["c"], "c": []}
    units = _tarjan_scc(["a", "b", "c"], adj)
    flat = [u[0] for u in units]
    assert flat.index("c") < flat.index("b") < flat.index("a"), flat

    evidence = _code_evidence(
        {"language_id": "MIPS:LE:32:default", "decompiler_id": "ghidra", "code": "x"}
    )
    assert "MIPS:LE:32:default" in evidence and evidence.endswith("Code:\nx")

    # A cycle collapses into one unit.
    adj = {"a": ["b"], "b": ["a"]}
    units = _tarjan_scc(["a", "b"], adj)
    assert len(units) == 1 and set(units[0]) == {"a", "b"}, units

    # A cycle feeding a downstream singleton: cycle's unit still comes first.
    adj = {"a": ["b"], "b": ["a", "c"], "c": []}
    units = _tarjan_scc(["a", "b", "c"], adj)
    sizes = [len(u) for u in units]
    assert sizes[0] == 1 and set(units[0]) == {"c"}, units
    assert len(units[1]) == 2 and set(units[1]) == {"a", "b"}, units

    print("ok")


if __name__ == "__main__":
    _selfcheck()
