"""Batch LLM enrichment: runs the LLM over a set of functions and persists the
output as notes and/or tags.

One combined LLM call per function returns summary + tags (see
`llm_service.summarize_and_tag`). Machine-generated output is marked so it can
be filtered and undone without touching human work:

- notes are stored with ``owner="llm"``, which the existing `note_owners`
  index already makes searchable;
- tags are applied in the ``flag:`` namespace, which the binary-similarity
  split routes to its flags axis: an LLM finding shows up next to the score
  without competing with Function ID for what counts as original code.

Undo targets the LLM tag *vocabulary* rather than the ``flag:`` prefix, because
that prefix is shared with tags a human raised by hand and a rerun must not
delete those. Free-form output is registered in the vocabulary as it is written,
so it stays undoable too.
"""

import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.note_service import note_service
from bsimvis.app.services.tag_service import tag_service
from bsimvis.app.services.redis_client import get_redis

LLM_NOTE_OWNER = "llm"
LLM_TAG_PREFIX = "flag:"
# Tags written before the `flag:` namespace existed; still recognised so an old
# run can be detected and cleaned up.
LEGACY_LLM_TAG_PREFIX = "llm:"

DEFAULT_MAX_BATCH = 1000
DEFAULT_CONCURRENCY = 2


def max_batch_size():
    return int(config_service.get("llm.batch_max", DEFAULT_MAX_BATCH))


def concurrency():
    # ponytail: one global setting. Make it per-provider when a second LLM
    # backend than Ollama exists.
    return max(1, int(config_service.get("llm.batch_concurrency", DEFAULT_CONCURRENCY)))


def _enriched_key(collection, action):
    return f"{collection}:llm_enriched:{action}"


def _mark_enriched(r, collection, func_id, action):
    r.sadd(_enriched_key(collection, action), func_id)


def _already_enriched(r, collection, func_id, action):
    """True if `action` already ran for this function.

    A run that legitimately produces no tags leaves nothing on the document,
    so presence of output alone would make every rerun redo those functions.
    The marker set records that the pass ran; the document check is the
    fallback for functions enriched before the marker existed.
    """
    if r.sismember(_enriched_key(collection, action), func_id):
        return True
    return (
        _has_llm_note(collection, func_id)
        if action == "notes"
        else _has_llm_tags(collection, func_id)
    )


def _has_llm_note(collection, func_id):
    return any(
        n.get("owner") == LLM_NOTE_OWNER
        for n in note_service.get_notes(collection, func_id) or []
    )


def _machine_tags(collection, func_id):
    """The tags on this function that an LLM run put there.

    Anything in the collection's LLM vocabulary, plus anything left by a run
    that predates the `flag:` namespace. A `flag:` tag a human added by hand is
    not in the vocabulary and is left alone.
    """
    vocab = {t.lower() for t in tag_service.get_llm_vocabulary(collection)}
    doc_id = tag_service._resolve_doc_id(collection, "function", func_id)
    doc = tag_service._get_doc(doc_id) or {}
    return [
        t
        for t in doc.get("user_tags") or []
        if isinstance(t, str)
        and (t.lower() in vocab or t.startswith(LEGACY_LLM_TAG_PREFIX))
    ]


def _has_llm_tags(collection, func_id):
    return bool(_machine_tags(collection, func_id))


def _remove_llm_tags(collection, func_id):
    for t in _machine_tags(collection, func_id):
        tag_service.remove_user_tag(collection, "function", func_id, t)


def _remove_llm_notes(collection, func_id):
    for n in note_service.get_notes(collection, func_id) or []:
        if n.get("owner") == LLM_NOTE_OWNER:
            note_service.remove_note(collection, func_id, n.get("id"))


class LLMBatchService:
    def __init__(self, r=None):
        self.r = r or get_redis()
        # Tag ids already known to be in the LLM vocabulary, for the run in
        # flight. Free-form output is registered once per run instead of once
        # per function, which is a redis write per function otherwise.
        self._vocab_seen = set()

    # --- per-function result bookkeeping -------------------------------

    def _result_key(self, job_id):
        return f"llm_batch:{job_id}:results"

    def _record(self, job_id, func_id, state, detail=None):
        self.r.hset(
            self._result_key(job_id),
            func_id,
            json.dumps({"state": state, "detail": detail}),
        )

    def get_results(self, job_id):
        """Per-function state map for a batch job."""
        raw = self.r.hgetall(self._result_key(job_id)) or {}
        out = {}
        for k, v in raw.items():
            k = k.decode() if isinstance(k, bytes) else k
            try:
                out[k] = json.loads(v)
            except Exception:
                out[k] = {"state": "unknown"}
        return out

    # --- execution -----------------------------------------------------

    def _process_one(
        self, collection, func_id, actions, overwrite, custom_prompt, vocabulary
    ):
        """Runs one function. Returns (state, detail)."""
        from bsimvis.app.routes.llm import get_code_for_llm

        do_notes = "notes" in actions
        do_tags = "tags" in actions

        if not overwrite:
            notes_done = not do_notes or _already_enriched(
                self.r, collection, func_id, "notes"
            )
            tags_done = not do_tags or _already_enriched(
                self.r, collection, func_id, "tags"
            )
            if notes_done and tags_done:
                return "skipped", "already enriched"

        res, error = get_code_for_llm(func_id)
        if error:
            return "failed", error

        summary, tags, llm_error = llm_service.summarize_and_tag(
            res["func_name"], res["code"], vocabulary, custom_prompt
        )
        if llm_error:
            return "failed", llm_error
        if not summary and not tags:
            return "failed", "empty LLM response"

        if do_notes and summary:
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
                marked = t if t.startswith(LLM_TAG_PREFIX) else f"{LLM_TAG_PREFIX}{t}"
                # Free-form output is registered as it is written, so the next
                # overwrite can find and remove it by vocabulary.
                if marked.lower() not in known:
                    tag_service.create_tag(collection, marked, llm=True)
                    known.add(marked.lower())
                if tag_service.add_user_tag(collection, "function", func_id, marked):
                    applied.append(marked)
            _mark_enriched(self.r, collection, func_id, "tags")

        return "done", ", ".join(applied) if applied else None

    def run_batch(
        self,
        collection,
        func_ids,
        actions,
        overwrite=False,
        custom_prompt=None,
        vocabulary=None,
        job_service=None,
        job_id=None,
    ):
        """Runs the batch, continuing past per-function failures.

        Returns True when the job ran to completion (even with failures), False
        when it was cancelled or given nothing valid to do.
        """
        actions = [a for a in (actions or []) if a in ("notes", "tags")]
        if not actions:
            actions = ["notes"]

        total = len(func_ids)
        if not total:
            if job_service and job_id:
                job_service.add_log(job_id, "No functions matched the selection.")
            return False

        if vocabulary is None and "tags" in actions:
            vocabulary = tag_service.get_llm_vocabulary(collection)
        self._vocab_seen = {t.lower() for t in vocabulary or ()}

        if "tags" in actions and not vocabulary:
            # Silent free-form is how a collection ends up with 60 one-off tags:
            # say it in the job log so the run is explainable afterwards.
            msg = (
                "No tag flagged for the LLM vocabulary in this collection -- "
                "tagging runs free-form and tag names will drift. Flag tags on "
                "the Tags page to constrain it."
            )
            logging.warning(f"LLM batch on {collection}: {msg}")
            if job_service and job_id:
                job_service.add_log(job_id, msg)

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"LLM batch over {total} functions | actions={','.join(actions)} "
                f"| overwrite={overwrite} | vocabulary={len(vocabulary or [])} tags",
            )

        counters = {"done": 0, "skipped": 0, "failed": 0}
        lock = threading.Lock()
        cancelled = threading.Event()

        def worker(func_id):
            if cancelled.is_set():
                return
            if job_service and job_id and job_service.is_cancelled(job_id):
                cancelled.set()
                return
            try:
                state, detail = self._process_one(
                    collection, func_id, actions, overwrite, custom_prompt, vocabulary
                )
            except Exception as e:
                logging.error(f"LLM batch: {func_id} failed: {e}")
                state, detail = "failed", str(e)

            if job_id:
                self._record(job_id, func_id, state, detail)

            with lock:
                counters[state] = counters.get(state, 0) + 1
                processed = sum(counters.values())
                if job_service and job_id:
                    job_service.update_progress(
                        job_id,
                        int(processed * 100 / total),
                        (
                            f"{processed}/{total} | {func_id} -> {state}"
                            f"{' (' + detail + ')' if detail else ''}"
                        ),
                    )

        with ThreadPoolExecutor(max_workers=concurrency()) as pool:
            list(pool.map(worker, func_ids))

        if job_service and job_id:
            job_service.add_log(
                job_id,
                f"LLM batch finished: {counters['done']} done, "
                f"{counters['skipped']} skipped, {counters['failed']} failed.",
            )

        return not cancelled.is_set()


llm_batch_service = LLMBatchService()


def _selfcheck():
    """Orchestration check with the LLM and storage stubbed out."""
    # This module, not a re-import: under `python -m` the re-import would be a
    # second copy and the stubs below would patch the wrong globals.
    import sys

    mod = sys.modules[__name__]

    class FakeRedis:
        def __init__(self):
            self.sets = {}

        def sadd(self, key, val):
            self.sets.setdefault(key, set()).add(val)

        def sismember(self, key, val):
            return val in self.sets.get(key, set())

    svc = LLMBatchService.__new__(LLMBatchService)
    svc.r = FakeRedis()
    svc._record = lambda *a, **k: None

    notes, tags, calls, registered = [], [], [], []

    class FakeJobs:
        def __init__(self, cancel_after=None):
            self.cancel_after = cancel_after
            self.progress = []
            self.logs = []
            self.seen = 0

        def add_log(self, job_id, msg):
            self.logs.append(msg)

        def update_progress(self, job_id, pct, msg=None):
            self.progress.append(pct)

        def is_cancelled(self, job_id):
            self.seen += 1
            return self.cancel_after is not None and self.seen > self.cancel_after

    def run(func_ids, jobs, actions=("notes", "tags"), overwrite=False, existing=()):
        notes.clear()
        tags.clear()
        calls.clear()
        registered.clear()
        svc.r = FakeRedis()
        mod._already_enriched = lambda r, c, f, action: f in existing
        mod._remove_llm_notes = lambda c, f: notes.append(("del", f))
        mod._remove_llm_tags = lambda c, f: tags.append(("del", f))
        mod.note_service = type(
            "N",
            (),
            {"add_note": staticmethod(lambda c, f, t, owner: notes.append((f, owner)))},
        )
        mod.tag_service = type(
            "T",
            (),
            {
                "add_user_tag": staticmethod(
                    lambda c, e, f, t: tags.append((f, t)) or True
                ),
                "get_llm_vocabulary": staticmethod(lambda c: ["crypto"]),
                "create_tag": staticmethod(
                    lambda c, t, **kw: registered.append(t) or True
                ),
            },
        )
        mod.llm_service = type(
            "L",
            (),
            {
                "summarize_and_tag": staticmethod(
                    lambda name, code, vocab, prompt: (
                        calls.append(name) or ("a summary", ["crypto"], None)
                    )
                )
            },
        )
        # Code fetch is stubbed by patching the late import target.
        import bsimvis.app.routes.llm as llm_routes

        llm_routes.get_code_for_llm = lambda fid: (
            {"code": "int f(){}", "func_name": fid},
            None,
        )

        return svc.run_batch(
            "col",
            func_ids,
            list(actions),
            overwrite=overwrite,
            job_service=jobs,
            job_id="j1",
        )

    # Happy path: every function gets a note and its tags, marked as LLM output.
    jobs = FakeJobs()
    assert run(["f1", "f2"], jobs) is True
    assert notes == [("f1", LLM_NOTE_OWNER), ("f2", LLM_NOTE_OWNER)], notes
    assert tags == [("f1", "flag:crypto"), ("f2", "flag:crypto")], tags
    assert jobs.progress[-1] == 100, jobs.progress
    # A vocabulary tag arrives bare and is namespaced on write; registering it
    # once is what makes the next overwrite able to take it back off.
    assert registered == ["flag:crypto"], registered

    # Already enriched functions are skipped -- a rerun stays cheap.
    jobs = FakeJobs()
    assert run(["f1", "f2"], jobs, existing={"f1"}) is True
    assert calls == ["f2"], calls

    # overwrite=true regenerates them, clearing the previous LLM output first.
    jobs = FakeJobs()
    run(["f1"], jobs, overwrite=True, existing={"f1"})
    assert ("del", "f1") in notes and ("del", "f1") in tags, (notes, tags)
    assert calls == ["f1"], calls

    # Cancellation stops the run and is reported as an incomplete job.
    jobs = FakeJobs(cancel_after=0)
    assert run(["f1", "f2"], jobs) is False
    assert calls == [], calls

    # Empty selection is refused rather than completing vacuously.
    assert run([], FakeJobs()) is False

    print("ok")


if __name__ == "__main__":
    _selfcheck()
