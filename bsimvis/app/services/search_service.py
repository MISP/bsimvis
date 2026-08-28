"""Persisted, fast relevance-triage searches.

A Search is its own UUID-keyed record (name, scope, query, status), separate
from the job that runs its classification -- same relationship `PoolService`
has to the jobs it starts (`pool_service.py`). Unlike the contextual/pair/file
LLM analysis pipeline (`analysis_orchestrator.py`), classification here does
not partition by call-graph locality: a relevance verdict is independent per
function, so there is nothing to gain from SCC/context-propagation handling,
and batches can be much larger since each result is a verdict word plus one
short line instead of a full severity/tags/summary object. It does reuse
`analysis_orchestrator._group_by_duplicate_code` -- a duplicate function
trivially shares its representative's verdict, no reason to reclassify it.
"""

import json
import logging
import time
import uuid

from bsimvis.app.services.analysis_orchestrator import (
    _group_by_duplicate_code,
    _with_dups,
)
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.llm_tools import get_function
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.tag_service import tag_service

# Flat, no call-graph constraint -- bigger than the deep-analysis
# CLUSTER_BATCH_SIZE (5) since each result is one verdict word + a short
# line, not a full severity/tags/summary object.
SEARCH_BATCH_SIZE = 25

SEARCHES_INDEX = "searches:global"
SEARCHES_CAP = 500
_VERDICT_ORDER = {"yes": 0, "maybe": 1, "no": 2}


def _search_key(search_id):
    return f"search:{search_id}"


def _results_key(search_id):
    return f"search:{search_id}:results"


def _decode(v):
    return v.decode() if isinstance(v, bytes) else v


class SearchService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    # --- persistence ------------------------------------------------------

    def create_search(self, collection, scope, query, func_ids, name=None):
        """Persists a Search record and enqueues its classification job.
        Returns (search_id, job_id, total)."""
        from bsimvis.app.services.job_service import JobService, JobType

        search_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        meta = {
            "id": search_id,
            "name": (name or query or "")[:80],
            "collection": collection,
            "scope": json.dumps(scope or {}),
            "query": query or "",
            "status": "running",
            "total": len(func_ids),
            "created_at": now,
            "updated_at": now,
        }
        self.r.hset(_search_key(search_id), mapping=meta)
        self.r.lpush(SEARCHES_INDEX, search_id)
        self.r.ltrim(SEARCHES_INDEX, 0, SEARCHES_CAP - 1)

        job_id = JobService().create_job(
            JobType.SEARCH_CLASSIFY,
            {
                "search_id": search_id,
                "collection": collection,
                "func_ids": func_ids,
                "query": query,
            },
        )
        self.r.hset(_search_key(search_id), "job_id", job_id)
        return search_id, job_id, len(func_ids)

    def get_search(self, search_id):
        raw = self.r.hgetall(_search_key(search_id))
        if not raw:
            return None
        meta = {_decode(k): _decode(v) for k, v in raw.items()}
        try:
            meta["scope"] = json.loads(meta.get("scope") or "{}")
        except (TypeError, ValueError):
            meta["scope"] = {}
        meta["total"] = int(meta.get("total") or 0)
        return meta

    def list_searches(self, limit=50, offset=0):
        ids = [
            _decode(i)
            for i in self.r.lrange(SEARCHES_INDEX, offset, offset + limit - 1)
        ]
        total = self.r.llen(SEARCHES_INDEX)
        searches = [s for s in (self.get_search(i) for i in ids) if s]
        return searches, total

    def delete_search(self, search_id):
        from bsimvis.app.services.job_service import JobService

        meta = self.get_search(search_id)
        if not meta:
            return False, "Search not found"
        job_id = meta.get("job_id")
        if job_id and meta.get("status") == "running":
            JobService().cancel_job(job_id)
        pipe = self.r.pipeline()
        pipe.delete(_search_key(search_id))
        pipe.delete(_results_key(search_id))
        pipe.lrem(SEARCHES_INDEX, 0, search_id)
        pipe.execute()
        return True, "Search deleted"

    def _finish(self, search_id, status):
        self.r.hset(
            _search_key(search_id),
            mapping={"status": status, "updated_at": int(time.time() * 1000)},
        )

    # --- results ------------------------------------------------------

    def save_result(self, search_id, func_id, verdict, evidence, suggested_tag):
        self.r.hset(
            _results_key(search_id),
            func_id,
            json.dumps(
                {
                    "verdict": verdict,
                    "evidence": evidence,
                    "suggested_tag": suggested_tag,
                }
            ),
        )

    def get_verdict_counts(self, search_id):
        """{'yes': n, 'maybe': n, 'no': n} tally over all stored results, so a
        search row can show how many functions actually matched instead of
        just how many were processed (`total`).
        # ponytail: scans the full results hash rather than an incremental
        # counter kept in sync at save_result() time -- simpler and avoids
        # a second write path to keep correct, at the cost of an O(results)
        # scan per row on the list page. Move to a counter if that list page
        # gets slow with many/large searches.
        """
        raw = self.r.hgetall(_results_key(search_id))
        counts = {"yes": 0, "maybe": 0, "no": 0}
        for v in raw.values():
            try:
                verdict = json.loads(v).get("verdict")
            except (TypeError, ValueError, AttributeError):
                continue
            if verdict in counts:
                counts[verdict] += 1
        return counts

    def get_results(self, search_id, offset=0, limit=100, verdict=None):
        """Ranked (yes before maybe before no), paginated results. `verdict`
        is an optional iterable/str of verdicts to keep."""
        raw = self.r.hgetall(_results_key(search_id))
        rows = []
        for fid, v in raw.items():
            try:
                data = json.loads(v)
            except (TypeError, ValueError):
                continue
            data["func_id"] = _decode(fid)
            rows.append(data)
        if verdict:
            wanted = {verdict} if isinstance(verdict, str) else set(verdict)
            rows = [r for r in rows if r.get("verdict") in wanted]
        rows.sort(key=lambda r: (_VERDICT_ORDER.get(r.get("verdict"), 3), r["func_id"]))
        total = len(rows)
        return rows[offset : offset + limit], total

    # --- classification -------------------------------------------------

    def run_search_classification(
        self, search_id, collection, func_ids, query, job_service=None, job_id=None
    ):
        total = len(func_ids)
        if not total:
            if job_service and job_id:
                job_service.add_log(job_id, "No functions matched the selection.")
            self._finish(search_id, "failed")
            return False

        representatives, dup_map, code_cache = _group_by_duplicate_code(func_ids)
        vocabulary = {
            t.lower() for t in tag_service.get_llm_vocabulary(collection) or ()
        }

        processed = 0
        try:
            for i in range(0, len(representatives), SEARCH_BATCH_SIZE):
                if job_service and job_id and job_service.is_cancelled(job_id):
                    job_service.add_log(job_id, "Cancelled.")
                    self._finish(search_id, "cancelled")
                    return False

                chunk = representatives[i : i + SEARCH_BATCH_SIZE]
                members = []
                for fid in chunk:
                    data = code_cache.get(fid) or get_function(fid)
                    if "error" in data:
                        self._save_with_dups(
                            search_id, fid, dup_map, "no", "function lookup failed", None
                        )
                        continue
                    members.append(
                        (fid, data.get("func_name", fid), data.get("code") or "")
                    )

                if members:
                    results, missing, err = llm_service.classify_relevance_batch(
                        members, query, vocabulary
                    )
                    if err:
                        for fid, _, _ in members:
                            self._save_with_dups(
                                search_id, fid, dup_map, "no",
                                f"classification failed: {err}", None,
                            )
                    else:
                        for fid, (verdict, evidence, tag) in results.items():
                            self._save_with_dups(
                                search_id, fid, dup_map, verdict, evidence, tag
                            )
                        for fid in missing:
                            self._save_with_dups(
                                search_id, fid, dup_map, "no",
                                "no result returned", None,
                            )

                processed += len(_with_dups(chunk, dup_map))
                if job_service and job_id:
                    job_service.update_progress(
                        job_id,
                        int(processed * 100 / total),
                        f"{processed}/{total} classified",
                        processed=processed,
                        total=total,
                    )
        except Exception as e:
            logging.error(f"search_service: classification failed for {search_id}: {e}")
            if job_service and job_id:
                job_service.add_log(job_id, f"Failed: {e}")
            self._finish(search_id, "failed")
            return False

        self._finish(search_id, "completed")
        if job_service and job_id:
            job_service.add_log(
                job_id, f"Search classification finished: {processed}/{total} functions."
            )
        return True

    def _save_with_dups(self, search_id, fid, dup_map, verdict, evidence, tag):
        self.save_result(search_id, fid, verdict, evidence, tag)
        for dup_fid in dup_map.get(fid, ()):
            self.save_result(search_id, dup_fid, verdict, evidence, tag)


search_service = SearchService()
