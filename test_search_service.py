"""Focused checks for the persisted fast-relevance search feature
(search_service.py + routes/searches.py's scope resolution)."""

from unittest.mock import MagicMock, patch

from bsimvis.app.services.search_service import SearchService


class FakeRedis:
    """Enough of the API for SearchService's hash/list usage."""

    def __init__(self):
        self.h = {}
        self.lists = {}

    def hset(self, key, field=None, value=None, mapping=None):
        d = self.h.setdefault(key, {})
        if mapping:
            d.update({k: str(v) for k, v in mapping.items()})
        if field is not None:
            d[field] = str(value)

    def hgetall(self, key):
        return dict(self.h.get(key, {}))

    def lpush(self, key, val):
        self.lists.setdefault(key, []).insert(0, val)

    def ltrim(self, key, start, end):
        self.lists[key] = self.lists.get(key, [])[start : end + 1]

    def lrange(self, key, start, end):
        lst = self.lists.get(key, [])
        return lst[start:] if end == -1 else lst[start : end + 1]

    def llen(self, key):
        return len(self.lists.get(key, []))

    def lrem(self, key, count, val):
        lst = self.lists.get(key, [])
        if val in lst:
            lst.remove(val)

    def delete(self, *keys):
        for k in keys:
            self.h.pop(k, None)

    def pipeline(self, transaction=False):
        return self

    def execute(self):
        return []


FUNCS = {
    "a:func:m:1": {
        "func_id": "a:func:m:1",
        "func_name": "f1",
        "code": "return DAT_00401000+1;",
    },
    "a:func:m:2": {
        # identical to f1 once Ghidra's address token is normalized -- a
        # duplicate that should inherit f1's verdict, not get its own call.
        "func_id": "a:func:m:2",
        "func_name": "f2",
        "code": "return DAT_00499999+1;",
    },
    "a:func:m:3": {
        "func_id": "a:func:m:3",
        "func_name": "f3",
        "code": "builds .dat wildcard byte by byte",
    },
}


def _fake_get_function(fid):
    return FUNCS.get(fid, {"error": "not found"})


def _fake_classify(members, query, vocabulary=None):
    out = {}
    for fid, _name, code in members:
        if ".dat" in code.lower():
            out[fid] = ("yes", "mentions .dat", None)
        else:
            out[fid] = ("no", "", None)
    return out, [], None


def _run(svc, func_ids=None, classify=_fake_classify):
    func_ids = func_ids if func_ids is not None else list(FUNCS)
    with patch(
        "bsimvis.app.services.search_service.get_function",
        side_effect=_fake_get_function,
    ), patch(
        "bsimvis.app.services.search_service.llm_service"
    ) as mock_llm, patch(
        "bsimvis.app.services.tag_service.tag_service.get_llm_vocabulary",
        return_value=[],
    ):
        mock_llm.classify_relevance_batch.side_effect = classify
        search_id, job_id, total = svc.create_search(
            "main", {"type": "filter"}, "find .dat handling", func_ids
        )
        ok = svc.run_search_classification(search_id, "main", func_ids, "find .dat handling")
    return search_id, job_id, total, ok


def test_duplicate_functions_share_a_verdict_without_a_second_llm_call():
    svc = SearchService(FakeRedis())
    search_id, _job_id, total, ok = _run(svc)
    assert total == 3 and ok is True

    rows, rtotal = svc.get_results(search_id)
    assert rtotal == 3
    by_id = {row["func_id"]: row for row in rows}
    assert by_id["a:func:m:1"]["verdict"] == "no"
    assert by_id["a:func:m:2"]["verdict"] == "no"  # inherited, not reclassified
    assert by_id["a:func:m:2"]["evidence"] == by_id["a:func:m:1"]["evidence"]
    assert by_id["a:func:m:3"]["verdict"] == "yes"
    # yes ranks before no
    assert rows[0]["func_id"] == "a:func:m:3"


def test_verdict_filter_and_pagination():
    svc = SearchService(FakeRedis())
    search_id, _job_id, _total, _ok = _run(svc)

    yes_only, yes_total = svc.get_results(search_id, verdict="yes")
    assert yes_total == 1 and yes_only[0]["func_id"] == "a:func:m:3"

    no_only, no_total = svc.get_results(search_id, verdict=["no"])
    assert no_total == 2

    page, _ = svc.get_results(search_id, offset=1, limit=1)
    assert len(page) == 1


def test_list_and_delete_search():
    svc = SearchService(FakeRedis())
    search_id, job_id, _total, _ok = _run(svc)

    searches, total = svc.list_searches()
    assert total == 1
    assert searches[0]["id"] == search_id
    assert searches[0]["status"] == "completed"

    with patch("bsimvis.app.services.job_service.JobService") as mock_js_cls:
        mock_js = MagicMock()
        mock_js_cls.return_value = mock_js
        # a completed search must not try to cancel a job that already finished
        ok, _msg = svc.delete_search(search_id)
        assert ok
        mock_js.cancel_job.assert_not_called()

    assert svc.get_search(search_id) is None


def test_delete_cancels_a_still_running_search():
    svc = SearchService(FakeRedis())
    search_id, job_id, _total, _ok = _run(svc, classify=lambda members, query, vocabulary=None: (
        {}, [fid for fid, _, _ in members], "boom"
    ))
    # force it back to "running" as if the job were still in flight
    svc.r.hset(f"search:{search_id}", "status", "running")

    with patch("bsimvis.app.services.job_service.JobService") as mock_js_cls:
        mock_js = MagicMock()
        mock_js_cls.return_value = mock_js
        ok, _msg = svc.delete_search(search_id)
        assert ok
        mock_js.cancel_job.assert_called_once_with(job_id)


def test_empty_selection_fails_without_calling_the_llm():
    svc = SearchService(FakeRedis())
    with patch("bsimvis.app.services.search_service.llm_service") as mock_llm:
        ok = svc.run_search_classification("nonexistent", "main", [], "query")
    assert ok is False
    mock_llm.classify_relevance_batch.assert_not_called()


def test_resolve_scope_dispatches_by_type():
    from bsimvis.app.routes.searches import _resolve_scope

    with patch(
        "bsimvis.app.routes.llm._resolve_filters_to_ids",
        return_value=(["a:func:m:1"], None),
    ) as mock_resolve:
        ids, err = _resolve_scope("main", {"type": "collection"})
        assert err is None and ids == ["a:func:m:1"]
        assert mock_resolve.call_args[0][1] == ""  # empty filter == whole collection

        mock_resolve.reset_mock()
        ids, err = _resolve_scope("main", {"type": "file", "md5": "abc123"})
        assert err is None
        assert "md5=abc123" in mock_resolve.call_args[0][1]

        mock_resolve.reset_mock()
        ids, err = _resolve_scope("main", {"type": "filter", "filters": "tag=x"})
        assert err is None
        assert mock_resolve.call_args[0][1] == "tag=x"

    ids, err = _resolve_scope("main", {"type": "filter"})
    assert ids is None and "filters" in err

    ids, err = _resolve_scope("main", {"type": "not-a-real-type"})
    assert ids is None and "scope.type" in err


def test_resolve_scope_pair_defaults_include_unchanged_true():
    from bsimvis.app.routes.searches import _resolve_scope

    fake_pair = {"diff": {}}
    with patch(
        "bsimvis.app.services.bin_sim_service.bin_sim_service.load_pair",
        return_value=("sid", fake_pair),
    ), patch(
        "bsimvis.app.services.analysis_orchestrator.analysis_orchestrator.pair_candidates"
    ) as mock_candidates:
        mock_candidates.return_value = [{"func_id": "a:func:m:1"}]
        ids, err = _resolve_scope(
            "main", {"type": "pair", "md5_a": "aa", "md5_b": "bb"}
        )
        assert err is None and ids == ["a:func:m:1"]
        # positional args: pair, threshold, include_unique, include_unchanged, ...
        args = mock_candidates.call_args[0]
        assert args[3] is True  # include_unchanged defaults True for search

        _resolve_scope(
            "main", {"type": "pair", "md5_a": "aa", "md5_b": "bb", "state": "unique"}
        )
        args = mock_candidates.call_args[0]
        assert args[1:4] == (0, True, False)

        _resolve_scope(
            "main", {"type": "pair", "md5_a": "aa", "md5_b": "bb", "state": "matched"}
        )
        args = mock_candidates.call_args[0]
        assert args[2:4] == (False, True)


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print(f"ok  {fn.__name__}")
