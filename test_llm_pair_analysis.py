"""Focused checks for the comparison-analysis selection and prompt."""

import json
from unittest.mock import patch

from bsimvis.app.services.bin_sim_service import BinSimService
from bsimvis.app.services.llm_service import LLMService
from bsimvis.app.services.analysis_orchestrator import (
    _cluster_units,
    _group_by_duplicate_code,
    _needs_more_context,
    _normalize_code_for_dedup,
    _pair_report_rules,
    _with_dups,
    AnalysisOrchestrator,
    _pair_report_prompt,
    _select_pair_candidates,
)


def test_pair_candidates_default_to_changed_and_unique():
    diff = {
        "unique_to_a": [{"func_id": "a:func:ma:10"}],
        "unique_to_b": [{"func_id": "b:func:mb:20"}],
        "matched": [
            {"func_a": "a:func:ma:30", "func_b": "b:func:mb:40", "similarity": 0.71},
            {"func_a": "a:func:ma:50", "func_b": "b:func:mb:60", "similarity": 0.99},
        ],
    }

    picked = _select_pair_candidates(diff, threshold=0.9)
    by_fid = {row["func_id"]: row for row in picked}
    assert set(by_fid) == {
        "a:func:ma:10",
        "b:func:mb:20",
        "a:func:ma:30",
        "b:func:mb:40",
    }
    assert by_fid["a:func:ma:10"]["reason"] == "unique"
    assert by_fid["a:func:ma:30"]["reason"] == "changed_match"
    assert all(not ({"tags", "severity", "malicious"} & set(row)) for row in picked)


def test_pair_report_prompt_forbids_guessing_from_diff_shape():
    prompt = _pair_report_prompt(
        {
            "md5_a": "ma",
            "md5_b": "mb",
            "score": 0.4,
            "diff": {
                "matched": [
                    {
                        "func_a": "a:func:ma:30",
                        "func_b": "b:func:mb:40",
                        "similarity": 0.99,
                    }
                ]
            },
        },
        {
            "a:func:ma:10": {
                "func_name": "f",
                "summary": "Copies bytes; intent is inconclusive.",
                "tags": ["severity:high", "category:impact:ddos"],
            }
        },
        [{"func_id": "a:func:ma:10", "side": "a", "reason": "unique"}],
    )
    lower = prompt.lower()
    assert "uniqueness is not evidence of maliciousness" in lower
    assert "state inconclusive" in lower
    assert "cite" in lower and "function" in lower
    assert "a:func:ma:30 <-> b:func:mb:40" in prompt
    assert "correspondence only" in lower
    assert "severity:high" in prompt


def test_pair_candidates_are_bounded_and_balanced():
    pair = {
        "diff": {
            "unique_to_a": [{"func_id": f"a:func:ma:{i}"} for i in range(3)],
            "unique_to_b": [{"func_id": f"b:func:mb:{i}"} for i in range(3)],
            "matched": [],
        }
    }
    values = {
        **{
            f"a:func:ma:{i}:meta": json.dumps({"bsim_features_count": 100 - i * 10})
            for i in range(3)
        },
        **{
            f"b:func:mb:{i}:meta": json.dumps({"bsim_features_count": 70 - i * 10})
            for i in range(3)
        },
    }

    class Pipe:
        def __init__(self):
            self.keys = []

        def get(self, key):
            self.keys.append(key)
            return self

        def execute(self):
            return [values.get(key) for key in self.keys]

    class Redis:
        def pipeline(self, transaction=False):
            return Pipe()

    picked = AnalysisOrchestrator(Redis()).pair_candidates(
        pair, skip_fid_tagged=False, max_functions=4
    )
    assert [row["func_id"] for row in picked] == [
        "a:func:ma:0",
        "b:func:mb:0",
        "a:func:ma:1",
        "b:func:mb:1",
    ]

    # max_functions=0 (default) is unlimited -- the global batch cap is a
    # route-level warning only, and must not silently truncate the diff pool.
    with patch(
        "bsimvis.app.services.analysis_orchestrator.max_batch_size", return_value=2
    ):
        uncapped = AnalysisOrchestrator(Redis()).pair_candidates(
            pair, skip_fid_tagged=False, max_functions=0
        )
    assert len(uncapped) == 6


def test_unresolved_verdict_abstains_without_speculation():
    summary, tags = LLMService._split_summary_tags(
        "The function purpose is unclear from unresolved callees. "
        "It suggests a daemon loop.\n"
    )
    assert summary == (
        "The function purpose is unclear from unresolved callees. "
        "It suggests a daemon loop."
    )
    assert tags == []
    assert _needs_more_context("Unclear result.\nNEED_MORE_CONTEXT")
    summary, tags = LLMService._split_summary_tags(
        "The function purpose is unclear.\nNEED_MORE_CONTEXT"
    )
    assert (summary, tags) == ("NEED_MORE_CONTEXT", [])


def test_shell_execution_alone_is_not_high_severity_c2():
    summary, tags = LLMService._split_summary_tags(
        "Invokes /bin/sh; this suggests potential C2 or remote execution.\n"
        "TAGS: severity:high, category:process:shell"
    )
    assert summary.startswith("Invokes /bin/sh")
    assert tags == ["category:process:shell"]

    _, tags = LLMService._split_summary_tags(
        "Receives commands from a C2 remote controller over a socket and dispatches "
        "DDoS attack routines.\n"
        "TAGS: severity:high, category:network:c2"
    )
    assert tags == ["severity:high", "category:network:c2"]


def test_pair_prompts_calibrate_direct_abuse_and_benign_utilities():
    rules = (
        _pair_report_rules()
        + __import__(
            "bsimvis.app.services.tag_taxonomy", fromlist=["prompt_rules"]
        ).prompt_rules()
    ).lower()
    assert "cpu hang. reading /proc" in rules
    assert "visible operation or resolved callee" in rules
    assert "cpu reading /proc" not in rules
    assert "daemon setup. hang" not in rules
    assert "does not require an identified victim" in rules
    assert "ordinary user-invoked" in rules
    assert "not persistence across reboot" in rules
    assert "do not call daemonization persistence" in rules
    assert "packet-construction loop" in rules
    assert "not scanning" in rules
    assert "invoking /bin/sh" in rules
    assert "process enumeration, not anti-analysis" in rules
    assert "do not speculate that process enumeration supports evasion" in rules
    assert "do not claim stack exhaustion" in rules
    assert "state machine or daemon loop" in rules
    assert "syscall number, privilege level, malware family, rootkit" in rules


def test_pair_run_groups_origins_writes_report_and_resplits_exact_sid():
    sid = "global:pool:p:bin_sim:uc:a:ma::b:mb"
    fids = ["a:func:ma:10", "b:func:mb:20"]
    pair = {
        "md5_a": "ma",
        "md5_b": "mb",
        "coll_a": "a",
        "coll_b": "b",
        "score": 0.4,
        "diff": {
            "unique_to_a": [{"func_id": fids[0]}],
            "unique_to_b": [{"func_id": fids[1]}],
            "matched": [],
        },
    }

    class Pipe:
        def __init__(self, values):
            self.values = values
            self.keys = []

        def get(self, key):
            self.keys.append(key)
            return self

        def execute(self):
            return [self.values.get(key) for key in self.keys]

    class DataRedis:
        def __init__(self):
            self.values = {
                sid: json.dumps(pair),
                f"{fids[0]}:meta": json.dumps({"bsim_features_count": 20}),
                f"{fids[1]}:meta": json.dumps({"bsim_features_count": 30}),
            }

        def get(self, key):
            return self.values.get(key)

        def pipeline(self, transaction=False):
            return Pipe(self.values)

    class JobRedis:
        def __init__(self):
            self.values = {}

        def hset(self, key, field, value):
            self.values[(key, field)] = value

    class JobService:
        def __init__(self):
            self.r = JobRedis()
            self.logs = []

        def add_log(self, job_id, message):
            self.logs.append(message)

    runner = AnalysisOrchestrator(DataRedis())
    calls = []

    def contextual(origin, func_ids, summaries_out=None, **kwargs):
        calls.append((origin, list(func_ids)))
        for fid in func_ids:
            summaries_out[fid] = {
                "func_name": fid[-2:],
                "summary": "Observed behavior.",
            }
        return True

    runner.run_contextual_batch = contextual
    job_service = JobService()
    with (
        patch(
            "bsimvis.app.services.analysis_orchestrator.llm_service.chat",
            return_value="report",
        ) as chat,
        patch(
            "bsimvis.app.services.bin_sim_service.bin_sim_service.resplit_bin_sim"
        ) as resplit,
    ):
        assert runner.run_pair_analysis(
            sid, "global:pool:p", algo="uc", job_service=job_service, job_id="j1"
        )

    assert calls == [("a", [fids[0]]), ("b", [fids[1]])]
    assert all(fid in chat.call_args.args[0][1]["content"] for fid in fids)
    assert job_service.r.values[("job:j1", "report")] == "report"
    resplit.assert_called_once_with("global:pool:p", algo="uc", sid=sid)

    with (
        patch(
            "bsimvis.app.services.analysis_orchestrator.llm_service.chat",
            return_value="Error: unavailable",
        ),
        patch(
            "bsimvis.app.services.bin_sim_service.bin_sim_service.resplit_bin_sim"
        ) as failed_report_resplit,
    ):
        assert not runner.run_pair_analysis(
            sid, "global:pool:p", algo="uc", job_service=job_service, job_id="j2"
        )
    failed_report_resplit.assert_called_once_with("global:pool:p", algo="uc", sid=sid)


def test_pair_cap_keeps_changed_matches_atomic_and_prefers_deltas():
    pair = {
        "diff": {
            "unique_to_a": [{"func_id": "a:func:ma:u"}],
            "unique_to_b": [{"func_id": "b:func:mb:u"}],
            "matched": [
                {
                    "func_a": "a:func:ma:changed",
                    "func_b": "b:func:mb:changed",
                    "similarity": 0.5,
                },
                {
                    "func_a": "a:func:ma:same",
                    "func_b": "b:func:mb:same",
                    "similarity": 0.99,
                },
            ],
        }
    }
    complexities = {
        "a:func:ma:u": 80,
        "b:func:mb:u": 70,
        "a:func:ma:changed": 100,
        "b:func:mb:changed": 10,
        "a:func:ma:same": 1000,
        "b:func:mb:same": 1000,
    }

    class Pipe:
        def __init__(self):
            self.keys = []

        def get(self, key):
            self.keys.append(key)
            return self

        def execute(self):
            return [
                json.dumps({"bsim_features_count": complexities[key[:-5]]})
                for key in self.keys
            ]

    class Redis:
        def pipeline(self, transaction=False):
            return Pipe()

    picked = AnalysisOrchestrator(Redis()).pair_candidates(
        pair,
        include_unchanged=True,
        skip_fid_tagged=False,
        max_functions=3,
    )
    picked_ids = {row["func_id"] for row in picked}
    assert {
        "a:func:ma:changed",
        "b:func:mb:changed",
    } <= picked_ids
    assert not ({"a:func:ma:same", "b:func:mb:same"} & picked_ids)


def test_high_severity_requires_a_malicious_category():
    _, tags = LLMService._split_summary_tags(
        "Implements an ordinary HTTP downloader.\n"
        "TAGS: severity:high, category:network:download"
    )
    assert tags == ["category:network:download"]
    _, tags = LLMService._split_summary_tags(
        "Purpose is unresolved.\nTAGS: severity:high"
    )
    assert tags == []
    _, tags = LLMService._split_summary_tags(
        "Implements a normal checksum utility.\nTAGS: severity:none"
    )
    assert tags == ["severity:none"]
    _, tags = LLMService._split_summary_tags(
        "Purpose is unresolved.\nNEED_MORE_CONTEXT\n"
        "TAGS: severity:high, category:evasion:rootkit"
    )
    assert tags == []
    _, tags = LLMService._split_summary_tags(
        "Executes an unidentified syscall then loops forever.\n"
        "TAGS: severity:high, category:evasion:rootkit, category:persistence:autostart"
    )
    assert tags == []


def test_unresolved_clause_does_not_hide_concrete_abuse():
    _, tags = LLMService._split_summary_tags(
        "Wrapper purpose is unclear. It repeatedly sends crafted packets to a target.\n"
        "TAGS: severity:high, category:impact:ddos"
    )
    assert tags == ["severity:high", "category:impact:ddos"]


def test_max_file_entrypoint_reads_the_full_file_index():
    class Redis:
        def smembers(self, key):
            assert key == "clean:idx:file:functions:reference"
            return {
                b"clean:func:reference:10",
                b"clean:func:reference:180021a90",
                b"malformed",
            }

    assert BinSimService(Redis()).max_file_entrypoint("clean", "reference") == int(
        "180021a90", 16
    )


def test_normalize_code_for_dedup_strips_address_tokens():
    a = "int FUN_00401230(void) { return DAT_0040c000 + 0x1234abcd; }"
    b = "int FUN_00512340(void) { return DAT_0050d111 + 0x99887766; }"
    assert _normalize_code_for_dedup(a) == _normalize_code_for_dedup(b)
    assert _normalize_code_for_dedup("int f(void) { return 1; }") != (
        _normalize_code_for_dedup("int f(void) { return 2; }")
    )


def test_group_by_duplicate_code_collapses_identical_bodies_only():
    funcs = {
        "a:func:m:1": {
            "func_id": "a:func:m:1",
            "func_name": "f1",
            "code": "return DAT_00401000 + 1;",
        },
        "a:func:m:2": {
            "func_id": "a:func:m:2",
            "func_name": "f2",
            # identical to f1 once the address token is normalized out
            "code": "return DAT_00499999 + 1;",
        },
        "a:func:m:3": {
            "func_id": "a:func:m:3",
            "func_name": "f3",
            "code": "return 2;",
        },
        "a:func:m:4": {"func_id": "a:func:m:4", "error": "not found"},
    }
    with patch(
        "bsimvis.app.services.analysis_orchestrator.get_function",
        side_effect=lambda fid: funcs[fid],
    ):
        reps, dup_map, cache = _group_by_duplicate_code(list(funcs))
    assert sorted(reps) == ["a:func:m:1", "a:func:m:3", "a:func:m:4"]
    assert dup_map == {"a:func:m:1": ["a:func:m:2"]}
    # duplicates' fetched data is not kept around once it has served the hash
    assert "a:func:m:2" not in cache
    assert cache["a:func:m:1"]["func_name"] == "f1"
    assert _with_dups(["a:func:m:1", "a:func:m:3"], dup_map) == [
        "a:func:m:1",
        "a:func:m:3",
        "a:func:m:2",
    ]


def test_cluster_units_batches_connected_singletons_keeps_scc_whole():
    # a -> b -> c is a non-cyclic chain (batchable); d + e call each other
    # (a true cycle, must stay a standalone "scc" unit); f is isolated.
    adj = {"a": ["b"], "b": ["c"], "d": ["e"], "e": ["d"]}
    units = [["a"], ["b"], ["c"], ["d", "e"], ["f"]]
    clustered = _cluster_units(units, adj, batch_size=5)
    assert ("batch", ["a", "b", "c"]) in clustered
    assert ("scc", ["d", "e"]) in clustered
    assert ("single", ["f"]) in clustered
    # the SCC must never be folded into a neighbouring batch
    for kind, fids in clustered:
        if kind == "scc":
            assert fids == ["d", "e"]


def test_cluster_units_respects_batch_size_and_disconnected_singletons():
    # every function calls the next one, but the cap is 2 per batch.
    adj = {"a": ["b"], "b": ["c"], "c": ["d"]}
    units = [["a"], ["b"], ["c"], ["d"]]
    clustered = _cluster_units(units, adj, batch_size=2)
    assert clustered == [
        ("batch", ["a", "b"]),
        ("batch", ["c", "d"]),
    ]

    # two singletons with no call relation must not be merged.
    clustered = _cluster_units([["x"], ["y"]], {}, batch_size=5)
    assert clustered == [("single", ["x"]), ("single", ["y"])]

    # batch_size<=1 disables batching entirely.
    clustered = _cluster_units([["a"], ["b"]], adj, batch_size=1)
    assert clustered == [("single", ["a"]), ("single", ["b"])]


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print(f"ok  {fn.__name__}")
