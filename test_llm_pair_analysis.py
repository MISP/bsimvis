"""Focused checks for the comparison-analysis selection and prompt."""

import json
from unittest.mock import patch

from bsimvis.app.services.bin_sim_service import BinSimService
from bsimvis.app.services.analysis_orchestrator import (
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


if __name__ == "__main__":
    for fn in [v for k, v in sorted(globals().items()) if k.startswith("test_")]:
        fn()
        print(f"ok  {fn.__name__}")
