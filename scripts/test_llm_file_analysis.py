"""Focused checks for file and collection-wide LLM analysis job creation."""

from unittest.mock import patch

from flask import Flask

from bsimvis.app.routes.llm_analysis import file_analysis
from bsimvis.app.services.job_service import JobType


class FakeDataRedis:
    def sscan_iter(self, key):
        assert key == "sample:all_files"
        return iter({b"sample:file:aaa", b"sample:file:bbb", b"sample:file:empty"})


class FakeJobService:
    tasks = None

    def create_group(self, tasks):
        self.__class__.tasks = tasks
        return "group_test"


def test_collection_creates_one_file_job_per_nonempty_file():
    app = Flask(__name__)

    def resolve(_collection, filters, _cap):
        md5 = filters.split("md5=", 1)[1].split("&", 1)[0]
        if md5 == "empty":
            return [], None
        return [f"sample:func:{md5}:1", f"sample:func:{md5}:2"], None

    with (
        app.test_request_context(
            json={
                "collection": "sample",
                "actions": ["notes"],
                "min_complexity": 7,
                "skip_fid_tagged": True,
                "custom_prompt": "Find persistence.",
            }
        ),
        patch(
            "bsimvis.app.services.analysis_orchestrator.max_batch_size",
            return_value=100,
        ),
        patch(
            "bsimvis.app.services.redis_client.get_redis", return_value=FakeDataRedis()
        ),
        patch("bsimvis.app.routes.llm._resolve_filters_to_ids", side_effect=resolve),
        patch("bsimvis.app.services.job_service.JobService", FakeJobService),
    ):
        result = file_analysis()

    assert result == {"job_id": "group_test", "total": 4, "files": 2}
    assert [task[0] for task in FakeJobService.tasks] == [
        JobType.LLM_FILE_ANALYSIS,
        JobType.LLM_FILE_ANALYSIS,
    ]
    payloads = [task[1] for task in FakeJobService.tasks]
    assert [payload["file_md5"] for payload in payloads] == ["aaa", "bbb"]
    assert all(payload["actions"] == ["notes"] for payload in payloads)
    assert all(payload["custom_prompt"] == "Find persistence." for payload in payloads)


if __name__ == "__main__":
    test_collection_creates_one_file_job_per_nonempty_file()
    print("ok")
