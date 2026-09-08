from flask import Flask

from bsimvis.app.routes import similarity
from bsimvis.app.services.job_service import JobType


class FakeRedis:
    def smembers(self, key):
        assert key == "sample:all_files"
        return {"sample:file:bbb", "sample:file:aaa"}


class FakeJobService:
    def __init__(self):
        self.groups = []
        self.lanes = []

    def create_group(self, tasks, enqueue=True):
        assert enqueue is False
        self.groups.append(tasks)
        return "group-id"

    def submit_to_lane(self, collection, tasks):
        self.lanes.append((collection, tasks))
        return "pipeline-id"


def test_all_similarity_builds_are_sharded_by_file(monkeypatch):
    jobs = FakeJobService()
    monkeypatch.setattr(similarity, "job_service", jobs)
    monkeypatch.setattr(similarity, "get_redis", lambda: FakeRedis())
    app = Flask(__name__)

    payload = {
        "collection": "sample",
        "all": True,
        "algo": "unweighted_cosine",
        "min_score": 0.9,
        "top_k": 1000,
        "min_features": 10,
    }
    with app.test_request_context("/", method="POST", json=payload):
        similarity.build_similarity()

    assert [task[1]["md5"] for task in jobs.groups[0]] == ["aaa", "bbb"]
    assert all(task[0] == JobType.BUILD_SIM for task in jobs.groups[0])
    assert all(task[1]["force"] is True for task in jobs.groups[0])
    assert jobs.lanes[0][1][0] == "group-id"
    assert jobs.lanes[0][1][1][0] == JobType.INDEX_SIM

    with app.test_request_context("/", method="POST", json=payload):
        similarity.rebuild_similarity()

    rebuild_tasks = jobs.lanes[1][1]
    assert rebuild_tasks[0][0] == JobType.CLEAR_SIM
    assert rebuild_tasks[1] == "group-id"
    assert rebuild_tasks[2][0] == JobType.INDEX_SIM


class MonkeyPatch:
    def setattr(self, obj, name, value):
        setattr(obj, name, value)


if __name__ == "__main__":
    test_all_similarity_builds_are_sharded_by_file(MonkeyPatch())
    print("PASS: collection-wide similarity builds shard by file")
