"""Focused transfer check; no Redis, Ghidra, or worker needed."""

import json

from flask import Flask

from bsimvis.app.routes import file as routes


class FakeRedis:
    def __init__(self, data):
        self.data = data

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value):
        self.data[key] = value

    def sismember(self, key, value):
        return key == "source:all_files" and value == "source:file:" + "a" * 32

    def smembers(self, key):
        if key == "global:collections":
            return {"source"}
        if key == "source:batch:batch-1:files":
            return {"a" * 32}
        return set()

    def sscan_iter(self, *_):
        return iter(())


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def get(self, key):
        self.commands.append(("get", key))

    def zrange(self, key, *_args, **_kwargs):
        self.commands.append(("zrange", key))

    def execute(self):
        return [
            self.redis.get(key) if kind == "get" else [] for kind, key in self.commands
        ]


class IndexedFakeRedis(FakeRedis):
    def __init__(self, data, sets):
        super().__init__(data)
        self.sets = sets

    def smembers(self, key):
        return self.sets.get(key, super().smembers(key))

    def pipeline(self, **_kwargs):
        return FakePipeline(self)


class FakeJobs:
    def __init__(self):
        self.pipelines = []

    def create_pipeline(self, tasks, enqueue=False):
        self.pipelines.append(tasks)
        return f"pipeline-{len(self.pipelines)}"

    def seal_wave(self, collection, extra_members, options):
        assert collection == "target" and extra_members == ["pipeline-1"]
        assert options["batch_uuid"]
        return "master"


def main():
    md5 = "a" * 32
    source_data = json.dumps(
        {"file_md5": md5, "file_metadata": {"file_md5": md5}, "functions": []}
    ).encode()
    redis = FakeRedis({f"source:file:{md5}:data": source_data})
    indexed_md5 = "b" * 32
    indexed_fid = f"source:func:{indexed_md5}:100"
    indexed = IndexedFakeRedis(
        {
            f"source:file:{indexed_md5}:meta": json.dumps(
                {"file_md5": indexed_md5, "file_name": "indexed.bin"}
            ).encode(),
            f"{indexed_fid}:meta": json.dumps(
                {"full_id": "fn:@100", "function_name": "fn"}
            ).encode(),
        },
        {f"source:idx:file:functions:{indexed_md5}": {indexed_fid}},
    )
    rebuilt = json.loads(routes._load_analyzed_data(indexed, "source", indexed_md5))
    assert (
        len(rebuilt["functions"]) == 1
        and rebuilt["file_metadata"]["file_name"] == "indexed.bin"
    )
    old_redis, old_jobs = routes.get_redis, routes.job_service
    routes.get_redis, routes.job_service = lambda: redis, FakeJobs()
    try:
        app = Flask(__name__)
        with app.test_request_context(
            "/api/file/transfer",
            method="POST",
            json={
                "collection": "target",
                "md5s": f'"{md5}":\n',
                "preview": True,
            },
        ):
            preview = routes.transfer_analyzed_files()
        assert preview["ready"] == 1 and preview["files"][0]["md5"] == md5
        with app.test_request_context(
            "/api/file/transfer",
            method="POST",
            json={
                "collection": "target",
                "batch_uuid": "batch-1",
                "preview": True,
            },
        ):
            batch_preview = routes.transfer_analyzed_files()
        assert batch_preview["ready"] == 1
        with app.test_request_context(
            "/api/file/transfer",
            method="POST",
            json={"collection": "target", "md5s": [md5]},
        ):
            result = routes.transfer_analyzed_files()
        assert result["transferred"] == 1 and result["master_pipeline_id"] == "master"
        copied = json.loads(redis.get(f"target:file:{md5}:data"))
        assert copied["collection"] == "target"
        assert copied["file_metadata"]["batch_uuid"] == result["batch_uuid"]
    finally:
        routes.get_redis, routes.job_service = old_redis, old_jobs


if __name__ == "__main__":
    main()
