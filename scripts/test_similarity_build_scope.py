"""Collection scheduling and incremental targets, without a running database."""

from unittest.mock import MagicMock, patch

from flask import Flask

with (
    patch("bsimvis.app.services.redis_client.get_redis", return_value=MagicMock()),
    patch(
        "bsimvis.app.services.redis_client.get_queue_redis", return_value=MagicMock()
    ),
):
    from bsimvis.app.routes import similarity as routes
    from bsimvis.app.services.similarity_service import SimilarityService
    from bsimvis.app.services.config_service import config_service


def main():
    app = Flask(__name__)
    jobs = MagicMock()
    db = MagicMock()
    db.smembers.return_value = {"test:file:a", "test:file:b"}
    with (
        patch.object(routes, "job_service", jobs),
        patch.object(routes, "get_redis", return_value=db),
    ):
        for route in (routes.build_similarity, routes.rebuild_similarity):
            for split in (False, True):
                jobs.reset_mock()
                with app.test_request_context(
                    json={"collection": "test", "all": True, "split_by_file": split}
                ):
                    route()
                tasks = jobs.submit_to_lane.call_args.args[1]
                if split:
                    children = jobs.create_group.call_args.args[0]
                    assert len(children) == 2
                    assert all(not p.get("force", False) for _, p in children)
                else:
                    jobs.create_group.assert_not_called()
                    build = next(p for t, p in tasks if t == routes.JobType.BUILD_SIM)
                    assert build["md5"] is None and build["batch_uuid"] is None
            with app.test_request_context(json={"all": True, "split_by_file": "false"}):
                assert route()[1] == 400
        with app.test_request_context(
            json={"collection": "test", "md5": "a", "force": True}
        ):
            routes.build_similarity()
        assert jobs.submit_to_lane.call_args.args[1][0][1]["force"] is True

    service = SimilarityService.__new__(SimilarityService)
    service.r = db
    service._reset_read_caches = lambda: None
    old, new = "test:func:a:1", "test:func:b:2"
    built = {old}
    db.get.return_value = "1"
    db.smembers.return_value = {old, new}
    db.delete.side_effect = lambda key: built.clear()

    # Inspect targets at discovery, without replacing build_batch's selection logic.
    class Targets(Exception):
        pass

    def snapshot(*args, **kwargs):
        raise Targets

    service.build_lca_snapshot = snapshot
    seen = []

    def unbuilt(key, ids):
        targets = [f for f in ids if f not in built]
        seen.append(set(targets))
        return targets

    service._unbuilt = unbuilt
    with patch.object(
        config_service,
        "get",
        side_effect=lambda key, default=None: (
            "rust_cpu" if key == "similarity.discovery_backend" else default
        ),
    ):
        for force, expected in ((False, {new}), (True, {old, new})):
            try:
                service.build_batch("test", force=force)
            except Targets:
                pass
            else:
                raise AssertionError("snapshot was not reached")
            assert seen[-1] == expected
        built.update({old, new})
        assert service.build_batch("test") is True
        assert seen[-1] == set()
    print("PASS: collection/file scheduling, force, and incremental target selection")


if __name__ == "__main__":
    main()
