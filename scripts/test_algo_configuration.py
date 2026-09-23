"""Fast checks for algorithm config and namespace selection; no services required."""

from unittest.mock import patch

from flask import Flask

from bsimvis.app.routes.search_collection import set_collection_config
from bsimvis.app.services import config_service
from bsimvis.app.services.collection_config import resolve_collection_algo
from bsimvis.app.services.pool_service import PoolService
from bsimvis.app.services.similarity_service import (
    BUILDABLE_ALGOS,
    assert_buildable_algo,
)
from bsimvis.similarity import registry


class RedisHash:
    def __init__(self):
        self.data = {}

    def hget(self, key, name):
        return self.data.get((key, name))

    def hset(self, key, name, value):
        self.data[(key, name)] = str(value).encode()

    def hgetall(self, key):
        return {
            name: value
            for (stored_key, name), value in self.data.items()
            if stored_key == key
        }


def check():
    r = RedisHash()
    app = Flask(__name__)
    with (
        patch("bsimvis.app.services.collection_config.get_redis", return_value=r),
        patch.object(
            config_service.config_service,
            "get",
            side_effect=lambda k, d=None: "jaccard" if k == "similarity.algo" else d,
        ),
        app.test_request_context(
            "/", method="POST", json={"collection": "alpha", "algo": "binary_cosine"}
        ),
    ):
        response = set_collection_config()
        assert response["updated"] == {"algo": "binary_cosine"}
        assert resolve_collection_algo("alpha", "jaccard") == "binary_cosine"
        assert resolve_collection_algo("beta", "jaccard") == "jaccard"
        assert resolve_collection_algo("beta") == "jaccard"
        assert (
            PoolService.similarity_algo(
                {"func_sim_params": {"algo": "weighted_cosine"}, "algo": "jaccard"}
            )
            == "weighted_cosine"
        )
        assert PoolService.similarity_algo({"algo": "jaccard"}) == "jaccard"
        assert PoolService.similarity_algo({}) == "jaccard"

        for algo in BUILDABLE_ALGOS:
            assert_buildable_algo(algo)
        assert set(BUILDABLE_ALGOS) == set(registry.names(buildable=True))
        try:
            from bsimvis.app.services.collection_config import set_collection_param

            set_collection_param("alpha", "algo", "not-an-algorithm")
        except ValueError:
            pass
        else:
            raise AssertionError("invalid collection algorithm was accepted")


if __name__ == "__main__":
    check()
    print("OK")
