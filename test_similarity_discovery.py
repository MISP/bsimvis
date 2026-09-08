from collections import OrderedDict
from math import sqrt

from bsimvis.app.services.similarity_service import SimilarityService


class FakePipeline:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def zcard(self, key):
        self.commands.append(("zcard", key))

    def zrange(self, key, start, stop, withscores=False):
        self.commands.append(("zrange", key, start, stop, withscores))

    def execute(self):
        self.redis.pipeline_executes += 1
        return [1 if command[0] == "zcard" else [] for command in self.commands]


class FakeRedis:
    def __init__(self):
        self.pipeline_executes = 0

    def pipeline(self, transaction=False):
        return FakePipeline(self)


def test_discovery_batches_posting_list_misses():
    service = object.__new__(SimilarityService)
    service.r = FakeRedis()
    service._pl_cache = OrderedDict()
    service._pl_pairs = 0
    service._pl_budget = 100
    service._count_cache = {}
    service._norm_cache = {}

    service._discover_find(
        [
            "target",
            "collection",
            "unweighted_cosine",
            0.5,
            2,
            sqrt(2),
            1000,
            0,
            "feature-a",
            1,
            "feature-b",
            1,
        ]
    )

    assert service.r.pipeline_executes == 2


def test_discovery_limits_prefetch_before_pruning():
    service = object.__new__(SimilarityService)
    service.r = FakeRedis()
    service._pl_cache = OrderedDict()
    service._pl_pairs = 0
    service._pl_budget = 100
    service._count_cache = {}
    service._norm_cache = {}

    features = [value for i in range(32) for value in (f"feature-{i}", 1)]
    service._discover_find(
        ["target", "collection", "unweighted_cosine", 0.9, 32, sqrt(32), 1000, 0]
        + features
    )

    assert service.r.pipeline_executes == 2
    assert len(service._pl_cache) == 16
