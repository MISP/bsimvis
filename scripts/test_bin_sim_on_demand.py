#!/usr/bin/env python3
"""Focused check: one unstored file pair is rebuilt from stored function sims."""

import json

from bsimvis.app.services.bin_sim_service import BinSimService


class Pipe:
    def __init__(self, redis):
        self.redis, self.keys = redis, []

    def get(self, key):
        self.keys.append(key)

    def execute(self):
        return [self.redis.docs.get(key) for key in self.keys]


class Redis:
    def __init__(self):
        self.fid_a = "main:func:aaa:1000"
        self.fid_b = "main:func:bbb:2000"
        self.docs = {
            "main:sim:unweighted_cosine:bbb:2000::aaa:1000": json.dumps(
                {"id1": self.fid_a, "id2": self.fid_b, "score": 0.95}
            ),
            f"{self.fid_a}:meta": json.dumps({"bsim_features_count": 10}),
            f"{self.fid_b}:meta": json.dumps({"bsim_features_count": 10}),
            "main:file:aaa:meta": json.dumps({"language_id": "x86"}),
            "main:file:bbb:meta": json.dumps({"language_id": "arm"}),
        }

    def smembers(self, key):
        return {self.fid_a} if key.endswith(":aaa") else {self.fid_b}

    def sinter(self, *_):
        return {"main:sim:unweighted_cosine:bbb:2000::aaa:1000"}

    def pipeline(self, transaction=False):
        return Pipe(self)

    def get(self, key):
        return None


class PoolRedis(Redis):
    def __init__(self):
        self.fid_a = "alpha:func:aaa:1000"
        self.fid_b = "beta:func:bbb:2000"
        self.docs = {
            "global:pool:p1:sim:beta:func:bbb:2000::alpha:func:aaa:1000": json.dumps(
                {
                    "id1": self.fid_a,
                    "id2": self.fid_b,
                    "coll_1": "alpha",
                    "md5_1": "aaa",
                    "coll_2": "beta",
                    "md5_2": "bbb",
                    "score": 0.91,
                }
            ),
            f"{self.fid_a}:meta": json.dumps({"bsim_features_count": 10}),
            f"{self.fid_b}:meta": json.dumps({"bsim_features_count": 10}),
            "alpha:file:aaa:meta": json.dumps({"language_id": "x86"}),
            "beta:file:bbb:meta": json.dumps({"language_id": "arm"}),
        }

    def smembers(self, key):
        return {self.fid_a} if key.endswith(":alpha:aaa") else {self.fid_b}

    def sinter(self, *_):
        return {"global:pool:p1:sim:beta:func:bbb:2000::alpha:func:aaa:1000"}


if __name__ == "__main__":
    redis = Redis()
    doc = BinSimService(redis).cached_pair_from_stored_sims(
        "main", "bbb", "aaa", "unweighted_cosine"
    )
    assert (doc["md5_a"], doc["md5_b"]) == ("aaa", "bbb")
    match = doc["diff"]["matched"][0]
    assert (match["func_a"], match["func_b"], match["similarity"]) == (
        redis.fid_a,
        redis.fid_b,
        0.95,
    )
    pool_redis = PoolRedis()
    pool_doc = BinSimService(pool_redis).cached_pair_from_stored_sims(
        "global:pool:p1:col:alpha",
        "aaa",
        "bbb",
        "unweighted_cosine",
        "beta",
        "p1",
    )
    assert (pool_doc["coll_a"], pool_doc["coll_b"]) == ("alpha", "beta")
    assert pool_doc["diff"]["matched"][0]["similarity"] == 0.91
    assert (
        BinSimService(pool_redis)
        .on_demand_pair_sid(
            "global:pool:p1:col:beta",
            "bbb",
            "aaa",
            "unweighted_cosine",
            "global:pool:p1:col:alpha",
            "p1",
        )
        .endswith("alpha:aaa::beta:bbb")
    )
    print("on-demand bin-sim passed")
