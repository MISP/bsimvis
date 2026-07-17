"""Self-check for pool -> origin-collection resolution.

Tags and notes always index back to the origin collection, never the pool, so
resolve_origin_collection is the single point where that rule is enforced.
Run: python3 test_origin_collection.py
"""

from bsimvis.app.services.index_service import resolve_origin_collection


class FakeRedis:
    """Only smembers is used by the resolver."""

    def __init__(self, members):
        self.members = members

    def smembers(self, key):
        return self.members.get(key, set())


def demo():
    r = FakeRedis({"global:pool:7:collections_list": {"mirai", "gafgyt"}})

    # A pool namespace resolves to the member collection owning the entity.
    assert (
        resolve_origin_collection("global:pool:7", "mirai:func:abc123:0x401000", r)
        == "mirai"
    )
    assert (
        resolve_origin_collection("global:pool:7", "gafgyt:file:deadbeef:meta", r)
        == "gafgyt"
    )

    # A collection with a name that collides with a key segment ("func") must
    # still resolve by membership, not by position or a blocklist.
    r2 = FakeRedis({"global:pool:9:collections_list": {"func"}})
    assert resolve_origin_collection("global:pool:9", "func:func:aa:0x1", r2) == "func"

    # Explicit :col: marker wins without touching redis.
    assert (
        resolve_origin_collection("global:pool:7:col:mirai", "anything", None) == "mirai"
    )

    # A real collection passes through untouched.
    assert resolve_origin_collection("mirai", "mirai:func:abc:0x1", r) == "mirai"

    # Unresolvable: entity does not belong to any member. Returns the pool
    # namespace unchanged — same as before, so callers behave as they did.
    assert (
        resolve_origin_collection("global:pool:7", "unknown:func:abc:0x1", r)
        == "global:pool:7"
    )

    # No entity id to resolve against (tag metadata reads) stays pool-scoped,
    # which is what the pool tags_metadata mirror expects.
    assert resolve_origin_collection("global:pool:7", None, r) == "global:pool:7"

    print("resolve_origin_collection: all checks passed")


if __name__ == "__main__":
    demo()
