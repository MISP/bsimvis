"""Self-check for bin_sim file-tag filtering.

bin_sim's file_tags_* buckets are a build-time snapshot, so tag filters must
resolve through the live file tag index instead.
Run: python3 test_bin_sim_file_tags.py
"""

from bsimvis.app.routes.search_bin_sim import _file_tag_union


class P:
    def __init__(s, r):
        s.r, s.q = r, []

    def smembers(s, k):
        s.q.append(k)
        return s.r.data.get(k, set())

    def execute(s):
        out = [s.r.data.get(k, set()) for k in s.q]
        s.q = []
        return out


class R:
    def __init__(s, data):
        s.data = data

    def sscan_iter(s, key, match=None, count=None):
        import fnmatch

        return [m for m in s.data.get(key, set()) if fnmatch.fnmatch(m, match)]

    def pipeline(s, transaction=False):
        return P(s)

    def smembers(s, k):
        return s.data.get(k, set())


# collection: tag added after build -> file index is current, bin_sim buckets are not
r = R(
    {
        "mirai:reg:file:user_tags": {"mirai:idx:file:user_tags:evil"},
        "mirai:idx:file:user_tags:evil": {"mirai:file:aa"},
        "mirai:bin_sim:involves:aa": {"mirai:bin_sim:uc:aa::bb"},
    }
)
assert _file_tag_union(r, "mirai", "evil") == {"mirai:bin_sim:uc:aa::bb"}
assert _file_tag_union(r, "mirai", "nope") == set()
# static-only field selection must not see a user tag
assert _file_tag_union(r, "mirai", "evil", fields=("tags",)) == set()

# pool: shared file ids keep origin collection; involves keys are coll-qualified
rp = R(
    {
        "global:pool:7:reg:file:user_tags": {"global:pool:7:idx:file:user_tags:evil"},
        "global:pool:7:idx:file:user_tags:evil": {"mirai:file:aa"},
        "global:pool:7:bin_sim:involves:mirai:aa": {
            "global:pool:7:bin_sim:uc:mirai:aa::g:bb"
        },
    }
)
assert _file_tag_union(rp, "global:pool:7", "evil", is_pool=True) == {
    "global:pool:7:bin_sim:uc:mirai:aa::g:bb"
}
# without is_pool the un-qualified key misses -> proves the pool form is required
assert _file_tag_union(rp, "global:pool:7", "evil") == set()
print("_file_tag_union: all checks passed")
