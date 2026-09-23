from bsimvis.app.services.similarity_axis_index import (
    pair_axis,
    ready_key,
    score_axis_key,
)

assert pair_axis({}, {}) == "code"
assert pair_axis({"tags": ["fid:libc"]}, {}) == "library"
assert pair_axis({}, {"user_tags": ["origin:lib:uclibc"]}) == "library"
assert pair_axis({"tags": ["malware:sample"]}, {}) == "code"

assert (
    score_axis_key("main", "code", "unweighted_cosine")
    == "main:sim:score_axis:code:unweighted_cosine"
)
assert ready_key("global:pool:p1") == "global:pool:p1:sim:score_axes_ready"
