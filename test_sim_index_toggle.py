"""Skip-toggle check for cluster -> similarity index propagation."""

from unittest.mock import MagicMock, patch

from bsimvis.app.services.cluster_service import ClusterService
from bsimvis.app.services.config_service import config_service


def _run(enabled):
    svc = ClusterService.__new__(ClusterService)
    svc.r = MagicMock()
    svc.r.smembers.return_value = set()
    svc.r.scan.return_value = (0, [])
    svc.r.zcard.return_value = 0
    svc.get_propagated_fields = lambda lvl: {"func": []}
    svc.get_native_fields = lambda lvl, native: []
    with patch.object(
        config_service,
        "get",
        side_effect=lambda k, d=None: enabled
        if k == "clustering.propagate_sim_indexes"
        else d,
    ):
        assert svc._update_similarity_indexing("col", "unweighted_cosine") is True
    return svc.r


def test_toggle():
    # Disabled: no writes at all beyond the (skipped) build path.
    r_off = _run(False)
    assert not r_off.delete.called, "disabled propagation must not touch indexes"

    # Enabled: build path runs and wipes the best_cluster hash.
    r_on = _run(True)
    assert (
        "col:sim:best_cluster:unweighted_cosine" in
        [c.args[0] for c in r_on.delete.call_args_list]
    ), "enabled propagation must wipe stale best_cluster index"


if __name__ == "__main__":
    test_toggle()
    print("ok")
