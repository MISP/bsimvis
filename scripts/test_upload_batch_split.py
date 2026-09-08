#!/usr/bin/env python3
"""Checks `bsimvis upload --batch-split N` chunking. No server needed:
upload_and_finalize is stubbed, so this only asserts what main() decides --
how targets are cut, which batch_uuid each part gets, and that batch_order
stays unique across the whole run."""

import argparse

from bsimvis.cli import bsimvis_upload as up

calls = []


def _stub(targets, args, config, order_offset=0):
    calls.append((list(targets), args.batch_uuid, args.batch_name, order_offset))
    return len(targets), 0, 0


def run(n_targets, split, limit=0):
    calls.clear()
    args = argparse.Namespace(
        local_analysis=False,
        config="bsimvis_config.toml",
        collections=["main"],
        batch_uuid="base",
        batch_name="Ghidra Batch",
        batch_split=split,
        metadata=None,
        hosts=["localhost:5000"],
        profile="p",
        limit=limit,
        threads=1,
        targets=[f"f{i}" for i in range(n_targets)],
    )
    assert up.main(args) == 0
    return list(calls)


def main():
    up.upload_and_finalize = _stub

    # 80 files split by 20 -> 4 independent batches, each with its own uuid.
    c = run(80, 20)
    assert [len(t) for t, *_ in c] == [20, 20, 20, 20], c
    assert [u for _, u, _, _ in c] == ["base-1", "base-2", "base-3", "base-4"], c
    # batch_order is an indexed field, so it must not restart per part.
    assert [o for *_, o in c] == [0, 20, 40, 60], c
    # Every target uploaded exactly once, in order.
    assert [f for t, *_ in c for f in t] == [f"f{i}" for i in range(80)], c

    # Ragged tail.
    assert [len(t) for t, *_ in run(50, 20)] == [20, 20, 10]

    # Split off -> one batch, uuid and name untouched.
    c = run(80, 0)
    assert len(c) == 1 and c[0][1] == "base" and c[0][2] == "Ghidra Batch", c

    # --limit still applies before the split.
    assert [len(t) for t, *_ in run(80, 20, limit=30)] == [20, 10]

    print("OK")


if __name__ == "__main__":
    main()
