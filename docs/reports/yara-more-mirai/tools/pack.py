"""Concatenate rules/*.yar into pack/more_mirai.yar, check it compiles as one unit
(duplicate names fail), and print an index of kind/family/rule counts."""

import collections
import glob
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import yc  # noqa: E402
from matrix import ROOT, meta_of, split_rules  # noqa: E402


def main():
    blocks = []
    for p in sorted(glob.glob(os.path.join(ROOT, "rules", "*.yar"))):
        blocks += [b.strip() for b in split_rules(open(p).read())]
    names = [re.search(r"rule\s+(\w+)", b).group(1) for b in blocks]
    dup = [n for n, c in collections.Counter(names).items() if c > 1]
    assert not dup, f"duplicate rule names: {dup}"
    text = "\n\n".join(blocks) + "\n"
    rep = yc.call("/yara/validate", {"rule": text})
    assert rep.get("ok"), rep
    os.makedirs(os.path.join(ROOT, "pack"), exist_ok=True)
    open(os.path.join(ROOT, "pack", "more_mirai.yar"), "w").write(text)
    kinds = collections.Counter(meta_of(b).get("kind") for b in blocks)
    print(len(blocks), "rules", dict(kinds))


if __name__ == "__main__":
    main()
