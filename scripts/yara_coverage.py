#!/usr/bin/env python3
"""How many samples in a directory does the vendored YARA ruleset tag?

    python scripts/yara_coverage.py <sample_dir> [--rules DIR] [--fp DIR ...]

Loads rules exactly the way `bsimvis.app.services.yara_service` does (recursive,
one namespace per file) so the number here is the number the worker would get.

A sample counts as tagged when a rule fires that names a family or behaviour;
file-format and packer rules are excluded, since those fire on benign ELFs too
and say nothing about what the sample is. `--fp` scans a directory of files that
are expected NOT to match, and reports whatever does.
"""

import argparse
import collections
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from bsimvis.app.services.yara_service import rules_dir  # noqa: E402

# Not family attribution: a packed or malformed benign ELF trips these too.
NOT_ATTRIBUTION = ("SUSP_", "elf_", "ELF_anomal", "INDICATOR_")


def compile_rules(base):
    import yara

    files = sorted(base.rglob("*.yar")) + sorted(base.rglob("*.yara"))
    if not files:
        sys.exit(f"no rule files under {base}")
    return len(files), yara.compile(
        filepaths={str(i): str(p) for i, p in enumerate(files)}
    )


def scan(rules, directory):
    """-> (files scanned, {sample: [attribution rules]})"""
    out = {}
    for p in sorted(pathlib.Path(directory).iterdir()):
        if not p.is_file():
            continue
        hits = [m.rule for m in rules.match(filepath=str(p), timeout=120)]
        out[p.name] = [h for h in hits if not h.startswith(NOT_ATTRIBUTION)]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sample_dir")
    ap.add_argument("--rules", default=None, help="default: the vendored ruleset")
    ap.add_argument(
        "--fp",
        action="append",
        default=[],
        help="directory of benign files; repeatable",
    )
    args = ap.parse_args()

    base = pathlib.Path(args.rules) if args.rules else rules_dir()
    n_files, rules = compile_rules(base)
    print(f"{n_files} rule files from {base}")

    hits = scan(rules, args.sample_dir)
    tagged = {k: v for k, v in hits.items() if v}
    print(
        f"tagged {len(tagged)}/{len(hits)} "
        f"({len(tagged) / max(len(hits), 1) * 100:.1f}%)"
    )
    counts = collections.Counter(r for v in tagged.values() for r in v)
    for rule, c in counts.most_common(15):
        print(f"  {c:5d}  {rule}")

    for d in args.fp:
        fp = {k: v for k, v in scan(rules, d).items() if v}
        print(f"false positives in {d}: {len(fp)}")
        for k, v in list(fp.items())[:20]:
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
