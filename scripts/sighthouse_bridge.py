#!/usr/bin/env python3
"""Bridge Sighthouse's compiled library corpus into a BSimVis reference collection.

Sighthouse (github.com/quarkslab/sighthouse, run locally as the `testing-pipeline`
compose stack) scrapes library sources, compiles them under several compiler
variants, and analyses the result with Ghidra. Every job that reaches the
"Ghidra Analyzer" success state leaves behind:

  success/Ghidra Analyzer/<uuid>.json   the job record, carrying the library
                                        name, version and compiler variant
  artifacts/<sha>.tar.gz                the compiled build tree it consumed

This script pulls those artifacts out of Sighthouse's S3 repo, keeps the object
files and shared libraries, and lays them out on disk grouped by
(library, version, variant). Each group is then uploaded to BSimVis as one
`bsimvis-upload` run so the group's tags land on every file in it.

The upload always passes --skip-sim: a reference collection is only ever
compared *against* other collections (via a pool with only_cross_collection),
so intra-collection similarities are wasted work.

Usage:
    # extract only, show what would be uploaded
    python scripts/sighthouse_bridge.py --out /tmp/refcorpus

    # extract and upload to a running BSimVis
    python scripts/sighthouse_bridge.py --out /tmp/refcorpus \\
        --upload -H 127.0.0.1:5000 -c stdlib-ref

Requires boto3 and Sighthouse's `sighthouse.core.utils.repo` on sys.path
(--sighthouse-src points at its checkout).
"""

import argparse
import csv
import hashlib
import io
import json
import logging
import posixpath
import re
import subprocess
import sys
import tarfile
from collections import defaultdict
from pathlib import Path

DEFAULT_REPO = "s3://admin:password@127.0.0.1:9000/uploads"
DEFAULT_SIGHTHOUSE_SRC = "/home/thomas/github/sighthouse/sighthouse-core/src"
ANALYZER_DIR = "success/Ghidra Analyzer"

# Build-tree paths that hold test harnesses, fuzzers and demo programs rather
# than library code. Their functions are not what we want to match malware
# against, and openssl alone contributes a few hundred of them.
NOISE_DIRS = ("test/", "tests/", "fuzz/", "doc/", "demos/", "examples/", "benchmark/")


def is_wanted(name):
    """True for a build product worth putting in the reference collection.

    Keeps object files and shared libraries. Only openssl and glibc emit .so in
    this corpus, so dropping .o would silently lose zlib/zstd/lz4/upx/mbedtls
    entirely -- .o is the primary payload here, not a fallback.
    """
    if any(part in name for part in NOISE_DIRS):
        return False
    return name.endswith(".o") or ".so" in posixpath.basename(name)


def variant_of(job_data):
    """Compiler variant string, e.g. 'x86_64-O2', from a job record.

    job_data["metadata"] looks like [["openssl", "openssl-3.0.15-x86_64-O2"]] --
    the variant is that second value with the version prefix stripped.
    """
    version = job_data.get("version") or ""
    for entry in job_data.get("metadata") or []:
        if len(entry) >= 2 and entry[1]:
            tail = entry[1]
            if version and tail.startswith(version + "-"):
                return tail[len(version) + 1 :]
            return tail
    return "unknown"


def slug(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "unknown")).strip("_")


def load_repo(uri, sighthouse_src):
    if sighthouse_src not in sys.path:
        sys.path.insert(0, sighthouse_src)
    try:
        from sighthouse.core.utils.repo import Repo
    except ImportError as exc:
        raise SystemExit(
            f"cannot import Sighthouse's Repo from {sighthouse_src}: {exc}\n"
            "point --sighthouse-src at the sighthouse-core/src checkout, and make "
            "sure boto3 and requests are installed"
        )
    # secure=False: the local rustfs speaks plain HTTP.
    return Repo(uri, secure=False)


def collect_groups(repo, limit=0):
    """Read the analyzer job records, grouped by (library, version, variant).

    The same artifact can be referenced by more than one record (retries, or a
    variant that produced an identical tree), so artifacts are de-duplicated by
    their tarball name.
    """
    groups = defaultdict(set)
    records = repo.list_directory(ANALYZER_DIR)
    logging.info("[i] %d analyzer job records", len(records))

    for record in records:
        raw = repo.get_file(record)
        if not raw:
            logging.warning("[!] unreadable record %s", record)
            continue
        job_data = json.loads(raw).get("job_data", {})
        artifact = job_data.get("file")
        if not artifact:
            continue
        key = (
            job_data.get("name") or "unknown",
            job_data.get("version") or "unknown",
            variant_of(job_data),
        )
        groups[key].add(artifact)

    if limit:
        groups = dict(list(groups.items())[:limit])
    return groups


def extract_group(repo, key, artifacts, out_root):
    """Unpack one group's artifacts to disk. Returns [(path, original_name)]."""
    library, version, variant = key
    group_dir = out_root / f"{slug(library)}__{slug(version)}__{slug(variant)}"
    group_dir.mkdir(parents=True, exist_ok=True)

    extracted = []
    for artifact in sorted(artifacts):
        raw = repo.get_file(f"artifacts/{artifact}")
        if not raw:
            logging.warning("[!] missing artifact %s", artifact)
            continue
        with tarfile.open(fileobj=io.BytesIO(raw)) as tar:
            for member in tar.getmembers():
                if not member.isfile() or not is_wanted(member.name):
                    continue
                payload = tar.extractfile(member)
                if payload is None:
                    continue
                data = payload.read()
                # Flatten: the build-tree path becomes the filename, so a name
                # collision between two artifacts in the same group cannot
                # silently overwrite, and the origin stays readable in the UI.
                flat = slug(member.name)
                target = group_dir / flat
                if target.exists() and target.read_bytes() != data:
                    flat = f"{hashlib.md5(data).hexdigest()[:8]}_{flat}"
                    target = group_dir / flat
                target.write_bytes(data)
                extracted.append((target, member.name))

    logging.info(
        "[+] %s %s %s -> %d files", library, version, variant, len(extracted)
    )
    return group_dir, extracted


def write_metadata_csv(rows, path):
    """Pipe-delimited CSV keyed by MD5, as bsimvis-upload --metadata expects.

    Only `names` is meaningful for us -- it carries the original build-tree path
    so the UI shows `crypto/aes/libcrypto-lib-aes_core.o` instead of the
    flattened on-disk name. The other columns exist because the parser is
    malware-shaped; they stay empty.
    """
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="|")
        writer.writerow(["HASH", "names", "filetype"])
        for md5, original in rows:
            writer.writerow([md5, original, "object"])


def upload_group(group_dir, key, args, metadata_csv):
    library, version, variant = key
    tags = [
        "stdlib",
        f"lib:{library}",
        f"ver:{version}",
        f"variant:{variant}",
    ]
    cmd = [
        "bsimvis-upload",
        str(group_dir),
        "-c",
        args.collection,
        # A reference collection is only compared across collections, so
        # building similarities inside it is wasted work.
        "--skip-sim",
        "--metadata",
        str(metadata_csv),
        "-n",
        str(args.threads),
    ]
    for host in args.hosts:
        cmd += ["-H", host]
    for tag in tags:
        cmd += ["-t", tag]

    if not args.upload:
        print("  " + " ".join(f"'{c}'" if " " in c else c for c in cmd))
        return True

    logging.info("[*] uploading %s", group_dir.name)
    result = subprocess.run(cmd)
    if result.returncode != 0:
        logging.error("[!] upload failed for %s (rc=%d)", group_dir.name, result.returncode)
        return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", default=DEFAULT_REPO, help="Sighthouse S3 repo URI")
    parser.add_argument("--sighthouse-src", default=DEFAULT_SIGHTHOUSE_SRC)
    parser.add_argument("--out", required=True, help="directory to extract into")
    parser.add_argument("-c", "--collection", default="stdlib-ref")
    parser.add_argument("-H", "--host", dest="hosts", action="append", default=[])
    parser.add_argument("-n", "--threads", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0, help="only process N groups")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="actually run bsimvis-upload (default: print the commands)",
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING - 10 * min(args.verbose, 2),
        format="%(message)s",
    )
    if args.upload and not args.hosts:
        parser.error("--upload needs at least one -H host")

    out_root = Path(args.out)
    out_root.mkdir(parents=True, exist_ok=True)

    repo = load_repo(args.repo, args.sighthouse_src)
    groups = collect_groups(repo, args.limit)
    print(f"[i] {len(groups)} groups to bridge")

    failures = 0
    total_files = 0
    for key in sorted(groups):
        group_dir, extracted = extract_group(repo, key, groups[key], out_root)
        if not extracted:
            logging.warning("[!] nothing extracted for %s, skipping", key)
            continue
        total_files += len(extracted)

        # Not with_suffix(): version strings contain dots, so it would truncate
        # "libc__mbedtls-3.0.0" to "libc__mbedtls-3.0.metadata.csv" and let two
        # versions collide on one file.
        metadata_csv = group_dir.parent / (group_dir.name + ".metadata.csv")
        write_metadata_csv(
            [
                (hashlib.md5(path.read_bytes()).hexdigest(), original)
                for path, original in extracted
            ],
            metadata_csv,
        )
        if not upload_group(group_dir, key, args, metadata_csv):
            failures += 1

    verb = "uploaded" if args.upload else "prepared"
    print(f"[i] {verb} {total_files} files across {len(groups)} groups into {out_root}")
    if failures:
        print(f"[!] {failures} group(s) failed")
        return 1
    return 0


def _selftest():
    """Checks on the two bits of real logic: selection and variant parsing."""
    assert is_wanted("crypto/aes/libcrypto-lib-aes_core.o")
    assert is_wanted("engines/afalg.so")
    assert is_wanted("lib/libcrypto.so.3")
    assert not is_wanted("test/helpers/ssl_test-bin-handshake.o"), "test code excluded"
    assert not is_wanted("fuzz/x509.o")
    assert not is_wanted("README.md")
    assert not is_wanted("src/packer.h")

    assert variant_of(
        {"version": "openssl-3.0.15", "metadata": [["openssl", "openssl-3.0.15-x86_64-O2"]]}
    ) == "x86_64-O2"
    assert variant_of(
        {"version": "v1.9.4", "metadata": [["lz4", "v1.9.4-x86_64-O1"]]}
    ) == "x86_64-O1"
    # No metadata at all: compiler-stage records look like this.
    assert variant_of({"version": "v4.0.2", "metadata": []}) == "unknown"
    assert variant_of({}) == "unknown"

    assert slug("crypto/aes/aes_core.o") == "crypto_aes_aes_core.o"
    print("selftest ok")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        sys.exit(main())
