"""Compile and run the YARA rulesets against a sample.

Two rulesets, handled differently because their sizes differ by two orders of
magnitude:

  * **Vendored** -- `data/yara_rules/`, 589 reviewed files in git. Compiled from
    source and cached for the life of the process: 0.46s once, ~12 MB resident.
    Mirrors unpack_service.capa_path(): an env var override, else a vendored
    default, so a deployment can point at its own ruleset without a code change.
  * **Mirror** -- `data/rulezet/`, up to ~130k rules pulled from rulezet.org by
    `rulezet_service`. Compiled once at sync time into `rules.compiled` and only
    *loaded* here, because compiling it per worker would cost ~55s. It is
    deliberately **not** cached: measured at +330 MB resident for the full
    mirror, against a per-worker budget of 2.5 GB that a 1536 MB JVM heap
    already dominates (launch.sh:91). Loading costs ~0.8s and a scan ~0.04s on a
    typical sample, against a Ghidra job measured in minutes -- so load, scan,
    free, and the ruleset never sits next to the JVM's high-water mark.

Neither is fatal by its absence: a missing ruleset behaves exactly like capa not
being installed -- skip the tags, do not fail the job.
"""

import json
import os
from pathlib import Path

# capa_path()/upx_path() resolve `parents[3]` from unpack_service.py, which
# lives in this same directory, to the repo root.
_DEFAULT_RULES_DIR = Path(__file__).resolve().parents[3] / "data" / "yara_rules"

_compiled = None
_compiled_dir = None


def rules_dir():
    """Directory to load `.yar`/`.yara` files from, recursively."""
    explicit = os.environ.get("YARA_RULES_DIR")
    return Path(explicit) if explicit else _DEFAULT_RULES_DIR


def _rule_files(base):
    if not base.is_dir():
        return []
    return sorted(base.rglob("*.yar")) + sorted(base.rglob("*.yara"))


def compiled_rules():
    """The compiled vendored ruleset, cached for the life of the process.

    Returns None when no rule files are found (ruleset not vendored, or
    `YARA_RULES_DIR` points somewhere empty) -- callers treat that exactly like
    capa not being installed: skip the tags, do not fail the job.
    """
    global _compiled, _compiled_dir
    base = rules_dir()
    if _compiled is not None and _compiled_dir == base:
        return _compiled

    files = _rule_files(base)
    if not files:
        return None

    import yara

    # yara.compile's `filepaths` is namespace -> path; the namespace only has
    # to be unique among the inputs, and the path already is -- which keeps two
    # rule files that define a same-named rule from colliding *and* leaves
    # `match.namespace` naming the file the rule came from, which is the only
    # record of that a match carries (`tag_provenance._match_record`).
    filepaths = {str(p): str(p) for p in files}
    _compiled = yara.compile(filepaths=filepaths)
    _compiled_dir = base
    return _compiled


# Kept as an alias so `rulezet_service.report_vendored()` reads as what it is --
# gating the vendored set, not whatever `compiled_rules()` happens to mean.
vendored_rules = compiled_rules


def mirror_tags():
    """`{rule uuid: [tag, ...]}` for the mirror, or `{}` when there is none.

    Small enough (a few MB) to read per scan alongside the ruleset it belongs
    to, and reading both together is what stops them drifting apart.
    """
    from bsimvis.app.services.rulezet_service import paths

    p = paths()["tags"]
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (ValueError, OSError):
        return {}


def scan_file(path, vendored=True, mirror=True):
    """YARA matches for one file, or `[]` when no ruleset is loaded.

    Returns `(matches, extra_tags)`: the mirror's tags cannot be read off a
    match the way the vendored set's are (they live in a sidecar keyed by the
    uuid YARA carries as the match namespace), so they travel alongside for
    `tag_taxonomy.yara_file_tags`/`yara_rule_hits` to fold in.

    The two rulesets are selectable independently because they cost
    differently: the vendored set is cached and effectively free after the
    first call, the mirror pays ~0.8s and ~330 MB on every single scan.
    """
    matches = []
    rules = compiled_rules() if vendored else None
    if rules is not None:
        matches.extend(rules.match(filepath=path))

    if not mirror:
        return matches, {}

    from bsimvis.app.services.rulezet_service import paths

    blob = paths()["compiled"]
    if not blob.exists():
        return matches, {}

    import yara

    # Loaded and dropped inside this call on purpose -- see the module docstring.
    # `mirror` going out of scope frees the ~330 MB before the caller does
    # anything else with the process.
    before = len(matches)
    try:
        mirror = yara.load(str(blob))
        matches.extend(mirror.match(filepath=path))
    except yara.Error:
        # A ruleset saved by a different yara-python version cannot be loaded.
        # That is a stale build artifact, not a reason to fail an ingest.
        return matches, {}
    finally:
        mirror = None

    # The sidecar is only worth reading when something in the mirror actually
    # matched, which for most files is never.
    return matches, (mirror_tags() if len(matches) > before else {})


def demo():
    """Prove the sidecar join against real YARA, not stand-in objects.

    `tag_taxonomy.demo()` checks the same join with hand-built match objects.
    This is the half that only fails for real: that a mirrored rule compiled
    with its uuid as the YARA namespace comes back out of `scan_file()` with
    that uuid in `match.namespace`, so the sidecar lookup hits.
    """
    import tempfile

    import yara

    from bsimvis.app.services import rulezet_service
    from bsimvis.app.services.tag_taxonomy import yara_file_tags

    uuid = "11111111-2222-3333-4444-555555555555"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "rules").mkdir()
        (root / "rules" / f"{uuid}.yara").write_text(
            "rule Sidecar_Canary {\n"
            '  meta:\n    category = "trojan"\n    malware = "canary"\n'
            '  strings:\n    $a = "bsimvis-sidecar-canary"\n'
            "  condition:\n    $a\n}\n"
        )
        (root / "tags.json").write_text(
            json.dumps({uuid: ["mitre:t1027", "cve:cve-2021-44228"]})
        )
        yara.compile(filepaths={uuid: str(root / "rules" / f"{uuid}.yara")}).save(
            str(root / "rules.compiled")
        )
        sample = root / "sample.bin"
        sample.write_bytes(b"\x7fELF padding bsimvis-sidecar-canary padding")

        original = rulezet_service.mirror_dir
        rulezet_service.mirror_dir = lambda: root
        try:
            matches, extra = scan_file(str(sample))
            mine = [m for m in matches if m.rule == "Sidecar_Canary"]
            assert mine, "mirrored rule did not match"
            assert mine[0].namespace == uuid, mine[0].namespace
            tags = yara_file_tags(matches, extra)
            assert "yara:trojan:canary#Sidecar_Canary" in tags, tags
            assert {"mitre:t1027", "cve:cve-2021-44228"} <= tags, tags

            # The two rulesets are separately switchable -- an upload can ask
            # for the cheap vendored set without paying the mirror's load.
            only_vendored, extra_v = scan_file(str(sample), mirror=False)
            assert not [m for m in only_vendored if m.rule == "Sidecar_Canary"]
            assert extra_v == {}, extra_v

            only_mirror, extra_m = scan_file(str(sample), vendored=False)
            assert [m for m in only_mirror if m.rule == "Sidecar_Canary"]
            assert extra_m, "mirror tags lost when the vendored set is off"
        finally:
            rulezet_service.mirror_dir = original

    # No mirror on disk -> still a (matches, tags) pair, never a bare list.
    matches, extra = scan_file("/bin/ls") if os.path.exists("/bin/ls") else ([], {})
    assert isinstance(matches, list) and isinstance(extra, dict)
    print("yara_service demo OK")


if __name__ == "__main__":
    demo()
