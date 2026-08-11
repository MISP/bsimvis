"""Compile and run the vendored YARA ruleset against a sample.

Mirrors unpack_service.capa_path(): an env var override, else a vendored
default, so a deployment can point at its own ruleset without a code change.
Unlike capa this is an in-process library, not a subprocess -- compiling the
~300 vendored rules takes well under a second, so a cached compile per worker
process is enough; there is no reason to shell out or write a project file for
it.
"""

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
    """The compiled ruleset, cached for the life of the process.

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
    # to be unique among the inputs, so the file's own index keeps two rule
    # files that happen to define a same-named rule from colliding.
    filepaths = {str(i): str(p) for i, p in enumerate(files)}
    _compiled = yara.compile(filepaths=filepaths)
    _compiled_dir = base
    return _compiled


def scan_file(path):
    """YARA matches for one file on disk, or `[]` when no ruleset is loaded."""
    rules = compiled_rules()
    if rules is None:
        return []
    return rules.match(filepath=path)
