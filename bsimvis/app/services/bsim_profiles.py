"""BSim signature-settings profiles.

A profile pairs a decompiler signature-settings mask with the weights table that
belongs to it. The two cannot be derived from each other: four of the five weight
tables Ghidra ships declare `settings="0x49"`, so the mask alone does not identify
a file. Ghidra resolves the pairing through database template configs
(``data/medium_nosize.xml`` and friends); we make it explicit in config instead.

See ``doc/bsim_signature_settings.md`` for what the mask bits mean.
"""

import logging
import os
from pathlib import Path
from xml.etree import ElementTree

from bsimvis.app.services.config_service import config_service

# Signature modifier flags, from Ghidra's GraphSigManager::Mods
# (Features/Decompiler/src/decompile/cpp/signature.hh:269-274).
SIG_COLLAPSE_SIZE = 0x1
SIG_COLLAPSE_INDNOISE = 0x2
SIG_DONOTUSE_CONST = 0x10
SIG_DONOTUSE_INPUT = 0x20
SIG_DONOTUSE_PERSIST = 0x40

FLAG_NAMES = {
    SIG_COLLAPSE_SIZE: "SIG_COLLAPSE_SIZE",
    SIG_COLLAPSE_INDNOISE: "SIG_COLLAPSE_INDNOISE",
    SIG_DONOTUSE_CONST: "SIG_DONOTUSE_CONST",
    SIG_DONOTUSE_INPUT: "SIG_DONOTUSE_INPUT",
    SIG_DONOTUSE_PERSIST: "SIG_DONOTUSE_PERSIST",
}

# Bit 0 is a check bit and the flags start at bit 2, so the set of legal masks is
# (all_flags << 2) | 1. Mirrors GraphSigManager::testSettings (signature.cc:914-924).
_ALL_FLAGS = (
    SIG_COLLAPSE_SIZE
    | SIG_COLLAPSE_INDNOISE
    | SIG_DONOTUSE_CONST
    | SIG_DONOTUSE_INPUT
    | SIG_DONOTUSE_PERSIST
)
VALID_SETTINGS_MASK = (_ALL_FLAGS << 2) | 1  # 0x1CD

DEFAULT_PROFILE_NAME = "nosize"

# Shipped with Ghidra. Used when config declares no profiles of its own.
BUILTIN_PROFILES = {
    "nosize": {"settings": 0x4D, "weights": "lshweights_nosize.xml"},
    "sized_32": {"settings": 0x49, "weights": "lshweights_32.xml"},
    "sized_64": {"settings": 0x49, "weights": "lshweights_64.xml"},
    "sized_64_32": {"settings": 0x49, "weights": "lshweights_64_32.xml"},
    "cpool": {"settings": 0x49, "weights": "lshweights_cpool.xml"},
}


class ProfileError(ValueError):
    """Raised for a malformed profile, an illegal mask, or a mismatched table."""


def test_settings(mask):
    """True if `mask` is a legal argument for setSignatureSettings.

    Mirrors GraphSigManager::testSettings: zero is rejected, and no bit outside
    VALID_SETTINGS_MASK may be set.
    """
    if not isinstance(mask, int) or isinstance(mask, bool):
        return False
    if mask == 0:
        return False
    return (mask & ~VALID_SETTINGS_MASK) == 0


def decode_settings(mask):
    """Return the flag names enabled by `mask`, decoding bit 0 / the >>2 shift."""
    if not test_settings(mask):
        raise ProfileError(
            f"Invalid signature settings {mask:#x}: must be non-zero and set no bit "
            f"outside {VALID_SETTINGS_MASK:#x}"
        )
    sigmods = mask >> 2
    return [name for bit, name in sorted(FLAG_NAMES.items()) if sigmods & bit]


class BsimProfile:
    """A validated (settings mask, weights table) pairing."""

    def __init__(self, name, settings, weights_path):
        self.name = name
        self.settings = settings
        self.weights_path = weights_path

    @property
    def flags(self):
        return decode_settings(self.settings)

    def __repr__(self):
        return (
            f"BsimProfile(name={self.name!r}, settings={self.settings:#x}, "
            f"weights_path={str(self.weights_path)!r})"
        )


def _ghidra_data_dir():
    """Directory holding Ghidra's shipped lshweights_*.xml tables."""
    configured = config_service.get("bsim.weights_dir")
    if configured:
        return Path(configured)

    root = config_service.get("ghidra.install_dir") or os.environ.get("GHIDRA_INSTALL_DIR")
    if root:
        return Path(root) / "Ghidra" / "Features" / "BSim" / "data"

    # Fall back to the pinned install in the repo's bin/ (not tracked in git, but
    # that is where install_ghidra.sh puts it).
    for candidate in sorted(Path("bin").glob("ghidra_*"), reverse=True):
        data = candidate / "Ghidra" / "Features" / "BSim" / "data"
        if data.is_dir():
            return data
    return Path("bin")


def _resolve_weights_path(weights):
    """Resolve a profile's `weights` entry to a concrete path.

    A bare filename resolves against Ghidra's data directory; anything with a
    separator is treated as a repo-relative (or absolute) path so custom tables
    can live in the project.
    """
    path = Path(weights)
    if path.is_absolute() or len(path.parts) > 1:
        return path
    return _ghidra_data_dir() / path


def weights_file_settings(path):
    """Read the `settings` attribute the weights table declares for itself."""
    try:
        root = ElementTree.parse(path).getroot()
    except (OSError, ElementTree.ParseError) as exc:
        raise ProfileError(f"Cannot read weights table {path}: {exc}") from exc

    declared = root.get("settings")
    if declared is None:
        raise ProfileError(f"Weights table {path} has no 'settings' attribute")
    try:
        return int(declared, 16 if declared.lower().startswith("0x") else 10)
    except ValueError as exc:
        raise ProfileError(
            f"Weights table {path} declares unparseable settings {declared!r}"
        ) from exc


def _configured_profiles():
    profiles = config_service.get("bsim.profiles")
    if isinstance(profiles, dict) and profiles:
        return profiles
    return BUILTIN_PROFILES


def get_profile(name=None, verify_weights=True):
    """Load and validate a profile by name (default: the configured one).

    Validates that the mask is legal and that the weights table declares the same
    mask. A mismatch is fatal: scoring with a table built for different signature
    settings silently produces meaningless numbers.
    """
    if name is None:
        name = config_service.get("bsim.profile", DEFAULT_PROFILE_NAME)

    profiles = _configured_profiles()
    entry = profiles.get(name)
    if entry is None:
        raise ProfileError(
            f"Unknown BSim profile {name!r}; configured profiles: "
            f"{sorted(profiles)}"
        )

    settings = entry.get("settings")
    if isinstance(settings, str):
        settings = int(settings, 16 if settings.lower().startswith("0x") else 10)
    if not test_settings(settings):
        raise ProfileError(
            f"Profile {name!r} has invalid signature settings {settings!r}: must be "
            f"non-zero and set no bit outside {VALID_SETTINGS_MASK:#x}"
        )

    weights = entry.get("weights")
    if not weights:
        raise ProfileError(f"Profile {name!r} declares no weights table")
    weights_path = _resolve_weights_path(weights)

    if verify_weights:
        declared = weights_file_settings(weights_path)
        if declared != settings:
            raise ProfileError(
                f"Profile {name!r} pairs settings {settings:#x} with {weights_path}, "
                f"which declares {declared:#x}. Refusing to score with a weights "
                f"table built for different signature settings."
            )

    return BsimProfile(name, settings, weights_path)


def parse_algo(algo):
    """Split an algorithm name into ``(base, profile_name_or_None)``.

    Weighted scores are namespaced by the profile that produced them
    (``weighted_cosine:nosize``) so that changing profile builds a new score set
    rather than silently overwriting one computed under different weights.
    """
    if algo and ":" in algo:
        base, _, profile = algo.partition(":")
        return base, (profile or None)
    return algo, None


def qualified_algo(base, profile_name=None):
    """Inverse of `parse_algo`: build the namespaced algorithm name."""
    if profile_name is None:
        profile_name = config_service.get("bsim.profile", DEFAULT_PROFILE_NAME)
    return f"{base}:{profile_name}"


def get_signature_settings():
    """The mask the decompiler should use, without touching the weights table.

    Feature extraction needs the mask but not the weights, and runs on hosts where
    the table may be absent; fall back to the default rather than failing ingest.
    """
    try:
        return get_profile(verify_weights=False).settings
    except ProfileError as exc:
        fallback = BUILTIN_PROFILES[DEFAULT_PROFILE_NAME]["settings"]
        logging.warning(
            f"[bsim] Falling back to signature settings {fallback:#x}: {exc}"
        )
        return fallback


def demo():
    """Self-check: mask validation and decoding."""
    assert test_settings(0x4D)
    assert test_settings(0x49)
    assert not test_settings(0), "zero must be rejected"
    assert not test_settings(0x2), "bit 1 is not a legal flag position"
    assert not test_settings(0x1CD | 0x200), "bits outside the mask are illegal"
    # Ghidra permits bit 0 but does not require it, so 0x4C is legal.
    assert test_settings(0x4C)
    assert not test_settings(True), "bools are not masks"

    assert decode_settings(0x4D) == [
        "SIG_COLLAPSE_SIZE",
        "SIG_COLLAPSE_INDNOISE",
        "SIG_DONOTUSE_CONST",
    ]
    # 0x49 is the sized configuration: the same minus SIG_COLLAPSE_SIZE.
    assert decode_settings(0x49) == ["SIG_COLLAPSE_INDNOISE", "SIG_DONOTUSE_CONST"]
    assert 0x4D >> 2 == 0x13

    assert parse_algo("weighted_cosine:nosize") == ("weighted_cosine", "nosize")
    assert parse_algo("unweighted_cosine") == ("unweighted_cosine", None)
    assert qualified_algo("weighted_cosine", "my_go_idf") == "weighted_cosine:my_go_idf"

    print("bsim_profiles demo OK")


if __name__ == "__main__":
    demo()
