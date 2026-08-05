"""Enumerates the Ghidra language IDs and compiler specs of the installed Ghidra.

Read straight from Ghidra's *.ldefs XML files rather than from the JVM: the API
process never boots Ghidra, and the definitions are exactly what
DefaultLanguageService exposes at runtime. Reading the install means the list
stays correct across Ghidra upgrades instead of rotting in a constant.
"""

import logging
import os
import xml.etree.ElementTree as ET
from functools import lru_cache
from pathlib import Path


def _ghidra_dir():
    path = os.environ.get("GHIDRA_INSTALL_DIR")
    return Path(path) if path else None


@lru_cache(maxsize=1)
def get_languages():
    """Returns [{id, processor, endian, size, variant, description, compilers}].

    compilers is [{id, name}], the compiler specs valid *for that language*.
    Deprecated languages are skipped, matching
    getLanguageDescriptions(includeDeprecated=False).
    """
    root = _ghidra_dir()
    if not root or not root.is_dir():
        return []

    languages = []
    for ldefs in sorted(root.glob("Ghidra/Processors/*/data/languages/*.ldefs")):
        try:
            tree = ET.parse(ldefs)
        except ET.ParseError as e:
            logging.warning(f"Skipping unparsable ldefs {ldefs}: {e}")
            continue

        for lang in tree.getroot().findall("language"):
            if lang.get("deprecated", "false").lower() == "true":
                continue
            lang_id = lang.get("id")
            if not lang_id:
                continue
            desc = lang.findtext("description") or ""
            languages.append(
                {
                    "id": lang_id,
                    "processor": lang.get("processor"),
                    "endian": lang.get("endian"),
                    "size": lang.get("size"),
                    "variant": lang.get("variant"),
                    "description": desc.strip(),
                    "compilers": [
                        {"id": c.get("id"), "name": c.get("name") or c.get("id")}
                        for c in lang.findall("compiler")
                        if c.get("id")
                    ],
                }
            )

    languages.sort(key=lambda x: x["id"])
    return languages


def validate(processor, cspec=None):
    """Returns an error string if the (processor, cspec) pair is invalid, else None.

    cspec validity is per-language, so the pair is checked together. An unknown
    install (empty list) validates nothing -- Ghidra stays the final authority.
    """
    if not processor:
        return None

    languages = get_languages()
    if not languages:
        return None

    lang = next((l for l in languages if l["id"] == processor), None)
    if lang is None:
        near = [l["id"] for l in languages if processor.lower() in l["id"].lower()][:5]
        hint = f" Did you mean: {', '.join(near)}?" if near else ""
        return f"Unknown processor '{processor}'.{hint} See /api/index/languages."

    if cspec and cspec not in [c["id"] for c in lang["compilers"]]:
        valid = ", ".join(c["id"] for c in lang["compilers"]) or "none"
        return (
            f"Compiler spec '{cspec}' is not valid for '{processor}'. Valid: {valid}."
        )

    return None
