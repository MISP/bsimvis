"""Program entry point from the file header, via LIEF.

LIEF reads the header for ELF, PE and Mach-O alike, so this does not depend on
Ghidra's `entry` label (missing on unstripped ELF) or on analysis having run.
"""

import logging


def entry_address(path, image_base):
    """Header entry point as a Ghidra address, or None.

    The offset from LIEF's own image base is re-applied on Ghidra's, which makes
    PIE ELF (LIEF 0x0, Ghidra 0x100000) and the other formats one formula.
    None for a library with no entry (e_entry == 0) or a non-executable format.
    """
    try:
        import lief

        lief.logging.disable()
        binary = lief.parse(path)
        if binary is None or not binary.entrypoint:
            return None
        return image_base + binary.entrypoint - binary.imagebase
    except Exception as e:
        logging.warning("LIEF entry point failed for %s: %s", path, e)
        return None
