#!/usr/bin/env python3
"""Does the Ghidra string extractor find a string, its xref and its YARA hit?

Drives the real JVM path (`GhidraService.extract_strings`) against a compiled
canary: one ASCII string only `uses_canary` touches, one wide string, one unused
string. Asserts the xref lands on the right function, the YARA match is badged
on the string (via `GhidraAnalyzer._strings_by_offset`), and the unreferenced
string is still in the table.

    GHIDRA_INSTALL_DIR=... python scripts/test_strings_extract.py

Skips (exit 0) when gcc or Ghidra is missing.
"""

import os
import shutil
import subprocess
import sys
import tempfile

SOURCE = r"""
#include <stdio.h>
#include <wchar.h>
const char *unused = "NEVER_REFERENCED_STRING_8841";
void uses_canary(void) { puts("PREFIX_BSIMVIS_STRING_CANARY_5513_SUFFIX"); }
void uses_wide(void) { wprintf(L"%ls\n", L"WIDE_CANARY_7720"); }
int main(void) { uses_canary(); uses_wide(); return 0; }
"""

RULES = """
rule Test_String_Canary {
    meta:
        category = "Trojan"
        malware = "CANARY"
    strings:
        $a = "STRING_CANARY_5513"
    condition:
        $a
}
"""


def main():
    if not shutil.which("gcc"):
        print("SKIP: gcc not available")
        sys.exit(0)
    if not os.environ.get("GHIDRA_INSTALL_DIR"):
        print("SKIP: GHIDRA_INSTALL_DIR not set")
        sys.exit(0)

    work = tempfile.mkdtemp(prefix="strings-extract-")
    try:
        binary = f"{work}/t"
        for name, text in (("t.c", SOURCE), ("r.yara", RULES)):
            with open(f"{work}/{name}", "w") as f:
                f.write(text)
        subprocess.run(["gcc", "-O0", "-o", binary, f"{work}/t.c"], check=True)

        import yara
        import pyghidra

        matches = list(yara.compile(filepath=f"{work}/r.yara").match(filepath=binary))
        assert matches, "rule did not match the canary"

        pyghidra.start()
        from bsimvis.app.services.ghidra_service import GhidraService
        from bsimvis.app.services.tag_provenance import match_offsets
        from bsimvis.app.services.tag_taxonomy import yara_rule_hits
        from bsimvis.ghidra_job import GhidraAnalyzer

        analyzer = object.__new__(GhidraAnalyzer)
        with pyghidra.open_program(
            binary, project_location=work, project_name="proj", analyze=True
        ) as flat:
            program = flat.getCurrentProgram()
            fm = program.getFunctionManager()
            entries = {
                str(f.getEntryPoint()).split(":")[-1]: f.getName()
                for f in fm.getFunctions(True)
            }
            opts = {
                "yara_string_tags": {
                    k: sorted(v)
                    for k, v in analyzer._strings_by_offset(
                        yara_rule_hits(matches), program
                    ).items()
                },
                "yara_string_rules": {
                    k: sorted(v)
                    for k, v in analyzer._strings_by_offset(
                        match_offsets(matches), program
                    ).items()
                },
            }
            svc = object.__new__(GhidraService)
            recs = svc.extract_strings(program, opts, set(entries))

        by_val = {r["value"]: r for r in recs}
        canary = by_val["PREFIX_BSIMVIS_STRING_CANARY_5513_SUFFIX"]
        assert [entries[e] for e in canary["funcs"]] == ["uses_canary"], canary
        assert canary["rules"] and canary["tags"], canary
        assert canary["section"] and canary["encoding"] == "ascii", canary
        unused = by_val["NEVER_REFERENCED_STRING_8841"]
        assert unused["funcs"] == [] and not unused["rules"], unused
        assert any("WIDE_CANARY_7720" in v for v in by_val), list(by_val)[:20]
        print(f"strings extract OK -> {len(recs)} strings, canary {canary}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
