#!/usr/bin/env python3
"""Does a YARA hit in .rodata reach the function that references the string?

The pure-Python half of the YARA path is covered by tag_taxonomy.demo(). The
half that needed a real JVM is `GhidraAnalyzer._funcs_referencing`, and it is
the half with the interesting failure mode: a rule string is usually a
*substring* of the literal it matched, so the match address lands mid-item
while every xref points at the item's start. Get that wrong and the whole
feature silently no-ops on the common case.

So this compiles a binary with a string only one function touches, matches a
substring of it, and asserts the tag lands on that function and nowhere else.
It also asserts the OLD behaviour first -- getFunctionContaining() returning
None -- because if the match ever does land inside a function body the test
stops proving anything.

    GHIDRA_INSTALL_DIR=... python scripts/test_yara_xref.py

Needs gcc and Ghidra; skips (exit 0) with a note if either is missing, so it
stays safe to call from a suite that runs where neither exists.
"""

import os
import shutil
import subprocess
import sys
import tempfile

SOURCE = """
#include <stdio.h>
void referenced_me(void) { puts("PREFIX_BSIMVIS_XREF_CANARY_9271_SUFFIX"); }
void unrelated_func(int n) { printf("no canary here: %d\\n", n * 3); }
int main(void) { referenced_me(); unrelated_func(7); return 0; }
"""

RULES = """
rule Test_Xref_Canary {
    meta:
        category = "Trojan"
        malware = "CANARY"
    strings:
        $a = "XREF_CANARY_9271"
    condition:
        $a
}
rule Test_Condition_Only {
    condition:
        uint32(0) == 0x464c457f
}
"""

CANARY = "yara:trojan:canary:Test_Xref_Canary"


def skip(msg):
    print(f"SKIP: {msg}")
    sys.exit(0)


def main():
    if not shutil.which("gcc"):
        skip("gcc not available")
    if not os.environ.get("GHIDRA_INSTALL_DIR"):
        skip("GHIDRA_INSTALL_DIR not set")

    # Ghidra's ProjectLocator rejects any path element starting with '.', so
    # mkdtemp (plain /tmp/...) rather than anything under the repo.
    work = tempfile.mkdtemp(prefix="yara-xref-")
    try:
        src, binary = f"{work}/t.c", f"{work}/t"
        rules_path = f"{work}/r.yara"
        with open(src, "w") as f:
            f.write(SOURCE)
        with open(rules_path, "w") as f:
            f.write(RULES)
        subprocess.run(["gcc", "-O0", "-o", binary, src], check=True)

        import yara

        matches = list(yara.compile(filepath=rules_path).match(filepath=binary))
        rules_hit = {m.rule for m in matches}
        assert rules_hit == {"Test_Xref_Canary", "Test_Condition_Only"}, rules_hit

        from bsimvis.app.services.tag_taxonomy import yara_file_tags, yara_rule_hits

        # The condition-only rule has no string instances, so it exists at file
        # level or not at all.
        file_tags = yara_file_tags(matches)
        assert "yara:unknown:unknown:Test_Condition_Only" in file_tags, file_tags
        assert CANARY in file_tags, file_tags

        import pyghidra

        pyghidra.start()
        from bsimvis.ghidra_job import GhidraAnalyzer

        analyzer = object.__new__(GhidraAnalyzer)
        hits = yara_rule_hits(matches)
        file_offset = next(iter(hits))

        with pyghidra.open_program(
            binary, project_location=work, project_name="proj", analyze=True
        ) as flat:
            program = flat.getCurrentProgram()
            fm = program.getFunctionManager()

            addrs = list(program.getMemory().locateAddressesForFileOffset(file_offset))
            assert addrs, f"file offset {hex(file_offset)} mapped nowhere"
            assert all(fm.getFunctionContaining(a) is None for a in addrs), (
                "match address is inside a function body -- the old code path would "
                "already catch it, so this run does not exercise the xref hop"
            )

            tags = analyzer._yara_tags_for_program(matches, program)
            factory = program.getAddressFactory()
            named = {
                str(fm.getFunctionAt(factory.getAddress(k)) or k): sorted(v)
                for k, v in tags.items()
            }

            # Same resolution, but carrying rule ids instead of tags: this is
            # what makes a function's tag chip name the one rule that put the
            # tag there rather than every rule in the ruleset carrying it.
            from bsimvis.app.services.tag_provenance import match_offsets

            rule_hits = analyzer._funcs_by_offset(
                match_offsets(matches),
                program,
                lambda f: str(f.getEntryPoint()).split(":")[-1],
            )
            named_rules = {
                str(fm.getFunctionAt(factory.getAddress(k)) or k): sorted(v)
                for k, v in rule_hits.items()
            }
            # The key has to be the address form function entity ids are built
            # from (`Address.toString()`), not `hex()` -- a `0x`-prefixed key
            # would record hits under an id no page ever asks for.
            addr_keys = list(rule_hits)

        assert named.get("referenced_me") == [CANARY], named
        assert "unrelated_func" not in named, f"tag leaked: {named}"
        assert "main" not in named, f"tag leaked: {named}"
        assert list(named_rules) == ["referenced_me"], named_rules
        (rid,) = named_rules["referenced_me"]
        assert rid.startswith("yara:") and rid.endswith("#Test_Xref_Canary"), rid
        assert all(
            k == k.lower() and not k.startswith("0x") and int(k, 16) for k in addr_keys
        ), addr_keys
        print(f"yara xref test OK -> {named}, rules -> {named_rules}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
