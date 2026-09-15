"""Cheap boilerplate tags from exact Ghidra function names."""

from bsimvis.app.services import tag_taxonomy

# Exact names only: a broad name rule would label application code as runtime.
BOILERPLATE_SYMBOLS = {
    "_start": "boilerplate:runtime:elf:startup",
    "_init": "boilerplate:runtime:elf:init",
    "_fini": "boilerplate:runtime:elf:fini",
    "__libc_start_main": "boilerplate:runtime:glibc:startup",
    "__libc_start_call_main": "boilerplate:runtime:glibc:startup",
    "__libc_csu_init": "boilerplate:runtime:glibc:init",
    "__libc_csu_fini": "boilerplate:runtime:glibc:fini",
    "__libc_start_init": "boilerplate:runtime:musl:init",
    "__libc_exit_fini": "boilerplate:runtime:musl:fini",
    "__uClibc_main": "boilerplate:runtime:uclibc:startup",
    "__libc_init_array": "boilerplate:runtime:newlib:init",
    "__libc_fini_array": "boilerplate:runtime:newlib:fini",
    "frame_dummy": "boilerplate:runtime:gcc:support",
    "register_tm_clones": "boilerplate:runtime:gcc:support",
    "deregister_tm_clones": "boilerplate:runtime:gcc:support",
    "__do_global_dtors_aux": "boilerplate:runtime:gcc:support",
    "__do_global_dtors_aux_fini_array_entry": "boilerplate:runtime:gcc:support",
    "__stack_chk_fail": "boilerplate:runtime:gcc:security",
    "__stack_chk_fail_local": "boilerplate:runtime:gcc:security",
    "mainCRTStartup": "boilerplate:runtime:msvc:startup",
    "wmainCRTStartup": "boilerplate:runtime:msvc:startup",
    "WinMainCRTStartup": "boilerplate:runtime:msvc:startup",
    "wWinMainCRTStartup": "boilerplate:runtime:msvc:startup",
    "DllMainCRTStartup": "boilerplate:runtime:msvc:startup",
    "_DllMainCRTStartup": "boilerplate:runtime:msvc:startup",
    "__DllMainCRTStartup": "boilerplate:runtime:msvc:startup",
    "__tmainCRTStartup": "boilerplate:runtime:msvc:startup",
    "__mingw_CRTStartup": "boilerplate:runtime:mingw:startup",
    "__main": "boilerplate:runtime:gcc:init",
    "___main": "boilerplate:runtime:mingw:init",
    "__scrt_common_main": "boilerplate:runtime:msvc:startup",
    "__scrt_common_main_seh": "boilerplate:runtime:msvc:startup",
    "__security_init_cookie": "boilerplate:runtime:msvc:security",
    "__report_gsfailure": "boilerplate:runtime:msvc:security",
    "_CRT_INIT": "boilerplate:runtime:msvc:init",
}


def boilerplate_tag_for_function_name(name):
    """Return a boilerplate tag, retaining the matched symbol as non-hierarchical detail."""
    family = BOILERPLATE_SYMBOLS.get(str(name or ""))
    return tag_taxonomy.canonical_tag_id(f"{family}#{name}") if family else None


def demo():
    assert (
        boilerplate_tag_for_function_name("__uClibc_main")
        == "boilerplate:runtime:uclibc:startup#__uClibc_main"
    )
    assert boilerplate_tag_for_function_name("main") is None


if __name__ == "__main__":
    demo()
    print("boilerplate_tag_service demo OK")
