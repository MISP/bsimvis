"""Cheap boilerplate tags from exact Ghidra function names."""

from bsimvis.app.services import tag_taxonomy

# Exact names only: a broad name rule would label application code as runtime.
BOILERPLATE_SYMBOLS = {
    "_start": "boilerplate:runtime:elf:startup",
    "_init": "boilerplate:runtime:elf:init",
    "_fini": "boilerplate:runtime:elf:fini",
    "entry": "boilerplate:runtime:elf:startup",
    "_DT_INIT": "boilerplate:runtime:elf:init",
    "_DT_FINI": "boilerplate:runtime:elf:fini",
    "__libc_start_main": "boilerplate:runtime:glibc:startup",
    "__libc_start_call_main": "boilerplate:runtime:glibc:startup",
    "__libc_csu_init": "boilerplate:runtime:glibc:init",
    "__libc_csu_fini": "boilerplate:runtime:glibc:fini",
    "__libc_start_init": "boilerplate:runtime:musl:init",
    "__libc_exit_fini": "boilerplate:runtime:musl:fini",
    "__uClibc_main": "boilerplate:runtime:uclibc:startup",
    "__uClibc_init": "boilerplate:runtime:uclibc:init",
    "__uClibc_fini": "boilerplate:runtime:uclibc:fini",
    "__libc_init_array": "boilerplate:runtime:newlib:init",
    "__libc_fini_array": "boilerplate:runtime:newlib:fini",
    "frame_dummy": "boilerplate:runtime:gcc:support",
    "register_tm_clones": "boilerplate:runtime:gcc:support",
    "deregister_tm_clones": "boilerplate:runtime:gcc:support",
    "__do_global_dtors_aux": "boilerplate:runtime:gcc:support",
    "__do_global_dtors_aux_fini_array_entry": "boilerplate:runtime:gcc:support",
    "__stack_chk_fail": "boilerplate:runtime:gcc:security",
    "__stack_chk_fail_local": "boilerplate:runtime:gcc:security",
    "__gmon_start__": "boilerplate:runtime:gcc:profiling",
    "call_gmon_start": "boilerplate:runtime:gcc:profiling",
    "__cxa_finalize": "boilerplate:runtime:gcc:support",
    "__cxa_atexit": "boilerplate:runtime:gcc:support",
    "__x86_return_thunk": "boilerplate:runtime:gcc:support",
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
    "_RTC_CheckStackVars": "boilerplate:runtime:msvc:security",
    "_RTC_InitBase": "boilerplate:runtime:msvc:security",
    "_RTC_Shutdown": "boilerplate:runtime:msvc:security",
    "__CxxFrameHandler3": "boilerplate:runtime:msvc:support",
    "__CxxFrameHandler4": "boilerplate:runtime:msvc:support",
    "pre_c_init": "boilerplate:runtime:msvc:startup",
    "pre_cpp_init": "boilerplate:runtime:msvc:startup",
    "_matherr": "boilerplate:runtime:msvc:support",
    "thread_self": "boilerplate:runtime:libc:support",
    "__check_one_fd": "boilerplate:runtime:uclibc:support",
    "__pthread_set_own_extricate_if": "boilerplate:runtime:uclibc:support",
    "__xstat_conv": "boilerplate:runtime:libc:support",
    "__xstat64_conv": "boilerplate:runtime:libc:support",
    "___chkstk_ms": "boilerplate:runtime:mingw:support",
    "___tmainCRTStartup": "boilerplate:runtime:mingw:startup",
    "__gnu_exception_handler@4": "boilerplate:runtime:mingw:support",
    "__matherr": "boilerplate:runtime:mingw:support",
    "___report_error": "boilerplate:runtime:mingw:support",
    "__lock_file": "boilerplate:runtime:mingw:support",
    "__unlock_file": "boilerplate:runtime:mingw:support",
    "___acrt_iob_func": "boilerplate:runtime:msvc:support",
    "__pei386_runtime_relocator": "boilerplate:runtime:mingw:startup",
    "___gcc_register_frame": "boilerplate:runtime:mingw:startup",
    "__ValidateImageBase": "boilerplate:runtime:mingw:support",
    "__FindPESection": "boilerplate:runtime:mingw:support",
    "__FindPESectionByName": "boilerplate:runtime:mingw:support",
    "__GetPEImageBase": "boilerplate:runtime:mingw:support",
    "__IsNonwritableInCurrentImage": "boilerplate:runtime:mingw:support",
    "_atexit": "boilerplate:runtime:msvc:support",
    "___do_global_ctors": "boilerplate:runtime:mingw:startup",
    "___dyn_tls_dtor@12": "boilerplate:runtime:mingw:support",
    "___dyn_tls_init@12": "boilerplate:runtime:mingw:support",
    "mark_section_writable": "boilerplate:runtime:mingw:support",
    "____w64_mingwthr_remove_key_dtor": "boilerplate:runtime:mingw:support",
    "___gdtoa": "boilerplate:runtime:libc:dtoa",
    "___freedtoa": "boilerplate:runtime:libc:dtoa",
    "dtoa_lock": "boilerplate:runtime:libc:dtoa",
    "__get_invalid_parameter_handler": "boilerplate:runtime:msvc:support",
    "__set_invalid_parameter_handler": "boilerplate:runtime:msvc:support",
    "___wcrtomb_cp": "boilerplate:runtime:mingw:support",
    "___mbrtowc_cp": "boilerplate:runtime:mingw:support",
    "____mb_cur_max_func": "boilerplate:runtime:mingw:support",
    "setlocale_codepage_hack": "boilerplate:runtime:mingw:support",
    "register_frame_ctor": "boilerplate:runtime:mingw:startup",
}

BOILERPLATE_PREFIXES = {
    "__stdio_": "boilerplate:runtime:libc:stdio",
    "_stdio_": "boilerplate:runtime:libc:stdio",
    "_ppfs_": "boilerplate:runtime:libc:printf",
    "__psfs_": "boilerplate:runtime:libc:scanf",
    "__scan_": "boilerplate:runtime:libc:scanf",
    "__heap_": "boilerplate:runtime:libc:allocator",
    "__x86.get_pc_thunk.": "boilerplate:runtime:gcc:support",
    "_charpad": "boilerplate:runtime:libc:printf",
    "_load_inttype": "boilerplate:runtime:libc:printf",
    "_store_inttype": "boilerplate:runtime:libc:printf",
    "_promoted_size": "boilerplate:runtime:libc:printf",
    "_uintmaxtostr": "boilerplate:runtime:libc:printf",
    "___mingw_": "boilerplate:runtime:mingw:support",
    "___pformat_": "boilerplate:runtime:mingw:printf",
}

BOILERPLATE_SUFFIXES = {
    "_D2A": "boilerplate:runtime:libc:dtoa",
}


def boilerplate_tag_for_function_name(name):
    """Return a boilerplate tag, retaining the matched symbol as non-hierarchical detail."""
    name_str = str(name or "")
    family = BOILERPLATE_SYMBOLS.get(name_str)

    if not family:
        for prefix, tag_fam in BOILERPLATE_PREFIXES.items():
            if name_str.startswith(prefix):
                family = tag_fam
                break

    if not family:
        for suffix, tag_fam in BOILERPLATE_SUFFIXES.items():
            if name_str.endswith(suffix):
                family = tag_fam
                break

    return tag_taxonomy.canonical_tag_id(f"{family}#{name}") if family else None


def demo():
    assert (
        boilerplate_tag_for_function_name("__uClibc_main")
        == "boilerplate:runtime:uclibc:startup#__uClibc_main"
    )
    assert (
        boilerplate_tag_for_function_name("__stdio_WRITE")
        == "boilerplate:runtime:libc:stdio#__stdio_WRITE"
    )
    assert (
        boilerplate_tag_for_function_name("___Balloc_D2A")
        == "boilerplate:runtime:libc:dtoa#___Balloc_D2A"
    )
    assert boilerplate_tag_for_function_name("main") is None


if __name__ == "__main__":
    demo()
    print("boilerplate_tag_service demo OK")
