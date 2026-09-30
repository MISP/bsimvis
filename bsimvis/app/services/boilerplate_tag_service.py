"""Cheap boilerplate tags from exact Ghidra function names."""

import re

from bsimvis.app.services import tag_taxonomy
from bsimvis.app.services.boilerplate_libc_names import (
    GATED_FAMILY_NAMES,
    GLIBC_PLAIN_NAMES,
)

# Exact names only: a broad name rule would label application code as runtime.
# `entry` is deliberately absent: it is Ghidra's name for any entry point, and
# in a packed file that is the unpacker stub, not CRT startup.
BOILERPLATE_SYMBOLS = {
    "_INIT_0": "boilerplate:runtime:elf:init",
    "_FINI_0": "boilerplate:runtime:elf:fini",
    "_PREINIT_0": "boilerplate:runtime:elf:init",
    "call_weak_fn": "boilerplate:runtime:gcc:support",
    # OpenSSL statically linked into a sample: library code, not runtime.
    "BIO_printf": "boilerplate:library:openssl:bio",
    "BIO_snprintf": "boilerplate:library:openssl:bio",
    "ERR_add_error_data": "boilerplate:library:openssl:err",
    "md5_block_asm_data_order": "boilerplate:library:openssl:asm",
    "bn_add_words": "boilerplate:library:openssl:bn",
    "bn_sub_words": "boilerplate:library:openssl:bn",
    "bn_mul_comba4": "boilerplate:library:openssl:bn",
    "bn_mul_comba8": "boilerplate:library:openssl:bn",
    "bn_sqr_comba4": "boilerplate:library:openssl:bn",
    "bn_sqr_comba8": "boilerplate:library:openssl:bn",
    "_start": "boilerplate:runtime:elf:startup",
    "_init": "boilerplate:runtime:elf:init",
    "_fini": "boilerplate:runtime:elf:fini",
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
    # glibc/libgcc-reserved namespaces: application code does not use these.
    "_IO_": "boilerplate:runtime:glibc:stdio",
    "__libio_": "boilerplate:runtime:glibc:stdio",
    "_dl_": "boilerplate:runtime:glibc:loader",
    "_dlfo_": "boilerplate:runtime:glibc:loader",
    "__gconv": "boilerplate:runtime:glibc:iconv",
    "_nl_": "boilerplate:runtime:glibc:locale",
    "__wcsmbs_": "boilerplate:runtime:glibc:locale",
    "__printf_": "boilerplate:runtime:glibc:printf",
    "__wprintf_": "boilerplate:runtime:glibc:printf",
    "__vfprintf_": "boilerplate:runtime:glibc:printf",
    "__vfscanf_": "boilerplate:runtime:glibc:scanf",
    "__strto": "boilerplate:runtime:glibc:strtod",
    "____strto": "boilerplate:runtime:glibc:strtod",
    "__isoc": "boilerplate:runtime:glibc:support",
    "__mpn_": "boilerplate:runtime:glibc:mpn",
    "__libc_": "boilerplate:runtime:glibc:support",
    "__nptl_": "boilerplate:runtime:glibc:thread",
    "__pthread_": "boilerplate:runtime:glibc:thread",
    "__futex_": "boilerplate:runtime:glibc:thread",
    "__lll_": "boilerplate:runtime:glibc:thread",
    "__tunable": "boilerplate:runtime:glibc:tunables",
    "__syscall_": "boilerplate:runtime:glibc:support",
    "__malloc_": "boilerplate:runtime:glibc:allocator",
    "_int_": "boilerplate:runtime:glibc:allocator",
    "__aeabi_": "boilerplate:runtime:libgcc:support",
    "__sync_": "boilerplate:runtime:libgcc:support",
    "__gnu_": "boilerplate:runtime:libgcc:support",
    "_Unwind_": "boilerplate:runtime:libgcc:unwind",
    "___Unwind_": "boilerplate:runtime:libgcc:unwind",
    "_nss_": "boilerplate:runtime:glibc:nss",
    "__nss_": "boilerplate:runtime:glibc:nss",
    "__nscd_": "boilerplate:runtime:glibc:nss",
    "__rpc_": "boilerplate:runtime:glibc:rpc",
    "_svcauth_": "boilerplate:runtime:glibc:rpc",
    "__res_": "boilerplate:runtime:glibc:resolver",
    "_res_": "boilerplate:runtime:glibc:resolver",
    "__resolv_": "boilerplate:runtime:glibc:resolver",
    "__dns_": "boilerplate:runtime:glibc:resolver",
    "_pthread_": "boilerplate:runtime:glibc:thread",
    "__sig": "boilerplate:runtime:glibc:support",
    "__sys_": "boilerplate:runtime:glibc:support",
    "__getdents": "boilerplate:runtime:glibc:support",
    "__GI_": "boilerplate:runtime:glibc:support",
    "__glibc_": "boilerplate:runtime:glibc:support",
    "__aarch64_": "boilerplate:runtime:libgcc:support",
    "_stdlib_": "boilerplate:runtime:libc:support",
    "_fp_out_": "boilerplate:runtime:libc:printf",
    # Ghidra's name for a byte-matched FID candidate set; not gated.
    "FID_conflict:": "boilerplate:runtime:libc:support",
}

BOILERPLATE_SUFFIXES = {
    "_D2A": "boilerplate:runtime:libc:dtoa",
}

# Tag-only prefixes: unlike BOILERPLATE_PREFIXES they are no evidence of a
# static libc, so they never count toward the gate below.
EXTRA_PREFIXES = {
    "__i686.get_pc_thunk.": "boilerplate:runtime:gcc:support",
    "vpaes_": "boilerplate:library:openssl:asm",
    "bsaes_": "boilerplate:library:openssl:asm",
}


# Plain glibc names (`read`, `check_match`, `do_sym`) also belong to application
# code, so they only count in a file that already shows a static libc through
# the reserved-namespace prefixes above. Any `__name` counts there too; a single
# leading underscore does not (malware helpers such as `_memcpy` use it).
# A smaller file still passes when a libc startup symbol backs a few prefixes
# (uClibc statics show 3-24 of them).
LIBC_GATE_MIN = 25
LIBC_GATE_SOFT = 3
LIBC_START_SYMBOLS = frozenset(
    ("__uClibc_main", "__uClibc_init", "__libc_start_main", "__libc_csu_init")
)
CLONE_SUFFIX = re.compile(r"(\.(isra|constprop|part)\.\d+)+$")
FAMILY_OF_GATED_NAME = {
    n: fam for fam, names in GATED_FAMILY_NAMES.items() for n in names
}

# Go names are `pkg/path.Func`; a C clone (`sort.part.0`) has the same shape, so
# the rule needs a Go runtime to be present as well as a stdlib root.
GO_GATE_MIN = 10
GO_STD_ROOTS = frozenset(
    "archive bufio bytes cmp compress container context crypto database debug "
    "embed encoding errors expvar flag fmt hash html image index internal io iter "
    "log maps math mime net os path plugin reflect regexp runtime slices sort "
    "strconv strings structs sync syscall testing text time unicode unique "
    "unsafe vendor weak".split()
)
GO_NAME = re.compile(
    r"^([a-z0-9_]+)(?:/[A-Za-z0-9_.\-]+)*\.(?!(isra|part|constprop|cold)\b)"
)
GO_RUNTIME_PREFIXES = (
    "runtime.",
    "internal/runtime/",
    "internal/abi.",
    "internal/cpu.",
    "internal/bytealg.",
    "internal/chacha8rand.",
)
GO_ASM_NAMES = frozenset(
    "gosave_systemstack_switch setg_gcc setg gogo callRet gcWriteBarrier "
    "go:textfipsstart go:textfipsend cmpbody memeqbody indexbytebody countbytebody "
    "countbody indexbody aeshashbody kernelcas kernelPublicationBarrier "
    "_rt0_arm_linux _rt0_arm_linux1 exit1 debugCall32 addMulVVWx armCas64 armXadd64 "
    "armXchg64 armLoad64 armStore64 armAnd8 armOr8 armXchg8".split()
)
# The compiler's equality/hash helper for a type: as generic as the type's package.
GO_TYPEGEN = re.compile(r"^type:\.(eq|hash)\.")


def _go_family(name):
    """Tag family of a Go standard-library symbol, or None."""
    name = GO_TYPEGEN.sub("", name)
    m = GO_NAME.match(name)
    if not m or m.group(1) not in GO_STD_ROOTS:
        return None
    if name.startswith(GO_RUNTIME_PREFIXES):
        return "boilerplate:runtime:go:runtime"
    return "boilerplate:library:go:stdlib"


def boilerplate_tags_for_names(names):
    """Map name -> tag for a whole file, adding gated plain libc and Go names."""
    names = list(names)
    tags = {n: boilerplate_tag_for_function_name(n) for n in names}
    prefixed = sum(
        1 for n in names if tags[n] and str(n).startswith(tuple(BOILERPLATE_PREFIXES))
    )
    libc_start = any(n in LIBC_START_SYMBOLS for n in names)
    if prefixed >= LIBC_GATE_MIN or (prefixed >= LIBC_GATE_SOFT and libc_start):
        for n in names:
            base = CLONE_SUFFIX.sub("", str(n))
            if tags[n]:
                continue
            fam = FAMILY_OF_GATED_NAME.get(base)
            if fam or base in GLIBC_PLAIN_NAMES or base.startswith("__"):
                fam = fam or "boilerplate:runtime:libc:support"
                tags[n] = tag_taxonomy.canonical_tag_id(f"{fam}#{n}")
    if sum(1 for n in names if str(n).startswith("runtime.")) >= GO_GATE_MIN:
        for n in names:
            fam = None if tags[n] else _go_family(str(n))
            if fam is None and not tags[n] and n in GO_ASM_NAMES:
                fam = "boilerplate:runtime:go:runtime"
            if fam:
                tags[n] = tag_taxonomy.canonical_tag_id(f"{fam}#{n}")
    return tags


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
        for prefix, tag_fam in EXTRA_PREFIXES.items():
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
    assert boilerplate_tag_for_function_name("_IO_flush_all")
    assert boilerplate_tag_for_function_name("read") is None
    assert boilerplate_tag_for_function_name("__clock_gettime64") is None

    libc = [f"_dl_fn{i}" for i in range(LIBC_GATE_MIN)]
    gated = boilerplate_tags_for_names(libc + ["read", "check_match.isra.0", "main"])
    assert gated["read"] and gated["check_match.isra.0"] and gated["main"] is None
    small = boilerplate_tags_for_names(["_dl_a", "read", "check_match"])
    assert small["read"] is None and small["check_match"] is None

    dunder = boilerplate_tags_for_names(
        libc + ["__clock_gettime64", "_memcpy", "_itoa"]
    )
    assert dunder["__clock_gettime64"] and dunder["_itoa"] and dunder["_memcpy"] is None
    assert (
        boilerplate_tags_for_names(["__clock_gettime64"])["__clock_gettime64"] is None
    )

    # `entry` is the unpacker stub in a packed file, so it is never boilerplate.
    assert boilerplate_tag_for_function_name("entry") is None
    assert boilerplate_tag_for_function_name("__i686.get_pc_thunk.bx")
    assert boilerplate_tag_for_function_name("_INIT_0")
    assert boilerplate_tag_for_function_name("bn_mul_comba8").startswith(
        "boilerplate:library:openssl:bn"
    )
    assert boilerplate_tag_for_function_name("bn_mul") is None

    # A small static uClibc passes the gate only with a libc startup symbol.
    soft = ["__stdio_a", "__stdio_b", "__stdio_c", "xdr_callhdr", "read", "main"]
    assert boilerplate_tags_for_names(soft)["xdr_callhdr"] is None
    soft.append("__uClibc_main")
    got = boilerplate_tags_for_names(soft)
    assert got["xdr_callhdr"].startswith("boilerplate:runtime:libc:rpc")
    assert got["read"] and got["main"] is None

    # Family names keep their family; unknown look-alikes stay untagged.
    fam = boilerplate_tags_for_names(libc + ["enqueue", "fde_radixsort", "attack_udp"])
    assert fam["enqueue"].startswith("boilerplate:runtime:libc:thread")
    assert fam["fde_radixsort"].startswith("boilerplate:runtime:libgcc:unwind")
    assert fam["attack_udp"] is None
    assert boilerplate_tags_for_names(["enqueue"])["enqueue"] is None

    # Go stdlib needs a Go runtime in the file; `main.` stays custom code.
    go = [f"runtime.fn{i}" for i in range(GO_GATE_MIN)]
    got = boilerplate_tags_for_names(
        go + ["crypto/tls.(*Conn).Read", "main.beacon", "sort.part.0", "net.Dial"]
    )
    assert got["runtime.fn0"].startswith("boilerplate:runtime:go:runtime")
    assert got["crypto/tls.(*Conn).Read"].startswith("boilerplate:library:go:stdlib")
    assert got["net.Dial"] and got["main.beacon"] is None and got["sort.part.0"] is None
    assert boilerplate_tags_for_names(["net.Dial"])["net.Dial"] is None
    typed = boilerplate_tags_for_names(
        go + ["type:.eq.internal/abi.Type", "type:.eq.main.Cfg", "gogo", "memeqbody"]
    )
    assert typed["type:.eq.internal/abi.Type"] and typed["gogo"] and typed["memeqbody"]
    assert typed["type:.eq.main.Cfg"] is None
    assert boilerplate_tags_for_names(["gogo"])["gogo"] is None


if __name__ == "__main__":
    demo()
    print("boilerplate_tag_service demo OK")
