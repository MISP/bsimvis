"""Unpacking and container extraction for uploads.

A packed executable (UPX), a container (zip/tar/APK) and a multi-architecture
binary (fat Mach-O) are the same shape of problem: one uploaded blob yields one
or more child blobs that hold the code we actually want to analyze, and every
child has to remember where it came from.

A handler recognises one format and returns its children. The ingest route
walks HANDLERS in order and stops at the first match, so a new format is a new
entry in HANDLERS -- not a new branch in the route.

`parent_is_code` splits the two kinds of parent, and decides two things:

* whether the parent is analyzed itself. A UPX-packed executable is code, so it
  gets its own document and packed-vs-unpacked becomes a normal binary diff. A
  zip is not code, so only its members are analyzed.
* what a failed unpack means. A packed binary that will not unpack is still a
  perfectly good sample, so it is analyzed as-is; a container that will not open
  yields nothing at all, so it is an error.
"""

import io
import logging
import os
import struct
import subprocess
import tempfile
import zipfile
from collections import namedtuple
from pathlib import Path
from shutil import which

from bsimvis.app.services import archive_service

# How deep we follow a container inside a container (a jar inside an APK).
# Two levels covers every real sample seen so far and stops a zip bomb from
# turning into an unbounded ingest.
MAX_DEPTH = 2

# ponytail: a container yielding more children than this is not a sample, it is
# a corpus. Truncate rather than flooding the collection.
MAX_CHILDREN = 200

UPX_TIMEOUT = 120

# Handler:
#   tag           analysis tag put on the parent and on every child
#   parent_is_code  see module docstring
#   detect(raw_bytes, file_name) -> bool
#   unpack(raw_bytes, file_name, options) -> [(child_name, child_bytes), ...]
Handler = namedtuple("Handler", "name tag parent_is_code detect unpack")


class UnpackError(Exception):
    pass


# --------------------------------------------------------------------------
# UPX
# --------------------------------------------------------------------------

UPX_MAGIC = b"UPX!"


def upx_path():
    """Path to the upx executable, or None when it is not installed."""
    explicit = os.environ.get("UPX_BIN")
    if explicit:
        return explicit if os.path.exists(explicit) else None
    # install.sh drops downloaded tools in <repo>/bin, which is not on PATH.
    local = Path(__file__).resolve().parents[3] / "bin" / "upx"
    if local.exists():
        return str(local)
    return which("upx")


def _is_upx(raw_bytes, file_name=""):
    # UPX writes its magic into the stub header near the start and into the
    # pack header at the very end, so scanning both ends avoids walking a
    # multi-megabyte sample on every upload. This is a heuristic -- a file that
    # merely contains the string fails `upx -d` later and is analyzed as-is.
    return UPX_MAGIC in raw_bytes[:8192] or UPX_MAGIC in raw_bytes[-8192:]


def _unpack_upx(raw_bytes, file_name, options):
    upx = upx_path()
    if not upx:
        raise UnpackError("upx is not installed (run ./install.sh)")

    with tempfile.TemporaryDirectory() as tmp:
        packed = os.path.join(tmp, "packed")
        unpacked = os.path.join(tmp, "unpacked")
        with open(packed, "wb") as fh:
            fh.write(raw_bytes)
        try:
            proc = subprocess.run(
                [upx, "-d", "-q", "-o", unpacked, packed],
                capture_output=True,
                timeout=UPX_TIMEOUT,
            )
        except subprocess.TimeoutExpired as e:
            raise UnpackError(f"upx -d timed out after {UPX_TIMEOUT}s") from e
        if proc.returncode != 0 or not os.path.exists(unpacked):
            detail = (proc.stderr or proc.stdout).decode(errors="replace").strip()
            raise UnpackError(
                f"upx -d failed: {detail.splitlines()[-1] if detail else ''}"
            )
        with open(unpacked, "rb") as fh:
            data = fh.read()

    if data == raw_bytes:
        raise UnpackError("upx -d produced an identical file")
    return [(f"{file_name}.unpacked", data)]


# --------------------------------------------------------------------------
# Fat (universal) Mach-O
# --------------------------------------------------------------------------

# magic -> (struct endianness, 64-bit fat_arch entries)
FAT_MAGICS = {
    b"\xca\xfe\xba\xbe": (">", False),
    b"\xbe\xba\xfe\xca": ("<", False),
    b"\xca\xfe\xba\xbf": (">", True),
    b"\xbf\xba\xfe\xca": ("<", True),
}

# Enough to name the common slices; anything else is reported by number.
CPU_NAMES = {
    7: "i386",
    0x01000007: "x86_64",
    12: "arm",
    0x0100000C: "arm64",
    0x0200000C: "arm64_32",
    18: "ppc",
    0x01000012: "ppc64",
}


def _fat_slices(raw_bytes):
    """[(cputype, offset, size), ...] for a fat Mach-O, or None if it is not one."""
    if len(raw_bytes) < 8:
        return None
    magic = FAT_MAGICS.get(bytes(raw_bytes[:4]))
    if magic is None:
        return None
    endian, is_64 = magic

    (count,) = struct.unpack(endian + "I", raw_bytes[4:8])
    # 0xcafebabe is also the Java class-file magic, where these same four bytes
    # are the class version (>= 45) rather than a slice count. No real universal
    # binary carries anywhere near 32 architectures.
    if not 1 <= count <= 32:
        return None

    # fat_arch:    cputype cpusubtype offset size align
    # fat_arch_64: cputype cpusubtype offset(64) size(64) align reserved
    fmt = endian + ("IIQQII" if is_64 else "IIIII")
    entry_size = struct.calcsize(fmt)

    out = []
    for i in range(count):
        start = 8 + i * entry_size
        if start + entry_size > len(raw_bytes):
            return None
        cputype, _cpusub, offset, size = struct.unpack(
            fmt, raw_bytes[start : start + entry_size]
        )[:4]
        if size == 0 or offset + size > len(raw_bytes):
            return None
        out.append((cputype, offset, size))
    return out


def _is_fat_macho(raw_bytes, file_name=""):
    return _fat_slices(raw_bytes) is not None


def _unpack_fat_macho(raw_bytes, file_name, options):
    slices = _fat_slices(raw_bytes)
    if not slices:
        raise UnpackError("not a fat Mach-O")
    return [
        (
            f"{file_name}:{CPU_NAMES.get(cputype, 'cpu%d' % cputype)}",
            bytes(raw_bytes[offset : offset + size]),
        )
        for cputype, offset, size in slices
    ]


# --------------------------------------------------------------------------
# APK / archives
# --------------------------------------------------------------------------

# An APK's assets/ and res/ would flood the corpus, and none of it is code BSim
# can index. Only dex bytecode and bundled native libraries are worth ingesting.
APK_CODE_SUFFIXES = (".dex", ".so", ".jar")


def _is_apk(raw_bytes, file_name=""):
    if raw_bytes[:4] != b"PK\x03\x04":
        return False
    if file_name.endswith((".apk", ".aab")):
        return True
    try:
        # Listing only reads the central directory, and an APK is never
        # encrypted, so stdlib zipfile is enough here.
        with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
            return "AndroidManifest.xml" in zf.namelist()
    except (zipfile.BadZipFile, OSError):
        return False


def _unpack_apk(raw_bytes, file_name, options):
    members = _extract_archive(raw_bytes, options)
    return [(n, b) for n, b in members if n.endswith(APK_CODE_SUFFIXES)]


def _extract_archive(raw_bytes, options):
    password = options.get("password", archive_service.DEFAULT_PASSWORD)
    try:
        return archive_service.extract(raw_bytes, password)
    except archive_service.ArchiveError as e:
        raise UnpackError(str(e)) from e


def _unpack_archive(raw_bytes, file_name, options):
    return _extract_archive(raw_bytes, options)


# --------------------------------------------------------------------------
# Registry
# --------------------------------------------------------------------------

# Exact-magic handlers come first: the UPX check is a byte scan that could hit
# a packed sample stored inside an archive, and the archive must win.
HANDLERS = [
    Handler("apk", "container:apk", False, _is_apk, _unpack_apk),
    Handler(
        "archive",
        "container:archive",
        False,
        archive_service.is_archive,
        _unpack_archive,
    ),
    Handler(
        "macho-fat", "container:macho-fat", False, _is_fat_macho, _unpack_fat_macho
    ),
    Handler("upx", "packer:upx", True, _is_upx, _unpack_upx),
]

# Namespaces a handler tag can live in. These describe how the *upload* was
# wrapped, which says nothing about any single function inside it, so they stay
# on the file document and are stripped before function tagging (see
# ghidra_service.stream_bsim_data). Derived from HANDLERS so a new handler
# cannot start leaking into function tags by accident.
FILE_SCOPE_TAG_PREFIXES = tuple(sorted({h.tag.split(":")[0] + ":" for h in HANDLERS}))


def find_handler(raw_bytes, file_name=""):
    """First handler that recognises this upload, or None."""
    for handler in HANDLERS:
        try:
            if handler.detect(raw_bytes, file_name):
                return handler
        except Exception as e:  # a broken detector must not fail the upload
            logging.debug(f"unpack detector {handler.name} raised on {file_name}: {e}")
    return None


def unpack(raw_bytes, file_name="", options=None):
    """Return (handler, children) for an upload, or (None, []) if nothing matched.

    Raises UnpackError when a recognised format could not be unpacked; the
    caller decides what that means using handler.parent_is_code.
    """
    handler = find_handler(raw_bytes, file_name)
    if handler is None:
        return None, []

    children = handler.unpack(raw_bytes, file_name, options or {})
    if len(children) > MAX_CHILDREN:
        logging.warning(
            f"[-] {file_name}: {handler.name} produced {len(children)} children, "
            f"keeping the first {MAX_CHILDREN}"
        )
        children = children[:MAX_CHILDREN]
    return handler, children


def demo():
    import tarfile

    # -- file-scope tag namespaces ----------------------------------------
    assert FILE_SCOPE_TAG_PREFIXES == ("container:", "packer:")
    assert all(h.tag.startswith(FILE_SCOPE_TAG_PREFIXES) for h in HANDLERS)
    # what ghidra_service strips before tagging functions
    assert [
        t
        for t in ["container:apk", "packer:upx", "mirai", "lib:libc:2.31"]
        if not t.startswith(FILE_SCOPE_TAG_PREFIXES)
    ] == ["mirai", "lib:libc:2.31"]

    # -- registry dispatch ------------------------------------------------
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("one.bin", b"\x7fELF one")
    plain_zip = buf.getvalue()
    assert find_handler(plain_zip, "s.zip").name == "archive"
    assert unpack(plain_zip, "s.zip")[1] == [("one.bin", b"\x7fELF one")]

    # A Ghidra project is imported whole, never unpacked.
    assert find_handler(plain_zip, "project.gpr.zip") is None

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("AndroidManifest.xml", b"manifest")
        zf.writestr("classes.dex", b"dex\n035\x00")
        zf.writestr("lib/arm64-v8a/libfoo.so", b"\x7fELF so")
        zf.writestr("res/drawable/icon.png", b"\x89PNG")
    apk = buf.getvalue()
    handler, children = unpack(apk, "app.apk")
    assert handler.name == "apk" and handler.tag == "container:apk"
    assert not handler.parent_is_code
    # resources dropped, code kept
    assert sorted(n for n, _ in children) == ["classes.dex", "lib/arm64-v8a/libfoo.so"]

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo("three.bin")
        info.size = 3
        tf.addfile(info, io.BytesIO(b"abc"))
    assert find_handler(buf.getvalue(), "s.tar").name == "archive"

    # -- fat Mach-O -------------------------------------------------------
    slices = [
        (0x01000007, b"\xcf\xfa\xed\xfe" + b"x86_64 slice"),
        (0x0100000C, b"\xcf\xfa\xed\xfe" + b"arm64 slice"),
    ]
    header = struct.pack(">II", 0xCAFEBABE, len(slices))
    offset = 8 + 20 * len(slices)
    body = b""
    for cputype, payload in slices:
        header += struct.pack(">IIIII", cputype, 0, offset + len(body), len(payload), 4)
        body += payload
    fat = header + body
    handler, children = unpack(fat, "tool")
    assert handler.name == "macho-fat", handler
    assert children == [("tool:x86_64", slices[0][1]), ("tool:arm64", slices[1][1])]

    # A Java class file shares the 0xcafebabe magic and must not be mistaken
    # for a universal binary (major version 52 reads as 52 "slices").
    assert find_handler(b"\xca\xfe\xba\xbe\x00\x00\x004rest of class") is None

    # A truncated fat header describes slices that are not there.
    assert find_handler(header + b"short") is None

    # -- UPX --------------------------------------------------------------
    assert _is_upx(b"\x7fELF" + b"\x00" * 100 + UPX_MAGIC + b"\x00" * 50)
    assert _is_upx(b"\x7fELF" + b"\x00" * 40000 + UPX_MAGIC)  # magic at the tail
    assert not _is_upx(b"\x7fELF" + b"\x00" * 40000)
    assert find_handler(b"\x7fELF" + UPX_MAGIC).tag == "packer:upx"
    assert find_handler(b"\x7fELF" + UPX_MAGIC).parent_is_code

    # An archive whose contents happen to hold the UPX magic stays an archive.
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("packed.bin", b"\x7fELF" + UPX_MAGIC)
    assert find_handler(buf.getvalue(), "s.zip").name == "archive"

    # -- errors -----------------------------------------------------------
    try:
        unpack(b"PK\x03\x04garbage", "bad.zip")
    except UnpackError:
        pass
    else:
        raise AssertionError("a bad zip should raise UnpackError")

    # -- caps -------------------------------------------------------------
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for i in range(MAX_CHILDREN + 10):
            zf.writestr(f"m{i}.bin", b"\x7fELF %d" % i)
    assert len(unpack(buf.getvalue(), "many.zip")[1]) == MAX_CHILDREN

    print("ok")


if __name__ == "__main__":
    demo()
