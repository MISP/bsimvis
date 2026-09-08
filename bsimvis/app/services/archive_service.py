"""Archive unpacking for uploads.

Malware samples are usually shipped zipped with the password "infected", so an
upload that turns out to be an archive is expanded and every member is analyzed
as its own binary.
"""

import io
import tarfile

# pyzipper is a drop-in zipfile superset that also reads AES-encrypted zips,
# which is what most "infected" malware zips actually use -- stdlib zipfile
# only handles legacy ZipCrypto and fails them with "That compression method is
# not supported".
# ponytail: no rar/7z. Add py7zr / rarfile if those actually show up.
import pyzipper as zipfile

DEFAULT_PASSWORD = "infected"


class ArchiveError(Exception):
    pass


def is_archive(raw_bytes, file_name=""):
    """True if the upload should be unpacked instead of analyzed directly."""
    # A .gpr.zip is a Ghidra project, not a container of samples: Ghidra imports
    # it as-is.
    if file_name.endswith(".gpr.zip"):
        return False
    if raw_bytes[:4] == b"PK\x03\x04":
        return True
    try:
        with tarfile.open(fileobj=io.BytesIO(raw_bytes)):
            return True
    except tarfile.TarError:
        return False


def extract(raw_bytes, password=DEFAULT_PASSWORD):
    """Return [(member_name, member_bytes), ...] for a zip or tar archive.

    Directories, empty members and anything unreadable are skipped. Nested
    archives are returned as-is, not recursed into.
    """
    if raw_bytes[:4] == b"PK\x03\x04":
        return _extract_zip(raw_bytes, password)
    return _extract_tar(raw_bytes)


def _extract_zip(raw_bytes, password):
    out = []
    try:
        with zipfile.AESZipFile(io.BytesIO(raw_bytes)) as zf:
            if password:
                zf.setpassword(password.encode())
            for info in zf.infolist():
                if info.is_dir() or info.file_size == 0:
                    continue
                try:
                    data = zf.read(info)
                except RuntimeError as e:
                    # Wrong/missing password, or an unsupported encryption
                    # method -- both surface here as RuntimeError.
                    raise ArchiveError(f"{info.filename}: {e}") from e
                except NotImplementedError as e:
                    raise ArchiveError(
                        f"{info.filename}: unsupported compression/encryption ({e})"
                    ) from e
                out.append((info.filename, data))
    except zipfile.BadZipFile as e:
        raise ArchiveError(f"Bad zip archive: {e}") from e
    return out


def _extract_tar(raw_bytes):
    out = []
    try:
        with tarfile.open(fileobj=io.BytesIO(raw_bytes)) as tf:
            for member in tf.getmembers():
                if not member.isfile() or member.size == 0:
                    continue
                fh = tf.extractfile(member)
                if fh is None:
                    continue
                out.append((member.name, fh.read()))
    except tarfile.TarError as e:
        raise ArchiveError(f"Bad tar archive: {e}") from e
    return out


def demo():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("a/", "")
        zf.writestr("a/one.bin", b"\x7fELF one")
        zf.writestr("two.bin", b"\x7fELF two")
    data = buf.getvalue()
    assert is_archive(data)
    assert not is_archive(data, "project.gpr.zip")
    assert extract(data) == [("a/one.bin", b"\x7fELF one"), ("two.bin", b"\x7fELF two")]

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        info = tarfile.TarInfo("three.bin")
        info.size = 3
        tf.addfile(info, io.BytesIO(b"abc"))
    data = buf.getvalue()
    assert is_archive(data)
    assert extract(data) == [("three.bin", b"abc")]

    assert not is_archive(b"\x7fELF plain binary")

    try:
        extract(b"PK\x03\x04garbage")
    except ArchiveError:
        pass
    else:
        raise AssertionError("bad zip should raise ArchiveError")

    print("ok")


if __name__ == "__main__":
    demo()
