"""Program entry point from the file header, via LIEF.

LIEF reads the header for ELF, PE and Mach-O alike, so this does not depend on
Ghidra's `entry` label (missing on unstripped ELF) or on analysis having run.
"""

import json
import logging

ENTRY_TAG = "entry:process"
# Ghidra's default load address for a base-0 (PIE / shared object) ELF.
GHIDRA_PIE_BASE = 0x100000


def entry_address(src, image_base=None):
    """Header entry point as a Ghidra address, or None.

    `src` is a path or the file's bytes. The offset from LIEF's own image base
    is re-applied on Ghidra's, which makes PIE ELF (LIEF 0x0, Ghidra 0x100000)
    and the other formats one formula. `image_base` is Ghidra's; without it
    (backfill, nothing stored) a base-0 image is assumed to sit at Ghidra's
    default. None for a library with no entry (e_entry == 0) or a format with
    no header entry.
    """
    try:
        import lief

        lief.logging.disable()
        binary = lief.parse(src if isinstance(src, str) else list(src))
        if binary is None or not binary.entrypoint:
            return None
        if image_base is None:
            image_base = binary.imagebase or GHIDRA_PIE_BASE
        return image_base + binary.entrypoint - binary.imagebase
    except Exception as e:
        logging.warning("LIEF entry point failed: %s", e)
        return None


def backfill_entrypoints(r, r_raw, collection, cursor="0", chunk_size=100):
    """Tag the entry function of files analysed before `entry:process` existed.

    One chunk per call, walked via `SSCAN` over `{collection}:all_files` (never
    `KEYS`); the worker re-enqueues a continuation until the cursor is "0".
    Needs the stored `{file}:raw` bytes, so a file uploaded without them is
    skipped. Idempotent, and Ghidra never runs: the function is the stored one
    whose body contains the entry address. A thunk or tiny stub that the
    min_func_len filter dropped therefore stays untagged.

    Returns `(next_cursor, files_seen, files_tagged)`.
    """
    from bsimvis.app.services import tag_taxonomy
    from bsimvis.app.services.index_service import save_function

    new_cursor, raw_ids = r.sscan(
        f"{collection}:all_files", cursor=int(cursor), count=chunk_size
    )
    file_ids = [f.decode() if isinstance(f, bytes) else f for f in raw_ids]
    tagged = 0
    for file_id in file_ids:
        md5 = file_id.rsplit(":", 1)[-1]
        meta_raw = r.get(f"{file_id}:meta")
        raw = r_raw.get(f"{file_id}:raw")
        if not meta_raw or not raw:
            continue
        meta = json.loads(meta_raw)
        if meta.get("entry_point"):
            continue
        entry = entry_address(raw)
        if entry is None:
            continue

        found = None
        for fid in r.smembers(f"{collection}:idx:file:functions:{md5}"):
            fid = fid.decode() if isinstance(fid, bytes) else fid
            fid = fid.removesuffix(":meta")
            addr = fid.rsplit(":", 1)[-1]
            fmeta_raw = r.get(f"{fid}:meta")
            if not fmeta_raw:
                continue
            fmeta = json.loads(fmeta_raw)
            start = int(addr, 16)
            if start <= entry < start + (fmeta.get("instruction_count") or 1):
                found = (addr, fmeta)
                break
        if not found:
            continue

        addr, fmeta = found
        pipe = r.pipeline(transaction=False)
        if ENTRY_TAG not in (fmeta.get("tags") or []):
            fmeta["tags"] = tag_taxonomy.filter_tags(
                list(fmeta.get("tags") or []) + [ENTRY_TAG], "analysis"
            )
            pipe.set(f"{collection}:func:{md5}:{addr}:meta", json.dumps(fmeta))
            save_function(pipe, collection, md5, addr, fmeta)
        meta["entry_point"] = addr
        pipe.set(f"{file_id}:meta", json.dumps(meta))
        pipe.execute()
        tagged += 1
    return str(new_cursor), len(file_ids), tagged
