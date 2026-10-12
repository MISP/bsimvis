from flask import request
from bsimvis.app.services.note_service import note_service
import logging
import re


def add_note():
    """
    Adds a note to a function.
    Payload: {
        "collection": str,
        "func_id": str,
        "text": str,
        "owner": str (default "user")
    }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    func_id = data.get("func_id")
    text = data.get("text")
    owner = data.get("owner", "user")

    if not all([collection, func_id, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.add_note(collection, func_id, text, owner)
    if note:
        return {"status": "success", "note": note}
    else:
        return {"error": "Could not add note"}, 500


def update_note():
    """
    Updates an existing note.
    Payload: {
        "collection": str,
        "func_id": str,
        "note_id": str,
        "text": str
    }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    func_id = data.get("func_id")
    note_id = data.get("note_id")
    text = data.get("text")

    if not all([collection, func_id, note_id, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.update_note(collection, func_id, note_id, text)
    if note:
        return {"status": "success", "note": note}
    else:
        return {"error": "Could not update note"}, 500


def remove_note():
    """
    Removes a note from a function.
    Payload: {
        "collection": str,
        "func_id": str,
        "note_id": str
    }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    func_id = data.get("func_id")
    note_id = data.get("note_id")

    if not all([collection, func_id, note_id]):
        return {"error": "Missing parameters"}, 400

    success = note_service.remove_note(collection, func_id, note_id)
    if success:
        return {"status": "success"}
    else:
        return {"error": "Could not remove note"}, 500


def get_notes():
    """
    Lists all notes for a function.
    Query Params: collection, func_id
    """
    collection = request.args.get("collection")
    pool = request.args.get("pool") or request.args.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    func_id = request.args.get("func_id")

    if not all([collection, func_id]):
        return {"error": "Missing parameters"}, 400

    notes = note_service.get_notes(collection, func_id)
    return {"status": "success", "notes": notes}


def rename_functions():
    """
    Sets or clears the analyst name of functions (empty `name` clears).
    Payload: {"func_ids": [str], "name": str, "owner": "user" | "llm"}, or a batch
    {"renames": [that, ...]} that flushes the N-way cache once.
    """
    data = request.json or {}
    batch = data.get("renames") or [data]
    todo = []
    for item in batch:
        func_ids = item.get("func_ids") or []
        if not func_ids or "name" not in item:
            return {"error": "Missing parameters"}, 400
        owner = "llm" if item.get("owner") == "llm" else "user"
        todo += [(f, item["name"], owner) for f in func_ids]
    done = note_service.set_user_names(todo)
    if not done:
        return {"error": "Could not rename"}, 500
    from bsimvis.app.services import nway_diff_service

    nway_diff_service.invalidate()
    return {"status": "success", "renamed": done}


def suggest_function_name():
    """
    Asks the LLM for a name for the functions of one N-way row. Never applies it.
    Payload: {"func_ids": [str]} (at most 3 are sent to the model)
    """
    from bsimvis.app.services.llm_service import llm_service
    from bsimvis.app.services.llm_tools import get_function

    func_ids = (request.json or {}).get("func_ids") or []
    parts = []
    for fid in func_ids[:3]:
        f = get_function(fid)
        if f.get("code"):
            parts.append(f"// {f.get('file_name')}\n{f['code'][:6000]}")
    if not parts:
        return {"error": "No code available"}, 404
    prompt = (
        "Name this function, which is the same function across several binaries. "
        "Reply with one C identifier in snake_case, nothing else.\n\n"
        + "\n\n".join(parts)
    )
    reply = llm_service.chat([{"role": "user", "content": prompt}])
    m = re.search(r"[A-Za-z_][A-Za-z0-9_]*", reply or "")
    if not m or reply.startswith("Error"):
        return {"error": reply or "No name returned"}, 502
    return {"name": m.group(0)[:64]}


# --- File notes ---


def add_file_note():
    """
    Adds a note to a file.
    Payload: { collection, file_id, text, owner }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    file_id = data.get("file_id")
    text = data.get("text")
    owner = data.get("owner", "user")

    if not all([collection, file_id, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.add_file_note(collection, file_id, text, owner)
    if note:
        return {"status": "success", "note": note}
    return {"error": "Could not add file note"}, 500


def update_file_note():
    """
    Updates an existing file note.
    Payload: { collection, file_id, note_id, text }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    file_id = data.get("file_id")
    note_id = data.get("note_id")
    text = data.get("text")

    if not all([collection, file_id, note_id, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.update_file_note(collection, file_id, note_id, text)
    if note:
        return {"status": "success", "note": note}
    return {"error": "Could not update file note"}, 500


def remove_file_note():
    """
    Removes a note from a file.
    Payload: { collection, file_id, note_id }
    """
    data = request.json or {}
    collection = data.get("collection")
    pool = data.get("pool") or data.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    file_id = data.get("file_id")
    note_id = data.get("note_id")

    if not all([collection, file_id, note_id]):
        return {"error": "Missing parameters"}, 400

    success = note_service.remove_file_note(collection, file_id, note_id)
    if success:
        return {"status": "success"}
    return {"error": "Could not remove file note"}, 500


def get_file_notes():
    """
    Lists all notes for a file.
    Query Params: collection, file_id
    """
    collection = request.args.get("collection")
    pool = request.args.get("pool") or request.args.get("pool_id")
    if pool and not (
        collection
        and (collection.startswith("pool:") or collection.startswith("global:pool:"))
    ):
        if collection:
            collection = f"global:pool:{pool}:col:{collection}"
        else:
            collection = f"global:pool:{pool}"

    file_id = request.args.get("file_id")

    if not all([collection, file_id]):
        return {"error": "Missing parameters"}, 400

    notes = note_service.get_file_notes(collection, file_id)
    return {"status": "success", "notes": notes}


# --- Bin_sim pair notes ---
#
# A pair's sid already fully qualifies its collection/pool scope
# (`{collection}:bin_sim:{algo}:{md5_a}::{md5_b}`), so unlike func/file
# notes there is no separate pool-rewrite step: collection is derived
# from the sid itself, never passed by the client.


def _bin_sim_collection(sid):
    return (sid or "").split(":bin_sim:", 1)[0] or None


def add_bin_sim_note():
    """
    Adds a note to a bin_sim pair.
    Payload: { sid, text, owner }
    """
    data = request.json or {}
    sid = data.get("sid")
    text = data.get("text")
    owner = data.get("owner", "user")
    collection = _bin_sim_collection(sid)

    if not all([collection, sid, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.add_bin_sim_note(collection, sid, text, owner)
    if note:
        return {"status": "success", "note": note}
    return {"error": "Could not add bin_sim note"}, 500


def update_bin_sim_note():
    """
    Updates an existing bin_sim pair note.
    Payload: { sid, note_id, text }
    """
    data = request.json or {}
    sid = data.get("sid")
    note_id = data.get("note_id")
    text = data.get("text")
    collection = _bin_sim_collection(sid)

    if not all([collection, sid, note_id, text]):
        return {"error": "Missing parameters"}, 400

    note = note_service.update_bin_sim_note(collection, sid, note_id, text)
    if note:
        return {"status": "success", "note": note}
    return {"error": "Could not update bin_sim note"}, 500


def remove_bin_sim_note():
    """
    Removes a note from a bin_sim pair.
    Payload: { sid, note_id }
    """
    data = request.json or {}
    sid = data.get("sid")
    note_id = data.get("note_id")
    collection = _bin_sim_collection(sid)

    if not all([collection, sid, note_id]):
        return {"error": "Missing parameters"}, 400

    success = note_service.remove_bin_sim_note(collection, sid, note_id)
    if success:
        return {"status": "success"}
    return {"error": "Could not remove bin_sim note"}, 500


def get_bin_sim_notes():
    """
    Lists all notes for a bin_sim pair.
    Query Params: sid
    """
    sid = request.args.get("sid")
    collection = _bin_sim_collection(sid)

    if not all([collection, sid]):
        return {"error": "Missing parameters"}, 400

    notes = note_service.get_bin_sim_notes(collection, sid)
    return {"status": "success", "notes": notes}
