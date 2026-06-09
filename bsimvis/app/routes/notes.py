from flask import request
from bsimvis.app.services.note_service import note_service
import logging


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
    data = request.json
    collection = data.get("collection")
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
    data = request.json
    collection = data.get("collection")
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
    data = request.json
    collection = data.get("collection")
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
    func_id = request.args.get("func_id")

    if not all([collection, func_id]):
        return {"error": "Missing parameters"}, 400

    notes = note_service.get_notes(collection, func_id)
    return {"status": "success", "notes": notes}
