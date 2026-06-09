import uuid
import time
import logging
from .redis_client import get_redis


class NoteService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _resolve_func_id(self, collection, entity_id):
        """Resolves a function ID into its meta document key."""
        # entity_id is expected to be {coll}:func:{md5}:{addr} or idx:...
        resolved_id = entity_id.replace(":function:", ":func:")
        if resolved_id.startswith("idx:"):
            # idx:collection:func:md5:addr
            parts = resolved_id.split(":")
            if len(parts) >= 5:
                resolved_id = f"{parts[1]}:func:{parts[3]}:{parts[4]}"
        
        if resolved_id.endswith(":meta"):
            return resolved_id
        return f"{resolved_id}:meta"

    def add_note(self, collection, func_id, text, owner="user"):
        """Adds a note to a function and updates indices."""
        r = self.r
        text = text.strip()
        if not text:
            return None

        doc_id = self._resolve_func_id(collection, func_id)
        note = {
            "id": str(uuid.uuid4()),
            "text": text,
            "owner": owner,
            "timestamp": int(time.time() * 1000)
        }

        try:
            # 1. Fetch current document
            doc_raw = r.json().get(doc_id, "$")
            if not doc_raw:
                logging.error(f"NoteService: Document {doc_id} not found.")
                return None
            
            doc = doc_raw[0] if isinstance(doc_raw, list) else doc_raw
            
            # 2. Update Notes
            notes = doc.get("notes", [])
            if not isinstance(notes, list):
                notes = []
            notes.append(note)
            r.json().set(doc_id, "$.notes", notes)
            r.json().set(doc_id, "$.note_count", len(notes))

            # 3. Update note_owners for indexing
            owners = doc.get("note_owners", [])
            if not isinstance(owners, list):
                owners = []
            
            if owner not in owners:
                owners.append(owner)
                r.json().set(doc_id, "$.note_owners", owners)
                
                # 4. Update Secondary Index for 'note_owners'
                # Standard Bucket: {col}:idx:func:note_owners:{owner}
                indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                index_key = f"{collection}:idx:func:note_owners:{owner.lower()}"
                r.sadd(index_key, indexed_id)

                # Standard Registry: {col}:reg:func:note_owners
                registry_key = f"{collection}:reg:func:note_owners"
                r.sadd(registry_key, index_key)

            return note
        except Exception as e:
            logging.error(f"NoteService: Error adding note to {func_id}: {e}")
            return None

    def update_note(self, collection, func_id, note_id, text):
        """Updates an existing note's text."""
        r = self.r
        doc_id = self._resolve_func_id(collection, func_id)
        
        try:
            notes = r.json().get(doc_id, "$.notes")[0]
            for note in notes:
                if note["id"] == note_id:
                    note["text"] = text
                    note["timestamp"] = int(time.time() * 1000)
                    r.json().set(doc_id, "$.notes", notes)
                    return note
            return None
        except Exception as e:
            logging.error(f"NoteService: Error updating note {note_id}: {e}")
            return None

    def remove_note(self, collection, func_id, note_id):
        """Removes a note and updates indices if necessary."""
        r = self.r
        doc_id = self._resolve_func_id(collection, func_id)

        try:
            notes = r.json().get(doc_id, "$.notes")[0]
            new_notes = [n for n in notes if n["id"] != note_id]
            
            if len(new_notes) == len(notes):
                return False

            r.json().set(doc_id, "$.notes", new_notes)
            r.json().set(doc_id, "$.note_count", len(new_notes))

            # Update owners index if this was the last note by this owner
            remaining_owners = set(n["owner"] for n in new_notes)
            old_owners = set(n["owner"] for n in notes)
            removed_owners = old_owners - remaining_owners

            r.json().set(doc_id, "$.note_owners", list(remaining_owners))
            
            indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
            for owner in removed_owners:
                index_key = f"{collection}:idx:func:note_owners:{owner.lower()}"
                r.srem(index_key, indexed_id)

            return True
        except Exception as e:
            logging.error(f"NoteService: Error removing note {note_id}: {e}")
            return False

    def get_notes(self, collection, func_id):
        """Returns all notes for a function."""
        doc_id = self._resolve_func_id(collection, func_id)
        try:
            notes = self.r.json().get(doc_id, "$.notes")
            return notes[0] if notes else []
        except:
            return []


note_service = NoteService()
