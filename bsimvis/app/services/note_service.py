import uuid
import time
import logging
import json
from .redis_client import get_redis
from bsimvis.app.services.index_service import resolve_origin_collection


def _normalize_collection(collection, entity_id=None):
    return resolve_origin_collection(collection, entity_id)


class NoteService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _resolve_func_id(self, collection, entity_id):
        """Resolves a function ID into its meta document key."""
        collection = _normalize_collection(collection, entity_id)
        resolved_id = entity_id.replace(":function:", ":func:")
        if ":col:" in resolved_id:
            resolved_id = resolved_id.split(":col:")[-1]
        elif resolved_id.startswith("idx:"):
            parts = resolved_id.split(":")
            if len(parts) >= 5:
                resolved_id = f"{parts[1]}:func:{parts[3]}:{parts[4]}"

        if resolved_id.endswith(":meta"):
            return resolved_id
        return f"{resolved_id}:meta"

    def _get_doc(self, doc_id):
        raw = self.r.get(doc_id)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception as e:
            logging.error(f"NoteService: Error decoding JSON for key {doc_id}: {e}")
            return None

    def _set_doc(self, doc_id, doc):
        self.r.set(doc_id, json.dumps(doc))

    def add_note(self, collection, func_id, text, owner="user"):
        """Adds a note to a function and updates indices."""
        collection = _normalize_collection(collection, func_id)
        r = self.r
        text = text.strip()
        if not text:
            return None

        doc_id = self._resolve_func_id(collection, func_id)
        note = {
            "id": str(uuid.uuid4()),
            "text": text,
            "owner": owner,
            "timestamp": int(time.time() * 1000),
        }

        try:
            # 1. Fetch current document
            doc = self._get_doc(doc_id)
            if not doc:
                logging.error(f"NoteService: Document {doc_id} not found.")
                return None

            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"

            # 2. Update Notes
            notes = doc.get(json_field, [])
            if not isinstance(notes, list):
                notes = []
            notes.append(note)
            doc[json_field] = notes
            doc[count_field] = len(notes)

            # 3. Update note_owners for indexing
            owners = doc.get(owner_field, [])
            if not isinstance(owners, list):
                owners = []

            if owner not in owners:
                owners.append(owner)
                doc[owner_field] = owners

            self._set_doc(doc_id, doc)

            # Indexing
            if owner not in doc.get(owner_field, []) or True:  # enforce check/index
                indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                index_key = f"{collection}:idx:func:note_owners:{owner.lower()}"
                r.sadd(index_key, indexed_id)

                registry_key = f"{collection}:reg:func:note_owners"
                r.sadd(registry_key, index_key)

                # Propagate to all associated pools
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:func:note_owners:{owner.lower()}"
                    r.sadd(pool_index_key, indexed_id)
                    pool_registry_key = f"{pool_coll}:reg:func:note_owners"
                    r.sadd(pool_registry_key, pool_index_key)

            return note
        except Exception as e:
            logging.error(f"NoteService: Error adding note to {func_id}: {e}")
            return None

    def update_note(self, collection, func_id, note_id, text):
        """Updates an existing note's text."""
        collection = _normalize_collection(collection, func_id)
        doc_id = self._resolve_func_id(collection, func_id)

        try:
            json_field = "notes"
            doc = self._get_doc(doc_id)
            if not doc:
                return None

            notes = doc.get(json_field, [])
            for note in notes:
                if note["id"] == note_id:
                    note["text"] = text
                    note["timestamp"] = int(time.time() * 1000)
                    doc[json_field] = notes
                    self._set_doc(doc_id, doc)
                    return note
            return None
        except Exception as e:
            logging.error(f"NoteService: Error updating note {note_id}: {e}")
            return None

    def remove_note(self, collection, func_id, note_id):
        """Removes a note and updates indices if necessary."""
        collection = _normalize_collection(collection, func_id)
        r = self.r
        doc_id = self._resolve_func_id(collection, func_id)

        try:
            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"

            doc = self._get_doc(doc_id)
            if not doc:
                return False

            notes = doc.get(json_field, [])
            new_notes = [n for n in notes if n["id"] != note_id]

            if len(new_notes) == len(notes):
                return False

            doc[json_field] = new_notes
            doc[count_field] = len(new_notes)

            # Update owners index if this was the last note by this owner
            remaining_owners = set(n["owner"] for n in new_notes)
            old_owners = set(n["owner"] for n in notes)
            removed_owners = old_owners - remaining_owners

            doc[owner_field] = list(remaining_owners)
            self._set_doc(doc_id, doc)

            indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
            for owner in removed_owners:
                index_key = f"{collection}:idx:func:note_owners:{owner.lower()}"
                r.srem(index_key, indexed_id)
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:func:note_owners:{owner.lower()}"
                    r.srem(pool_index_key, indexed_id)

            return True
        except Exception as e:
            logging.error(f"NoteService: Error removing note {note_id}: {e}")
            return False

    def get_notes(self, collection, func_id):
        """Returns all notes for a function."""
        collection = _normalize_collection(collection, func_id)
        doc_id = self._resolve_func_id(collection, func_id)
        json_field = "notes"
        try:
            doc = self._get_doc(doc_id)
            if not doc:
                return []
            return doc.get(json_field, [])
        except:
            return []

    # --- File notes ---

    def _resolve_file_id(self, collection, file_id):
        """Resolves a file ID into its meta document key."""
        collection = _normalize_collection(collection, file_id)
        resolved_id = file_id
        if ":col:" in resolved_id:
            resolved_id = resolved_id.split(":col:")[-1]
        if resolved_id.endswith(":meta"):
            return resolved_id
        return f"{resolved_id}:meta"

    def add_file_note(self, collection, file_id, text, owner="user"):
        """Adds a note to a file and updates indices."""
        collection = _normalize_collection(collection, file_id)
        r = self.r
        text = text.strip()
        if not text:
            return None

        doc_id = self._resolve_file_id(collection, file_id)
        note = {
            "id": str(uuid.uuid4()),
            "text": text,
            "owner": owner,
            "timestamp": int(time.time() * 1000),
        }

        try:
            doc = self._get_doc(doc_id)
            if not doc:
                logging.error(f"NoteService: File document {doc_id} not found.")
                return None

            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"

            notes = doc.get(json_field, [])
            if not isinstance(notes, list):
                notes = []
            notes.append(note)
            doc[json_field] = notes
            doc[count_field] = len(notes)

            owners = doc.get(owner_field, [])
            if not isinstance(owners, list):
                owners = []

            if owner not in owners:
                owners.append(owner)
                doc[owner_field] = owners

            self._set_doc(doc_id, doc)

            # Indexing
            if owner not in doc.get(owner_field, []) or True:
                indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                index_key = f"{collection}:idx:file:note_owners:{owner.lower()}"
                r.sadd(index_key, indexed_id)

                registry_key = f"{collection}:reg:file:note_owners"
                r.sadd(registry_key, index_key)

                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:file:note_owners:{owner.lower()}"
                    r.sadd(pool_index_key, indexed_id)
                    pool_registry_key = f"{pool_coll}:reg:file:note_owners"
                    r.sadd(pool_registry_key, pool_index_key)

            return note
        except Exception as e:
            logging.error(f"NoteService: Error adding file note to {file_id}: {e}")
            return None

    def update_file_note(self, collection, file_id, note_id, text):
        """Updates an existing file note's text."""
        collection = _normalize_collection(collection, file_id)
        doc_id = self._resolve_file_id(collection, file_id)
        json_field = "notes"

        try:
            doc = self._get_doc(doc_id)
            if not doc:
                return None
            notes = doc.get(json_field, [])
            for note in notes:
                if note["id"] == note_id:
                    note["text"] = text
                    note["timestamp"] = int(time.time() * 1000)
                    doc[json_field] = notes
                    self._set_doc(doc_id, doc)
                    return note
            return None
        except Exception as e:
            logging.error(f"NoteService: Error updating file note {note_id}: {e}")
            return None

    def remove_file_note(self, collection, file_id, note_id):
        """Removes a file note and updates indices if necessary."""
        collection = _normalize_collection(collection, file_id)
        r = self.r
        doc_id = self._resolve_file_id(collection, file_id)
        json_field = "notes"
        count_field = "note_count"
        owner_field = "note_owners"

        try:
            doc = self._get_doc(doc_id)
            if not doc:
                return False
            notes = doc.get(json_field, [])
            new_notes = [n for n in notes if n["id"] != note_id]

            if len(new_notes) == len(notes):
                return False

            doc[json_field] = new_notes
            doc[count_field] = len(new_notes)

            remaining_owners = set(n["owner"] for n in new_notes)
            old_owners = set(n["owner"] for n in notes)
            removed_owners = old_owners - remaining_owners

            doc[owner_field] = list(remaining_owners)
            self._set_doc(doc_id, doc)

            indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
            for owner in removed_owners:
                index_key = f"{collection}:idx:file:note_owners:{owner.lower()}"
                r.srem(index_key, indexed_id)
                associated_pools = r.smembers(f"{collection}:pools")
                for p_id in associated_pools:
                    p_id = p_id.decode() if isinstance(p_id, bytes) else p_id
                    pool_coll = f"global:pool:{p_id}"
                    pool_index_key = f"{pool_coll}:idx:file:note_owners:{owner.lower()}"
                    r.srem(pool_index_key, indexed_id)

            return True
        except Exception as e:
            logging.error(f"NoteService: Error removing file note {note_id}: {e}")
            return False

    def get_file_notes(self, collection, file_id):
        """Returns all notes for a file."""
        collection = _normalize_collection(collection, file_id)
        doc_id = self._resolve_file_id(collection, file_id)
        json_field = "notes"
        try:
            doc = self._get_doc(doc_id)
            if not doc:
                return []
            return doc.get(json_field, [])
        except:
            return []


note_service = NoteService()
