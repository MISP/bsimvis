import uuid
import time
import logging
from .redis_client import get_redis
from bsimvis.app.services.index_service import get_pool_id


def _normalize_collection(collection):
    pool_id = get_pool_id(collection)
    return f"global:pool:{pool_id}" if pool_id else collection


class NoteService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _resolve_func_id(self, collection, entity_id):
        """Resolves a function ID into its meta document key."""
        collection = _normalize_collection(collection)
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
        orig_collection = collection
        collection = _normalize_collection(collection)
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
            doc_raw = r.json().get(doc_id, "$")
            if not doc_raw:
                logging.error(f"NoteService: Document {doc_id} not found.")
                return None

            doc = doc_raw[0] if isinstance(doc_raw, list) else doc_raw

            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"
            pool_id = get_pool_id(collection)
            if pool_id:
                json_field = f"pool_notes_{pool_id}"
                count_field = f"pool_note_count_{pool_id}"
                owner_field = f"pool_note_owners_{pool_id}"

            # 2. Update Notes
            notes = doc.get(json_field, [])
            if not isinstance(notes, list):
                notes = []
            notes.append(note)
            r.json().set(doc_id, f"$.{json_field}", notes)
            r.json().set(doc_id, f"$.{count_field}", len(notes))

            # 3. Update note_owners for indexing
            owners = doc.get(owner_field, [])
            if not isinstance(owners, list):
                owners = []

            if owner not in owners:
                owners.append(owner)
                r.json().set(doc_id, f"$.{owner_field}", owners)

                # Standard Bucket: {col}:idx:func:note_owners:{owner}
                indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                index_key = f"{orig_collection}:idx:func:note_owners:{owner.lower()}"
                r.sadd(index_key, indexed_id)

                # Standard Registry: {col}:reg:func:note_owners
                registry_key = f"{orig_collection}:reg:func:note_owners"
                r.sadd(registry_key, index_key)

                # Also index in pool namespace if pool exists
                pool_id = get_pool_id(orig_collection)
                if pool_id:
                    pool_coll = f"global:pool:{pool_id}"
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
        collection = _normalize_collection(collection)
        r = self.r
        doc_id = self._resolve_func_id(collection, func_id)

        try:
            json_field = "notes"
            pool_id = get_pool_id(collection)
            if pool_id:
                json_field = f"pool_notes_{pool_id}"

            notes = r.json().get(doc_id, f"$.{json_field}")[0]
            for note in notes:
                if note["id"] == note_id:
                    note["text"] = text
                    note["timestamp"] = int(time.time() * 1000)
                    r.json().set(doc_id, f"$.{json_field}", notes)
                    return note
            return None
        except Exception as e:
            logging.error(f"NoteService: Error updating note {note_id}: {e}")
            return None

    def remove_note(self, collection, func_id, note_id):
        """Removes a note and updates indices if necessary."""
        orig_collection = collection
        collection = _normalize_collection(collection)
        r = self.r
        doc_id = self._resolve_func_id(collection, func_id)

        try:
            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"
            pool_id = get_pool_id(collection)
            if pool_id:
                json_field = f"pool_notes_{pool_id}"
                count_field = f"pool_note_count_{pool_id}"
                owner_field = f"pool_note_owners_{pool_id}"

            notes = r.json().get(doc_id, f"$.{json_field}")[0]
            new_notes = [n for n in notes if n["id"] != note_id]

            if len(new_notes) == len(notes):
                return False

            r.json().set(doc_id, f"$.{json_field}", new_notes)
            r.json().set(doc_id, f"$.{count_field}", len(new_notes))

            # Update owners index if this was the last note by this owner
            remaining_owners = set(n["owner"] for n in new_notes)
            old_owners = set(n["owner"] for n in notes)
            removed_owners = old_owners - remaining_owners

            r.json().set(doc_id, f"$.{owner_field}", list(remaining_owners))

            indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
            pool_id = get_pool_id(orig_collection)
            for owner in removed_owners:
                index_key = f"{orig_collection}:idx:func:note_owners:{owner.lower()}"
                r.srem(index_key, indexed_id)
                if pool_id:
                    pool_coll = f"global:pool:{pool_id}"
                    pool_index_key = f"{pool_coll}:idx:func:note_owners:{owner.lower()}"
                    r.srem(pool_index_key, indexed_id)

            return True
        except Exception as e:
            logging.error(f"NoteService: Error removing note {note_id}: {e}")
            return False

    def get_notes(self, collection, func_id):
        """Returns all notes for a function."""
        collection = _normalize_collection(collection)
        doc_id = self._resolve_func_id(collection, func_id)
        pool_id = get_pool_id(collection)
        json_field = f"pool_notes_{pool_id}" if pool_id else "notes"
        try:
            notes = self.r.json().get(doc_id, f"$.{json_field}")
            return notes[0] if notes else []
        except:
            return []

    # --- File notes ---

    def _resolve_file_id(self, collection, file_id):
        """Resolves a file ID into its meta document key."""
        collection = _normalize_collection(collection)
        # Accepts: {col}:file:{md5} or {col}:file:{md5}:meta
        if file_id.endswith(":meta"):
            return file_id
        return f"{file_id}:meta"

    def add_file_note(self, collection, file_id, text, owner="user"):
        """Adds a note to a file and updates indices."""
        orig_collection = collection
        collection = _normalize_collection(collection)
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
            doc_raw = r.json().get(doc_id, "$")
            if not doc_raw:
                logging.error(f"NoteService: File document {doc_id} not found.")
                return None

            doc = doc_raw[0] if isinstance(doc_raw, list) else doc_raw

            json_field = "notes"
            count_field = "note_count"
            owner_field = "note_owners"
            pool_id = get_pool_id(collection)
            if pool_id:
                json_field = f"pool_notes_{pool_id}"
                count_field = f"pool_note_count_{pool_id}"
                owner_field = f"pool_note_owners_{pool_id}"

            notes = doc.get(json_field, [])
            if not isinstance(notes, list):
                notes = []
            notes.append(note)
            r.json().set(doc_id, f"$.{json_field}", notes)
            r.json().set(doc_id, f"$.{count_field}", len(notes))

            owners = doc.get(owner_field, [])
            if not isinstance(owners, list):
                owners = []

            if owner not in owners:
                owners.append(owner)
                r.json().set(doc_id, f"$.{owner_field}", owners)

                # Index: {col}:idx:file:note_owners:{owner}
                indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
                index_key = f"{orig_collection}:idx:file:note_owners:{owner.lower()}"
                r.sadd(index_key, indexed_id)

                registry_key = f"{orig_collection}:reg:file:note_owners"
                r.sadd(registry_key, index_key)

                # Also index in pool namespace if pool exists
                pool_id = get_pool_id(orig_collection)
                if pool_id:
                    pool_coll = f"global:pool:{pool_id}"
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
        collection = _normalize_collection(collection)
        r = self.r
        doc_id = self._resolve_file_id(collection, file_id)
        pool_id = get_pool_id(collection)
        json_field = f"pool_notes_{pool_id}" if pool_id else "notes"

        try:
            notes = r.json().get(doc_id, f"$.{json_field}")[0]
            for note in notes:
                if note["id"] == note_id:
                    note["text"] = text
                    note["timestamp"] = int(time.time() * 1000)
                    r.json().set(doc_id, f"$.{json_field}", notes)
                    return note
            return None
        except Exception as e:
            logging.error(f"NoteService: Error updating file note {note_id}: {e}")
            return None

    def remove_file_note(self, collection, file_id, note_id):
        """Removes a file note and updates indices if necessary."""
        orig_collection = collection
        collection = _normalize_collection(collection)
        r = self.r
        doc_id = self._resolve_file_id(collection, file_id)
        pool_id = get_pool_id(collection)
        json_field = f"pool_notes_{pool_id}" if pool_id else "notes"
        count_field = f"pool_note_count_{pool_id}" if pool_id else "note_count"
        owner_field = f"pool_note_owners_{pool_id}" if pool_id else "note_owners"

        try:
            notes = r.json().get(doc_id, f"$.{json_field}")[0]
            new_notes = [n for n in notes if n["id"] != note_id]

            if len(new_notes) == len(notes):
                return False

            r.json().set(doc_id, f"$.{json_field}", new_notes)
            r.json().set(doc_id, f"$.{count_field}", len(new_notes))

            remaining_owners = set(n["owner"] for n in new_notes)
            old_owners = set(n["owner"] for n in notes)
            removed_owners = old_owners - remaining_owners

            r.json().set(doc_id, f"$.{owner_field}", list(remaining_owners))

            indexed_id = doc_id[:-5] if doc_id.endswith(":meta") else doc_id
            pool_id = get_pool_id(orig_collection)
            for owner in removed_owners:
                index_key = f"{orig_collection}:idx:file:note_owners:{owner.lower()}"
                r.srem(index_key, indexed_id)
                if pool_id:
                    pool_coll = f"global:pool:{pool_id}"
                    pool_index_key = f"{pool_coll}:idx:file:note_owners:{owner.lower()}"
                    r.srem(pool_index_key, indexed_id)

            return True
        except Exception as e:
            logging.error(f"NoteService: Error removing file note {note_id}: {e}")
            return False

    def get_file_notes(self, collection, file_id):
        """Returns all notes for a file."""
        collection = _normalize_collection(collection)
        doc_id = self._resolve_file_id(collection, file_id)
        pool_id = get_pool_id(collection)
        json_field = f"pool_notes_{pool_id}" if pool_id else "notes"
        try:
            notes = self.r.json().get(doc_id, f"$.{json_field}")
            return notes[0] if notes else []
        except:
            return []


note_service = NoteService()
