"""Small helpers for address-scoped similarity maintenance."""

import json

from bsimvis.app.services.index_service import delete_similarity


class MaintenanceService:
    def __init__(self, redis_client, similarity_service):
        self.r = redis_client
        self.similarity_service = similarity_service

    def _functions(self, collection, md5=None, batch_uuid=None, address=None):
        selected = None

        def narrow(values):
            nonlocal selected
            values = {v.decode() if isinstance(v, bytes) else v for v in values}
            selected = values if selected is None else selected & values

        if md5:
            narrow(self.r.smembers(f"{collection}:idx:file:functions:{md5}"))
        if batch_uuid:
            narrow(self.r.smembers(f"{collection}:batch:{batch_uuid}:functions"))
        if address:
            narrow(
                self.r.smembers(
                    f"{collection}:idx:func:entrypoint_address:{str(address).lower()}"
                )
            )
        return sorted(selected or ())

    def build(self, collection, md5=None, batch_uuid=None, address=None, **kwargs):
        for fid in self._functions(collection, md5, batch_uuid, address):
            self.similarity_service.build_function(collection, fid, **kwargs)
        return True

    def clear(self, collection, md5=None, batch_uuid=None, address=None, algo=None):
        function_ids = self._functions(collection, md5, batch_uuid, address)
        prefix = f"{collection}:func:"
        sids = set()
        for fid in function_ids:
            clean = fid[len(prefix) :] if fid.startswith(prefix) else fid
            sids.update(self.r.smembers(f"{collection}:sim:involves:func:{clean}"))
        reader = self.r.pipeline(transaction=False)
        sids = [sid.decode() if isinstance(sid, bytes) else sid for sid in sids]
        for sid in sids:
            reader.get(sid)
        pipe = self.r.pipeline(transaction=False)
        for sid, raw in zip(sids, reader.execute()):
            try:
                doc = json.loads(raw) if raw else None
            except (TypeError, ValueError):
                doc = None
            if not doc or (algo and doc.get("algo", "unweighted_cosine") != algo):
                continue
            current_algo = doc.get("algo", "unweighted_cosine")
            for fid in (doc.get("id1"), doc.get("id2")):
                if fid:
                    clean = fid[len(prefix) :] if fid.startswith(prefix) else fid
                    pipe.srem(f"{collection}:sim:involves:func:{clean}", sid)
            for file_md5 in (doc.get("md5_1"), doc.get("md5_2")):
                if file_md5:
                    pipe.srem(f"{collection}:sim:involves:file:{file_md5}", sid)
            pipe.zrem(f"{collection}:sim:score:{current_algo}", sid)
            delete_similarity(pipe, collection, sid, doc)
            pipe.delete(sid)
        for fid in function_ids:
            pipe.srem(
                f"{collection}:built:functions:{algo or 'unweighted_cosine'}", fid
            )
        pipe.execute()
        return True
