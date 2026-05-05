import logging
import sys
import os
from bsimvis.app.services.redis_client import get_redis
from bsimvis.app.services.milvus_service import milvus_service

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def sync_collection(collection_name):
    r = get_redis()
    logging.info(f"[*] Starting sync for collection: {collection_name}")
    success = milvus_service.sync_collection(collection_name, r)
    if success:
        logging.info("[+] Sync completed successfully!")
    else:
        logging.error("[!] Sync failed or resulted in empty collections.")


if __name__ == "__main__":
    coll = sys.argv[1] if len(sys.argv) > 1 else "mirai"
    sync_collection(coll)
