import sys
import argparse
import hashlib
import logging
from tqdm import tqdm
from bsimvis.app.services.redis_client import get_redis

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


def compute_lsh_buckets(features_raw, num_bands, rows_per_band):
    """Generates SimHash LSH buckets for a set of features."""
    num_features = num_bands * rows_per_band
    if not features_raw:
        return []

    projections = [0.0] * num_features
    for f_hash, f_tf_raw in features_raw:
        f = f_hash.decode() if isinstance(f_hash, bytes) else str(f_hash)
        tf = float(f_tf_raw)
        for j in range(num_features):
            h = int(hashlib.md5(f"{f}:{j}".encode()).hexdigest(), 16)
            weight = 1.0 if (h % 2 == 1) else -1.0
            projections[j] += tf * weight

    sig = [1 if val >= 0 else 0 for val in projections]

    buckets = []
    for band in range(num_bands):
        start = band * rows_per_band
        band_sig = sig[start : start + rows_per_band]
        band_str = "".join(map(str, band_sig))
        bucket_hash = hashlib.md5(band_str.encode()).hexdigest()
        buckets.append((band, bucket_hash))
    return buckets


def main():
    parser = argparse.ArgumentParser(
        description="Populate/Update LSH buckets for an existing collection."
    )
    parser.add_argument(
        "-c", "--collection", required=True, help="Target collection name"
    )
    parser.add_argument(
        "-b", "--bands", type=int, default=30, help="Number of LSH bands (default: 30)"
    )
    parser.add_argument(
        "-r", "--rows", type=int, default=4, help="Rows per band (default: 4)"
    )
    parser.add_argument(
        "--clear-only",
        action="store_true",
        help="Only clear existing LSH keys without rebuilding",
    )

    args = parser.parse_args()

    r = get_redis()
    collection = args.collection
    num_bands = args.bands
    rows_per_band = args.rows

    # 1. Check if collection has functions
    indexed_set_key = f"{collection}:indexed:functions"
    total_funcs = r.scard(indexed_set_key)
    if total_funcs == 0:
        logging.error(
            f"No indexed functions found in collection '{collection}' (key: {indexed_set_key})"
        )
        sys.exit(1)

    logging.info(
        f"[*] Found {total_funcs} indexed functions in collection '{collection}'"
    )

    # 2. Clean up any existing LSH bucket keys for the collection
    logging.info(
        f"[*] Clearing existing LSH bucket keys matching '{collection}:lsh:*'..."
    )
    cursor = 0
    clear_count = 0
    while True:
        cursor, keys = r.scan(cursor, match=f"{collection}:lsh:*", count=1000)
        if keys:
            r.delete(*keys)
            clear_count += len(keys)
        if cursor == 0:
            break
    logging.info(f"[+] Cleared {clear_count} existing LSH keys.")

    if args.clear_only:
        logging.info("[+] Done (clear-only).")
        sys.exit(0)

    # 3. Retrieve functions and compute LSH in batches
    function_ids = list(r.smembers(indexed_set_key))
    batch_size = 100
    logging.info(
        f"[*] Generating LSH buckets with parameters: b={num_bands}, r={rows_per_band} (signature size={num_bands * rows_per_band} bits)..."
    )

    for i in tqdm(range(0, len(function_ids), batch_size), desc="Processing functions"):
        chunk = function_ids[i : i + batch_size]

        # Fetch vector features for chunk
        pipe = r.pipeline()
        for fid in chunk:
            # Delete old bucket keys
            for band in range(num_bands):
                pipe.delete(f"{fid}:lsh:bucket_key:{band}")
            # Fetch tf vectors
            pipe.zrange(f"{fid}:vec:tf", 0, -1, withscores=True)

        pipe_results = pipe.execute()

        # Save LSH buckets
        save_pipe = r.pipeline()
        for idx, fid in enumerate(chunk):
            # zrange output is at index: idx * (num_bands + 1) + num_bands
            features = pipe_results[idx * (num_bands + 1) + num_bands]
            if not features:
                continue

            buckets = compute_lsh_buckets(features, num_bands, rows_per_band)
            for band, b_hash in buckets:
                bucket_key = f"{collection}:lsh:bucket:{band}:{b_hash}"
                save_pipe.sadd(bucket_key, fid)
                save_pipe.set(f"{fid}:lsh:bucket_key:{band}", bucket_key)

        save_pipe.execute()

    logging.info(
        f"[+] LSH buckets successfully populated for {total_funcs} functions in '{collection}'!"
    )


if __name__ == "__main__":
    main()
