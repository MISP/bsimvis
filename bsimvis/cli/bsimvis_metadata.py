import csv
import logging
import requests
import sys
from bsimvis.app.services.index_service import parse_timestamp
from bsimvis.app.services.metadata_service import normalize_collection_timestamps


def parse_seen(val, reducer):
    values = [parse_timestamp(v.strip()) for v in (val or "").split(",")]
    values = [v for v in values if v]
    return reducer(values) if values else None


def parse_metadata_file(filepath):
    """
    Parses a pipe-delimited metadata CSV file into a dictionary of updates.
    """
    updates = {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter="|")
            reader.fieldnames = [n.strip() for n in reader.fieldnames]
            for row in reader:
                hash_val = row.get("HASH", "").strip()
                if not hash_val:
                    continue

                def parse_list(val):
                    if not val or val.strip() == "-":
                        return []
                    return [v.strip() for v in val.split(",")]

                names = parse_list(row.get("names", ""))
                extra = {
                    "filetype": parse_list(row.get("filetype", "")),
                    "avtype": parse_list(row.get("avtype", "")),
                    "yara": parse_list(row.get("yara", "")),
                    "file_names": names,
                    "cc_ip": parse_list(row.get("CC ip", "")),
                }
                first_seen = parse_seen(row.get("first_seen", ""), min)
                last_seen = parse_seen(row.get("last_seen", ""), max)
                if first_seen:
                    extra["first_seen"] = first_seen
                if last_seen:
                    extra["last_seen"] = last_seen
                if names:
                    extra["file_name"] = names[0]
                updates[hash_val] = extra
        return updates
    except Exception as e:
        logging.error(f"[!] Failed to parse metadata file {filepath}: {e}")
        return None


def run_normalize_time(args):
    result = normalize_collection_timestamps(args.collection)
    print(
        f"[+] Normalized {result['files']} files, {result['functions']} functions, {result['batches']} batches."
    )


def run_metadata(host, port, args):
    """
    Submits metadata updates via API for propagation.
    """
    if args.action == "normalize-time":
        return run_normalize_time(args)

    updates = parse_metadata_file(args.metadata)
    if updates is None:
        sys.exit(1)

    if not updates:
        print("[!] No valid metadata found in file.")
        sys.exit(1)

    api_url = f"http://{host}:{port}/api/file/metadata/propagate"
    payload = {"collection": args.collection, "updates": updates}

    try:
        print(f"[*] Submitting updates for {len(updates)} files to {api_url}...")
        resp = requests.post(api_url, json=payload)
        resp.raise_for_status()
        res = resp.json()
        print(f"[+] Success! Metadata propagation job enqueued.")
        print(f"[+] Job ID: {res.get('job_id')}")
    except Exception as e:
        print(f"[!] Failed to submit metadata updates: {e}")
        sys.exit(1)
