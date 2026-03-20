import redis
import argparse
import sys

def delete_by_prefix(host, port, prefix, batch_size, force):
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        # Test connection
        r.ping()
    except redis.ConnectionError:
        print(f"Error: Could not connect to Redis at {host}:{port}")
        sys.exit(1)

    search_pattern = f"{prefix}*"
    
    # 1. Count check (Optional but helpful for safety)
    print(f"[*] Scanning for keys matching: '{search_pattern}'...")
    
    # We use a list to store keys for the final confirmation if not forced
    if not force:
        confirm = input(f" [!] Are you sure you want to delete all keys starting with '{prefix}'? (y/N): ")
        if confirm.lower() != 'y':
            print("[-] Operation cancelled.")
            return

    count = 0
    pipe = r.pipeline()
    
    for key in r.scan_iter(match=search_pattern, count=batch_size):
        pipe.delete(key)
        count += 1
        
        if count % batch_size == 0:
            pipe.execute()
            print(f" [i] Deleted {count} keys...")

    # Final sweep
    pipe.execute()
    print(f"\n[+] Success! Total keys removed: {count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete Redis keys by prefix safely.")
    
    # Arguments
    parser.add_argument("--prefix", type=str, required=True, help="The prefix to match (e.g., 'main:file:')")
    parser.add_argument("--host", type=str, default="localhost", help="Redis host (default: localhost)")
    parser.add_argument("--port", type=int, default=6667, help="Redis port (default: 6667)")
    parser.add_argument("--batch", type=int, default=1000, help="Number of keys to delete per batch (default: 1000)")
    parser.add_argument("--force", action="store_true", help="Skip confirmation prompt")

    args = parser.parse_args()

    delete_by_prefix(args.host, args.port, args.prefix, args.batch, args.force)