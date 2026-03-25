import redis
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, TextField
import argparse

_r = None

def get_redis(host="localhost", port=6666):
    global _r
    if _r is None:
        _r = redis.Redis(host=host, port=port, decode_responses=True)
    return _r

def setup_similarity_index(collection, r=None):
    r = r or get_redis()
    idx_name = f"{collection}:idx:similarities"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' for similarity metadata...")
    
    cmd = [
        "FT.CREATE", idx_name,
        "ON", "JSON",
        "PREFIX", "1", f"{collection}:sim_meta",
        "SCHEMA",
        "type", "TAG",
        "collection", "TAG",
        "algo", "TAG",
        "score", "NUMERIC",
        "id1", "TAG",
        "id2", "TAG",
        "name1", "TAG",
        "name2", "TAG",
        "md5_1", "TAG",
        "md5_2", "TAG",
        "tags1", "TAG",
        "tags2", "TAG",
        "batch_uuid1", "TAG",
        "batch_uuid2", "TAG",
        "language_id1", "TAG",
        "language_id2", "TAG",
        "feat_count1", "NUMERIC",
        "feat_count2", "NUMERIC",
        "is_cross_binary", "TAG"
    ]
    try:
        r.execute_command(*cmd)
        print(f"[+] Index '{idx_name}' created successfully.")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

def run_setup(host, port, args):
    r = get_redis(host, port)
    
    collections = []
    if args.index and "all" in args.index:
        # Re-map "all" logic if needed, but usually it's handled by default
        index_types = ["functions", "files", "similarities"]
    else:
        index_types = args.index

    if args.collection:
        collections = [args.collection]
    
    if not collections:
        print("[-] No collections specified.")
        return

    setup_indices(collections, index_types=index_types, r_override=r)

def setup_indices(collections, index_types=None, r_override=None):
    if index_types is None:
        index_types = ['functions', 'files', 'similarities']
        
    print(f"[*] Setting up {index_types} indices for {len(collections)} collections...")
    
    for coll in collections:
        print(f"\n--- Processing Collection: {coll} ---")
        if 'functions' in index_types:
            setup_function_index(coll, r_override)
        if 'files' in index_types:
            setup_file_index(coll, r_override)
        if 'similarities' in index_types:
            setup_similarity_index(coll, r_override)
    
    print("\n[+] All requested indices set up.")

def setup_file_index(collection, r=None):
    r = r or get_redis()
    idx_name = f"{collection}:idx:files"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}'...")

    # Manual command to avoid IndexDefinition/PREFIX/FILTER issues on Kvrocks
    cmd = [
        "FT.CREATE", idx_name, "ON", "JSON", 
        "PREFIX", "1", f"{collection}:file",
        "SCHEMA",
        "type", "TAG",
        "collection", "TAG",
        "batch_uuid", "TAG",
        "batch_order", "NUMERIC",
        "file_md5", "TAG",
        "language_id", "TAG",
        "tags", "TAG",
        "file_name", "TAG",
        "entry_date", "TAG",
        "file_date", "TAG"
    ]
    try:
        r.execute_command(*cmd)
        print(f"[+] Index '{idx_name}' created successfully.")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

def setup_function_index(collection, r=None):
    r = r or get_redis()
    idx_name = f"{collection}:idx:functions"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}'...")
    
    cmd = [
        "FT.CREATE", idx_name,
        "ON", "JSON",
        "PREFIX", "1", f"{collection}:function",
        "SCHEMA",
        "type", "TAG",
        "collection", "TAG",
        "batch_uuid", "TAG",
        "batch_order", "NUMERIC",
        "file_md5", "TAG",
        "language_id", "TAG",
        "tags", "TAG",
        "file_name", "TAG",
        "entry_date", "TAG",
        "file_date", "TAG",
        "function_name", "TAG",
        "decompiler_id", "TAG",
        "return_type", "TAG",
        "calling_convention", "TAG",
        "entrypoint_address", "TAG",
        "instruction_count", "NUMERIC",
        "bsim_features_count", "NUMERIC"
    ]
    try:
        r.execute_command(*cmd)
        print(f"[+] Index '{idx_name}' created successfully.")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

def main():
    parser = argparse.ArgumentParser(description="Setup Redis/Kvrocks search indexes for BSimVis.")
    parser.add_argument("--all", action="store_true", help="Setup for all existing collections in Redis.")
    parser.add_argument("-c", "--collection", nargs="+", help="Specific collection names to setup (even if they don't exist yet).")
    parser.add_argument("-i", "--index", nargs="+", choices=["functions", "files", "similarities"],
                        help="Specific index types to setup. Default is all.")
    
    args = parser.parse_args()
    
    if not args.all and not args.collection:
        parser.print_help()
        exit(0)
        
    collections = []
    if args.all:
        r = get_redis()
        # Kvrocks smembers returns set/list of strings
        collections = list(r.smembers("global:collections"))
        # decode bytes if necessary (depending on redis version/client config)
        collections = [c.decode() if isinstance(c, bytes) else c for c in collections]
        print(f"[*] Found {len(collections)} existing collections: {collections}")
    
    if args.collection:
        # Avoid duplicates if --all and -c are both used
        for c in args.collection:
            if c not in collections:
                collections.append(c)
    
    if not collections:
        print("[-] No collections found or specified. Nothing to do.")
        exit(0)
        
    setup_indices(collections, index_types=args.index)

if __name__ == "__main__":
    main()