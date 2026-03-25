from bsimvis.app.services.redis_client import get_redis
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, TextField
import argparse

def setup_similarity_index(collections):
    r = get_redis()
    idx_name = "idx:similarities"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' for similarity metadata...")
    
    # We use explicit prefixes to avoid scanning non-JSON keys (like 'global:collections') 
    # which causes WRONGTYPE errors on Kvrocks.
    prefixes = [f"{c}:sim_meta" for c in collections]
    
    cmd = [
        "FT.CREATE", idx_name,
        "ON", "JSON",
        "PREFIX", str(len(prefixes))
    ] + prefixes + [
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
        print(f"[+] Index '{idx_name}' created successfully with prefixes: {prefixes}")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

def setup_indices(collections, index_types=None):
    if index_types is None:
        index_types = ['functions', 'files', 'similarities']
        
    print(f"Setting up {index_types} indices for collections: {collections}")
    
    if 'functions' in index_types:
        setup_function_index(collections)
    if 'files' in index_types:
        setup_file_index(collections)
    if 'similarities' in index_types:
        setup_similarity_index(collections)
    
    print("Indices set up")

def setup_file_index(collections):
    r = get_redis()
    idx_name = "idx:files"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")

    prefixes = [f"{c}:file" for c in collections]

    # Manual command to avoid IndexDefinition/PREFIX/FILTER issues on Kvrocks
    cmd = [
        "FT.CREATE", idx_name, "ON", "JSON", 
        "PREFIX", str(len(prefixes))
    ] + prefixes + [
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
        print(f"[+] Index '{idx_name}' created successfully with prefixes: {prefixes}")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

def setup_function_index(collections):
    r = get_redis()
    idx_name = "idx:functions"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")
    
    prefixes = [f"{c}:function" for c in collections]

    # Manual command to avoid IndexDefinition/PREFIX/FILTER issues on Kvrocks
    cmd = [
        "FT.CREATE", idx_name,
        "ON", "JSON",
        "PREFIX", str(len(prefixes))
    ] + prefixes + [
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
        #"is_thunk", "TAG",
        "decompiler_id", "TAG",
        "return_type", "TAG",
        "calling_convention", "TAG",
        "entrypoint_address", "TAG",
        "instruction_count", "NUMERIC",
        "bsim_features_count", "NUMERIC"
    ]
    try:
        r.execute_command(*cmd)
        print(f"[+] Index '{idx_name}' created successfully with prefixes: {prefixes}")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

if __name__ == "__main__":
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