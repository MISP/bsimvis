from bsimvis.app.services.redis_client import get_redis
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, TextField

def setup_indices():
    print("Setting up indices")
    setup_function_index()
    setup_file_index()
    
    print("Indices set up")

def setup_file_index():
    r = get_redis()
    idx_name = "idx:files"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")
    
    # Manual command to avoid IndexDefinition/PREFIX/FILTER issues on Kvrocks
    cmd = [
        "FT.CREATE", idx_name, "ON", "JSON", "SCHEMA",
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

def setup_function_index():
    r = get_redis()
    idx_name = "idx:functions"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")
    
    # Manual command to avoid IndexDefinition/PREFIX/FILTER issues on Kvrocks
    cmd = [
        "FT.CREATE", idx_name,
        "ON", "JSON",
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
        print(f"[+] Index '{idx_name}' created successfully.")
    except Exception as e:
        print(f"[-] Failed to create index '{idx_name}': {e}")

if __name__ == "__main__":
    setup_indices()