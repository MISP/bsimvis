from bsimvis.app.services.redis_client import get_redis
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.field import TagField, TextField
def setup_indices():
    setup_file_index()
    setup_function_index()

def setup_file_index():
    r = get_redis()
    idx_name = "idx:files"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")
    
    schema = (
        # 1. Identifiers
        TagField("$.collection", as_name="collection"), 
        TagField("$.batch-uuid", as_name="batch_uuid"),
        TagField("$.file-md5", as_name="file_md5"),       # Was 'md5'
        TagField("$.language-id", as_name="language_id"), # Was 'lang_id'
        
        # 2. Collections & Tags
        TagField("$.tags[*]", as_name="tag"),
        
        # 3. Searchable Text
        TextField("$.file-name", as_name="file_name", sortable=True), # Was 'name'
        
        # 4. Dates
        TextField("$.entry-date", as_name="entry_date", sortable=True),
        TextField("$.file-date", as_name="file_date", sortable=True)
    )

    # Prefix remains [":file:"] to isolate from function data
    r.ft(idx_name).create_index(
        schema, 
        definition=IndexDefinition(
            prefix=[],
            filter="contains(@__key, ':file:')",
            index_type=IndexType.JSON
        )
    )

def setup_function_index():
    r = get_redis()
    idx_name = "idx:functions"
    
    try:
        r.ft(idx_name).dropindex(delete_documents=False)
    except:
        pass
    
    print(f"[*] Creating index '{idx_name}' with comprehensive filters...")
    
    schema = (
        # 1. Identifiers
        TagField("$.collection", as_name="collection"), 
        TagField("$.batch-uuid", as_name="batch_uuid"),
        TagField("$.file-md5", as_name="file_md5"),       # Was 'md5'
        TagField("$.language-id", as_name="language_id"), # Was 'lang_id'
        
        # 2. Collections & Tags
        TagField("$.tags[*]", as_name="tag"),
        
        # 3. Searchable Text
        TextField("$.file-name", as_name="file_name", sortable=True), # Was 'name'
        
        # 4. Dates
        TextField("$.entry-date", as_name="entry_date", sortable=True),
        TextField("$.file-date", as_name="file_date", sortable=True),


        # Function metadatas
        TextField("$.function-name", as_name="function_name", sortable=True),
        TagField("$.is-thunk", as_name="is_thunk"),
        
        # decompiler-id (TagField for exact matches on IDs with special characters)
        TagField("$.decompiler-id", as_name="decompiler_id"),
        TextField("$.return-type", as_name="return_type", sortable=True),
        TextField("$.calling-convention", as_name="calling_convention", sortable=True),
        TextField("$.entrypoint-address", as_name="entrypoint_address", sortable=True),
    )

    # Prefix remains [":file:"] to isolate from function data
    r.ft(idx_name).create_index(
        schema, 
        definition=IndexDefinition(
            prefix=[""],
            filter="contains(@__key, ':function:')", 
            index_type=IndexType.JSON
        )
    )

if __name__ == "__main__":
    setup_indices()