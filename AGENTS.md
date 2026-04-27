# AGENTS.md

## Ports
Kvrocks : 6666 -> storage of functions, binaries and similarities
Redis : 6379 -> Job queue only
API : 5000 -> localhost:5000/api

## Databases tip

Never use `keys` or other commands that might freeze the Kvrocks database.
The database holds millions of similarities. 
Dont whipeout the database.
Dont change database structure unless user asks.

### Kvrocks db
Indexes, and inverse indexes are stored in [collection]:idx:[level]:[field]:[value]
Like `main_collection:idx:file:file_name:file.exe`

Registries hold all the key of indexes, for quick search : [collection]:reg:[level]:[field]
Like `main_collection:reg:file:file_name`

This doesnt apply to global indexes and registries which : 

### Redis db

Since its only for jobs, the jobs are in : 

| Key Pattern | Type | Description |
|:--- |:--- |:--- |
| `global:job:{id}` | **Hash** | Status and metadata for a background job. |
| `global:pipeline:{id}:jobs` | **List** | Ordered list of job IDs for a multi-step pipeline. |

## Contributions

Always minimal code change unless user asks drastic change.
Dont be destructive of features when building new.
Comments must be simple, they are only required for complex code
Use `black .` to clear up python synthax.
