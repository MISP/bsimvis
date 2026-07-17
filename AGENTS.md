# AGENTS.md

## Python

Use uv run to run


## Ports
Configurable via `.env` file:
- Kvrocks : `KVROCKS_PORT` (default: 6666) -> storage of functions, binaries and similarities
- Redis : `REDIS_PORT` (default: 6379) -> Job queue only
- API : `APP_PORT` (default: 5000) -> localhost:5000/api

Hosts are also configurable via `KVROCKS_HOST`, `REDIS_HOST`, and `APP_HOST`.

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

## Worktree testing

Never read `data/kvrocks/` or `hs_err_pid*.log` — confidential (real binary md5s /
function data). Tests use only the git-tracked `data/test/` fixtures.

In a linked worktree, run `./scripts/wt-test.sh` before committing. It symlinks
`bin/` from the main repo (never recompiled — 1.4G of downloaded tools), writes an
isolated `.env` (own `PROJECT_NAME` + offset ports + fresh local data dir, so it can
run alongside the main stack without touching its confidential DB), launches the full
stack via `launch_tmux.sh`, runs `test_api_endpoints.py`, and tears
down. Do NOT commit if it prints `RESULT: FAIL` or the run was skipped. Show the output.

## Contributions

Always minimal code change unless user asks drastic change.
Dont be destructive of features when building new.
Comments must be simple, they are only required for complex code
Use `uv run black .` to clear up python synthax.

## API Development

The backend API uses Flask-RESTX for routing and Swagger documentation.
- **Serialization**: Do not mix `jsonify()` and Flask-RESTX `api.model` marshaling. When using Flask-RESTX `@api.response`, simply return a Python dictionary (e.g., `return {"status": "success"}`) instead of `jsonify(...)` to avoid double-serialization bugs or missing headers.
- **Endpoints**: Routes are defined in `bsimvis/app/swagger.py` and import their implementation from `bsimvis/app/routes/`.
- **Validation**: Rely on Swagger doc and `@api.expect` for parameter validation and schema definition.
