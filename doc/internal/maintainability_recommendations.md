# BSimVis maintainability and sustainability recommendations

This review focuses on making BSimVis easier to maintain after the internship period, while preserving the current research and analyst workflow. The project already has a useful separation between API routes, service modules, CLI commands, Lua scripts, and static frontend assets, plus domain documentation for the API, Kvrocks schema, and similarity filtering. The recommendations below prioritize small, incremental changes first, then larger long-term investments.

## Review scope and code signals

Reviewed areas:

- Application bootstrap and operational defaults: `bsimvis/app/__init__.py`, `app.py`, `launch.sh`, `docker-compose.yml`, `.env.example`.
- API layer: `bsimvis/app/swagger.py` and `bsimvis/app/routes/`.
- Service and persistence layer: `bsimvis/app/services/`, `bsimvis/app/lua/`, and `doc/kvrocks_database_structure.md`.
- CLI, worker, and ingestion flow: `bsimvis/cli/`, `bsimvis/worker.py`, `test_api_endpoints.py`.
- Frontend assets: `bsimvis/app/static/js/`, `bsimvis/app/static/css/`, and HTML pages.
- Dependency and packaging metadata: `pyproject.toml`, `requirements.txt`, `uv.lock`.

Important sustainability signals:

- Several modules have become large coordination points: `bsimvis/app/swagger.py`, `bsimvis/app/routes/search_similarity.py`, `bsimvis/app/static/js/dashboard.js`, and `bsimvis/app/static/js/cluster_views.js`.
- Runtime dependencies are split between `pyproject.toml`, `requirements.txt`, `uv.lock`, install scripts, Docker Compose images, and locally installed Ghidra/Kvrocks/Redis binaries.
- The application stores high-value analysis data in Kvrocks with many handwritten key patterns, indexes, Lua scripts, and propagated metadata.
- The only visible automated validation is an end-to-end API script, which is useful but expensive and dependent on running services.
- The project is a strong internship prototype with real functionality; the main risk is future maintainers needing to infer architecture and invariants from implementation details.

## Current strengths to preserve

- **Clear domain focus**: the README explains why BSimVis exists and what it adds beyond Ghidra BSim.
- **Documented API and data model**: Swagger routes and the `doc/` directory already document core endpoints, examples, similarity filtering, and Kvrocks storage.
- **Service separation exists**: routes delegate important logic to services such as feature, index, similarity, tag, cluster, processing, and job services.
- **Operational configurability**: ports, hosts, workers, data directories, and optional Milvus can be configured through environment variables.
- **Performance-aware storage choices**: the use of Lua scripts, Redis/Kvrocks sets, sorted sets, registries, and pipelines shows deliberate attention to large collections.
- **CLI and worker workflows**: the CLI, worker, and job queue make batch analysis possible without requiring everything to happen inside the web request lifecycle.

## Main long-run risks

| Risk | Why it matters | Suggested owner |
|:---|:---|:---|
| Knowledge concentrated in large modules | Future maintainers must understand many endpoint, UI, and storage concerns at once. | Core maintainer |
| Weak automated test pyramid | Regressions in key building, filters, Lua behavior, and job transitions may only be found manually. | Core maintainer + contributors |
| Implicit database contracts | Changing key formats or propagated indexes can silently break search, tags, clusters, or enrichment. | Data/API maintainer |
| Unbounded API and upload defaults | Very large request limits, open CORS, and no visible auth/rate limiting are risky outside trusted lab networks. | Operations/security maintainer |
| Dependency and runtime drift | Ghidra, pyghidra, Python, Redis, Kvrocks, Milvus, Docker images, and JS libraries may drift independently. | Release maintainer |
| Prototype operations | `screen`-based launch is convenient but hard to monitor, restart, backup, or run as a durable service. | Operations maintainer |

## Short-term recommendations: next 1-4 weeks

### 1. Add a maintainer onboarding map

Create a concise `doc/architecture.md` or extend this document with diagrams showing:

- Request flow: browser/API/CLI -> Flask-RESTX route -> service -> Kvrocks/Redis/Milvus.
- Job flow: API/CLI -> Redis queue -> worker -> service -> Kvrocks indexes.
- Similarity flow: feature vectors -> similarity builder -> score indexes -> search Lua -> enrichment.
- Data ownership: which service owns files, functions, features, similarities, tags, clusters, jobs, and batches.

This is the fastest way to transfer internship knowledge to future CIRCL maintainers.

### 2. Introduce a lightweight test pyramid

Keep `test_api_endpoints.py` as an integration smoke test, but add faster tests that do not need a full Ghidra analysis:

- Unit tests for key builders, ID normalization, tag resolution, index configuration, timestamp parsing, and request parameter parsing.
- Service tests with a local disposable Redis/Kvrocks database and a tiny fixture collection.
- Lua script regression tests for search candidate selection, similarity filtering, and clearing similarities.
- Contract tests that compare Flask-RESTX documented models with representative responses.
- CLI tests for argument parsing and generated API requests.

Recommended initial commands:

```bash
uv add --dev pytest ruff mypy types-redis
uv run pytest
uv run ruff check .
uv run black --check .
```

Start with a small goal: protect the database key formats and the similarity search filters before refactoring.

### 3. Standardize dependencies and release metadata

The project currently has `pyproject.toml`, `requirements.txt`, and `uv.lock`. Choose one source of truth:

- Prefer `pyproject.toml` plus `uv.lock` for application development.
- Keep `requirements.txt` only if it is generated for legacy deployment, and document the generation command.
- Replace placeholder package metadata with a real description, authors/maintainers, license metadata, classifiers, and supported Python versions.
- Pin or document tested versions for Ghidra, pyghidra, Kvrocks, Redis, and optional Milvus.
- Avoid `latest` Docker tags for Redis/Kvrocks in production examples; use known-good versions.

This reduces breakage when a maintainer rebuilds the environment months later.

### 4. Add CI for low-cost checks

A minimal CI pipeline should run on every pull request:

```bash
uv sync --locked
uv run black --check .
uv run python -m compileall bsimvis scripts test_api_endpoints.py
uv run pytest
```

If service containers are available in CI, add an optional integration job for Redis/Kvrocks and the API smoke test. Keep Ghidra-heavy tests optional or nightly because they may be slow and platform-specific.

### 5. Centralize database key construction

Introduce a small module such as `bsimvis/app/services/key_schema.py` with functions/constants for key patterns:

- `file_key(collection, md5)`
- `function_key(collection, md5, addr)`
- `similarity_key(collection, algo, id1, id2)`
- `index_bucket_key(collection, level, field, value)`
- `registry_key(collection, level, field)`
- `job_key(job_id)` and `pipeline_jobs_key(pipeline_id)`

Use it first in new code and tests, then migrate existing modules gradually. This avoids future changes breaking Lua scripts, tags, search enrichment, or index cleanup.

### 6. Define API limits and validation in one place

Move common request validation to helpers or typed schema objects:

- Collection names and algorithm names.
- Pagination defaults and maximum limits.
- Score ranges and feature count bounds.
- Upload size limits and accepted content types.
- Common error response format.

This also makes Swagger documentation more accurate. The current API already uses Flask-RESTX; the next step is making validation reusable instead of endpoint-specific.

### 7. Make security assumptions explicit

If BSimVis is intended for trusted internal CIRCL networks only, state that clearly. If it may become multi-user or internet-facing, prioritize:

- Authentication and authorization for upload, deletion, rebuild, clear, tag, and cluster operations.
- Restricted CORS configuration instead of open defaults.
- Conservative `MAX_CONTENT_LENGTH` defaults with documented overrides.
- Rate limits or queue limits for expensive similarity, cluster, and upload actions.
- Audit logging for destructive or high-cost operations.
- Safe handling of uploaded binaries and Ghidra analysis directories.

### 8. Add operational runbooks

Add `doc/operations.md` with:

- How to start, stop, restart, and check health of Redis, Kvrocks, API, workers, and optional Milvus.
- How to back up and restore Kvrocks data.
- How to recover stuck jobs or pipelines.
- How to rebuild indexes safely without wiping data.
- Which commands must never be run on production-sized Kvrocks collections, especially broad `KEYS`-style operations.
- Expected disk, memory, and CPU profiles for small, medium, and large collections.

### 9. Use structured logging and consistent errors

Replace ad-hoc prints and broad exception handling in long-running paths with structured logging fields:

- `collection`, `job_id`, `pipeline_id`, `file_md5`, `algo`, `duration_ms`, `count`, `status`.
- Central error helpers for API responses.
- Clear separation between user-facing errors, retryable operational errors, and programmer bugs.

This will make bug reports and incident response much easier.

### 10. Add contributor guardrails

Add or update:

- `CONTRIBUTING.md` with setup, test, style, and PR expectations.
- `.pre-commit-config.yaml` for Black, Ruff, basic YAML/TOML checks, and trailing whitespace.
- `doc/adr/` for architecture decision records, starting with database layout, Redis-vs-Kvrocks split, optional Milvus, and bare-JS frontend choice.

## Medium-term recommendations: next 1-3 months

### 1. Split oversized modules by responsibility

Refactor incrementally with tests in place first:

- Move Swagger model definitions from `bsimvis/app/swagger.py` into `bsimvis/app/api_models/` or per-namespace files.
- Keep route registration in Swagger, but move endpoint classes close to their namespace or use Flask blueprints per domain.
- Split `search_similarity` into parameter parsing, filter resolution, Lua execution, caching, enrichment, export formatting, and response assembly.
- Split large frontend files by feature state, API client, rendering, event handlers, and persisted settings.

Avoid a big rewrite. Move one domain at a time and keep compatibility tests passing.

### 2. Create a stable internal API for services

For each service, document public methods and data contracts:

- Input types and required fields.
- Returned shape and error behavior.
- Which service owns writes to which Kvrocks/Redis keys.
- Whether methods are safe to call in request handlers, workers, or both.

This lowers the risk of circular dependencies and makes future plugins or external tooling easier.

### 3. Add schema/version management for persisted data

Introduce:

- A collection metadata key with `schema_version`, `created_at`, `last_indexed_at`, and `source_tool_versions`.
- Migration/backfill scripts with dry-run mode.
- Health checks that compare registry sets, all-document sets, and expected index counts.
- Rebuild commands that can operate on one collection, one batch, or one file.

This is especially important because analysis data can be large and expensive to regenerate.

### 4. Improve job lifecycle reliability

Add stronger job semantics:

- Lease/heartbeat for workers processing a job.
- Retry policy per job type.
- Dead-letter queue for permanently failed jobs.
- Idempotency keys for uploads and rebuild operations.
- Cancellation checks inside long loops.
- Progress model that distinguishes queued, running, waiting on subtask, failed, cancelled, and completed.

The current worker model is a good foundation; these additions make it safer for long-running production use.

### 5. Add performance budgets and benchmarks to CI/nightly runs

Keep benchmark scripts, but formalize baseline metrics:

- Upload/index time per number of functions and features.
- Similarity build time by algorithm and collection size.
- Search latency by filter type and result size.
- Cluster build time and memory by collection size.
- Redis/Kvrocks command count per endpoint.

Track regressions over time and document expected hardware.

### 6. Introduce frontend build discipline without overengineering

The current static JavaScript is simple to serve, but large files will become difficult to maintain. A pragmatic path:

- Add ESLint and Prettier first.
- Add a small API client module to centralize fetch, errors, retries, and base URLs.
- Add component-level organization for dashboard, graph, tags, jobs, upload, and cluster views.
- Consider TypeScript only after API response shapes are stable.
- Avoid adopting a heavy framework unless the UI roadmap justifies it.

### 7. Harden destructive and expensive operations

For endpoints and CLI commands that clear, rebuild, cluster, or process many records:

- Require explicit confirmation in CLI.
- Add dry-run mode where possible.
- Log actor, target collection, target algorithm, and estimated impact.
- Enforce allowlists or admin-only controls if authentication is added.
- Add safeguards against accidentally running broad operations on production collections.

## Long-term recommendations: 3-12 months

### 1. Decide the product boundary

Clarify whether BSimVis should become:

1. A CIRCL-internal research/analysis platform.
2. A maintainable open-source tool for outside analysts.
3. A library/API that other systems integrate with.
4. A prototype kept alive mainly for selected investigations.

This decision affects packaging, security, documentation, release cadence, support commitments, and UI investment.

### 2. Establish release and compatibility policy

Define:

- Supported Python versions.
- Supported Ghidra/pyghidra versions.
- Supported Redis/Kvrocks versions.
- Data schema compatibility guarantees.
- Deprecation policy for API endpoints and CLI commands.
- How to upgrade collections created by older versions.

Without this, operational drift will become the main sustainability risk.

### 3. Build an extensibility model for analysis backends

If optional Milvus, additional similarity algorithms, or new reverse-engineering sources are expected, define explicit interfaces:

- Feature extraction provider.
- Similarity index provider.
- Storage/index backend provider.
- Cluster provider.
- Export provider.

A small plugin-style boundary avoids scattering backend-specific checks throughout routes and services.

### 4. Treat the database schema as a public contract

For long-lived collections, the schema is as important as the API. Maintain:

- Versioned schema docs.
- Example fixtures for each schema version.
- Automated compatibility tests.
- Migration scripts.
- Clear backup and restore procedures.
- Index verification and repair tools.

### 5. Move from prototype launcher to production deployment options

Keep `launch.sh` for local research, but add durable deployment paths:

- Docker Compose profile for full local stack.
- Systemd units or container deployment docs for CIRCL infrastructure.
- Health endpoints for API, worker, Redis, Kvrocks, and Milvus.
- Metrics endpoint or OpenTelemetry-compatible instrumentation.
- Log rotation and data retention policy.

### 6. Create maintainership and governance documentation

Document:

- Code owners by area.
- How issues are triaged.
- How releases are cut.
- How schema changes are approved.
- How security reports are handled.
- What minimum tests are required for changes.

This is especially valuable when ownership moves from an intern to a permanent team.

## Suggested roadmap

### Phase 0: Preservation and handover

- Add architecture, operations, and contribution docs.
- Pin known-good runtime versions.
- Add compile, formatting, and unit-test CI.
- Document current data schema and known limitations.

### Phase 1: Safety net

- Add tests for key schema, parameter parsing, services, Lua scripts, and API contracts.
- Add disposable fixtures for tiny collections.
- Add health checks and index verification commands.
- Add structured logging fields for jobs and expensive operations.

### Phase 2: Controlled refactoring

- Extract key schema helpers.
- Split Swagger models and oversized route modules.
- Split frontend dashboard and cluster scripts.
- Consolidate dependency management.
- Introduce service public contracts.

### Phase 3: Operational maturity

- Add durable deployment docs.
- Add backup/restore and migration procedures.
- Add authentication/authorization if the tool leaves a trusted local environment.
- Add performance budgets and nightly benchmark tracking.

## Practical first pull requests after this document

1. **Add CI and formatting checks**: `compileall`, Black check, and a placeholder pytest suite.
2. **Add `key_schema.py`**: implement key builder helpers and tests, then migrate one low-risk service.
3. **Add architecture and operations docs**: include diagrams and safe database operation guidance.
4. **Split Swagger models**: move models to `bsimvis/app/api_models.py` without changing endpoints.
5. **Add small fixture-based tests**: one file, two functions, a few features, one similarity pair, and one tag.

## Definition of done for sustainable changes

For future maintainability PRs, prefer this checklist:

- The change is covered by at least one fast test.
- Database key changes are centralized or documented.
- Swagger/API docs remain accurate.
- CLI and API behavior stay compatible unless explicitly deprecated.
- Expensive operations have clear limits, progress, and logs.
- Documentation explains new maintainer-facing behavior.
- The change can be rolled back without data loss.
