from flask import Blueprint, request, jsonify
from flask_restx import Api, Resource, fields, Namespace
import json

# Create a blueprint for the Swagger UI and API
api_bp = Blueprint("api", __name__, url_prefix="/api")
api = Api(
    api_bp,
    version="1.0",
    title="BsimVis API",
    description="API for Binary Similarity Visualization and Analysis. Supports Ghidra-based BSim features, side-by-side diffing, and similarity searching.",
    doc="/",  # Swagger UI at /api/
    mask_swagger=False,
)


# Namespaces (matching the first part of the path after /api)
ns_index = Namespace("index", description="Database statistics and status")
ns_jobs = Namespace("jobs", description="Background job management")
ns_collection = Namespace("collection", description="Collection-level operations")
ns_batch = Namespace("batch", description="Ingestion batch operations")
ns_file = Namespace("file", description="File-level operations and search")
ns_function = Namespace("function", description="Function-level analysis and search")
ns_feature = Namespace("feature", description="Global feature search")
ns_search = Namespace("search", description="Unified search and metadata utilities")
ns_similarity = Namespace("similarity", description="Similarity engine and results")
ns_tags = Namespace("tags", description="Tag management")
ns_cluster = Namespace("cluster", description="Hierarchical clustering and analysis")
ns_bin_cluster = Namespace(
    "bin_cluster", description="Binary-level hierarchical clustering"
)
ns_features = Namespace("features", description="Global feature indexing and status")
ns_diff = Namespace("diff", description="Function diff and alignment")
ns_bin_sim = Namespace(
    "bin_sim", description="Binary-level similarity and clustering comparison"
)
ns_notes = Namespace("notes", description="Function notes management")
ns_llm = Namespace("llm", description="Large Language Model integration (Ollama)")

api.add_namespace(ns_index)
api.add_namespace(ns_jobs)
api.add_namespace(ns_collection)
api.add_namespace(ns_batch)
api.add_namespace(ns_file)
api.add_namespace(ns_function)
api.add_namespace(ns_feature)
api.add_namespace(ns_search)
api.add_namespace(ns_similarity)
api.add_namespace(ns_tags)
api.add_namespace(ns_cluster)
api.add_namespace(ns_bin_cluster)
api.add_namespace(ns_features)
api.add_namespace(ns_diff)
api.add_namespace(ns_bin_sim)
api.add_namespace(ns_notes)
api.add_namespace(ns_llm)

# --- Models & Examples ---

# Common Models
error_model = api.model(
    "Error",
    {
        "detail": fields.String(
            description="Error message", example="Function not found"
        )
    },
)

# Index Models
index_stats_model = api.model(
    "IndexStats",
    {
        "collection": fields.String(example="main"),
        "file_count": fields.Integer(example=120),
        "function_count": fields.Integer(example=45000),
        "feature_count": fields.Integer(example=1200000),
        "similarity_pairs": fields.Integer(example=850000),
        "last_updated": fields.Integer(example=1775639990508),
    },
)

# Job Models
job_model = api.model(
    "Job",
    {
        "id": fields.String(example="7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d"),
        "type": fields.String(example="build_sim"),
        "status": fields.String(example="completed"),
        "progress": fields.Float(example=1.0),
        "created_at": fields.Integer(example=1775639990508),
        "error": fields.String(example=""),
        "logs": fields.List(
            fields.String,
            example=["Starting similarity build...", "Processing batch 1/10..."],
        ),
    },
)

# Function Models
function_meta_model = api.model(
    "FunctionMeta",
    {
        "function_name": fields.String(example="main"),
        "file_name": fields.String(example="libc.so.6"),
        "file_md5": fields.String(example="16c2addf057b3e3b2703500462e38c1c"),
        "language_id": fields.String(example="AARCH64:LE:64:v8A"),
        "return_type": fields.String(example="int"),
        "parameters": fields.List(fields.String, example=["int argc", "char** argv"]),
        "bsim_features_count": fields.Integer(example=42),
        "entry_date": fields.String(example="2026-05-26 10:00:00"),
    },
)

# File Models
file_upload_data_model = api.model(
    "FileUploadData",
    {
        "collection": fields.String(default="main", description="Collection name"),
        "file_md5": fields.String(
            description="File MD5 (will be calculated if missing)"
        ),
        "top_k": fields.Integer(description="Top K matches per function"),
        "min_score": fields.Float(description="Minimum similarity score threshold"),
        "min_features": fields.Integer(description="Minimum feature count required"),
        "algo": fields.String(
            default="unweighted_cosine",
            description="Similarity algorithm (jaccard, unweighted_cosine, milvus_sparse)",
        ),
        "skip_sim": fields.Boolean(default=False, description="Skip similarity build"),
    },
)

file_metadata_update_model = api.model(
    "FileMetadataUpdate",
    {
        "collection": fields.String(default="main", description="Collection name"),
        "metadata": fields.Raw(required=True, description="Dictionary of metadata fields to update")
    }
)

bulk_metadata_propagate_model = api.model(
    "BulkMetadataPropagate",
    {
        "collection": fields.String(default="main", description="Collection name"),
        "updates": fields.Raw(required=True, description="Mapping of MD5 to metadata dictionary")
    }
)

# Similarity Models
similarity_build_model = api.model(
    "SimilarityBuild",
    {
        "collection": fields.String(required=True, example="main"),
        "md5": fields.String(example="16c2addf..."),
        "batch": fields.String(example="uuid..."),
        "algo": fields.String(default="unweighted_cosine"),
        "min_score": fields.Float(default=0.95),
        "top_k": fields.Integer(default=20),
        "min_features": fields.Integer(default=0),
        "all": fields.Boolean(default=False),
    },
)

similarity_clear_model = api.model(
    "SimilarityClear",
    {
        "collection": fields.String(required=True, example="main"),
        "md5": fields.String(example="16c2addf..."),
        "batch": fields.String(example="uuid..."),
        "algo": fields.String(default="unweighted_cosine"),
    },
)

bin_sim_build_model = api.model(
    "BinSimBuild",
    {
        "collection": fields.String(default="main"),
        "algo": fields.String(default="unweighted_cosine"),
        "md5_a": fields.String(),
        "md5_b": fields.String(),
        "min_cohesion": fields.Float(default=0.5),
    },
)
bin_sim_clear_model = api.model(
    "BinSimClear",
    {
        "collection": fields.String(default="main"),
        "algo": fields.String(default="unweighted_cosine"),
        "md5": fields.String(),
    },
)

# Note Models
note_model = api.model(
    "Note",
    {
        "id": fields.String(example="7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d"),
        "text": fields.String(example="This function handles input validation"),
        "owner": fields.String(example="user"),
        "timestamp": fields.Integer(example=1775639990508),
    },
)

note_add_model = api.model(
    "NoteAdd",
    {
        "collection": fields.String(required=True, example="main"),
        "func_id": fields.String(required=True, example="main:func:123:456"),
        "text": fields.String(required=True, example="This function handles input validation"),
        "owner": fields.String(example="user"),
    },
)

note_update_model = api.model(
    "NoteUpdate",
    {
        "collection": fields.String(required=True, example="main"),
        "func_id": fields.String(required=True, example="main:func:123:456"),
        "note_id": fields.String(required=True, example="uuid"),
        "text": fields.String(required=True, example="Updated note text"),
    },
)

note_remove_model = api.model(
    "NoteRemove",
    {
        "collection": fields.String(required=True, example="main"),
        "func_id": fields.String(required=True, example="main:func:123:456"),
        "note_id": fields.String(required=True, example="uuid"),
    },
)

# LLM Models
llm_summary_request_model = api.model(
    "LLMSummaryRequest",
    {
        "func_id": fields.String(required=True, example="main:func:123:456"),
        "prompt": fields.String(description="Optional custom prompt"),
        "code": fields.String(description="Optional code string"),
        "func_name": fields.String(description="Optional function name"),
    },
)

llm_chat_request_model = api.model(
    "LLMChatRequest",
    {
        "messages": fields.List(
            fields.Raw,
            required=True,
            example=[{"role": "user", "content": "What does this function do?"}],
        )
    },
)

# --- Routes & Resources ---
sim_pair_model = api.model(
    "SimilarityPair",
    {
        "id1": fields.String(example="main:func:16c2addf:10400"),
        "id2": fields.String(example="main:func:0ed905e8:10520"),
        "name1": fields.String(example="main"),
        "name2": fields.String(example="main"),
        "score": fields.Float(example=0.985),
        "feat_count": fields.Integer(example=42),
        "meta1": fields.Nested(function_meta_model),
        "meta2": fields.Nested(function_meta_model),
    },
)

similarity_search_response = api.model(
    "SimilaritySearchResponse",
    {
        "total": fields.Integer(example=1500),
        "offset": fields.Integer(example=0),
        "limit": fields.Integer(example=50),
        "pairs": fields.List(fields.Nested(sim_pair_model)),
    },
)


# --- Index Namespace ---
@ns_index.route("/status")
class IndexStatus(Resource):
    @ns_index.doc(
        params={
            "collection": "Collection name (default: main)",
            "details": "Return detailed stats (true/false)",
        }
    )
    @ns_index.response(200, "Success", index_stats_model)
    def get(self):
        """Returns database index statistics and counts."""
        from bsimvis.app.routes.index import get_index_status

        return get_index_status()


# --- Jobs Namespace ---
@ns_jobs.route("")
class JobList(Resource):
    @ns_jobs.doc(
        params={
            "limit": {
                "description": "Number of jobs to return",
                "default": 100,
                "example": 20,
            },
            "offset": {"description": "Pagination offset", "default": 0, "example": 0},
        }
    )
    def get(self):
        """Lists recent and active background jobs."""
        from bsimvis.app.routes.jobs import list_jobs

        return list_jobs()


@ns_jobs.route("/stats")
class JobStats(Resource):
    def get(self):
        """Returns aggregate metrics across all jobs (total, completed, failed, pending)."""
        from bsimvis.app.routes.jobs import get_global_stats

        return get_global_stats()


@ns_jobs.route("/<string:job_id>")
class JobDetail(Resource):
    @ns_jobs.doc(
        params={
            "job_id": {
                "description": "Job or pipeline UUID",
                "example": "7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d",
            }
        }
    )
    @ns_jobs.response(200, "Success", job_model)
    @ns_jobs.response(404, "Job not found", error_model)
    def get(self, job_id):
        """Returns detailed status and logs for a specific job or pipeline."""
        from bsimvis.app.routes.jobs import get_job

        return get_job(job_id)


@ns_jobs.route("/all/cancel")
class JobCancelAll(Resource):
    def post(self):
        """Cancels all pending or running jobs and pipelines."""
        from bsimvis.app.routes.jobs import cancel_all_jobs

        return cancel_all_jobs()


@ns_jobs.route("/<string:job_id>/cancel")
class JobCancel(Resource):
    @ns_jobs.doc(
        params={
            "job_id": {
                "description": "Job or pipeline UUID to cancel",
                "example": "7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d",
            }
        }
    )
    def post(self, job_id):
        """Cancels a pending or running job/pipeline."""
        from bsimvis.app.routes.jobs import cancel_job

        return cancel_job(job_id)


@ns_jobs.route("/<string:job_id>/retry")
class JobRetry(Resource):
    @ns_jobs.doc(
        params={
            "job_id": {
                "description": "Job or pipeline UUID to retry",
                "example": "7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d",
            }
        }
    )
    def post(self, job_id):
        """Retries a failed or cancelled job/pipeline. For pipelines, resets all sub-tasks."""
        from bsimvis.app.routes.jobs import retry_job

        return retry_job(job_id)


# --- Collection Namespace ---
@ns_collection.route("/search")
class CollectionSearch(Resource):
    @ns_collection.doc(
        params={
            "offset": {"description": "Pagination offset", "default": 0, "example": 0},
            "limit": {
                "description": "Max results per page",
                "default": 100,
                "example": 50,
            },
            "q": {
                "description": "Keyword search across collection names",
                "example": "main",
            },
            "format": {"description": "Export format: csv or json", "example": "json"},
        }
    )
    def get(self):
        """Lists and searches available collections. Supports keyword filtering and CSV/JSON export."""
        from bsimvis.app.routes.search_collection import search_collections

        return search_collections()


@ns_collection.route("/delete")
class CollectionDelete(Resource):
    @ns_collection.expect(
        api.model(
            "CollectionDelete",
            {
                "collection": fields.String(required=True, description="Collection name to delete"),
            },
        )
    )
    def post(self):
        """Wipes and deletes a collection entirely (asynchronous background job)."""
        from bsimvis.app.routes.search_collection import delete_collection

        return delete_collection()


@ns_collection.route("/clean")
class CollectionClean(Resource):
    @ns_collection.expect(
        api.model(
            "CollectionClean",
            {
                "collection": fields.String(required=True, description="Collection name to clean"),
            },
        )
    )
    def post(self):
        """Cleans up temporary raw and JSON upload keys in a collection (asynchronous background job)."""
        from bsimvis.app.routes.search_collection import clean_collection

        return clean_collection()




# --- Batch Namespace ---
@ns_batch.route("/search")
class BatchSearch(Resource):
    @ns_batch.doc(
        params={
            "collection": {
                "description": "Target collection name",
                "required": True,
                "example": "main",
            },
            "q": {
                "description": "Keyword search across batch UUID/name",
                "example": "my_batch",
            },
            "offset": {"description": "Pagination offset", "default": 0},
            "limit": {"description": "Max results", "default": 100},
            "format": {"description": "Export format: csv or json", "example": "json"},
        }
    )
    def get(self):
        """Lists and searches ingestion batches within a collection."""
        from bsimvis.app.routes.search_collection import search_batches

        return search_batches()


# --- File Namespace ---
@ns_file.route("/search")
class FileSearch(Resource):
    @ns_file.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "q": {
                "description": "Global keyword search (name, md5, language, batch)",
                "example": "libc",
            },
            "file_name": {
                "description": "Filter by filename substring",
                "example": "libcrypto",
            },
            "file_md5": {
                "description": "Filter by exact file MD5",
                "example": "59281a167473ca9b98515b11cb709f82",
            },
            "language_id": {
                "description": "Filter by Ghidra language ID",
                "example": "x86:LE:64:default",
            },
            "batch_uuid": {
                "description": "Filter by batch UUID",
                "example": "uuid-1234-abcd",
            },
            "bin_cluster_uuid": {
                "description": "Filter by binary cluster UUID",
                "example": "a1b2c3d4e5f6",
            },
            "bin_cluster_name": {
                "description": "Filter by binary cluster name substring",
                "example": "libc",
            },
            "tag": {
                "description": "Filter by tag (static or user)",
                "example": "malware",
            },
            "static_tag": {
                "description": "Filter by static analysis tag only",
                "example": "packed",
            },
            "user_tag": {
                "description": "Filter by user-assigned tag only",
                "example": "reviewed",
            },
            "file_tag": {
                "description": "Filter by file-level tag (alias for tag)",
            },
            "file_static_tag": {
                "description": "Filter by file-level static tag",
            },
            "file_user_tag": {
                "description": "Filter by file-level user tag",
            },
            "exclude_tag": {
                "description": "Exclude files with this tag",
                "example": "benign",
            },
            "exclude_static_tag": {
                "description": "Exclude files with this static tag",
            },
            "exclude_user_tag": {
                "description": "Exclude files with this user tag",
            },
            "exclude_file_tag": {
                "description": "Exclude files with this file-level tag",
            },
            "exclude_file_static_tag": {
                "description": "Exclude files with this file-level static tag",
            },
            "exclude_file_user_tag": {
                "description": "Exclude files with this file-level user tag",
            },
            "min_function_count": {
                "description": "Minimum number of indexed functions",
                "example": 10,
            },
            "max_function_count": {
                "description": "Maximum number of indexed functions",
                "example": 500,
            },
            "min_entry_date": {
                "description": "Earliest upload date (ISO or timestamp)",
                "example": "2026-01-01",
            },
            "max_entry_date": {"description": "Latest upload date (ISO or timestamp)"},
            "min_file_date": {
                "description": "Earliest file date (ISO or timestamp)",
            },
            "max_file_date": {
                "description": "Latest file date (ISO or timestamp)",
            },
            "min_cohesion": {
                "description": "Minimum cluster cohesion score (0.0–1.0)",
                "example": 0.5,
            },
            "max_cohesion": {
                "description": "Maximum cluster cohesion score (0.0–1.0)",
                "example": 1.0,
            },
            "algo": {
                "description": "Similarity algorithm",
                "default": "unweighted_cosine",
            },
            "sort_by": {
                "description": "Sort field: entry_date, file_date, function_count",
                "example": "entry_date",
            },
            "sort_order": {
                "description": "Sort direction: asc or desc",
                "default": "desc",
            },
            "offset": {"description": "Pagination offset", "default": 0},
            "limit": {"description": "Results per page", "default": 100},
            "format": {"description": "Export format: csv or json"},
        }
    )
    def get(self):
        """Search for files within a collection with rich filtering, sorting, and export."""
        from bsimvis.app.routes.search_file import search_files

        return search_files()



@ns_file.route("/details/<string:file_md5>")
class FileDetails(Resource):
    @ns_file.doc(description="Get full metadata for a file including its clusters")
    @ns_file.expect(
        api.parser()
        .add_argument("collection", type=str, default="main", location="args")
        .add_argument("algo", type=str, default="unweighted_cosine", location="args")
    )
    def get(self, file_md5):
        from flask import request
        collection = request.args.get("collection", "main")
        from bsimvis.app.routes.search_file import get_file_details
        return get_file_details(collection, file_md5)

@ns_file.route("/upload_file_data")

class FileUpload(Resource):
    @ns_file.expect(file_upload_data_model)
    def post(self):
        """Uploads raw analysis data for a new file."""
        from bsimvis.app.routes.file import upload_file_data

        return upload_file_data()


@ns_file.route("/upload")
class RawFileUpload(Resource):
    @ns_file.doc(
        params={
            "collection": "Collection name (default: main)",
            "file_name": "Original name of the file",
            "batch_uuid": "Batch UUID",
            "batch_name": "Batch Name (default: Ghidra Batch)",
            "tags": "Optional tags to associate with the uploaded file",
            "profile": "Ghidra analysis profile: fast or full (default: fast)",
            "min_func_len": "Minimum function length (default: 10)",
            "processor": "Force a specific Ghidra Language ID (e.g., 'x86:LE:64:default')",
            "cspec": "Force a specific Ghidra Compiler Spec ID (e.g., 'gcc')",
            "top_k": "Top K matches per function",
            "min_score": "Minimum similarity score threshold",
            "min_features": "Minimum feature count required",
            "algo": "Similarity algorithm (jaccard, unweighted_cosine, milvus_sparse)",
            "skip_sim": "Set to true to skip building similarities",
        }
    )
    def post(self):
        """Uploads a raw binary file for server-side analysis."""
        from bsimvis.app.routes.file import upload_raw_binary

        return upload_raw_binary()


@ns_file.route("/upload/batch_finalize")
class BatchFinalize(Resource):
    @ns_file.doc(
        params={
            "pipeline_ids": "List of pipeline IDs to group",
            "batch_uuid": "Batch UUID",
            "collection": "Collection name",
            "algo": "Similarity algorithm",
            "skip_sim": "Skip binary similarity",
        }
    )
    def post(self):
        """Finalizes a batch upload by orchestrating a master pipeline."""
        from bsimvis.app.routes.file import finalize_batch_upload

        return finalize_batch_upload()


@ns_file.route("/<string:file_md5>/metadata")
class FileMetadata(Resource):
    @ns_file.doc(description="Updates metadata fields for a file and propagates them")
    @ns_file.expect(file_metadata_update_model)
    def patch(self, file_md5):
        """Partially updates metadata for a file and triggers propagation."""
        from bsimvis.app.routes.file import update_file_metadata
        return update_file_metadata(file_md5)


@ns_file.route("/metadata/propagate")
class BulkMetadataPropagate(Resource):
    @ns_file.doc(description="Updates metadata fields in bulk and propagates them")
    @ns_file.expect(bulk_metadata_propagate_model)
    def post(self):
        """Updates metadata fields in bulk and propagates them."""
        from bsimvis.app.routes.file import bulk_propagate_metadata
        return bulk_propagate_metadata()


@ns_file.route("/call_graph")
class FileCallGraph(Resource):
    @ns_file.doc(params={"collection": "Collection name", "file_md5": "File MD5"})
    def get(self):
        """Returns the full call graph for a file."""
        from bsimvis.app.routes.function_code import get_file_call_graph

        return get_file_call_graph()


# --- Function Namespace ---
@ns_function.route("/search")
class FunctionSearch(Resource):
    @ns_function.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "q": {
                "description": "Global keyword search across all indexed fields",
                "example": "encrypt",
            },
            "function_name": {
                "description": "Filter by function name substring",
                "example": "aes",
            },
            "file_md5": {
                "description": "Filter by file MD5",
                "example": "59281a167473ca9b98515b11cb709f82",
            },
            "file_name": {
                "description": "Filter by file name substring",
                "example": "libcrypto",
            },
            "language_id": {
                "description": "Filter by Ghidra language ID",
                "example": "x86:LE:64:default",
            },
            "namespace": {"description": "Filter by namespace", "example": "std"},
            "return_type": {"description": "Filter by return type", "example": "int"},
            "entrypoint_address": {
                "description": "Filter by entrypoint address",
                "example": "0x401000",
            },
            "tag": {
                "description": "Filter by tag (static or user)",
                "example": "crypto",
            },
            "static_tag": {"description": "Filter by static analysis tag only"},
            "user_tag": {"description": "Filter by user-assigned tag only"},
            "func_tag": {"description": "Filter by function-level tag"},
            "func_static_tag": {"description": "Filter by function-level static tag"},
            "func_user_tag": {"description": "Filter by function-level user tag"},
            "file_tag": {"description": "Filter by file-level tag"},
            "file_static_tag": {"description": "Filter by file-level static tag"},
            "file_user_tag": {"description": "Filter by file-level user tag"},
            "exclude_tag": {"description": "Exclude functions with this tag"},
            "exclude_static_tag": {
                "description": "Exclude functions with this static tag"
            },
            "exclude_user_tag": {"description": "Exclude functions with this user tag"},
            "exclude_func_tag": {
                "description": "Exclude functions with this function-level tag"
            },
            "exclude_func_static_tag": {
                "description": "Exclude functions with this function-level static tag"
            },
            "exclude_func_user_tag": {
                "description": "Exclude functions with this function-level user tag"
            },
            "exclude_file_tag": {
                "description": "Exclude functions with this file-level tag"
            },
            "exclude_file_static_tag": {
                "description": "Exclude functions with this file-level static tag"
            },
            "exclude_file_user_tag": {
                "description": "Exclude functions with this file-level user tag"
            },
            "min_features": {"description": "Minimum BSim feature count", "example": 5},
            "min_cohesion": {
                "description": "Minimum cluster cohesion score (0.0–1.0). Clusters below this threshold are excluded from the response.",
                "example": 0.5,
            },
            "sort_by": {
                "description": "Sort field: id, function_name, bsim_features_count",
                "example": "bsim_features_count",
            },
            "sort_order": {
                "description": "Sort direction: asc or desc",
                "default": "desc",
            },
            "offset": {"description": "Pagination offset", "default": 0},
            "limit": {"description": "Results per page", "default": 100},
            "pool_limit": {
                "description": "Max candidates to intersect (default: 1000000)"
            },
            "format": {"description": "Export format: csv or json"},
        }
    )
    def get(self):
        """Search for functions with rich filtering: name, file, tags, features, sorting, and export."""
        from bsimvis.app.routes.search_function import search_functions

        return search_functions()


@ns_function.route("/code")
class FunctionCode(Resource):
    @ns_function.doc(params={"id": "Function ID (idx:col:func:md5:addr)"})
    def get(self):
        """Returns decompiler tokens and metadata for a single function."""
        from bsimvis.app.routes.function_code import get_function_code

        return get_function_code()


@ns_function.route("/diff")
class FunctionDiff(Resource):
    @ns_function.doc(params={"id1": "First function ID", "id2": "Second function ID"})
    def get(self):
        """Returns side-by-side aligned diff of two functions."""
        from bsimvis.app.routes.function_diff import diff_api

        return diff_api()


@ns_function.route("/features")
class FunctionFeatures(Resource):
    @ns_function.doc(params={"id": "Function ID"})
    def get(self):
        """Lists all features for a function with their code context."""
        from bsimvis.app.routes.function_feature import get_function_features

        return get_function_features()


# --- Feature Namespace ---
@ns_feature.route("/search")
class FeatureSearch(Resource):
    @ns_feature.doc(
        params={
            "collection": "Collection name",
            "q": "Search query",
            "hash": "Filter by feature hash (hex prefix)",
            "type": "Filter by feature type",
            "op": "Filter by opcode",
            "sort_by": "Sort by 'tf_score' or 'default'",
            "sort_order": "Sort direction (asc/desc)",
            "offset": "Pagination offset",
            "limit": "Max results",
            "format": "Output format (json/csv)",
        }
    )
    def get(self):
        """Search for BSim features and their frequency across the collection."""
        from bsimvis.app.routes.search_feature import search_features

        return search_features()


@ns_feature.route("/details/<string:f_hash>")
class FeatureDetails(Resource):
    @ns_feature.doc(
        params={
            "collection": "Collection name",
            "offset": "Pagination offset",
            "limit": "Max results",
        }
    )
    def get(self, f_hash):
        """Returns all function occurrences for a specific feature hash."""
        from bsimvis.app.routes.search_feature import get_feature_details
        from flask import g

        g.f_hash = f_hash
        return get_feature_details(f_hash)


# --- Search Namespace ---
@ns_search.route("/autocomplete")
class SearchAutocomplete(Resource):
    @ns_search.doc(
        params={
            "collection": "Collection name",
            "level": "Index level (func, file, sim)",
            "field": "Field to search (e.g., function_name)",
            "q": "Search query prefix",
            "limit": "Max results (default: 50)",
        }
    )
    def get(self):
        """Autocomplete for metadata fields."""
        from bsimvis.app.routes.search_similarity import autocomplete

        return autocomplete()


@ns_search.route("/fields")
class SearchFields(Resource):
    @ns_search.doc(
        params={
            "collection": "Collection name",
            "level": "Index level",
            "field": "List of fields to get stats for",
        }
    )
    def get(self):
        """Returns cardinality stats for specified metadata fields."""
        from bsimvis.app.routes.search_similarity import get_field_stats

        return get_field_stats()


# --- Similarity Namespace ---
@ns_similarity.route("")
class SimilarityPair(Resource):
    @ns_similarity.doc(
        params={
            "id1": {
                "description": "First function ID",
                "required": True,
                "example": "main:func:59281a167473ca9b98515b11cb709f82:00101144",
            },
            "id2": {
                "description": "Second function ID",
                "required": True,
                "example": "main:func:0ed905e8abcdef12:00101144",
            },
        }
    )
    def get(self):
        """Returns similarity score and tags for a specific function pair."""
        from bsimvis.app.routes.function_similarity import similarity_api

        return similarity_api()


@ns_similarity.route("/search")
class SimilaritySearch(Resource):
    @ns_similarity.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "algo": {
                "description": "Similarity algorithm",
                "default": "unweighted_cosine",
                "example": "unweighted_cosine",
            },
            "min_score": {
                "description": "Minimum similarity score (inclusive)",
                "default": 0.95,
                "example": 0.95,
            },
            "max_score": {
                "description": "Maximum similarity score (inclusive)",
                "default": 1.0,
                "example": 1.0,
            },
            "q": {
                "description": "Global keyword search across all metadata",
                "example": "encrypt",
            },
            "name": {
                "description": "Filter by function name substring",
                "example": "aes",
            },
            "file_name": {
                "description": "Filter by file name substring",
                "example": "libcrypto",
            },
            "md5": {
                "description": "Filter pairs involving this file MD5",
                "example": "59281a167473ca9b98515b11cb709f82",
            },
            "tag": {
                "description": "Filter by tag (static or user, any entity)",
                "example": "crypto",
            },
            "static_tag": {"description": "Filter by static analysis tag only"},
            "user_tag": {"description": "Filter by user-assigned tag only"},
            "sim_tag": {"description": "Filter by similarity-level tag"},
            "sim_static_tag": {"description": "Filter by similarity-level static tag"},
            "sim_user_tag": {"description": "Filter by similarity-level user tag"},
            "func_tag": {"description": "Filter by function-level tag"},
            "func_static_tag": {"description": "Filter by function-level static tag"},
            "func_user_tag": {"description": "Filter by function-level user tag"},
            "file_tag": {"description": "Filter by file-level tag"},
            "file_static_tag": {"description": "Filter by file-level static tag"},
            "file_user_tag": {"description": "Filter by file-level user tag"},
            "exclude_tag": {"description": "Exclude pairs with this tag"},
            "exclude_static_tag": {"description": "Exclude pairs with this static tag"},
            "exclude_user_tag": {"description": "Exclude pairs with this user tag"},
            "exclude_sim_tag": {
                "description": "Exclude pairs with this similarity-level tag"
            },
            "exclude_sim_static_tag": {
                "description": "Exclude pairs with this similarity-level static tag"
            },
            "exclude_sim_user_tag": {
                "description": "Exclude pairs with this similarity-level user tag"
            },
            "exclude_func_tag": {
                "description": "Exclude pairs with this function-level tag"
            },
            "exclude_func_static_tag": {
                "description": "Exclude pairs with this function-level static tag"
            },
            "exclude_func_user_tag": {
                "description": "Exclude pairs with this function-level user tag"
            },
            "exclude_file_tag": {
                "description": "Exclude pairs with this file-level tag"
            },
            "exclude_file_static_tag": {
                "description": "Exclude pairs with this file-level static tag"
            },
            "exclude_file_user_tag": {
                "description": "Exclude pairs with this file-level user tag"
            },
            "language": {
                "description": "Filter by language ID",
                "example": "x86:LE:64:default",
            },
            "namespace": {"description": "Filter by namespace", "example": "std"},
            "ret_type": {"description": "Filter by return type", "example": "int"},
            "address": {
                "description": "Filter by entrypoint address",
                "example": "0x401000",
            },
            "cross_binary": {
                "description": "Only cross-binary pairs: true or false",
                "example": "true",
            },
            "match_mode": {
                "description": "any = either function matches, both = both must match",
                "default": "any",
            },
            "min_features": {"description": "Minimum feature count", "example": 5},
            "min_cohesion": {
                "description": "Minimum cluster cohesion score (0.0-1.0). Clusters below this threshold are excluded.",
                "example": 0.5,
            },
            "sort_by": {
                "description": "Sort field: score or feat_count",
                "default": "score",
            },
            "sort_order": {
                "description": "Sort direction: asc or desc",
                "default": "desc",
            },
            "offset": {"description": "Pagination offset", "default": 0},
            "limit": {"description": "Results per page", "default": 100},
            "use_cache": {
                "description": "Use cached result for this filter set (true/false)",
                "default": "false",
            },
            "format": {"description": "Export format: csv or json"},
        }
    )
    def get(self):
        """Main similarity search engine with rich filtering, cross-binary detection, caching, and export."""
        from bsimvis.app.routes.search_similarity import similarity_search

        return similarity_search()


@ns_similarity.route("/tag")
class SimilarityTag(Resource):
    @ns_similarity.expect(
        api.model(
            "SimilarityTagRequest",
            {
                "collection": fields.String(required=True, example="main"),
                "id1": fields.String(
                    required=True,
                    description="First function ID",
                    example="main:func:59281a167473ca9b98515b11cb709f82:00101144",
                ),
                "id2": fields.String(
                    required=True,
                    description="Second function ID",
                    example="main:func:0ed905e8abcdef12:00101144",
                ),
                "algo": fields.String(default="unweighted_cosine"),
                "tag": fields.String(required=True, example="interesting"),
            },
        )
    )
    def post(self):
        """Adds a user tag to a similarity pair."""
        from bsimvis.app.routes.similarity import tag_similarity

        return tag_similarity()


@ns_similarity.route("/untag")
class SimilarityUntag(Resource):
    @ns_similarity.expect(
        api.model(
            "SimilarityUntagRequest",
            {
                "collection": fields.String(required=True, example="main"),
                "id1": fields.String(
                    required=True,
                    example="main:func:59281a167473ca9b98515b11cb709f82:00101144",
                ),
                "id2": fields.String(
                    required=True, example="main:func:0ed905e8abcdef12:00101144"
                ),
                "algo": fields.String(default="unweighted_cosine"),
                "tag": fields.String(required=True, example="interesting"),
            },
        )
    )
    def post(self):
        """Removes a user tag from a similarity pair."""
        from bsimvis.app.routes.similarity import untag_similarity

        return untag_similarity()


@ns_similarity.route("/status")
class SimilarityStatus(Resource):
    @ns_similarity.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "md5": {
                "description": "File MD5 to check build status for",
                "example": "59281a167473ca9b98515b11cb709f82",
            },
            "batch": {
                "description": "Batch UUID to check build status for",
                "example": "uuid-1234-abcd",
            },
            "algo": {"description": "Algorithm", "default": "unweighted_cosine"},
        }
    )
    def get(self):
        """Returns similarity build status (total vs built) for a target."""
        from bsimvis.app.routes.similarity import similarity_status

        return similarity_status()


@ns_similarity.route("/batches")
class SimilarityBatches(Resource):
    @ns_similarity.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "by": {
                "description": "Group by 'batch' or 'md5'",
                "default": "batch",
                "example": "md5",
            },
            "algo": {"description": "Algorithm", "default": "unweighted_cosine"},
        }
    )
    def get(self):
        """Lists similarity build status grouped by batch or file."""
        from bsimvis.app.routes.similarity import list_batches

        return list_batches()


@ns_similarity.route("/list")
class SimilarityList(Resource):
    @ns_similarity.doc(
        params={
            "collection": "Collection name (default: main)",
            "md5": "File MD5 (required unless batch is provided)",
            "batch": "Batch UUID (required unless md5 is provided)",
            "algo": "Similarity algorithm (default: unweighted_cosine)",
            "limit": "Max results to return (default: 20)",
            "offset": "Pagination offset (default: 0)",
        }
    )
    def get(self):
        """Lists pre-calculated similarity results for a file."""
        from bsimvis.app.routes.similarity import list_similarities

        return list_similarities()


@ns_similarity.route("/build")
class SimilarityBuild(Resource):
    @ns_similarity.expect(similarity_build_model)
    def post(self):
        """Enqueues a job to pre-calculate similarities."""
        from bsimvis.app.routes.similarity import build_similarity

        return build_similarity()


@ns_similarity.route("/rebuild")
class SimilarityRebuild(Resource):
    @ns_similarity.expect(similarity_build_model)
    def post(self):
        """Enqueues a clear + build pipeline for similarities."""
        from bsimvis.app.routes.similarity import rebuild_similarity

        return rebuild_similarity()


@ns_similarity.route("/clear")
class SimilarityClear(Resource):
    @ns_similarity.expect(similarity_clear_model)
    def post(self):
        """Enqueues a similarity clear job."""
        from bsimvis.app.routes.similarity import clear_similarity

        return clear_similarity()


# --- Tags Namespace ---
@ns_tags.route("")
class TagList(Resource):
    @ns_tags.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            }
        }
    )
    def get(self):
        """Returns the global tag index (all tags with colors and priorities) for a collection."""
        from bsimvis.app.routes.tags import get_tags

        return get_tags()


@ns_tags.route("/add")
class TagAdd(Resource):
    @ns_tags.expect(
        api.model(
            "TagAdd",
            {
                "collection": fields.String(required=True, example="main"),
                "entity_type": fields.String(
                    required=True, enum=["file", "function", "similarity"]
                ),
                "entity_id": fields.String(required=True, example="16c2addf..."),
                "tag": fields.String(required=True, example="vulnerable"),
            },
        )
    )
    def post(self):
        """Adds a tag to an entity."""
        from bsimvis.app.routes.tags import add_tag

        return add_tag()


@ns_tags.route("/bulk_add")
class TagBulkAdd(Resource):
    @ns_tags.expect(
        api.model(
            "TagBulkAdd",
            {
                "collection": fields.String(required=True),
                "entity_type": fields.String(required=True),
                "entity_ids": fields.List(fields.String, required=True),
                "tag": fields.String(required=True),
            },
        )
    )
    def post(self):
        """Adds a tag to multiple entities."""
        from bsimvis.app.routes.tags import add_bulk_tags

        return add_bulk_tags()


@ns_tags.route("/remove")
class TagRemove(Resource):
    @ns_tags.expect(
        api.model(
            "TagRemove",
            {
                "collection": fields.String(required=True),
                "entity_type": fields.String(required=True),
                "entity_id": fields.String(required=True),
                "tag": fields.String(required=True),
            },
        )
    )
    def post(self):
        """Removes a tag from an entity."""
        from bsimvis.app.routes.tags import remove_tag

        return remove_tag()


@ns_tags.route("/bulk_remove")
class TagBulkRemove(Resource):
    @ns_tags.expect(
        api.model(
            "TagBulkRemove",
            {
                "collection": fields.String(required=True),
                "entity_type": fields.String(required=True),
                "entity_ids": fields.List(fields.String, required=True),
                "tag": fields.String(required=True),
            },
        )
    )
    def post(self):
        """Removes a tag from multiple entities."""
        from bsimvis.app.routes.tags import remove_bulk_tags

        return remove_bulk_tags()


@ns_tags.route("/metadata")
class TagMetadata(Resource):
    @ns_tags.doc(params={"collection": "Collection name"})
    def get(self):
        """Returns all tag metadata for a collection."""
        from bsimvis.app.routes.tags import get_metadata

        return get_metadata()


@ns_tags.route("/stats")
class TagStats(Resource):
    @ns_tags.doc(params={"collection": "Collection name", "tag": "Tag name"})
    def get(self):
        """Returns statistics for a specific tag."""
        from bsimvis.app.routes.tags import get_tag_stats

        return get_tag_stats()


@ns_tags.route("/color")
class TagSetColor(Resource):
    @ns_tags.expect(
        api.model(
            "TagSetColor",
            {
                "collection": fields.String(required=True),
                "tag": fields.String(required=True),
                "color": fields.String(required=True, example="#ff0000"),
            },
        )
    )
    def post(self):
        """Sets a custom color for a tag."""
        from bsimvis.app.routes.tags import set_color

        return set_color()


@ns_tags.route("/priority")
class TagSetPriority(Resource):
    @ns_tags.expect(
        api.model(
            "TagSetPriority",
            {
                "collection": fields.String(required=True),
                "tag": fields.String(required=True),
                "priority": fields.Integer(required=True),
            },
        )
    )
    def post(self):
        """Sets a custom priority for a tag."""
        from bsimvis.app.routes.tags import set_priority

        return set_priority()


# --- Cluster Namespace ---
@ns_cluster.route("/build")
class ClusterBuild(Resource):
    @ns_cluster.expect(
        api.model(
            "ClusterBuild",
            {
                "collection": fields.String(default="main"),
                "algo": fields.String(default="unweighted_cosine"),
                "min_cluster_size": fields.Integer(default=2),
                "min_samples": fields.Integer(default=1),
                "epsilon": fields.Float(default=0.1),
                "selection_method": fields.String(default="eom"),
                "min_sim": fields.Float(default=0.0),
                "min_features": fields.Integer(default=0),
            },
        )
    )
    def post(self):
        """Enqueues a clustering job."""
        from bsimvis.app.routes.cluster import build_cluster

        return build_cluster()


@ns_cluster.route("/rebuild")
class ClusterRebuild(Resource):
    @ns_cluster.expect(api.models["ClusterBuild"])
    def post(self):
        """Enqueues a clear + cluster pipeline."""
        from bsimvis.app.routes.cluster import rebuild_cluster

        return rebuild_cluster()


@ns_cluster.route("/rebuild_all")
class ClusterRebuildAll(Resource):
    @ns_cluster.expect(api.models["ClusterBuild"])
    def post(self):
        """Enqueues a full re-analysis pipeline (Clusters + Binary Sim)."""
        from bsimvis.app.routes.cluster import rebuild_all_pipeline

        return rebuild_all_pipeline()


@ns_cluster.route("/clear")
class ClusterClear(Resource):
    @ns_cluster.expect(
        api.model(
            "ClusterClear",
            {
                "collection": fields.String(default="main"),
                "algo": fields.String(default="unweighted_cosine"),
            },
        )
    )
    def post(self):
        """Enqueues a cluster clear job."""
        from bsimvis.app.routes.cluster import clear_cluster

        return clear_cluster()


@ns_cluster.route("/list")
class ClusterList(Resource):
    @ns_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "min_stability": "Min cluster stability",
            "min_count": "Min member count",
            "min_features": "Min features",
            "min_cohesion": "Min cohesion score",
            "sort_by": "Sort field (count, stability, features, cohesion)",
            "sort_order": "Sort order (asc/desc)",
            "format": "Output format (json/csv)",
            "q": "Search query across IDs and names",
            "cluster_id": "Filter by cluster ID",
            "cluster_uuid": "Filter by cluster UUID",
            "cluster_name": "Filter by cluster name",
            "show_members": "Whether to return direct member IDs/names (true/false)",
        }
    )
    def get(self):
        """Lists discovered clusters with metadata and filtering."""
        from bsimvis.app.routes.cluster import list_clusters

        return list_clusters()


@ns_cluster.route("/tree")
class ClusterTree(Resource):
    @ns_cluster.doc(params={"collection": "Collection name", "algo": "Algorithm"})
    def get(self):
        """Returns the condensed tree for the clustering."""
        from bsimvis.app.routes.cluster import get_cluster_tree

        return get_cluster_tree()


@ns_cluster.route("/meta")
class ClusterMeta(Resource):
    @ns_cluster.expect(
        api.model(
            "ClusterMetaUpdate",
            {
                "collection": fields.String(required=True),
                "algo": fields.String(default="unweighted_cosine"),
                "cluster_id": fields.String(required=True),
                "cluster_name": fields.String(required=True),
            },
        )
    )
    def post(self):
        """Updates metadata for a cluster (e.g. rename)."""
        from bsimvis.app.routes.cluster import update_cluster_meta

        return update_cluster_meta()


@ns_cluster.route("/members")
class ClusterMembers(Resource):
    @ns_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "cluster_id": "Target cluster ID",
            "limit": "Max results",
            "offset": "Pagination offset",
        }
    )
    def get(self):
        """Lists all function IDs in a specific cluster."""
        from bsimvis.app.routes.cluster import list_cluster_members

        return list_cluster_members()


@ns_cluster.route("/functions")
class ClusterFunctions(Resource):
    @ns_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "cluster_uuid": "Target cluster UUID",
            "limit": "Max results",
            "offset": "Pagination offset",
        }
    )
    def get(self):
        """Returns a quick sample of function metadata for a cluster UUID."""
        from bsimvis.app.routes.cluster import get_cluster_functions

        return get_cluster_functions()


# --- Binary Cluster Namespace ---
@ns_bin_cluster.route("/build")
class BinClusterBuild(Resource):
    @ns_bin_cluster.expect(
        api.model(
            "BinClusterBuild",
            {
                "collection": fields.String(default="main"),
                "algo": fields.String(default="unweighted_cosine"),
                "min_cluster_size": fields.Integer(default=2),
                "min_samples": fields.Integer(default=1),
                "epsilon": fields.Float(default=0.1),
                "selection_method": fields.String(default="eom"),
                "min_sim": fields.Float(default=0.0),
            },
        )
    )
    def post(self):
        """Enqueues a binary clustering job."""
        from bsimvis.app.routes.bin_cluster import build_bin_cluster

        return build_bin_cluster()


@ns_bin_cluster.route("/rebuild")
class BinClusterRebuild(Resource):
    @ns_bin_cluster.expect(api.models["BinClusterBuild"])
    def post(self):
        """Enqueues a clear + cluster pipeline for binaries."""
        from bsimvis.app.routes.bin_cluster import rebuild_bin_cluster

        return rebuild_bin_cluster()


@ns_bin_cluster.route("/clear")
class BinClusterClear(Resource):
    @ns_bin_cluster.expect(
        api.model(
            "BinClusterClear",
            {
                "collection": fields.String(default="main"),
                "algo": fields.String(default="unweighted_cosine"),
            },
        )
    )
    def post(self):
        """Enqueues a binary cluster clear job."""
        from bsimvis.app.routes.bin_cluster import clear_bin_cluster

        return clear_bin_cluster()


@ns_bin_cluster.route("/list")
class BinClusterList(Resource):
    @ns_bin_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "min_stability": "Min cluster stability",
            "min_count": "Min member count",
            "min_cohesion": "Min cohesion score",
            "sort_by": "Sort field (count, stability, cohesion)",
            "sort_order": "Sort order (asc/desc)",
            "format": "Output format (json/csv)",
            "q": "Search query across IDs and names",
            "cluster_id": "Filter by cluster ID",
            "cluster_uuid": "Filter by cluster UUID",
            "cluster_name": "Filter by cluster name",
            "show_members": "Whether to return direct member IDs/names (true/false)",
        }
    )
    def get(self):
        """Lists discovered binary clusters with metadata and filtering."""
        from bsimvis.app.routes.bin_cluster import list_bin_clusters

        return list_bin_clusters()


@ns_bin_cluster.route("/tree")
class BinClusterTree(Resource):
    @ns_bin_cluster.doc(params={"collection": "Collection name", "algo": "Algorithm"})
    def get(self):
        """Returns the condensed tree for binary clustering."""
        from bsimvis.app.routes.bin_cluster import get_bin_cluster_tree

        return get_bin_cluster_tree()


@ns_bin_cluster.route("/meta")
class BinClusterMeta(Resource):
    @ns_bin_cluster.expect(
        api.model(
            "BinClusterMetaUpdate",
            {
                "collection": fields.String(required=True),
                "algo": fields.String(default="unweighted_cosine"),
                "cluster_id": fields.String(required=True),
                "cluster_name": fields.String(required=True),
            },
        )
    )
    def post(self):
        """Updates metadata for a binary cluster (e.g. rename)."""
        from bsimvis.app.routes.bin_cluster import update_bin_cluster_meta

        return update_bin_cluster_meta()


@ns_bin_cluster.route("/members")
class BinClusterMembers(Resource):
    @ns_bin_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "cluster_id": "Target cluster ID",
            "limit": "Max results",
            "offset": "Pagination offset",
        }
    )
    def get(self):
        """Lists all file IDs in a specific binary cluster."""
        from bsimvis.app.routes.bin_cluster import list_bin_cluster_members

        return list_bin_cluster_members()


@ns_bin_cluster.route("/files")
class BinClusterFiles(Resource):
    @ns_bin_cluster.doc(
        params={
            "collection": "Collection name",
            "algo": "Algorithm",
            "cluster_uuid": "Target cluster UUID",
            "limit": "Max results",
            "offset": "Pagination offset",
        }
    )
    def get(self):
        """Returns a quick sample of file metadata for a cluster UUID."""
        from bsimvis.app.routes.bin_cluster import get_bin_cluster_files

        return get_bin_cluster_files()


# --- Features Namespace ---
@ns_features.route("/status")
class FeaturesStatus(Resource):
    @ns_features.doc(params={"collection": "Collection", "details": "Boolean"})
    def get(self):
        """Returns feature indexing status."""
        from bsimvis.app.routes.features import get_status

        return get_status()


@ns_features.route("/files")
class FeaturesFiles(Resource):
    @ns_features.doc(params={"collection": "Collection"})
    def get(self):
        """Returns indexing status for all files."""
        from bsimvis.app.routes.features import get_file_status

        return get_file_status()


@ns_features.route("/index")
class FeaturesIndex(Resource):
    @ns_features.expect(
        api.model(
            "FeatureIndexRequest",
            {
                "collection": fields.String(required=True),
                "md5": fields.String(),
                "batch": fields.String(),
            },
        )
    )
    def post(self):
        """Enqueues a feature indexing job."""
        from bsimvis.app.routes.features import index_features

        return index_features()


@ns_features.route("/clear")
class FeaturesClear(Resource):
    @ns_features.expect(
        api.model(
            "FeatureClearRequest",
            {
                "collection": fields.String(required=True),
                "md5": fields.String(),
                "batch": fields.String(),
            },
        )
    )
    def post(self):
        """Enqueues a feature clear job."""
        from bsimvis.app.routes.features import clear_features

        return clear_features()


# --- Diff Namespace ---
@ns_diff.route("")
class DiffView(Resource):
    @ns_diff.doc(params={"id1": "First function ID", "id2": "Second function ID"})
    def get(self):
        """Returns side-by-side aligned diff of two functions."""
        from bsimvis.app.routes.function_diff import diff_api

        return diff_api()


# --- Bin Sim Namespace ---
@ns_bin_sim.route("/build")
class BinSimBuild(Resource):
    @ns_bin_sim.expect(bin_sim_build_model)
    def post(self):
        """Enqueues a job to build binary similarities."""
        from bsimvis.app.routes.bin_sim import build_bin_sim

        return build_bin_sim()


@ns_bin_sim.route("/rebuild")
class BinSimRebuild(Resource):
    @ns_bin_sim.expect(bin_sim_build_model)
    def post(self):
        """Enqueues a pipeline to clear and build binary similarities."""
        from bsimvis.app.routes.bin_sim import rebuild_bin_sim

        return rebuild_bin_sim()


@ns_bin_sim.route("/clear")
class BinSimClear(Resource):
    @ns_bin_sim.expect(bin_sim_clear_model)
    def post(self):
        """Enqueues a job to clear binary similarities."""
        from bsimvis.app.routes.bin_sim import clear_bin_sim

        return clear_bin_sim()


@ns_bin_sim.route("/diff")
class BinSimDiff(Resource):
    @ns_bin_sim.doc(
        params={
            "collection": "Collection name (default: main)",
            "algo": "Algorithm (default: unweighted_cosine)",
            "md5_a": "First binary MD5",
            "md5_b": "Second binary MD5",
        }
    )
    def get(self):
        """Returns binary similarity diff doc for a pair of binaries."""
        from bsimvis.app.routes.bin_sim import get_bin_sim

        return get_bin_sim()


@ns_bin_sim.route("/list")
class BinSimList(Resource):
    @ns_bin_sim.doc(
        params={
            "collection": "Collection name (default: main)",
            "algo": "Algorithm (default: unweighted_cosine)",
            "md5": "Target binary MD5",
            "limit": "Max results",
            "offset": "Pagination offset",
        }
    )
    def get(self):
        """Lists pre-calculated similar binaries for a given binary MD5."""
        from bsimvis.app.routes.bin_sim import list_bin_sims

        return list_bin_sims()


@ns_bin_sim.route("/search")
class BinSimSearch(Resource):
    @ns_bin_sim.doc(
        params={
            "collection": {
                "description": "Collection name",
                "required": True,
                "example": "main",
            },
            "algo": {
                "description": "Algorithm (default: unweighted_cosine)",
                "default": "unweighted_cosine",
            },
            "q": {"description": "Keyword search (MD5, file names)", "example": "libc"},
            "md5": {"description": "Filter pairs involving this MD5 (either side)"},
            "md5_a": {"description": "Filter by exact md5_a"},
            "md5_b": {"description": "Filter by exact md5_b"},
            "file_name": {
                "description": "Filter by file name substring (either side)",
                "example": "libc",
            },
            "file_tag": {
                "description": "Filter by file tag (either side)",
                "example": "malware",
            },
            "min_score": {
                "description": "Minimum score (collection-weighted)",
                "example": 0.5,
            },
            "max_score": {"description": "Maximum score", "example": 1.0},
            "min_coverage_a": {
                "description": "Minimum coverage for binary A",
                "example": 0.5,
            },
            "max_coverage_a": {"description": "Maximum coverage for binary A"},
            "min_coverage_b": {"description": "Minimum coverage for binary B"},
            "max_coverage_b": {"description": "Maximum coverage for binary B"},
            "min_shared": {"description": "Minimum shared clusters", "example": 5},
            "max_shared": {"description": "Maximum shared clusters"},
            "sort_by": {
                "description": "Sort by: score (default), coverage_a, coverage_b, shared_clusters, computed_at"
            },
            "sort_order": {"description": "Sort direction: desc (default) or asc"},
            "offset": {"description": "Pagination offset", "default": 0},
            "limit": {"description": "Results per page", "default": 50},
        }
    )
    def get(self):
        """Search binary similarity pairs with rich filtering and sorting."""
        from bsimvis.app.routes.search_bin_sim import search_bin_sims

        return search_bin_sims()



@ns_bin_sim.route("/reindex")
class BinSimReindex(Resource):
    @ns_bin_sim.expect(
        api.model(
            "BinSimReindex",
            {
                "collection": fields.String(default="main"),
                "algo": fields.String(default="unweighted_cosine"),
            },
        )
    )
    def post(self):
        """Rebuilds secondary indexes for all existing binary similarity pairs (backfill)."""
        from bsimvis.app.routes.bin_sim import reindex_bin_sim

        return reindex_bin_sim()

# --- Notes Routes ---

@ns_notes.route("/add")
class NoteAdd(Resource):
    @ns_notes.expect(note_add_model)
    @ns_notes.response(200, "Success", note_model)
    def post(self):
        """Adds a note to a function."""
        from bsimvis.app.routes.notes import add_note
        return add_note()

@ns_notes.route("/update")
class NoteUpdate(Resource):
    @ns_notes.expect(note_update_model)
    @ns_notes.response(200, "Success", note_model)
    def put(self):
        """Updates an existing note."""
        from bsimvis.app.routes.notes import update_note
        return update_note()

@ns_notes.route("/remove")
class NoteRemove(Resource):
    @ns_notes.expect(note_remove_model)
    @ns_notes.response(200, "Success")
    def delete(self):
        """Removes a note from a function."""
        from bsimvis.app.routes.notes import remove_note
        return remove_note()

@ns_notes.route("/list")
class NoteList(Resource):
    @ns_notes.doc(params={
        "collection": "Collection name",
        "func_id": "Function ID"
    })
    @ns_notes.response(200, "Success", fields.List(fields.Nested(note_model)))
    def get(self):
        """Lists all notes for a function."""
        from bsimvis.app.routes.notes import get_notes
        return get_notes()

# --- LLM Namespace ---

@ns_llm.route("/summarize")
class LLMSummarize(Resource):
    @ns_llm.expect(llm_summary_request_model)
    def post(self):
        """Generates a summary for a function using LLM."""
        from bsimvis.app.routes.llm import summarize
        return summarize()

@ns_llm.route("/chat")
class LLMChat(Resource):
    @ns_llm.expect(llm_chat_request_model)
    def post(self):
        """Continues a discussion about a function using LLM."""
        from bsimvis.app.routes.llm import chat
        return chat()
