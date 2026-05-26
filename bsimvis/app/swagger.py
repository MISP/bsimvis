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

# Monkey-patch to handle Flask Response objects (like from jsonify) under Python 3.13 / Flask 3
original_make_response = api.make_response

def custom_make_response(data, *args, **kwargs):
    from flask import Response
    if isinstance(data, Response):
        return data
    if isinstance(data, tuple) and len(data) > 0 and isinstance(data[0], Response):
        return data[0]
    return original_make_response(data, *args, **kwargs)

api.make_response = custom_make_response

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
ns_features = Namespace("features", description="Global feature indexing and status")
ns_diff = Namespace("diff", description="Function diff and alignment")

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
api.add_namespace(ns_features)
api.add_namespace(ns_diff)

# --- Models & Examples ---

# Common Models
error_model = api.model("Error", {
    "detail": fields.String(description="Error message", example="Function not found")
})

# Index Models
index_stats_model = api.model("IndexStats", {
    "collection": fields.String(example="main"),
    "file_count": fields.Integer(example=120),
    "function_count": fields.Integer(example=45000),
    "feature_count": fields.Integer(example=1200000),
    "similarity_pairs": fields.Integer(example=850000),
    "last_updated": fields.Integer(example=1775639990508)
})

# Job Models
job_model = api.model("Job", {
    "id": fields.String(example="7b8e23af-4b2a-4e6c-8a1d-3c9f2b1a0e5d"),
    "type": fields.String(example="build_sim"),
    "status": fields.String(example="completed"),
    "progress": fields.Float(example=1.0),
    "created_at": fields.Integer(example=1775639990508),
    "error": fields.String(example=""),
    "logs": fields.List(fields.String, example=["Starting similarity build...", "Processing batch 1/10..."])
})

# Function Models
function_meta_model = api.model("FunctionMeta", {
    "function_name": fields.String(example="main"),
    "file_name": fields.String(example="libc.so.6"),
    "file_md5": fields.String(example="16c2addf057b3e3b2703500462e38c1c"),
    "language_id": fields.String(example="AARCH64:LE:64:v8A"),
    "return_type": fields.String(example="int"),
    "parameters": fields.List(fields.String, example=["int argc", "char** argv"]),
    "bsim_features_count": fields.Integer(example=42),
    "entry_date": fields.String(example="2026-05-26 10:00:00")
})

# Similarity Models
sim_pair_model = api.model("SimilarityPair", {
    "id1": fields.String(example="main:func:16c2addf:10400"),
    "id2": fields.String(example="main:func:0ed905e8:10520"),
    "name1": fields.String(example="main"),
    "name2": fields.String(example="main"),
    "score": fields.Float(example=0.985),
    "feat_count": fields.Integer(example=42),
    "meta1": fields.Nested(function_meta_model),
    "meta2": fields.Nested(function_meta_model)
})

similarity_search_response = api.model("SimilaritySearchResponse", {
    "total": fields.Integer(example=1500),
    "offset": fields.Integer(example=0),
    "limit": fields.Integer(example=50),
    "pairs": fields.List(fields.Nested(sim_pair_model))
})

# --- Index Namespace ---
@ns_index.route("/status")
class IndexStatus(Resource):
    @ns_index.doc(params={
        "collection": "Collection name (default: main)",
        "details": "Return detailed stats (true/false)"
    })
    @ns_index.marshal_with(index_stats_model)
    def get(self):
        """Returns database index statistics and counts."""
        from bsimvis.app.routes.index import get_index_status
        return get_index_status()

# --- Jobs Namespace ---
@ns_jobs.route("")
class JobList(Resource):
    @ns_jobs.doc(params={
        "limit": "Number of jobs to return (default: 50)",
        "offset": "Pagination offset"
    })
    def get(self):
        """Lists recent and active background jobs."""
        from bsimvis.app.routes.jobs import list_jobs
        return list_jobs()

@ns_jobs.route("/stats")
class JobStats(Resource):
    def get(self):
        """Returns aggregate metrics across all jobs."""
        from bsimvis.app.routes.jobs import get_global_stats
        return get_global_stats()

@ns_jobs.route("/<string:job_id>")
class JobDetail(Resource):
    @ns_jobs.marshal_with(job_model)
    @ns_jobs.response(404, "Job not found", error_model)
    def get(self, job_id):
        """Returns detailed status and logs for a specific job."""
        from bsimvis.app.routes.jobs import get_job
        return get_job(job_id)

@ns_jobs.route("/<string:job_id>/cancel")
class JobCancel(Resource):
    def post(self, job_id):
        """Cancels a pending or running job."""
        from bsimvis.app.routes.jobs import cancel_job
        return cancel_job(job_id)

@ns_jobs.route("/<string:job_id>/retry")
class JobRetry(Resource):
    def post(self, job_id):
        """Retries a failed or cancelled job/pipeline."""
        from bsimvis.app.routes.jobs import retry_job
        return retry_job(job_id)

# --- Collection Namespace ---
@ns_collection.route("/search")
class CollectionSearch(Resource):
    @ns_collection.doc(params={"offset": "Pagination offset", "limit": "Max results", "q": "Keyword search"})
    def get(self):
        """Lists and searches available collections."""
        from bsimvis.app.routes.search_collection import search_collections
        return search_collections()

# --- Batch Namespace ---
@ns_batch.route("/search")
class BatchSearch(Resource):
    @ns_batch.doc(params={"collection": "Target collection", "q": "Keyword search"})
    def get(self):
        """Lists and searches ingestion batches."""
        from bsimvis.app.routes.search_collection import search_batches
        return search_batches()

# --- File Namespace ---
@ns_file.route("/search")
class FileSearch(Resource):
    @ns_file.doc(params={
        "collection": "Collection name",
        "file_name": "Filter by filename",
        "tag": "Filter by tag",
    })
    def get(self):
        """Search for files within a collection."""
        from bsimvis.app.routes.search_file import search_files
        return search_files()

@ns_file.route("/upload/file_data")
class FileUpload(Resource):
    def post(self):
        """Uploads raw analysis data for a new file."""
        from bsimvis.app.routes.file import upload_file_data
        return upload_file_data()

@ns_file.route("/call_graph")
class FileCallGraph(Resource):
    @ns_file.doc(params={
        "collection": "Collection name",
        "file_md5": "File MD5"
    })
    def get(self):
        """Returns the full call graph for a file."""
        from bsimvis.app.routes.function_code import get_file_call_graph
        return get_file_call_graph()

# --- Function Namespace ---
@ns_function.route("/search")
class FunctionSearch(Resource):
    @ns_function.doc(params={
        "collection": "Collection name",
        "function_name": "Filter by function name",
        "file_md5": "Filter by file MD5",
    })
    def get(self):
        """Search for functions within a collection."""
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
    @ns_function.doc(params={
        "id1": "First function ID",
        "id2": "Second function ID"
    })
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
    @ns_feature.doc(params={
        "collection": "Collection name",
        "hash": "Filter by feature hash (hex prefix)",
        "sort": "Sort by 'tf' or 'default'"
    })
    def get(self):
        """Search for BSim features and their frequency across the collection."""
        from bsimvis.app.routes.search_feature import search_features
        return search_features()

@ns_feature.route("/details/<string:f_hash>")
class FeatureDetails(Resource):
    @ns_feature.doc(params={
        "collection": "Collection name",
        "offset": "Pagination offset",
        "limit": "Max results"
    })
    def get(self, f_hash):
        """Returns all function occurrences for a specific feature hash."""
        from bsimvis.app.routes.search_feature import get_feature_details
        from flask import g
        g.f_hash = f_hash
        return get_feature_details(f_hash)

# --- Search Namespace ---
@ns_search.route("/autocomplete")
class SearchAutocomplete(Resource):
    @ns_search.doc(params={
        "collection": "Collection name",
        "level": "Index level (func, file, sim)",
        "field": "Field to search (e.g., function_name)",
        "q": "Search query prefix",
        "limit": "Max results (default: 50)"
    })
    def get(self):
        """Autocomplete for metadata fields."""
        from bsimvis.app.routes.search_similarity import autocomplete
        return autocomplete()

@ns_search.route("/fields")
class SearchFields(Resource):
    @ns_search.doc(params={
        "collection": "Collection name",
        "level": "Index level",
        "field": "List of fields to get stats for"
    })
    def get(self):
        """Returns cardinality stats for specified metadata fields."""
        from bsimvis.app.routes.search_similarity import get_field_stats
        return get_field_stats()

# --- Similarity Namespace ---
@ns_similarity.route("")
class SimilarityPair(Resource):
    @ns_similarity.doc(params={
        "id1": "First function ID",
        "id2": "Second function ID"
    })
    def get(self):
        """Returns similarity scores and tags for a specific function pair."""
        from bsimvis.app.routes.function_similarity import similarity_api
        return similarity_api()


@ns_similarity.route("/search")
class SimilaritySearch(Resource):
    @ns_similarity.doc(params={
        "collection": "Collection name",
        "algo": "Algorithm (unweighted_cosine, milvus_sparse)",
        "min_score": "Min similarity (0.95)",
        "max_score": "Max similarity (1.0)",
        "q": "General metadata query",
        "name": "Function/File name filter",
        "tag": "Filter by any tag",
        "md5": "Filter by file MD5",
        "cross_binary": "Filter cross-binary pairs (true/false)",
        "sort_by": "Sort by 'score' or 'feat_count'",
        "offset": "Pagination offset",
        "limit": "Results per page"
    })
    def get(self):
        """Main similarity search engine with complex filtering."""
        from bsimvis.app.routes.search_similarity import similarity_search
        return similarity_search()

@ns_similarity.route("/status")
class SimilarityStatus(Resource):
    @ns_similarity.doc(params={"collection": "Collection", "md5": "File MD5", "batch": "Batch UUID"})
    def get(self):
        """Returns similarity build status (total vs built) for a target."""
        from bsimvis.app.routes.similarity import similarity_status
        return similarity_status()

@ns_similarity.route("/batches")
class SimilarityBatches(Resource):
    @ns_similarity.doc(params={"collection": "Collection", "by": "batch or md5"})
    def get(self):
        """Lists build status for all batches or files in a collection."""
        from bsimvis.app.routes.similarity import list_batches
        return list_batches()

@ns_similarity.route("/list")
class SimilarityList(Resource):
    @ns_similarity.doc(params={"collection": "Collection", "md5": "File MD5"})
    def get(self):
        """Lists pre-calculated similarity results for a file."""
        from bsimvis.app.routes.similarity import list_similarities
        return list_similarities()

@ns_similarity.route("/build")
class SimilarityBuild(Resource):
    @ns_similarity.expect(api.model("SimilarityBuild", {
        "collection": fields.String(required=True, example="main"),
        "md5": fields.String(example="16c2addf..."),
        "batch": fields.String(example="uuid..."),
        "algo": fields.String(default="unweighted_cosine"),
        "min_score": fields.Float(default=0.95),
        "top_k": fields.Integer(default=20)
    }))
    def post(self):
        """Enqueues a job to pre-calculate similarities."""
        from bsimvis.app.routes.similarity import build_similarity
        return build_similarity()

@ns_similarity.route("/rebuild")
class SimilarityRebuild(Resource):
    def post(self):
        """Enqueues a clear + build pipeline for similarities."""
        from bsimvis.app.routes.similarity import rebuild_similarity
        return rebuild_similarity()

@ns_similarity.route("/clear")
class SimilarityClear(Resource):
    def post(self):
        """Enqueues a similarity clear job."""
        from bsimvis.app.routes.similarity import clear_similarity
        return clear_similarity()

# --- Tags Namespace ---
@ns_tags.route("")
class TagList(Resource):
    @ns_tags.doc(params={"collection": "Collection name"})
    def get(self):
        """Returns the global tag index for a collection."""
        from bsimvis.app.routes.tags import get_tags
        return get_tags()

@ns_tags.route("/add")
class TagAdd(Resource):
    @ns_tags.expect(api.model("TagAdd", {
        "collection": fields.String(required=True, example="main"),
        "entity_type": fields.String(required=True, enum=["file", "function", "similarity"]),
        "entity_id": fields.String(required=True, example="16c2addf..."),
        "tag": fields.String(required=True, example="vulnerable")
    }))
    def post(self):
        """Adds a tag to an entity."""
        from bsimvis.app.routes.tags import add_tag
        return add_tag()

@ns_tags.route("/bulk_add")
class TagBulkAdd(Resource):
    @ns_tags.expect(api.model("TagBulkAdd", {
        "collection": fields.String(required=True),
        "entity_type": fields.String(required=True),
        "entity_ids": fields.List(fields.String, required=True),
        "tag": fields.String(required=True)
    }))
    def post(self):
        """Adds a tag to multiple entities."""
        from bsimvis.app.routes.tags import add_bulk_tags
        return add_bulk_tags()

@ns_tags.route("/remove")
class TagRemove(Resource):
    @ns_tags.expect(api.model("TagRemove", {
        "collection": fields.String(required=True),
        "entity_type": fields.String(required=True),
        "entity_id": fields.String(required=True),
        "tag": fields.String(required=True)
    }))
    def post(self):
        """Removes a tag from an entity."""
        from bsimvis.app.routes.tags import remove_tag
        return remove_tag()

@ns_tags.route("/bulk_remove")
class TagBulkRemove(Resource):
    @ns_tags.expect(api.model("TagBulkRemove", {
        "collection": fields.String(required=True),
        "entity_type": fields.String(required=True),
        "entity_ids": fields.List(fields.String, required=True),
        "tag": fields.String(required=True)
    }))
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

@ns_tags.route("/set_color")
@ns_tags.route("/color")
class TagSetColor(Resource):
    @ns_tags.expect(api.model("TagSetColor", {
        "collection": fields.String(required=True),
        "tag": fields.String(required=True),
        "color": fields.String(required=True, example="#ff0000")
    }))
    def post(self):
        """Sets a custom color for a tag."""
        from bsimvis.app.routes.tags import set_color
        return set_color()

@ns_tags.route("/set_priority")
@ns_tags.route("/priority")
class TagSetPriority(Resource):
    @ns_tags.expect(api.model("TagSetPriority", {
        "collection": fields.String(required=True),
        "tag": fields.String(required=True),
        "priority": fields.Integer(required=True)
    }))
    def post(self):
        """Sets a custom priority for a tag."""
        from bsimvis.app.routes.tags import set_priority
        return set_priority()

# --- Cluster Namespace ---
@ns_cluster.route("/build")
class ClusterBuild(Resource):
    @ns_cluster.expect(api.model("ClusterBuild", {
        "collection": fields.String(default="main"),
        "algo": fields.String(default="unweighted_cosine"),
        "min_cluster_size": fields.Integer(default=5),
        "min_sim": fields.Float(default=0.0)
    }))
    def post(self):
        """Enqueues a clustering job."""
        from bsimvis.app.routes.cluster import build_cluster
        return build_cluster()

@ns_cluster.route("/rebuild")
class ClusterRebuild(Resource):
    def post(self):
        """Enqueues a clear + cluster pipeline."""
        from bsimvis.app.routes.cluster import rebuild_cluster
        return rebuild_cluster()

@ns_cluster.route("/clear")
class ClusterClear(Resource):
    def post(self):
        """Enqueues a cluster clear job."""
        from bsimvis.app.routes.cluster import clear_cluster
        return clear_cluster()

@ns_cluster.route("/list")
class ClusterList(Resource):
    @ns_cluster.doc(params={
        "collection": "Collection name",
        "algo": "Algorithm",
        "min_stability": "Min cluster stability",
        "min_count": "Min member count",
        "sort_by": "Sort field (count, stability, features, cohesion)"
    })
    def get(self):
        """Lists discovered clusters with metadata and filtering."""
        from bsimvis.app.routes.cluster import list_clusters
        return list_clusters()

@ns_cluster.route("/tree")
class ClusterTree(Resource):
    def get(self):
        """Returns the condensed tree for the clustering."""
        from bsimvis.app.routes.cluster import get_cluster_tree
        return get_cluster_tree()

@ns_cluster.route("/meta")
class ClusterMeta(Resource):
    @ns_cluster.expect(api.model("ClusterMetaUpdate", {
        "collection": fields.String(required=True),
        "cluster_id": fields.String(required=True),
        "cluster_name": fields.String(required=True)
    }))
    def post(self):
        """Updates metadata for a cluster (e.g. rename)."""
        from bsimvis.app.routes.cluster import update_cluster_meta
        return update_cluster_meta()

@ns_cluster.route("/members")
class ClusterMembers(Resource):
    @ns_cluster.doc(params={"cluster_id": "Target cluster ID", "limit": "Max results"})
    def get(self):
        """Lists all function IDs in a specific cluster."""
        from bsimvis.app.routes.cluster import list_cluster_members
        return list_cluster_members()

@ns_cluster.route("/functions")
class ClusterFunctions(Resource):
    @ns_cluster.doc(params={"cluster_uuid": "Target cluster UUID"})
    def get(self):
        """Returns a quick sample of function metadata for a cluster UUID."""
        from bsimvis.app.routes.cluster import get_cluster_functions
        return get_cluster_functions()

@ns_cluster.route("/dendrogram")
class ClusterDendrogram(Resource):
    @ns_cluster.doc(params={
        "stability_threshold": "Cut-off stability",
        "min_cluster_size": "Min size filter"
    })
    def get(self):
        """Returns a hierarchical tree of clusters (D3-compatible)."""
        from bsimvis.app.routes.cluster import get_cluster_dendrogram
        return get_cluster_dendrogram()

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
    @ns_features.expect(api.model("FeatureIndexRequest", {
        "collection": fields.String(required=True),
        "md5": fields.String(),
        "batch": fields.String()
    }))
    def post(self):
        """Enqueues a feature indexing job."""
        from bsimvis.app.routes.features import index_features
        return index_features()

@ns_features.route("/clear")
class FeaturesClear(Resource):
    @ns_features.expect(api.model("FeatureClearRequest", {
        "collection": fields.String(required=True),
        "md5": fields.String(),
        "batch": fields.String()
    }))
    def post(self):
        """Enqueues a feature clear job."""
        from bsimvis.app.routes.features import clear_features
        return clear_features()

# --- Diff Namespace ---
@ns_diff.route("")
class DiffView(Resource):
    @ns_diff.doc(params={
        "id1": "First function ID",
        "id2": "Second function ID"
    })
    def get(self):
        """Returns side-by-side aligned diff of two functions."""
        from bsimvis.app.routes.function_diff import diff_api
        return diff_api()
