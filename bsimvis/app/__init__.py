from flask import Flask, send_from_directory, request, g
from flask_cors import CORS
import os
import time
import logging


def create_app():

    # Tell Flask the static folder is one level up
    app = Flask(__name__, static_folder="static")
    CORS(app)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s"
    )

    # 1. Initialize Lua Scripts
    from .services.lua_manager import lua_manager
    lua_manager.init_app(app)

    # 2. Performance Hooks
    @app.before_request
    def start_timer():
        g.start_time = time.time()

    @app.after_request
    def log_response(response):
        if hasattr(g, "start_time"):
            elapsed = (time.time() - g.start_time) * 1000
            # Only log API calls to keep the terminal clean from static file spam
            if request.path.startswith("/api"):
                logging.info(
                    f"{request.method} {request.path} "
                    f"| Status: {response.status_code} "
                    f"| Time: {elapsed:.2f}ms"
                )
        return response

    from .routes.function_diff import function_diff_bp
    from .routes.function_code import function_code_bp
    from .routes.function_feature import function_feature_bp
    from .routes.search_collection import search_collection_bp
    from .routes.search_file import search_file_bp
    from .routes.search_function import search_function_bp
    from .routes.search_feature import search_feature_bp
    from .routes.search_similarity import search_similarity_bp
    from .routes.function_similarity import function_similarity_bp
    from .routes.jobs import jobs_bp
    from .routes.file import file_bp
    from .routes.similarity import similarity_bp
    from .routes.features import features_bp
    from .routes.index import index_bp

    app.register_blueprint(function_diff_bp)
    app.register_blueprint(function_code_bp)
    app.register_blueprint(function_feature_bp)
    app.register_blueprint(search_collection_bp)
    app.register_blueprint(search_file_bp)
    app.register_blueprint(search_function_bp)
    app.register_blueprint(search_feature_bp)
    app.register_blueprint(search_similarity_bp)
    app.register_blueprint(function_similarity_bp)
    app.register_blueprint(jobs_bp)
    app.register_blueprint(file_bp)
    app.register_blueprint(similarity_bp)
    app.register_blueprint(features_bp)
    app.register_blueprint(index_bp)

    # Serve the Bare JS frontend
    @app.route("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/<path:path>")
    def serve_static(path):
        return send_from_directory(app.static_folder, path)

    return app
