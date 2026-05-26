from flask import Flask, send_from_directory, request, g
from flask_cors import CORS
import os
import time
import logging


def create_app():

    # Tell Flask the static folder is one level up
    app = Flask(__name__, static_folder="static")
    CORS(app)

    # Disable default Flask static file caching
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0

    # Allow large JSON uploads (e.g., 1GB)
    app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024 * 1024 * 1024
    # Increase form memory size for multi-part forms if needed
    app.config["MAX_FORM_MEMORY_SIZE"] = 100 * 1024 * 1024 * 1024
    app.config["RESTX_MASK_SWAGGER"] = False

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
        # Prevent proxy and browser caching of all assets/responses to ensure fresh reload
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

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

    from .swagger import api_bp
    app.register_blueprint(api_bp)

    # Serve the Bare JS frontend
    @app.route("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/<path:path>")
    def serve_static(path):
        return send_from_directory(app.static_folder, path)

    return app
