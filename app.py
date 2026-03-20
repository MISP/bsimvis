from bsimvis.app import create_app
from bsimvis.database.setup_redis import setup_indices

setup_indices()
app = create_app()

if __name__ == "__main__":
    # Standard development port
    app.run(host="0.0.0.0", port=8000, debug=True)