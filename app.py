import os
from dotenv import load_dotenv
from bsimvis.app import create_app

# Load environment variables
load_dotenv()

app = create_app()

if __name__ == "__main__":
    # Get host and port from environment or use defaults
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 5000))

    app.run(host=host, port=port, debug=True)
