import logging
import os
import tomllib
from pathlib import Path

DEFAULT_CONFIG_NAME = "bsimvis_config.toml"
# Uploaded binaries are written back out of Kvrocks to a directory on disk
# before Ghidra, capa and upx read them. /tmp is a small tmpfs on plenty of
# hosts, so keep them under the install instead; storage.upload_dir overrides.
DEFAULT_UPLOAD_DIR = "data/uploads"


def upload_dir():
    """Where an uploaded binary is written for analysis. None = system temp.

    ponytail: a `dir=` at the call sites that hold the upload, not a global
    tempfile.tempdir/TMPDIR. The global version also moved Ghidra's scratch
    project, and Ghidra rejects any path element starting with "." -- which is
    every worktree here, since they live under .claude/worktrees.
    """
    path = config_service.get("storage.upload_dir", DEFAULT_UPLOAD_DIR)
    if not path:
        return None
    path = os.path.abspath(os.path.expanduser(path))
    try:
        os.makedirs(path, exist_ok=True)
    except OSError as e:
        logging.warning(f"storage.upload_dir {path} unusable, using system temp: {e}")
        return None
    return path


class ConfigService:
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigService, cls).__new__(cls)
            cls._instance._load_config()
        return cls._instance

    def _load_config(self):
        try:
            config_path = Path(DEFAULT_CONFIG_NAME)
            if config_path.exists():
                with open(config_path, "rb") as f:
                    self._config = tomllib.load(f)
            else:
                example_path = Path("bsimvis_config.toml.example")
                if example_path.exists():
                    with open(example_path, "rb") as f:
                        self._config = tomllib.load(f)
                else:
                    self._config = {}
        except Exception as e:
            logging.warning(f"Failed to load default config: {e}")
            self._config = {}

    def get(self, key_path, default=None):
        """
        Retrieves a value from the config using a dot-separated path.
        Example: get("clustering.epsilon", 0.1)
        """
        parts = key_path.split(".")
        val = self._config
        try:
            for part in parts:
                val = val[part]
            return val
        except (KeyError, TypeError):
            return default


config_service = ConfigService()


if __name__ == "__main__":
    # python -m bsimvis.app.services.config_service
    import shutil
    import tempfile

    # Configured value wins and the directory is created on demand.
    target = os.path.join(os.path.abspath(DEFAULT_UPLOAD_DIR), "_selfcheck")
    config_service._config["storage"] = {"upload_dir": target}
    assert upload_dir() == target and os.path.isdir(target), upload_dir()
    scratch = tempfile.mkdtemp(prefix="bsim_worker_", dir=upload_dir())
    assert scratch.startswith(target), scratch
    shutil.rmtree(target)

    # Empty value opts out, and an uncreatable one falls back to the system
    # temp instead of taking the worker down.
    for path in ("", "/proc/nope/x"):
        config_service._config["storage"] = {"upload_dir": path}
        assert upload_dir() is None, path

    print("config_service self-check OK")
