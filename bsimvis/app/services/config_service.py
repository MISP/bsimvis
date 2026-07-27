import logging
import os
import tomllib
from pathlib import Path

DEFAULT_CONFIG_NAME = "bsimvis_config.toml"


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
