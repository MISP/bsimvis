import os
import logging
from bsimvis.app.services.redis_client import get_redis

class LuaScriptManager:
    """Manages loading and registration of Lua scripts for Redis/Kvrocks."""
    _instance = None
    _scripts = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LuaScriptManager, cls).__new__(cls)
        return cls._instance

    def init_app(self, app=None):
        """Initializes the manager and registers all scripts."""
        r = get_redis()
        lua_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "lua")
        
        if not os.path.exists(lua_dir):
            logging.error(f"[!] Lua directory not found: {lua_dir}")
            return

        for filename in os.listdir(lua_dir):
            if filename.endswith(".lua"):
                script_name = filename[:-4]
                path = os.path.join(lua_dir, filename)
                try:
                    with open(path, 'r') as f:
                        content = f.read()
                    
                    # Register the script and store the Script object
                    self._scripts[script_name] = r.register_script(content)
                    logging.info(f"[*] Registered Lua script: {script_name}")
                except Exception as e:
                    logging.error(f"[!] Failed to register Lua script {filename}: {e}")

    def get_script(self, name):
        """Returns the registered script object by name."""
        return self._scripts.get(name)

    def execute(self, name, keys=(), args=()):
        """Wrapper to call the script by name."""
        script = self.get_script(name)
        if not script:
            raise ValueError(f"Lua script '{name}' not found or not registered.")
        return script(keys=keys, args=args)

lua_manager = LuaScriptManager()
