import redis
from bsimvis.app.services.lua_manager import lua_manager

r = redis.Redis(port=7720)
lua_manager.r = r
lua_manager.register_all(force=True)
script = lua_manager.get_script("get_or_create_vclass")
v_hash = "2773db2a0c334c84c65b940191fd42953601154e87d1f9d0b2ddd9f49fc9fe34"
res = script(keys=[v_hash], args=["test_bench"], client=r)
print(res)
