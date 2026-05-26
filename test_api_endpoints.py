import requests
import json

BASE_URL = "http://localhost:5000"
COLLECTION = "test_api"
FILE_MD5 = "59281a167473ca9b98515b11cb709f82"
FUNC_ID1 = "test_api:func:59281a167473ca9b98515b11cb709f82:00101144" # main
FUNC_ID2 = "test_api:func:59281a167473ca9b98515b11cb709f82:00100974" # tea_encrypt

results = []

def test_endpoint(method, path, params=None, data=None):
    url = f"{BASE_URL}{path}"
    try:
        if method == "GET":
            response = requests.get(url, params=params)
        elif method == "POST":
            response = requests.post(url, json=data)
        
        status = response.status_code
        try:
            body = response.json()
        except:
            body = response.text[:100]
            
        results.append({
            "path": path,
            "method": method,
            "status": status,
            "success": 200 <= status < 300,
            "error": body if status >= 400 else None
        })
        print(f"{method} {path} -> {status}")
    except Exception as e:
        results.append({
            "path": path,
            "method": method,
            "status": "ERROR",
            "success": False,
            "error": str(e)
        })
        print(f"{method} {path} -> ERROR: {e}")

# Index
test_endpoint("GET", "/api/index/status", {"collection": COLLECTION})

# Jobs
test_endpoint("GET", "/api/jobs")
test_endpoint("GET", "/api/jobs/stats")

# Batch
test_endpoint("GET", "/api/batch/search", {"collection": COLLECTION})

# File
test_endpoint("GET", "/api/file/call_graph", {"collection": COLLECTION, "file_md5": FILE_MD5})

# Function
test_endpoint("GET", "/api/function/code", {"id": FUNC_ID1})
test_endpoint("GET", "/api/function/diff", {"id1": FUNC_ID1, "id2": FUNC_ID2})
test_endpoint("GET", "/api/function/features", {"id": FUNC_ID1})

# Feature
test_endpoint("GET", "/api/feature/search", {"collection": COLLECTION})

# Search
test_endpoint("GET", "/api/search/autocomplete", {"collection": COLLECTION, "level": "func", "field": "function_name", "q": "ma"})
test_endpoint("GET", "/api/search/fields", {"collection": COLLECTION, "level": "func", "field": "function_name"})

# Similarity
test_endpoint("GET", "/api/similarity", {"id1": FUNC_ID1, "id2": FUNC_ID2})
test_endpoint("GET", "/api/similarity/search", {"collection": COLLECTION})
test_endpoint("GET", "/api/similarity/list", {"collection": COLLECTION, "md5": FILE_MD5})

# Tags
test_endpoint("GET", "/api/tags/metadata", {"collection": COLLECTION})
test_endpoint("GET", "/api/tags/stats", {"collection": COLLECTION, "tag": "hey"})

# Cluster
test_endpoint("GET", "/api/cluster/list", {"collection": COLLECTION})
test_endpoint("GET", "/api/cluster/tree", {"collection": COLLECTION})
# For members/functions we need a cluster ID/UUID, let's skip for now or get from list if successful

# Features status
test_endpoint("GET", "/api/features/status", {"collection": COLLECTION})
test_endpoint("GET", "/api/features/files", {"collection": COLLECTION})

with open("api_test_report.json", "w") as f:
    json.dump(results, f, indent=2)

print("\nReport saved to api_test_report.json")
