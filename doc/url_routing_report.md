# URL Routing & Navigation Report

This report documents how URL routing, redirection, and URL hierarchy/navigation work in the current version of the `bsimvis` webapp.

---

## 1. Flask Routing (Backend Catch-All)

The backend Flask application in [`bsimvis/app/__init__.py`](file:///home/thomas/projects/bsimvis2/bsimvis/app/__init__.py) acts as a catch-all server for Frontend Single Page Application (SPA) routes. It returns the central `index.html` file for any matched UI paths:

```python
@app.route("/collection/<collection>/<path:rest>")
@app.route("/collections/<collection>/<path:rest>")
@app.route("/collection/<collection>")
@app.route("/collections/<collection>")
@app.route("/collections")
@app.route("/pools/<pool_id>/collections/<collection>/<path:rest>")
@app.route("/pools/<pool_id>/collections/<collection>")
@app.route("/pools/<collection>/<path:rest>")
@app.route("/pools/<collection>")
@app.route("/pools")
@app.route("/pool/<pool_id>/collections/<collection>/<path:rest>")
@app.route("/pool/<pool_id>/collections/<collection>")
@app.route("/pool/<collection>/<path:rest>")
@app.route("/pool/<collection>")
@app.route("/pool")
@app.route("/jobs")
@app.route("/upload")
def dashboard_ui(collection=None, rest=None, pool_id=None):
    return send_from_directory(app.static_folder, "index.html")
```

Any unknown static route that raises a 404 is also redirected to `index.html` to serve as a fallback:
```python
@app.route("/<path:path>")
def serve_static(path):
    try:
        return send_from_directory(app.static_folder, path)
    except NotFound:
        return send_from_directory(app.static_folder, "index.html")
```

---

## 2. Client-Side Parsing & Internal Normalization

When the page loads, the frontend parses the URL path using `parseRestfulPath()` in [`utils.js`](file:///home/thomas/projects/bsimvis2/bsimvis/app/static/js/utils.js). 

### Collection ID Syntax mapping:
To keep routing uniform, pools and sub-collections are internally normalized into special colon-delimited collection strings:
- A pool `pool_id` is mapped to the collection ID: `pool:{pool_id}`
- A collection `coll_id` inside pool `pool_id` is mapped to: `pool:{pool_id}:col:{coll_id}`

### Path normalization inside `parseRestfulPath()`:
Before parsing segments, the path string is normalized:
```javascript
path = path.replace(/\/pools?\/([^/]+)\/collections?\/([^/]+)/g, '/collections/pool:$1:col:$2');
path = path.replace(/\/pools?\/([^/]+)/g, '/collections/pool:$1');
```

Then, the segments are split and mapped to views as follows:

| Path Pattern (normalized representation) | Extracted Params | Target SPA View (`viewKey`) |
| :--- | :--- | :--- |
| `/collections` or `/` | `collection = 'main'` | `collections` (Dashboard) |
| `/jobs` | | `jobs` |
| `/upload` | | `upload` |
| `/collections/{coll}/batches` | | `batches` |
| `/collections/{coll}/files` | | `files` |
| `/collections/{coll}/files/similarities` | | `binary-similarity` |
| `/collections/{coll}/files/clusters` | | `bin-clusters` |
| `/collections/{coll}/files/{md5}` | `md5` | `file` |
| `/collections/{coll}/files/{md5}/functions` | `md5` | `call_graph` |
| `/collections/{coll}/files/{md5}/functions/{addr}` | `md5`, `address` | `function` |
| `/collections/{coll}/files/{md5}/functions/{addr}/features` | `md5`, `address` | `function_features` |
| `/collections/{coll}/files/{md5}/functions/{addr}/vs/{coll_b}/{md5_b}/{addr_b}` | `md5`, `address`, `coll_b`, `md5_b`, `addr_b` | `diff` (Function Diff) |
| `/collections/{coll}/files/{md5}/vs/{coll_b}/{md5_b}` | `md5`, `coll_b`, `md5_b` | `bin_sim` (File Diff) |
| `/collections/{coll}/functions` | | `functions` |
| `/collections/{coll}/functions/similarities` | | `function-similarity` |
| `/collections/{coll}/functions/clusters` | | `clusters` |
| `/collections/{coll}/functions/{md5}/{address}` | `md5`, `address` | `function` |
| `/collections/{coll}/functions/{md5}/{address}/features` | `md5`, `address` | `function_features` |
| `/collections/{coll}/functions/{md5_a}/{addr_a}/vs/{coll_b}/{md5_b}/{addr_b}` | `id1`, `id2` | `diff` |
| `/collections/{coll}/features` | | `features-global` |
| `/collections/{coll}/features/{hash}` | `hash` | `feature` |

---

## 3. Redirection & URL Rewriting Mechanisms

URL rewriting and redirection happen dynamically in the client via two mechanisms in [`navigation.js`](file:///home/thomas/projects/bsimvis2/bsimvis/app/static/js/navigation.js):

### A. Pre-Navigation Path Normalization (`openPath`):
When navigating via code (e.g. clicking buttons, rows, breadcrumbs, using `window.Nav.openPath(path, event)`):
1. The path is parsed, and pool collections are reconstructed back into clean plural URLs.
2. For instance, `/collections/pool:p1:col:c1/files` becomes `/pools/p1/collections/c1/files` (or `/pool/p1/collections/c1/files` depending on the current pathname prefix).
3. SPA routing uses `history.pushState(null, '', targetPath)` followed by `window.refreshData()`.

### B. Anchor Hover/Click Normalization (`normalizeAnchor`):
To prevent `<a>` elements from displaying internal collection format links (e.g., `/collections/pool:foo...`) when hovered or clicked, a document-level event listener automatically intercept hover/mousedown/contextmenu/focus events to rewrite the `href` attribute to the human-readable clean format:
```javascript
target = target.replace(/\/collections?\/pool(:|%3A|%253A)([^/]+)(:|%3A|%253A)col(:|%3A|%253A)([^/]+)/g, `/${prefix}/$2/collections/$5`);
target = target.replace(/\/collections?\/pool(:|%3A|%253A)/g, `/${prefix}/`);
```

---

## 4. Current URL Mapping Summary

Based on the mapping above, here are the actual URLs exposed to the user in the current version of the webapp:

### Pool Navigation and Views:
- **Pool List**: `/pools`
- **Pool Base**: `/pools/{pool_id}` (maps to `files` view of the pool)
- **Pool Files List**: `/pools/{pool_id}/files`
- **Pool Files Similarities**: `/pools/{pool_id}/files/similarities`
- **Pool Functions List**: `/pools/{pool_id}/functions`
- **Pool Functions Similarities**: `/pools/{pool_id}/functions/similarities`
- **Pool Collection Files List**: `/pools/{pool_id}/collections/{coll_id}/files`
- **Pool Collection Functions List**: `/pools/{pool_id}/collections/{coll_id}/functions`

### Details and Diffing under Collections (and Pool Collections):
- **File Detail**: `/collections/{coll}/files/{md5}`
- **Call Graph**: `/collections/{coll}/files/{md5}/functions`
- **Function Detail**: `/collections/{coll}/files/{md5}/functions/{addr}`
- **Function Features**: `/collections/{coll}/files/{md5}/functions/{addr}/features`
- **Function Diff**: `/collections/{coll}/files/{md5}/functions/{addr}/vs/{coll_b}/{md5_b}/{addr_b}`
- **File Diff**: `/collections/{coll}/files/{md5}/vs/{coll_b}/{md5_b}`
