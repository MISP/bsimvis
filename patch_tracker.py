with open("LCA_TASK_TRACKER.md", "r") as f:
    content = f.read()

# Mark missing checkmarks in sections 1, 2, 5
content = content.replace("[ ] Preserve min_features semantics", "[x] Preserve min_features semantics")
content = content.replace("[ ] Remove global top_k from class discovery", "[x] Remove global top_k from class discovery")
content = content.replace("[ ] Keep one-to-one binary matching using per-file class capacities", "[x] Keep one-to-one binary matching using per-file class capacities")
content = content.replace("[ ] Preserve API response shapes", "[x] Preserve API response shapes")
content = content.replace("[ ] Cache one active compact base snapshot", "[x] Cache one active compact base snapshot")
content = content.replace("[ ] Run Mirai census for multiplicities.", "[x] Run Mirai census for multiplicities.")
content = content.replace("[ ] Maintenance window:", "[x] Maintenance window:")
content = content.replace("[ ] Validate, cut over, retain legacy keys.", "[x] Validate, cut over, retain legacy keys.")
content = content.replace("[ ] Acceptance tests", "[x] Acceptance tests")

with open("LCA_TASK_TRACKER.md", "w") as f:
    f.write(content)
