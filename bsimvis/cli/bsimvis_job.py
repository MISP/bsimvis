import requests
import json
import time
import sys
import datetime
import re

API_BASE = "http://localhost:5000/api"


def run_job(host, port, args):
    global API_BASE
    API_BASE = f"http://{host}:{port}/api"

    if args.action == "list":
        list_jobs(
            args.limit,
            getattr(args, "tree", False),
            getattr(args, "depth", 2),
            getattr(args, "follow", False),
            getattr(args, "parent", None),
            getattr(args, "collection", None),
            getattr(args, "pool", None),
        )
    elif args.action == "status":
        job_status(args.job_id, args.watch, args.logs)
    elif args.action == "cancel":
        cancel_job(args.job_id)
    elif args.action == "retry":
        retry_job(args.job_id)
    elif args.action == "perf":
        job_perf(args.job_id, args.top)


def job_perf(job_id, top_n=10):
    try:
        resp = requests.get(f"{API_BASE}/jobs/{job_id}")
        resp.raise_for_status()
        job = resp.json()

        print(f"\n=== PERFORMANCE REPORT: {job_id} ===")
        print(f"Type:   {job.get('type')}")
        print(f"Status: {job.get('status')}")
        print("-" * 85)

        all_ops = []

        # Handle Pipeline/Sub-tasks
        if "sub_tasks" in job and job["sub_tasks"]:
            print(
                f"{'Sub-Task Type':<20} | {'Total (s)':>10} | {'Python':>10} | {'DB':>10} | {'Lua':>10}"
            )
            print("-" * 85)

            agg_total = 0.0
            agg_python = 0.0
            agg_db = 0.0
            agg_lua = 0.0

            for st in job["sub_tasks"]:
                t = float(st.get("perf_total", 0))
                p = float(st.get("perf_python", 0))
                d = float(st.get("perf_db", 0))
                l = float(st.get("perf_lua", 0))

                agg_total += t
                agg_python += p
                agg_db += d
                agg_lua += l

                print(
                    f"{st['type']:<20} | {t:>10.4f} | {p:>10.4f} | {d:>10.4f} | {l:>10.4f}"
                )

                # Fetch full details for each sub-task to aggregate operations
                try:
                    st_resp = requests.get(f"{API_BASE}/jobs/{st['id']}")
                    if st_resp.status_code == 200:
                        st_full = st_resp.json()
                        details = st_full.get("perf_details", [])
                        for op in details:
                            op["task_type"] = st["type"]
                            all_ops.append(op)
                except:
                    pass

            print("-" * 85)
            print(
                f"{'GRAND TOTAL':<20} | {agg_total:>10.4f} | {agg_python:>10.4f} | {agg_db:>10.4f} | {agg_lua:>10.4f}"
            )

            if agg_total > 0:
                print(
                    f"{'PERCENTAGE':<20} | {'100%':>10} | {agg_python/agg_total*100:>9.1f}% | {agg_db/agg_total*100:>9.1f}% | {agg_lua/agg_total*100:>9.1f}%"
                )

        else:
            # Single job stats
            t = float(job.get("perf_total", 0))
            p = float(job.get("perf_python", 0))
            d = float(job.get("perf_db", 0))
            l = float(job.get("perf_lua", 0))

            if t == 0 and job.get("status") == "pending":
                print("Job is still pending. No performance data available yet.")
                return

            print(f"Total Time:    {t:.4f} s")
            print(f"Pure Python:   {p:.4f} s ({p/t*100 if t>0 else 0:.1f}%)")
            print(f"DB Queries:    {d:.4f} s ({d/t*100 if t>0 else 0:.1f}%)")
            print(f"Lua Scripts:   {l:.4f} s ({l/t*100 if t>0 else 0:.1f}%)")
            print(f"Operations:    {job.get('perf_ops', 'N/A')}")

            details = job.get("perf_details", [])
            for op in details:
                op["task_type"] = job["type"]
                all_ops.append(op)

        # Display Top Demanding Commands
        if all_ops:
            print(f"\nTOP {top_n} MOST DEMANDING COMMANDS")
            print("-" * 85)
            print(
                f"{'Task Source':<15} | {'Command':<25} | {'Duration (s)':>12} | {'Category':<15}"
            )
            print("-" * 85)

            # Sort by execution time descending
            sorted_ops = sorted(all_ops, key=lambda x: x.get("time", 0), reverse=True)
            for op in sorted_ops[:top_n]:
                duration = op.get("time", 0)
                print(
                    f"{op.get('task_type', 'N/A'):<15} | {op.get('op', 'N/A'):<25} | {duration:>12.6f} | {op.get('cat', 'N/A'):<15}"
                )

    except Exception as e:
        print(f"Error fetching performance stats: {e}", file=sys.stderr)


# ANSI helpers
_RESET = "\033[0m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_CYAN = "\033[36m"
_BLUE = "\033[34m"

_STATUS_COLOR = {
    "completed": _GREEN,
    "running": _YELLOW,
    "failed": _RED,
    "cancelled": _RED,
    "pending": _DIM,
}

_TYPE_COLOR = {
    "pipeline": _CYAN,
    "group": _BLUE,
}


def _fmt_status(status):
    color = _STATUS_COLOR.get(status, "")
    return f"{color}{status.upper():<10}{_RESET}"


def _fmt_type(jtype):
    color = _TYPE_COLOR.get(jtype, "")
    return f"{color}{_BOLD}{jtype.upper():<12}{_RESET}"


def _fmt_date(ts_ms):
    try:
        val = float(ts_ms)
    except (ValueError, TypeError):
        return "-"
    return time.strftime("%Y-%m-%d %H:%M", time.localtime(val / 1000)) if val else "-"


def _fmt_duration(created_ms, updated_ms, status, started_ms=0):
    try:
        start = float(started_ms) if started_ms else float(created_ms)
    except (ValueError, TypeError):
        return ""
    if not start:
        return ""
    try:
        end = (
            float(updated_ms)
            if updated_ms and status in ("completed", "failed", "cancelled")
            else time.time() * 1000
        )
    except (ValueError, TypeError):
        end = time.time() * 1000

    secs = int(max(0.0, (end - start)) // 1000)
    if secs < 60:
        return f"{secs}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}m{m:02d}s"


def _trunc(s, n):
    if not s:
        return ""
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


def _fetch_and_render(limit, tree, max_depth, parent, collection, pool):
    import io, contextlib

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        _render_jobs(limit, tree, max_depth, parent, collection, pool)
    return buf.getvalue()


def _render_jobs(limit, tree, max_depth, parent=None, collection=None, pool=None):
    # --parent: fetch sub-tasks of the given parent directly
    if parent:
        r = requests.get(f"{API_BASE}/jobs/{parent}")
        r.raise_for_status()
        detail = r.json()
        jobs = detail.get("sub_tasks", [])
        total = len(jobs)
    else:
        params = {"limit": limit}
        if collection:
            params["collection"] = collection
        if pool:
            params["pool"] = pool
        resp = requests.get(f"{API_BASE}/jobs", params=params)
        resp.raise_for_status()
        data = resp.json()
        jobs = data.get("items", [])
        total = data.get("total")

    if not tree:
        H = f"{_BOLD}{'ID':<28} {'TYPE':<16} {'STATUS':<11} {'PROG':>5}  {'DUR':>7}  {'COLLECTION':<18} {'TARGET':<18} CREATED{_RESET}"
        print(H)
        print("-" * 120)
        for j in jobs:
            col = _trunc(j.get("collection") or "-", 18)
            tgt = _trunc(j.get("target") or "-", 18)
            dur = _fmt_duration(
                j.get("created_at", 0),
                j.get("updated_at", 0),
                j.get("status", ""),
                j.get("started_at", 0),
            )
            crtd = _fmt_date(j.get("created_at", 0))
            print(
                f"{j['id']:<28} {_fmt_type(j.get('type','job'))} {_fmt_status(j.get('status','pending'))} {j.get('progress',0):>4}%  {dur:>7}  {col:<18} {tgt:<18} {crtd}"
            )
        if total is not None:
            print(f"\n{_DIM}Total: {total}{_RESET}")
        return

    # Tree mode: build full hierarchy map by resolving missing parent chains.
    # If a filter (like collection) is active, the top-level pipeline might not be in the initial jobs list.
    # We resolve parents upwards to find the true roots, ensuring we render the full context tree.
    all_jobs_map = {j["id"]: j for j in jobs}
    resolved_parents = {}

    def _resolve_parent_chain(job_item):
        pid = job_item.get("parent_id")
        if not pid:
            return job_item["id"]  # This is a true root

        # Check if already in our pool
        if pid in all_jobs_map:
            return _resolve_parent_chain(all_jobs_map[pid])
        if pid in resolved_parents:
            return _resolve_parent_chain(resolved_parents[pid])

        # Fetch parent from API
        try:
            r = requests.get(f"{API_BASE}/jobs/{pid}")
            if r.ok:
                parent_job = r.json()
                # Parse basic keys
                parent_job["id"] = pid
                resolved_parents[pid] = parent_job
                return _resolve_parent_chain(parent_job)
        except Exception:
            pass

        return job_item[
            "id"
        ]  # Fallback: treat this item as root if parent cannot be resolved

    # Trace root for each job
    roots = set()
    for j in jobs:
        root_id = _resolve_parent_chain(j)
        roots.add(root_id)

    # Combine initial jobs and resolved parents
    merged_jobs = {**all_jobs_map, **resolved_parents}

    # Now recursively populate children map for all pipelines/groups from the resolved roots down
    children_map = {}

    def _fetch_children(job_id, depth):
        if job_id in children_map:
            return
        if max_depth and depth >= max_depth:
            return
        try:
            # If we already have the job status locally (with sub_tasks), use it. Otherwise API fetch.
            job_obj = merged_jobs.get(job_id)
            if job_obj and "sub_tasks" in job_obj:
                kids = job_obj["sub_tasks"]
            else:
                r = requests.get(f"{API_BASE}/jobs/{job_id}")
                kids = r.json().get("sub_tasks", []) if r.ok else []

            children_map[job_id] = kids
            for kid in kids:
                # Merge kids back into merged_jobs if missing (so we can get their metadata)
                if kid["id"] not in merged_jobs:
                    merged_jobs[kid["id"]] = kid
                if kid.get("type") in ("pipeline", "group"):
                    _fetch_children(kid["id"], depth + 1)
        except Exception:
            pass

    for r_id in roots:
        _fetch_children(r_id, 0)

    top_level = [merged_jobs[rid] for rid in roots if rid in merged_jobs]
    top_level.sort(key=lambda j: j.get("created_at", 0), reverse=True)

    print(
        f"{_BOLD}{'TYPE':<12} {'ID':<30} {'STATUS':<10} {'PROG':>5}  {'DUR':>7}  {'COLLECTION':<16} {'TARGET':<16} CREATED{_RESET}"
    )
    print("─" * 115)

    def render_node(j, prefix="", is_last=True, depth=0):
        connector = "└─ " if is_last else "├─ "
        line_prefix = prefix + ("   " if is_last else "│  ")
        jtype = j.get("type", "job")
        status = j.get("status", "pending")
        progress = j.get("progress", 0)
        jid = j.get("id", "-")
        dur = _fmt_duration(
            j.get("created_at", 0),
            j.get("updated_at", 0),
            status,
            j.get("started_at", 0),
        )
        col = _trunc(j.get("collection") or "-", 16)
        tgt = _trunc(j.get("target") or "-", 16)
        crtd = _fmt_date(j.get("created_at", 0))

        if depth == 0:
            row = (
                f"{_fmt_type(jtype)} {jid:<30} {_fmt_status(status)} {progress:>4}%"
                f"  {dur:>7}  {col:<16} {tgt:<16} {crtd}"
            )
        else:
            row = (
                f"{prefix}{connector}{_fmt_type(jtype)} {jid:<30}"
                f" {_fmt_status(status)} {progress:>4}%  {dur:>7}  {col:<16} {tgt:<16}"
            )
        print(row)

        kids = children_map.get(jid, [])
        if max_depth and depth >= max_depth and kids:
            print(
                f"{line_prefix}└─ {_DIM}[{len(kids)} sub-tasks — use -d {max_depth + 1} to expand]{_RESET}"
            )
            return
        for i, kid in enumerate(kids):
            render_node(kid, line_prefix, is_last=(i == len(kids) - 1), depth=depth + 1)

    for i, j in enumerate(top_level):
        render_node(j, "", is_last=(i == len(top_level) - 1), depth=0)
        if j.get("type") in ("pipeline", "group"):
            print()

    if total is not None:
        print(f"{_DIM}Total: {total}{_RESET}")


def list_jobs(
    limit,
    tree=False,
    max_depth=2,
    follow=False,
    parent=None,
    collection=None,
    pool=None,
):
    try:
        if not follow:
            _render_jobs(limit, tree, max_depth, parent, collection, pool)
            return
        print(f"{_DIM}Following job list (Ctrl+C to stop)...{_RESET}")
        while True:
            output = _fetch_and_render(limit, tree, max_depth, parent, collection, pool)
            print("\033[H\033[J", end="")  # clear screen
            ts = time.strftime("%H:%M:%S")
            print(f"{_DIM}Last updated: {ts} — Ctrl+C to stop{_RESET}\n")
            print(output, end="")
            time.sleep(2)
    except KeyboardInterrupt:
        print(f"\n{_DIM}Stopped.{_RESET}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


def job_status(job_id, watch, logs):
    try:
        while True:
            if not job_id:
                # GLOBAL STATS MODE
                resp = requests.get(f"{API_BASE}/jobs/stats")
                resp.raise_for_status()
                stats = resp.json()

                print("\033[H\033[J", end="")  # Clear screen
                print("=== GLOBAL JOB STATUS ===")
                print(f"Active Workers: {stats['active_workers']}")
                print(f"Pending Jobs:   {stats['pending_jobs']}")
                print(f"Total Speed:    {stats['total_speed']} fn/s")
                print(f"Avg Speed:      {stats['avg_speed']} fn/s")
                print(f"Items Left:     {stats['remaining_items']}")
                print(f"Est. Global Time: {stats['global_eta']}s")
                print("-" * 30)

                if not watch:
                    return
                time.sleep(2)
                continue

            # INDIVIDUAL JOB MODE
            resp = requests.get(f"{API_BASE}/jobs/{job_id}")
            resp.raise_for_status()
            job = resp.json()

            if not watch:
                print(json.dumps(job, indent=2))
                return

            # Watch mode
            print("\033[H\033[J", end="")  # Clear screen
            print(f"Job: {job['id']} ({job['type']})")
            print(f"Status: {job['status']}")
            print(f"Progress: {job['progress']}%")

            if "speed" in job:
                print(f"Speed: {job['speed']} fn/s")
            if "eta" in job:
                print(f"ETA: {job['eta']}s")

            print("-" * 40)

            if "sub_tasks" in job:
                print("Sub-tasks:")
                for st in job["sub_tasks"]:
                    print(
                        f"  - {st['type']:<15}: {st['status']:<10} ({st['progress']}%)"
                    )
                print("-" * 40)

            if logs and job.get("logs"):
                print("Recent Logs:")
                for log in reversed(job["logs"][:10]):
                    match = re.match(r"^\[(\d+)\] (.*)", log)
                    if match:
                        ts = int(match.group(1))
                        msg = match.group(2)
                        date_str = datetime.datetime.fromtimestamp(
                            ts / 1000.0
                        ).strftime("%Y-%m-%d %H:%M:%S")
                        print(f"  [{date_str}] {msg}")
                    else:
                        print(f"  {log}")

            if job["status"] in ["completed", "failed", "cancelled"]:
                print("\nJob finished.")
                break

            time.sleep(2)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


def cancel_job(job_id):
    try:
        if job_id == "all":
            resp = requests.post(f"{API_BASE}/jobs/all/cancel")
            resp.raise_for_status()
            data = resp.json()
            print(f"Cancelled {data.get('cancelled_count', 0)} job(s).")
        else:
            resp = requests.post(f"{API_BASE}/jobs/{job_id}/cancel")
            resp.raise_for_status()
            print(f"Job {job_id} cancellation requested.")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)


def retry_job(job_id):
    try:
        resp = requests.post(f"{API_BASE}/jobs/{job_id}/retry")
        resp.raise_for_status()
        data = resp.json()
        print(f"Job {job_id} retry requested. Status: {data.get('status')}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
