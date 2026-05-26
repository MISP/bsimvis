import csv
import io
import json
from datetime import datetime
from flask import Response


def format_date(val):
    if not val:
        return ""
    try:
        ts = float(val)
        if ts > 0:
            # Handle timestamps (which could be in seconds or milliseconds)
            # Typically timestamps are in seconds. If they look like milliseconds, scale down.
            if ts > 9999999999:
                ts = ts / 1000.0
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        pass
    return str(val)


def format_parameters(params):
    if not params:
        return ""
    if not isinstance(params, list):
        return str(params)
    formatted = []
    for p in params:
        if isinstance(p, dict):
            p_type = p.get("type") or p.get("data_type") or ""
            p_name = p.get("name") or ""
            if p_type and p_name:
                formatted.append(f"{p_type} {p_name}")
            elif p_type:
                formatted.append(p_type)
            elif p_name:
                formatted.append(p_name)
            else:
                formatted.append(str(p))
        else:
            formatted.append(str(p))
    return ", ".join(formatted)


def export_to_csv(items, search_type):
    """
    Converts a list of search result dictionaries to a CSV flask response.
    """
    output = io.StringIO()
    writer = csv.writer(output)

    if search_type == "collections":
        headers = ["Name", "Files", "Functions", "Last Updated"]
        writer.writerow(headers)
        for item in items:
            writer.writerow(
                [
                    item.get("name", ""),
                    item.get("total_files", 0),
                    item.get("total_functions", 0),
                    format_date(item.get("last_updated", "")),
                ]
            )

    elif search_type == "batches":
        headers = ["Batch Name", "UUID", "Files", "Functions", "Timestamp"]
        writer.writerow(headers)
        for item in items:
            writer.writerow(
                [
                    item.get("batch_name", ""),
                    item.get("batch_uuid", ""),
                    item.get("total_files", 0),
                    item.get("total_functions", 0),
                    format_date(item.get("last_updated", item.get("created_at", ""))),
                ]
            )

    elif search_type == "files":
        headers = [
            "Filename",
            "MD5",
            "Language",
            "Batch UUID",
            "Functions",
            "Entry Date",
            "Tags",
            "User Tags",
        ]
        writer.writerow(headers)
        for item in items:
            writer.writerow(
                [
                    item.get("file_name", ""),
                    item.get("file_md5", ""),
                    item.get("language_id", ""),
                    item.get("batch_uuid", ""),
                    item.get("function_count", 0),
                    format_date(item.get("entry_date", "")),
                    ", ".join(item.get("tags", []) or []),
                    ", ".join(item.get("user_tags", []) or []),
                ]
            )

    elif search_type == "functions":
        headers = [
            "Function",
            "Address",
            "Return Type",
            "Namespace",
            "Parameters",
            "Function Tags",
            "Function User Tags",
            "Cluster Names",
            "Cluster UUIDs",
            "Features",
            "File Name",
            "MD5",
            "File Tags",
            "File User Tags",
            "Language",
            "Date",
        ]
        writer.writerow(headers)
        for item in items:
            cluster_names = ", ".join(
                [
                    c.get("cluster_name", "")
                    for c in item.get("clusters", []) or []
                    if c.get("cluster_name")
                ]
            )
            cluster_uuids = ", ".join(
                [
                    c.get("cluster_uuid", "")
                    for c in item.get("clusters", []) or []
                    if c.get("cluster_uuid")
                ]
            )
            addr = item.get("entrypoint_address", "")
            if isinstance(addr, int):
                addr = hex(addr)
            writer.writerow(
                [
                    item.get("function_name", ""),
                    addr,
                    item.get("return_type", ""),
                    item.get("namespace", ""),
                    format_parameters(item.get("parameters", [])),
                    ", ".join(item.get("tags", []) or []),
                    ", ".join(item.get("user_tags", []) or []),
                    cluster_names,
                    cluster_uuids,
                    item.get("bsim_features_count", 0),
                    item.get("file_name", ""),
                    item.get("file_md5", ""),
                    ", ".join(item.get("file_tags", []) or []),
                    ", ".join(item.get("file_user_tags", []) or []),
                    item.get("language_id", ""),
                    format_date(item.get("entry_date", "")),
                ]
            )

    elif search_type == "features":
        headers = [
            "Feature Hash",
            "Type",
            "Op",
            "PCode Context",
            "C Code Context",
            "Frequency",
            "TF Score",
        ]
        writer.writerow(headers)
        for item in items:
            ctx = item.get("context", {}) or {}
            c_code_list = ctx.get("c_code") or []
            c_code_str = (
                "".join([t.get("text", "") for t in c_code_list])
                if isinstance(c_code_list, list)
                else ""
            )
            writer.writerow(
                [
                    item.get("hash", ""),
                    ctx.get("type", ""),
                    ctx.get("op", ""),
                    ctx.get("pcode_full", ""),
                    c_code_str,
                    item.get("frequency", 0),
                    item.get("tf_score", 0),
                ]
            )

    elif search_type == "similarity":
        headers = [
            "Similarity",
            "Function 1",
            "Function 2",
            "Addr 1",
            "Addr 2",
            "Return Type 1",
            "Return Type 2",
            "Namespace 1",
            "Namespace 2",
            "Parameters 1",
            "Parameters 2",
            "Func Tags 1",
            "Func Tags 2",
            "Func User Tags 1",
            "Func User Tags 2",
            "Cluster Names 1",
            "Cluster UUIDs 1",
            "Cluster Names 2",
            "Cluster UUIDs 2",
            "Feat",
            "File Name 1",
            "File Name 2",
            "MD5 1",
            "MD5 2",
            "File Tags 1",
            "File Tags 2",
            "File User Tags 1",
            "File User Tags 2",
            "Language 1",
            "Language 2",
            "Date",
            "Similarity Tags",
            "Similarity User Tags",
        ]
        writer.writerow(headers)
        for item in items:
            meta1 = item.get("meta1", {}) or {}
            meta2 = item.get("meta2", {}) or {}

            addr1 = meta1.get("entrypoint_address", "")
            if isinstance(addr1, int):
                addr1 = hex(addr1)
            addr2 = meta2.get("entrypoint_address", "")
            if isinstance(addr2, int):
                addr2 = hex(addr2)

            cluster_names1 = ", ".join(
                [
                    c.get("cluster_name", "")
                    for c in meta1.get("clusters", []) or []
                    if c.get("cluster_name")
                ]
            )
            cluster_uuids1 = ", ".join(
                [
                    c.get("cluster_uuid", "")
                    for c in meta1.get("clusters", []) or []
                    if c.get("cluster_uuid")
                ]
            )
            cluster_names2 = ", ".join(
                [
                    c.get("cluster_name", "")
                    for c in meta2.get("clusters", []) or []
                    if c.get("cluster_name")
                ]
            )
            cluster_uuids2 = ", ".join(
                [
                    c.get("cluster_uuid", "")
                    for c in meta2.get("clusters", []) or []
                    if c.get("cluster_uuid")
                ]
            )

            writer.writerow(
                [
                    item.get("score", 0.0),
                    item.get("name1", ""),
                    item.get("name2", ""),
                    addr1,
                    addr2,
                    meta1.get("return_type", ""),
                    meta2.get("return_type", ""),
                    meta1.get("namespace", ""),
                    meta2.get("namespace", ""),
                    format_parameters(meta1.get("parameters", [])),
                    format_parameters(meta2.get("parameters", [])),
                    ", ".join(meta1.get("tags", []) or []),
                    ", ".join(meta2.get("tags", []) or []),
                    ", ".join(meta1.get("user_tags", []) or []),
                    ", ".join(meta2.get("user_tags", []) or []),
                    cluster_names1,
                    cluster_uuids1,
                    cluster_names2,
                    cluster_uuids2,
                    item.get("feat_count", 0),
                    meta1.get("file_name", ""),
                    meta2.get("file_name", ""),
                    meta1.get("file_md5", ""),
                    meta2.get("file_md5", ""),
                    ", ".join(meta1.get("file_tags", []) or []),
                    ", ".join(meta2.get("file_tags", []) or []),
                    ", ".join(meta1.get("file_user_tags", []) or []),
                    ", ".join(meta2.get("file_user_tags", []) or []),
                    meta1.get("language_id", ""),
                    meta2.get("language_id", ""),
                    format_date(item.get("entry_date", "")),
                    ", ".join(item.get("tags", []) or []),
                    ", ".join(item.get("user_tags", []) or []),
                ]
            )

    elif search_type == "clusters":
        headers = [
            "Cluster ID",
            "UUID",
            "Name",
            "Functions",
            "Stability",
            "Avg Feat",
            "Cohesion",
            "Created",
        ]
        writer.writerow(headers)
        for item in items:
            writer.writerow(
                [
                    item.get("cluster_id", ""),
                    item.get("cluster_uuid", ""),
                    item.get("cluster_name", ""),
                    item.get("count", 0),
                    item.get("avg_stability", 0.0),
                    item.get("avg_features", 0),
                    item.get("cohesion_score", 0),
                    format_date(item.get("created_at", "")),
                ]
            )

    csv_content = output.getvalue()
    output.close()

    filename = f"export_{search_type}.csv"
    return Response(
        csv_content,
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-type": "text/csv",
        },
    )


def export_to_json(data_dict, search_type):
    json_content = json.dumps(data_dict, indent=2)
    filename = f"export_{search_type}.json"
    return Response(
        json_content,
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
