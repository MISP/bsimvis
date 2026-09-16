"""Small cluster-selection helpers shared across bin_sim / similarity / cluster services.

Dedupes the "pick the highest-cohesion cluster" logic that was previously copied into
bin_sim_service, similarity_service and cluster_service.
"""

from collections import Counter

MAX_CLUSTER_NAME_LEN = 40


def default_bin_cluster_name(names_list, avtype_list, yara_list, fallback):
    """Short, human-meaningful default name for a binary cluster.

    A raw member filename (the old default) is often a long malware-scanner
    submission name, which reads as noise in a cluster list. AV family labels
    ('Emotet', 'Gafgyt') and YARA rule names are short and already describe
    what the cluster *is*, so prefer them; fall back to a truncated filename,
    then to the caller's generic placeholder.
    """
    if avtype_list:
        return Counter(avtype_list).most_common(1)[0][0]
    if yara_list:
        return Counter(yara_list).most_common(1)[0][0]
    if names_list:
        name = Counter(names_list).most_common(1)[0][0]
        if len(name) > MAX_CLUSTER_NAME_LEN:
            name = name[: MAX_CLUSTER_NAME_LEN - 3] + "..."
        return name
    return fallback


def collect_member_values(metas):
    """Flatten member file metadata into the value lists a cluster summary counts.

    `metas` is an iterable of per-file meta dicts. Ingest paths store these
    fields either as a scalar or as a list, so each one is coerced here.
    Returns (names, md5s, yara, avtype, filetype, cc_ip).
    """
    names_list = []
    md5s_list = []
    yara_list = []
    avtype_list = []
    filetype_list = []
    ccip_list = []

    for m in metas:
        if m.get("file_names"):
            names_list.extend(m["file_names"])
        elif m.get("file_name"):
            names_list.append(m["file_name"])

        if m.get("file_md5"):
            md5s_list.append(m["file_md5"])

        if m.get("yara"):
            yara_list.extend(m["yara"] if isinstance(m["yara"], list) else [m["yara"]])
        if m.get("avtype"):
            avtype_list.extend(
                m["avtype"] if isinstance(m["avtype"], list) else [m["avtype"]]
            )
        if m.get("filetype"):
            filetype_list.extend(
                m["filetype"] if isinstance(m["filetype"], list) else [m["filetype"]]
            )
        if m.get("cc_ip"):
            ccip_list.extend(
                m["cc_ip"] if isinstance(m["cc_ip"], list) else [m["cc_ip"]]
            )

    return names_list, md5s_list, yara_list, avtype_list, filetype_list, ccip_list


def build_freq(items, member_count):
    """Top-5 value frequencies for one cluster distribution.

    `percent` is a share of `member_count` -- the cluster's members -- never of
    `items`, which is flattened: a file carrying two yara hits contributes two
    entries. Every writer of a cluster `:meta` key has to use the same
    denominator, or the same cluster reports different percents depending on
    which path wrote it last.
    """
    return (
        [
            {
                "value": k,
                "count": v,
                "percent": round((v / member_count) * 100),
            }
            for k, v in Counter(items).most_common(5)
        ]
        if items
        else []
    )


def pick_best_shared_cluster(cids_a, cids_b, cluster_meta):
    """Highest-cohesion cluster shared by two functions, or None.

    cids_a/cids_b: iterables of cluster ids. cluster_meta: {cid -> meta dict with
    'cohesion_score'}. Returns the winning meta dict (not the id) or None when the two
    share no cluster present in cluster_meta.
    """
    best, best_coh = None, -1.0
    for cid in set(cids_a) & set(cids_b):
        m = cluster_meta.get(cid)
        if m is None:
            continue
        coh = float(m.get("cohesion_score", 0.0))
        if coh > best_coh:
            best, best_coh = m, coh
    return best


def pick_best_cluster(cids, cluster_meta):
    """Best (highest-cohesion) cluster for a single function, or None."""
    return pick_best_shared_cluster(cids, cids, cluster_meta)
