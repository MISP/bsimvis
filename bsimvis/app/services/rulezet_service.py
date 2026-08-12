"""Mirror rulezet.org's YARA rules into a local, gated ruleset.

The vendored ruleset in `data/yara_rules/` is 589 hand-picked rules with a
measured coverage number behind it. This is the opposite: ~130k rules nobody
reviewed, most of them bulk GitHub imports. Both feed the same `yara:` tags at
scan time; only the trust differs, so only this one gets a false-positive gate.

Three facts about rulezet.org's API shape the whole module:

  * **No endpoint ever returns a rule's tags.** `Rule.to_json()` has no tags
    field and every read path uses it -- public search, detail, CVE search,
    private search, `dumpRules`, the bundle zip. Tags exist server-side but are
    only readable through login-gated UI routes, one rule per call. So the
    MISP-style tags this module attaches are *produced locally* (see
    `platform_tags`), never received.
  * `per_page` caps at 100, so a keyless full mirror is ~1300 requests. With an
    API key `dumpRules` returns everything in one POST and takes an
    `updated_after` for incremental syncs. Both are reads -- nothing is ever
    written back to rulezet.org.
  * A rule's `cve_id` *is* in every response, so `cve:`/`ghsa:` tags are free.

Tags live in a `tags.json` sidecar keyed by rule uuid rather than being injected
into the rule text: `yara.compile(filepaths={uuid: path})` makes `match.namespace`
the uuid, which makes the join exact even though rule *names* collide freely
across 130k rules from different repos. It also means nothing here has to parse
or rewrite YARA source.
"""

import json
import os
import re
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.tag_taxonomy import route_source_tag
# Safe at import time: tag_provenance's own imports are stdlib, and it reaches
# back into this module lazily inside functions.
from bsimvis.app.services.tag_provenance import rulezet_row

DEFAULT_URL = "https://rulezet.org"

# Rulezet auto-tags its bulk imports by running regexes over each rule's title
# and description (`app/core/utils/default_platform_tag_configs.json`). Running
# the same table locally reproduces most of the tag set they hold with zero
# requests and no API key. It is *downloaded* into the gitignored mirror rather
# than vendored into this repo: rulezet-core is AGPL-3.0 and BSimVis is
# Apache-2.0, so their file is treated exactly like their rules -- fetched at
# sync time, never committed.
TAG_CONFIG_URL = (
    "https://raw.githubusercontent.com/rulezet/rulezet-core/main/"
    "app/core/utils/default_platform_tag_configs.json"
)

# misp-galaxy is the same public source rulezet imports its tag vocabulary from.
# Used only by `index_tags()`, to enumerate the tag names worth querying.
GALAXY_URL = "https://raw.githubusercontent.com/MISP/misp-galaxy/main/clusters/{}.json"

# A vulnerability id in the `cve_id` column. Rulezet stores CVE, GHSA and PYSEC
# ids there, as a JSON list or a bare string depending on the import path.
VULN_RE = re.compile(r"\b(CVE|GHSA|PYSEC)[-–][\w.-]+", re.I)


def cfg(key, default=None):
    return config_service.get(f"rulezet.{key}", default)


def mirror_dir():
    """Root of the mirror. Under `data/`, which is gitignored wholesale."""
    explicit = os.environ.get("RULEZET_DIR") or cfg("mirror_dir")
    if explicit:
        return Path(explicit)
    return Path(__file__).resolve().parents[3] / "data" / "rulezet"


def paths():
    d = mirror_dir()
    return {
        "root": d,
        "rules": d / "rules",
        "quarantine": d / "quarantine",
        "compiled": d / "rules.compiled",
        "tags": d / "tags.json",
        "quarantine_log": d / "quarantine.txt",
        "released": d / "released.txt",
        "state": d / "state.json",
        "tag_config": d / "platform_tag_configs.json",
        "readme": d / "SYNCED_FROM.md",
    }


# --- HTTP -------------------------------------------------------------------
# urllib rather than requests: three call sites, no session state, no retries
# worth the dependency. Every one of these is a read.


def _get(url, timeout=120):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def _post(url, body, api_key, timeout=600):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json", "X-API-KEY": api_key},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def fetch_rules(since=None, limit=None, log=print):
    """Every YARA rule on the instance, as the API's own dicts.

    With an API key this is one `dumpRules` POST (and `since` makes it
    incremental). Without one it pages `searchPage` at the API's 100/page cap,
    which costs ~1300 requests for a full mirror -- measured at ~25s with 8
    threads, but kept serial here because a sync is not the thing to be clever
    about.
    """
    base = (cfg("url") or DEFAULT_URL).rstrip("/")
    api_key = cfg("api_key") or os.environ.get("RULEZET_API_KEY")

    # `dumpRules` has no size parameter -- it is all-or-nothing, ~130k rules and
    # 128 MB before the first byte is usable. So a trial run pages the public
    # endpoint instead, which really does stop early. Otherwise `--limit 2000`
    # would still cost the full ~2 minute download to then throw 128k rules away.
    if api_key and limit:
        log(
            f"--limit {limit}: paging the public endpoint "
            f"(dumpRules cannot fetch a subset)"
        )
        api_key = None

    if api_key:
        body = {"format_name": "yara"}
        if since:
            body["updated_after"] = since
        log(
            f"dumpRules (incremental since {since})"
            if since
            else "dumpRules (full): ~130k rules, 128 MB, ~2 min before "
            "anything is written"
        )
        try:
            doc = _post(f"{base}/api/rule/private/dumpRules", body, api_key)
        except urllib.error.HTTPError as e:
            # An incremental sync with nothing new answers 404 "No rules found
            # to dump." That is the ordinary quiet case, not a failure -- every
            # re-run between updates would otherwise raise.
            if e.code == 404:
                return []
            raise
        rules = (doc.get("data", {}).get("rules_by_format", {}) or {}).get("yara", [])
        return rules[:limit] if limit else rules

    out, page = [], 1
    while True:
        url = (
            f"{base}/api/rule/public/searchPage"
            f"?rule_type=yara&per_page=100&page={page}"
        )
        doc = _get(url)
        out.extend(doc.get("results") or [])
        if page == 1:
            log(
                f"{doc.get('total_rules_found', 0)} yara rules available, "
                f"paging at 100/request ({doc.get('total_pages', 0)} pages)"
            )
        if limit and len(out) >= limit:
            return out[:limit]
        if not (doc.get("pagination") or {}).get("next_page"):
            return out
        page += 1
        if page % 50 == 0:
            log(f"  page {page}, {len(out)} rules")


# --- Local tagging ----------------------------------------------------------


def load_tag_config(refresh=False, log=print):
    """Rulezet's own regex->MISP-tag table, cached in the mirror directory."""
    p = paths()["tag_config"]
    if refresh or not p.exists():
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(_get(TAG_CONFIG_URL)))
        except (urllib.error.URLError, OSError, ValueError) as e:
            log(f"  tag config unavailable ({e}); rules get cve tags only")
            return []
    try:
        groups = json.loads(p.read_text())
    except ValueError:
        return []
    out = []
    for entries in groups.values():
        for e in entries:
            if not e.get("enabled", True):
                continue
            try:
                out.append((re.compile(e["regex"], re.I), e["tag_name"]))
            except re.error:
                continue
    return out


def _vulns(rule):
    """Vulnerability ids off the `cve_id` column, whatever shape it arrived in."""
    raw = rule.get("cve_id")
    if not raw:
        return []
    if isinstance(raw, list):
        text = " ".join(str(x) for x in raw)
    else:
        text = str(raw)
    return sorted(
        {m.group(0).upper().replace("–", "-") for m in VULN_RE.finditer(text)}
    )


def platform_tags(rule, tag_config):
    """The raw, MISP-style source tags for one rule.

    Two producers, neither of which needs an API key: the vulnerability ids that
    ship in the rule row, and Rulezet's own title/description regex table. The
    optional third -- their curated per-rule tags -- is what `index_tags()`
    reaches for, because no bulk endpoint exposes it.
    """
    tags = [f"{v.split('-')[0].lower()}:{v}" for v in _vulns(rule)]
    # Underscores become spaces first. Rulezet's regexes are `\b`-anchored and
    # written for prose descriptions, but a mirrored rule's *title* is almost
    # always underscore-joined (`Win32_Ransomware_LockBit`) -- and `_` is a word
    # character, so `\bransom(ware)?\b` matches none of them. Without this the
    # table scores against descriptions only and misses most of the bulk
    # imports, which are exactly the rules with nothing but a title to go on.
    hay = f"{rule.get('title') or ''} {rule.get('description') or ''}".replace("_", " ")
    for pattern, tag_name in tag_config:
        if pattern.search(hay):
            tags.append(tag_name)
    return tags


def route_tags(raw_tags):
    """Source tags -> BSimVis tag ids, dropping whatever the config drops."""
    tag_map = cfg("tags") or {}
    drops = cfg("drop") or []
    out = set()
    for raw in raw_tags:
        routed = route_source_tag(raw, tag_map, drops)
        if routed:
            out.add(routed)
    return sorted(out)


# --- Mirror on disk ---------------------------------------------------------


def _write_rules(rules, tag_config, log=print, write_files=True):
    """Rule text to `rules/<uuid>.yara`, tags to the sidecar.

    Returns `(sidecar tags, provenance rows)`. The rows are built here rather
    than in a second pass because they need the same routed tags the sidecar
    does, and routing 214k rules through the regex table twice is the only
    other way to get them.

    `write_files=False` is the `--meta-only` backfill: the rule files are
    already on disk from an earlier sync, only the metadata is missing.
    """
    p = paths()
    p["rules"].mkdir(parents=True, exist_ok=True)
    quarantined = {f.stem for f in p["quarantine"].glob("*.yara")}
    tags, rows, written, skipped = {}, {}, 0, 0

    allow = [x.lower() for x in (cfg("allow_licenses") or [])]
    for rule in rules:
        uuid = rule.get("uuid") or str(rule.get("id") or "")
        text = rule.get("to_string") or rule.get("content") or ""
        if not uuid or not text.strip():
            skipped += 1
            continue
        if allow and str(rule.get("license") or "").strip().lower() not in allow:
            skipped += 1
            continue
        if write_files and uuid not in quarantined:
            (p["rules"] / f"{uuid}.yara").write_text(text)
            written += 1
        routed = route_tags(platform_tags(rule, tag_config))
        if routed:
            tags[uuid] = routed

        # Everything a user needs to get back to this rule on rulezet.org,
        # taken while the API response is still in hand: after a sync all that
        # is left on disk is `<uuid>.yara`, and no read endpoint takes a uuid.
        rows[uuid] = rulezet_row(rule, tags=routed)

    log(
        f"  {written} rule files written, {skipped} skipped "
        f"(license/empty), {len(tags)} carry tags"
    )
    return tags, rows


def _store_rows(rows, log=print, chunk=5000):
    """Provenance rows into the database, one row per rule.

    Chunked because a full mirror is ~214k rules and a single pipeline of that
    many commands is a spike in both the client's and kvrocks' memory for no
    gain -- the write is not atomic in any useful sense anyway.

    Note what is *not* indexed here: a mirrored rule's own `yara:` tag depends
    on `meta.category`/`meta.malware` inside the rule text, which this module
    deliberately never parses. That tag joins the index the first time the rule
    actually fires (`tag_provenance.match_rows`).
    """
    from bsimvis.app.services.tag_provenance import put_rules_bulk

    ids = list(rows)
    for i in range(0, len(ids), chunk):
        put_rules_bulk({k: rows[k] for k in ids[i:i + chunk]})
    log(f"  {len(rows)} provenance rows stored")
    return len(rows)


def _merge_tags(new_tags):
    """Fold newly routed tags into the sidecar, preserving indexed galaxy tags.

    `index_tags()` writes tags no bulk sync can rediscover, so a later sync must
    not wipe them -- it can only add to what is already there.
    """
    p = paths()["tags"]
    old = {}
    if p.exists():
        try:
            old = json.loads(p.read_text())
        except ValueError:
            old = {}
    for uuid, tags in new_tags.items():
        old[uuid] = sorted(set(old.get(uuid, [])) | set(tags))
    p.write_text(json.dumps(old))
    return old


def compile_mirror(log=print, validate=True):
    """Validate every rule file, compile the survivors, save the ruleset.

    Per-file validation first: one bad rule fails the whole bulk compile, and at
    this scale there is always a bad rule. Measured ~7ms a file, so ~15 min at
    130k -- the price of not having a single syntax error cost you the sync.

    `validate=False` skips it, for the rebuild after the gate: those same files
    were just validated, and quarantining moves files out rather than changing
    any, so a second pass can only reach the same verdict at full price.
    """
    import yara

    p = paths()
    files, bad = {}, []
    t0 = time.time()
    for f in sorted(p["rules"].glob("*.yara")):
        if validate:
            try:
                yara.compile(filepath=str(f))
            except yara.Error:
                bad.append(f)
                continue
        # The uuid *is* the namespace, which is what makes the tag sidecar join
        # exact -- rule names collide across 130k rules from different repos.
        files[f.stem] = str(f)
    for f in bad:
        f.unlink()
    if validate:
        log(
            f"  validated {len(files)} rules, dropped {len(bad)} unparseable "
            f"({time.time() - t0:.0f}s)"
        )

    if not files:
        return None
    t0 = time.time()
    rules = yara.compile(filepaths=files)
    rules.save(str(p["compiled"]))
    log(
        f"  compiled + saved in {time.time() - t0:.0f}s "
        f"({p['compiled'].stat().st_size / 1e6:.0f} MB)"
    )
    return rules


# --- The false-positive gate ------------------------------------------------


def baseline_files():
    """Known-clean binaries every mirrored rule has to stay silent on."""
    dirs = cfg("baseline_dirs") or ["/usr/bin"]
    cap = int(cfg("baseline_max_files") or 300)
    out = []
    for d in dirs:
        for f in sorted(Path(d).glob("*")):
            if len(out) >= cap:
                return out
            if f.is_file() and not f.is_symlink():
                out.append(f)
    return out


def gate(rules, log=print):
    """Quarantine every mirrored rule that fires on known-clean binaries.

    Measured on a random 3k slice of the mirror: 103 of 150 clean `/bin`
    binaries matched a Windows malware rule, essentially all of it from two junk
    rules. At 130k rules that is not a review queue anyone works through, so it
    is automatic and re-runnable -- `released.txt` is the override, and it is
    honoured on every later sync so a decision is never re-litigated.
    """
    p = paths()
    if rules is None:
        return {}
    released = set()
    if p["released"].exists():
        released = {
            l.strip()
            for l in p["released"].read_text().splitlines()
            if l.strip() and not l.startswith("#")
        }

    hits = {}
    files = baseline_files()
    for f in files:
        try:
            matches = rules.match(str(f), timeout=300)
        except Exception:
            continue
        for m in matches:
            # namespace is the uuid for mirrored rules; vendored rules are not
            # in this ruleset at all.
            hits.setdefault(m.namespace, {"rule": m.rule, "n": 0, "where": []})
            hits[m.namespace]["n"] += 1
            if len(hits[m.namespace]["where"]) < 3:
                hits[m.namespace]["where"].append(f.name)

    for uuid in released:
        hits.pop(uuid, None)

    p["quarantine"].mkdir(parents=True, exist_ok=True)
    lines = []
    for uuid, info in sorted(hits.items(), key=lambda kv: -kv[1]["n"]):
        src = p["rules"] / f"{uuid}.yara"
        if src.exists():
            shutil.move(str(src), str(p["quarantine"] / f"{uuid}.yara"))
        lines.append(
            f"{uuid}\t{info['rule']}\t{info['n']} hits\t{','.join(info['where'])}"
        )
    if lines:
        header = (
            f"# quarantined {time.strftime('%Y-%m-%d')} -- fired on "
            f"{len(files)} known-clean binaries\n"
            "# uuid\trule\thits\texamples\n"
        )
        p["quarantine_log"].write_text(header + "\n".join(lines) + "\n")
    log(
        f"  gate: {len(files)} clean binaries scanned, "
        f"{len(hits)} rules quarantined"
    )
    return hits


def report_vendored(log=print):
    """Same gate against the vendored ruleset -- reports, never moves.

    Those files are reviewed and committed; deleting one is a human's call. But
    if an upstream rule starts firing on clean system binaries you want to hear
    about it in the same breath.
    """
    from bsimvis.app.services.yara_service import vendored_rules

    rules = vendored_rules()
    if rules is None:
        return {}
    hits = {}
    for f in baseline_files():
        try:
            for m in rules.match(str(f), timeout=300):
                hits[m.rule] = hits.get(m.rule, 0) + 1
        except Exception:
            continue
    if hits:
        log("  vendored ruleset also fired on clean binaries (not quarantined):")
        for rule, n in sorted(hits.items(), key=lambda kv: -kv[1])[:10]:
            log(f"    {rule}: {n}")
    return hits


# --- Curated tags (opt-in, needs an API key) --------------------------------


def index_tags(galaxies, log=print, limit=None):
    """Recover Rulezet's curated tags for the named MISP galaxies.

    No endpoint returns a rule's tags, but `POST /rule/private/search` *filters*
    by exact tag name. So the map is rebuilt in reverse: ask for every rule
    carrying one tag, and every uuid that comes back is known to carry it. That
    is one request per tag name, which is why this is opt-in per galaxy --
    `mitre-attack-pattern` alone is 1266 clusters, and all of misp-galaxy would
    be tens of thousands of requests.

    The tag vocabulary comes from the public misp-galaxy repo, which is the same
    source rulezet imports.
    """
    api_key = cfg("api_key") or os.environ.get("RULEZET_API_KEY")
    if not api_key:
        log("index-tags needs rulezet.api_key -- no curated tags without one.")
        return {}

    base = (cfg("url") or DEFAULT_URL).rstrip("/")
    found = {}
    for galaxy in galaxies:
        try:
            clusters = _get(GALAXY_URL.format(galaxy), timeout=180).get("values") or []
        except (urllib.error.URLError, ValueError) as e:
            log(f"  {galaxy}: cannot read galaxy ({e})")
            continue
        if limit:
            clusters = clusters[:limit]
        log(f"  {galaxy}: {len(clusters)} tag names to query")
        hit_rules = 0
        for i, cluster in enumerate(clusters, 1):
            value = cluster.get("value")
            if not value:
                continue
            raw = f'misp-galaxy:{galaxy}="{value}"'
            try:
                doc = _post(
                    f"{base}/api/rule/private/search",
                    {
                        "tags": [raw],
                        "rule_type": "yara",
                        "fields": ["uuid"],
                        "paginate": False,
                    },
                    api_key,
                )
            except (urllib.error.URLError, ValueError):
                continue
            routed = route_tags([raw])
            if not routed:
                continue
            for r in doc.get("rules") or []:
                uuid = r.get("uuid")
                if uuid:
                    found.setdefault(uuid, set()).update(routed)
                    hit_rules += 1
            if i % 200 == 0:
                log(f"    {i}/{len(clusters)} tags, {hit_rules} rule-tags so far")
        log(f"  {galaxy}: {hit_rules} rule-tags recovered")

    if found:
        _merge_tags({k: sorted(v) for k, v in found.items()})
    return found


# --- Orchestration ----------------------------------------------------------


def sync(full=False, limit=None, log=print, meta_only=False):
    """Mirror, tag, compile, gate. Safe to re-run; incremental with an API key.

    `meta_only` is the backfill for mirrors synced before provenance existed:
    fetch every rule, store its metadata, touch nothing else. It ignores the
    last-sync date by definition -- the rules it is describing are the old ones.
    """
    p = paths()
    p["root"].mkdir(parents=True, exist_ok=True)
    state = {}
    if p["state"].exists() and not (full or meta_only):
        try:
            state = json.loads(p["state"].read_text())
        except ValueError:
            state = {}

    t0 = time.time()
    rules = fetch_rules(since=state.get("last_sync"), limit=limit, log=log)
    log(f"fetched {len(rules)} rules in {time.time() - t0:.0f}s")
    if not rules:
        log("nothing new.")
        return

    tag_config = load_tag_config(log=log)
    tags, rows = _write_rules(rules, tag_config, log=log, write_files=not meta_only)
    _merge_tags(tags)
    _store_rows(rows, log=log)

    if meta_only:
        log(f"meta-only sync done in {time.time() - t0:.0f}s")
        return

    compiled = compile_mirror(log=log)
    if gate(compiled, log=log):
        # Quarantined rules are gone from the directory but still in the
        # compiled ruleset, so it has to be rebuilt from what survived.
        compile_mirror(log=log, validate=False)
    report_vendored(log=log)

    p["state"].write_text(json.dumps({"last_sync": time.strftime("%Y-%m-%d %H:%M")}))
    p["readme"].write_text(
        "# Synced from rulezet.org\n\n"
        f"Last sync: {time.strftime('%Y-%m-%d %H:%M')}\n"
        f"Rules: {len(list(p['rules'].glob('*.yara')))} "
        f"(+{len(list(p['quarantine'].glob('*.yara')))} quarantined)\n\n"
        "Build artifacts, not source: regenerated by `bsimvis rulezet sync`,\n"
        "never committed. Rule licenses vary per rule -- see `rulezet.allow_licenses`.\n"
        "`platform_tag_configs.json` is downloaded from rulezet-core (AGPL-3.0)\n"
        "and is deliberately not vendored into this Apache-2.0 repo.\n"
    )
    log(f"sync done in {time.time() - t0:.0f}s")


def demo():
    """Self-check for the parts that are pure logic."""
    assert _vulns({"cve_id": '["CVE-2021-44228", "GHSA-j8v8-6h6r-m6pq"]'}) == [
        "CVE-2021-44228",
        "GHSA-J8V8-6H6R-M6PQ",
    ]
    assert _vulns({"cve_id": "CVE-2025-53521"}) == ["CVE-2025-53521"]
    assert _vulns({"cve_id": None}) == [] and _vulns({}) == []

    cfgs = [
        (re.compile(r"\bupx\b", re.I), 'runtime-packer:pe="upx"'),
        (
            re.compile(r"\bransom(ware)?\b", re.I),
            'ms-caro-malware-full:malware-type="Ransom"',
        ),
    ]
    tags = platform_tags(
        {
            "title": "Win32_Ransomware_LockBit",
            "description": "packed with UPX",
            "cve_id": "CVE-2021-44228",
        },
        cfgs,
    )
    assert 'runtime-packer:pe="upx"' in tags, tags
    # From the title alone, which only works because `_` is normalised away --
    # this is the assert that fails if that ever gets "simplified" out.
    assert 'ms-caro-malware-full:malware-type="Ransom"' in tags, tags
    assert "cve:CVE-2021-44228" in tags, tags
    # A rule that matches nothing gets no tags rather than an empty-string tag.
    assert platform_tags({"title": "x", "description": "y"}, cfgs) == []
    print("rulezet_service demo OK")


if __name__ == "__main__":
    demo()
