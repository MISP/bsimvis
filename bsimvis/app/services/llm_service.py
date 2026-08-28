import json
import logging
from ollama import Client
from bsimvis.app.services import tag_taxonomy
from bsimvis.app.services.config_service import config_service


class LLMService:
    def __init__(self):
        self._load_config()

    def _load_config(self):
        self.ollama_url = config_service.get("llm.ollama_url", "http://localhost:11434")
        self.model = config_service.get("llm.model", "qwen3.6:35b")
        self.default_prompt = config_service.get(
            "llm.prompt",
            "Act as a senior malware reverse engineer. In at most two sentences, state the function's observed purpose and the decisive code evidence. If its purpose cannot be established, say so without suggesting possibilities. Do not discuss vulnerabilities or hypothetical impact.",
        )

    def summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": tag_taxonomy.analysis_rules()},
                    {"role": "user", "content": full_prompt},
                ],
                stream=False,
                think=False,
                options={"num_predict": -1, "temperature": 0.1},
            )
            msg = response.get("message", {})
            return msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService error: {e}")
            return f"Error: Could not get summary from LLM. {e}"

    def summarize_and_tag(
        self, function_name, code, vocabulary=None, custom_prompt=None
    ):
        """One bounded LLM call returning both a summary and validated tags."""
        self._load_config()
        prompt = custom_prompt or self.default_prompt

        # The core namespace strategy is always active. Severity and category are
        # separate tags, not one welded `risk:capability` id, so the two can be
        # crossed later ("high severity AND network") instead of only counted.
        base_rule = tag_taxonomy.prompt_rules()

        if vocabulary:
            tag_rule = (
                f"{base_rule}\n"
                f"Additionally, you MAY include these specific custom tags if highly relevant: {', '.join(vocabulary)}. "
                "Example tags: severity:medium, category:crypto:cipher, custom_tag_name."
            )
        else:
            tag_rule = (
                f"{base_rule}\n"
                "Example tags: severity:medium, category:crypto:cipher, category:network:c2."
            )

        full_prompt = (
            f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}\n\n"
            "Return the required severity verdict and put category/custom tags only in "
            "the tags field, never inside summary. Keep summary under 90 words and "
            "include only observed behavior and decisive evidence."
        )
        custom_tags = [
            tag
            for tag in (vocabulary or [])
            if tag.split(":", 1)[0].lower() not in ("severity", "category")
        ]
        allowed_tags = list(
            dict.fromkeys(tag_taxonomy.CATEGORY_TAGS + tuple(custom_tags))
        )
        response_format = {
            "type": "object",
            "properties": {
                "severity": {
                    "type": "string",
                    "description": "Observed malware severity; unknown when purpose is unresolved.",
                    "enum": ["unknown", *tag_taxonomy.SEVERITY_LEVELS],
                },
                "tags": {
                    "type": "array",
                    "description": "All classification tags; empty when purpose is unresolved.",
                    "items": {"type": "string", "enum": allowed_tags},
                    "maxItems": 3,
                },
                "summary": {
                    "type": "string",
                    "description": "Observed behavior and evidence only; never include tag IDs.",
                },
            },
            "required": ["severity", "tags", "summary"],
            "additionalProperties": False,
        }

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": tag_rule},
                    {"role": "user", "content": full_prompt},
                ],
                stream=False,
                think=False,
                format=response_format,
                options={"num_predict": 256, "temperature": 0.1},
            )
            msg = response.get("message", {})
            text = msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService summarize_and_tag error: {e}")
            return None, [], str(e)

        summary, tags = self._split_summary_tags(text, vocabulary)
        return summary, tags, None

    def summarize_batch(self, members, vocabulary=None, custom_prompt=None):
        """One LLM call judging several call-connected functions at once,
        each independently attributed -- unlike a mutually-recursive SCC's
        one-shared-verdict call, these functions are not assumed to behave
        as a unit. `members` is [(func_id, func_name, code), ...]. Returns
        ({func_id: (summary, tags)}, missing_func_ids, error)."""
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        base_rule = tag_taxonomy.prompt_rules()
        custom_tags = [
            tag
            for tag in (vocabulary or [])
            if tag.split(":", 1)[0].lower() not in ("severity", "category")
        ]
        allowed_tags = list(
            dict.fromkeys(tag_taxonomy.CATEGORY_TAGS + tuple(custom_tags))
        )
        if vocabulary:
            tag_rule = (
                f"{base_rule}\n"
                f"Additionally, you MAY include these specific custom tags if highly relevant: {', '.join(vocabulary)}. "
                "Example tags: severity:medium, category:crypto:cipher, custom_tag_name."
            )
        else:
            tag_rule = (
                f"{base_rule}\n"
                "Example tags: severity:medium, category:crypto:cipher, category:network:c2."
            )

        func_ids = [fid for fid, _, _ in members]
        blocks = "\n\n".join(
            f"=== FUNCTION {fid} ({name}) ===\nCode:\n{code}"
            for fid, name, code in members
        )
        full_prompt = (
            f"{prompt}\n\n{len(members)} functions that directly call each "
            "other, but each has its own behavior -- judge and tag every one "
            f"independently, never merge their behaviors into one verdict.\n\n{blocks}\n\n"
            "Return one result per function id above, using its id exactly as "
            "given. Keep each summary under 90 words and put category/custom "
            "tags only in that function's tags field, never inside summary."
        )
        response_format = {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "minItems": len(members),
                    "maxItems": len(members),
                    "items": {
                        "type": "object",
                        "properties": {
                            "func_id": {"type": "string", "enum": func_ids},
                            "severity": {
                                "type": "string",
                                "description": "Observed malware severity; unknown when purpose is unresolved.",
                                "enum": ["unknown", *tag_taxonomy.SEVERITY_LEVELS],
                            },
                            "tags": {
                                "type": "array",
                                "description": "All classification tags; empty when purpose is unresolved.",
                                "items": {"type": "string", "enum": allowed_tags},
                                "maxItems": 3,
                            },
                            "summary": {
                                "type": "string",
                                "description": "Observed behavior and evidence only; never include tag IDs.",
                            },
                        },
                        "required": ["func_id", "severity", "tags", "summary"],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["results"],
            "additionalProperties": False,
        }

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": tag_rule},
                    {"role": "user", "content": full_prompt},
                ],
                stream=False,
                think=False,
                format=response_format,
                options={"num_predict": 256 * len(members), "temperature": 0.1},
            )
            msg = response.get("message", {})
            text = msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService summarize_batch error: {e}")
            return {}, func_ids, str(e)

        return self._parse_batch_response(text, func_ids, vocabulary)

    @staticmethod
    def _parse_batch_response(text, func_ids, vocabulary=None):
        """Splits one batched structured response into independently
        filtered per-function results, replaying each item through
        `_split_summary_tags` as its own single-object JSON so the same
        grounding/tag rules apply as the single-function path."""
        out = {}
        try:
            value = json.loads(text) if text else {}
            items = value.get("results") if isinstance(value, dict) else None
        except (TypeError, ValueError):
            items = None
        for item in items or []:
            if not isinstance(item, dict):
                continue
            fid = item.get("func_id")
            if fid not in func_ids or fid in out:
                continue
            item_text = json.dumps(
                {
                    "summary": item.get("summary"),
                    "tags": item.get("tags"),
                    "severity": item.get("severity"),
                }
            )
            summary, tags = LLMService._split_summary_tags(item_text, vocabulary)
            out[fid] = (summary, tags)
        missing = [fid for fid in func_ids if fid not in out]
        return out, missing, None

    def classify_relevance_batch(self, members, query, vocabulary=None):
        """Fast relevance triage: for every function, does it plausibly
        relate to the analyst's free-text question? Not a behavioral
        analysis -- a search filter. One call judges many functions (no
        call-graph batching needed, each verdict is independent of the
        others), and the per-item output is tiny (a verdict word + one short
        evidence phrase) instead of `summarize_batch`'s full
        severity/tags/summary object, which is where the speed comes from.
        `members` is [(func_id, func_name, code), ...]. Returns
        ({func_id: (verdict, evidence, suggested_tag)}, missing_func_ids,
        error). `suggested_tag` is `None` when nothing in the taxonomy fits.
        """
        self._load_config()
        custom_tags = [
            tag
            for tag in (vocabulary or [])
            if tag.split(":", 1)[0].lower() not in ("severity", "category")
        ]
        allowed_tags = list(
            dict.fromkeys(tag_taxonomy.CATEGORY_TAGS + tuple(custom_tags))
        )

        func_ids = [fid for fid, _, _ in members]
        blocks = "\n\n".join(
            f"=== FUNCTION {fid} ({name}) ===\nCode:\n{code}"
            for fid, name, code in members
        )
        full_prompt = (
            "An analyst is triaging functions and is looking for: "
            f"{query}\n\n{blocks}\n\n"
            "For every function above, decide only whether it plausibly "
            "relates to what the analyst described. Reason from the actual "
            "code shown, including constructions that don't look like an "
            "obvious keyword match -- e.g. a string assembled byte by byte "
            "or built from individual constants, not just visible literal "
            "text. Do not judge general maliciousness or severity here. "
            "Evidence is one short phrase citing the specific code, empty "
            "when the verdict is no."
        )
        response_format = {
            "type": "object",
            "properties": {
                "results": {
                    "type": "array",
                    "minItems": len(members),
                    "maxItems": len(members),
                    "items": {
                        "type": "object",
                        "properties": {
                            "func_id": {"type": "string", "enum": func_ids},
                            "verdict": {
                                "type": "string",
                                "enum": ["yes", "maybe", "no"],
                            },
                            "evidence": {"type": "string"},
                            "suggested_tag": {
                                "type": "string",
                                "description": "A taxonomy tag this function's behavior supports, or 'none'.",
                                "enum": ["none", *allowed_tags],
                            },
                        },
                        "required": [
                            "func_id",
                            "verdict",
                            "evidence",
                            "suggested_tag",
                        ],
                        "additionalProperties": False,
                    },
                }
            },
            "required": ["results"],
            "additionalProperties": False,
        }

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a fast relevance-triage step, not a full "
                            "malware analyst. Judge only relevance to the "
                            "analyst's stated question. Never invent behavior "
                            "the shown code does not support."
                        ),
                    },
                    {"role": "user", "content": full_prompt},
                ],
                stream=False,
                think=False,
                format=response_format,
                options={"num_predict": 40 * len(members), "temperature": 0.1},
            )
            msg = response.get("message", {})
            text = msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService classify_relevance_batch error: {e}")
            return {}, func_ids, str(e)

        return self._parse_classify_response(text, func_ids, allowed_tags)

    @staticmethod
    def _parse_classify_response(text, func_ids, allowed_tags=()):
        """Network-free parsing of `classify_relevance_batch`'s structured
        response, covered by `_selfcheck` below. `allowed_tags` is whatever
        set the caller's schema constrained `suggested_tag` to (taxonomy +
        any custom vocabulary) -- a model that ignores the schema and
        returns something outside it gets nulled out rather than trusted."""
        allowed = set(allowed_tags)
        out = {}
        try:
            value = json.loads(text) if text else {}
            items = value.get("results") if isinstance(value, dict) else None
        except (TypeError, ValueError):
            items = None
        for item in items or []:
            if not isinstance(item, dict):
                continue
            fid = item.get("func_id")
            if fid not in func_ids or fid in out:
                continue
            verdict = item.get("verdict")
            if verdict not in ("yes", "maybe", "no"):
                continue
            evidence = (item.get("evidence") or "").strip()
            tag = item.get("suggested_tag")
            if not tag or tag == "none" or tag not in allowed:
                tag = None
            out[fid] = (verdict, evidence, tag)
        missing = [fid for fid in func_ids if fid not in out]
        return out, missing, None

    @staticmethod
    def _split_summary_tags(text, vocabulary=None):
        """Parses structured output, with the old TAGS line as a fallback."""
        if not text:
            return "", []

        try:
            value = json.loads(text)
            if isinstance(value, dict) and isinstance(value.get("summary"), str):
                raw_tags = value.get("tags")
                if isinstance(raw_tags, list):
                    summary_lines = value["summary"].strip().splitlines()
                    embedded_tags = []
                    while summary_lines:
                        candidate = summary_lines[-1].strip().strip("*`[] ").lower()
                        if candidate.startswith(("severity:", "category:")):
                            embedded_tags.insert(0, candidate)
                            summary_lines.pop()
                            continue
                        if not candidate and embedded_tags:
                            summary_lines.pop()
                            continue
                        break
                    raw_tags = [*raw_tags, *embedded_tags]
                    summary_text = "\n".join(summary_lines).strip()
                    severity = value.get("severity")
                    if severity in tag_taxonomy.SEVERITY_LEVELS:
                        raw_tags = [f"severity:{severity}", *raw_tags]
                    text = f"{summary_text}\nTAGS: {','.join(map(str, raw_tags))}"
        except (TypeError, ValueError):
            pass

        lines = text.strip().splitlines()
        tag_idx = None
        for i in range(len(lines) - 1, -1, -1):
            # Models decorate the label: `TAGS:`, `**TAGS**:`, `## TAGS:` ...
            stripped = lines[i].strip().lstrip("#*- ").upper()
            if stripped.startswith("TAGS"):
                tag_idx = i
                break

        if tag_idx is None:
            # Some Ollama servers ignore `format` but still append one bare tag
            # per line. Accept only a trailing reserved-namespace block.
            bare_tags = []
            i = len(lines) - 1
            while i >= 0:
                candidate = lines[i].strip().strip("*`[] ").lower()
                if candidate.startswith(("severity:", "category:")):
                    bare_tags.append(candidate)
                    i -= 1
                    continue
                if not candidate and bare_tags:
                    i -= 1
                    continue
                break
            if bare_tags:
                lines = lines[: i + 1] + [f"TAGS: {','.join(reversed(bare_tags))}"]
            else:
                lines = [*lines, "TAGS:"]
            tag_idx = len(lines) - 1

        label, _, raw_tags = lines[tag_idx].partition(":")
        if not raw_tags:
            raw_tags = ""
        summary = "\n".join(lines[:tag_idx]).strip()

        tags = []
        for t in raw_tags.replace("*", "").split(","):
            t = t.strip().strip("[]`\"'").lower()
            if not t or t in ("none", "n/a"):
                continue
            tags.append(t)

        allowed = {v.lower(): v for v in vocabulary or []}

        def is_allowed(t):
            # Always allow a tag from the fixed taxonomy, otherwise the tag
            # has to be one the collection registered. `is_taxonomy_tag`
            # rejects `origin:` on purpose: a hallucinated library
            # attribution must not enter through the summarisation path.
            reserved = t.split(":", 1)[0] in ("severity", "category")
            return tag_taxonomy.is_taxonomy_tag(t) or (not reserved and t in allowed)

        tags = [allowed.get(t, t) for t in tags if is_allowed(t)]

        # Dedupe, preserve order.
        seen = set()
        tags = [t for t in tags if not (t in seen or seen.add(t))]
        summary_lower = summary.lower()
        marker = any(
            line.strip().strip("*# ").upper() == "NEED_MORE_CONTEXT"
            for line in summary.splitlines()
        )
        if marker:
            summary = "NEED_MORE_CONTEXT"
            tags = []

        elif tags == ["severity:none"] and any(
            phrase in summary_lower
            for phrase in (
                "purpose is unknown",
                "purpose is indeterminate",
                "purpose is unresolved",
                "cannot determine its purpose",
            )
        ):
            tags = []
        unsupported_syscall_claim = (
            "syscall" in summary_lower
            and "loop" in summary_lower
            and any(
                term in summary_lower
                for term in ("unidentified", "unknown", "indeterminate", "unresolved")
            )
        )
        if unsupported_syscall_claim:
            tags = [
                tag
                for tag in tags
                if tag != "category:evasion:rootkit"
                and not tag.startswith("category:persistence:")
            ]
        high_risk_tags = {
            "category:network:c2",
            "category:network:scan",
            "category:process:inject",
            "category:process:privesc",
            "category:recon:creds",
            "category:evasion:rootkit",
        }
        has_high_risk_category = any(
            tag in high_risk_tags
            or tag.startswith(("category:impact:", "category:persistence:"))
            for tag in tags
        )
        if "severity:high" in tags and not has_high_risk_category:
            tags.remove("severity:high")

        return summary, tags

    def stream_summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[
                    {"role": "system", "content": tag_taxonomy.analysis_rules()},
                    {"role": "user", "content": full_prompt},
                ],
                stream=True,
                think=False,
                options={"num_predict": -1, "temperature": 0.1},
            )
            for chunk in response:
                msg = (
                    chunk.get("message", {})
                    if isinstance(chunk, dict)
                    else getattr(chunk, "message", None)
                )
                if msg:
                    content = (
                        getattr(msg, "content", "") or getattr(msg, "thinking", "")
                        if not isinstance(msg, dict)
                        else msg.get("content", "") or msg.get("thinking", "")
                    )
                    if content:
                        yield content
        except Exception as e:
            logging.error(f"LLMService streaming error: {e}")
            yield f"Error: {e}"

    def chat(self, history):
        self._load_config()
        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=history,
                stream=False,
                think=False,
                options={"num_predict": -1, "temperature": 0.1},
            )
            msg = response.get("message", {})
            return msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService chat error: {e}")
            return f"Error: Could not chat with LLM. {e}"

    def stream_chat(self, history):
        self._load_config()
        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=history,
                stream=True,
                think=False,
                options={"num_predict": -1, "temperature": 0.1},
            )
            for chunk in response:
                msg = (
                    chunk.get("message", {})
                    if isinstance(chunk, dict)
                    else getattr(chunk, "message", None)
                )
                if msg:
                    content = (
                        getattr(msg, "content", "") or getattr(msg, "thinking", "")
                        if not isinstance(msg, dict)
                        else msg.get("content", "") or msg.get("thinking", "")
                    )
                    if content:
                        yield content
        except Exception as e:
            logging.error(f"LLMService streaming chat error: {e}")
            yield f"Error: {e}"

    def stream_summarize_file(self, file_meta, clusters, inferred_meta):
        """Streams an LLM summary for a binary file using all available context."""
        self._load_config()

        # Build structured context block
        lines = ["Act as a senior malware analyst and reverse engineer."]
        lines.append(
            "Analyze the following binary file and provide a structured threat intelligence summary.\n"
        )

        # Base metadata
        lines.append("## Binary Metadata")
        for key, label in [
            ("file_name", "File Name"),
            ("file_md5", "MD5"),
            ("language_id", "Architecture"),
            ("function_count", "Function Count"),
            ("filetype", "File Type"),
            ("avtype", "AV Classification"),
            ("yara", "Yara Matches"),
            ("cc_ip", "C2 IPs"),
        ]:
            val = file_meta.get(key)
            if val:
                # Normalize list fields to a clean string
                if isinstance(val, list):
                    val = ", ".join(str(v) for v in val if v)
                if val:
                    lines.append(f"- **{label}**: {val}")

        # Binary clusters
        if clusters:
            lines.append("\n## Binary Cluster Memberships")
            for c in clusters:
                name = c.get("cluster_name", "Unknown")
                cohesion = c.get("cohesion_score", 0)
                size = c.get("member_count") or c.get("size", "?")
                lines.append(
                    f"\n### Cluster: {name} (cohesion={cohesion:.2f}, members={size})"
                )
                for dist_key, dist_label in [
                    ("yara_distribution", "Yara"),
                    ("avtype_distribution", "AV Types"),
                    ("filetype_distribution", "File Types"),
                    ("ccip_distribution", "C2 IPs"),
                    ("filename_distribution", "File Names"),
                ]:
                    dist = c.get(dist_key)
                    if dist:
                        top = sorted(
                            dist, key=lambda x: x.get("percent", 0), reverse=True
                        )[:5]
                        items = ", ".join(
                            f"{d['value']} ({d.get('percent', 0)}%)"
                            for d in top
                            if d.get("value")
                        )
                        if items:
                            lines.append(f"  - {dist_label}: {items}")

        # Inferred metadata
        if inferred_meta:
            lines.append("\n## Inferred Metadata (from similar binaries in clusters)")
            for key, label in [
                ("yara", "Yara"),
                ("avtype", "AV Family"),
                ("filetype", "File Type"),
                ("ccip", "C2 IPs"),
                ("filename", "File Names"),
            ]:
                data = inferred_meta.get(key, {})
                if data:
                    top = sorted(
                        data.items(), key=lambda x: x[1].get("percent", 0), reverse=True
                    )[:5]
                    items = ", ".join(f"{k} ({v.get('percent', 0)}%)" for k, v in top)
                    lines.append(f"- **{label}**: {items}")

        lines.append("\n---")
        lines.append("Provide your analysis in this format:")
        lines.append("**CLASSIFICATION**: [Malware family / benign / unknown]")
        lines.append("**THREAT LEVEL**: [Critical / High / Medium / Low / Unknown]")
        lines.append("**CAPABILITIES**: [Bullet list of observed capabilities]")
        lines.append("**INDICATORS**: [Key IOCs: hashes, IPs, filenames, Yara rules]")
        lines.append(
            "**CLUSTER CONTEXT**: [What the cluster membership tells us about this binary]"
        )
        lines.append("**NOTES**: [Any additional observations or caveats]")

        prompt = "\n".join(lines)

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                stream=True,
                think=False,
                options={"num_predict": -1, "temperature": 0.1},
            )
            for chunk in response:
                msg = (
                    chunk.get("message", {})
                    if isinstance(chunk, dict)
                    else getattr(chunk, "message", None)
                )
                if msg:
                    content = (
                        getattr(msg, "content", "") or getattr(msg, "thinking", "")
                        if not isinstance(msg, dict)
                        else msg.get("content", "") or msg.get("thinking", "")
                    )
                    if content:
                        yield content
        except Exception as e:
            logging.error(f"LLMService file summary error: {e}")
            yield f"Error: {e}"


llm_service = LLMService()


def _selfcheck():
    split = LLMService._split_summary_tags

    # Tags line parsed off the end, summary kept intact.
    s, t = split(
        "**TLDR**: does aes\nmore text\nTAGS: severity:medium, category:crypto:cipher"
    )
    assert s == "**TLDR**: does aes\nmore text", s
    assert t == ["severity:medium", "category:crypto:cipher"], t

    # No TAGS line: whole text is the summary, no tags.
    s, t = split("just a summary")
    assert (s, t) == ("just a summary", [])

    # 'none' and decorations are dropped; duplicates collapse.
    assert split("x\nTAGS: none")[1] == []
    assert split(
        "x\n**TAGS:** `category:crypto:cipher`, category:crypto:cipher, [category:util:parser]"
    )[1] == ["category:crypto:cipher", "category:util:parser"]

    # Invented tags are rejected even when no custom vocabulary was supplied.
    assert split("x\nTAGS: vulnerable, category:network:telepathy")[1] == []

    # A vocabulary constrains *custom* tags but never the fixed taxonomy, and it
    # restores the collection's canonical casing for its own entries.
    s, t = split("x\nTAGS: category:crypto:Cipher, MyTag, invented", ["mytag"])
    assert t == ["category:crypto:cipher", "mytag"], t

    # An invented leaf is not in the taxonomy, so a vocabulary drops it.
    assert split("x\nTAGS: category:network:telepathy", ["mytag"])[1] == []
    assert split(
        "x\nTAGS: category:network:ddos, severity:critical, MyTag",
        ["category:network:ddos", "severity:critical", "mytag"],
    )[1] == ["mytag"]

    # The model must not be able to assert provenance.
    assert split("x\nTAGS: origin:lib:libc:2.31", ["mytag"])[1] == []

    # Only the last TAGS line counts (models sometimes echo the instruction).
    assert split("TAGS: ignored\nbody\nTAGS: severity:low")[1] == ["severity:low"]
    # Older Ollama servers ignore JSON format and append one bare tag per line.
    s, t = split(
        "Observed network packet flood.\nseverity:high\ncategory:impact:ddos\ncategory:"
    )
    assert s == "Observed network packet flood."
    assert t == ["severity:high", "category:impact:ddos"]
    assert (
        split(
            "The syscall identity is unknown; its purpose is indeterminate.\nseverity:none"
        )[1]
        == []
    )

    # An unresolved structured verdict is a successful abstention, not severity:none.
    assert split(
        json.dumps(
            {"severity": "unknown", "summary": "Purpose is unresolved.", "tags": []}
        )
    ) == (
        "Purpose is unresolved.",
        [],
    )

    s, t = split(
        json.dumps(
            {
                "severity": "unknown",
                "tags": [],
                "summary": "Its purpose is unknown.\n\nseverity:none",
            }
        )
    )
    assert (s, t) == ("Its purpose is unknown.", [])
    assert split(
        "Actively probes randomized addresses.\nseverity:high\ncategory:network:scan"
    )[1] == ["severity:high", "category:network:scan"]
    assert split(
        "Exchanges commands with scanned hosts.\nseverity:high\ncategory:network:scan"
    )[1] == ["severity:high", "category:network:scan"]

    class FakeClient:
        def __init__(self):
            self.calls = []

        def chat(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "message": {
                    "content": json.dumps(
                        {
                            "severity": "high",
                            "tags": ["category:impact:ddos"],
                            "summary": "Observed packet flood.",
                        }
                    )
                }
            }

    real_client = globals()["Client"]
    fake_client = FakeClient()
    globals()["Client"] = lambda host: fake_client
    try:
        summary, tags, error = LLMService().summarize_and_tag("attack", "send loop")
    finally:
        globals()["Client"] = real_client
    assert error is None and summary == "Observed packet flood."
    assert tags == ["severity:high", "category:impact:ddos"]
    assert len(fake_client.calls) == 1
    response_format = fake_client.calls[0]["format"]
    assert response_format["required"] == ["severity", "tags", "summary"]
    assert next(iter(response_format["properties"])) == "severity"
    assert fake_client.calls[0]["options"]["num_predict"] == 256

    # _parse_batch_response: per-item attribution, missing/unknown ids, and
    # the same grounding filters as the single-function path apply per item.
    parse = LLMService._parse_batch_response
    out, missing, err = parse(
        json.dumps(
            {
                "results": [
                    {
                        "func_id": "f1",
                        "severity": "high",
                        "tags": ["category:impact:ddos"],
                        "summary": "Floods packets.",
                    },
                    {
                        # severity:high with no high-risk category is stripped,
                        # same as summarize_and_tag's single-function path.
                        "func_id": "f2",
                        "severity": "high",
                        "tags": ["category:util:parser"],
                        "summary": "Parses a header.",
                    },
                    {
                        # not one of the ids in play -- dropped.
                        "func_id": "unknown",
                        "severity": "low",
                        "tags": [],
                        "summary": "x",
                    },
                ]
            }
        ),
        ["f1", "f2", "f3"],
    )
    assert err is None
    assert out["f1"] == ("Floods packets.", ["severity:high", "category:impact:ddos"])
    assert out["f2"] == ("Parses a header.", ["category:util:parser"])
    assert "unknown" not in out
    assert missing == ["f3"]

    class FakeBatchClient:
        def __init__(self):
            self.calls = []

        def chat(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "message": {
                    "content": json.dumps(
                        {
                            "results": [
                                {
                                    "func_id": "a:func:1",
                                    "severity": "unknown",
                                    "tags": [],
                                    "summary": "Purpose is unresolved.",
                                },
                                {
                                    "func_id": "a:func:2",
                                    "severity": "medium",
                                    "tags": ["category:crypto:cipher"],
                                    "summary": "Runs a byte XOR loop.",
                                },
                            ]
                        }
                    )
                }
            }

    fake_batch_client = FakeBatchClient()
    globals()["Client"] = lambda host: fake_batch_client
    try:
        results, missing, error = LLMService().summarize_batch(
            [
                ("a:func:1", "f1", "code1"),
                ("a:func:2", "f2", "code2"),
            ]
        )
    finally:
        globals()["Client"] = real_client
    assert error is None and missing == []
    assert results["a:func:1"] == ("Purpose is unresolved.", [])
    assert results["a:func:2"] == (
        "Runs a byte XOR loop.",
        ["severity:medium", "category:crypto:cipher"],
    )
    batch_format = fake_batch_client.calls[0]["format"]
    assert batch_format["properties"]["results"]["minItems"] == 2
    item_schema = batch_format["properties"]["results"]["items"]
    assert item_schema["properties"]["func_id"]["enum"] == ["a:func:1", "a:func:2"]

    # _parse_classify_response: verdict/evidence pass through, a tag outside
    # the caller's allowed set is nulled rather than trusted, a missing verdict
    # drops the item (surfaces via `missing`) instead of guessing one.
    parse_classify = LLMService._parse_classify_response
    out, missing, err = parse_classify(
        json.dumps(
            {
                "results": [
                    {
                        "func_id": "f1",
                        "verdict": "yes",
                        "evidence": "builds .dat byte by byte",
                        "suggested_tag": "category:persistence:file",
                    },
                    {
                        "func_id": "f2",
                        "verdict": "no",
                        "evidence": "",
                        "suggested_tag": "not-a-real-tag",
                    },
                    {
                        "func_id": "f3",
                        "verdict": "not-a-verdict",
                        "evidence": "x",
                        "suggested_tag": "none",
                    },
                ]
            }
        ),
        ["f1", "f2", "f3", "f4"],
        allowed_tags=["category:persistence:file"],
    )
    assert err is None
    assert out["f1"] == ("yes", "builds .dat byte by byte", "category:persistence:file")
    assert out["f2"] == ("no", "", None)
    assert "f3" not in out  # invalid verdict, not silently coerced
    assert missing == ["f3", "f4"]

    class FakeClassifyClient:
        def __init__(self):
            self.calls = []

        def chat(self, **kwargs):
            self.calls.append(kwargs)
            return {
                "message": {
                    "content": json.dumps(
                        {
                            "results": [
                                {
                                    "func_id": "a:func:1",
                                    "verdict": "maybe",
                                    "evidence": "opens a file by extension",
                                    "suggested_tag": "none",
                                },
                            ]
                        }
                    )
                }
            }

    fake_classify_client = FakeClassifyClient()
    globals()["Client"] = lambda host: fake_classify_client
    try:
        results, missing, error = LLMService().classify_relevance_batch(
            [("a:func:1", "f1", "code1")], "the .dat decrypt routine"
        )
    finally:
        globals()["Client"] = real_client
    assert error is None and missing == []
    assert results["a:func:1"] == ("maybe", "opens a file by extension", None)
    classify_format = fake_classify_client.calls[0]["format"]
    assert classify_format["properties"]["results"]["items"]["properties"]["verdict"][
        "enum"
    ] == ["yes", "maybe", "no"]
    # the short triage prompt, not the full severity/category grounding rules
    assert "relevance" in fake_classify_client.calls[0]["messages"][0]["content"].lower()
    assert fake_classify_client.calls[0]["options"]["num_predict"] == 40

    print("ok")


if __name__ == "__main__":
    _selfcheck()
