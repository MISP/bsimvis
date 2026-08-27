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

    print("ok")


if __name__ == "__main__":
    _selfcheck()
