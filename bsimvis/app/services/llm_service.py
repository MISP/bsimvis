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
            "Act as a senior malware reverse engineer. Provide a concise, rapid-triage summary of this function. **TLDR**: [Maximum 2 sentences explaining the core purpose and intent] **KEY_EVIDENCE**: [Comma-separated list of ONLY the most critical Windows APIs, syscalls, or magic constants. Leave blank if none.]",
        )

    def summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{"role": "user", "content": full_prompt}],
                stream=False,
                think=False,
                options={"num_predict": -1, "temperature": 0.3},
            )
            msg = response.get("message", {})
            return msg.get("content", "") or msg.get("thinking", "")
        except Exception as e:
            logging.error(f"LLMService error: {e}")
            return f"Error: Could not get summary from LLM. {e}"

    def summarize_and_tag(
        self, function_name, code, vocabulary=None, custom_prompt=None
    ):
        """One LLM call returning both a summary and tags.

        Halves the token cost versus two calls. Tags come back on a single
        `TAGS:` line; if that line is missing or unparseable the summary is
        still returned with an empty tag list -- a note without tags beats
        failing the whole function.
        """
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
                "Example: TAGS: severity:medium, category:crypto:cipher, custom_tag_name"
            )
        else:
            tag_rule = (
                f"{base_rule}\n"
                "Example: TAGS: severity:medium, category:crypto:cipher, category:network:c2. "
                "If the function is trivial, write 'TAGS: severity:none, category:util:init'."
            )

        full_prompt = (
            f"{prompt}\n\n{tag_rule}\n\n"
            f"Function Name: {function_name}\n\nCode:\n{code}"
        )

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{"role": "user", "content": full_prompt}],
                stream=False,
                think=False,
                options={"num_predict": -1, "temperature": 0.3},
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
        """Splits an LLM response into (summary, tags) on the last TAGS: line."""
        if not text:
            return "", []

        lines = text.strip().splitlines()
        tag_idx = None
        for i in range(len(lines) - 1, -1, -1):
            # Models decorate the label: `TAGS:`, `**TAGS**:`, `## TAGS:` ...
            stripped = lines[i].strip().lstrip("#*- ").upper()
            if stripped.startswith("TAGS"):
                tag_idx = i
                break

        if tag_idx is None:
            return text.strip(), []

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
            return tag_taxonomy.is_taxonomy_tag(t) or t in allowed

        tags = [allowed.get(t, t) for t in tags if is_allowed(t)]

        # Dedupe, preserve order.
        seen = set()
        tags = [t for t in tags if not (t in seen or seen.add(t))]
        return summary, tags

    def stream_summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{"role": "user", "content": full_prompt}],
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

    # The model must not be able to assert provenance.
    assert split("x\nTAGS: origin:lib:libc:2.31", ["mytag"])[1] == []

    # Only the last TAGS line counts (models sometimes echo the instruction).
    assert split("TAGS: ignored\nbody\nTAGS: severity:high")[1] == ["severity:high"]

    print("ok")


if __name__ == "__main__":
    _selfcheck()
