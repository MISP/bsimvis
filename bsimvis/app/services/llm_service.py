import logging
from ollama import Client
from bsimvis.app.services.config_service import config_service

class LLMService:
    def __init__(self):
        self._load_config()

    def _load_config(self):
        self.ollama_url = config_service.get("llm.ollama_url", "http://localhost:11434")
        self.model = config_service.get("llm.model", "qwen3.6:35b")
        self.default_prompt = config_service.get("llm.prompt", "Act as a senior reverse engineer. Provide a structured, keyword-focused summary of this function. **SUMMARY**: [One-line summary of functionality] **KEYWORDS**: [List 5-10 key technical terms, API calls, or algorithm names] **IMPACT**: [Side-effects, security implications, or critical dependencies] **LOGIC**: [Brief description of data transformation or logic path]")

    def summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"
        
        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{'role': 'user', 'content': full_prompt}],
                stream=False,
                think=False,
                options={
                    'num_predict': -1,
                    'temperature': 0.3
                }
            )
            msg = response.get('message', {})
            return msg.get('content', '') or msg.get('thinking', '')
        except Exception as e:
            logging.error(f"LLMService error: {e}")
            return f"Error: Could not get summary from LLM. {e}"

    def stream_summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"
        
        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{'role': 'user', 'content': full_prompt}],
                stream=True,
                think=False,
                options={
                    'num_predict': -1,
                    'temperature': 0.1
                }
            )
            for chunk in response:
                msg = chunk.get('message', {}) if isinstance(chunk, dict) else getattr(chunk, 'message', None)
                if msg:
                    content = getattr(msg, 'content', '') or getattr(msg, 'thinking', '') if not isinstance(msg, dict) else msg.get('content', '') or msg.get('thinking', '')
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
                options={
                    'num_predict': -1,
                    'temperature': 0.1
                }
            )
            msg = response.get('message', {})
            return msg.get('content', '') or msg.get('thinking', '')
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
                options={
                    'num_predict': -1,
                    'temperature': 0.1
                }
            )
            for chunk in response:
                msg = chunk.get('message', {}) if isinstance(chunk, dict) else getattr(chunk, 'message', None)
                if msg:
                    content = getattr(msg, 'content', '') or getattr(msg, 'thinking', '') if not isinstance(msg, dict) else msg.get('content', '') or msg.get('thinking', '')
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
        lines.append("Analyze the following binary file and provide a structured threat intelligence summary.\n")

        # Base metadata
        lines.append("## Binary Metadata")
        for key, label in [
            ("file_name", "File Name"), ("file_md5", "MD5"),
            ("language_id", "Architecture"), ("function_count", "Function Count"),
            ("filetype", "File Type"), ("avtype", "AV Classification"),
            ("yara", "Yara Matches"), ("cc_ip", "C2 IPs"),
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
                lines.append(f"\n### Cluster: {name} (cohesion={cohesion:.2f}, members={size})")
                for dist_key, dist_label in [
                    ("yara_distribution", "Yara"), ("avtype_distribution", "AV Types"),
                    ("filetype_distribution", "File Types"), ("ccip_distribution", "C2 IPs"),
                    ("filename_distribution", "File Names"),
                ]:
                    dist = c.get(dist_key)
                    if dist:
                        top = sorted(dist, key=lambda x: x.get("percent", 0), reverse=True)[:5]
                        items = ", ".join(f"{d['value']} ({d.get('percent', 0)}%)" for d in top if d.get("value"))
                        if items:
                            lines.append(f"  - {dist_label}: {items}")

        # Inferred metadata
        if inferred_meta:
            lines.append("\n## Inferred Metadata (from similar binaries in clusters)")
            for key, label in [
                ("yara", "Yara"), ("avtype", "AV Family"), ("filetype", "File Type"),
                ("ccip", "C2 IPs"), ("filename", "File Names"),
            ]:
                data = inferred_meta.get(key, {})
                if data:
                    top = sorted(data.items(), key=lambda x: x[1].get("percent", 0), reverse=True)[:5]
                    items = ", ".join(f"{k} ({v.get('percent', 0)}%)" for k, v in top)
                    lines.append(f"- **{label}**: {items}")

        lines.append("\n---")
        lines.append("Provide your analysis in this format:")
        lines.append("**CLASSIFICATION**: [Malware family / benign / unknown]")
        lines.append("**THREAT LEVEL**: [Critical / High / Medium / Low / Unknown]")
        lines.append("**CAPABILITIES**: [Bullet list of observed capabilities]")
        lines.append("**INDICATORS**: [Key IOCs: hashes, IPs, filenames, Yara rules]")
        lines.append("**CLUSTER CONTEXT**: [What the cluster membership tells us about this binary]")
        lines.append("**NOTES**: [Any additional observations or caveats]")

        prompt = "\n".join(lines)

        try:
            client = Client(host=self.ollama_url)
            response = client.chat(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                stream=True,
                think=False,
                options={"num_predict": -1, "temperature": 0.1}
            )
            for chunk in response:
                msg = chunk.get("message", {}) if isinstance(chunk, dict) else getattr(chunk, "message", None)
                if msg:
                    content = getattr(msg, "content", "") or getattr(msg, "thinking", "") if not isinstance(msg, dict) else msg.get("content", "") or msg.get("thinking", "")
                    if content:
                        yield content
        except Exception as e:
            logging.error(f"LLMService file summary error: {e}")
            yield f"Error: {e}"


llm_service = LLMService()

