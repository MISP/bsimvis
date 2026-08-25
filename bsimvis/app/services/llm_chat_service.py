"""Interactive, tool-using LLM analyst chat.

Unlike `llm_service.chat` (a single stateless completion), this loop lets the
model call back into the collection -- pull another function's code, walk its
call graph, check similarity/cluster membership -- before answering, so an
analyst question like "does this look like it's imitating a legitimate
installer" can actually be investigated rather than answered off one
function's text alone.

Sessions persist in Redis (not the job queue) as a plain message list, so a
conversation survives a page reload and page refresh cost is one GET.
"""

import json
import logging
import time
import uuid

from ollama import Client

from bsimvis.app.services.config_service import config_service
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.llm_tools import TOOLS, call_tool
from bsimvis.app.services.redis_client import get_redis

logger = logging.getLogger(__name__)

MAX_TOOL_ITERATIONS = 8
SESSION_TTL = 7 * 24 * 3600  # a week -- long enough to resume, short enough to not pile up

DEFAULT_SYSTEM_PROMPT = (
    "You are a senior reverse engineer and malware analyst assisting a human "
    "analyst inside a binary-analysis tool. You have tools to look up any "
    "function's decompiled code, its call graph, its BSim similarity "
    "neighbours, tag/search across the collection, and file- or "
    "cluster-level metadata. Use them: do not guess about a function's "
    "behaviour or a binary's intent from its name alone, and do not assume "
    "your first read of one function is the whole story -- check its "
    "callers and callees when behaviour is ambiguous.\n\n"
    "A recurring judgment call in this tool is distinguishing genuinely "
    "legitimate code from malware imitating legitimate code (fake installer "
    "text, spoofed version/company strings, a benign-looking name wrapping "
    "hostile logic). When relevant, actively look for that: check whether "
    "strings/behaviour match what a real instance of that library/installer "
    "does, whether the function is BSim-similar to known-good code or "
    "unique to this binary, and whether its call-graph neighbours fit the "
    "claimed purpose.\n\n"
    "Cite what you actually checked (function ids, tags, scores) rather than "
    "asserting conclusions. If a question cannot be answered with the "
    "available tools, say so instead of speculating."
)


def _session_key(session_id):
    return f"llm_chat:session:{session_id}"


class LLMChatService:
    def __init__(self, r=None):
        self.r = r or get_redis()

    def _load_config(self):
        self.ollama_url = config_service.get("llm.ollama_url", "http://localhost:11434")
        self.model = config_service.get("llm.model", "qwen3.6:35b")
        self.system_prompt = config_service.get(
            "llm.chat_system_prompt", DEFAULT_SYSTEM_PROMPT
        )

    # --- session persistence -------------------------------------------

    def _load_history(self, session_id):
        raw = self.r.get(_session_key(session_id))
        if not raw:
            return None
        return json.loads(raw)

    def _save_history(self, session_id, history):
        self.r.set(_session_key(session_id), json.dumps(history), ex=SESSION_TTL)

    def start_session(self, collection, custom_system_prompt=None):
        self._load_config()
        session_id = str(uuid.uuid4())
        system = custom_system_prompt or self.system_prompt
        system += f"\n\nDefault collection for tool calls: '{collection}' (pass this unless the analyst names another)."
        history = [{"role": "system", "content": system, "ts": int(time.time())}]
        self._save_history(session_id, history)
        return session_id

    def get_session(self, session_id):
        history = self._load_history(session_id)
        if history is None:
            return None
        # Tool-call plumbing messages are not analyst-facing; keep user/assistant turns.
        return [m for m in history if m["role"] in ("user", "assistant")]

    # --- turn execution ---------------------------------------------------

    def send_message(self, session_id, user_message):
        """Runs one analyst turn to completion (including any tool calls the
        model makes along the way). Returns {reply, tool_calls, session_id}."""
        self._load_config()
        history = self._load_history(session_id)
        if history is None:
            return {"error": "Unknown or expired session"}

        history.append(
            {"role": "user", "content": user_message, "ts": int(time.time())}
        )

        client = Client(host=self.ollama_url)
        tool_calls_made = []

        for _ in range(MAX_TOOL_ITERATIONS):
            wire_messages = [
                {k: v for k, v in m.items() if k != "ts"} for m in history
            ]
            try:
                response = client.chat(
                    model=self.model,
                    messages=wire_messages,
                    tools=TOOLS,
                    stream=False,
                    think=False,
                    options={"num_predict": -1, "temperature": 0.2},
                )
            except Exception as e:
                logger.error(f"LLMChatService: chat call failed: {e}")
                return {"error": str(e)}

            # `response` is an ollama `ChatResponse` (dict-like via `.get`, not
            # an actual dict) -- match the `.get(...)` pattern the rest of
            # llm_service.py uses rather than guarding with `isinstance(dict)`,
            # which is always false for it and silently drops every reply.
            msg = response.get("message", {})
            content = msg.get("content", "") or ""
            # Ollama returns tool calls as pydantic ToolCall objects, not plain
            # dicts -- json.dumps (in _save_history, on the very next line that
            # stores one) would raise on them, so normalise once here rather
            # than downstream.
            raw_calls = [
                tc.model_dump() if hasattr(tc, "model_dump") else tc
                for tc in (msg.get("tool_calls") or [])
            ]

            if not raw_calls:
                history.append(
                    {"role": "assistant", "content": content, "ts": int(time.time())}
                )
                self._save_history(session_id, history)
                return {
                    "session_id": session_id,
                    "reply": content,
                    "tool_calls": tool_calls_made,
                }

            history.append(
                {
                    "role": "assistant",
                    "content": content,
                    "tool_calls": raw_calls,
                    "ts": int(time.time()),
                }
            )

            for tc in raw_calls:
                fn = tc.get("function", {})
                name = fn.get("name")
                args = fn.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        args = {}
                result = call_tool(name, args)
                tool_calls_made.append({"name": name, "arguments": args})
                history.append(
                    {
                        "role": "tool",
                        "content": json.dumps(result)[:8000],
                        "ts": int(time.time()),
                    }
                )

        # Iteration budget spent without a final answer -- return what the
        # model has said so far rather than raising, so the analyst still
        # gets a partial trail instead of an opaque failure.
        self._save_history(session_id, history)
        return {
            "session_id": session_id,
            "reply": "(stopped after too many tool calls without a final answer)",
            "tool_calls": tool_calls_made,
        }


llm_chat_service = LLMChatService()
