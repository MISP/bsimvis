import logging
from ollama import Client
from bsimvis.app.services.config_service import config_service

class LLMService:
    def __init__(self):
        self._load_config()

    def _load_config(self):
        self.ollama_url = config_service.get("llm.ollama_url", "http://localhost:11434")
        self.model = config_service.get("llm.model", "qwen2.5:32b")
        self.default_prompt = config_service.get("llm.prompt", "Provide an ultra-concise TL;DR summary (max 3 sentences) of this function's purpose. Focus on core logic only.")

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
                    'num_predict': 500,
                    'temperature': 0.1
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
                    'num_predict': 500,
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
                    'num_predict': 500,
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
                    'num_predict': 500,
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

llm_service = LLMService()
