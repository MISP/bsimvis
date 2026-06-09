import logging
import asyncio
from ollama import AsyncClient
from bsimvis.app.services.config_service import config_service

class LLMService:
    def __init__(self):
        self._load_config()
        self.client = AsyncClient(host=self.ollama_url)

    def _load_config(self):
        self.ollama_url = config_service.get("llm.ollama_url", "http://localhost:11434")
        self.model = config_service.get("llm.model", "qwen2.5:32b")
        self.default_prompt = config_service.get("llm.prompt", "Provide an ultra-concise TL;DR summary (max 3 sentences) of this function's purpose. Focus on core logic only.")

    async def summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"
        
        try:
            response = await self.client.chat(
                model=self.model,
                messages=[{'role': 'user', 'content': full_prompt}],
                stream=False
            )
            return response.message.content
        except Exception as e:
            logging.error(f"LLMService error: {e}")
            return f"Error: Could not get summary from LLM. {e}"

    async def stream_summarize_function(self, function_name, code, custom_prompt=None):
        self._load_config()
        prompt = custom_prompt or self.default_prompt
        full_prompt = f"{prompt}\n\nFunction Name: {function_name}\n\nCode:\n{code}"
        
        try:
            async for chunk in await self.client.chat(
                model=self.model,
                messages=[{'role': 'user', 'content': full_prompt}],
                stream=True
            ):
                if chunk.message.content:
                    yield chunk.message.content
        except Exception as e:
            logging.error(f"LLMService streaming error: {e}")
            yield f"Error: {e}"

    async def chat(self, history):
        self._load_config()
        try:
            response = await self.client.chat(
                model=self.model,
                messages=history,
                stream=False
            )
            return response.message.content
        except Exception as e:
            logging.error(f"LLMService chat error: {e}")
            return f"Error: Could not chat with LLM. {e}"

    async def stream_chat(self, history):
        self._load_config()
        try:
            async for chunk in await self.client.chat(
                model=self.model,
                messages=history,
                stream=True
            ):
                if chunk.message.content:
                    yield chunk.message.content
        except Exception as e:
            logging.error(f"LLMService streaming chat error: {e}")
            yield f"Error: {e}"

llm_service = LLMService()
