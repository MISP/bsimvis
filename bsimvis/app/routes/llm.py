import asyncio
from flask import request, Response
from bsimvis.app.services.llm_service import llm_service
from bsimvis.app.services.function_service import fetch_function_data
import logging

def get_code_for_llm(func_id):
    """Helper to fetch raw code for a function ID."""
    try:
        parts = func_id.split(":")
        if len(parts) < 4:
            return None, "Invalid ID format"

        if parts[0] == "idx":
            collection = parts[1]
            md5 = parts[3]
            addr = parts[4]
        else:
            collection = parts[0]
            md5 = parts[2]
            addr = parts[3]

        source, _, meta, _ = fetch_function_data(collection, md5, addr)
        if not source:
            return None, "Function not found"

        # Use raw decompiled lines if available
        c_lines = source.get("c_lines")
        if c_lines:
            code = "\n".join(c_lines)
        else:
            # Fallback to reconstructing from tokens if c_lines is missing
            tokens = source.get("c_tokens", [])
            if not tokens:
                return None, "No tokens or lines found"
            
            # Group tokens by line
            max_line = max(t["line"] for t in tokens)
            lines = [[] for _ in range(max_line + 1)]
            for t in tokens:
                lines[t["line"]].append(t["t"])
            code = "\n".join(["".join(line_tokens) for line_tokens in lines])

        func_name = meta.get("function_name", "unknown") if meta else "unknown"
        
        return {"code": code, "func_name": func_name}, None
    except Exception as e:
        return None, str(e)

def summarize():
    data = request.json
    func_id = data.get("func_id")
    custom_prompt = data.get("prompt")
    code = data.get("code")
    func_name = data.get("func_name")

    if not code and func_id:
        res, error = get_code_for_llm(func_id)
        if error:
            return {"error": error}, 400
        code = res["code"]
        func_name = res["func_name"]

    if not code:
        return {"error": "Missing code or func_id"}, 400

    def generate():
        # Create a new event loop for this thread to run the async generator
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async_gen = llm_service.stream_summarize_function(func_name or "unknown", code, custom_prompt)
        
        try:
            while True:
                try:
                    chunk = loop.run_until_complete(async_gen.__anext__())
                    yield chunk
                except StopAsyncIteration:
                    break
        finally:
            loop.close()

    return Response(generate(), mimetype='text/plain')

def chat():
    data = request.json
    messages = data.get("messages", [])
    if not messages:
        return {"error": "Missing messages"}, 400
        
    def generate():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async_gen = llm_service.stream_chat(messages)
        
        try:
            while True:
                try:
                    chunk = loop.run_until_complete(async_gen.__anext__())
                    yield chunk
                except StopAsyncIteration:
                    break
        finally:
            loop.close()

    return Response(generate(), mimetype='text/plain')
