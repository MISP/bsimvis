"""MCP server exposing bsimvis's read-only analysis tools over stdio.

Reuses `llm_tools.TOOLS` (schema) and `llm_tools.call_tool` (dispatch) as-is --
the same tool layer the in-app chat agent already uses via
`analysis_orchestrator` / `llm_chat_service`. No separate schema or
dispatcher to keep in sync.

Runs as its own process (no Flask/gunicorn), same standalone-app pattern as
`worker.py`. Point an MCP client (Claude Desktop, Claude Code) at this
script; it talks JSON-RPC over stdin/stdout.

Launch: uv run bsimvis/mcp_server.py
"""

import asyncio
import json
import logging
import sys

from dotenv import load_dotenv

load_dotenv()

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from bsimvis.app.services import llm_tools

# stdout is the JSON-RPC transport -- all logging must go to stderr.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

server = Server("bsimvis")


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name=t["function"]["name"],
            description=t["function"]["description"],
            inputSchema=t["function"]["parameters"],
        )
        for t in llm_tools.TOOLS
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    result = llm_tools.call_tool(name, arguments)
    return [TextContent(type="text", text=json.dumps(result))]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
