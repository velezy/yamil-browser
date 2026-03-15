"""
Browser Chat Endpoint — Direct LLMRouter for YAMIL Browser AI Sidebar.

Replaces the orchestrator proxy with direct LLM calls.
Supports streaming (SSE) and non-streaming modes.
"""

import os
import json
import logging
from typing import Optional
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter()

# Try to import LLMRouter from assemblyline-common
try:
    from assemblyline_common.llm.router import get_llm_router
    from assemblyline_common.llm.provider_interface import LLMRequest
    LLM_AVAILABLE = True
except ImportError as e:
    logger.warning(f"LLMRouter not available: {e}")
    LLM_AVAILABLE = False


SYSTEM_PROMPT = """You are YAMIL, an intelligent browser assistant built into the YAMIL Browser.

Your capabilities:
- Answer questions about the page the user is viewing
- Help with web research, summarization, and analysis
- Remember user preferences and facts they tell you
- Assist with coding, writing, and general knowledge
- Navigate and interact with web pages when asked

Guidelines:
- Be concise and helpful
- When given page context, reference it naturally
- If the user asks to navigate somewhere, include the URL in your response
- Format responses with markdown when useful
- You have access to multiple LLM providers (Ollama local, OpenAI, Claude, Gemini, Grok)
"""


class BrowserChatRequest(BaseModel):
    message: str
    pageContext: Optional[dict] = None
    stream: bool = True
    provider: str = "auto"
    model: Optional[str] = None
    conversation_history: Optional[list] = None


@router.post("/browser-chat")
async def browser_chat(request: BrowserChatRequest):
    """
    Main endpoint for the YAMIL Browser AI sidebar.
    Routes to the best available LLM provider via LLMRouter.
    """
    if not LLM_AVAILABLE:
        return {"response": "AI service not available. Check that assemblyline-common is installed.", "model": "none"}

    # Build system prompt with page context
    system = SYSTEM_PROMPT
    if request.pageContext:
        title = request.pageContext.get("title", "")
        url = request.pageContext.get("url", "")
        text = request.pageContext.get("text", "")
        system += f"\n\nThe user is currently viewing: \"{title}\" ({url})"
        if text:
            system += f"\n\nPage content (first 3000 chars):\n{text[:3000]}"

    # Build LLM request
    llm_request = LLMRequest(
        prompt=request.message,
        system_prompt=system,
        provider=request.provider,
        model=request.model,
        prefer_local=True,
        stream=request.stream,
        messages=request.conversation_history,
    )

    if request.stream:
        return StreamingResponse(
            _stream_response(llm_request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    else:
        try:
            llm_router = await get_llm_router()
            response = await llm_router.generate(llm_request)
            return {
                "response": response.content,
                "model": response.model,
                "provider": response.provider.value,
                "latency_ms": response.latency_ms,
                "cost_usd": response.cost_usd,
            }
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return {"response": f"AI error: {str(e)}", "model": "error"}


async def _stream_response(llm_request: LLMRequest):
    """Stream SSE tokens from LLMRouter."""
    try:
        llm_router = await get_llm_router()
        full_response = ""

        async for chunk in llm_router.generate_stream(llm_request):
            full_response += chunk
            yield f"data: {json.dumps({'type': 'token', 'content': chunk})}\n\n"

        yield f"data: {json.dumps({'type': 'done', 'response': full_response})}\n\n"

    except Exception as e:
        logger.error(f"Streaming LLM error: {e}")
        yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"


@router.get("/llm/status")
async def llm_status():
    """
    Show which LLM providers are available and healthy.
    Called by the sidebar to show connection status.
    """
    if not LLM_AVAILABLE:
        return {"available": False, "providers": [], "message": "LLMRouter not installed"}

    try:
        llm_router = await get_llm_router()
        healthy = await llm_router.get_healthy_providers()
        all_providers = llm_router.get_available_providers()

        return {
            "available": len(healthy) > 0,
            "healthy": healthy,
            "configured": all_providers,
            "default_provider": llm_router.config.default_provider,
            "prefer_local": llm_router.config.prefer_local_when_possible,
            "monthly_cost_usd": llm_router.get_monthly_cost(),
        }
    except Exception as e:
        logger.error(f"LLM status check failed: {e}")
        return {"available": False, "providers": [], "error": str(e)}
