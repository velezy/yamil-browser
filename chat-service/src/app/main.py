"""
YAMIL Browser Chat Service (Standalone)
Conversation management, streaming responses, voice chat, browser AI sidebar.
Adapted from AssemblyLine chat-service for standalone desktop use.
"""
import os
import sys
import httpx
from datetime import datetime
from typing import Optional, List, AsyncGenerator
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import asyncio
import json
import logging
import base64
import jwt

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
# Also add the chat service directory for voice module imports
chat_service_dir = os.path.join(os.path.dirname(__file__), '..')
if chat_service_dir not in sys.path:
    sys.path.insert(0, chat_service_dir)

# Import shared config
try:
    from assemblyline_common.config import config
except ImportError:
    # Minimal config fallback for standalone mode
    class _StandaloneConfig:
        JWT_SECRET = os.getenv("JWT_SECRET", "yamil-browser-local")
        JWT_ALGORITHM = "HS256"
        ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", "http://localhost:8024")
        RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL", "http://rag-service:8022")
        AUDIT_SERVICE_URL = ""
    config = _StandaloneConfig()

# JWT configuration from shared config
JWT_SECRET = config.JWT_SECRET
JWT_ALGORITHM = config.JWT_ALGORITHM

# Standalone mode: skip auth for local desktop use
SKIP_AUTH = os.getenv("SKIP_AUTH", "false").lower() == "true"

# Initialize observability (tracing, metrics, structured logging, PII masking)
try:
    from assemblyline_common.observability import get_logger
    logger = get_logger(__name__)
    OBSERVABILITY_AVAILABLE = True
except ImportError:
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    OBSERVABILITY_AVAILABLE = False

# Database imports
try:
    from assemblyline_common.database import (
        get_db_pool,
        close_db_pool,
        initialize_schema,
        ConversationRepository,
        MessageRepository,
    )
    DB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Database module not available: {e}")
    DB_AVAILABLE = False

# Auth imports for multi-tenant isolation
# In standalone mode (SKIP_AUTH=true), provide default user for local desktop use
if SKIP_AUTH:
    AUTH_AVAILABLE = True
    def require_user_id(request: Request) -> int:
        return 1  # Default local user
    def require_user_info(request: Request) -> dict:
        return {"user_id": 1, "organization_id": 1, "email": "local@yamil-browser"}
else:
    try:
        from assemblyline_common.auth import require_user_id, require_user_info
        AUTH_AVAILABLE = True
    except ImportError:
        AUTH_AVAILABLE = False
        def require_user_id(request: Request) -> int:
            return 1
        def require_user_info(request: Request) -> dict:
            return {"user_id": 1, "organization_id": 1}

# Voice module imports
try:
    from app.voice import (
        SSEStreamManager,
        SSEEventType,
        get_stt_service,
        get_tts_service,
        AVAILABLE_VOICES,
        get_streaming_pipeline,
    )
    from app.voice.websocket import VoiceChatHandler, create_voice_chat_handler
    VOICE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Voice module not available: {e}")
    VOICE_AVAILABLE = False
    logger.info(f"Voice import path debug: {sys.path[:3]}")

# Summarization import for conversation context compression
SUMMARIZATION_AVAILABLE = False
_summarization_agent = None
SUMMARIZATION_THRESHOLD = int(os.getenv("SUMMARIZATION_THRESHOLD", "20"))

async def get_summarization_agent():
    """Get or create summarization agent instance."""
    global _summarization_agent, SUMMARIZATION_AVAILABLE
    if _summarization_agent is None:
        try:
            # Add orchestrator path
            orch_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'orchestrator')
            if orch_path not in sys.path:
                sys.path.insert(0, orch_path)
            from app.agents.summarization_agent import get_summarization_agent as get_agent
            _summarization_agent = get_agent()
            SUMMARIZATION_AVAILABLE = True
            logger.info("✅ Summarization agent loaded")
        except ImportError as e:
            logger.warning(f"Summarization agent not available: {e}")
            SUMMARIZATION_AVAILABLE = False
    return _summarization_agent


async def get_conversation_history_with_summary(
    conversation_id: int,
    messages: list,
    threshold: int = None
) -> list:
    """
    Get conversation history, using summary for long conversations.

    If message count > threshold:
      - Get or generate summary of older messages
      - Return: [summary_message] + last 5 messages
    Otherwise:
      - Return: last 10 messages as normal
    """
    if threshold is None:
        threshold = SUMMARIZATION_THRESHOLD

    if len(messages) <= threshold:
        # Short conversation - use last 10 messages
        return [{"role": m.role, "content": m.content} for m in messages[-10:]]

    # Long conversation - try to use summary
    agent = await get_summarization_agent()
    if not agent:
        # Fallback to last 10 messages
        return [{"role": m.role, "content": m.content} for m in messages[-10:]]

    try:
        # Try to get existing summary
        summary_data = await agent.get_current_summary(conversation_id)

        if summary_data and summary_data.get("summary_text"):
            summary_text = summary_data["summary_text"]
        else:
            # Generate new summary
            message_list = [{"role": m.role, "content": m.content} for m in messages[:-5]]
            summary_result = await agent.process(
                query="",
                conversation_id=conversation_id,
                messages=message_list
            )
            summary_text = summary_result.get("summary_text", "")

        if summary_text:
            # Return summary + last 5 messages
            history = [
                {"role": "system", "content": f"[Summary of earlier conversation: {summary_text}]"}
            ]
            history.extend([{"role": m.role, "content": m.content} for m in messages[-5:]])
            logger.info(f"Using summary + last 5 messages for conversation {conversation_id} ({len(messages)} total messages)")
            return history
    except Exception as e:
        logger.warning(f"Summary generation failed: {e}")

    # Fallback to last 10 messages
    return [{"role": m.role, "content": m.content} for m in messages[-10:]]


# Rate limiting import
RATE_LIMITING_AVAILABLE = False
try:
    from assemblyline_common.utils.dragonfly_cache import check_rate_limit
    RATE_LIMITING_AVAILABLE = True
    logger.info("✅ Rate limiting loaded")
except ImportError as e:
    logger.warning(f"Rate limiting not available: {e}")
    async def check_rate_limit(user_id, ip_address, endpoint, max_requests=None):
        return True, {}


# Guardrails import
GUARDRAILS_AVAILABLE = False
try:
    # Add RAG service path for guardrails
    rag_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'rag')
    if rag_path not in sys.path:
        sys.path.insert(0, rag_path)

    from app.utils.guardrails import (
        get_guardrails_engine,
        check_input_safety,
        check_output_safety,
        should_block_request,
        get_safe_response,
        GuardrailResult,
        ThreatCategory,
        RiskLevel,
    )
    GUARDRAILS_AVAILABLE = True
    logger.info("✅ Guardrails module loaded")
except ImportError as e:
    logger.warning(f"Guardrails module not available: {e}")

    # Provide fallback stubs
    async def check_input_safety(text, user_id=None):
        class FakeResult:
            is_safe = True
            threat_category = None
            risk_level = None
            suggested_response = None
        return FakeResult()

    async def check_output_safety(text):
        class FakeResult:
            is_safe = True
        return FakeResult()

    def should_block_request(result):
        return False

    def get_safe_response(result):
        return "I can't help with that request."


# Lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup"""
    if DB_AVAILABLE:
        try:
            await get_db_pool()
            await initialize_schema()
            logger.info("✅ Database connected")
        except Exception as e:
            logger.warning(f"⚠️ Database not available: {e}")
    yield
    if DB_AVAILABLE:
        try:
            await close_db_pool()
        except Exception:
            pass


app = FastAPI(
    title="YAMIL Browser Chat Service",
    description="AI sidebar, conversation management, streaming responses, voice I/O",
    version="3.0.0",
    lifespan=lifespan
)

# ── CORS middleware (Electron renderer needs cross-origin access) ──────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Mount browser chat endpoint (direct LLMRouter, no orchestrator needed) ──
try:
    from app.browser_chat import router as browser_chat_router
    app.include_router(browser_chat_router)
    logger.info("Browser chat endpoint mounted at /browser-chat")
except ImportError as e:
    logger.warning(f"Browser chat endpoint not available: {e}")

# Add API versioning middleware
try:
    from assemblyline_common.api.versioning import APIVersionMiddleware, VersionedAPIRouter
    app.add_middleware(APIVersionMiddleware, default_version="1")
    API_VERSIONING_AVAILABLE = True
    logger.info("API versioning middleware added")
except ImportError as e:
    logger.warning(f"API versioning not available: {e}")
    API_VERSIONING_AVAILABLE = False

# Add Prometheus metrics endpoint and middleware
if OBSERVABILITY_AVAILABLE:
    try:
        from assemblyline_common.observability.prometheus_metrics import (
            PrometheusMiddleware, create_metrics_endpoint
        )
        app.add_middleware(PrometheusMiddleware, service_name="chat")
        create_metrics_endpoint(app)
        logger.info("Prometheus /metrics endpoint added to Chat service")
    except ImportError as e:
        logger.warning(f"Could not add Prometheus metrics: {e}")

# Service URLs from shared config (orchestrator/RAG are sibling containers)
ORCHESTRATOR_URL = os.getenv("ORCHESTRATOR_URL", getattr(config, "ORCHESTRATOR_URL", "http://localhost:8024"))
RAG_SERVICE_URL = os.getenv("RAG_SERVICE_URL", getattr(config, "RAG_SERVICE_URL", "http://rag-service:8022"))
AUDIT_SERVICE_URL = os.getenv("AUDIT_SERVICE_URL", getattr(config, "AUDIT_SERVICE_URL", ""))


# Schemas
class ChatRequest(BaseModel):
    message: str
    conversation_id: Optional[int] = None
    use_rag: bool = True
    stream: bool = True
    image: Optional[str] = None  # Base64 data URL for vision model (single, backward compat)
    images: Optional[List[str]] = None  # Multi-image support (array of base64 data URLs)
    conversation_history: Optional[List[dict]] = None  # For stateless sessions (GlobalAssistant)
    page_context: Optional[dict] = None  # Frontend page snapshot for AI context awareness


class MessageResponse(BaseModel):
    id: int
    role: str
    content: str
    sources: Optional[List[dict]] = None
    agent_used: Optional[str] = None
    model_used: Optional[str] = None
    processing_time_ms: Optional[int] = None
    created_at: datetime


class ConversationResponse(BaseModel):
    id: int
    title: Optional[str] = None
    messages: List[MessageResponse] = []
    created_at: datetime
    updated_at: datetime


def msg_to_response(msg) -> dict:
    """Convert database message to response dict"""
    if isinstance(msg, dict):
        return {
            "id": msg["id"],
            "role": msg["role"],
            "content": msg["content"],
            "sources": msg.get("sources"),
            "agent_used": msg.get("agent_used"),
            "model_used": msg.get("model_used"),
            "processing_time_ms": msg.get("processing_time_ms"),
            "created_at": msg.get("created_at", datetime.utcnow())
        }
    return {
        "id": msg.id,
        "role": msg.role,
        "content": msg.content,
        "sources": msg.sources,
        "agent_used": msg.agent_used,
        "model_used": msg.model_used,
        "processing_time_ms": msg.processing_time_ms,
        "created_at": msg.created_at
    }


@app.get("/")
async def root():
    return {
        "service": "T.A.L.O.S. Chat Service",
        "version": "2.0.0",
        "status": "running",
        "port": 17001,
        "database": "postgresql" if DB_AVAILABLE else "unavailable"
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "chat", "database": DB_AVAILABLE}


@app.post("/conversations")
async def create_conversation(request: Request, title: Optional[str] = None):
    """Create a new conversation for the current user"""
    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    user_id = require_user_id(request)
    conv = await ConversationRepository.create(title=title, user_id=user_id)
    logger.info(f"Created conversation {conv.id} for user {user_id}")

    return {
        "id": conv.id,
        "title": conv.title,
        "created_at": conv.created_at,
        "updated_at": conv.updated_at
    }


@app.get("/conversations")
async def list_conversations(request: Request):
    """List all conversations for the current user"""
    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    user_id = require_user_id(request)
    convs = await ConversationRepository.list_all(user_id)
    return [
        {
            "id": c.id,
            "title": c.title,
            "created_at": c.created_at,
            "updated_at": c.updated_at
        }
        for c in convs
    ]


@app.get("/conversations/{conversation_id}")
async def get_conversation(conversation_id: int, request: Request):
    """Get conversation with messages - user can only access their own"""
    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    user_id = require_user_id(request)
    conv = await ConversationRepository.get_by_id(conversation_id, user_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    msgs = await MessageRepository.get_by_conversation(conversation_id)

    return {
        "id": conv.id,
        "title": conv.title,
        "created_at": conv.created_at,
        "updated_at": conv.updated_at,
        "messages": [msg_to_response(m) for m in msgs]
    }


@app.delete("/conversations/{conversation_id}")
async def delete_conversation(conversation_id: int, request: Request):
    """Delete a conversation - user can only delete their own"""
    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    user_id = require_user_id(request)
    conv = await ConversationRepository.get_by_id(conversation_id, user_id)
    if not conv:
        raise HTTPException(status_code=404, detail="Conversation not found")

    await ConversationRepository.delete(conversation_id, user_id)
    return {"success": True}


def get_user_id_from_request(http_request: Request) -> Optional[int]:
    """Extract user_id from JWT token in Authorization header"""
    auth_header = http_request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]  # Remove "Bearer " prefix
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return int(payload.get("sub", 0))
    except (jwt.InvalidTokenError, ValueError):
        return None


@app.post("/send")
async def send_message(request: ChatRequest, http_request: Request):
    """Send a message and get AI response - user can only access their own conversations"""
    # Extract user info from JWT token (required for multi-tenant isolation and org-specific AI routing)
    user_info = require_user_info(http_request)
    user_id = user_info["user_id"]
    organization_id = user_info.get("organization_id")

    # ==========================================================================
    # RATE LIMITING: Check per-user rate limit (60 req/min for /send)
    # ==========================================================================
    client_ip = http_request.client.host if http_request.client else "unknown"
    allowed, rate_headers = await check_rate_limit(
        user_id=user_id,
        ip_address=client_ip,
        endpoint="/send",
        max_requests=60
    )
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please slow down.",
            headers=rate_headers
        )

    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    start_time = datetime.utcnow()

    # ==========================================================================
    # GUARDRAILS: Check input for threats
    # ==========================================================================
    guardrail_result = await check_input_safety(request.message)

    if should_block_request(guardrail_result):
        logger.warning(f"Guardrail blocked request: {guardrail_result.threat_category}")

        # Create conversation to store the blocked interaction
        if request.conversation_id is None:
            title = request.message[:50].strip()
            if len(request.message) > 50:
                title = title.rsplit(' ', 1)[0] + '...'
            conv = await ConversationRepository.create(title=title, user_id=user_id)
            conversation_id = conv.id
        else:
            # Verify user owns this conversation
            conv = await ConversationRepository.get_by_id(request.conversation_id, user_id)
            if not conv:
                raise HTTPException(status_code=404, detail="Conversation not found")
            conversation_id = request.conversation_id

        # Store user message (for audit trail)
        await MessageRepository.create(
            conversation_id=conversation_id,
            role="user",
            content=request.message
        )

        # Store safe response
        safe_response = get_safe_response(guardrail_result)
        ai_msg = await MessageRepository.create(
            conversation_id=conversation_id,
            role="assistant",
            content=safe_response,
            agent_used="guardrails",
            model_used="security_filter",
            processing_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000)
        )

        return {
            "message": msg_to_response(ai_msg),
            "conversation_id": conversation_id,
            "sources": None,
            "guardrail_triggered": True,
            "threat_category": guardrail_result.threat_category.value if hasattr(guardrail_result.threat_category, 'value') else str(guardrail_result.threat_category)
        }
    # ==========================================================================

    # Create conversation if needed
    if request.conversation_id is None:
        # Generate title from first message (truncate to 50 chars)
        title = request.message[:50].strip()
        if len(request.message) > 50:
            title = title.rsplit(' ', 1)[0] + '...'
        conv = await ConversationRepository.create(title=title, user_id=user_id)
        conversation_id = conv.id
    else:
        # Verify user owns this conversation
        conv = await ConversationRepository.get_by_id(request.conversation_id, user_id)
        if not conv:
            raise HTTPException(status_code=404, detail="Conversation not found")
        conversation_id = request.conversation_id

    # Store user message
    user_msg = await MessageRepository.create(
        conversation_id=conversation_id,
        role="user",
        content=request.message
    )

    # Get RAG context if enabled
    context = []
    if request.use_rag:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                rag_response = await client.post(
                    f"{RAG_SERVICE_URL}/search",
                    json={"query": request.message, "top_k": 5}
                )
                if rag_response.status_code == 200:
                    context = rag_response.json().get("results", [])
        except Exception as e:
            logger.warning(f"RAG service unavailable: {e}")

    # Get AI response from orchestrator
    ai_content = "I apologize, I'm having trouble processing your request right now."
    agent_used = None
    model_used = None

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # Get conversation history (with summary for long conversations)
            conv_messages = await MessageRepository.get_by_conversation(conversation_id)
            history = await get_conversation_history_with_summary(conversation_id, conv_messages)

            orch_response = await client.post(
                f"{ORCHESTRATOR_URL}/process",
                json={
                    "query": request.message,
                    "context": context,
                    "conversation_history": history,
                    "user_id": user_id,  # Pass user_id for memory learning
                    "organization_id": organization_id,  # Pass org_id for org-specific AI provider routing
                    "metadata": {
                        "conversation_id": conversation_id,
                        "message_id": user_msg.id if user_msg else None
                    }
                }
            )

            if orch_response.status_code == 200:
                result = orch_response.json()
                ai_content = result.get("response", "I apologize, I couldn't generate a response.")
                agent_used = result.get("agent_used", "unknown")
                model_used = result.get("model_used", "unknown")

    except Exception as e:
        logger.error(f"Orchestrator error: {e}")

    # ==========================================================================
    # GUARDRAILS: Check output for sensitive information leakage
    # ==========================================================================
    output_check = await check_output_safety(ai_content)
    if not output_check.is_safe:
        logger.warning(f"Guardrail filtered output: potential {output_check.threat_category if hasattr(output_check, 'threat_category') else 'sensitive'} leakage")
        ai_content = "I generated a response but it was filtered for security reasons. Please rephrase your question."
        agent_used = "guardrails_output_filter"
    # ==========================================================================

    # Calculate processing time
    processing_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    # Store AI response
    ai_msg = await MessageRepository.create(
        conversation_id=conversation_id,
        role="assistant",
        content=ai_content,
        sources=context[:3] if context else None,
        agent_used=agent_used,
        model_used=model_used,
        processing_time_ms=processing_time
    )

    logger.info(f"Chat response generated in {processing_time}ms using {agent_used}")

    return {
        "message": msg_to_response(ai_msg),
        "conversation_id": conversation_id,
        "sources": context[:3] if context else None
    }


@app.post("/send/stream")
async def send_message_stream(request: ChatRequest, http_request: Request):
    """Send a message and stream the AI response - user can only access their own conversations"""

    # Extract user info (required for multi-tenant isolation and org-specific AI routing)
    user_info = require_user_info(http_request)
    user_id = user_info["user_id"]
    organization_id = user_info.get("organization_id")

    # ==========================================================================
    # RATE LIMITING: Check per-user rate limit (30 req/min for streaming)
    # ==========================================================================
    client_ip = http_request.client.host if http_request.client else "unknown"
    allowed, rate_headers = await check_rate_limit(
        user_id=user_id,
        ip_address=client_ip,
        endpoint="/send/stream",
        max_requests=30
    )
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please slow down.",
            headers=rate_headers
        )

    async def generate():
        import aiohttp
        start_time = datetime.utcnow()
        conversation_id = request.conversation_id

        # ==========================================================================
        # GUARDRAILS: Check input for threats
        # ==========================================================================
        guardrail_result = await check_input_safety(request.message)

        if should_block_request(guardrail_result):
            logger.warning(f"Guardrail blocked streaming request: {guardrail_result.threat_category}")
            safe_response = get_safe_response(guardrail_result)

            # Create conversation to store the blocked interaction
            if request.conversation_id is None:
                title = request.message[:50].strip()
                if len(request.message) > 50:
                    title = title.rsplit(' ', 1)[0] + '...'
                conv = await ConversationRepository.create(title=title, user_id=user_id)
                conversation_id = conv.id
            else:
                # Verify user owns this conversation
                conv = await ConversationRepository.get_by_id(request.conversation_id, user_id)
                if not conv:
                    yield f"data: {json.dumps({'type': 'error', 'message': 'Conversation not found'})}\n\n"
                    return
                conversation_id = request.conversation_id

            # Store user message
            await MessageRepository.create(
                conversation_id=conversation_id,
                role="user",
                content=request.message
            )

            # Store blocked response
            await MessageRepository.create(
                conversation_id=conversation_id,
                role="assistant",
                content=safe_response,
                agent_used="guardrails",
                model_used="security_filter",
                processing_time_ms=int((datetime.utcnow() - start_time).total_seconds() * 1000)
            )

            # Stream the safe response
            for char in safe_response:
                yield f"data: {json.dumps({'type': 'token', 'content': char})}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'response': safe_response, 'conversation_id': conversation_id})}\n\n"
            return
        # ==========================================================================

        # Create conversation if needed
        if request.conversation_id is None:
            title = request.message[:50].strip()
            if len(request.message) > 50:
                title = title.rsplit(' ', 1)[0] + '...'
            conv = await ConversationRepository.create(title=title, user_id=user_id)
            conversation_id = conv.id
        else:
            # Verify user owns this conversation
            conv = await ConversationRepository.get_by_id(request.conversation_id, user_id)
            if not conv:
                yield f"data: {json.dumps({'type': 'error', 'message': 'Conversation not found'})}\n\n"
                return
            conversation_id = request.conversation_id

        # Store user message
        user_msg = await MessageRepository.create(
            conversation_id=conversation_id,
            role="user",
            content=request.message
        )

        # Get conversation history (with summary for long conversations)
        # For stateless sessions (GlobalAssistant), use passed history; otherwise fetch from DB
        if request.conversation_history:
            # Use directly passed conversation history (for stateless GlobalAssistant sessions)
            history = request.conversation_history
            logger.info(f"Using passed conversation history: {len(history)} messages")
        else:
            # Fetch from database for persistent conversations
            conv_messages = await MessageRepository.get_by_conversation(conversation_id)
            history = await get_conversation_history_with_summary(conversation_id, conv_messages)

        full_response = ""
        agent_used = "unknown"
        model_used = "unknown"
        sources = []  # RAG source documents

        try:
            # Use aiohttp for streaming connection to orchestrator
            async with aiohttp.ClientSession() as session:
                # Extract JWT for MCP providers that need auth (e.g. canvas_vision)
                auth_header = http_request.headers.get("Authorization", "")
                jwt_token = auth_header[7:] if auth_header.startswith("Bearer ") else None

                payload = {
                    "query": request.message,
                    "context": [],
                    "conversation_history": history,
                    "use_workflow": True,
                    "use_rag": request.use_rag,  # Pass RAG toggle to orchestrator
                    "user_id": user_id,
                    "organization_id": organization_id,  # For org-specific AI provider routing
                    "metadata": {
                        "conversation_id": conversation_id,
                        "message_id": user_msg.id if user_msg else None
                    }
                }
                # Include auth token for MCP operations
                if jwt_token:
                    payload["auth_token"] = jwt_token
                # Include images for vision model if provided
                # Prefer images array, fall back to single image
                effective_images = request.images or ([request.image] if request.image else None)
                if effective_images:
                    payload["images"] = effective_images
                    payload["image"] = effective_images[0]  # backward compat
                # Include page context for AI awareness of user's current page
                if request.page_context:
                    payload["page_context"] = request.page_context

                async with session.post(
                    f"{ORCHESTRATOR_URL}/process/stream",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=600, sock_read=120)
                ) as response:
                    if response.status != 200:
                        raise Exception(f"Orchestrator returned {response.status}")

                    # Stream SSE events from orchestrator
                    buffer = ""
                    async for chunk in response.content.iter_any():
                        if chunk:
                            buffer += chunk.decode('utf-8')
                            while '\n\n' in buffer:
                                event, buffer = buffer.split('\n\n', 1)
                                if event.startswith('data: '):
                                    data = event[6:]
                                    try:
                                        parsed = json.loads(data)
                                        event_type = parsed.get('type')

                                        if event_type == 'token':
                                            content = parsed.get('content', '')
                                            full_response += content
                                            yield f"data: {json.dumps({'type': 'token', 'content': content})}\n\n"

                                        elif event_type == 'done':
                                            full_response = parsed.get('response', full_response)
                                            agent_used = parsed.get('agent', 'coordinator')
                                            model_used = parsed.get('model', 'lrm-pipeline')
                                            sources = parsed.get('sources', [])  # Capture RAG sources
                                            # Don't yield done yet - we'll do it after saving

                                        elif event_type == 'error':
                                            yield f"data: {json.dumps(parsed)}\n\n"
                                            return

                                    except json.JSONDecodeError:
                                        continue

        except Exception as e:
            logger.error(f"Streaming orchestrator error: {e}")
            # Fallback to non-streaming
            try:
                async with httpx.AsyncClient(timeout=120.0) as client:
                    orch_response = await client.post(
                        f"{ORCHESTRATOR_URL}/process",
                        json={
                            "query": request.message,
                            "context": [],
                            "conversation_history": history,
                            "user_id": user_id,
                            "organization_id": organization_id  # For org-specific AI provider routing
                        }
                    )
                    if orch_response.status_code == 200:
                        result = orch_response.json()
                        full_response = result.get("response", "I apologize, I couldn't generate a response.")
                        agent_used = result.get("agent_used", "unknown")
                        model_used = result.get("model_used", "unknown")

                        # Stream the fallback response
                        for i in range(0, len(full_response), 3):
                            chunk = full_response[i:i+3]
                            yield f"data: {json.dumps({'type': 'token', 'content': chunk})}\n\n"
            except Exception as e2:
                logger.error(f"Fallback orchestrator error: {e2}")
                full_response = "I apologize, I'm having trouble processing your request right now."

        # Calculate processing time
        processing_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

        # Store AI response
        await MessageRepository.create(
            conversation_id=conversation_id,
            role="assistant",
            content=full_response,
            agent_used=agent_used,
            model_used=model_used,
            processing_time_ms=processing_time
        )

        logger.info(f"Streaming chat response generated in {processing_time}ms using {agent_used}")

        # Send done event with full response, sources, and conversation_id
        yield f"data: {json.dumps({'type': 'done', 'response': full_response, 'sources': sources, 'conversation_id': conversation_id})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream"
    )


@app.get("/stats")
async def get_stats():
    """Get chat service statistics"""
    if not DB_AVAILABLE:
        return {"error": "Database not available"}

    from assemblyline_common.database import get_connection

    async with get_connection() as conn:
        conv_count = await conn.fetchval("SELECT COUNT(*) FROM conversations")
        msg_count = await conn.fetchval("SELECT COUNT(*) FROM messages")
        avg_time = await conn.fetchval(
            "SELECT AVG(processing_time_ms) FROM messages WHERE processing_time_ms IS NOT NULL"
        )

    return {
        "total_conversations": conv_count,
        "total_messages": msg_count,
        "avg_processing_time_ms": float(avg_time) if avg_time else 0,
        "database": "postgresql",
        "voice_available": VOICE_AVAILABLE
    }


# =============================================================================
# SIMPLE CHAT ENDPOINT (No conversation, no guardrails)
# =============================================================================

class SimpleChatRequest(BaseModel):
    message: str
    model: Optional[str] = None
    max_tokens: Optional[int] = 500


@app.post("/chat/simple")
async def simple_chat(request: SimpleChatRequest, http_request: Request):
    """Simple chat endpoint for internal tools (query generation, etc.)

    This endpoint:
    - Does NOT create conversations or store messages
    - Does NOT apply guardrails (for internal use only)
    - Returns a simple response
    """
    # Require authentication
    try:
        require_user_id(http_request)
    except Exception:
        # Allow internal calls without auth (from gateway)
        pass

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/process",
                json={
                    "query": request.message,
                    "context": [],
                    "conversation_history": [],
                    "model": request.model,
                    "max_tokens": request.max_tokens
                }
            )

            if response.status_code == 200:
                result = response.json()
                return {
                    "response": result.get("response", ""),
                    "content": result.get("response", ""),  # Alias for compatibility
                    "model": result.get("model_used", request.model)
                }
            else:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Orchestrator error: {response.text}"
                )
    except httpx.RequestError as e:
        raise HTTPException(status_code=503, detail=f"Service unavailable: {e}")


# =============================================================================
# VOICE CHAT ENDPOINTS
# =============================================================================

@app.get("/voice/status")
async def voice_status():
    """Get voice service status"""
    if not VOICE_AVAILABLE:
        return {
            "available": False,
            "message": "Voice module not installed. Install faster-whisper and kokoro."
        }

    stt = get_stt_service()
    tts = get_tts_service()

    return {
        "available": True,
        "stt": stt.get_info(),
        "tts": tts.get_info(),
        "voices": AVAILABLE_VOICES
    }


@app.get("/voice/voices")
async def list_voices():
    """List available TTS voices"""
    if not VOICE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Voice module not available")

    return {
        "voices": AVAILABLE_VOICES,
        "default": "af_heart"
    }


@app.get("/voice/streaming-providers")
async def streaming_providers_status():
    """
    Get available streaming TTS providers.

    Returns info about which providers are available:
    - Kokoro-82M (open source, fast, high quality)
    - Coqui XTTS (open source, highest quality)
    - ElevenLabs (commercial, lowest latency)
    - Edge TTS (free, always available)
    """
    try:
        from app.voice.streaming_providers import (
            StreamingConfig,
            KokoroStreamingProvider,
            CoquiStreamingProvider,
            ElevenLabsStreamingProvider,
            auto_select_provider,
        )
        import os

        providers = []
        config = StreamingConfig()

        # Check Kokoro
        kokoro = KokoroStreamingProvider(config)
        kokoro_available = await kokoro._check_availability()
        providers.append({
            "id": "kokoro",
            "name": "Kokoro-82M",
            "description": "Open source, fast, high quality (Apache 2.0)",
            "available": kokoro_available,
            "streaming": True,
            "url": config.kokoro_url,
        })

        # Check Coqui
        coqui = CoquiStreamingProvider(config)
        coqui_available = await coqui._check_availability()
        providers.append({
            "id": "coqui",
            "name": "Coqui XTTS",
            "description": "Open source, highest quality",
            "available": coqui_available,
            "streaming": True,
            "url": config.coqui_url,
        })

        # Check ElevenLabs
        elevenlabs = ElevenLabsStreamingProvider(config)
        providers.append({
            "id": "elevenlabs",
            "name": "ElevenLabs",
            "description": "Commercial, lowest latency (~200ms)",
            "available": elevenlabs.is_available,
            "streaming": True,
            "requires_api_key": True,
        })

        # Edge TTS (always available)
        providers.append({
            "id": "edge",
            "name": "Edge TTS",
            "description": "Free Microsoft neural voices (fallback)",
            "available": True,
            "streaming": False,  # Chunked HTTP, not true streaming
        })

        # Auto-select best provider
        best_provider = await auto_select_provider(config)
        selected = best_provider.get_info()

        return {
            "providers": providers,
            "selected": selected,
            "true_streaming_available": any(p["available"] and p["streaming"] for p in providers if p["id"] != "edge"),
        }

    except Exception as e:
        logger.error(f"Error checking streaming providers: {e}")
        return {
            "providers": [{
                "id": "edge",
                "name": "Edge TTS",
                "description": "Free Microsoft neural voices",
                "available": True,
                "streaming": False,
            }],
            "selected": {"provider": "edge"},
            "true_streaming_available": False,
            "error": str(e),
        }


class TranscribeRequest(BaseModel):
    audio: str  # Base64 encoded audio
    language: Optional[str] = None


@app.post("/voice/transcribe")
async def transcribe_audio(request: TranscribeRequest):
    """
    Transcribe audio to text.

    Send base64-encoded audio (WAV or PCM format).
    """
    if not VOICE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Voice module not available")

    try:
        audio_bytes = base64.b64decode(request.audio)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 audio data")

    stt = get_stt_service()
    result = await stt.transcribe(audio_bytes, language=request.language)

    return {
        "text": result.text,
        "language": result.language,
        "duration": result.duration,
        "confidence": result.confidence,
        "segments": result.segments
    }


class SynthesizeRequest(BaseModel):
    text: str
    voice: Optional[str] = "af_heart"
    speed: Optional[float] = 1.0
    provider: Optional[str] = None  # TTS provider: "edge" or "kokoro" (None = auto-select)
    strip_markdown: Optional[bool] = True  # Strip markdown formatting for natural speech


@app.post("/voice/synthesize")
async def synthesize_speech(request: SynthesizeRequest):
    """
    Synthesize text to speech.

    Returns audio directly as MP3 binary for efficient playback.
    Automatically strips markdown formatting (bold, italic, headers, etc.)
    for natural-sounding speech.

    Uses the best available TTS provider:
    1. Piper (if server running) - fastest synthesis, open source
    2. Kokoro-82M (if server running) - fast, high quality, open source
    3. Coqui XTTS (if server running) - highest quality
    4. Edge TTS (fallback) - free, always available
    """
    if not VOICE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Voice module not available")

    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Text is required")

    # Clean text for TTS (strip markdown like **bold**, *italic*, etc.)
    text_to_speak = request.text
    if request.strip_markdown:
        try:
            from app.voice.text_utils import clean_for_tts
            text_to_speak = clean_for_tts(request.text)
        except ImportError:
            pass  # Fallback to raw text if module not available

    # Use streaming providers - respect user's provider preference
    try:
        from app.voice.streaming_providers import (
            auto_select_provider, StreamingConfig, TTSProvider,
            get_streaming_provider, EdgeStreamingProvider, KokoroStreamingProvider,
            ChatterboxStreamingProvider, HiggsAudioStreamingProvider
        )
        config = StreamingConfig(
            voice=request.voice,
            chatterbox_url=os.getenv("CHATTERBOX_URL", "http://localhost:4123"),
            chatterbox_voice=request.voice,
            higgs_audio_url=os.getenv("HIGGS_AUDIO_URL", "http://localhost:8000"),
            higgs_audio_voice=request.voice,
        )

        # Use specified provider or auto-select
        if request.provider == "edge":
            provider = EdgeStreamingProvider(config)
        elif request.provider == "kokoro":
            provider = KokoroStreamingProvider(config)
        elif request.provider == "chatterbox":
            provider = ChatterboxStreamingProvider(config)
        elif request.provider == "higgs_audio":
            provider = HiggsAudioStreamingProvider(config)
        else:
            provider = await auto_select_provider(config)

        provider.set_voice(request.voice)

        # Collect streamed audio into single response
        # Use short timeout for Chatterbox/Higgs Audio (GPU inference can be slow for long text)
        provider_timeout = 20 if request.provider in ("chatterbox", "higgs_audio") else None

        async def _collect_audio():
            chunks = []
            async for chunk in provider.stream_audio(text_to_speak, speed=request.speed):
                chunks.append(chunk)
            return b''.join(chunks)

        import asyncio
        if provider_timeout:
            try:
                audio_data = await asyncio.wait_for(_collect_audio(), timeout=provider_timeout)
            except asyncio.TimeoutError:
                logger.warning(f"{request.provider} timed out after {provider_timeout}s, falling back to Edge TTS")
                audio_data = b''
        else:
            audio_data = await _collect_audio()

        if audio_data:
            from fastapi.responses import Response
            return Response(
                content=audio_data,
                media_type="audio/mpeg",
                headers={
                    "Content-Disposition": "inline",
                    "X-TTS-Provider": provider.get_info().get("provider", "unknown"),
                }
            )
    except Exception as e:
        logger.warning(f"Streaming provider failed, falling back to Edge TTS: {e}")

    # Fallback to Edge TTS
    tts = get_tts_service(voice=request.voice)
    result = await tts.synthesize(text_to_speak, speed=request.speed)

    from fastapi.responses import Response
    return Response(
        content=result.audio_data,
        media_type="audio/mpeg",
        headers={
            "Content-Disposition": "inline",
            "X-Audio-Duration": str(result.duration_seconds),
            "X-Sample-Rate": str(result.sample_rate)
        }
    )


# LLM generator for voice chat
async def _llm_token_generator(text: str) -> AsyncGenerator[str, None]:
    """Generate LLM response tokens"""
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                f"{ORCHESTRATOR_URL}/process",
                json={"query": text, "context": [], "conversation_history": []}
            )

            if response.status_code == 200:
                result = response.json()
                content = result.get("response", "I couldn't generate a response.")

                # Simulate streaming by yielding words
                words = content.split()
                for word in words:
                    yield word + " "
                    await asyncio.sleep(0.02)
            else:
                yield "I'm sorry, I couldn't process your request."

    except Exception as e:
        logger.error(f"LLM generation error: {e}")
        yield "I'm having trouble processing your request right now."


@app.websocket("/ws/voice-chat")
async def voice_chat_websocket(
    websocket: WebSocket,
    token: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for real-time voice chat.

    Protocol:
    Client → Server:
    - {"type": "config", "voice": "af_heart", "language": "en"}
    - {"type": "start_listening"}
    - {"type": "audio_chunk", "audio": "base64..."}
    - {"type": "stop_listening"}
    - {"type": "text_input", "text": "Question?"}
    - {"type": "interrupt"}

    Server → Client:
    - {"type": "state", "state": "idle|listening|processing|speaking"}
    - {"type": "transcript", "text": "User speech..."}
    - {"type": "token", "text": "AI"} (streaming)
    - {"type": "response", "text": "Full response", "has_audio": true}
    - {"type": "audio", "data": "base64...", "sample_rate": 24000}
    """
    if not VOICE_AVAILABLE:
        await websocket.accept()
        await websocket.send_json({
            "type": "error",
            "message": "Voice module not available"
        })
        await websocket.close()
        return

    handler = create_voice_chat_handler(llm_generator=_llm_token_generator)
    await handler.handle(websocket)


# =============================================================================
# STREAMING TTS WEBSOCKET (Ultra-low latency)
# =============================================================================

@app.websocket("/ws/streaming-tts")
async def streaming_tts_websocket(
    websocket: WebSocket,
    token: Optional[str] = Query(None)
):
    """
    WebSocket endpoint for ultra-low latency streaming TTS.

    This endpoint streams audio chunks in real-time as LLM tokens arrive,
    providing fluid, human-like speech instead of sentence-by-sentence audio.

    Protocol:
    Client → Server:
    - {"type": "config", "voice": "af_heart", "speed": 1.0}
    - {"type": "start"}  - Begin streaming session
    - {"type": "token", "text": "Hello "}  - Send LLM token
    - {"type": "end"}  - End of LLM response
    - {"type": "stop"}  - Cancel streaming

    Server → Client:
    - {"type": "ready"}  - Connection established
    - {"type": "config_updated", "voice": "...", "speed": 1.0}
    - {"type": "audio", "data": "base64...", "index": 0, "phrase": 0}  - Audio chunk
    - {"type": "complete"}  - TTS finished
    - {"type": "error", "message": "..."}

    Example integration:
    1. Connect to WebSocket when chat starts
    2. Optionally send config message with preferred voice
    3. Send {"type": "start"} when LLM response begins
    4. Forward each LLM token as {"type": "token", "text": token}
    5. Send {"type": "end"} when LLM response complete
    6. Play audio chunks as they arrive
    """
    if not VOICE_AVAILABLE:
        await websocket.accept()
        await websocket.send_json({
            "type": "error",
            "message": "Voice module not available"
        })
        await websocket.close()
        return

    try:
        from app.voice.streaming_tts import StreamingTTSWebSocket
        handler = StreamingTTSWebSocket()
        await handler.handle(websocket)
    except ImportError as e:
        await websocket.accept()
        await websocket.send_json({
            "type": "error",
            "message": f"Streaming TTS module not available: {e}"
        })
        await websocket.close()


# =============================================================================
# AI Command WebSocket — lets the AI push commands to the frontend
# (navigate, toast, highlight)
# =============================================================================
from starlette.websockets import WebSocketDisconnect

ai_command_connections: dict[str, WebSocket] = {}


@app.websocket("/ws/ai-commands")
async def ai_commands_ws(
    websocket: WebSocket,
    token: Optional[str] = Query(None)
):
    """
    WebSocket for AI-to-frontend commands.

    The AI (via orchestrator) pushes commands here; the frontend listens and acts.

    Protocol (server → client):
    - {"type": "navigate", "path": "/settings", "tab": "integrations"}
    - {"type": "toast", "message": "File System enabled", "variant": "success"}
    - {"type": "highlight", "selector": ".file-system-card", "duration": 3000}

    Client → Server:
    - {"type": "heartbeat"} — keep-alive
    """
    # Extract user_id from token or query param
    user_id = websocket.query_params.get("user_id", "anonymous")
    if token:
        try:
            payload = jwt.decode(token, options={"verify_signature": False})
            user_id = str(payload.get("user_id", user_id))
        except Exception:
            pass

    await websocket.accept()
    ai_command_connections[user_id] = websocket
    logger.info(f"AI command WebSocket connected: user={user_id}")

    try:
        while True:
            data = await websocket.receive_json()
            # Handle heartbeat — just acknowledge
            if data.get("type") == "heartbeat":
                await websocket.send_json({"type": "heartbeat_ack"})
    except WebSocketDisconnect:
        logger.info(f"AI command WebSocket disconnected: user={user_id}")
    except Exception as e:
        logger.warning(f"AI command WebSocket error: {e}")
    finally:
        ai_command_connections.pop(user_id, None)


@app.post("/ai-commands/send")
async def send_ai_command(request: Request):
    """
    REST endpoint for the orchestrator to push commands to a user's frontend.
    Called by the Navigation MCP provider.
    """
    body = await request.json()
    user_id = str(body.get("user_id", ""))
    command = body.get("command", {})

    if not user_id or not command:
        raise HTTPException(status_code=400, detail="user_id and command required")

    ws = ai_command_connections.get(user_id)
    if not ws:
        raise HTTPException(status_code=404, detail=f"No active WebSocket for user {user_id}")

    try:
        await ws.send_json(command)
        return {"success": True, "message": "Command sent"}
    except Exception as e:
        ai_command_connections.pop(user_id, None)
        raise HTTPException(status_code=500, detail=f"Failed to send command: {e}")


class StreamChatRequest(BaseModel):
    message: str
    conversation_id: Optional[int] = None
    use_rag: bool = True
    include_audio: bool = False
    voice: Optional[str] = "af_heart"


@app.post("/send/stream/sse")
async def send_message_stream_sse(request: Request, chat_request: StreamChatRequest):
    """
    Send message with SSE streaming and optional TTS.

    Uses proper SSE format with heartbeat.
    """
    if not VOICE_AVAILABLE and chat_request.include_audio:
        raise HTTPException(
            status_code=503,
            detail="Voice module not available for audio"
        )

    # Extract user info for memory learning and org-specific AI routing
    from assemblyline_common.auth import get_user_info_from_request
    user_info = get_user_info_from_request(request)
    user_id = user_info.get("user_id") if user_info else None
    organization_id = user_info.get("organization_id") if user_info else None

    async def generate_events():
        # Get response from orchestrator
        full_response = ""

        try:
            async with httpx.AsyncClient(timeout=120.0) as client:
                response = await client.post(
                    f"{ORCHESTRATOR_URL}/process",
                    json={
                        "query": chat_request.message,
                        "context": [],
                        "conversation_history": [],
                        "user_id": user_id,  # Pass user_id for memory learning
                        "organization_id": organization_id  # For org-specific AI provider routing
                    }
                )

                if response.status_code == 200:
                    result = response.json()
                    full_response = result.get("response", "")

                    # Stream tokens
                    words = full_response.split()
                    for word in words:
                        yield {"type": "token", "text": word + " "}
                        await asyncio.sleep(0.02)

        except Exception as e:
            yield {"type": "error", "message": str(e)}
            return

        # Generate TTS if requested
        if chat_request.include_audio and full_response and VOICE_AVAILABLE:
            tts = get_tts_service(voice=chat_request.voice)
            result = await tts.synthesize(full_response)

            yield {
                "type": "audio",
                "data": base64.b64encode(result.audio_data).decode(),
                "sample_rate": result.sample_rate,
                "format": "wav"
            }

    sse_manager = SSEStreamManager()
    return await sse_manager.stream_response(request, generate_events())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=17001)
