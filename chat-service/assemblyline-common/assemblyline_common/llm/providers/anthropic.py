"""
Anthropic Claude LLM Provider
=============================
Cloud LLM provider using Anthropic API.
"""

import os
import json
import time
import logging
import httpx
from typing import AsyncIterator, List, Optional, Dict, Any

from ..provider_interface import (
    BaseLLMProvider,
    LLMProvider,
    LLMRequest,
    LLMResponse,
    PROVIDER_CAPABILITIES,
)

logger = logging.getLogger(__name__)


class AnthropicProvider(BaseLLMProvider):
    """
    Anthropic Claude provider.

    Features:
    - Claude 3.5 Sonnet, Haiku, Opus
    - Vision support
    - Function calling (tool use)
    - Best-in-class reasoning
    - 200K context window
    """

    provider_type = LLMProvider.ANTHROPIC
    BASE_URL = "https://api.anthropic.com/v1"
    API_VERSION = "2023-06-01"

    DEFAULT_MODELS = {
        "default": "claude-3-5-sonnet-20241022",
        "quality": "claude-3-opus-20240229",
        "fast": "claude-3-5-haiku-20241022",
        "vision": "claude-3-5-sonnet-20241022",
        "reasoning": "claude-3-opus-20240229",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 120.0,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        self.base_url = base_url or os.getenv("ANTHROPIC_BASE_URL", self.BASE_URL)
        self.default_model = default_model or os.getenv("ANTHROPIC_MODEL", self.DEFAULT_MODELS["default"])
        self.timeout = timeout

        if not self.api_key:
            logger.warning("Anthropic API key not configured")

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "x-api-key": self.api_key,
            "anthropic-version": self.API_VERSION,
            "content-type": "application/json",
        }

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response using Claude."""
        if not self.api_key:
            raise ValueError("Anthropic API key not configured")

        start_time = time.time()
        model = request.model or self._select_model(request)

        # Build messages
        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": request.max_tokens,
        }

        # Add system prompt
        if request.system_prompt:
            payload["system"] = request.system_prompt

        # Add function calling if tools provided
        if request.tools:
            payload["tools"] = self._convert_tools(request.tools)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/messages",
                headers=self._get_headers(),
                json=payload
            )
            response.raise_for_status()
            data = response.json()

        latency_ms = (time.time() - start_time) * 1000

        # Extract content
        content = ""
        tool_calls = None

        for block in data.get("content", []):
            if block.get("type") == "text":
                content += block.get("text", "")
            elif block.get("type") == "tool_use":
                if tool_calls is None:
                    tool_calls = []
                tool_calls.append({
                    "id": block.get("id"),
                    "type": "function",
                    "function": {
                        "name": block.get("name"),
                        "arguments": json.dumps(block.get("input", {}))
                    }
                })

        # Calculate cost
        usage = data.get("usage", {})
        cost = self.calculate_cost(
            usage.get("input_tokens", 0),
            usage.get("output_tokens", 0),
            model
        )

        return LLMResponse(
            content=content,
            provider=LLMProvider.ANTHROPIC,
            model=model,
            tokens_used=usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            tokens_prompt=usage.get("input_tokens", 0),
            tokens_completion=usage.get("output_tokens", 0),
            latency_ms=latency_ms,
            cost_usd=cost,
            finish_reason=data.get("stop_reason", "stop"),
            tool_calls=tool_calls,
        )

    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream a response token by token."""
        if not self.api_key:
            raise ValueError("Anthropic API key not configured")

        model = request.model or self._select_model(request)
        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": request.max_tokens,
            "stream": True,
        }

        if request.system_prompt:
            payload["system"] = request.system_prompt

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/messages",
                headers=self._get_headers(),
                json=payload
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data_str = line[6:]
                        try:
                            data = json.loads(data_str)
                            if data.get("type") == "content_block_delta":
                                text = data.get("delta", {}).get("text", "")
                                if text:
                                    yield text
                        except json.JSONDecodeError:
                            continue

    async def health_check(self) -> bool:
        """Check if Anthropic is available."""
        if not self.api_key:
            return False

        try:
            # Anthropic doesn't have a dedicated health endpoint
            # We'll do a minimal request to check
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.base_url}/messages",
                    headers=self._get_headers(),
                    json={
                        "model": "claude-3-5-haiku-20241022",
                        "messages": [{"role": "user", "content": "Hi"}],
                        "max_tokens": 1,
                    }
                )
                # 200 = success, 401 = bad key, 429 = rate limited (but working)
                return response.status_code in [200, 429]
        except Exception as e:
            logger.debug(f"Anthropic health check failed: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """List available models."""
        return list(PROVIDER_CAPABILITIES.get(LLMProvider.ANTHROPIC, {}).keys())

    def _build_messages(self, request: LLMRequest) -> List[Dict[str, Any]]:
        """Build messages array for Claude."""
        messages: List[Dict[str, Any]] = []

        # Use existing messages if provided
        if request.messages:
            for msg in request.messages:
                # Skip system messages (handled separately)
                if msg.get("role") == "system":
                    continue
                messages.append(dict(msg))
        else:
            # Build user message
            content: Any = request.prompt

            # Add context
            if request.context:
                context_text = "\n\n".join(request.context)
                content = f"Context:\n{context_text}\n\nQuestion: {request.prompt}"

            # Add images for vision
            if request.images:
                content = [
                    {"type": "text", "text": content if isinstance(content, str) else request.prompt}
                ]
                for img in request.images:
                    content.append({
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": img
                        }
                    })

            messages.append({"role": "user", "content": content})

        return messages

    def _convert_tools(self, tools: List[Dict]) -> List[Dict]:
        """Convert OpenAI tool format to Anthropic format."""
        anthropic_tools = []
        for tool in tools:
            if tool.get("type") == "function":
                func = tool.get("function", {})
                anthropic_tools.append({
                    "name": func.get("name"),
                    "description": func.get("description", ""),
                    "input_schema": func.get("parameters", {})
                })
        return anthropic_tools

    def _select_model(self, request: LLMRequest) -> str:
        """Select the best model based on request."""
        if request.task_type == "vision" or request.images:
            return self.DEFAULT_MODELS["vision"]
        elif request.task_type == "reasoning":
            return self.DEFAULT_MODELS["reasoning"]
        elif request.task_type == "fast":
            return self.DEFAULT_MODELS["fast"]
        elif request.task_type == "quality":
            return self.DEFAULT_MODELS["quality"]

        return self.default_model
