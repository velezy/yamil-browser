"""
OpenAI LLM Provider
===================
Cloud LLM provider using OpenAI API.
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


class OpenAIProvider(BaseLLMProvider):
    """
    OpenAI ChatGPT provider.

    Features:
    - GPT-4o, GPT-4o-mini, GPT-4-turbo
    - Vision support
    - Function calling
    - Streaming
    """

    provider_type = LLMProvider.OPENAI
    BASE_URL = "https://api.openai.com/v1"

    DEFAULT_MODELS = {
        "default": "gpt-4o-mini",
        "quality": "gpt-4o",
        "fast": "gpt-4o-mini",
        "vision": "gpt-4o",
        "code": "gpt-4o",
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        default_model: Optional[str] = None,
        base_url: Optional[str] = None,
        timeout: float = 120.0,
        organization: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        self.base_url = base_url or os.getenv("OPENAI_BASE_URL", self.BASE_URL)
        self.default_model = default_model or os.getenv("OPENAI_MODEL", self.DEFAULT_MODELS["default"])
        self.timeout = timeout
        self.organization = organization or os.getenv("OPENAI_ORG_ID")

        if not self.api_key:
            logger.warning("OpenAI API key not configured")

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.organization:
            headers["OpenAI-Organization"] = self.organization
        return headers

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response using OpenAI."""
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")

        start_time = time.time()
        model = request.model or self._select_model(request)

        # Build messages
        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
        }

        # Add function calling if tools provided
        if request.tools:
            payload["tools"] = request.tools
            payload["tool_choice"] = "auto"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/chat/completions",
                headers=self._get_headers(),
                json=payload
            )
            response.raise_for_status()
            data = response.json()

        latency_ms = (time.time() - start_time) * 1000

        # Extract response
        choice = data["choices"][0]
        content = choice["message"].get("content", "")
        tool_calls = None

        if choice["message"].get("tool_calls"):
            tool_calls = [tc for tc in choice["message"]["tool_calls"]]

        # Calculate cost
        usage = data.get("usage", {})
        cost = self.calculate_cost(
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
            model
        )

        return LLMResponse(
            content=content,
            provider=LLMProvider.OPENAI,
            model=model,
            tokens_used=usage.get("total_tokens", 0),
            tokens_prompt=usage.get("prompt_tokens", 0),
            tokens_completion=usage.get("completion_tokens", 0),
            latency_ms=latency_ms,
            cost_usd=cost,
            finish_reason=choice.get("finish_reason", "stop"),
            tool_calls=tool_calls,
        )

    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream a response token by token."""
        if not self.api_key:
            raise ValueError("OpenAI API key not configured")

        model = request.model or self._select_model(request)
        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": True,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/chat/completions",
                headers=self._get_headers(),
                json=payload
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data_str = line[6:]
                        if data_str == "[DONE]":
                            break
                        try:
                            data = json.loads(data_str)
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            continue

    async def health_check(self) -> bool:
        """Check if OpenAI is available."""
        if not self.api_key:
            return False

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/models",
                    headers=self._get_headers()
                )
                return response.status_code == 200
        except Exception as e:
            logger.debug(f"OpenAI health check failed: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """List available models."""
        return list(PROVIDER_CAPABILITIES.get(LLMProvider.OPENAI, {}).keys())

    def _build_messages(self, request: LLMRequest) -> List[Dict[str, Any]]:
        """Build messages array for chat completion."""
        messages: List[Dict[str, Any]] = []

        # Add system prompt
        if request.system_prompt:
            messages.append({"role": "system", "content": request.system_prompt})

        # Use existing messages if provided
        if request.messages:
            for msg in request.messages:
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
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{img}"}
                    })

            messages.append({"role": "user", "content": content})

        return messages

    def _select_model(self, request: LLMRequest) -> str:
        """Select the best model based on request."""
        if request.task_type == "vision" or request.images:
            return self.DEFAULT_MODELS["vision"]
        elif request.task_type == "code":
            return self.DEFAULT_MODELS["code"]
        elif request.task_type == "fast":
            return self.DEFAULT_MODELS["fast"]
        elif request.task_type == "quality":
            return self.DEFAULT_MODELS["quality"]

        return self.default_model
