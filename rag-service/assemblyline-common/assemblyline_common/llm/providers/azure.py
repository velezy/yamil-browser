"""
Azure OpenAI LLM Provider
=========================
Enterprise cloud LLM provider using Azure OpenAI.
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
)

logger = logging.getLogger(__name__)


class AzureOpenAIProvider(BaseLLMProvider):
    """
    Azure OpenAI provider.

    Features:
    - Enterprise compliance
    - Data residency options
    - SLA guarantees
    - Private networking
    """

    provider_type = LLMProvider.AZURE
    API_VERSION = "2024-02-01"

    def __init__(
        self,
        api_key: Optional[str] = None,
        endpoint: Optional[str] = None,
        deployment: Optional[str] = None,
        api_version: Optional[str] = None,
        timeout: float = 120.0,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.api_key = api_key or os.getenv("AZURE_OPENAI_API_KEY")
        self.endpoint = endpoint or os.getenv("AZURE_OPENAI_ENDPOINT")
        self.deployment = deployment or os.getenv("AZURE_OPENAI_DEPLOYMENT")
        self.api_version = api_version or os.getenv("AZURE_OPENAI_API_VERSION", self.API_VERSION)
        self.timeout = timeout

        if not self.api_key or not self.endpoint:
            logger.warning("Azure OpenAI not fully configured")

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers."""
        return {
            "api-key": self.api_key,
            "Content-Type": "application/json",
        }

    def _get_url(self, operation: str = "chat/completions") -> str:
        """Build API URL."""
        return f"{self.endpoint}/openai/deployments/{self.deployment}/{operation}?api-version={self.api_version}"

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response using Azure OpenAI."""
        if not self.api_key or not self.endpoint or not self.deployment:
            raise ValueError("Azure OpenAI not fully configured")

        start_time = time.time()

        # Build messages
        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
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
                self._get_url(),
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

        # Calculate cost (Azure pricing varies by region/agreement)
        usage = data.get("usage", {})
        cost = self.calculate_cost(
            usage.get("prompt_tokens", 0),
            usage.get("completion_tokens", 0),
            self.deployment
        )

        return LLMResponse(
            content=content,
            provider=LLMProvider.AZURE,
            model=self.deployment,
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
        if not self.api_key or not self.endpoint or not self.deployment:
            raise ValueError("Azure OpenAI not fully configured")

        messages = self._build_messages(request)

        payload: Dict[str, Any] = {
            "messages": messages,
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
            "stream": True,
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                self._get_url(),
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
        """Check if Azure OpenAI is available."""
        if not self.api_key or not self.endpoint or not self.deployment:
            return False

        try:
            # Quick validation request
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    self._get_url(),
                    headers=self._get_headers(),
                    json={
                        "messages": [{"role": "user", "content": "Hi"}],
                        "max_tokens": 1,
                    }
                )
                return response.status_code in [200, 429]
        except Exception as e:
            logger.debug(f"Azure OpenAI health check failed: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """List available models (deployments)."""
        if self.deployment:
            return [self.deployment]
        return []

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
