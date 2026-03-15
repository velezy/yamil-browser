"""
Google Gemini LLM Provider
==========================
Cloud LLM provider using Google AI API.
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


class GeminiProvider(BaseLLMProvider):
    """
    Google Gemini provider.

    Features:
    - Gemini 1.5 Flash, Pro, 2.0 Flash
    - Vision support
    - Function calling
    - 1M+ context window
    - Cost-effective
    """

    provider_type = LLMProvider.GEMINI
    BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

    DEFAULT_MODELS = {
        "default": "gemini-1.5-flash",
        "quality": "gemini-1.5-pro",
        "fast": "gemini-1.5-flash",
        "vision": "gemini-1.5-pro",
        "latest": "gemini-2.0-flash-exp",
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
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        self.base_url = base_url or os.getenv("GEMINI_BASE_URL", self.BASE_URL)
        self.default_model = default_model or os.getenv("GEMINI_MODEL", self.DEFAULT_MODELS["default"])
        self.timeout = timeout

        if not self.api_key:
            logger.warning("Google API key not configured")

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response using Gemini."""
        if not self.api_key:
            raise ValueError("Google API key not configured")

        start_time = time.time()
        model = request.model or self._select_model(request)

        # Build content
        contents = self._build_contents(request)

        payload: Dict[str, Any] = {
            "contents": contents,
            "generationConfig": {
                "temperature": request.temperature,
                "maxOutputTokens": request.max_tokens,
            }
        }

        # Add system instruction
        if request.system_prompt:
            payload["systemInstruction"] = {
                "parts": [{"text": request.system_prompt}]
            }

        # Add function calling if tools provided
        if request.tools:
            payload["tools"] = self._convert_tools(request.tools)

        url = f"{self.base_url}/models/{model}:generateContent"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                url,
                params={"key": self.api_key},
                headers={"Content-Type": "application/json"},
                json=payload
            )
            response.raise_for_status()
            data = response.json()

        latency_ms = (time.time() - start_time) * 1000

        # Extract content
        content = ""
        tool_calls = None

        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            for part in parts:
                if "text" in part:
                    content += part["text"]
                elif "functionCall" in part:
                    if tool_calls is None:
                        tool_calls = []
                    fc = part["functionCall"]
                    tool_calls.append({
                        "type": "function",
                        "function": {
                            "name": fc.get("name"),
                            "arguments": json.dumps(fc.get("args", {}))
                        }
                    })

        # Calculate cost
        usage = data.get("usageMetadata", {})
        cost = self.calculate_cost(
            usage.get("promptTokenCount", 0),
            usage.get("candidatesTokenCount", 0),
            model
        )

        finish_reason = "stop"
        if candidates:
            finish_reason = candidates[0].get("finishReason", "STOP").lower()

        return LLMResponse(
            content=content,
            provider=LLMProvider.GEMINI,
            model=model,
            tokens_used=usage.get("totalTokenCount", 0),
            tokens_prompt=usage.get("promptTokenCount", 0),
            tokens_completion=usage.get("candidatesTokenCount", 0),
            latency_ms=latency_ms,
            cost_usd=cost,
            finish_reason=finish_reason,
            tool_calls=tool_calls,
        )

    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream a response token by token."""
        if not self.api_key:
            raise ValueError("Google API key not configured")

        model = request.model or self._select_model(request)
        contents = self._build_contents(request)

        payload: Dict[str, Any] = {
            "contents": contents,
            "generationConfig": {
                "temperature": request.temperature,
                "maxOutputTokens": request.max_tokens,
            }
        }

        if request.system_prompt:
            payload["systemInstruction"] = {
                "parts": [{"text": request.system_prompt}]
            }

        url = f"{self.base_url}/models/{model}:streamGenerateContent"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                url,
                params={"key": self.api_key, "alt": "sse"},
                headers={"Content-Type": "application/json"},
                json=payload
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        data_str = line[6:]
                        try:
                            data = json.loads(data_str)
                            candidates = data.get("candidates", [])
                            if candidates:
                                parts = candidates[0].get("content", {}).get("parts", [])
                                for part in parts:
                                    if "text" in part:
                                        yield part["text"]
                        except json.JSONDecodeError:
                            continue

    async def health_check(self) -> bool:
        """Check if Gemini is available."""
        if not self.api_key:
            return False

        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.base_url}/models",
                    params={"key": self.api_key}
                )
                return response.status_code == 200
        except Exception as e:
            logger.debug(f"Gemini health check failed: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """List available models."""
        return list(PROVIDER_CAPABILITIES.get(LLMProvider.GEMINI, {}).keys())

    def _build_contents(self, request: LLMRequest) -> List[Dict[str, Any]]:
        """Build contents array for Gemini."""
        contents: List[Dict[str, Any]] = []

        # Use existing messages if provided
        if request.messages:
            for msg in request.messages:
                role = msg.get("role", "user")
                # Gemini uses "user" and "model" roles
                if role == "assistant":
                    role = "model"
                elif role == "system":
                    continue  # Handled separately

                content = msg.get("content", "")
                parts = [{"text": content}]
                contents.append({"role": role, "parts": parts})
        else:
            # Build user message
            parts: List[Dict[str, Any]] = []

            # Add context
            text_content = request.prompt
            if request.context:
                context_text = "\n\n".join(request.context)
                text_content = f"Context:\n{context_text}\n\nQuestion: {request.prompt}"

            parts.append({"text": text_content})

            # Add images for vision
            if request.images:
                for img in request.images:
                    parts.append({
                        "inline_data": {
                            "mime_type": "image/jpeg",
                            "data": img
                        }
                    })

            contents.append({"role": "user", "parts": parts})

        return contents

    def _convert_tools(self, tools: List[Dict]) -> List[Dict]:
        """Convert OpenAI tool format to Gemini format."""
        function_declarations = []
        for tool in tools:
            if tool.get("type") == "function":
                func = tool.get("function", {})
                function_declarations.append({
                    "name": func.get("name"),
                    "description": func.get("description", ""),
                    "parameters": func.get("parameters", {})
                })
        return [{"function_declarations": function_declarations}]

    def _select_model(self, request: LLMRequest) -> str:
        """Select the best model based on request."""
        if request.task_type == "vision" or request.images:
            return self.DEFAULT_MODELS["vision"]
        elif request.task_type == "quality":
            return self.DEFAULT_MODELS["quality"]
        elif request.task_type == "fast":
            return self.DEFAULT_MODELS["fast"]

        return self.default_model
