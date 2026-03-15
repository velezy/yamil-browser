"""
Ollama LLM Provider
===================
Local LLM provider using Ollama.
"""

import os
import json
import time
import logging
import httpx
from typing import AsyncIterator, List, Optional

from ..provider_interface import (
    BaseLLMProvider,
    LLMProvider,
    LLMRequest,
    LLMResponse,
)

logger = logging.getLogger(__name__)


class OllamaProvider(BaseLLMProvider):
    """
    Ollama local model provider.

    Features:
    - Local inference (privacy-first)
    - Zero cost
    - Multiple model support
    - Vision models (qwen2.5-vl, llava)
    """

    provider_type = LLMProvider.OLLAMA

    # Default models by task (matched to installed Ollama models)
    DEFAULT_MODELS = {
        "default": "qwen3:8b",
        "fast": "llama3.2:3b",
        "quality": "qwen3:8b",
        "vision": "qwen2.5vl:3b",
        "code": "qwen2.5-coder:7b",
        "math": "qwen3:8b",
        "embedding": "nomic-embed-text:latest",
    }

    def __init__(
        self,
        base_url: Optional[str] = None,
        default_model: Optional[str] = None,
        timeout: float = 120.0,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.base_url = base_url or os.getenv("OLLAMA_URL", "http://localhost:11434")
        self.default_model = default_model or os.getenv("OLLAMA_MODEL", self.DEFAULT_MODELS["default"])
        self.timeout = timeout

    async def generate(self, request: LLMRequest) -> LLMResponse:
        """Generate a response using Ollama."""
        start_time = time.time()

        model = request.model or self._select_model(request)

        # Build the prompt
        if request.messages:
            # Chat format
            response = await self._chat(request, model)
        else:
            # Simple generate
            response = await self._generate(request, model)

        latency_ms = (time.time() - start_time) * 1000

        return LLMResponse(
            content=response["content"],
            provider=LLMProvider.OLLAMA,
            model=model,
            tokens_used=response.get("total_tokens", 0),
            tokens_prompt=response.get("prompt_tokens", 0),
            tokens_completion=response.get("completion_tokens", 0),
            latency_ms=latency_ms,
            cost_usd=0.0,  # Local = free
            finish_reason=response.get("finish_reason", "stop"),
        )

    async def _generate(self, request: LLMRequest, model: str) -> dict:
        """Simple generate endpoint."""
        prompt = request.prompt
        if request.system_prompt:
            prompt = f"{request.system_prompt}\n\n{prompt}"

        if request.context:
            context_text = "\n\n".join(request.context)
            prompt = f"Context:\n{context_text}\n\nQuestion: {prompt}"

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": request.temperature,
                "num_predict": request.max_tokens,
            }
        }

        # Add images for vision models
        if request.images and self._is_vision_model(model):
            payload["images"] = request.images

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/generate",
                json=payload
            )
            response.raise_for_status()
            data = response.json()

        return {
            "content": data.get("response", ""),
            "total_tokens": data.get("eval_count", 0) + data.get("prompt_eval_count", 0),
            "prompt_tokens": data.get("prompt_eval_count", 0),
            "completion_tokens": data.get("eval_count", 0),
            "finish_reason": "stop" if data.get("done") else "length",
        }

    async def _chat(self, request: LLMRequest, model: str) -> dict:
        """Chat endpoint."""
        messages = list(request.messages) if request.messages else []

        # Add system prompt if provided and not already in messages
        if request.system_prompt and not any(m.get("role") == "system" for m in messages):
            messages.insert(0, {"role": "system", "content": request.system_prompt})

        # Add context to the last user message if provided
        if request.context and messages:
            for i in range(len(messages) - 1, -1, -1):
                if messages[i].get("role") == "user":
                    context_text = "\n\n".join(request.context)
                    messages[i]["content"] = f"Context:\n{context_text}\n\nQuestion: {messages[i]['content']}"
                    break

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": request.temperature,
                "num_predict": request.max_tokens,
            }
        }

        # Add images for vision models
        if request.images and self._is_vision_model(model):
            # Add images to the last user message
            for i in range(len(messages) - 1, -1, -1):
                if messages[i].get("role") == "user":
                    messages[i]["images"] = request.images
                    break

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/chat",
                json=payload
            )
            response.raise_for_status()
            data = response.json()

        return {
            "content": data.get("message", {}).get("content", ""),
            "total_tokens": data.get("eval_count", 0) + data.get("prompt_eval_count", 0),
            "prompt_tokens": data.get("prompt_eval_count", 0),
            "completion_tokens": data.get("eval_count", 0),
            "finish_reason": "stop" if data.get("done") else "length",
        }

    async def generate_stream(self, request: LLMRequest) -> AsyncIterator[str]:
        """Stream a response token by token."""
        model = request.model or self._select_model(request)

        if request.messages:
            async for chunk in self._chat_stream(request, model):
                yield chunk
        else:
            async for chunk in self._generate_stream(request, model):
                yield chunk

    async def _generate_stream(self, request: LLMRequest, model: str) -> AsyncIterator[str]:
        """Stream from generate endpoint."""
        prompt = request.prompt
        if request.system_prompt:
            prompt = f"{request.system_prompt}\n\n{prompt}"

        if request.context:
            context_text = "\n\n".join(request.context)
            prompt = f"Context:\n{context_text}\n\nQuestion: {prompt}"

        payload = {
            "model": model,
            "prompt": prompt,
            "stream": True,
            "options": {
                "temperature": request.temperature,
                "num_predict": request.max_tokens,
            }
        }

        if request.images and self._is_vision_model(model):
            payload["images"] = request.images

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/api/generate",
                json=payload
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        data = json.loads(line)
                        if data.get("response"):
                            yield data["response"]
                        if data.get("done"):
                            break

    async def _chat_stream(self, request: LLMRequest, model: str) -> AsyncIterator[str]:
        """Stream from chat endpoint."""
        messages = list(request.messages) if request.messages else []

        if request.system_prompt and not any(m.get("role") == "system" for m in messages):
            messages.insert(0, {"role": "system", "content": request.system_prompt})

        if request.context and messages:
            for i in range(len(messages) - 1, -1, -1):
                if messages[i].get("role") == "user":
                    context_text = "\n\n".join(request.context)
                    messages[i]["content"] = f"Context:\n{context_text}\n\nQuestion: {messages[i]['content']}"
                    break

        payload = {
            "model": model,
            "messages": messages,
            "stream": True,
            "options": {
                "temperature": request.temperature,
                "num_predict": request.max_tokens,
            }
        }

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream(
                "POST",
                f"{self.base_url}/api/chat",
                json=payload
            ) as response:
                async for line in response.aiter_lines():
                    if line:
                        data = json.loads(line)
                        content = data.get("message", {}).get("content", "")
                        if content:
                            yield content
                        if data.get("done"):
                            break

    async def health_check(self) -> bool:
        """Check if Ollama is available."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.base_url}/api/tags")
                return response.status_code == 200
        except Exception as e:
            logger.debug(f"Ollama health check failed: {e}")
            return False

    def get_available_models(self) -> List[str]:
        """List available models."""
        return list(self.DEFAULT_MODELS.values())

    async def list_models(self) -> List[str]:
        """List models from Ollama API."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"{self.base_url}/api/tags")
                response.raise_for_status()
                data = response.json()
                return [m["name"] for m in data.get("models", [])]
        except Exception as e:
            logger.error(f"Failed to list Ollama models: {e}")
            return self.get_available_models()

    def _select_model(self, request: LLMRequest) -> str:
        """Select the best model based on request."""
        # Task-based selection
        if request.task_type == "vision" or request.images:
            return self.DEFAULT_MODELS["vision"]
        elif request.task_type == "code":
            return self.DEFAULT_MODELS["code"]
        elif request.task_type == "math":
            return self.DEFAULT_MODELS["math"]
        elif request.task_type == "fast":
            return self.DEFAULT_MODELS["fast"]

        return self.default_model

    def _is_vision_model(self, model: str) -> bool:
        """Check if model supports vision."""
        vision_models = ["qwen2.5-vl", "llava", "bakllava", "moondream"]
        return any(vm in model.lower() for vm in vision_models)
