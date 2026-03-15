"""
Cloud LLM Provider with Privacy Protection
===========================================
Provides unified interface to cloud LLMs (Claude, GPT, Gemini, Grok)
with automatic PII redaction before sending and restoration after response.

This ensures customer data privacy even when using cloud LLMs:
1. PII is detected and replaced with tokens BEFORE sending to cloud
2. Cloud LLM only sees sanitized data
3. Original values are restored in the response

Usage:
    provider = CloudLLMProvider(api_key="sk-...")
    response = await provider.generate("What is [PERSON_1]'s account balance?")
    # Response will have original names restored
"""

import os
import asyncio
import httpx
import logging
from typing import Dict, List, Optional, Any, AsyncGenerator
from dataclasses import dataclass
from enum import Enum
from abc import ABC, abstractmethod

from .pii_redactor import (
    PIIRedactor,
    get_pii_redactor,
    redact_query_for_cloud,
    restore_response_from_cloud
)

logger = logging.getLogger(__name__)


class CloudProvider(Enum):
    """Supported cloud LLM providers."""
    ANTHROPIC = "anthropic"  # Claude
    OPENAI = "openai"  # GPT-4, GPT-3.5
    GOOGLE = "google"  # Gemini
    XAI = "xai"  # Grok
    AWS_BEDROCK = "aws_bedrock"  # AWS Bedrock (Converse API)


@dataclass
class CloudLLMConfig:
    """Configuration for a cloud LLM provider."""
    provider: CloudProvider
    api_key: str
    model: str
    base_url: Optional[str] = None
    max_tokens: int = 4096
    temperature: float = 0.7
    timeout: int = 120
    enable_pii_redaction: bool = True  # Default to enabled for privacy
    # AWS Bedrock-specific (api_key unused for Bedrock)
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_region: str = "us-east-1"


@dataclass
class CloudLLMResponse:
    """Response from a cloud LLM."""
    content: str
    model: str
    provider: str
    usage: Dict[str, int]
    pii_redacted: bool
    pii_count: int
    latency_ms: int


class BaseCloudLLM(ABC):
    """Base class for cloud LLM providers."""

    def __init__(self, config: CloudLLMConfig):
        self.config = config
        self.redactor = get_pii_redactor() if config.enable_pii_redaction else None

    @abstractmethod
    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> CloudLLMResponse:
        """Generate a response from the cloud LLM."""
        pass

    @abstractmethod
    async def stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> AsyncGenerator[str, None]:
        """Stream a response from the cloud LLM."""
        pass

    async def _prepare_input(
        self,
        prompt: str,
        context: Optional[List[str]] = None
    ) -> tuple[str, List[str], Dict[str, str]]:
        """Prepare input by redacting PII if enabled."""
        if not self.config.enable_pii_redaction:
            return prompt, context or [], {}

        return await redact_query_for_cloud(prompt, context)

    async def _restore_output(
        self,
        response: str,
        token_map: Dict[str, str]
    ) -> str:
        """Restore PII in output if redaction was used."""
        if not token_map:
            return response
        return await restore_response_from_cloud(response, token_map)


class AnthropicLLM(BaseCloudLLM):
    """Claude (Anthropic) provider with PII redaction."""

    DEFAULT_BASE_URL = "https://api.anthropic.com/v1"

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> CloudLLMResponse:
        import time
        start_time = time.time()

        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        # Build messages
        messages = []
        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            messages.append({
                "role": "user",
                "content": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"
            })
        else:
            messages.append({"role": "user", "content": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.post(
                f"{base_url}/messages",
                headers={
                    "x-api-key": self.config.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "system": system_prompt or "You are a helpful assistant.",
                    "messages": messages
                }
            )
            response.raise_for_status()
            data = response.json()

        content = data["content"][0]["text"]

        # Restore PII in response
        restored_content = await self._restore_output(content, token_map)

        latency_ms = int((time.time() - start_time) * 1000)

        return CloudLLMResponse(
            content=restored_content,
            model=self.config.model,
            provider="anthropic",
            usage={
                "input_tokens": data.get("usage", {}).get("input_tokens", 0),
                "output_tokens": data.get("usage", {}).get("output_tokens", 0)
            },
            pii_redacted=len(token_map) > 0,
            pii_count=len(token_map),
            latency_ms=latency_ms
        )

    async def stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> AsyncGenerator[str, None]:
        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        # Build messages
        messages = []
        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            messages.append({
                "role": "user",
                "content": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"
            })
        else:
            messages.append({"role": "user", "content": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        # Collect full response for PII restoration
        full_response = ""

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/messages",
                headers={
                    "x-api-key": self.config.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "system": system_prompt or "You are a helpful assistant.",
                    "messages": messages,
                    "stream": True
                }
            ) as response:
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        import json
                        try:
                            data = json.loads(line[6:])
                            if data.get("type") == "content_block_delta":
                                text = data.get("delta", {}).get("text", "")
                                full_response += text
                                yield text
                        except json.JSONDecodeError:
                            continue

        # Note: For streaming, PII restoration happens at the end
        # The caller should collect the full response and restore
        if token_map:
            logger.info(f"Streaming response has {len(token_map)} PII tokens to restore")


class OpenAILLM(BaseCloudLLM):
    """OpenAI (GPT) provider with PII redaction."""

    DEFAULT_BASE_URL = "https://api.openai.com/v1"

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> CloudLLMResponse:
        import time
        start_time = time.time()

        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        # Build messages
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            messages.append({
                "role": "user",
                "content": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"
            })
        else:
            messages.append({"role": "user", "content": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.post(
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "temperature": self.config.temperature,
                    "messages": messages
                }
            )
            response.raise_for_status()
            data = response.json()

        content = data["choices"][0]["message"]["content"]

        # Restore PII in response
        restored_content = await self._restore_output(content, token_map)

        latency_ms = int((time.time() - start_time) * 1000)

        return CloudLLMResponse(
            content=restored_content,
            model=self.config.model,
            provider="openai",
            usage=data.get("usage", {}),
            pii_redacted=len(token_map) > 0,
            pii_count=len(token_map),
            latency_ms=latency_ms
        )

    async def stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> AsyncGenerator[str, None]:
        # Similar to generate but with streaming
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})

        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            messages.append({
                "role": "user",
                "content": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"
            })
        else:
            messages.append({"role": "user", "content": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": self.config.model,
                    "max_tokens": self.config.max_tokens,
                    "temperature": self.config.temperature,
                    "messages": messages,
                    "stream": True
                }
            ) as response:
                import json
                async for line in response.aiter_lines():
                    if line.startswith("data: ") and line != "data: [DONE]":
                        try:
                            data = json.loads(line[6:])
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                yield content
                        except json.JSONDecodeError:
                            continue


class GoogleLLM(BaseCloudLLM):
    """Google (Gemini) provider with PII redaction."""

    DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> CloudLLMResponse:
        import time
        start_time = time.time()

        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        # Build content
        parts = []
        if system_prompt:
            parts.append({"text": system_prompt})

        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            parts.append({"text": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"})
        else:
            parts.append({"text": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            response = await client.post(
                f"{base_url}/models/{self.config.model}:generateContent",
                params={"key": self.config.api_key},
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": parts}],
                    "generationConfig": {
                        "maxOutputTokens": self.config.max_tokens,
                        "temperature": self.config.temperature
                    }
                }
            )
            response.raise_for_status()
            data = response.json()

        content = data["candidates"][0]["content"]["parts"][0]["text"]

        # Restore PII in response
        restored_content = await self._restore_output(content, token_map)

        latency_ms = int((time.time() - start_time) * 1000)

        return CloudLLMResponse(
            content=restored_content,
            model=self.config.model,
            provider="google",
            usage={
                "input_tokens": data.get("usageMetadata", {}).get("promptTokenCount", 0),
                "output_tokens": data.get("usageMetadata", {}).get("candidatesTokenCount", 0)
            },
            pii_redacted=len(token_map) > 0,
            pii_count=len(token_map),
            latency_ms=latency_ms
        )

    async def stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> AsyncGenerator[str, None]:
        # Gemini streaming implementation
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        parts = []
        if system_prompt:
            parts.append({"text": system_prompt})

        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            parts.append({"text": f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"})
        else:
            parts.append({"text": redacted_prompt})

        base_url = self.config.base_url or self.DEFAULT_BASE_URL

        async with httpx.AsyncClient(timeout=self.config.timeout) as client:
            async with client.stream(
                "POST",
                f"{base_url}/models/{self.config.model}:streamGenerateContent",
                params={"key": self.config.api_key, "alt": "sse"},
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": parts}],
                    "generationConfig": {
                        "maxOutputTokens": self.config.max_tokens,
                        "temperature": self.config.temperature
                    }
                }
            ) as response:
                import json
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        try:
                            data = json.loads(line[6:])
                            text = data.get("candidates", [{}])[0].get(
                                "content", {}
                            ).get("parts", [{}])[0].get("text", "")
                            if text:
                                yield text
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue


class BedrockLLM(BaseCloudLLM):
    """AWS Bedrock (Converse API) provider with PII redaction."""

    def _get_bedrock_client(self):
        """Create a boto3 bedrock-runtime client with org credentials."""
        import boto3
        session = boto3.Session(
            aws_access_key_id=self.config.aws_access_key,
            aws_secret_access_key=self.config.aws_secret_key,
            region_name=self.config.aws_region,
        )
        return session.client("bedrock-runtime")

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> CloudLLMResponse:
        import time
        start_time = time.time()

        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        # Build messages for Bedrock Converse API
        user_content = redacted_prompt
        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            user_content = f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"

        messages = [{"role": "user", "content": [{"text": user_content}]}]
        system_messages = [{"text": system_prompt or "You are a helpful assistant."}]

        client = self._get_bedrock_client()

        # boto3 is synchronous — run in executor
        response = await asyncio.to_thread(
            client.converse,
            modelId=self.config.model,
            messages=messages,
            system=system_messages,
            inferenceConfig={
                "maxTokens": self.config.max_tokens,
                "temperature": self.config.temperature,
            },
        )

        content = response["output"]["message"]["content"][0]["text"]

        # Restore PII in response
        restored_content = await self._restore_output(content, token_map)

        latency_ms = int((time.time() - start_time) * 1000)
        usage = response.get("usage", {})

        return CloudLLMResponse(
            content=restored_content,
            model=self.config.model,
            provider="aws_bedrock",
            usage={
                "input_tokens": usage.get("inputTokens", 0),
                "output_tokens": usage.get("outputTokens", 0),
            },
            pii_redacted=len(token_map) > 0,
            pii_count=len(token_map),
            latency_ms=latency_ms,
        )

    async def stream(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        context: Optional[List[str]] = None
    ) -> AsyncGenerator[str, None]:
        # Redact PII
        redacted_prompt, redacted_context, token_map = await self._prepare_input(prompt, context)

        user_content = redacted_prompt
        if redacted_context:
            context_text = "\n\n".join(redacted_context)
            user_content = f"Context:\n{context_text}\n\nQuestion: {redacted_prompt}"

        messages = [{"role": "user", "content": [{"text": user_content}]}]
        system_messages = [{"text": system_prompt or "You are a helpful assistant."}]

        client = self._get_bedrock_client()

        # boto3 streaming uses converse_stream
        response = await asyncio.to_thread(
            client.converse_stream,
            modelId=self.config.model,
            messages=messages,
            system=system_messages,
            inferenceConfig={
                "maxTokens": self.config.max_tokens,
                "temperature": self.config.temperature,
            },
        )

        stream = response.get("stream")
        if stream:
            for event in stream:
                if "contentBlockDelta" in event:
                    text = event["contentBlockDelta"].get("delta", {}).get("text", "")
                    if text:
                        yield text

        if token_map:
            logger.info(f"Bedrock streaming response has {len(token_map)} PII tokens to restore")


# =============================================================================
# FACTORY AND CONVENIENCE FUNCTIONS
# =============================================================================

def create_cloud_llm(
    provider: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    enable_pii_redaction: bool = True,
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_region: Optional[str] = None,
) -> BaseCloudLLM:
    """
    Factory function to create a cloud LLM provider.

    Args:
        provider: Provider name (anthropic, openai, google, aws_bedrock)
        api_key: API key (or from environment). Not used for aws_bedrock.
        model: Model name (or default for provider)
        enable_pii_redaction: Whether to enable PII redaction
        aws_access_key: AWS access key ID (for aws_bedrock)
        aws_secret_key: AWS secret access key (for aws_bedrock)
        aws_region: AWS region (for aws_bedrock, defaults to us-east-1)

    Returns:
        Configured cloud LLM provider
    """
    provider_lower = provider.lower()

    if provider_lower in ("anthropic", "claude"):
        api_key = api_key or os.getenv("ANTHROPIC_API_KEY")
        model = model or "claude-3-5-sonnet-20241022"
        config = CloudLLMConfig(
            provider=CloudProvider.ANTHROPIC,
            api_key=api_key,
            model=model,
            enable_pii_redaction=enable_pii_redaction
        )
        return AnthropicLLM(config)

    elif provider_lower in ("openai", "gpt"):
        api_key = api_key or os.getenv("OPENAI_API_KEY")
        model = model or "gpt-4o"
        config = CloudLLMConfig(
            provider=CloudProvider.OPENAI,
            api_key=api_key,
            model=model,
            enable_pii_redaction=enable_pii_redaction
        )
        return OpenAILLM(config)

    elif provider_lower in ("google", "gemini"):
        api_key = api_key or os.getenv("GOOGLE_API_KEY")
        model = model or "gemini-1.5-pro"
        config = CloudLLMConfig(
            provider=CloudProvider.GOOGLE,
            api_key=api_key,
            model=model,
            enable_pii_redaction=enable_pii_redaction
        )
        return GoogleLLM(config)

    elif provider_lower in ("aws_bedrock", "bedrock"):
        model = model or "anthropic.claude-3-sonnet-20240229-v1:0"
        config = CloudLLMConfig(
            provider=CloudProvider.AWS_BEDROCK,
            api_key="bedrock",  # Not used, but field is required
            model=model,
            enable_pii_redaction=enable_pii_redaction,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_region=aws_region or "us-east-1",
        )
        return BedrockLLM(config)

    else:
        raise ValueError(f"Unsupported provider: {provider}")


# =============================================================================
# ORCHESTRATOR INTEGRATION
# =============================================================================

_cloud_llm_cache: Dict[str, BaseCloudLLM] = {}


async def query_cloud_llm(
    prompt: str,
    provider: str = "anthropic",
    model: Optional[str] = None,
    context: Optional[List[str]] = None,
    system_prompt: Optional[str] = None
) -> CloudLLMResponse:
    """
    Query a cloud LLM with automatic PII protection.

    This is the main entry point for the orchestrator to use cloud LLMs.
    PII is automatically redacted before sending and restored after response.

    Args:
        prompt: The user's query
        provider: Cloud provider (anthropic, openai, google)
        model: Optional specific model
        context: Optional context chunks from RAG
        system_prompt: Optional system prompt

    Returns:
        CloudLLMResponse with PII restored
    """
    cache_key = f"{provider}:{model or 'default'}"

    if cache_key not in _cloud_llm_cache:
        _cloud_llm_cache[cache_key] = create_cloud_llm(
            provider=provider,
            model=model,
            enable_pii_redaction=True
        )

    llm = _cloud_llm_cache[cache_key]

    response = await llm.generate(
        prompt=prompt,
        system_prompt=system_prompt,
        context=context
    )

    if response.pii_redacted:
        logger.info(
            f"Cloud LLM query: {response.pii_count} PII items redacted "
            f"before sending to {provider}"
        )

    return response


async def stream_cloud_llm(
    prompt: str,
    provider: str = "anthropic",
    model: Optional[str] = None,
    context: Optional[List[str]] = None,
    system_prompt: Optional[str] = None
) -> AsyncGenerator[str, None]:
    """
    Stream from a cloud LLM with PII protection.

    Note: For streaming, PII is redacted before sending, but restoration
    must happen after the full response is collected.

    Args:
        prompt: The user's query
        provider: Cloud provider
        model: Optional specific model
        context: Optional context chunks
        system_prompt: Optional system prompt

    Yields:
        Response tokens (with PII tokens, caller must restore)
    """
    cache_key = f"{provider}:{model or 'default'}"

    if cache_key not in _cloud_llm_cache:
        _cloud_llm_cache[cache_key] = create_cloud_llm(
            provider=provider,
            model=model,
            enable_pii_redaction=True
        )

    llm = _cloud_llm_cache[cache_key]

    async for token in llm.stream(
        prompt=prompt,
        system_prompt=system_prompt,
        context=context
    ):
        yield token
