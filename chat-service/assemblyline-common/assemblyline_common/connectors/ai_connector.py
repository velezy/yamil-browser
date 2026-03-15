"""
Enterprise AI Provider Connectors.

Supports:
- Amazon Bedrock (Claude, Llama, etc.)
- Azure OpenAI
- Retry with exponential backoff
- Model fallback chains
- Token counting and budget limits
- Response streaming
- Circuit breaker

Usage:
    from assemblyline_common.connectors import (
        get_bedrock_connector,
        get_azure_openai_connector,
        BedrockConfig,
        AzureOpenAIConfig,
    )

    # Amazon Bedrock
    bedrock = await get_bedrock_connector(BedrockConfig(
        region="us-east-1",
        model_id="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
    ))
    response = await bedrock.invoke("Hello, how are you?")

    # Azure OpenAI
    azure = await get_azure_openai_connector(AzureOpenAIConfig(
        endpoint="https://my-resource.openai.azure.com",
        deployment="gpt-4",
    ))
    response = await azure.chat([{"role": "user", "content": "Hello"}])
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, AsyncIterator, Union

import aioboto3
import httpx

from assemblyline_common.circuit_breaker import (
    get_circuit_breaker,
    CircuitBreaker,
    CircuitOpenError,
)
from assemblyline_common.retry import RetryHandler, RetryConfig

logger = logging.getLogger(__name__)


@dataclass
class AIResponse:
    """AI model response."""
    content: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    finish_reason: Optional[str] = None
    latency_ms: float = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BedrockConfig:
    """Configuration for Amazon Bedrock connector."""
    region: str = "us-east-1"
    endpoint_url: Optional[str] = None  # VPC endpoint

    # Credentials (if not using IAM role)
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None

    # Model settings
    model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0"
    fallback_models: List[str] = field(default_factory=list)

    # Inference parameters
    max_tokens: int = 4096
    temperature: float = 0.7
    top_p: float = 0.9
    top_k: int = 250

    # Rate limiting and budgets
    max_tokens_per_minute: int = 100000
    max_requests_per_minute: int = 100

    # Retry and circuit breaker
    enable_retry: bool = True
    max_retries: int = 3
    enable_circuit_breaker: bool = True

    # Streaming
    enable_streaming: bool = False

    # Tenant settings
    tenant_id: Optional[str] = None
    budget_tokens_per_day: Optional[int] = None


@dataclass
class AzureOpenAIConfig:
    """Configuration for Azure OpenAI connector."""
    endpoint: str = ""  # https://resource.openai.azure.com
    api_key: str = ""
    api_version: str = "2024-02-15-preview"

    # Deployment settings
    deployment: str = ""  # Deployment name
    fallback_deployments: List[str] = field(default_factory=list)

    # Model parameters
    max_tokens: int = 4096
    temperature: float = 0.7
    top_p: float = 0.9
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0

    # Rate limiting
    max_tokens_per_minute: int = 100000
    max_requests_per_minute: int = 100

    # Retry and circuit breaker
    enable_retry: bool = True
    max_retries: int = 3
    enable_circuit_breaker: bool = True

    # Streaming
    enable_streaming: bool = False

    # Tenant settings
    tenant_id: Optional[str] = None
    budget_tokens_per_day: Optional[int] = None


class TokenBudgetManager:
    """Manages token budgets per tenant."""

    def __init__(self, daily_limit: Optional[int] = None):
        self.daily_limit = daily_limit
        self._usage: Dict[str, Dict[str, int]] = {}  # date -> {tenant: tokens}
        self._lock = asyncio.Lock()

    def _get_today(self) -> str:
        """Get today's date string."""
        from datetime import date
        return date.today().isoformat()

    async def check_budget(self, tenant_id: str, tokens: int) -> bool:
        """Check if tenant has budget for tokens."""
        if not self.daily_limit:
            return True

        async with self._lock:
            today = self._get_today()
            if today not in self._usage:
                self._usage = {today: {}}

            current = self._usage[today].get(tenant_id, 0)
            return (current + tokens) <= self.daily_limit

    async def record_usage(self, tenant_id: str, tokens: int) -> None:
        """Record token usage."""
        async with self._lock:
            today = self._get_today()
            if today not in self._usage:
                self._usage = {today: {}}

            current = self._usage[today].get(tenant_id, 0)
            self._usage[today][tenant_id] = current + tokens

    async def get_usage(self, tenant_id: str) -> Dict[str, int]:
        """Get token usage for tenant."""
        async with self._lock:
            today = self._get_today()
            return {
                "used_today": self._usage.get(today, {}).get(tenant_id, 0),
                "daily_limit": self.daily_limit,
            }


class BedrockConnector:
    """
    Enterprise Amazon Bedrock connector.

    Features:
    - Multiple model support (Claude, Llama, Titan)
    - Model fallback chains
    - Token counting and budgets
    - Response streaming
    - Circuit breaker
    - IAM role and VPC endpoint support
    """

    def __init__(
        self,
        config: BedrockConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._session: Optional[aioboto3.Session] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._budget_manager: Optional[TokenBudgetManager] = None
        self._metrics: Dict[str, int] = {
            "requests": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "errors": 0,
            "fallbacks": 0,
        }
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        self._session = aioboto3.Session(
            aws_access_key_id=self.config.access_key_id,
            aws_secret_access_key=self.config.secret_access_key,
            region_name=self.config.region,
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        if self.config.enable_retry:
            self._retry_handler = RetryHandler(
                config=RetryConfig(
                    max_attempts=self.config.max_retries,
                    base_delay=1.0,
                    max_delay=30.0,
                )
            )

        # Initialize budget manager
        if self.config.budget_tokens_per_day:
            self._budget_manager = TokenBudgetManager(
                daily_limit=self.config.budget_tokens_per_day
            )

        logger.info(
            "Bedrock connector initialized",
            extra={
                "event_type": "bedrock_initialized",
                "region": self.config.region,
                "model_id": self.config.model_id,
            }
        )

    def _build_request_body(
        self,
        prompt: str,
        system: Optional[str] = None,
        messages: Optional[List[Dict[str, str]]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Build request body based on model type."""
        model_id = kwargs.get("model_id", self.config.model_id)

        if "anthropic" in model_id:
            # Claude models
            body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
                "temperature": kwargs.get("temperature", self.config.temperature),
                "top_p": kwargs.get("top_p", self.config.top_p),
                "top_k": kwargs.get("top_k", self.config.top_k),
            }

            if messages:
                body["messages"] = messages
            else:
                body["messages"] = [{"role": "user", "content": prompt}]

            if system:
                body["system"] = system

        elif "meta" in model_id or "llama" in model_id.lower():
            # Llama models
            full_prompt = prompt
            if system:
                full_prompt = f"<s>[INST] <<SYS>>\n{system}\n<</SYS>>\n\n{prompt} [/INST]"

            body = {
                "prompt": full_prompt,
                "max_gen_len": kwargs.get("max_tokens", self.config.max_tokens),
                "temperature": kwargs.get("temperature", self.config.temperature),
                "top_p": kwargs.get("top_p", self.config.top_p),
            }

        elif "titan" in model_id.lower():
            # Amazon Titan
            body = {
                "inputText": prompt,
                "textGenerationConfig": {
                    "maxTokenCount": kwargs.get("max_tokens", self.config.max_tokens),
                    "temperature": kwargs.get("temperature", self.config.temperature),
                    "topP": kwargs.get("top_p", self.config.top_p),
                },
            }

        else:
            # Generic format
            body = {
                "prompt": prompt,
                "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            }

        return body

    def _parse_response(
        self,
        response_body: Dict[str, Any],
        model_id: str,
    ) -> AIResponse:
        """Parse response based on model type."""
        if "anthropic" in model_id:
            content = response_body.get("content", [{}])[0].get("text", "")
            return AIResponse(
                content=content,
                model=model_id,
                input_tokens=response_body.get("usage", {}).get("input_tokens", 0),
                output_tokens=response_body.get("usage", {}).get("output_tokens", 0),
                finish_reason=response_body.get("stop_reason"),
            )

        elif "meta" in model_id or "llama" in model_id.lower():
            return AIResponse(
                content=response_body.get("generation", ""),
                model=model_id,
                output_tokens=response_body.get("generation_token_count", 0),
                input_tokens=response_body.get("prompt_token_count", 0),
                finish_reason=response_body.get("stop_reason"),
            )

        elif "titan" in model_id.lower():
            results = response_body.get("results", [{}])
            return AIResponse(
                content=results[0].get("outputText", "") if results else "",
                model=model_id,
                input_tokens=response_body.get("inputTextTokenCount", 0),
                output_tokens=results[0].get("tokenCount", 0) if results else 0,
            )

        else:
            return AIResponse(
                content=str(response_body),
                model=model_id,
            )

    async def invoke(
        self,
        prompt: str,
        system: Optional[str] = None,
        messages: Optional[List[Dict[str, str]]] = None,
        **kwargs,
    ) -> AIResponse:
        """
        Invoke a Bedrock model.

        Args:
            prompt: User prompt
            system: System prompt
            messages: Chat messages (for Claude)
            **kwargs: Additional parameters

        Returns:
            AIResponse with content and metadata
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        # Check budget
        tenant_id = self.config.tenant_id or "default"
        if self._budget_manager:
            # Estimate tokens
            estimated_tokens = len(prompt.split()) * 2 + self.config.max_tokens
            if not await self._budget_manager.check_budget(tenant_id, estimated_tokens):
                raise RuntimeError(f"Token budget exceeded for tenant {tenant_id}")

        # Build model list (primary + fallbacks)
        models = [self.config.model_id] + self.config.fallback_models
        last_error = None

        for model_id in models:
            circuit_name = f"bedrock:{model_id}"

            if self._circuit_breaker:
                if not await self._circuit_breaker.can_execute(circuit_name):
                    continue  # Try next model

            body = self._build_request_body(prompt, system, messages, model_id=model_id, **kwargs)

            async def do_invoke() -> AIResponse:
                start_time = time.time()

                async with self._session.client(
                    "bedrock-runtime",
                    endpoint_url=self.config.endpoint_url,
                ) as bedrock:
                    response = await bedrock.invoke_model(
                        modelId=model_id,
                        body=json.dumps(body),
                        contentType="application/json",
                        accept="application/json",
                    )

                    response_body = json.loads(
                        await response["body"].read()
                    )

                latency_ms = (time.time() - start_time) * 1000
                result = self._parse_response(response_body, model_id)
                result.latency_ms = latency_ms
                result.total_tokens = result.input_tokens + result.output_tokens

                return result

            try:
                if self._retry_handler:
                    result = await self._retry_handler.execute(
                        do_invoke,
                        operation_id=f"bedrock-{model_id}",
                    )
                else:
                    result = await do_invoke()

                # Record usage
                if self._budget_manager:
                    await self._budget_manager.record_usage(
                        tenant_id, result.total_tokens
                    )

                # Update metrics
                self._metrics["requests"] += 1
                self._metrics["input_tokens"] += result.input_tokens
                self._metrics["output_tokens"] += result.output_tokens

                if self._circuit_breaker:
                    await self._circuit_breaker.record_success(circuit_name)

                if model_id != self.config.model_id:
                    self._metrics["fallbacks"] += 1

                logger.info(
                    "Bedrock invocation completed",
                    extra={
                        "event_type": "bedrock_invocation",
                        "model_id": model_id,
                        "input_tokens": result.input_tokens,
                        "output_tokens": result.output_tokens,
                        "latency_ms": result.latency_ms,
                    }
                )

                return result

            except Exception as e:
                last_error = e
                self._metrics["errors"] += 1

                if self._circuit_breaker:
                    await self._circuit_breaker.record_failure(circuit_name, e)

                logger.warning(f"Bedrock invocation failed for {model_id}: {e}")
                continue

        # All models failed
        raise last_error or RuntimeError("All models failed")

    async def invoke_stream(
        self,
        prompt: str,
        system: Optional[str] = None,
        **kwargs,
    ) -> AsyncIterator[str]:
        """
        Stream response from Bedrock model.

        Yields content chunks as they arrive.
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._session:
            await self.initialize()

        model_id = self.config.model_id
        body = self._build_request_body(prompt, system, **kwargs)

        async with self._session.client(
            "bedrock-runtime",
            endpoint_url=self.config.endpoint_url,
        ) as bedrock:
            response = await bedrock.invoke_model_with_response_stream(
                modelId=model_id,
                body=json.dumps(body),
                contentType="application/json",
                accept="application/json",
            )

            async for event in response["body"]:
                chunk = event.get("chunk")
                if chunk:
                    chunk_data = json.loads(chunk["bytes"])

                    # Extract text based on model
                    if "anthropic" in model_id:
                        if chunk_data.get("type") == "content_block_delta":
                            text = chunk_data.get("delta", {}).get("text", "")
                            if text:
                                yield text

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return self._metrics

    async def get_usage(self) -> Dict[str, Any]:
        """Get token usage."""
        if self._budget_manager:
            tenant_id = self.config.tenant_id or "default"
            return await self._budget_manager.get_usage(tenant_id)
        return {}

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        self._session = None
        logger.info("Bedrock connector closed")


class AzureOpenAIConnector:
    """
    Enterprise Azure OpenAI connector.

    Features:
    - Chat and completion endpoints
    - Deployment fallback chains
    - Token counting and budgets
    - Response streaming
    - Circuit breaker
    - Managed identity support
    """

    def __init__(
        self,
        config: AzureOpenAIConfig,
        circuit_breaker: Optional[CircuitBreaker] = None,
    ):
        self.config = config
        self._circuit_breaker = circuit_breaker
        self._client: Optional[httpx.AsyncClient] = None
        self._retry_handler: Optional[RetryHandler] = None
        self._budget_manager: Optional[TokenBudgetManager] = None
        self._metrics: Dict[str, int] = {
            "requests": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "errors": 0,
            "fallbacks": 0,
        }
        self._closed = False

    async def initialize(self) -> None:
        """Initialize the connector."""
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "api-key": self.config.api_key,
                "Content-Type": "application/json",
            },
        )

        # Initialize circuit breaker
        if self.config.enable_circuit_breaker and not self._circuit_breaker:
            self._circuit_breaker = await get_circuit_breaker()

        # Initialize retry handler
        if self.config.enable_retry:
            self._retry_handler = RetryHandler(
                config=RetryConfig(
                    max_attempts=self.config.max_retries,
                    base_delay=1.0,
                    max_delay=30.0,
                    retryable_status_codes={429, 500, 502, 503, 504},
                )
            )

        # Initialize budget manager
        if self.config.budget_tokens_per_day:
            self._budget_manager = TokenBudgetManager(
                daily_limit=self.config.budget_tokens_per_day
            )

        logger.info(
            "Azure OpenAI connector initialized",
            extra={
                "event_type": "azure_openai_initialized",
                "endpoint": self.config.endpoint,
                "deployment": self.config.deployment,
            }
        )

    def _get_url(self, deployment: str) -> str:
        """Get API URL for deployment."""
        base = self.config.endpoint.rstrip("/")
        return f"{base}/openai/deployments/{deployment}/chat/completions?api-version={self.config.api_version}"

    async def chat(
        self,
        messages: List[Dict[str, str]],
        **kwargs,
    ) -> AIResponse:
        """
        Send chat completion request.

        Args:
            messages: List of message dicts with role and content
            **kwargs: Additional parameters

        Returns:
            AIResponse with content and metadata
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._client:
            await self.initialize()

        # Check budget
        tenant_id = self.config.tenant_id or "default"
        if self._budget_manager:
            estimated_tokens = sum(len(m["content"].split()) for m in messages) * 2
            if not await self._budget_manager.check_budget(tenant_id, estimated_tokens):
                raise RuntimeError(f"Token budget exceeded for tenant {tenant_id}")

        # Build deployment list
        deployments = [self.config.deployment] + self.config.fallback_deployments
        last_error = None

        for deployment in deployments:
            circuit_name = f"azure-openai:{deployment}"

            if self._circuit_breaker:
                if not await self._circuit_breaker.can_execute(circuit_name):
                    continue

            url = self._get_url(deployment)
            body = {
                "messages": messages,
                "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
                "temperature": kwargs.get("temperature", self.config.temperature),
                "top_p": kwargs.get("top_p", self.config.top_p),
                "frequency_penalty": kwargs.get(
                    "frequency_penalty", self.config.frequency_penalty
                ),
                "presence_penalty": kwargs.get(
                    "presence_penalty", self.config.presence_penalty
                ),
            }

            async def do_chat() -> AIResponse:
                start_time = time.time()

                response = await self._client.post(url, json=body)
                response.raise_for_status()

                data = response.json()
                latency_ms = (time.time() - start_time) * 1000

                choice = data.get("choices", [{}])[0]
                usage = data.get("usage", {})

                return AIResponse(
                    content=choice.get("message", {}).get("content", ""),
                    model=data.get("model", deployment),
                    input_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                    total_tokens=usage.get("total_tokens", 0),
                    finish_reason=choice.get("finish_reason"),
                    latency_ms=latency_ms,
                )

            try:
                if self._retry_handler:
                    result = await self._retry_handler.execute(
                        do_chat,
                        operation_id=f"azure-openai-{deployment}",
                    )
                else:
                    result = await do_chat()

                # Record usage
                if self._budget_manager:
                    await self._budget_manager.record_usage(
                        tenant_id, result.total_tokens
                    )

                # Update metrics
                self._metrics["requests"] += 1
                self._metrics["input_tokens"] += result.input_tokens
                self._metrics["output_tokens"] += result.output_tokens

                if self._circuit_breaker:
                    await self._circuit_breaker.record_success(circuit_name)

                if deployment != self.config.deployment:
                    self._metrics["fallbacks"] += 1

                logger.info(
                    "Azure OpenAI chat completed",
                    extra={
                        "event_type": "azure_openai_chat",
                        "deployment": deployment,
                        "input_tokens": result.input_tokens,
                        "output_tokens": result.output_tokens,
                        "latency_ms": result.latency_ms,
                    }
                )

                return result

            except Exception as e:
                last_error = e
                self._metrics["errors"] += 1

                if self._circuit_breaker:
                    await self._circuit_breaker.record_failure(circuit_name, e)

                logger.warning(f"Azure OpenAI chat failed for {deployment}: {e}")
                continue

        raise last_error or RuntimeError("All deployments failed")

    async def chat_stream(
        self,
        messages: List[Dict[str, str]],
        **kwargs,
    ) -> AsyncIterator[str]:
        """
        Stream chat completion response.

        Yields content chunks as they arrive.
        """
        if self._closed:
            raise RuntimeError("Connector is closed")

        if not self._client:
            await self.initialize()

        url = self._get_url(self.config.deployment)
        body = {
            "messages": messages,
            "max_tokens": kwargs.get("max_tokens", self.config.max_tokens),
            "temperature": kwargs.get("temperature", self.config.temperature),
            "stream": True,
        }

        async with self._client.stream("POST", url, json=body) as response:
            async for line in response.aiter_lines():
                if line.startswith("data: "):
                    data = line[6:]
                    if data == "[DONE]":
                        break

                    try:
                        chunk = json.loads(data)
                        delta = chunk.get("choices", [{}])[0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            yield content
                    except json.JSONDecodeError:
                        continue

    def get_metrics(self) -> Dict[str, Any]:
        """Get connector metrics."""
        return self._metrics

    async def get_usage(self) -> Dict[str, Any]:
        """Get token usage."""
        if self._budget_manager:
            tenant_id = self.config.tenant_id or "default"
            return await self._budget_manager.get_usage(tenant_id)
        return {}

    async def close(self) -> None:
        """Close the connector."""
        self._closed = True
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info("Azure OpenAI connector closed")


# Singleton instances
_bedrock_connectors: Dict[str, BedrockConnector] = {}
_azure_openai_connectors: Dict[str, AzureOpenAIConnector] = {}
_ai_lock = asyncio.Lock()


async def get_bedrock_connector(
    config: Optional[BedrockConfig] = None,
    name: Optional[str] = None,
) -> BedrockConnector:
    """Get or create a Bedrock connector."""
    config = config or BedrockConfig()
    connector_name = name or f"bedrock-{config.region}-{config.model_id}"

    if connector_name in _bedrock_connectors:
        return _bedrock_connectors[connector_name]

    async with _ai_lock:
        if connector_name in _bedrock_connectors:
            return _bedrock_connectors[connector_name]

        connector = BedrockConnector(config)
        await connector.initialize()
        _bedrock_connectors[connector_name] = connector

        return connector


async def get_azure_openai_connector(
    config: Optional[AzureOpenAIConfig] = None,
    name: Optional[str] = None,
) -> AzureOpenAIConnector:
    """Get or create an Azure OpenAI connector."""
    config = config or AzureOpenAIConfig()
    connector_name = name or f"azure-openai-{config.deployment}"

    if connector_name in _azure_openai_connectors:
        return _azure_openai_connectors[connector_name]

    async with _ai_lock:
        if connector_name in _azure_openai_connectors:
            return _azure_openai_connectors[connector_name]

        connector = AzureOpenAIConnector(config)
        await connector.initialize()
        _azure_openai_connectors[connector_name] = connector

        return connector


async def close_all_ai_connectors() -> None:
    """Close all AI connectors."""
    for connector in _bedrock_connectors.values():
        await connector.close()
    _bedrock_connectors.clear()

    for connector in _azure_openai_connectors.values():
        await connector.close()
    _azure_openai_connectors.clear()
