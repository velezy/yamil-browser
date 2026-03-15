"""
T.A.L.O.S. Fast LLM Integration
vLLM and llama.cpp - faster alternatives to Ollama

Performance:
- vLLM: 5x throughput for batch/multi-user scenarios
- llama.cpp: 2x faster for single-user, lower memory

When to use:
- vLLM: High concurrency, multiple users, batch processing
- llama.cpp: Single user, low memory, edge deployment
- Ollama: Simple setup, development, casual use
"""

import os
import logging
import asyncio
from typing import Optional, List, Dict, Any, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

class LLMBackend(Enum):
    VLLM = "vllm"
    LLAMA_CPP = "llama_cpp"
    OLLAMA = "ollama"
    AUTO = "auto"


@dataclass
class LLMConfig:
    """LLM configuration"""
    # Backend selection
    backend: LLMBackend = LLMBackend.AUTO

    # vLLM settings (OpenAI-compatible API)
    vllm_url: str = os.getenv("VLLM_URL", "http://localhost:8000")
    vllm_model: str = os.getenv("VLLM_MODEL", "microsoft/phi-2")

    # llama.cpp settings
    llama_cpp_url: str = os.getenv("LLAMA_CPP_URL", "http://localhost:8080")

    # Ollama settings (fallback)
    ollama_url: str = os.getenv("OLLAMA_URL", "http://localhost:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "gemma3:4b")

    # Generation settings
    max_tokens: int = 2048
    temperature: float = 0.7
    top_p: float = 0.9
    timeout: float = 120.0


# =============================================================================
# BASE LLM CLIENT
# =============================================================================

class BaseLLMClient:
    """Base class for LLM clients"""

    async def generate(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Generate completion"""
        raise NotImplementedError

    async def generate_stream(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Generate completion with streaming"""
        raise NotImplementedError

    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Chat completion"""
        raise NotImplementedError

    async def health_check(self) -> bool:
        """Check if server is healthy"""
        raise NotImplementedError


# =============================================================================
# VLLM CLIENT
# =============================================================================

class VLLMClient(BaseLLMClient):
    """
    vLLM client - 5x throughput for batch/multi-user.

    Features:
    - Continuous batching
    - PagedAttention
    - Tensor parallelism
    - OpenAI-compatible API
    """

    def __init__(self, config: LLMConfig):
        self.config = config
        self.base_url = config.vllm_url
        self.model = config.vllm_model

    async def generate(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Generate completion using vLLM"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/v1/completions",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "max_tokens": max_tokens or self.config.max_tokens,
                        "temperature": temperature or self.config.temperature,
                        "top_p": self.config.top_p,
                        **kwargs
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()
                return result["choices"][0]["text"]

        except Exception as e:
            logger.error(f"vLLM generation failed: {e}")
            raise

    async def generate_stream(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream generation from vLLM"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/v1/completions",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "max_tokens": max_tokens or self.config.max_tokens,
                        "temperature": temperature or self.config.temperature,
                        "stream": True,
                        **kwargs
                    },
                    timeout=self.config.timeout
                ) as response:
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            data = line[6:]
                            if data == "[DONE]":
                                break
                            import json
                            chunk = json.loads(data)
                            if chunk["choices"]:
                                yield chunk["choices"][0].get("text", "")

        except Exception as e:
            logger.error(f"vLLM streaming failed: {e}")
            raise

    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Chat completion using vLLM"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/v1/chat/completions",
                    json={
                        "model": self.model,
                        "messages": messages,
                        "max_tokens": max_tokens or self.config.max_tokens,
                        "temperature": temperature or self.config.temperature,
                        **kwargs
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()
                return result["choices"][0]["message"]["content"]

        except Exception as e:
            logger.error(f"vLLM chat failed: {e}")
            raise

    async def health_check(self) -> bool:
        """Check vLLM health"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/health",
                    timeout=5.0
                )
                return response.status_code == 200
        except Exception:
            return False


# =============================================================================
# LLAMA.CPP CLIENT
# =============================================================================

class LlamaCppClient(BaseLLMClient):
    """
    llama.cpp client - 2x faster for single-user, lower memory.

    Features:
    - Optimized for single-user
    - Low memory footprint
    - CPU-friendly
    - GGUF model support
    """

    def __init__(self, config: LLMConfig):
        self.config = config
        self.base_url = config.llama_cpp_url

    async def generate(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Generate completion using llama.cpp"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/completion",
                    json={
                        "prompt": prompt,
                        "n_predict": max_tokens or self.config.max_tokens,
                        "temperature": temperature or self.config.temperature,
                        "top_p": self.config.top_p,
                        **kwargs
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()
                return result.get("content", "")

        except Exception as e:
            logger.error(f"llama.cpp generation failed: {e}")
            raise

    async def generate_stream(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream generation from llama.cpp"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/completion",
                    json={
                        "prompt": prompt,
                        "n_predict": max_tokens or self.config.max_tokens,
                        "temperature": temperature or self.config.temperature,
                        "stream": True,
                        **kwargs
                    },
                    timeout=self.config.timeout
                ) as response:
                    async for line in response.aiter_lines():
                        if line.startswith("data: "):
                            import json
                            data = json.loads(line[6:])
                            content = data.get("content", "")
                            if content:
                                yield content
                            if data.get("stop", False):
                                break

        except Exception as e:
            logger.error(f"llama.cpp streaming failed: {e}")
            raise

    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Chat completion using llama.cpp"""
        # Convert messages to prompt
        prompt = self._messages_to_prompt(messages)
        return await self.generate(prompt, max_tokens, temperature, **kwargs)

    def _messages_to_prompt(self, messages: List[Dict[str, str]]) -> str:
        """Convert chat messages to prompt format"""
        prompt_parts = []
        for msg in messages:
            role = msg["role"]
            content = msg["content"]
            if role == "system":
                prompt_parts.append(f"System: {content}")
            elif role == "user":
                prompt_parts.append(f"User: {content}")
            elif role == "assistant":
                prompt_parts.append(f"Assistant: {content}")
        prompt_parts.append("Assistant:")
        return "\n\n".join(prompt_parts)

    async def health_check(self) -> bool:
        """Check llama.cpp health"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/health",
                    timeout=5.0
                )
                return response.status_code == 200
        except Exception:
            return False


# =============================================================================
# OLLAMA CLIENT (FALLBACK)
# =============================================================================

class OllamaClient(BaseLLMClient):
    """
    Ollama client - simple setup, development use.

    Features:
    - Easy model management
    - Good for development
    - Simple API
    """

    def __init__(self, config: LLMConfig):
        self.config = config
        self.base_url = config.ollama_url
        self.model = config.ollama_model

    async def generate(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Generate completion using Ollama"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                        "options": {
                            "num_predict": max_tokens or self.config.max_tokens,
                            "temperature": temperature or self.config.temperature,
                        }
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()
                return result.get("response", "")

        except Exception as e:
            logger.error(f"Ollama generation failed: {e}")
            raise

    async def generate_stream(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream generation from Ollama"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "POST",
                    f"{self.base_url}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": True,
                        "options": {
                            "num_predict": max_tokens or self.config.max_tokens,
                            "temperature": temperature or self.config.temperature,
                        }
                    },
                    timeout=self.config.timeout
                ) as response:
                    async for line in response.aiter_lines():
                        if line:
                            import json
                            data = json.loads(line)
                            yield data.get("response", "")
                            if data.get("done", False):
                                break

        except Exception as e:
            logger.error(f"Ollama streaming failed: {e}")
            raise

    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Chat completion using Ollama"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.base_url}/api/chat",
                    json={
                        "model": self.model,
                        "messages": messages,
                        "stream": False,
                        "options": {
                            "num_predict": max_tokens or self.config.max_tokens,
                            "temperature": temperature or self.config.temperature,
                        }
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()
                return result.get("message", {}).get("content", "")

        except Exception as e:
            logger.error(f"Ollama chat failed: {e}")
            raise

    async def health_check(self) -> bool:
        """Check Ollama health"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.base_url}/api/tags",
                    timeout=5.0
                )
                return response.status_code == 200
        except Exception:
            return False


# =============================================================================
# SMART LLM CLIENT
# =============================================================================

class SmartLLM:
    """
    Smart LLM client that automatically selects the best backend.

    Priority:
    1. vLLM (if available and multi-user scenario)
    2. llama.cpp (if available and single-user)
    3. Ollama (fallback)
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig()
        self._clients: Dict[LLMBackend, BaseLLMClient] = {}
        self._active_backend: Optional[LLMBackend] = None

    async def _init_clients(self):
        """Initialize and check available backends"""
        if self._active_backend:
            return

        # Create clients
        self._clients = {
            LLMBackend.VLLM: VLLMClient(self.config),
            LLMBackend.LLAMA_CPP: LlamaCppClient(self.config),
            LLMBackend.OLLAMA: OllamaClient(self.config),
        }

        # Check availability
        if self.config.backend == LLMBackend.AUTO:
            # Try in order: vLLM, llama.cpp, Ollama
            for backend in [LLMBackend.VLLM, LLMBackend.LLAMA_CPP, LLMBackend.OLLAMA]:
                if await self._clients[backend].health_check():
                    self._active_backend = backend
                    logger.info(f"Using {backend.value} as LLM backend")
                    break

            if not self._active_backend:
                logger.warning("No LLM backend available")
        else:
            self._active_backend = self.config.backend

    def _get_client(self) -> Optional[BaseLLMClient]:
        """Get active client"""
        if self._active_backend:
            return self._clients.get(self._active_backend)
        return None

    async def generate(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Generate completion"""
        await self._init_clients()
        client = self._get_client()

        if not client:
            raise RuntimeError("No LLM backend available")

        return await client.generate(prompt, max_tokens, temperature, **kwargs)

    async def generate_stream(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Stream generation"""
        await self._init_clients()
        client = self._get_client()

        if not client:
            raise RuntimeError("No LLM backend available")

        async for chunk in client.generate_stream(prompt, max_tokens, temperature, **kwargs):
            yield chunk

    async def chat(
        self,
        messages: List[Dict[str, str]],
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        **kwargs
    ) -> str:
        """Chat completion"""
        await self._init_clients()
        client = self._get_client()

        if not client:
            raise RuntimeError("No LLM backend available")

        return await client.chat(messages, max_tokens, temperature, **kwargs)

    async def get_backend(self) -> Optional[str]:
        """Get active backend name"""
        await self._init_clients()
        return self._active_backend.value if self._active_backend else None


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_llm: Optional[SmartLLM] = None


async def get_llm() -> SmartLLM:
    """Get or create LLM singleton"""
    global _llm
    if _llm is None:
        _llm = SmartLLM()
    return _llm


async def quick_generate(prompt: str, **kwargs) -> str:
    """Quick generate text"""
    llm = await get_llm()
    return await llm.generate(prompt, **kwargs)


async def quick_chat(messages: List[Dict[str, str]], **kwargs) -> str:
    """Quick chat completion"""
    llm = await get_llm()
    return await llm.chat(messages, **kwargs)


# =============================================================================
# SETUP INSTRUCTIONS
# =============================================================================
"""
vLLM Setup (for high concurrency):
    pip install vllm
    python -m vllm.entrypoints.openai.api_server \
        --model microsoft/phi-2 \
        --port 8000

llama.cpp Setup (for single-user, low memory):
    # Build llama.cpp
    git clone https://github.com/ggerganov/llama.cpp
    cd llama.cpp && make

    # Run server
    ./server -m models/gemma-2b-it-q4_k_m.gguf -c 4096 --port 8080

Ollama Setup (for development):
    brew install ollama
    ollama serve
    ollama pull gemma3:4b

Performance Comparison:
    Ollama:     ~30-50 tokens/sec
    llama.cpp:  ~60-100 tokens/sec (2x faster)
    vLLM:       ~150-300 tokens/sec batched (5x throughput)

When to use each:
    - vLLM: Production, multiple users, batch processing
    - llama.cpp: Single user, edge deployment, low RAM
    - Ollama: Development, testing, simple deployments
"""
