"""
T.A.L.O.S. Infinity Embedding Server
Replaces sentence-transformers - 10,000+ vs 100-500 embeddings/sec

Features:
- High-performance embedding server
- Dynamic batching
- ONNX optimized
- OpenAI-compatible API
"""

import os
import logging
import asyncio
import hashlib
from typing import Optional, List, Dict, Any, Union
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class InfinityConfig:
    """Infinity embedding server configuration"""
    # Server URL
    url: str = os.getenv("INFINITY_URL", "http://localhost:7997")

    # Model settings
    default_model: str = os.getenv("EMBEDDING_MODEL", "BAAI/bge-small-en-v1.5")

    # Request settings
    timeout: float = 60.0
    batch_size: int = 32  # Infinity handles dynamic batching, but we can chunk
    max_retries: int = 3

    # Caching
    cache_enabled: bool = True
    cache_ttl: int = 86400  # 24 hours


# =============================================================================
# INFINITY CLIENT
# =============================================================================

class InfinityEmbeddings:
    """
    High-performance embedding client for Infinity server.

    Performance comparison:
    - sentence-transformers: 100-500 embeddings/sec
    - Infinity server: 10,000+ embeddings/sec

    Features:
    - Dynamic batching
    - ONNX/TensorRT optimization
    - OpenAI-compatible API
    """

    def __init__(self, config: Optional[InfinityConfig] = None):
        self.config = config or InfinityConfig()
        self._cache: Dict[str, List[float]] = {}

    def _hash_text(self, text: str) -> str:
        """Generate hash for text caching"""
        return hashlib.md5(text.encode()).hexdigest()

    async def embed_single(
        self,
        text: str,
        model: Optional[str] = None
    ) -> List[float]:
        """
        Embed a single text.

        Args:
            text: Text to embed
            model: Optional model override

        Returns:
            Embedding vector
        """
        embeddings = await self.embed_batch([text], model)
        return embeddings[0] if embeddings else []

    async def embed_batch(
        self,
        texts: List[str],
        model: Optional[str] = None
    ) -> List[List[float]]:
        """
        Embed multiple texts with automatic batching.

        Args:
            texts: List of texts to embed
            model: Optional model override

        Returns:
            List of embedding vectors
        """
        import httpx

        model = model or self.config.default_model

        # Check cache
        results = [None] * len(texts)
        texts_to_embed = []
        text_indices = []

        if self.config.cache_enabled:
            for i, text in enumerate(texts):
                cache_key = f"{model}:{self._hash_text(text)}"
                if cache_key in self._cache:
                    results[i] = self._cache[cache_key]
                else:
                    texts_to_embed.append(text)
                    text_indices.append(i)
        else:
            texts_to_embed = texts
            text_indices = list(range(len(texts)))

        if not texts_to_embed:
            return results

        # Embed in batches
        all_embeddings = []

        for i in range(0, len(texts_to_embed), self.config.batch_size):
            batch = texts_to_embed[i:i + self.config.batch_size]

            for attempt in range(self.config.max_retries):
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.post(
                            f"{self.config.url}/embeddings",
                            json={
                                "model": model,
                                "input": batch
                            },
                            timeout=self.config.timeout
                        )
                        response.raise_for_status()
                        result = response.json()

                    # Extract embeddings (OpenAI format)
                    batch_embeddings = [
                        item["embedding"]
                        for item in sorted(result["data"], key=lambda x: x["index"])
                    ]
                    all_embeddings.extend(batch_embeddings)
                    break

                except Exception as e:
                    if attempt == self.config.max_retries - 1:
                        logger.error(f"Embedding failed after {self.config.max_retries} attempts: {e}")
                        # Return empty embeddings for failed batch
                        all_embeddings.extend([[]] * len(batch))
                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))

        # Update results and cache
        for i, embedding in enumerate(all_embeddings):
            idx = text_indices[i]
            results[idx] = embedding

            if self.config.cache_enabled and embedding:
                cache_key = f"{model}:{self._hash_text(texts_to_embed[i])}"
                self._cache[cache_key] = embedding

        return results

    async def embed_query(
        self,
        query: str,
        model: Optional[str] = None
    ) -> List[float]:
        """
        Embed a query (alias for embed_single).

        Args:
            query: Query text
            model: Optional model override

        Returns:
            Embedding vector
        """
        return await self.embed_single(query, model)

    async def embed_documents(
        self,
        documents: List[str],
        model: Optional[str] = None
    ) -> List[List[float]]:
        """
        Embed multiple documents.

        Args:
            documents: List of document texts
            model: Optional model override

        Returns:
            List of embedding vectors
        """
        return await self.embed_batch(documents, model)

    async def similarity(
        self,
        text1: str,
        text2: str,
        model: Optional[str] = None
    ) -> float:
        """
        Calculate cosine similarity between two texts.

        Args:
            text1: First text
            text2: Second text
            model: Optional model override

        Returns:
            Cosine similarity score (0-1)
        """
        embeddings = await self.embed_batch([text1, text2], model)

        if not embeddings[0] or not embeddings[1]:
            return 0.0

        return self._cosine_similarity(embeddings[0], embeddings[1])

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity"""
        import math

        dot_product = sum(x * y for x, y in zip(a, b))
        magnitude_a = math.sqrt(sum(x * x for x in a))
        magnitude_b = math.sqrt(sum(x * x for x in b))

        if magnitude_a == 0 or magnitude_b == 0:
            return 0.0

        return dot_product / (magnitude_a * magnitude_b)

    async def rerank(
        self,
        query: str,
        documents: List[str],
        model: Optional[str] = None,
        top_k: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Rerank documents by relevance to query.

        Args:
            query: Query text
            documents: List of documents to rerank
            model: Optional model override
            top_k: Return top K results

        Returns:
            List of {"index", "score", "document"}
        """
        import httpx

        model = model or self.config.default_model

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.config.url}/rerank",
                    json={
                        "model": model,
                        "query": query,
                        "documents": documents,
                        "top_n": top_k or len(documents)
                    },
                    timeout=self.config.timeout
                )
                response.raise_for_status()
                result = response.json()

            return [
                {
                    "index": item["index"],
                    "score": item["relevance_score"],
                    "document": documents[item["index"]]
                }
                for item in result.get("results", [])
            ]

        except Exception as e:
            logger.error(f"Rerank failed: {e}")
            # Fallback to embedding similarity
            return await self._rerank_fallback(query, documents, model, top_k)

    async def _rerank_fallback(
        self,
        query: str,
        documents: List[str],
        model: str,
        top_k: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Fallback reranking using embeddings"""
        query_embedding = await self.embed_single(query, model)
        doc_embeddings = await self.embed_batch(documents, model)

        scores = []
        for i, doc_emb in enumerate(doc_embeddings):
            if doc_emb:
                score = self._cosine_similarity(query_embedding, doc_emb)
            else:
                score = 0.0
            scores.append({
                "index": i,
                "score": score,
                "document": documents[i]
            })

        # Sort by score descending
        scores.sort(key=lambda x: x["score"], reverse=True)

        if top_k:
            return scores[:top_k]
        return scores

    def clear_cache(self):
        """Clear embedding cache"""
        self._cache.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "cached_embeddings": len(self._cache),
            "cache_enabled": self.config.cache_enabled,
            "cache_ttl": self.config.cache_ttl
        }

    async def get_model_info(self) -> Dict[str, Any]:
        """Get information about available models"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.config.url}/models",
                    timeout=10.0
                )
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"Failed to get model info: {e}")
            return {"error": str(e)}

    async def health_check(self) -> bool:
        """Check if Infinity server is healthy"""
        import httpx

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.config.url}/health",
                    timeout=5.0
                )
                return response.status_code == 200
        except Exception:
            return False


# =============================================================================
# FALLBACK: OLLAMA EMBEDDINGS
# =============================================================================

class OllamaEmbeddingsFallback:
    """
    Fallback to Ollama when Infinity is unavailable.
    Uses nomic-embed-text for 768-dimensional embeddings.
    """

    def __init__(self, model_name: str = "nomic-embed-text"):
        self.model_name = model_name
        self.ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using Ollama (one at a time since Ollama doesn't support batch)"""
        import httpx

        embeddings = []
        async with httpx.AsyncClient(timeout=60.0) as client:
            for text in texts:
                try:
                    response = await client.post(
                        f"{self.ollama_url}/api/embeddings",
                        json={
                            "model": self.model_name,
                            "prompt": text
                        }
                    )
                    if response.status_code == 200:
                        data = response.json()
                        embedding = data.get("embedding", [])
                        embeddings.append(embedding)
                    else:
                        logger.warning(f"Ollama embedding failed: {response.status_code}")
                        embeddings.append([])
                except Exception as e:
                    logger.error(f"Ollama embedding error: {e}")
                    embeddings.append([])

        return embeddings

    async def embed_single(self, text: str) -> List[float]:
        """Embed single text"""
        embeddings = await self.embed_batch([text])
        return embeddings[0] if embeddings else []


# =============================================================================
# FALLBACK: SENTENCE TRANSFORMERS (Legacy)
# =============================================================================

class SentenceTransformersFallback:
    """
    Fallback to local sentence-transformers when Infinity is unavailable.
    Much slower but works offline.
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None

    def _load_model(self):
        """Lazy load the model"""
        if self._model is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._model = SentenceTransformer(self.model_name)
                logger.info(f"Loaded sentence-transformers model: {self.model_name}")
            except ImportError:
                logger.error("sentence-transformers not installed")
                raise

    async def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using local model"""
        self._load_model()

        # Run in thread pool to not block event loop
        loop = asyncio.get_event_loop()
        embeddings = await loop.run_in_executor(
            None,
            lambda: self._model.encode(texts, normalize_embeddings=True).tolist()
        )
        return embeddings

    async def embed_single(self, text: str) -> List[float]:
        """Embed single text"""
        embeddings = await self.embed_batch([text])
        return embeddings[0] if embeddings else []


# =============================================================================
# SMART EMBEDDINGS CLIENT
# =============================================================================

class SmartEmbeddings:
    """
    Smart embedding client that uses Infinity when available,
    falls back to Ollama with nomic-embed-text (768 dimensions).
    """

    def __init__(self, config: Optional[InfinityConfig] = None):
        self.config = config or InfinityConfig()
        self._infinity = InfinityEmbeddings(config)
        self._fallback: Optional[OllamaEmbeddingsFallback] = None
        self._use_infinity = True
        self._checked = False

    async def _check_infinity(self) -> bool:
        """Check if Infinity is available"""
        if await self._infinity.health_check():
            self._use_infinity = True
            self._checked = True
            return True

        logger.warning("Infinity unavailable, using Ollama fallback (nomic-embed-text)")
        self._use_infinity = False
        self._checked = True

        if self._fallback is None:
            self._fallback = OllamaEmbeddingsFallback()

        return False

    async def embed_batch(
        self,
        texts: List[str],
        model: Optional[str] = None
    ) -> List[List[float]]:
        """Embed texts using best available method"""
        # Check availability on first call
        if not self._checked:
            await self._check_infinity()

        if self._use_infinity:
            try:
                return await self._infinity.embed_batch(texts, model)
            except Exception:
                await self._check_infinity()

        if self._fallback:
            return await self._fallback.embed_batch(texts)

        return [[]] * len(texts)

    async def embed_single(self, text: str, model: Optional[str] = None) -> List[float]:
        """Embed single text"""
        embeddings = await self.embed_batch([text], model)
        return embeddings[0] if embeddings else []


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_embeddings: Optional[SmartEmbeddings] = None


async def get_embeddings() -> SmartEmbeddings:
    """Get or create embeddings singleton"""
    global _embeddings
    if _embeddings is None:
        _embeddings = SmartEmbeddings()
    return _embeddings


async def quick_embed(text: str, model: Optional[str] = None) -> List[float]:
    """Quick embed a single text"""
    embeddings = await get_embeddings()
    return await embeddings.embed_single(text, model)


async def quick_embed_batch(texts: List[str], model: Optional[str] = None) -> List[List[float]]:
    """Quick embed multiple texts"""
    embeddings = await get_embeddings()
    return await embeddings.embed_batch(texts, model)


# =============================================================================
# INFINITY SERVER SETUP
# =============================================================================
"""
Infinity Installation:
    pip install infinity-emb[all]

Start Infinity Server:
    infinity_emb --model-name-or-path BAAI/bge-small-en-v1.5 --port 7997

With multiple models:
    infinity_emb \
        --model-name-or-path BAAI/bge-small-en-v1.5 \
        --model-name-or-path BAAI/bge-reranker-base \
        --port 7997

With Docker:
    docker run -p 7997:7997 michaelf34/infinity:latest \
        --model-name-or-path BAAI/bge-small-en-v1.5

Performance:
    sentence-transformers: 100-500 embeddings/sec
    Infinity (CPU):        1,000-2,000 embeddings/sec
    Infinity (GPU):        10,000+ embeddings/sec

Supported models:
    - BAAI/bge-small-en-v1.5 (384 dim, fast)
    - BAAI/bge-base-en-v1.5 (768 dim, balanced)
    - BAAI/bge-large-en-v1.5 (1024 dim, highest quality)
    - sentence-transformers/all-MiniLM-L6-v2 (384 dim)
    - sentence-transformers/all-mpnet-base-v2 (768 dim)
"""
