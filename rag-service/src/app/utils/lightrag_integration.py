"""
LightRAG Integration — Document-Level Knowledge Graph

Integrates LightRAG (HKUDS, MIT license) for document-level entity/relationship
extraction with dual-mode retrieval (local entity + global relationship).

Architecture:
  Documents → RAG Service → LightRAG → working_dir (persistent volume)
                                     → PostgreSQL kg_entities (mirrored)
  Queries → LightRAG dual-mode → enriched context for LLM

Uses Ollama for entity extraction and sentence-transformers for embeddings.
"""

import os
import json
import asyncio
import logging
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Feature flag
LIGHTRAG_ENABLED = os.getenv("ENABLE_LIGHTRAG", "true").lower() in ("true", "1", "yes")

# Configuration
LIGHTRAG_WORKING_DIR = os.getenv("LIGHTRAG_WORKING_DIR", "/app/data/lightrag")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gemma3:4b")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")

# Try to import LightRAG
try:
    from lightrag import LightRAG, QueryParam
    from lightrag.llm.ollama import ollama_model_complete, ollama_embed
    LIGHTRAG_AVAILABLE = True
except ImportError:
    LIGHTRAG_AVAILABLE = False
    logger.info("LightRAG not installed — document graph features disabled. "
                "Install with: pip install lightrag-hku")

# Try to import database for entity mirroring
try:
    from assemblyline_common.database import get_db_pool
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False


@dataclass
class LightRAGResult:
    """Result from LightRAG query"""
    content: str
    mode: str  # "local", "global", "hybrid", "naive"
    entities_found: List[str]
    query_time_ms: float


class LightRAGService:
    """
    Document-level knowledge graph using LightRAG.

    Provides:
    - Entity/relationship extraction during document ingestion
    - Dual-mode retrieval (local entity + global relationship)
    - Entity mirroring to PostgreSQL for frontend display
    - Graceful fallback when unavailable
    """

    def __init__(self):
        self._rag: Optional['LightRAG'] = None
        self._initialized = False
        self._documents_indexed = 0

    async def initialize(self) -> bool:
        """Initialize LightRAG with Ollama backend."""
        if not LIGHTRAG_AVAILABLE:
            logger.warning("LightRAG package not available")
            return False

        if not LIGHTRAG_ENABLED:
            logger.info("LightRAG disabled via ENABLE_LIGHTRAG=false")
            return False

        try:
            # Ensure working directory exists
            os.makedirs(LIGHTRAG_WORKING_DIR, exist_ok=True)

            self._rag = LightRAG(
                working_dir=LIGHTRAG_WORKING_DIR,
                llm_model_func=ollama_model_complete,
                llm_model_name=OLLAMA_MODEL,
                llm_model_kwargs={
                    "host": OLLAMA_URL,
                    "options": {"num_ctx": 32768}
                },
                embedding_func=EmbeddingFunc(
                    embedding_dim=768,
                    max_token_size=8192,
                    func=self._embed_texts,
                ),
            )

            self._initialized = True
            logger.info(
                f"LightRAG initialized (model={OLLAMA_MODEL}, "
                f"working_dir={LIGHTRAG_WORKING_DIR})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to initialize LightRAG: {e}")
            self._initialized = False
            return False

    async def _embed_texts(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using Ollama."""
        import httpx

        embeddings = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            for text in texts:
                try:
                    resp = await client.post(
                        f"{OLLAMA_URL}/api/embeddings",
                        json={"model": EMBEDDING_MODEL, "prompt": text}
                    )
                    resp.raise_for_status()
                    embedding = resp.json().get("embedding", [])
                    embeddings.append(embedding)
                except Exception as e:
                    logger.warning(f"Embedding failed for text chunk: {e}")
                    # Return zero vector as fallback
                    embeddings.append([0.0] * 768)
        return embeddings

    async def ingest_document(
        self,
        content: str,
        document_id: int,
        filename: str = "",
        user_id: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Ingest a document into LightRAG for entity/relationship extraction.

        Called after the standard vector indexing pipeline completes.
        Runs as a background task to not block the main ingestion.

        Returns:
            Dict with extraction stats (entities_extracted, relationships_extracted)
        """
        if not self._initialized or self._rag is None:
            return {"success": False, "reason": "LightRAG not initialized"}

        start_time = datetime.utcnow()

        try:
            # Add document metadata as prefix for better entity extraction
            prefix = ""
            if filename:
                prefix = f"[Document: {filename}] "
            if metadata:
                doc_type = metadata.get("type", "")
                if doc_type:
                    prefix += f"[Type: {doc_type}] "

            # Insert into LightRAG (handles entity/relationship extraction internally)
            text_to_ingest = prefix + content

            # LightRAG processes the text, extracts entities, builds graph
            await self._rag.ainsert(text_to_ingest)

            self._documents_indexed += 1

            # Mirror entities to PostgreSQL for frontend display
            entities_mirrored = 0
            if DB_AVAILABLE:
                entities_mirrored = await self._mirror_entities_to_db(
                    document_id=document_id,
                    content=content,
                    user_id=user_id,
                )

            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            logger.info(
                f"LightRAG ingested doc {document_id} ({filename}): "
                f"mirrored {entities_mirrored} entities, {elapsed_ms:.0f}ms"
            )

            return {
                "success": True,
                "document_id": document_id,
                "entities_mirrored": entities_mirrored,
                "processing_time_ms": elapsed_ms,
            }

        except Exception as e:
            logger.warning(f"LightRAG ingestion failed for doc {document_id}: {e}")
            return {"success": False, "error": str(e)}

    async def query(
        self,
        query: str,
        mode: str = "hybrid",
        top_k: int = 10,
    ) -> Optional[LightRAGResult]:
        """
        Query the document knowledge graph.

        Modes:
        - "local": Entity-centric retrieval (find specific entities)
        - "global": Relationship-centric retrieval (find patterns/themes)
        - "hybrid": Both local + global (best for most queries)
        - "naive": Simple keyword matching (fallback)
        """
        if not self._initialized or self._rag is None:
            return None

        import time
        start = time.time()

        try:
            param = QueryParam(mode=mode, top_k=top_k)
            result = await self._rag.aquery(query, param=param)

            elapsed_ms = (time.time() - start) * 1000

            # Extract entity names from the result text (simple heuristic)
            entities_found = self._extract_entity_mentions(result, query)

            return LightRAGResult(
                content=result,
                mode=mode,
                entities_found=entities_found,
                query_time_ms=elapsed_ms,
            )

        except Exception as e:
            logger.warning(f"LightRAG query failed: {e}")
            return None

    async def _mirror_entities_to_db(
        self,
        document_id: int,
        content: str,
        user_id: Optional[int] = None,
    ) -> int:
        """Mirror extracted entities to PostgreSQL kg_entities table."""
        if not DB_AVAILABLE:
            return 0

        try:
            # Extract entities using simple NER patterns (fast, no LLM needed)
            entities = self._extract_entities_simple(content)
            if not entities:
                return 0

            pool = await get_db_pool()
            count = 0
            async with pool.acquire() as conn:
                for entity in entities:
                    try:
                        await conn.execute("""
                            INSERT INTO kg_entities (name, entity_type, description, importance_score, mention_count)
                            VALUES ($1, $2, $3, $4, 1)
                            ON CONFLICT (name) DO UPDATE SET
                                mention_count = kg_entities.mention_count + 1,
                                importance_score = GREATEST(kg_entities.importance_score, $4),
                                updated_at = NOW()
                        """,
                            entity["name"],
                            entity["type"],
                            entity.get("description", ""),
                            entity.get("importance", 0.5),
                        )
                        count += 1
                    except Exception as e:
                        logger.debug(f"Failed to mirror entity {entity['name']}: {e}")

            return count

        except Exception as e:
            logger.warning(f"Entity mirroring failed: {e}")
            return 0

    def _extract_entities_simple(self, text: str) -> List[Dict[str, Any]]:
        """Extract entities using regex patterns (fast, no LLM call)."""
        import re
        entities = []
        seen = set()

        # Capitalized multi-word names (people, organizations)
        name_pattern = re.compile(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b')
        for match in name_pattern.finditer(text):
            name = match.group(1).strip()
            if name not in seen and len(name) > 3:
                seen.add(name)
                entities.append({
                    "name": name,
                    "type": "person" if len(name.split()) <= 3 else "organization",
                    "importance": 0.6,
                })

        # Technical terms (ALL CAPS abbreviations)
        abbr_pattern = re.compile(r'\b([A-Z]{2,6})\b')
        for match in abbr_pattern.finditer(text):
            abbr = match.group(1)
            if abbr not in seen and abbr not in {"THE", "AND", "FOR", "NOT", "BUT", "ARE", "WAS", "HAS", "HAD", "CAN", "MAY", "HIS", "HER", "WHO", "ALL", "ANY"}:
                seen.add(abbr)
                entities.append({
                    "name": abbr,
                    "type": "term",
                    "importance": 0.4,
                })

        # Email addresses
        email_pattern = re.compile(r'[\w.+-]+@[\w-]+\.[\w.-]+')
        for match in email_pattern.finditer(text):
            email = match.group(0)
            if email not in seen:
                seen.add(email)
                entities.append({
                    "name": email,
                    "type": "person",
                    "importance": 0.5,
                })

        # URLs/domains
        url_pattern = re.compile(r'https?://[\w./\-?=&#]+')
        for match in url_pattern.finditer(text):
            url = match.group(0)
            if url not in seen:
                seen.add(url)
                entities.append({
                    "name": url[:100],
                    "type": "location",
                    "importance": 0.3,
                })

        return entities[:50]  # Cap at 50 entities per document

    def _extract_entity_mentions(self, result_text: str, query: str) -> List[str]:
        """Extract entity names mentioned in a LightRAG result."""
        import re
        entities = []
        # Look for quoted terms or capitalized sequences
        quoted = re.findall(r'"([^"]+)"', result_text)
        entities.extend(quoted[:10])

        capitalized = re.findall(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b', result_text)
        for cap in capitalized[:10]:
            if cap not in entities and cap.lower() not in query.lower():
                entities.append(cap)

        return entities[:15]

    def get_stats(self) -> Dict[str, Any]:
        """Get LightRAG statistics."""
        return {
            "initialized": self._initialized,
            "available": LIGHTRAG_AVAILABLE,
            "enabled": LIGHTRAG_ENABLED,
            "documents_indexed": self._documents_indexed,
            "working_dir": LIGHTRAG_WORKING_DIR,
            "model": OLLAMA_MODEL,
        }

    async def close(self):
        """Cleanup resources."""
        self._rag = None
        self._initialized = False
        logger.info("LightRAG service closed")


# Try to import EmbeddingFunc from lightrag
try:
    from lightrag.utils import EmbeddingFunc
except ImportError:
    # Stub for when lightrag is not installed
    class EmbeddingFunc:
        def __init__(self, embedding_dim=768, max_token_size=8192, func=None):
            self.embedding_dim = embedding_dim
            self.max_token_size = max_token_size
            self.func = func


# =============================================================================
# SINGLETON
# =============================================================================

_lightrag_service: Optional[LightRAGService] = None


async def get_lightrag_service() -> LightRAGService:
    """Get or create singleton LightRAG service."""
    global _lightrag_service
    if _lightrag_service is None:
        _lightrag_service = LightRAGService()
        await _lightrag_service.initialize()
    return _lightrag_service


async def close_lightrag_service():
    """Close the singleton LightRAG service."""
    global _lightrag_service
    if _lightrag_service:
        await _lightrag_service.close()
        _lightrag_service = None
