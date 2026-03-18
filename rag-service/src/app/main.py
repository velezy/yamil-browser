"""
RAG Service - Port 7002
Document retrieval, embeddings, hybrid search with pgvector
PostgreSQL 17 + pgvector - No in-memory fallback
"""
import os
import sys
import asyncio
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
# SentenceTransformer imported lazily to avoid OOM during startup
import numpy as np
import json
import logging
import jwt
from fastapi import UploadFile, File, Form

# JWT configuration - import from shared config to match auth service
try:
    from assemblyline_common.config import config
    JWT_SECRET = config.JWT_SECRET
except ImportError:
    JWT_SECRET = os.getenv("JWT_SECRET", "drivesentinel-jwt-secret-change-in-production")
JWT_ALGORITHM = "HS256"

# Add shared module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Initialize observability (tracing, metrics, structured logging, PII masking)
try:
    from assemblyline_common.observability import get_logger
    logger = get_logger(__name__)
    OBSERVABILITY_AVAILABLE = True
except (ImportError, TypeError):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    OBSERVABILITY_AVAILABLE = False

# Import shared database
try:
    from assemblyline_common.database import get_db_pool, close_db_pool, get_connection
    DB_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Database module not available: {e}")
    DB_AVAILABLE = False

# DocumentRepository is optional — used for document tracking in crawler/ingest
# but NOT required for core vector store operations
try:
    from assemblyline_common.database import DocumentRepository, DocumentStatus
except (ImportError, TypeError):
    DocumentRepository = None
    DocumentStatus = None

# Auth imports for multi-tenant isolation
try:
    from assemblyline_common.auth import require_user_id, get_user_id_from_request
    AUTH_AVAILABLE = True
except ImportError:
    AUTH_AVAILABLE = False
    def require_user_id(request: Request) -> int:
        raise HTTPException(status_code=503, detail="Auth module not available")
    def get_user_id_from_request(request: Request) -> Optional[int]:
        return None

# Import our vector store
from app.utils.vector_store import (
    PgVectorStore,
    VectorStoreConfig,
    get_vector_store,
    close_vector_store
)

# Import intent classifier
from app.utils.intent_classifier import (
    QueryIntentClassifier,
    QueryIntent,
    RetrievalStrategy,
    get_intent_classifier,
    build_retrieval_config
)

# Import cross-referencer
from app.utils.cross_referencer import (
    CrossReferenceLinker,
    get_cross_referencer,
    enhance_context_with_references
)

# Import advanced RAG modules
try:
    from app.utils.reranker import (
        NeuralReranker,
        get_reranker,
        rerank_chunks,
        RankedChunk
    )
    RERANKER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Reranker module not available: {e}")
    RERANKER_AVAILABLE = False

try:
    from app.utils.hyde import (
        HyDEGenerator,
        HyDERetriever,
        get_hyde_generator,
        generate_hyde_embeddings
    )
    HYDE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"HyDE module not available: {e}")
    HYDE_AVAILABLE = False

try:
    from app.utils.query_decomposition import (
        QueryDecomposer,
        DecompositionStrategy,
        get_query_decomposer,
        decompose_query,
        ParallelQueryExecutor
    )
    QUERY_DECOMPOSITION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Query decomposition module not available: {e}")
    QUERY_DECOMPOSITION_AVAILABLE = False

try:
    from app.utils.query_expander import (
        QueryExpander,
        ExpansionConfig,
        QueryExpansionResult,
        get_query_expander,
        expand_query
    )
    QUERY_EXPANSION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Query expansion module not available: {e}")
    QUERY_EXPANSION_AVAILABLE = False

try:
    from app.utils.parent_child_chunking import (
        ParentChildChunker,
        ParentChildRetriever,
        Chunk,
        ChunkLevel,
        create_parent_child_chunks
    )
    PARENT_CHILD_CHUNKING_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Parent-child chunking module not available: {e}")
    PARENT_CHILD_CHUNKING_AVAILABLE = False

# Import incremental indexer
from app.utils.incremental_indexer import (
    IncrementalIndexer,
    get_incremental_indexer,
    index_chunks_incrementally
)

# Import document processor
from app.utils.document_processor import (
    DocumentProcessor,
    ProcessorConfig as DocProcessorConfig,
    ProcessedDocument,
    DocumentType,
    get_document_processor
)

# Import query processor
from app.utils.query_processor import (
    RAGQueryProcessor,
    ProcessorConfig as QueryProcessorConfig,
    QueryResult,
    get_query_processor
)

# Import Ollama optimization module
try:
    from app.ollama import (
        get_ollama_service,
        OllamaOptimizationService,
        TokenCounter,
        OLLAMA_CONFIG
    )
    OLLAMA_OPTIMIZATION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Ollama optimization module not available: {e}")
    OLLAMA_OPTIMIZATION_AVAILABLE = False

# Import Knowledge Graph module
try:
    from app.utils.graph_rag import (
        get_knowledge_graph,
        get_hybrid_pipeline,
        extract_and_build_graph,
        get_graph_enhanced_context,
        ContentKnowledgeGraph,
        HybridRAGPipeline,
        HybridEntityExtractor,
        RelationshipType
    )
    KNOWLEDGE_GRAPH_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Knowledge graph module not available: {e}")
    KNOWLEDGE_GRAPH_AVAILABLE = False

# Import Knowledge Graph Repository for persistence
try:
    from assemblyline_common.database.repositories import KnowledgeGraphRepository
    KG_PERSISTENCE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Knowledge graph persistence not available: {e}")
    KG_PERSISTENCE_AVAILABLE = False
    KnowledgeGraphRepository = None

# Import Web Crawler module
try:
    from app.utils.web_crawler import (
        WebCrawler,
        CrawlConfig,
        CrawlResult,
        get_crawler,
        cleanup_crawler
    )
    from app.utils.agentic_crawler import (
        AgenticCrawler,
        CrawlPlan,
        ContentEvaluation,
        SynthesizedContent,
        get_agentic_crawler,
        cleanup_agentic_crawler
    )
    WEB_CRAWLER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Web crawler module not available: {e}")
    WEB_CRAWLER_AVAILABLE = False

# Import Multi-Agent Pipeline
try:
    from app.agents import (
        AgentOrchestrator,
        AgentContext,
        create_agent_pipeline
    )
    AGENT_PIPELINE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Agent pipeline module not available: {e}")
    AGENT_PIPELINE_AVAILABLE = False

# Import AI Tools for data analysis
try:
    from app.tools import (
        create_all_tools,
        CalculatorTool,
        ChartTool,
        SQLQueryTool,
        DataInsightsTool,
    )
    TOOLS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"AI Tools module not available: {e}")
    TOOLS_AVAILABLE = False

# Import Semantic Clustering module
try:
    from app.utils.semantic_clustering import (
        SemanticClusteringEngine,
        ClusterConfig,
        ClusteringAlgorithm,
        ClusteringResult,
        get_clustering_engine
    )
    SEMANTIC_CLUSTERING_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Semantic clustering module not available: {e}")
    SEMANTIC_CLUSTERING_AVAILABLE = False

# Import Predictive Caching module
try:
    from app.utils.predictive_cache import (
        PatternLearner,
        PredictiveCache,
        PatternType,
        BehaviorPattern,
        CacheStats,
        get_pattern_learner,
        get_predictive_cache
    )
    PREDICTIVE_CACHE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Predictive cache module not available: {e}")
    PREDICTIVE_CACHE_AVAILABLE = False

# Import Continuous Eval Runner
try:
    from app.utils.eval_runner import get_eval_runner, ContinuousEvalRunner
    EVAL_RUNNER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Eval runner module not available: {e}")
    EVAL_RUNNER_AVAILABLE = False

# Import LightRAG integration (document-level knowledge graph)
try:
    from app.utils.lightrag_integration import (
        get_lightrag_service,
        close_lightrag_service,
        LightRAGService,
        LIGHTRAG_AVAILABLE,
        LIGHTRAG_ENABLED,
    )
    LIGHTRAG_INTEGRATION_AVAILABLE = LIGHTRAG_AVAILABLE and LIGHTRAG_ENABLED
except ImportError as e:
    logger.warning(f"LightRAG integration not available: {e}")
    LIGHTRAG_INTEGRATION_AVAILABLE = False


# =============================================================================
# CONFIGURATION
# =============================================================================

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "768"))  # nomic-embed-text default

# Global instances
embedder = None
vector_store = None
document_processor = None
query_processor = None
ollama_service = None
knowledge_graph = None
hybrid_pipeline = None
lightrag_service = None
web_crawler = None
agentic_crawler = None
# Advanced RAG components
neural_reranker = None
hyde_generator = None
query_decomposer = None
query_expander = None
parent_child_chunker = None
agent_pipeline = None
semantic_clustering_engine = None


def get_embedder():
    """Lazy load embedding model on first use to avoid OOM at startup"""
    global embedder
    if embedder is None:
        try:
            from sentence_transformers import SentenceTransformer
            embedder = SentenceTransformer(EMBEDDING_MODEL)
            logger.info(f"✅ Lazy loaded embedding model: {EMBEDDING_MODEL}")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise HTTPException(status_code=503, detail="Embedding model unavailable")
    return embedder


# =============================================================================
# LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup"""
    global embedder, vector_store, document_processor, query_processor, ollama_service, knowledge_graph, hybrid_pipeline, web_crawler, agentic_crawler, neural_reranker, hyde_generator, query_decomposer, query_expander, parent_child_chunker, agent_pipeline

    # Skip embedding model loading at startup to avoid OOM
    # Model will be loaded lazily on first embed request
    embedder = None
    logger.info("✅ Embedding model configured (will load on first use)")

    # Initialize vector store (PostgreSQL required - no fallback)
    if DB_AVAILABLE:
        try:
            config = VectorStoreConfig(
                embedding_dim=EMBEDDING_DIM,
                index_type="hnsw",
                distance_metric="cosine"
            )
            vector_store = await get_vector_store(config=config)
            logger.info("✅ Vector store initialized with PostgreSQL + pgvector")
        except Exception as e:
            logger.error(f"Failed to initialize vector store: {e}")
            logger.error("PostgreSQL with pgvector is required for RAG service")
            vector_store = None
    else:
        logger.error("Database module not available - RAG service requires PostgreSQL")
        vector_store = None

    # Initialize document processor
    try:
        document_processor = get_document_processor()
        logger.info("✅ Document processor initialized")
    except Exception as e:
        logger.error(f"Failed to initialize document processor: {e}")
        document_processor = None

    # Initialize query processor
    if vector_store and embedder:
        try:
            query_processor = get_query_processor(vector_store, embedder)
            logger.info("✅ Query processor initialized")
        except Exception as e:
            logger.error(f"Failed to initialize query processor: {e}")
            query_processor = None

    # Initialize Ollama optimization service
    if OLLAMA_OPTIMIZATION_AVAILABLE:
        try:
            ollama_service = get_ollama_service()
            await ollama_service.start()
            logger.info("✅ Ollama optimization service started")
        except Exception as e:
            logger.error(f"Failed to start Ollama optimization service: {e}")
            ollama_service = None

    # Initialize Knowledge Graph (doesn't require embedder - uses spaCy/Ollama)
    if KNOWLEDGE_GRAPH_AVAILABLE:
        try:
            knowledge_graph = get_knowledge_graph()
            # Use spaCy by default (fast), enable Ollama for deep extraction if available
            use_ollama = ollama_service is not None
            hybrid_pipeline = get_hybrid_pipeline(
                use_spacy=True,
                use_ollama=use_ollama
            )
            logger.info(f"✅ Knowledge graph initialized (spaCy: True, Ollama: {use_ollama})")
        except Exception as e:
            logger.error(f"Failed to initialize knowledge graph: {e}")
            knowledge_graph = None
            hybrid_pipeline = None

    # Initialize LightRAG (document-level knowledge graph)
    global lightrag_service
    if LIGHTRAG_INTEGRATION_AVAILABLE:
        try:
            lightrag_service = await get_lightrag_service()
            if lightrag_service._initialized:
                logger.info("✅ LightRAG initialized (document knowledge graph)")
            else:
                logger.warning("LightRAG loaded but not initialized (missing dependencies)")
                lightrag_service = None
        except Exception as e:
            logger.warning(f"LightRAG init failed (non-fatal): {e}")
            lightrag_service = None

    # Initialize Web Crawler
    if WEB_CRAWLER_AVAILABLE:
        try:
            web_crawler = get_crawler()
            agentic_crawler = get_agentic_crawler()
            logger.info("✅ Web crawler initialized")
        except Exception as e:
            logger.error(f"Failed to initialize web crawler: {e}")
            web_crawler = None
            agentic_crawler = None

    # Initialize Advanced RAG components
    if RERANKER_AVAILABLE:
        try:
            neural_reranker = get_reranker()
            logger.info("✅ Neural reranker initialized (lazy loading enabled)")
        except Exception as e:
            logger.error(f"Failed to initialize neural reranker: {e}")
            neural_reranker = None

    if HYDE_AVAILABLE:
        try:
            hyde_generator = get_hyde_generator()
            logger.info("✅ HyDE generator initialized")
        except Exception as e:
            logger.error(f"Failed to initialize HyDE generator: {e}")
            hyde_generator = None

    if QUERY_DECOMPOSITION_AVAILABLE:
        try:
            query_decomposer = get_query_decomposer()
            logger.info("✅ Query decomposer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize query decomposer: {e}")
            query_decomposer = None

    if QUERY_EXPANSION_AVAILABLE:
        try:
            query_expander = get_query_expander(embedder=embedder)
            logger.info("✅ Query expander initialized")
        except Exception as e:
            logger.error(f"Failed to initialize query expander: {e}")
            query_expander = None

    if PARENT_CHILD_CHUNKING_AVAILABLE:
        try:
            parent_child_chunker = ParentChildChunker(
                parent_chunk_size=1024,
                child_chunk_size=256,
                parent_chunk_overlap=128,
                child_chunk_overlap=32
            )
            logger.info("✅ Parent-child chunker initialized")
        except Exception as e:
            logger.error(f"Failed to initialize parent-child chunker: {e}")
            parent_child_chunker = None

    # Initialize Multi-Agent Pipeline
    if AGENT_PIPELINE_AVAILABLE and ollama_service and vector_store:
        try:
            agent_pipeline = create_agent_pipeline(
                ollama_client=ollama_service.client,
                model_router=ollama_service.router,
                vector_store=vector_store,
                embedding_model=embedder,
                reranker=neural_reranker,
                max_iterations=3,
                quality_threshold=0.75,
            )
            logger.info("✅ Multi-agent pipeline initialized")

            # Register AI Tools for data analysis
            if TOOLS_AVAILABLE:
                try:
                    db_pool = await get_db_pool() if DB_AVAILABLE else None
                    tools = create_all_tools(db_pool=db_pool)

                    for tool_name, tool in tools.items():
                        agent_pipeline.register_tool(tool_name, tool)

                    logger.info(f"✅ Registered {len(tools)} AI tools: {list(tools.keys())}")
                except Exception as e:
                    logger.warning(f"Failed to register tools: {e}")

        except Exception as e:
            logger.error(f"Failed to initialize agent pipeline: {e}")
            agent_pipeline = None

    # Start continuous RAGAS evaluation runner
    eval_runner = None
    if EVAL_RUNNER_AVAILABLE:
        try:
            eval_runner = get_eval_runner()
            await eval_runner.start()
            logger.info("✅ Continuous RAGAS eval runner started")
        except Exception as e:
            logger.warning(f"Failed to start eval runner: {e}")
            eval_runner = None

    yield

    # Cleanup eval runner
    if eval_runner:
        try:
            await eval_runner.stop()
        except Exception:
            pass

    # Cleanup
    if ollama_service:
        try:
            await ollama_service.stop()
            logger.info("🛑 Ollama optimization service stopped")
        except Exception as e:
            logger.warning(f"Error stopping Ollama service: {e}")

    # Cleanup LightRAG
    if lightrag_service:
        try:
            await close_lightrag_service()
            logger.info("LightRAG service closed")
        except Exception as e:
            logger.warning(f"Error closing LightRAG: {e}")

    # Cleanup web crawler
    if WEB_CRAWLER_AVAILABLE:
        try:
            await cleanup_crawler()
            await cleanup_agentic_crawler()
            logger.info("🛑 Web crawler stopped")
        except Exception as e:
            logger.warning(f"Error stopping web crawler: {e}")

    await close_vector_store()
    if DB_AVAILABLE:
        try:
            await close_db_pool()
        except Exception:
            pass


app = FastAPI(
    title="DriveSentinel RAG Service",
    description="Document retrieval and embeddings with pgvector",
    version="2.0.0",
    lifespan=lifespan
)

# Add API versioning middleware
try:
    from assemblyline_common.api.versioning import APIVersionMiddleware
    app.add_middleware(APIVersionMiddleware, default_version="1")
    logger.info("API versioning middleware added")
except ImportError as e:
    logger.warning(f"API versioning not available: {e}")

# Add Prometheus metrics endpoint and middleware
if OBSERVABILITY_AVAILABLE:
    try:
        from assemblyline_common.observability.prometheus_metrics import (
            PrometheusMiddleware, create_metrics_endpoint
        )
        app.add_middleware(PrometheusMiddleware, service_name="rag")
        create_metrics_endpoint(app)
        logger.info("Prometheus /metrics endpoint added to RAG service")
    except ImportError as e:
        logger.warning(f"Could not add Prometheus metrics: {e}")


# =============================================================================
# SCHEMAS
# =============================================================================

class SearchRequest(BaseModel):
    query: str
    top_k: int = 10
    min_score: float = 0.3  # Balanced threshold - typical semantic scores are 0.2-0.5 for relevant content
    document_ids: Optional[List[int]] = None
    metadata_filter: Optional[Dict[str, Any]] = None
    stream: bool = False
    # Enterprise department isolation (optional)
    organization_id: Optional[int] = None  # Search across all users in organization
    department: Optional[str] = None  # Search within specific department (requires organization_id)
    # Service-to-service auth (optional) - used when orchestrator calls RAG without JWT
    user_id: Optional[int] = None  # For internal service calls that already validated user


class SearchResult(BaseModel):
    chunk_id: int
    document_id: int
    content: str
    score: float
    metadata: Optional[Dict[str, Any]] = None


class EmbedRequest(BaseModel):
    texts: List[str]


class IndexRequest(BaseModel):
    document_id: int
    chunks: List[Dict[str, Any]]  # {"content": str, "metadata": dict}


class BatchIndexRequest(BaseModel):
    documents: List[IndexRequest]


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    return {
        "service": "T.A.L.O.S. RAG Service",
        "version": "2.0.0",
        "status": "running",
        "port": 7002,
        "embedding_model": EMBEDDING_MODEL,
        "embedding_dim": EMBEDDING_DIM,
        "vector_store": "pgvector",
        "database": "postgresql" if DB_AVAILABLE else "unavailable"
    }


@app.get("/health")
async def health():
    stats = await vector_store.get_stats() if vector_store else {}
    return {
        "status": "healthy" if vector_store else "degraded",
        "service": "rag",
        "database": DB_AVAILABLE,
        "embedding_model_configured": True,  # Lazy loaded on first use
        "embedding_model_loaded": embedder is not None,
        "vector_store_initialized": vector_store is not None,
        "vector_store_stats": stats
    }


@app.get("/storage")
async def storage_info():
    """Get RAG storage usage information"""
    import shutil

    # Get disk usage for the data directory
    data_dir = os.getenv("DATA_DIR", "/app/data")

    try:
        # Get disk usage stats
        total, used, free = shutil.disk_usage(data_dir if os.path.exists(data_dir) else "/")

        # Get vector store stats if available (with defensive error handling)
        vs_stats = {}
        vectors_count = 0
        if vector_store is not None:
            try:
                vs_stats = await vector_store.get_stats()
                # Support both 'total_vectors' and 'total_chunks' field names
                vectors_count = vs_stats.get("total_vectors", vs_stats.get("total_chunks", 0))
            except Exception as vs_error:
                logger.warning(f"Could not get vector store stats: {vs_error}")
                # Continue with zero counts rather than failing

        # Estimate RAG-specific storage (embeddings + metadata)
        # Each vector is approximately 1536 floats * 4 bytes = 6KB, plus metadata
        estimated_rag_size = vectors_count * 8 * 1024  # ~8KB per vector with overhead

        return {
            "used_bytes": estimated_rag_size,
            "total_bytes": total,
            "free_bytes": free,
            "percentage": (estimated_rag_size / total * 100) if total > 0 else 0,
            "vectors_count": vectors_count,
            "disk_used_bytes": used,
            "disk_percentage": (used / total * 100) if total > 0 else 0
        }
    except Exception as e:
        logger.error(f"Failed to get storage info: {e}")
        return {
            "used_bytes": 0,
            "total_bytes": 0,
            "percentage": 0,
            "error": str(e)
        }


@app.post("/embed")
async def generate_embeddings(request: EmbedRequest):
    """Generate embeddings for texts"""
    # Lazy load embedder if not already loaded
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    embeddings = get_embedder().encode(request.texts, normalize_embeddings=True).tolist()
    return {
        "embeddings": embeddings,
        "dimensions": len(embeddings[0]) if embeddings else 0,
        "count": len(embeddings)
    }


# Batch embedding configuration
EMBEDDING_BATCH_SIZE = int(os.getenv("EMBEDDING_BATCH_SIZE", 32))


# =============================================================================
# QUALITY METRICS HELPER
# =============================================================================

async def record_quality_metrics(
    query: str,
    context_chunks: List[Dict[str, Any]],
    response_time_ms: float,
    user_id: Optional[int] = None,
    model_used: str = "unknown",
    agent_used: str = "rag",
    was_successful: bool = True,
    error_message: Optional[str] = None
) -> Optional[int]:
    """
    Record quality metrics for a RAG query to the database.

    Calculates simplified RAGAS-like metrics:
    - Context precision: Based on similarity scores of retrieved chunks
    - Context recall: Estimated from coverage (chunks retrieved vs available)
    - Answer relevancy: Estimated from query-context alignment
    - Faithfulness: Estimated from context availability

    Returns the metric ID if successful, None otherwise.
    """
    if not DB_AVAILABLE:
        return None

    try:
        pool = await get_db_pool()

        # Calculate metrics from context chunks
        if context_chunks:
            # Context precision: average similarity score of chunks
            scores = [c.get("score", 0.5) for c in context_chunks]
            context_precision = sum(scores) / len(scores) if scores else 0.0

            # Context recall: estimate based on number of chunks (more = better recall)
            chunks_retrieved = len(context_chunks)
            context_recall = min(1.0, chunks_retrieved / 5.0)  # 5 chunks = 100% recall

            # Answer relevancy: based on top chunk score
            answer_relevancy = max(scores) if scores else 0.0

            # Faithfulness: high if we have good context
            faithfulness = context_precision * 0.8 + 0.2 if context_precision > 0.5 else context_precision
        else:
            context_precision = 0.0
            context_recall = 0.0
            answer_relevancy = 0.0
            faithfulness = 0.0
            chunks_retrieved = 0

        # Overall score
        overall_score = (faithfulness + answer_relevancy + context_precision + context_recall) / 4

        # Generate a query ID
        import uuid
        query_id = str(uuid.uuid4())[:8]

        async with pool.acquire() as conn:
            result = await conn.fetchrow("""
                INSERT INTO quality_metrics (
                    query_id, user_id, faithfulness, answer_relevancy,
                    context_precision, context_recall, overall_score,
                    response_time_ms, chunks_retrieved, model_used,
                    agent_used, was_successful, error_message, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id
            """,
                query_id, user_id, faithfulness, answer_relevancy,
                context_precision, context_recall, overall_score,
                int(response_time_ms), chunks_retrieved, model_used,
                agent_used, was_successful, error_message,
                json.dumps({"query_preview": query[:100]})
            )

            logger.info(f"📊 Recorded quality metrics: overall={overall_score:.2f}, chunks={chunks_retrieved}")
            return result["id"] if result else None

    except Exception as e:
        logger.warning(f"Failed to record quality metrics: {e}")
        return None


@app.post("/index")
async def index_document(request: IndexRequest):
    """
    Index document chunks with embeddings.

    Features:
    - Batch embedding generation (configurable batch size)
    - Progress tracking for large documents
    - Optimized for speed with sentence-transformers
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    start_time = time.time()

    # Extract content for embedding
    contents = [chunk.get("content", "") for chunk in request.chunks]
    valid_indices = [i for i, c in enumerate(contents) if c.strip()]
    valid_contents = [contents[i] for i in valid_indices]
    valid_chunks = [request.chunks[i] for i in valid_indices]

    if not valid_contents:
        return {"success": True, "indexed_chunks": 0, "message": "No content to index"}

    # Generate embeddings in optimized batches
    all_embeddings = []
    embed_start = time.time()

    for i in range(0, len(valid_contents), EMBEDDING_BATCH_SIZE):
        batch = valid_contents[i:i + EMBEDDING_BATCH_SIZE]
        batch_embeddings = get_embedder().encode(
            batch,
            normalize_embeddings=True,
            show_progress_bar=False,
            batch_size=EMBEDDING_BATCH_SIZE
        ).tolist()
        all_embeddings.extend(batch_embeddings)

    embed_time = (time.time() - embed_start) * 1000

    # Upsert to vector store
    upsert_start = time.time()
    count = await vector_store.upsert(
        document_id=request.document_id,
        chunks=valid_chunks,
        embeddings=all_embeddings
    )
    upsert_time = (time.time() - upsert_start) * 1000

    processing_time = (time.time() - start_time) * 1000

    logger.info(
        f"📥 Indexed {count} chunks for document {request.document_id} | "
        f"embed: {embed_time:.1f}ms, upsert: {upsert_time:.1f}ms, total: {processing_time:.1f}ms"
    )

    return {
        "success": True,
        "indexed_chunks": count,
        "document_id": request.document_id,
        "processing_time_ms": processing_time,
        "embedding_time_ms": embed_time,
        "upsert_time_ms": upsert_time,
        "batch_size": EMBEDDING_BATCH_SIZE,
        "avg_ms_per_chunk": processing_time / max(count, 1)
    }


@app.post("/index/batch")
async def batch_index_documents(request: BatchIndexRequest):
    """Batch index multiple documents"""
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    results = []
    total_chunks = 0
    start_time = time.time()

    for doc in request.documents:
        try:
            result = await index_document(doc)
            results.append(result)
            total_chunks += result["indexed_chunks"]
        except Exception as e:
            results.append({
                "success": False,
                "document_id": doc.document_id,
                "error": str(e)
            })

    return {
        "success": True,
        "documents_processed": len(request.documents),
        "total_chunks_indexed": total_chunks,
        "processing_time_ms": (time.time() - start_time) * 1000,
        "results": results
    }


async def get_user_search_scope(user_id: int) -> dict:
    """
    Determine the appropriate search scope based on user's license tier.

    Returns:
        dict with keys:
        - license_tier: 'free', 'pro', or 'enterprise'
        - organization_id: user's organization ID
        - department: user's department (for enterprise)
        - accessible_departments: list of departments user can access (for enterprise)
        - search_mode: 'user_only', 'organization', or 'department'
    """
    if not DB_AVAILABLE:
        return {"search_mode": "user_only", "license_tier": "pro"}

    try:
        from assemblyline_common.database import get_connection, DepartmentAccessGrantRepository

        async with get_connection() as conn:
            # Get user with organization info
            user = await conn.fetchrow("""
                SELECT u.id, u.organization_id, u.department,
                       o.license_tier, o.name as org_name
                FROM users u
                LEFT JOIN organizations o ON o.id = u.organization_id
                WHERE u.id = $1
            """, user_id)

            if not user or not user['organization_id']:
                return {"search_mode": "user_only", "license_tier": "pro"}

            license_tier = user['license_tier'] or 'pro'

            if license_tier in ('free', 'pro'):
                # Consumer tier: search all documents in organization (shared workspace)
                return {
                    "search_mode": "organization",
                    "license_tier": license_tier,
                    "organization_id": user['organization_id'],
                    "department": None,
                    "accessible_departments": None
                }
            else:
                # Enterprise tier: search only user's department + granted departments
                accessible_depts = await DepartmentAccessGrantRepository.get_accessible_departments(
                    user_id=user_id,
                    organization_id=user['organization_id']
                )

                return {
                    "search_mode": "department",
                    "license_tier": license_tier,
                    "organization_id": user['organization_id'],
                    "department": user['department'],
                    "accessible_departments": accessible_depts
                }
    except Exception as e:
        logger.warning(f"Failed to get user search scope: {e}")
        return {"search_mode": "user_only", "license_tier": "pro"}


@app.post("/search")
async def search(request: SearchRequest, http_request: Request):
    """
    Ultra-fast semantic search using pgvector with user isolation

    Matches Memobytes pattern:
    - Cosine similarity with HNSW index
    - <50ms search time vs 5-15s AI processing
    - Streaming support for large result sets
    - User-level data isolation for multi-tenancy

    Access Control:
    - Consumer/Pro tier: Search all documents in organization (shared workspace)
    - Enterprise tier: Search only user's department + granted departments
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    # Extract user_id for multi-tenant isolation
    # First try JWT auth, then fall back to request body for service-to-service calls
    user_id = get_user_id_from_request(http_request)
    if user_id is None and request.user_id:
        # Service-to-service call with pre-validated user_id
        user_id = request.user_id
        logger.debug(f"Using user_id from request body: {user_id}")
    if user_id is None:
        raise HTTPException(
            status_code=401,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"}
        )

    # Determine search scope based on license tier
    search_scope = await get_user_search_scope(user_id)
    logger.debug(f"Search scope for user {user_id}: {search_scope}")

    # Apply access control
    organization_id = None
    department = None

    if search_scope["search_mode"] == "organization":
        # Consumer/Pro: search all org documents
        organization_id = search_scope["organization_id"]
    elif search_scope["search_mode"] == "department":
        # Enterprise: search user's accessible departments
        organization_id = search_scope["organization_id"]
        # If user specifies a department, verify they have access
        if request.department:
            if request.department not in search_scope["accessible_departments"]:
                raise HTTPException(
                    status_code=403,
                    detail=f"You don't have access to the {request.department} department"
                )
            department = request.department
        else:
            # Default to user's own department
            department = search_scope["department"]

    start_time = time.time()

    # Generate query embedding
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    # Stream results if requested
    if request.stream:
        return StreamingResponse(
            stream_search_results(
                query_embedding=query_embedding,
                user_id=user_id,
                limit=request.top_k,
                threshold=request.min_score,
                document_ids=request.document_ids,
                metadata_filter=request.metadata_filter,
                organization_id=organization_id,
                department=department
            ),
            media_type="text/event-stream"
        )

    # Standard search with access control
    # Get more results with low threshold to allow filename boosting to surface relevant docs
    results = await vector_store.search(
        query_embedding=query_embedding,
        user_id=user_id,
        limit=request.top_k * 5,  # Get more results for filename boosting to work with
        threshold=min(request.min_score * 0.2, 0.05),  # Very low threshold, filter after boosting
        document_ids=request.document_ids,
        metadata_filter=request.metadata_filter,
        organization_id=organization_id,
        department=department
    )

    # Apply filename boosting to help surface documents when user mentions their name
    query_lower = request.query.lower()
    query_terms = [t for t in query_lower.split() if len(t) > 2]

    for result in results:
        original_score = result.get("score", 0.0)
        boosted_score = original_score

        # Check if query terms match filename
        metadata = result.get("metadata", {})
        source = metadata.get("source", "") or metadata.get("filename", "")
        if source:
            source_lower = source.lower().replace("_", " ").replace("-", " ").replace(".", " ")
            filename_matches = sum(1 for term in query_terms if term in source_lower)
            if filename_matches > 0:
                # Significant boost for filename matches (0.3 per term, max 0.6)
                filename_boost = min(filename_matches * 0.3, 0.6)
                boosted_score = min(original_score + filename_boost, 1.0)
                logger.debug(f"Filename boost +{filename_boost:.2f} for '{source}'")

        result["score"] = boosted_score
        result["original_score"] = original_score

    # Re-sort by boosted score and filter
    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    results = [r for r in results if r.get("score", 0) >= request.min_score][:request.top_k]

    search_time = (time.time() - start_time) * 1000
    logger.info(f"🚀 Vector search: {search_time:.1f}ms, {len(results)} results")

    # Record quality metrics (non-blocking)
    try:
        await record_quality_metrics(
            query=request.query,
            context_chunks=results,
            response_time_ms=search_time,
            model_used="semantic_vector",
            agent_used="rag_search",
            was_successful=True,
            error_message=None
        )
    except Exception as e:
        logger.warning(f"Failed to record search quality metrics: {e}")

    return {
        "results": results,
        "query": request.query,
        "total_results": len(results),
        "search_time_ms": search_time,
        "search_method": "semantic_vector"
    }


async def stream_search_results(
    query_embedding: List[float],
    user_id: int,
    limit: int,
    threshold: float,
    document_ids: Optional[List[int]],
    metadata_filter: Optional[Dict[str, Any]],
    organization_id: Optional[int] = None,
    department: Optional[str] = None
):
    """
    Stream search results using SSE with user isolation
    Matches Memobytes async pipeline pattern

    Supports enterprise department filtering via organization_id and department params.
    """
    yield f"data: {json.dumps({'type': 'started', 'message': 'Search initiated'})}\n\n"

    result_count = 0

    async for result in vector_store.stream_search(
        query_embedding=query_embedding,
        user_id=user_id,
        limit=limit,
        batch_size=5,
        organization_id=organization_id,
        department=department
    ):
        if result.get("score", 0) >= threshold:
            if document_ids is None or result.get("document_id") in document_ids:
                result_count += 1
                yield f"data: {json.dumps({'type': 'result', 'data': result})}\n\n"

    yield f"data: {json.dumps({'type': 'complete', 'total_results': result_count})}\n\n"


@app.post("/search/hybrid")
async def hybrid_search(request: SearchRequest):
    """
    Hybrid search combining vector similarity and keyword matching
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    start_time = time.time()

    # Vector search
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    vector_results = await vector_store.search(
        query_embedding=query_embedding,
        limit=request.top_k * 2,  # Get more for reranking
        threshold=request.min_score * 0.8,  # Lower threshold for hybrid
        document_ids=request.document_ids
    )

    # Keyword boost (simple implementation)
    query_words = set(request.query.lower().split())

    for result in vector_results:
        content_words = set(result["content"].lower().split())
        keyword_overlap = len(query_words & content_words) / max(len(query_words), 1)

        # Boost score with keyword match
        result["keyword_score"] = keyword_overlap
        result["hybrid_score"] = (result["score"] * 0.7) + (keyword_overlap * 0.3)

    # Re-rank by hybrid score
    vector_results.sort(key=lambda x: x["hybrid_score"], reverse=True)
    results = vector_results[:request.top_k]

    search_time = (time.time() - start_time) * 1000

    return {
        "results": results,
        "query": request.query,
        "total_results": len(results),
        "search_time_ms": search_time,
        "search_method": "hybrid"
    }


# =============================================================================
# ADVANCED RAG ENDPOINTS
# =============================================================================

class AdvancedSearchRequest(BaseModel):
    query: str
    top_k: int = 10
    min_score: float = 0.5
    document_ids: Optional[List[int]] = None
    use_hyde: bool = True
    use_reranking: bool = True
    use_decomposition: bool = False
    decomposition_strategy: str = "simple"  # simple, sequential, multi_hop
    use_query_expansion: bool = False
    expansion_config: Optional[Dict[str, Any]] = None  # {use_synonyms, use_paraphrases, max_queries}


@app.post("/search/advanced")
async def advanced_search(request: AdvancedSearchRequest):
    """
    Advanced RAG search with neural reranking, HyDE, query expansion, and decomposition.

    Pipeline:
    1. Query expansion (optional) - generate synonyms and paraphrases
    2. Query decomposition (optional) - break complex queries into sub-queries
    3. HyDE (optional) - generate hypothetical documents for better embedding
    4. Vector search with HNSW
    5. Neural reranking with cross-encoder (optional)

    Features:
    - Query Expansion: Generates synonym substitutions and LLM paraphrases
    - HyDE: Generates hypothetical document to improve semantic matching
    - Neural Reranking: Uses cross-encoder for more accurate relevance scoring
    - Query Decomposition: Breaks complex queries into simpler sub-queries
    """
    # Lazy load embedder if not already loaded
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    start_time = time.time()
    search_metadata = {
        "hyde_used": False,
        "reranking_used": False,
        "decomposition_used": False,
        "query_expansion_used": False,
        "sub_queries": [],
        "expanded_queries": []
    }

    # Step 0: Query Expansion (optional) - generates synonyms and paraphrases
    queries_to_expand = [request.query]

    if request.use_query_expansion and QUERY_EXPANSION_AVAILABLE and query_expander:
        try:
            # Build expansion config from request
            exp_config = ExpansionConfig(
                use_synonyms=request.expansion_config.get("use_synonyms", True) if request.expansion_config else True,
                use_paraphrases=request.expansion_config.get("use_paraphrases", True) if request.expansion_config else True,
                max_total_queries=request.expansion_config.get("max_queries", 5) if request.expansion_config else 5,
                include_original=True
            )

            expansion_result = await query_expander.expand(request.query, exp_config)

            if len(expansion_result.expanded_queries) > 1:
                queries_to_expand = expansion_result.expanded_queries
                search_metadata["query_expansion_used"] = True
                search_metadata["expanded_queries"] = queries_to_expand
                logger.info(f"Query expanded into {len(queries_to_expand)} variants")

        except Exception as e:
            logger.warning(f"Query expansion failed, using original query: {e}")

    # Step 1: Query Decomposition (optional) - applied to each expanded query
    queries_to_search = queries_to_expand.copy()

    if request.use_decomposition and QUERY_DECOMPOSITION_AVAILABLE and query_decomposer:
        try:
            strategy = DecompositionStrategy(request.decomposition_strategy)
            decomposed = await query_decomposer.decompose(request.query, strategy)

            if len(decomposed.sub_queries) > 1:
                queries_to_search = [sq.query for sq in decomposed.sub_queries]
                search_metadata["decomposition_used"] = True
                search_metadata["sub_queries"] = queries_to_search
                search_metadata["decomposition_strategy"] = request.decomposition_strategy
                logger.info(f"Query decomposed into {len(queries_to_search)} sub-queries")

        except Exception as e:
            logger.warning(f"Query decomposition failed, using original query: {e}")

    # Step 2: Generate embeddings (with optional HyDE)
    all_results = []

    for query in queries_to_search:
        if request.use_hyde and HYDE_AVAILABLE and hyde_generator:
            try:
                # Generate hypothetical document and use its embedding
                hyde_result = await hyde_generator.generate(query)
                if hyde_result and hyde_result.hypothetical_documents:
                    # Combine query with hypothetical document for embedding
                    combined_text = f"{query}\n\n{hyde_result.hypothetical_documents[0]}"
                    query_embedding = get_embedder().encode(
                        combined_text,
                        normalize_embeddings=True
                    ).tolist()
                    search_metadata["hyde_used"] = True
                    logger.debug("Using HyDE-enhanced embedding")
                else:
                    query_embedding = get_embedder().encode(
                        query,
                        normalize_embeddings=True
                    ).tolist()
            except Exception as e:
                logger.warning(f"HyDE generation failed, using standard embedding: {e}")
                query_embedding = get_embedder().encode(
                    query,
                    normalize_embeddings=True
                ).tolist()
        else:
            query_embedding = get_embedder().encode(
                query,
                normalize_embeddings=True
            ).tolist()

        # Vector search
        results = await vector_store.search(
            query_embedding=query_embedding,
            limit=request.top_k * 2 if request.use_reranking else request.top_k,
            threshold=request.min_score * 0.8 if request.use_reranking else request.min_score,
            document_ids=request.document_ids
        )

        all_results.extend(results)

    # Deduplicate results by chunk_id
    seen_chunks = set()
    unique_results = []
    for result in all_results:
        chunk_key = (result["document_id"], result["chunk_id"])
        if chunk_key not in seen_chunks:
            seen_chunks.add(chunk_key)
            unique_results.append(result)

    # Step 3: Neural Reranking (optional)
    if request.use_reranking and RERANKER_AVAILABLE and neural_reranker:
        try:
            ranked_results = await neural_reranker.rerank(
                query=request.query,
                chunks=unique_results,
                top_k=request.top_k
            )

            # Convert RankedChunk back to dict format
            final_results = [
                {
                    "document_id": r.document_id,
                    "chunk_id": r.chunk_index,
                    "content": r.content,
                    "score": r.rerank_score,
                    "original_score": r.original_score,
                    "metadata": r.metadata
                }
                for r in ranked_results
            ]
            search_metadata["reranking_used"] = True
            logger.debug("Applied neural reranking")

        except Exception as e:
            logger.warning(f"Neural reranking failed, using original scores: {e}")
            final_results = unique_results[:request.top_k]
    else:
        # Sort by score and limit
        unique_results.sort(key=lambda x: x["score"], reverse=True)
        final_results = unique_results[:request.top_k]

    search_time = (time.time() - start_time) * 1000

    return {
        "results": final_results,
        "query": request.query,
        "total_results": len(final_results),
        "search_time_ms": search_time,
        "search_method": "advanced",
        "pipeline": search_metadata
    }


@app.post("/search/rerank")
async def search_with_reranking(request: SearchRequest):
    """
    Search with neural reranking.

    Retrieves candidates with vector search, then re-scores with cross-encoder.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if not RERANKER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Reranker not available")

    start_time = time.time()

    # Generate query embedding
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    # Get more candidates for reranking
    candidates = await vector_store.search(
        query_embedding=query_embedding,
        limit=request.top_k * 3,
        threshold=request.min_score * 0.7,
        document_ids=request.document_ids
    )

    # Rerank
    reranker = get_reranker()
    ranked = await reranker.rerank(
        query=request.query,
        chunks=candidates,
        top_k=request.top_k
    )

    results = [
        {
            "document_id": r.document_id,
            "chunk_id": r.chunk_index,
            "content": r.content,
            "score": r.rerank_score,
            "original_score": r.original_score,
            "metadata": r.metadata
        }
        for r in ranked
    ]

    search_time = (time.time() - start_time) * 1000

    return {
        "results": results,
        "query": request.query,
        "total_results": len(results),
        "search_time_ms": search_time,
        "search_method": "reranked",
        "candidates_considered": len(candidates)
    }


@app.post("/search/hyde")
async def search_with_hyde(request: SearchRequest):
    """
    Search with HyDE (Hypothetical Document Embeddings).

    Generates a hypothetical answer to the query and uses its embedding
    for better semantic matching.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if not HYDE_AVAILABLE:
        raise HTTPException(status_code=503, detail="HyDE not available")

    start_time = time.time()

    # Generate hypothetical document
    hyde_gen = get_hyde_generator()
    hyde_result = await hyde_gen.generate(request.query)

    if not hyde_result or not hyde_result.hypothetical_documents:
        # Fall back to standard search
        query_embedding = get_embedder().encode(
            request.query,
            normalize_embeddings=True
        ).tolist()
        hyde_used = False
    else:
        # Use hypothetical document for embedding
        combined = f"{request.query}\n\n{hyde_result.hypothetical_documents[0]}"
        query_embedding = get_embedder().encode(
            combined,
            normalize_embeddings=True
        ).tolist()
        hyde_used = True

    # Search
    results = await vector_store.search(
        query_embedding=query_embedding,
        limit=request.top_k,
        threshold=request.min_score,
        document_ids=request.document_ids
    )

    search_time = (time.time() - start_time) * 1000

    return {
        "results": results,
        "query": request.query,
        "total_results": len(results),
        "search_time_ms": search_time,
        "search_method": "hyde",
        "hyde_used": hyde_used,
        "hypothetical_document": hyde_result.hypothetical_documents[0] if hyde_used else None
    }


class DecomposeRequest(BaseModel):
    query: str
    strategy: str = "simple"  # simple, sequential, multi_hop
    max_sub_queries: int = 4


@app.post("/query/decompose")
async def decompose_query_endpoint(request: DecomposeRequest):
    """
    Decompose a complex query into simpler sub-queries.

    Strategies:
    - simple: Independent sub-queries that can be searched in parallel
    - sequential: Sub-queries that build on previous answers
    - multi_hop: For multi-hop reasoning questions
    """
    if not QUERY_DECOMPOSITION_AVAILABLE:
        raise HTTPException(status_code=503, detail="Query decomposition not available")

    decomposer = get_query_decomposer()

    try:
        strategy = DecompositionStrategy(request.strategy)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid strategy: {request.strategy}")

    result = await decomposer.decompose(
        query=request.query,
        strategy=strategy,
        max_sub_queries=request.max_sub_queries
    )

    return {
        "original_query": result.original_query,
        "strategy": result.strategy.value,
        "reasoning": result.reasoning,
        "sub_queries": [
            {
                "query": sq.query,
                "order": sq.order,
                "depends_on": sq.depends_on,
                "purpose": sq.purpose
            }
            for sq in result.sub_queries
        ],
        "count": len(result.sub_queries)
    }


@app.get("/advanced-rag/status")
async def get_advanced_rag_status():
    """Get status of advanced RAG components"""
    return {
        "reranker": {
            "available": RERANKER_AVAILABLE,
            "initialized": neural_reranker is not None,
            "model": neural_reranker.model_name if neural_reranker else None
        },
        "hyde": {
            "available": HYDE_AVAILABLE,
            "initialized": hyde_generator is not None,
            "model": hyde_generator.model if hyde_generator else None
        },
        "query_decomposition": {
            "available": QUERY_DECOMPOSITION_AVAILABLE,
            "initialized": query_decomposer is not None,
            "strategies": ["simple", "sequential", "multi_hop"]
        },
        "parent_child_chunking": {
            "available": PARENT_CHILD_CHUNKING_AVAILABLE,
            "initialized": parent_child_chunker is not None,
            "config": {
                "parent_chunk_size": 1024,
                "child_chunk_size": 256
            } if parent_child_chunker else None
        }
    }


class ParentChildChunkRequest(BaseModel):
    """Request for parent-child chunking"""
    text: str
    document_id: str
    parent_size: int = 1024
    child_size: int = 256
    metadata: Optional[dict] = None


@app.post("/chunking/parent-child")
async def create_parent_child_chunks_endpoint(request: ParentChildChunkRequest):
    """
    Create parent-child chunks from text.

    Parent chunks provide context, child chunks are used for precise search.
    When a child matches, the parent is returned for broader context.
    """
    if not PARENT_CHILD_CHUNKING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Parent-child chunking not available")

    try:
        # Create a chunker with custom sizes if provided
        chunker = ParentChildChunker(
            parent_chunk_size=request.parent_size,
            child_chunk_size=request.child_size
        )

        parent_chunks, child_chunks = chunker.chunk_document(
            text=request.text,
            document_id=request.document_id,
            metadata=request.metadata
        )

        return {
            "document_id": request.document_id,
            "parent_chunks": [
                {
                    "id": p.id,
                    "content": p.content,
                    "start_char": p.start_char,
                    "end_char": p.end_char
                }
                for p in parent_chunks
            ],
            "child_chunks": [
                {
                    "id": c.id,
                    "content": c.content,
                    "parent_id": c.parent_id,
                    "start_char": c.start_char,
                    "end_char": c.end_char
                }
                for c in child_chunks
            ],
            "stats": {
                "parent_count": len(parent_chunks),
                "child_count": len(child_chunks),
                "avg_children_per_parent": len(child_chunks) / len(parent_chunks) if parent_chunks else 0
            }
        }
    except Exception as e:
        logger.error(f"Parent-child chunking failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats")
async def get_stats():
    """Get RAG service statistics"""
    if vector_store is None:
        return {"error": "Vector store not initialized"}

    stats = await vector_store.get_stats()
    stats.update({
        "embedding_model": EMBEDDING_MODEL,
        "embedding_model_loaded": embedder is not None,
        "database_available": DB_AVAILABLE,
        "document_processor_loaded": document_processor is not None,
        "query_processor_loaded": query_processor is not None,
        "ollama_optimization_available": OLLAMA_OPTIMIZATION_AVAILABLE,
        "ollama_service_running": ollama_service is not None and ollama_service.running,
        "knowledge_graph_available": KNOWLEDGE_GRAPH_AVAILABLE,
        "knowledge_graph_initialized": knowledge_graph is not None,
        "hybrid_pipeline_initialized": hybrid_pipeline is not None,
        # Advanced RAG capabilities
        "advanced_rag": {
            "reranker_available": RERANKER_AVAILABLE,
            "reranker_initialized": neural_reranker is not None,
            "hyde_available": HYDE_AVAILABLE,
            "hyde_initialized": hyde_generator is not None,
            "query_decomposition_available": QUERY_DECOMPOSITION_AVAILABLE,
            "query_decomposer_initialized": query_decomposer is not None,
            "parent_child_chunking_available": PARENT_CHILD_CHUNKING_AVAILABLE,
            "parent_child_chunker_initialized": parent_child_chunker is not None
        }
    })

    # Add knowledge graph stats if available
    if knowledge_graph is not None:
        kg_stats = knowledge_graph.get_stats()
        stats["knowledge_graph_stats"] = kg_stats

    return stats


# =============================================================================
# OLLAMA OPTIMIZATION ENDPOINTS
# =============================================================================

@app.get("/ollama/stats")
async def get_ollama_stats():
    """
    Get Ollama optimization service statistics.

    Returns:
    - Cache hit rate and size
    - Warm models and warmup times
    - Model routing statistics
    - Context optimization metrics
    """
    if not OLLAMA_OPTIMIZATION_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization module not available"
        )

    if ollama_service is None:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization service not running"
        )

    return ollama_service.get_stats()


@app.post("/ollama/warm/{model_name}")
async def warm_model(model_name: str):
    """
    Manually warm up a specific model.

    Preloads model into Ollama memory to avoid cold start latency.
    """
    if ollama_service is None:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization service not running"
        )

    success = await ollama_service.warmer.warm_up(model_name)

    return {
        "success": success,
        "model": model_name,
        "warm_models": ollama_service.warmer.get_warm_models()
    }


@app.post("/ollama/cache/clear")
async def clear_ollama_cache():
    """Clear the Ollama response cache."""
    if ollama_service is None:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization service not running"
        )

    count = ollama_service.clear_cache()

    return {
        "success": True,
        "cleared_entries": count
    }


class OllamaGenerateRequest(BaseModel):
    prompt: str
    task_type: str = "synthesis"
    system: Optional[str] = None
    use_cache: bool = True
    temperature: Optional[float] = None
    max_tokens: Optional[int] = None


@app.post("/ollama/generate")
async def ollama_generate(request: OllamaGenerateRequest):
    """
    Generate response using optimized Ollama client.

    Features:
    - Automatic model selection based on task type
    - Response caching for repeated queries
    - Model warm-up to avoid cold starts

    Task types: classification, simple_qa, extraction, summarization,
                tool_selection, code_gen, synthesis, reasoning, reflection
    """
    if ollama_service is None:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization service not running"
        )

    start_time = time.time()

    options = {}
    if request.temperature is not None:
        options['temperature'] = request.temperature
    if request.max_tokens is not None:
        options['num_predict'] = request.max_tokens

    result = await ollama_service.generate(
        prompt=request.prompt,
        task_type=request.task_type,
        system=request.system,
        use_cache=request.use_cache,
        **options
    )

    generation_time = (time.time() - start_time) * 1000

    return {
        "response": result.get("response", ""),
        "model": result.get("model", "unknown"),
        "generation_time_ms": generation_time,
        "total_duration_ns": result.get("total_duration", 0),
        "prompt_tokens": result.get("prompt_eval_count", 0),
        "response_tokens": result.get("eval_count", 0),
        "cached": generation_time < 100  # Sub-100ms suggests cache hit
    }


@app.post("/ollama/count-tokens")
async def count_tokens(text: str):
    """Count tokens in text for context sizing."""
    if ollama_service is None:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization service not running"
        )

    count = ollama_service.count_tokens(text)

    return {
        "text_length": len(text),
        "token_count": count,
        "avg_chars_per_token": len(text) / max(count, 1)
    }


@app.get("/ollama/models")
async def get_available_models():
    """Get available models and their profiles."""
    if not OLLAMA_OPTIMIZATION_AVAILABLE:
        raise HTTPException(
            status_code=503,
            detail="Ollama optimization module not available"
        )

    from app.ollama import MODEL_PROFILES

    warm_models = []
    if ollama_service:
        warm_models = ollama_service.warmer.get_warm_models()

    return {
        "models": MODEL_PROFILES,
        "warm_models": warm_models,
        "config": OLLAMA_CONFIG if OLLAMA_OPTIMIZATION_AVAILABLE else {}
    }


@app.delete("/documents/{document_id}")
async def delete_document_index(document_id: int):
    """Delete all indexed chunks for a document"""
    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    count = await vector_store.delete_document(document_id)

    if count == 0:
        raise HTTPException(status_code=404, detail="Document not found in index")

    return {"success": True, "deleted_chunks": count, "document_id": document_id}


# =============================================================================
# ASYNC SEARCH PIPELINE (Memobytes Pattern)
# =============================================================================

class AsyncSearchPipeline:
    """
    Non-blocking async pipeline for AI search operations
    Matches Memobytes async_search_pipeline.py
    """

    def __init__(self):
        self.active_pipelines: Dict[str, Dict] = {}

    async def process_query_async(
        self,
        query: str,
        document_ids: Optional[List[int]] = None,
        top_k: int = 10
    ) -> Dict[str, Any]:
        """
        Process query through async pipeline with parallel operations
        """
        start_time = time.time()
        pipeline_id = f"search_{int(start_time * 1000)}"

        self.active_pipelines[pipeline_id] = {
            "status": "processing",
            "stage": "embedding",
            "progress": 0.0
        }

        try:
            # Stage 1: Generate embedding
            self._update_status(pipeline_id, "embedding", 0.2)
            query_embedding = get_embedder().encode(query, normalize_embeddings=True).tolist()

            # Stage 2: Vector search
            self._update_status(pipeline_id, "searching", 0.5)
            results = await vector_store.search(
                query_embedding=query_embedding,
                limit=top_k,
                document_ids=document_ids
            )

            # Stage 3: Context synthesis
            self._update_status(pipeline_id, "synthesizing", 0.8)
            context = self._synthesize_context(results)

            # Complete
            self._update_status(pipeline_id, "complete", 1.0)

            processing_time = (time.time() - start_time) * 1000

            return {
                "pipeline_id": pipeline_id,
                "results": results,
                "synthesized_context": context,
                "processing_time_ms": processing_time,
                "async_processing": True
            }

        except Exception as e:
            self.active_pipelines[pipeline_id]["status"] = "error"
            raise
        finally:
            self.active_pipelines.pop(pipeline_id, None)

    def _update_status(self, pipeline_id: str, stage: str, progress: float):
        if pipeline_id in self.active_pipelines:
            self.active_pipelines[pipeline_id].update({
                "stage": stage,
                "progress": progress
            })

    def _synthesize_context(self, results: List[Dict]) -> str:
        """Create context from search results"""
        if not results:
            return "No relevant content found."

        context_parts = []
        for i, result in enumerate(results[:5], 1):
            content = result["content"][:200]
            score = result["score"]
            context_parts.append(f"{i}. [{score:.2f}] {content}")

        return "\n".join(context_parts)


# Global pipeline instance
_async_pipeline = None


def get_async_pipeline() -> AsyncSearchPipeline:
    global _async_pipeline
    if _async_pipeline is None:
        _async_pipeline = AsyncSearchPipeline()
    return _async_pipeline


@app.post("/search/async")
async def async_search(request: SearchRequest):
    """
    Async pipeline search with parallel processing
    Matches Memobytes async_search_pipeline
    """
    pipeline = get_async_pipeline()
    return await pipeline.process_query_async(
        query=request.query,
        document_ids=request.document_ids,
        top_k=request.top_k
    )


# =============================================================================
# RAG EVALUATION ENDPOINTS (Industry Standard Metrics)
# =============================================================================

class EvaluationQueryRequest(BaseModel):
    query: str
    relevant_chunk_ids: List[int]
    relevant_document_ids: Optional[List[int]] = None
    expected_answer: Optional[str] = None


class EvaluationDatasetRequest(BaseModel):
    queries: List[EvaluationQueryRequest]
    include_generation: bool = False
    answers: Optional[List[str]] = None


@app.post("/evaluate/single")
async def evaluate_single_query(request: EvaluationQueryRequest):
    """
    Evaluate retrieval quality for a single query.

    Computes industry-standard metrics:
    - Precision@k, Recall@k, F1@k
    - Mean Reciprocal Rank (MRR)
    - nDCG@k
    - Hit Rate@k

    Targets:
    - Precision@5 >= 0.7
    - Recall@20 >= 0.8
    - MRR >= 0.6
    """
    from app.utils.evaluator import (
        compute_retrieval_metrics,
        EvaluationQuery,
        EvaluationResult
    )

    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    start_time = time.time()

    # Generate query embedding and search
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    results = await vector_store.search(
        query_embedding=query_embedding,
        limit=20,
        threshold=0.0
    )

    latency_ms = (time.time() - start_time) * 1000

    # Extract retrieved chunk IDs
    retrieved_ids = [r["chunk_id"] for r in results]

    # Compute metrics
    metrics = compute_retrieval_metrics(
        retrieved_ids=retrieved_ids,
        relevant_ids=request.relevant_chunk_ids,
        k_values=[1, 3, 5, 10, 20]
    )

    return {
        "query": request.query,
        "metrics": metrics.to_dict(),
        "latency_ms": latency_ms,
        "retrieved_count": len(results),
        "relevant_count": len(request.relevant_chunk_ids),
        "targets": {
            "precision_5": {"value": metrics.precision_at_k.get(5, 0), "target": 0.7, "passed": metrics.precision_at_k.get(5, 0) >= 0.7},
            "recall_20": {"value": metrics.recall_at_k.get(20, 0), "target": 0.8, "passed": metrics.recall_at_k.get(20, 0) >= 0.8},
            "mrr": {"value": metrics.mrr, "target": 0.6, "passed": metrics.mrr >= 0.6}
        }
    }


@app.post("/evaluate/dataset")
async def evaluate_dataset(request: EvaluationDatasetRequest):
    """
    Evaluate retrieval quality across a dataset of queries.

    Returns aggregated metrics with target compliance.
    """
    from app.utils.evaluator import (
        compute_retrieval_metrics,
        aggregate_metrics,
        EvaluationResult,
        RetrievalMetrics
    )

    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    results = []
    total_start = time.time()

    for i, query_req in enumerate(request.queries):
        start_time = time.time()

        # Search
        query_embedding = get_embedder().encode(
            query_req.query,
            normalize_embeddings=True
        ).tolist()

        search_results = await vector_store.search(
            query_embedding=query_embedding,
            limit=20,
            threshold=0.0
        )

        latency_ms = (time.time() - start_time) * 1000

        # Compute metrics
        retrieved_ids = [r["chunk_id"] for r in search_results]
        metrics = compute_retrieval_metrics(
            retrieved_ids=retrieved_ids,
            relevant_ids=query_req.relevant_chunk_ids,
            k_values=[1, 3, 5, 10, 20]
        )

        results.append(EvaluationResult(
            query=query_req.query,
            retrieval_metrics=metrics,
            latency_ms=latency_ms,
            retrieved_count=len(search_results),
            relevant_count=len(query_req.relevant_chunk_ids)
        ))

        logger.info(f"Evaluated {i+1}/{len(request.queries)}: P@5={metrics.precision_at_k.get(5, 0):.3f}")

    # Aggregate
    summary = aggregate_metrics(results)
    total_time = (time.time() - total_start) * 1000

    return {
        "summary": summary.to_dict(),
        "total_queries": len(request.queries),
        "total_time_ms": total_time,
        "individual_results": [r.to_dict() for r in results]
    }


@app.get("/evaluate/targets")
async def get_evaluation_targets():
    """
    Get the industry-standard evaluation targets.
    """
    return {
        "retrieval": {
            "precision_at_5": {"target": 0.7, "description": "Top-5 results should be 70% relevant"},
            "recall_at_20": {"target": 0.8, "description": "Should retrieve 80% of relevant content in top-20"},
            "mrr": {"target": 0.6, "description": "First relevant result should appear in top 2 on average"},
            "hit_rate_at_5": {"target": 0.9, "description": "90% of queries should have a relevant result in top-5"}
        },
        "generation": {
            "faithfulness": {"target": 0.9, "description": "90% of claims should be supported by context"},
            "answer_relevancy": {"target": 0.8, "description": "Answers should be 80% relevant to questions"},
            "hallucination_rate": {"target": 0.05, "description": "Less than 5% of claims should be hallucinated"}
        },
        "latency": {
            "p95_ms": {"target": 300, "description": "95th percentile latency under 300ms"},
            "p99_ms": {"target": 500, "description": "99th percentile latency under 500ms"}
        },
        "sources": [
            "Qdrant RAG Evaluation Guide",
            "RAGAS Framework",
            "LlamaIndex Evaluation",
            "Evidently AI"
        ]
    }


# =============================================================================
# RAGAS FRAMEWORK ENDPOINTS
# =============================================================================

class RAGASEvaluationRequest(BaseModel):
    questions: List[str]
    answers: List[str]
    contexts: List[List[str]]
    ground_truths: Optional[List[str]] = None
    metrics: Optional[List[str]] = None


@app.get("/evaluate/ragas/status")
async def get_ragas_status():
    """Check if RAGAS framework is available"""
    from app.utils.evaluator import is_ragas_available, RAGASEvaluator

    available = is_ragas_available()

    if available:
        try:
            evaluator = RAGASEvaluator()
            metrics = evaluator.get_metric_descriptions()
        except Exception as e:
            metrics = {"error": str(e)}
    else:
        metrics = None

    return {
        "ragas_available": available,
        "supported_metrics": list(metrics.keys()) if metrics and "error" not in metrics else [],
        "metric_descriptions": metrics,
        "install_command": "pip install ragas datasets langchain langchain-community" if not available else None
    }


@app.post("/evaluate/ragas")
async def evaluate_with_ragas(request: RAGASEvaluationRequest):
    """
    Evaluate RAG system using RAGAS framework.

    RAGAS provides state-of-the-art metrics:
    - faithfulness: Is the answer grounded in context?
    - answer_relevancy: Does the answer address the question?
    - context_precision: Is the retrieved context relevant?
    - context_recall: Does context cover the ground truth?
    - answer_similarity: Semantic similarity to ground truth
    - answer_correctness: Factual correctness vs ground truth

    Note: Some metrics (context_recall, answer_similarity, answer_correctness)
    require ground_truths to be provided.
    """
    from app.utils.evaluator import (
        is_ragas_available,
        get_ragas_evaluator,
        RAGASEvaluator
    )

    if not is_ragas_available():
        raise HTTPException(
            status_code=503,
            detail="RAGAS not available. Install with: pip install ragas datasets langchain"
        )

    # Validate input
    if len(request.questions) != len(request.answers):
        raise HTTPException(
            status_code=400,
            detail="questions and answers must have the same length"
        )
    if len(request.questions) != len(request.contexts):
        raise HTTPException(
            status_code=400,
            detail="questions and contexts must have the same length"
        )

    try:
        evaluator = await get_ragas_evaluator(metrics=request.metrics)

        result = await evaluator.evaluate(
            questions=request.questions,
            answers=request.answers,
            contexts=request.contexts,
            ground_truths=request.ground_truths
        )

        return {
            "success": True,
            "metrics": result.metrics.to_dict(),
            "num_samples": result.num_samples,
            "evaluation_time_seconds": result.evaluation_time_seconds,
            "timestamp": result.timestamp,
            "targets": {
                "faithfulness": {"value": result.metrics.faithfulness, "target": 0.9, "passed": result.metrics.faithfulness >= 0.9},
                "answer_relevancy": {"value": result.metrics.answer_relevancy, "target": 0.8, "passed": result.metrics.answer_relevancy >= 0.8},
                "context_precision": {"value": result.metrics.context_precision, "target": 0.7, "passed": result.metrics.context_precision >= 0.7}
            }
        }

    except Exception as e:
        logger.error(f"RAGAS evaluation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class RAGASSingleEvaluationRequest(BaseModel):
    question: str
    answer: str
    contexts: List[str]
    ground_truth: Optional[str] = None


@app.post("/evaluate/ragas/single")
async def evaluate_single_with_ragas(request: RAGASSingleEvaluationRequest):
    """
    Evaluate a single question-answer pair using RAGAS.
    """
    from app.utils.evaluator import is_ragas_available, get_ragas_evaluator

    if not is_ragas_available():
        raise HTTPException(
            status_code=503,
            detail="RAGAS not available. Install with: pip install ragas datasets langchain"
        )

    try:
        evaluator = await get_ragas_evaluator()

        metrics = await evaluator.evaluate_single(
            question=request.question,
            answer=request.answer,
            contexts=request.contexts,
            ground_truth=request.ground_truth
        )

        return {
            "success": True,
            "question": request.question,
            "metrics": metrics.to_dict()
        }

    except Exception as e:
        logger.error(f"RAGAS single evaluation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# WEEKLY EVALUATION REPORTS
# =============================================================================

class WeeklyReportRequest(BaseModel):
    test_dataset_id: Optional[str] = None
    include_ragas: bool = True


@app.post("/evaluate/weekly-report")
async def generate_weekly_report(request: WeeklyReportRequest):
    """
    Generate a weekly RAG evaluation report.

    This endpoint runs a comprehensive evaluation against the test dataset
    and generates a report with:
    - Retrieval metrics (P@k, R@k, MRR, nDCG)
    - Generation metrics (Faithfulness, Relevancy)
    - RAGAS metrics (if available)
    - Target compliance
    - Trend analysis vs previous week
    - Actionable recommendations
    """
    from app.utils.evaluator import (
        RAGEvaluator,
        WeeklyReportGenerator,
        create_sample_test_dataset,
        is_ragas_available,
        get_ragas_evaluator,
        RetrievalResult
    )

    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    # Create search function for evaluator
    async def search_fn(query: str) -> List:
        query_embedding = get_embedder().encode(query, normalize_embeddings=True).tolist()
        results = await vector_store.search(
            query_embedding=query_embedding,
            limit=20,
            threshold=0.0
        )
        return [
            RetrievalResult(
                chunk_id=r["chunk_id"],
                document_id=r["document_id"],
                content=r["content"],
                score=r["score"],
                metadata=r.get("metadata", {})
            )
            for r in results
        ]

    # Initialize evaluators
    evaluator = RAGEvaluator(search_fn=search_fn)

    ragas_evaluator = None
    if request.include_ragas and is_ragas_available():
        try:
            ragas_evaluator = await get_ragas_evaluator()
        except Exception as e:
            logger.warning(f"Could not initialize RAGAS evaluator: {e}")

    # Get test dataset
    test_queries = create_sample_test_dataset()

    # Generate report
    report_generator = WeeklyReportGenerator(
        evaluator=evaluator,
        ragas_evaluator=ragas_evaluator,
        report_storage_path=os.getenv("RAG_REPORTS_PATH", "/tmp/rag_reports")
    )

    try:
        report = await report_generator.generate_report(test_queries)

        return {
            "success": True,
            "report_id": report.report_id,
            "summary": {
                "overall_score": report.overall_score,
                "targets_met": report.targets_met,
                "total_queries": report.total_queries_evaluated
            },
            "report": report.to_dict(),
            "markdown": report.to_markdown()
        }

    except Exception as e:
        logger.error(f"Weekly report generation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/evaluate/reports")
async def list_evaluation_reports(limit: int = 10):
    """List recent evaluation reports"""
    import os
    import glob

    reports_path = os.getenv("RAG_REPORTS_PATH", "/tmp/rag_reports")

    if not os.path.exists(reports_path):
        return {"reports": [], "count": 0}

    # Find JSON report files
    pattern = os.path.join(reports_path, "weekly_*.json")
    files = sorted(glob.glob(pattern), reverse=True)[:limit]

    reports = []
    for f in files:
        try:
            with open(f, "r") as fp:
                import json
                data = json.load(fp)
                reports.append({
                    "report_id": data.get("report_id"),
                    "week_start": data.get("week_start"),
                    "week_end": data.get("week_end"),
                    "overall_score": data.get("overall_score"),
                    "timestamp": data.get("timestamp")
                })
        except Exception:
            pass

    return {"reports": reports, "count": len(reports)}


@app.get("/evaluate/reports/{report_id}")
async def get_evaluation_report(report_id: str, format: str = "json"):
    """Get a specific evaluation report"""
    import os

    reports_path = os.getenv("RAG_REPORTS_PATH", "/tmp/rag_reports")

    if format == "markdown":
        file_path = os.path.join(reports_path, f"{report_id}.md")
    else:
        file_path = os.path.join(reports_path, f"{report_id}.json")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Report not found")

    try:
        with open(file_path, "r") as f:
            content = f.read()

        if format == "markdown":
            return {"report_id": report_id, "format": "markdown", "content": content}
        else:
            import json
            return json.loads(content)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# QUERY UNDERSTANDING (INTENT CLASSIFICATION)
# =============================================================================

class IntentRequest(BaseModel):
    query: str


@app.post("/intent/classify")
async def classify_query_intent(request: IntentRequest):
    """
    Classify query intent to optimize retrieval strategy.

    Returns:
    - Primary and secondary intents
    - Recommended retrieval strategy
    - Query complexity assessment
    - Extracted keywords and entities
    - Expanded queries for better recall
    """
    classifier = get_intent_classifier(embedder=embedder)
    result = classifier.classify(request.query)

    # Build retrieval configuration
    retrieval_config = build_retrieval_config(result)

    return {
        "classification": result.to_dict(),
        "retrieval_config": retrieval_config
    }


@app.post("/search/smart")
async def smart_search(request: SearchRequest):
    """
    Smart search with automatic intent classification.

    Automatically adjusts retrieval parameters based on query intent.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    start_time = time.time()

    # Classify intent
    classifier = get_intent_classifier(embedder=embedder)
    intent_result = classifier.classify(request.query)
    retrieval_config = build_retrieval_config(intent_result)

    # Generate query embedding
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    # Search with optimized parameters
    results = await vector_store.search(
        query_embedding=query_embedding,
        limit=retrieval_config.get("top_k", request.top_k),
        threshold=retrieval_config.get("min_score", request.min_score),
        document_ids=request.document_ids
    )

    # Apply hybrid reranking if configured
    if retrieval_config.get("use_hybrid", True):
        query_words = set(request.query.lower().split())
        for result in results:
            content_words = set(result["content"].lower().split())
            keyword_overlap = len(query_words & content_words) / max(len(query_words), 1)
            semantic_weight = retrieval_config.get("semantic_weight", 0.7)
            keyword_weight = retrieval_config.get("keyword_weight", 0.3)
            result["hybrid_score"] = (result["score"] * semantic_weight) + (keyword_overlap * keyword_weight)

        results.sort(key=lambda x: x.get("hybrid_score", x["score"]), reverse=True)

    search_time = (time.time() - start_time) * 1000

    return {
        "results": results[:request.top_k],
        "query": request.query,
        "intent": intent_result.primary_intent.value,
        "strategy": intent_result.strategy.value,
        "complexity": intent_result.complexity.value,
        "total_results": len(results),
        "search_time_ms": search_time,
        "expanded_queries": intent_result.expanded_queries
    }


# =============================================================================
# INCREMENTAL INDEXING
# =============================================================================

@app.post("/index/incremental")
async def index_document_incrementally(request: IndexRequest):
    """
    Index document chunks incrementally with real-time progress.

    Uses streaming approach for better performance with large documents.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    result = await index_chunks_incrementally(
        vector_store=vector_store,
        embedder=embedder,
        document_id=request.document_id,
        chunks=request.chunks,
        batch_size=EMBEDDING_BATCH_SIZE
    )

    return result


@app.get("/index/progress/{document_id}")
async def get_indexing_progress(document_id: int):
    """Get real-time indexing progress for a document"""
    indexer = get_incremental_indexer(vector_store, embedder)
    progress = indexer.get_progress(document_id)

    if progress is None:
        return {"status": "not_found", "document_id": document_id}

    return progress.to_dict()


# =============================================================================
# CROSS-REFERENCE LINKING
# =============================================================================

class CrossRefRequest(BaseModel):
    chunk_id: int
    limit: int = 5
    min_score: float = 0.6
    cross_document_only: bool = False


class BuildReferencesRequest(BaseModel):
    document_id: int


@app.post("/crossref/related")
async def get_related_chunks(request: CrossRefRequest):
    """
    Get chunks related to a given chunk via cross-references.

    Finds semantically similar and entity-linked content across documents.
    """
    cross_linker = get_cross_referencer(embedder=embedder)

    related = await cross_linker.get_related_chunks(
        chunk_id=request.chunk_id,
        limit=request.limit,
        min_score=request.min_score,
        cross_document_only=request.cross_document_only
    )

    return {
        "source_chunk_id": request.chunk_id,
        "related_chunks": related,
        "count": len(related)
    }


@app.post("/crossref/build")
async def build_document_references(request: BuildReferencesRequest):
    """
    Build cross-references for all chunks in a document.

    This should be called after document indexing to enable
    cross-document context retrieval.
    """
    cross_linker = get_cross_referencer(embedder=embedder)

    # Get all chunks for the document from vector store
    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    # For each chunk in the document, build references
    chunks = await vector_store.get_document_chunks(request.document_id)
    total_refs = 0

    for chunk in chunks:
        # Add chunk to cross-referencer
        await cross_linker.add_chunk(
            chunk_id=chunk["chunk_id"],
            document_id=request.document_id,
            content=chunk["content"],
            embedding=chunk.get("embedding")
        )

        # Build references
        refs = await cross_linker.build_references(chunk["chunk_id"])
        total_refs += len(refs)

    return {
        "document_id": request.document_id,
        "chunks_processed": len(chunks),
        "references_created": total_refs,
        "avg_refs_per_chunk": total_refs / max(len(chunks), 1)
    }


@app.get("/crossref/graph/{document_id}")
async def get_document_graph(document_id: int):
    """
    Get the knowledge graph for a document.

    Returns nodes (chunks) and edges (cross-references) for visualization.
    """
    cross_linker = get_cross_referencer(embedder=embedder)
    return await cross_linker.get_document_graph(document_id)


@app.get("/crossref/related-docs/{document_id}")
async def get_related_documents(document_id: int, limit: int = 10):
    """
    Get documents related to a given document.

    Based on cross-references between their chunks.
    """
    cross_linker = get_cross_referencer(embedder=embedder)
    return {
        "source_document_id": document_id,
        "related_documents": await cross_linker.get_cross_document_links(document_id, limit)
    }


@app.get("/crossref/stats")
async def get_crossref_stats():
    """Get cross-referencer statistics"""
    cross_linker = get_cross_referencer(embedder=embedder)
    return cross_linker.get_stats()


@app.get("/crossref/visualization")
async def get_crossref_visualization():
    """
    Get semantic clustering visualization data for all documents.

    Returns nodes, edges, and cluster metrics for D3.js/Plotly visualization.
    Useful for understanding document relationships and semantic clusters.
    """
    cross_linker = get_cross_referencer(embedder=embedder)
    stats = cross_linker.get_stats()

    # Build global graph from all indexed documents
    all_nodes = []
    all_edges = []
    document_clusters = {}

    # Get all document IDs from the linker
    for doc_id in cross_linker._document_chunks.keys():
        graph = await cross_linker.get_document_graph(doc_id)

        # Add nodes with document grouping
        for node in graph["nodes"]:
            node["group"] = doc_id  # For clustering visualization
            all_nodes.append(node)

        # Add edges
        all_edges.extend(graph["edges"])

        # Track document cluster info
        document_clusters[doc_id] = {
            "node_count": len(graph["nodes"]),
            "edge_count": len(graph["edges"]),
            "density": len(graph["edges"]) / max(len(graph["nodes"]), 1)
        }

    # Calculate cluster metrics
    avg_cluster_size = stats["total_chunks"] / max(stats["total_documents"], 1)
    connectivity = stats["total_references"] / max(stats["total_chunks"], 1)

    return {
        "visualization": {
            "nodes": all_nodes,
            "edges": all_edges,
            "format": "d3-force-graph"  # Compatible with D3.js force-directed graph
        },
        "clusters": document_clusters,
        "metrics": {
            "total_nodes": len(all_nodes),
            "total_edges": len(all_edges),
            "total_documents": stats["total_documents"],
            "avg_cluster_size": round(avg_cluster_size, 2),
            "connectivity_ratio": round(connectivity, 3),
            "unique_entities": stats["unique_entities"],
            "unique_topics": stats["unique_topics"]
        },
        "summary": f"{len(all_nodes)} chunks across {stats['total_documents']} documents with {len(all_edges)} cross-references"
    }


@app.get("/crossref/clusters")
async def get_semantic_clusters(min_similarity: float = 0.7):
    """
    Get document-level semantic clusters.

    Groups documents by semantic similarity based on their cross-references.
    """
    cross_linker = get_cross_referencer(embedder=embedder)

    # Build document similarity matrix
    doc_similarities = {}

    for doc_id in cross_linker._document_chunks.keys():
        related = await cross_linker.get_cross_document_links(doc_id, limit=20)
        for rel in related:
            if rel["average_similarity"] >= min_similarity:
                key = tuple(sorted([doc_id, rel["document_id"]]))
                if key not in doc_similarities:
                    doc_similarities[key] = rel["average_similarity"]

    # Group into clusters (simple connected components)
    clusters = []
    visited = set()

    for doc_id in cross_linker._document_chunks.keys():
        if doc_id in visited:
            continue

        # BFS to find connected documents
        cluster = set()
        queue = [doc_id]

        while queue:
            current = queue.pop(0)
            if current in visited:
                continue

            visited.add(current)
            cluster.add(current)

            # Find connected docs
            for key, sim in doc_similarities.items():
                if current in key:
                    other = key[0] if key[1] == current else key[1]
                    if other not in visited:
                        queue.append(other)

        if cluster:
            clusters.append({
                "cluster_id": len(clusters),
                "document_ids": list(cluster),
                "size": len(cluster),
                "coherence": sum(
                    doc_similarities.get(tuple(sorted([a, b])), 0)
                    for a in cluster for b in cluster if a < b
                ) / max(len(cluster) * (len(cluster) - 1) / 2, 1)
            })

    return {
        "clusters": sorted(clusters, key=lambda x: x["size"], reverse=True),
        "total_clusters": len(clusters),
        "min_similarity_threshold": min_similarity,
        "isolated_documents": sum(1 for c in clusters if c["size"] == 1)
    }


# =============================================================================
# ENHANCED SEARCH WITH CROSS-REFERENCES
# =============================================================================

@app.post("/search/enhanced")
async def enhanced_search(request: SearchRequest):
    """
    Enhanced search that includes cross-referenced content.

    Augments retrieved results with related content from other documents
    for more comprehensive context.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    start_time = time.time()

    # Generate query embedding
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    # Standard search
    results = await vector_store.search(
        query_embedding=query_embedding,
        limit=request.top_k,
        threshold=request.min_score,
        document_ids=request.document_ids
    )

    # Enhance with cross-references
    cross_linker = get_cross_referencer(embedder=embedder)
    enhanced_results = await enhance_context_with_references(
        cross_linker=cross_linker,
        retrieved_chunks=results,
        max_additions=3
    )

    search_time = (time.time() - start_time) * 1000

    return {
        "results": enhanced_results,
        "query": request.query,
        "total_results": len(enhanced_results),
        "original_results": len(results),
        "cross_ref_additions": len(enhanced_results) - len(results),
        "search_time_ms": search_time,
        "search_method": "enhanced_crossref"
    }


# =============================================================================
# DOCUMENT PROCESSING ENDPOINTS
# =============================================================================

class DocumentUploadResponse(BaseModel):
    success: bool
    document_id: int
    filename: str
    document_type: str
    chunks_created: int
    chunks_indexed: int
    processing_time_ms: float
    error: Optional[str] = None


class DocumentResponse(BaseModel):
    id: int
    filename: str
    file_type: str
    file_size: int
    status: str
    chunk_count: int
    created_at: str
    processed_at: Optional[str] = None
    error_message: Optional[str] = None


@app.get("/documents")
async def list_documents(
    user_id: Optional[int] = None,
    status: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """List all documents with optional filtering"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            query = """
                SELECT id, filename, file_type, file_size, status, chunk_count,
                       created_at, processed_at, error_message
                FROM documents
                WHERE ($1::int IS NULL OR user_id = $1)
                  AND ($2::text IS NULL OR status = $2)
                ORDER BY created_at DESC
                LIMIT $3 OFFSET $4
            """
            rows = await conn.fetch(query, user_id, status, limit, offset)

            # Get total count
            count_query = """
                SELECT COUNT(*) FROM documents
                WHERE ($1::int IS NULL OR user_id = $1)
                  AND ($2::text IS NULL OR status = $2)
            """
            total = await conn.fetchval(count_query, user_id, status)

            documents = [
                {
                    "id": row["id"],
                    "filename": row["filename"],
                    "file_type": row["file_type"],
                    "file_size": row["file_size"],
                    "status": row["status"],
                    "chunk_count": row["chunk_count"],
                    "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                    "processed_at": row["processed_at"].isoformat() if row["processed_at"] else None,
                    "error_message": row["error_message"]
                }
                for row in rows
            ]

            return {"documents": documents, "total": total, "limit": limit, "offset": offset}
    except Exception as e:
        logger.error(f"Error listing documents: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/documents/{document_id}")
async def get_document(document_id: int):
    """Get a specific document by ID"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, filename, file_type, file_size, status, chunk_count,
                       created_at, processed_at, error_message, metadata
                FROM documents WHERE id = $1
                """,
                document_id
            )

            if not row:
                raise HTTPException(status_code=404, detail="Document not found")

            return {
                "id": row["id"],
                "filename": row["filename"],
                "file_type": row["file_type"],
                "file_size": row["file_size"],
                "status": row["status"],
                "chunk_count": row["chunk_count"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
                "processed_at": row["processed_at"].isoformat() if row["processed_at"] else None,
                "error_message": row["error_message"],
                "metadata": row["metadata"]
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/documents/{document_id}")
async def delete_document(document_id: int):
    """Delete a document and its chunks"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Check if document exists
            exists = await conn.fetchval(
                "SELECT id FROM documents WHERE id = $1", document_id
            )
            if not exists:
                raise HTTPException(status_code=404, detail="Document not found")

            # Delete from vector store
            if vector_store:
                await vector_store.delete_by_document(document_id)

            # Delete document (chunks cascade delete)
            await conn.execute("DELETE FROM documents WHERE id = $1", document_id)

            logger.info(f"🗑️ Deleted document {document_id}")
            return {"success": True, "document_id": document_id}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/documents/upload", response_model=DocumentUploadResponse)
async def upload_document(
    file: UploadFile = File(...),
    document_id: int = Form(...)
):
    """
    Upload and process a document.

    Supported formats:
    - PDF (.pdf)
    - Word documents (.docx)
    - Text files (.txt, .md)
    - Images with OCR (.png, .jpg, .jpeg)

    Pipeline:
    1. Detect document type
    2. Extract text
    3. Chunk content
    4. Generate embeddings
    5. Store in vector database
    """
    if document_processor is None:
        raise HTTPException(status_code=503, detail="Document processor not initialized")

    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    start_time = time.time()

    try:
        # Read file content
        content = await file.read()

        # Process document
        processed = await document_processor.process(
            document_id=document_id,
            filename=file.filename,
            content=content
        )

        if not processed.success:
            return DocumentUploadResponse(
                success=False,
                document_id=document_id,
                filename=file.filename,
                document_type=processed.metadata.document_type.value,
                chunks_created=0,
                chunks_indexed=0,
                processing_time_ms=processed.processing_time_ms,
                error=processed.error
            )

        # Index chunks
        chunks_for_index = [
            {
                "content": chunk.content,
                "metadata": {
                    "chunk_id": chunk.chunk_id,
                    "page_number": chunk.page_number,
                    "section": chunk.section,
                    "source": file.filename,
                    **chunk.metadata
                }
            }
            for chunk in processed.chunks
        ]

        # Generate embeddings in batches
        contents = [c["content"] for c in chunks_for_index]
        all_embeddings = []

        for i in range(0, len(contents), EMBEDDING_BATCH_SIZE):
            batch = contents[i:i + EMBEDDING_BATCH_SIZE]
            batch_embeddings = get_embedder().encode(
                batch,
                normalize_embeddings=True,
                show_progress_bar=False
            ).tolist()
            all_embeddings.extend(batch_embeddings)

        # Upsert to vector store
        indexed_count = await vector_store.upsert(
            document_id=document_id,
            chunks=chunks_for_index,
            embeddings=all_embeddings
        )

        processing_time = (time.time() - start_time) * 1000

        logger.info(
            f"📄 Uploaded {file.filename}: {len(processed.chunks)} chunks, "
            f"{indexed_count} indexed, {processing_time:.1f}ms"
        )

        # Extract entities for knowledge graph (background task, non-blocking)
        entities_extracted = 0
        if KNOWLEDGE_GRAPH_AVAILABLE and hybrid_pipeline is not None:
            try:
                # Combine all chunk content for entity extraction
                full_content = "\n\n".join([chunk.content for chunk in processed.chunks])
                if len(full_content) > 100:  # Only extract if meaningful content
                    result = await extract_and_build_graph(
                        content=full_content[:10000],  # Limit to first 10k chars
                        document_id=document_id
                    )
                    entities_extracted = len(result.get("entities", []))
                    logger.info(f"🔗 Extracted {entities_extracted} entities from {file.filename}")
            except Exception as kg_error:
                logger.warning(f"Knowledge graph extraction failed (non-fatal): {kg_error}")

        # Auto-assign document to best matching cluster (non-blocking)
        cluster_name = None
        if SEMANTIC_CLUSTERING_AVAILABLE and all_embeddings:
            try:
                pool = await get_db_pool()
                engine = await get_clustering_engine(db_pool=pool)

                # Use average of all chunk embeddings as document embedding
                doc_embedding = np.mean(all_embeddings, axis=0)

                # Find best matching cluster
                # TODO: Get user_id from request context
                user_id = 1  # Default for now
                cluster_info = await engine.find_cluster_for_document(
                    user_id=user_id,
                    document_embedding=doc_embedding
                )

                if cluster_info:
                    await engine.assign_document_to_cluster(
                        document_id=document_id,
                        cluster_id=cluster_info['cluster_id'],
                        membership_score=cluster_info['similarity']
                    )
                    cluster_name = cluster_info['cluster_name']
                    logger.info(f"📁 Assigned {file.filename} to cluster '{cluster_name}' (similarity: {cluster_info['similarity']:.2f})")
            except Exception as cluster_error:
                logger.warning(f"Cluster assignment failed (non-fatal): {cluster_error}")

        return DocumentUploadResponse(
            success=True,
            document_id=document_id,
            filename=file.filename,
            document_type=processed.metadata.document_type.value,
            chunks_created=len(processed.chunks),
            chunks_indexed=indexed_count,
            processing_time_ms=processing_time
        )

    except Exception as e:
        logger.error(f"Document upload error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/documents/process")
async def process_document_content(
    document_id: int,
    filename: str,
    content: str
):
    """
    Process text content directly (without file upload).

    Useful for:
    - Processing pasted text
    - Processing content from external sources
    - Testing document processing
    """
    if document_processor is None:
        raise HTTPException(status_code=503, detail="Document processor not initialized")

    # Convert string to bytes
    content_bytes = content.encode("utf-8")

    processed = await document_processor.process(
        document_id=document_id,
        filename=filename,
        content=content_bytes
    )

    return processed.to_dict()


# =============================================================================
# TEXT INGESTION ENDPOINT
# =============================================================================

class IngestTextRequest(BaseModel):
    """Ingest raw text into the RAG pipeline (for email content, scraped pages, etc.)."""
    user_id: int
    content: str
    filename: str = "ingested_text.txt"
    metadata: Optional[Dict[str, Any]] = None


class IngestTextResponse(BaseModel):
    success: bool
    document_id: Optional[int] = None
    chunks_created: int = 0
    chunks_indexed: int = 0
    processing_time_ms: float = 0.0
    error: Optional[str] = None


@app.post("/ingest/text", response_model=IngestTextResponse)
async def ingest_text(request: IngestTextRequest):
    """
    Ingest raw text content into the RAG pipeline.

    Creates a document record, chunks the text, generates embeddings,
    and stores everything in the vector store. Used by the email sync
    pipeline and any other source that produces raw text.

    Pipeline:
    1. Create document record in database
    2. Process text into chunks
    3. Generate embeddings in batches
    4. Store in vector database
    """
    if document_processor is None:
        raise HTTPException(status_code=503, detail="Document processor not initialized")

    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    if not request.content or not request.content.strip():
        return IngestTextResponse(success=False, error="Content is empty")

    start_time = time.time()

    try:
        # Create document record in database
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            doc_row = await conn.fetchrow(
                """INSERT INTO documents (user_id, filename, file_type, file_size, status, upload_date)
                   VALUES ($1, $2, 'txt', $3, 'processed', NOW())
                   RETURNING id""",
                request.user_id,
                request.filename,
                len(request.content.encode("utf-8")),
            )
            document_id = doc_row["id"]

        # Process text into chunks
        content_bytes = request.content.encode("utf-8")
        processed = await document_processor.process(
            document_id=document_id,
            filename=request.filename,
            content=content_bytes,
        )

        if not processed.success:
            return IngestTextResponse(
                success=False,
                document_id=document_id,
                error=processed.error,
                processing_time_ms=(time.time() - start_time) * 1000,
            )

        # Build chunk dicts for indexing
        extra_meta = request.metadata or {}
        chunks_for_index = [
            {
                "content": chunk.content,
                "metadata": {
                    "chunk_id": chunk.chunk_id,
                    "page_number": chunk.page_number,
                    "section": chunk.section,
                    "source": request.filename,
                    "user_id": request.user_id,
                    **extra_meta,
                    **chunk.metadata,
                },
            }
            for chunk in processed.chunks
        ]

        # Generate embeddings in batches
        contents = [c["content"] for c in chunks_for_index]
        all_embeddings = []
        for i in range(0, len(contents), EMBEDDING_BATCH_SIZE):
            batch = contents[i : i + EMBEDDING_BATCH_SIZE]
            batch_embeddings = get_embedder().encode(
                batch, normalize_embeddings=True, show_progress_bar=False
            ).tolist()
            all_embeddings.extend(batch_embeddings)

        # Upsert to vector store
        indexed_count = await vector_store.upsert(
            document_id=document_id,
            chunks=chunks_for_index,
            embeddings=all_embeddings,
        )

        processing_time = (time.time() - start_time) * 1000

        logger.info(
            f"📥 Ingested text '{request.filename}': {len(processed.chunks)} chunks, "
            f"{indexed_count} indexed, {processing_time:.1f}ms"
        )

        # Background: feed document to LightRAG for entity/relationship extraction
        if lightrag_service and lightrag_service._initialized:
            async def _lightrag_ingest():
                try:
                    await lightrag_service.ingest_document(
                        content=request.content,
                        document_id=document_id,
                        filename=request.filename,
                        user_id=request.user_id,
                        metadata=request.metadata,
                    )
                except Exception as e:
                    logger.warning(f"LightRAG background ingestion failed: {e}")
            asyncio.create_task(_lightrag_ingest())

        return IngestTextResponse(
            success=True,
            document_id=document_id,
            chunks_created=len(processed.chunks),
            chunks_indexed=indexed_count,
            processing_time_ms=processing_time,
        )

    except Exception as e:
        logger.error(f"Text ingestion error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# RAG QUERY PROCESSOR ENDPOINTS
# =============================================================================

class RAGQueryRequest(BaseModel):
    query: str
    document_ids: Optional[List[int]] = None
    top_k: int = 5
    min_score: float = 0.5
    include_context: bool = True


class RAGQueryResponse(BaseModel):
    query: str
    context: str
    chunks: List[Dict[str, Any]]
    total_chunks: int
    processing_time_ms: float
    success: bool
    error: Optional[str] = None


@app.post("/query", response_model=RAGQueryResponse)
async def process_rag_query(request: RAGQueryRequest):
    """
    Process a RAG query through the full pipeline.

    Pipeline:
    1. Generate query embedding
    2. Retrieve relevant chunks (hybrid search)
    3. Rerank by relevance
    4. Build context for LLM
    5. Return structured result

    Returns context ready for LLM consumption.
    """
    if query_processor is None:
        raise HTTPException(status_code=503, detail="Query processor not initialized")

    result = await query_processor.process_query(
        query=request.query,
        document_ids=request.document_ids,
        top_k=request.top_k,
        min_score=request.min_score
    )

    # Record quality metrics (non-blocking)
    try:
        chunk_dicts = [c.to_dict() for c in result.chunks]
        await record_quality_metrics(
            query=request.query,
            context_chunks=chunk_dicts,
            response_time_ms=result.processing_time_ms,
            model_used="rag-pipeline",
            agent_used="query_processor",
            was_successful=result.success,
            error_message=result.error
        )
    except Exception as e:
        logger.warning(f"Failed to record quality metrics: {e}")

    return RAGQueryResponse(
        query=result.query,
        context=result.context if request.include_context else "",
        chunks=[c.to_dict() for c in result.chunks],
        total_chunks=result.total_chunks,
        processing_time_ms=result.processing_time_ms,
        success=result.success,
        error=result.error
    )


@app.post("/query/context")
async def get_llm_context(request: RAGQueryRequest):
    """
    Get context and sources optimized for LLM consumption.

    Returns:
    - context: Formatted context string
    - sources: List of source documents with previews
    """
    if query_processor is None:
        raise HTTPException(status_code=503, detail="Query processor not initialized")

    context, sources = await query_processor.get_context_for_llm(
        query=request.query,
        document_ids=request.document_ids,
        max_chunks=request.top_k
    )

    return {
        "query": request.query,
        "context": context,
        "sources": sources,
        "context_length": len(context)
    }


# =============================================================================
# KNOWLEDGE GRAPH ENDPOINTS
# =============================================================================

class EntityExtractionRequest(BaseModel):
    content: str
    document_id: Optional[int] = None
    use_ollama: bool = False


class GraphQueryRequest(BaseModel):
    query: str
    document_ids: Optional[List[int]] = None
    top_k: int = 10
    min_score: float = 0.5
    include_graph_context: bool = True
    max_hops: int = 2


@app.get("/graph/status")
async def get_knowledge_graph_status():
    """Get knowledge graph status and configuration"""
    return {
        "knowledge_graph_available": KNOWLEDGE_GRAPH_AVAILABLE,
        "knowledge_graph_initialized": knowledge_graph is not None,
        "hybrid_pipeline_initialized": hybrid_pipeline is not None,
        "spacy_enabled": hybrid_pipeline is not None,
        "ollama_enabled": hybrid_pipeline is not None and ollama_service is not None
    }


@app.get("/graph/stats")
async def get_knowledge_graph_stats():
    """Get detailed knowledge graph statistics"""
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    stats = knowledge_graph.get_stats()
    return stats


@app.post("/graph/extract")
async def extract_entities(request: EntityExtractionRequest):
    """
    Extract entities and relationships from content.

    Uses hybrid extraction:
    - spaCy for fast NLP-based extraction
    - Ollama for deep semantic extraction (optional)

    Returns:
    - entities: List of extracted entities
    - relationships: List of relationships between entities
    """
    if hybrid_pipeline is None:
        raise HTTPException(status_code=503, detail="Knowledge graph pipeline not initialized")

    start_time = time.time()

    try:
        result = await extract_and_build_graph(
            content=request.content,
            document_id=request.document_id
        )

        # Persist entities to PostgreSQL for permanent storage
        entities_persisted = 0
        relationships_persisted = 0
        if KG_PERSISTENCE_AVAILABLE and KnowledgeGraphRepository:
            for entity in result.get("entities", []):
                try:
                    await KnowledgeGraphRepository.add_entity(
                        name=entity.get("name"),
                        entity_type=entity.get("type", "unknown"),
                        importance=entity.get("importance", 0.5),
                        document_id=request.document_id
                    )
                    entities_persisted += 1
                except Exception as e:
                    logger.debug(f"Failed to persist entity {entity.get('name')}: {e}")

            for rel in result.get("relationships", []):
                try:
                    await KnowledgeGraphRepository.add_relationship(
                        source_name=rel.get("source"),
                        target_name=rel.get("target"),
                        relationship_type=rel.get("type", "related_to"),
                        document_id=request.document_id
                    )
                    relationships_persisted += 1
                except Exception as e:
                    logger.debug(f"Failed to persist relationship: {e}")

            if entities_persisted > 0:
                logger.info(f"Persisted {entities_persisted} entities, {relationships_persisted} relationships to database")

        processing_time = (time.time() - start_time) * 1000

        return {
            "success": True,
            "entities": result.get("entities", []),
            "relationships": result.get("relationships", []),
            "entity_count": len(result.get("entities", [])),
            "relationship_count": len(result.get("relationships", [])),
            "entities_persisted": entities_persisted,
            "relationships_persisted": relationships_persisted,
            "processing_time_ms": processing_time
        }

    except Exception as e:
        logger.error(f"Entity extraction failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/graph/build")
async def build_document_graph(document_id: int, content: str):
    """
    Build knowledge graph for a document.

    Extracts entities and relationships, adds them to the graph,
    and stores in PostgreSQL for persistence.
    """
    if hybrid_pipeline is None:
        raise HTTPException(status_code=503, detail="Knowledge graph pipeline not initialized")

    start_time = time.time()

    try:
        result = await hybrid_pipeline.process_content(
            content=content,
            document_id=document_id
        )

        processing_time = (time.time() - start_time) * 1000

        return {
            "success": True,
            "document_id": document_id,
            "entities_added": len(result.get("entities", [])),
            "relationships_added": len(result.get("relationships", [])),
            "processing_time_ms": processing_time
        }

    except Exception as e:
        logger.error(f"Graph building failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/graph/entities")
async def list_entities(
    entity_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    List entities in the knowledge graph.

    Optionally filter by entity type:
    - concept, topic, definition, process, term
    - person, organization, event, location, other
    """
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    entities = []
    for node_id in knowledge_graph.graph.nodes():
        node_data = knowledge_graph.graph.nodes[node_id]
        if entity_type is None or node_data.get("entity_type") == entity_type:
            entities.append({
                "id": node_id,
                "name": node_data.get("name", node_id),
                "entity_type": node_data.get("entity_type", "concept"),
                "importance": node_data.get("importance_score", 0.5),
                "difficulty": node_data.get("difficulty_level", "medium"),
                "description": node_data.get("description", ""),
                "created_at": node_data.get("created_at"),
                "updated_at": node_data.get("updated_at")
            })

    # Sort by importance
    entities.sort(key=lambda x: x["importance"], reverse=True)

    # Apply pagination
    paginated = entities[offset:offset + limit]

    return {
        "entities": paginated,
        "total": len(entities),
        "offset": offset,
        "limit": limit
    }


@app.get("/graph/entity/{entity_name}")
async def get_entity(entity_name: str):
    """Get details for a specific entity"""
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    entity_key = entity_name.lower()
    if entity_key not in knowledge_graph.graph:
        raise HTTPException(status_code=404, detail="Entity not found")

    node_data = knowledge_graph.graph.nodes[entity_key]

    # Get connected entities
    related = knowledge_graph.get_related_concepts(entity_key, max_depth=1)

    return {
        "name": entity_name,
        "entity_type": node_data.get("entity_type", "concept"),
        "importance": node_data.get("importance_score", 0.5),
        "difficulty": node_data.get("difficulty_level", "medium"),
        "description": node_data.get("description", ""),
        "related_entities": related[:10],
        "document_id": node_data.get("document_id")
    }


@app.get("/graph/entity/{entity_name}/related")
async def get_related_entities(
    entity_name: str,
    max_depth: int = 2,
    relationship_type: Optional[str] = None
):
    """Get entities related to a given entity"""
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    entity_key = entity_name.lower()
    if entity_key not in knowledge_graph.graph:
        raise HTTPException(status_code=404, detail="Entity not found")

    related = knowledge_graph.get_related_concepts(
        entity_key,
        max_depth=max_depth,
        relationship_type=relationship_type
    )

    return {
        "entity": entity_name,
        "related": related,
        "count": len(related)
    }


@app.get("/graph/entity/{entity_name}/prerequisites")
async def get_entity_prerequisites(entity_name: str):
    """Get prerequisite concepts for understanding an entity"""
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    entity_key = entity_name.lower()
    if entity_key not in knowledge_graph.graph:
        raise HTTPException(status_code=404, detail="Entity not found")

    prerequisites = knowledge_graph.get_prerequisite_order(entity_key)

    return {
        "entity": entity_name,
        "prerequisites": prerequisites,
        "count": len(prerequisites)
    }


@app.get("/graph/communities")
async def get_communities():
    """
    Get topic communities (clusters) in the knowledge graph.

    Uses Louvain community detection to find related topic groups.
    """
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    communities = knowledge_graph.get_communities()

    return {
        "communities": communities,
        "count": len(communities)
    }


@app.get("/graph/central")
async def get_central_entities(top_k: int = 20):
    """
    Get most central/important entities in the knowledge graph.

    Uses PageRank to determine entity importance.
    """
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    central = knowledge_graph.get_central_entities(top_n=top_k)

    return {
        "central_entities": central,
        "count": len(central)
    }


@app.post("/search/graph-enhanced")
async def graph_enhanced_search(request: GraphQueryRequest):
    """
    Graph-enhanced RAG search.

    Combines vector similarity search with knowledge graph traversal
    for more comprehensive and contextually aware retrieval.

    Process:
    1. Vector search for semantically similar chunks
    2. Extract entities from query
    3. Traverse graph to find related concepts
    4. Augment results with graph context
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    start_time = time.time()

    # Generate query embedding
    query_embedding = get_embedder().encode(
        request.query,
        normalize_embeddings=True
    ).tolist()

    # Vector search
    vector_results = await vector_store.search(
        query_embedding=query_embedding,
        limit=request.top_k,
        threshold=request.min_score,
        document_ids=request.document_ids
    )

    search_time = (time.time() - start_time) * 1000

    # If knowledge graph is available, enhance with graph context
    graph_context = ""
    graph_entities = []
    if request.include_graph_context and knowledge_graph is not None:
        try:
            graph_context = get_graph_enhanced_context(
                query=request.query,
                vector_results=vector_results
            )

            # Extract entities from query for additional context
            if hybrid_pipeline is not None:
                extraction_result = await hybrid_pipeline.extractor.extract(request.query)
                graph_entities = [e.get("name", e.get("entity", "")) for e in extraction_result.get("entities", [])]

        except Exception as e:
            logger.warning(f"Graph enhancement failed: {e}")

    total_time = (time.time() - start_time) * 1000

    return {
        "results": vector_results,
        "query": request.query,
        "total_results": len(vector_results),
        "search_time_ms": search_time,
        "total_time_ms": total_time,
        "search_method": "graph_enhanced",
        "graph_context": graph_context,
        "query_entities": graph_entities,
        "graph_enhanced": bool(graph_context)
    }


@app.post("/query/graph-rag")
async def graph_rag_query(request: GraphQueryRequest):
    """
    Full Graph-RAG query pipeline.

    Combines:
    1. Vector similarity search
    2. Knowledge graph traversal
    3. Cross-reference linking
    4. Context synthesis

    Returns context optimized for LLM consumption with
    entity relationships and prerequisite information.
    """
    if get_embedder() is None:
        raise HTTPException(status_code=503, detail="Embedding model failed to load")

    if hybrid_pipeline is None:
        raise HTTPException(status_code=503, detail="Knowledge graph pipeline not initialized")

    start_time = time.time()

    try:
        # Use hybrid pipeline for full RAG context
        context = await hybrid_pipeline.build_rag_context(
            query=request.query,
            vector_results=[]  # Will be populated by internal search
        )

        # Also do vector search for direct results
        query_embedding = get_embedder().encode(
            request.query,
            normalize_embeddings=True
        ).tolist()

        vector_results = await vector_store.search(
            query_embedding=query_embedding,
            limit=request.top_k,
            threshold=request.min_score,
            document_ids=request.document_ids
        )

        processing_time = (time.time() - start_time) * 1000

        return {
            "query": request.query,
            "context": context,
            "results": vector_results,
            "total_results": len(vector_results),
            "processing_time_ms": processing_time,
            "pipeline": "graph_rag"
        }

    except Exception as e:
        logger.error(f"Graph RAG query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/graph/clear")
async def clear_knowledge_graph(request: Request):
    """Clear the in-memory knowledge graph - SUPERADMIN ONLY"""
    require_superadmin(request)  # Permission check
    if knowledge_graph is None:
        raise HTTPException(status_code=503, detail="Knowledge graph not initialized")

    knowledge_graph.graph.clear()

    return {
        "success": True,
        "message": "In-memory knowledge graph cleared"
    }


@app.get("/graph/data")
async def get_graph_data():
    """Get complete graph data including entities, relationships, and stats from PostgreSQL"""
    # Try to get data from PostgreSQL first (persistent storage)
    if KG_PERSISTENCE_AVAILABLE and KnowledgeGraphRepository:
        try:
            # Get entities from database
            db_entities = await KnowledgeGraphRepository.get_all_entities()
            entities = [
                {
                    "name": e["name"],
                    "type": e["entity_type"],
                    "importance": e["importance"],
                    "description": e.get("description", ""),
                    "mention_count": e.get("mention_count", 1),
                    "created_at": e.get("created_at").isoformat() if e.get("created_at") else None,
                    "updated_at": e.get("updated_at").isoformat() if e.get("updated_at") else None
                }
                for e in db_entities
            ]

            # Get relationships from database
            db_relationships = await KnowledgeGraphRepository.get_all_relationships()
            relationships = [
                {
                    "source": r["source_name"],
                    "target": r["target_name"],
                    "type": r["relationship_type"],
                    "weight": r.get("weight", 1.0)
                }
                for r in db_relationships
            ]

            # Get stats from database
            db_stats = await KnowledgeGraphRepository.get_stats()

            # Calculate graph metrics
            entity_count = len(entities)
            relationship_count = len(relationships)

            # Calculate density
            density = 0.0
            if entity_count > 1:
                max_edges = entity_count * (entity_count - 1)
                density = relationship_count / max_edges if max_edges > 0 else 0.0

            stats = {
                "entity_count": entity_count,
                "relationship_count": relationship_count,
                "node_count": entity_count,
                "edge_count": relationship_count,
                "community_count": 1,  # Could be calculated with NetworkX
                "is_connected": True,
                "density": round(density * 100, 2),
                "entity_types": db_stats.get("entity_types", {}),
                "relationship_types": db_stats.get("relationship_types", {}),
            }

            return {
                "entities": entities,
                "relationships": relationships,
                "stats": stats
            }
        except Exception as e:
            logger.warning(f"Failed to get graph data from database: {e}, falling back to in-memory")

    # Fallback to in-memory graph
    if knowledge_graph is None:
        return {
            "entities": [],
            "relationships": [],
            "stats": {
                "entity_count": 0,
                "relationship_count": 0,
                "node_count": 0,
                "edge_count": 0,
                "community_count": 0,
                "is_connected": True,
                "density": 0,
                "entity_types": {},
                "relationship_types": {},
            }
        }

    # Get entities from in-memory graph
    entities = []
    for node_id in knowledge_graph.graph.nodes():
        node_data = knowledge_graph.graph.nodes[node_id]
        entities.append({
            "name": node_data.get("name", node_id),
            "type": node_data.get("entity_type", "concept"),
            "importance": node_data.get("importance_score", 0.5),
            "description": node_data.get("description", "")
        })

    # Get relationships from in-memory graph
    relationships = []
    for source, target, data in knowledge_graph.graph.edges(data=True):
        source_name = knowledge_graph.graph.nodes[source].get("name", source)
        target_name = knowledge_graph.graph.nodes[target].get("name", target)
        relationships.append({
            "source": source_name,
            "target": target_name,
            "type": data.get("relationship_type", "related_to"),
            "description": data.get("description", ""),
            "confidence": data.get("confidence", 0.8)
        })

    # Get stats
    stats = knowledge_graph.get_stats()

    return {
        "entities": entities,
        "relationships": relationships,
        "stats": stats
    }


@app.get("/graph/search")
async def search_graph_entities(query: str, limit: int = 20):
    """Search entities by name or description"""
    if knowledge_graph is None:
        return {"entities": []}

    query_lower = query.lower()
    matching = []

    for node_id in knowledge_graph.graph.nodes():
        node_data = knowledge_graph.graph.nodes[node_id]
        name = node_data.get("name", node_id)
        description = node_data.get("description", "")

        if query_lower in name.lower() or query_lower in description.lower():
            matching.append({
                "name": name,
                "type": node_data.get("entity_type", "concept"),
                "importance": node_data.get("importance_score", 0.5),
                "description": description
            })

    # Sort by importance and limit
    matching.sort(key=lambda x: x["importance"], reverse=True)

    return {"entities": matching[:limit]}


@app.get("/graph/related/{entity_name}")
async def get_graph_related_entities(entity_name: str, max_hops: int = 2):
    """Get entities related to a given entity"""
    if knowledge_graph is None:
        return {"entities": []}

    # Find the node
    target_node = None
    for node_id in knowledge_graph.graph.nodes():
        if knowledge_graph.graph.nodes[node_id].get("name", "").lower() == entity_name.lower():
            target_node = node_id
            break

    if target_node is None:
        return {"entities": []}

    # Get neighbors
    related = []
    visited = {target_node}

    def get_neighbors(node, depth):
        if depth > max_hops:
            return
        for neighbor in knowledge_graph.graph.neighbors(node):
            if neighbor not in visited:
                visited.add(neighbor)
                node_data = knowledge_graph.graph.nodes[neighbor]
                related.append({
                    "name": node_data.get("name", neighbor),
                    "type": node_data.get("entity_type", "concept"),
                    "importance": node_data.get("importance_score", 0.5),
                    "description": node_data.get("description", "")
                })
                get_neighbors(neighbor, depth + 1)

    get_neighbors(target_node, 1)

    return {"entities": related}


@app.post("/graph/rebuild")
async def rebuild_knowledge_graph(request: Request):
    """
    Rebuild the knowledge graph from all documents - SUPERADMIN ONLY
    """
    require_superadmin(request)  # Permission check
    if not KNOWLEDGE_GRAPH_AVAILABLE or hybrid_pipeline is None:
        raise HTTPException(status_code=503, detail="Knowledge graph extraction not available")

    if not KG_PERSISTENCE_AVAILABLE or KnowledgeGraphRepository is None:
        raise HTTPException(status_code=503, detail="Knowledge graph persistence not available")

    start_time = time.time()
    total_entities = 0
    total_relationships = 0
    processed_docs = 0
    errors = []

    try:
        # Get all documents from database directly (superadmin bypass)
        from assemblyline_common.database.connection import get_db_pool

        pool = await get_db_pool()

        # Query documents directly - superadmin sees all completed documents
        async with pool.acquire() as conn:
            doc_rows = await conn.fetch(
                "SELECT id, filename, status FROM documents WHERE status = 'completed' ORDER BY created_at DESC"
            )
        documents = [{"id": row["id"], "filename": row["filename"]} for row in doc_rows]
        logger.info(f"🔄 Rebuilding knowledge graph from {len(documents)} documents")

        for doc in documents:
            try:
                doc_id = doc["id"]
                # Get chunks/embeddings for this document from embeddings table
                async with pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT content FROM embeddings WHERE document_id = $1 ORDER BY chunk_id",
                        doc_id
                    )
                if not rows:
                    continue

                # Combine chunk content
                content = "\n\n".join([row["content"] for row in rows if row["content"]])
                if len(content) < 100:
                    continue

                # Extract entities and relationships using process_content
                extraction_result = await hybrid_pipeline.process_content(
                    content[:15000],  # Limit content
                    document_id=doc_id,
                    extract_entities=True,
                    max_entities=20,
                    enhance_with_ollama=True
                )

                # Persist entities to database
                for entity in extraction_result.get("entities", []):
                    try:
                        await KnowledgeGraphRepository.add_entity(
                            name=entity["name"],
                            entity_type=entity["type"],
                            importance=entity.get("importance", 0.5),
                            description=None,
                            document_id=doc_id,
                            metadata={}
                        )
                        total_entities += 1
                    except Exception as e:
                        logger.warning(f"Failed to add entity {entity.get('name', 'unknown')}: {e}")

                # Persist relationships to database
                for rel in extraction_result.get("relationships", []):
                    try:
                        await KnowledgeGraphRepository.add_relationship(
                            source_name=rel["source"],
                            target_name=rel["target"],
                            relationship_type=rel["type"],
                            weight=1.0,
                            document_id=doc_id
                        )
                        total_relationships += 1
                    except Exception as e:
                        logger.warning(f"Failed to add relationship: {e}")

                processed_docs += 1
                if processed_docs % 10 == 0:
                    logger.info(f"📊 Processed {processed_docs}/{len(documents)} documents")

            except Exception as e:
                errors.append(f"Document {getattr(doc, 'filename', doc_id)}: {str(e)}")
                logger.warning(f"Error processing document {doc_id}: {e}")

        elapsed = time.time() - start_time
        logger.info(f"✅ Knowledge graph rebuilt: {total_entities} entities, {total_relationships} relationships in {elapsed:.2f}s")

        return {
            "success": True,
            "documents_processed": processed_docs,
            "total_documents": len(documents),
            "entities_extracted": total_entities,
            "relationships_extracted": total_relationships,
            "elapsed_seconds": round(elapsed, 2),
            "errors": errors[:10] if errors else []  # Limit error list
        }

    except Exception as e:
        logger.error(f"Failed to rebuild knowledge graph: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to rebuild: {str(e)}")


@app.delete("/graph/clear-db")
async def clear_knowledge_graph_db(request: Request):
    """Clear all entities and relationships from PostgreSQL - SUPERADMIN ONLY (permanent)"""
    require_superadmin(request)  # Permission check - DANGEROUS operation
    if not KG_PERSISTENCE_AVAILABLE or KnowledgeGraphRepository is None:
        raise HTTPException(status_code=503, detail="Knowledge graph persistence not available")

    try:
        await KnowledgeGraphRepository.clear_all()
        logger.warning(f"Knowledge graph database cleared by superadmin")
        return {"success": True, "message": "Knowledge graph database cleared"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear: {str(e)}")


# =============================================================================
# LIGHTRAG ENDPOINTS (Document Knowledge Graph)
# =============================================================================

class LightRAGQueryRequest(BaseModel):
    query: str
    mode: str = "hybrid"  # "local", "global", "hybrid", "naive"
    top_k: int = 10


@app.post("/lightrag/query")
async def lightrag_query(request: LightRAGQueryRequest):
    """
    Query the LightRAG document knowledge graph.

    Modes:
    - local: Entity-focused retrieval (find specific entities mentioned in docs)
    - global: Relationship-focused retrieval (find patterns, themes, connections)
    - hybrid: Both local + global (recommended for most queries)
    - naive: Simple keyword matching (fallback)
    """
    if lightrag_service is None or not lightrag_service._initialized:
        raise HTTPException(status_code=503, detail="LightRAG not initialized")

    result = await lightrag_service.query(
        query=request.query,
        mode=request.mode,
        top_k=request.top_k,
    )

    if result is None:
        return {
            "success": False,
            "content": "",
            "mode": request.mode,
            "entities_found": [],
            "query_time_ms": 0,
        }

    return {
        "success": True,
        "content": result.content,
        "mode": result.mode,
        "entities_found": result.entities_found,
        "query_time_ms": result.query_time_ms,
    }


@app.get("/lightrag/stats")
async def lightrag_stats():
    """Get LightRAG service statistics."""
    if lightrag_service is None:
        return {
            "initialized": False,
            "available": LIGHTRAG_INTEGRATION_AVAILABLE,
            "enabled": False,
            "documents_indexed": 0,
        }
    return lightrag_service.get_stats()


@app.get("/eval/results")
async def get_eval_results(limit: int = 20):
    """
    Get recent RAGAS continuous evaluation results.

    Used by the Quality Dashboard to show evaluation trends.
    """
    if not EVAL_RUNNER_AVAILABLE:
        return {"results": [], "runner_info": {"enabled": False}}

    try:
        runner = get_eval_runner()
        results = await runner.get_recent_results(limit=limit)
        return {
            "results": results,
            "runner_info": runner.get_info(),
        }
    except Exception as e:
        logger.warning(f"Failed to get eval results: {e}")
        return {"results": [], "runner_info": {"enabled": False}, "error": str(e)}


@app.post("/eval/trigger")
async def trigger_eval_run(request: Request):
    """
    Manually trigger a RAGAS evaluation run.

    AI-first: The assistant can trigger this autonomously
    to check quality after ingesting new documents.
    """
    if not EVAL_RUNNER_AVAILABLE:
        raise HTTPException(status_code=503, detail="Eval runner not available")

    try:
        runner = get_eval_runner()
        # Run evaluation in background
        asyncio.create_task(runner._run_evaluation())
        return {"success": True, "message": "Evaluation run triggered"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# USER MEMORY ENDPOINTS
# =============================================================================

def get_user_id_from_token(request: Request) -> Optional[int]:
    """Extract user_id from JWT token in Authorization header"""
    auth_header = request.headers.get("Authorization", "")
    logger.info(f"Auth header present: {bool(auth_header)}, starts with Bearer: {auth_header.startswith('Bearer ')}")

    if not auth_header.startswith("Bearer "):
        logger.warning("No Bearer token found in Authorization header")
        return None

    token = auth_header[7:]  # Remove "Bearer " prefix
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = int(payload.get("sub", 0))
        logger.info(f"Successfully decoded JWT for user_id: {user_id}")
        return user_id
    except jwt.ExpiredSignatureError:
        logger.warning("JWT token has expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid JWT token: {e}")
        return None
    except ValueError as e:
        logger.warning(f"Invalid user_id in JWT: {e}")
        return None


def get_user_claims_from_token(request: Request) -> Optional[Dict[str, Any]]:
    """Extract all claims from JWT token including role permissions"""
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {
            "user_id": int(payload.get("sub", 0)),
            "email": payload.get("email"),
            "is_admin": payload.get("is_admin", False),
            "is_superadmin": payload.get("is_superadmin", False),
        }
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError, ValueError):
        return None


def require_superadmin(request: Request) -> Dict[str, Any]:
    """Require superadmin role for endpoint access"""
    claims = get_user_claims_from_token(request)
    if not claims:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not claims.get("is_superadmin", False):
        logger.warning(f"Permission denied: User {claims.get('email')} attempted superadmin action")
        raise HTTPException(status_code=403, detail="This action requires system administrator privileges")
    return claims


def require_admin(request: Request) -> Dict[str, Any]:
    """Require admin or superadmin role for endpoint access"""
    claims = get_user_claims_from_token(request)
    if not claims:
        raise HTTPException(status_code=401, detail="Unauthorized")
    if not claims.get("is_admin", False) and not claims.get("is_superadmin", False):
        logger.warning(f"Permission denied: User {claims.get('email')} attempted admin action")
        raise HTTPException(status_code=403, detail="This action requires administrator privileges")
    return claims


@app.get("/memory/list")
async def list_user_memories(request: Request):
    """List all memories for the current user from PostgreSQL"""
    user_id = get_user_id_from_token(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    memories = []
    stats = {"total_memories": 0, "categories": {}}

    try:
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                # Fetch user memories (excluding deleted)
                rows = await conn.fetch("""
                    SELECT id, content, source, category, confidence, metadata,
                           created_at, updated_at
                    FROM user_memories
                    WHERE user_id = $1 AND is_active = TRUE AND deleted_at IS NULL
                    ORDER BY updated_at DESC
                    LIMIT 100
                """, user_id)

                for row in rows:
                    memory = {
                        "id": str(row["id"]),
                        "content": row["content"],
                        "source": row["source"] or "AI learned",
                        "category": row["category"] or "general",
                        "confidence": float(row["confidence"]) if row["confidence"] else 0.8,
                        "timestamp": row["updated_at"].isoformat() if row["updated_at"] else row["created_at"].isoformat(),
                        "metadata": json.loads(row["metadata"]) if row["metadata"] else {}
                    }
                    memories.append(memory)

                    # Count by category
                    cat = memory["category"]
                    stats["categories"][cat] = stats["categories"].get(cat, 0) + 1

                stats["total_memories"] = len(memories)

                if memories:
                    stats["oldest_memory"] = memories[-1]["timestamp"]
                    stats["newest_memory"] = memories[0]["timestamp"]

    except Exception as e:
        logger.error(f"Error fetching memories: {e}")

    return {"memories": memories, "stats": stats}


@app.post("/memory/add")
async def add_user_memory(request: Request):
    """Add a new memory for the user to PostgreSQL"""
    user_id = get_user_id_from_token(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.json()
    content = body.get("content", "")
    source = body.get("source", "Manual entry")
    category = body.get("category", "general")

    if not content:
        raise HTTPException(status_code=400, detail="Content is required")

    memory_id = None
    timestamp = datetime.utcnow()

    try:
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    INSERT INTO user_memories (user_id, content, source, category, confidence, metadata)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id, created_at
                """, user_id, content, source, category, 1.0, json.dumps({"key": f"manual_{timestamp.isoformat()}"}))

                if row:
                    memory_id = str(row["id"])
                    timestamp = row["created_at"]
    except Exception as e:
        logger.error(f"Error adding memory: {e}")
        raise HTTPException(status_code=500, detail="Failed to add memory")

    return {
        "success": True,
        "message": "Memory added successfully",
        "memory": {
            "id": memory_id,
            "content": content,
            "category": category,
            "timestamp": timestamp.isoformat()
        }
    }


@app.put("/memory/{memory_id}")
async def update_user_memory(memory_id: str, request: Request):
    """Update an existing memory in PostgreSQL"""
    user_id = get_user_id_from_token(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    body = await request.json()
    content = body.get("content", "")

    if not content:
        raise HTTPException(status_code=400, detail="Content is required")

    try:
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                result = await conn.execute("""
                    UPDATE user_memories
                    SET content = $1, updated_at = NOW()
                    WHERE id = $2 AND user_id = $3 AND is_active = TRUE
                """, content, int(memory_id), user_id)

                if "UPDATE 0" in result:
                    raise HTTPException(status_code=404, detail="Memory not found")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid memory ID")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating memory: {e}")
        raise HTTPException(status_code=500, detail="Failed to update memory")

    return {
        "success": True,
        "message": "Memory updated successfully"
    }


@app.delete("/memory/{memory_id}")
async def delete_user_memory(memory_id: str, permanent: bool = False, request: Request = None):
    """Delete a memory. Default is soft delete (moves to trash)."""
    user_id = get_user_id_from_token(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                if permanent:
                    # Permanent delete
                    await conn.execute("""
                        DELETE FROM user_memories
                        WHERE id = $1 AND user_id = $2
                    """, int(memory_id), user_id)
                    logger.info(f"Memory permanently deleted: {memory_id} by user {user_id}")
                    return {
                        "success": True,
                        "message": "Memory permanently deleted"
                    }
                else:
                    # Soft delete - set deleted_at timestamp
                    result = await conn.execute("""
                        UPDATE user_memories
                        SET deleted_at = NOW(), is_active = FALSE, updated_at = NOW()
                        WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL
                    """, int(memory_id), user_id)
                    logger.info(f"Memory moved to trash: {memory_id} by user {user_id}")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid memory ID")
    except Exception as e:
        logger.error(f"Error deleting memory: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete memory")

    return {
        "success": True,
        "message": "Memory moved to trash"
    }


@app.delete("/memory/clear")
async def clear_user_memories(permanent: bool = False, request: Request = None):
    """Clear all memories for the user. Default is soft delete (moves to trash)."""
    user_id = get_user_id_from_token(request)
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Permanent deletion requires superadmin
    if permanent:
        require_superadmin(request)

    try:
        pool = await get_db_pool()
        if pool:
            async with pool.acquire() as conn:
                if permanent:
                    # Permanent delete all - SUPERADMIN ONLY
                    await conn.execute("""
                        DELETE FROM user_memories
                        WHERE user_id = $1
                    """, user_id)
                    logger.warning(f"All memories permanently deleted for user {user_id} by superadmin")
                    return {
                        "success": True,
                        "message": "All memories permanently deleted"
                    }
                else:
                    # Soft delete - move all to trash
                    await conn.execute("""
                        UPDATE user_memories
                        SET deleted_at = NOW(), is_active = FALSE, updated_at = NOW()
                        WHERE user_id = $1 AND deleted_at IS NULL
                    """, user_id)
                    logger.info(f"All memories moved to trash for user {user_id}")
    except Exception as e:
        logger.error(f"Error clearing memories: {e}")
        raise HTTPException(status_code=500, detail="Failed to clear memories")

    return {
        "success": True,
        "message": "All memories moved to trash"
    }


# =============================================================================
# QUALITY METRICS ENDPOINTS
# =============================================================================

@app.get("/quality/metrics")
async def get_quality_metrics(range: str = "7d"):
    """Get RAG quality metrics for the dashboard"""
    # Calculate time range
    days = 7
    if range == "30d":
        days = 30
    elif range == "90d":
        days = 90

    # Default response
    default_response = {
        "current": {
            "faithfulness": 0.0,
            "answer_relevancy": 0.0,
            "context_precision": 0.0,
            "context_recall": 0.0,
            "overall_score": 0.0,
        },
        "performance": {
            "avg_response_time": 0,
            "p95_response_time": 0,
            "total_queries": 0,
            "successful_queries": 0,
            "error_rate": 0.0,
            "cache_hit_rate": 0.0,
        },
        "trends": [],
        "top_issues": [],
        "recommendations": [
            "Upload documents to start building your knowledge base",
            "Ask questions to generate quality metrics",
        ],
    }

    if not DB_AVAILABLE:
        return default_response

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Get aggregated metrics for the time range
            metrics = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_queries,
                    COUNT(*) FILTER (WHERE was_successful = true) as successful_queries,
                    AVG(faithfulness) as avg_faithfulness,
                    AVG(answer_relevancy) as avg_relevancy,
                    AVG(context_precision) as avg_precision,
                    AVG(context_recall) as avg_recall,
                    AVG(overall_score) as avg_overall,
                    AVG(response_time_ms) as avg_response_time,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) as p95_response_time,
                    AVG(chunks_retrieved) as avg_chunks
                FROM quality_metrics
                WHERE created_at > NOW() - INTERVAL '%s days'
            """ % days)

            # Get daily trends
            trends = await conn.fetch("""
                SELECT
                    DATE(created_at) as date,
                    AVG(overall_score) as score,
                    COUNT(*) as count
                FROM quality_metrics
                WHERE created_at > NOW() - INTERVAL '%s days'
                GROUP BY DATE(created_at)
                ORDER BY date
            """ % days)

            total = metrics["total_queries"] or 0
            successful = metrics["successful_queries"] or 0

            if total == 0:
                return default_response

            # Build response with real data
            return {
                "current": {
                    "faithfulness": float(metrics["avg_faithfulness"] or 0),
                    "answer_relevancy": float(metrics["avg_relevancy"] or 0),
                    "context_precision": float(metrics["avg_precision"] or 0),
                    "context_recall": float(metrics["avg_recall"] or 0),
                    "overall_score": float(metrics["avg_overall"] or 0),
                },
                "performance": {
                    "avg_response_time": int(metrics["avg_response_time"] or 0),
                    "p95_response_time": int(metrics["p95_response_time"] or 0),
                    "total_queries": total,
                    "successful_queries": successful,
                    "error_rate": (total - successful) / total if total > 0 else 0.0,
                    "cache_hit_rate": 0.0,  # Not implemented yet
                },
                "trends": [
                    {
                        "date": str(t["date"]),
                        "score": float(t["score"] or 0),
                        "count": t["count"]
                    }
                    for t in trends
                ],
                "top_issues": [],
                "recommendations": _get_quality_recommendations(
                    float(metrics["avg_overall"] or 0),
                    total,
                    float(metrics["avg_precision"] or 0)
                ),
            }

    except Exception as e:
        logger.warning(f"Failed to fetch quality metrics: {e}")
        return default_response


class QualityRecordRequest(BaseModel):
    """Request model for recording quality metrics externally"""
    query: str
    response_time_ms: int
    user_id: Optional[int] = None
    model_used: str = "unknown"
    agent_used: str = "chat"
    was_successful: bool = True
    error_message: Optional[str] = None
    # Optional pre-calculated metrics (0.0-1.0 scale)
    faithfulness: Optional[float] = None
    answer_relevancy: Optional[float] = None
    context_precision: Optional[float] = None
    context_recall: Optional[float] = None
    chunks_retrieved: int = 0
    context_used: bool = False


@app.post("/quality/record")
async def post_quality_record(request: QualityRecordRequest):
    """
    Record quality metrics from external services (orchestrator, chat).

    This allows the orchestrator to report quality metrics after processing
    queries, ensuring the Quality Dashboard has complete data.
    """
    if not DB_AVAILABLE:
        return {"success": False, "error": "Database not available"}

    try:
        pool = await get_db_pool()

        # Use provided metrics or calculate defaults
        if request.context_used and request.chunks_retrieved > 0:
            # If context was used, use provided metrics or estimate
            faithfulness = request.faithfulness if request.faithfulness is not None else 0.7
            answer_relevancy = request.answer_relevancy if request.answer_relevancy is not None else 0.7
            context_precision = request.context_precision if request.context_precision is not None else 0.6
            context_recall = min(1.0, request.chunks_retrieved / 5.0)
        else:
            # No context used - lower but non-zero scores for general chat
            faithfulness = request.faithfulness if request.faithfulness is not None else 0.5
            answer_relevancy = request.answer_relevancy if request.answer_relevancy is not None else 0.6
            context_precision = 0.0  # No context retrieved
            context_recall = 0.0

        # Overall score
        overall_score = (faithfulness + answer_relevancy + context_precision + context_recall) / 4

        # Generate query ID
        import uuid
        query_id = str(uuid.uuid4())[:8]

        async with pool.acquire() as conn:
            result = await conn.fetchrow("""
                INSERT INTO quality_metrics (
                    query_id, user_id, faithfulness, answer_relevancy,
                    context_precision, context_recall, overall_score,
                    response_time_ms, chunks_retrieved, model_used,
                    agent_used, was_successful, error_message, metadata
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                RETURNING id
            """,
                query_id, request.user_id, faithfulness, answer_relevancy,
                context_precision, context_recall, overall_score,
                request.response_time_ms, request.chunks_retrieved, request.model_used,
                request.agent_used, request.was_successful, request.error_message,
                json.dumps({"query_preview": request.query[:100]})
            )

            logger.info(f"📊 External quality metrics recorded: overall={overall_score:.2f}, agent={request.agent_used}")
            return {
                "success": True,
                "id": result["id"] if result else None,
                "overall_score": overall_score
            }

    except Exception as e:
        logger.warning(f"Failed to record external quality metrics: {e}")
        return {"success": False, "error": str(e)}


def _get_quality_recommendations(overall_score: float, total_queries: int, context_precision: float) -> List[str]:
    """Generate recommendations based on metrics"""
    recommendations = []

    if total_queries == 0:
        recommendations.append("Ask questions to generate quality metrics")
        recommendations.append("Upload documents to start building your knowledge base")
    elif total_queries < 10:
        recommendations.append("Continue asking questions to build more reliable metrics")

    if overall_score > 0 and overall_score < 0.5:
        recommendations.append("Consider adding more relevant documents to improve answers")
        recommendations.append("Try breaking complex questions into simpler parts")

    if context_precision > 0 and context_precision < 0.6:
        recommendations.append("Review document chunking settings for better retrieval")

    if not recommendations:
        recommendations.append("Quality metrics look good! Keep monitoring for changes.")

    return recommendations


# =============================================================================
# WEB CRAWLER ENDPOINTS
# =============================================================================

class CrawlUrlRequest(BaseModel):
    url: str
    use_js: bool = True


class CrawlSiteRequest(BaseModel):
    url: str
    max_depth: int = 2
    max_pages: int = 20
    use_js: bool = True


class ResearchTopicRequest(BaseModel):
    topic: str
    seed_urls: Optional[List[str]] = None
    max_pages: int = 10
    min_relevance: float = 0.5
    use_js: bool = True
    index_to_rag: bool = True


@app.get("/crawler/status")
async def crawler_status():
    """Get web crawler status and capabilities"""
    return {
        "web_crawler_available": WEB_CRAWLER_AVAILABLE,
        "crawler_initialized": web_crawler is not None,
        "agentic_crawler_initialized": agentic_crawler is not None,
        "stats": web_crawler.get_stats() if web_crawler else {}
    }


@app.post("/crawler/preview")
async def preview_url(request: CrawlUrlRequest):
    """
    Quick preview of a URL without full crawling.
    Returns title and word count estimate.
    """
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    try:
        # Use lighter crawl without JS for preview
        result = await web_crawler.crawl_page(request.url, use_js=False)

        if not result.success:
            return {
                "success": False,
                "url": request.url,
                "error": result.error
            }

        return {
            "success": True,
            "url": result.url,
            "title": result.title,
            "word_count": result.word_count,
            "preview": result.content[:500] if result.content else ""
        }
    except Exception as e:
        logger.error(f"Preview failed for {request.url}: {e}")
        return {
            "success": False,
            "url": request.url,
            "error": str(e)
        }


@app.post("/crawler/crawl")
async def crawl_single_url(request: CrawlUrlRequest):
    """
    Crawl a single URL and extract content.

    Returns the extracted text content, markdown, and metadata.
    """
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    try:
        result = await web_crawler.crawl_page(request.url, use_js=request.use_js)

        return {
            "success": result.success,
            "url": result.url,
            "title": result.title,
            "content": result.content,
            "markdown": result.markdown,
            "word_count": result.word_count,
            "links_found": len(result.links),
            "links": result.links[:20],  # Limit to first 20 links
            "metadata": result.metadata,
            "crawled_at": result.crawled_at.isoformat(),
            "error": result.error
        }
    except Exception as e:
        logger.error(f"Crawl failed for {request.url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/crawler/crawl-site")
async def crawl_website(request: CrawlSiteRequest):
    """
    Crawl a website starting from the given URL.

    Follows links up to the specified depth and page limit.
    Returns all crawled pages with their content.
    """
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    try:
        results = []
        async for result in web_crawler.crawl_site(
            start_url=request.url,
            max_depth=request.max_depth,
            max_pages=request.max_pages,
            use_js=request.use_js
        ):
            results.append({
                "url": result.url,
                "title": result.title,
                "word_count": result.word_count,
                "success": result.success,
                "error": result.error
            })

        return {
            "success": True,
            "start_url": request.url,
            "pages_crawled": len(results),
            "results": results
        }
    except Exception as e:
        logger.error(f"Site crawl failed for {request.url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/crawler/research")
async def research_topic(request: ResearchTopicRequest):
    """
    Perform agentic research on a topic.

    Uses LLM agents to:
    1. Plan which URLs to crawl
    2. Evaluate content relevance
    3. Synthesize information across pages
    4. Index content to RAG system and create document entries
    """
    if agentic_crawler is None:
        raise HTTPException(status_code=503, detail="Agentic crawler not initialized")

    try:
        # Perform research
        research_results = await agentic_crawler.research_topic(
            topic=request.topic,
            seed_urls=request.seed_urls,
            max_pages=request.max_pages,
            min_relevance=request.min_relevance,
            use_js=request.use_js
        )

        # Index to RAG and create document entries
        indexed_count = 0
        document_ids = []

        if request.index_to_rag and vector_store and embedder:
            import hashlib

            # Get crawl results and evaluations
            crawl_results = research_results.get("crawl_results", [])
            evaluations = research_results.get("evaluations", [])

            # Create document entries for each crawled page that should be indexed
            for i, crawl_result in enumerate(crawl_results):
                eval_data = evaluations[i] if i < len(evaluations) else {}

                # Skip if not flagged for indexing
                if not eval_data.get("should_index", True):
                    continue

                url = crawl_result.get("url", "")
                title = crawl_result.get("title", url)
                content = crawl_result.get("content", "")
                word_count = crawl_result.get("word_count", 0)

                if not content or len(content) < 50:
                    continue

                # Create document entry in database
                doc_id = None
                if DB_AVAILABLE and DocumentRepository:
                    try:
                        content_hash = hashlib.sha256((url + content[:1000]).encode()).hexdigest()

                        existing = await DocumentRepository.get_by_hash(content_hash)
                        if existing:
                            doc_id = existing.id
                        else:
                            doc = await DocumentRepository.create(
                                filename=title or url,
                                file_hash=content_hash,
                                file_size=len(content.encode('utf-8')),
                                file_type="web",
                                metadata={
                                    "source": "web_import",
                                    "url": url,
                                    "title": title,
                                    "word_count": word_count,
                                    "topic": request.topic,
                                    "relevance_score": eval_data.get("relevance_score", 0),
                                    "quality_score": eval_data.get("quality_score", 0)
                                }
                            )
                            doc_id = doc.id
                        # Always append doc_id if we have one (for both new and existing docs)
                        if doc_id:
                            document_ids.append(doc_id)
                    except Exception as e:
                        logger.warning(f"Failed to create document entry for {url}: {e}")

                # Split content into chunks
                chunks = []
                paragraphs = content.split("\n\n")
                current_chunk = ""

                for para in paragraphs:
                    if len(current_chunk.split()) + len(para.split()) < 500:
                        current_chunk += para + "\n\n"
                    else:
                        if current_chunk.strip():
                            chunks.append(current_chunk.strip())
                        current_chunk = para + "\n\n"

                if current_chunk.strip():
                    chunks.append(current_chunk.strip())

                if not chunks:
                    chunks = [content[:2000]]

                # Generate embeddings
                embeddings = get_embedder().encode(chunks, normalize_embeddings=True).tolist()

                # Index using upsert if we have a doc_id
                if doc_id:
                    chunk_dicts = [{"content": chunk, "metadata": {
                        "source": "web_import",
                        "url": url,
                        "title": title,
                        "topic": request.topic
                    }} for chunk in chunks]
                    await vector_store.upsert(doc_id, chunk_dicts, embeddings)

                    # Update document status
                    try:
                        await DocumentRepository.update_status(
                            doc_id,
                            DocumentStatus.COMPLETED,
                            chunk_count=len(chunks)
                        )
                    except Exception as e:
                        logger.warning(f"Failed to update document status: {e}")
                else:
                    # Fallback to add_vectors
                    metadata_list = [{
                        "content": chunk,
                        "source": "web_import",
                        "url": url,
                        "title": title,
                        "topic": request.topic
                    } for chunk in chunks]
                    await vector_store.add_vectors(vectors=embeddings, metadata=metadata_list)

                indexed_count += 1

        research_results["indexed_to_rag"] = indexed_count > 0
        research_results["indexed_count"] = indexed_count
        research_results["document_ids"] = document_ids

        return research_results

    except Exception as e:
        logger.error(f"Research failed for topic '{request.topic}': {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/crawler/index-url")
async def index_url_to_rag(request: CrawlUrlRequest):
    """
    Crawl a URL and index its content directly to the RAG system.

    Extracts content, generates embeddings, and stores in vector database.
    Also creates a document entry so it appears in the documents list.
    """
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    if vector_store is None or embedder is None:
        raise HTTPException(status_code=503, detail="Vector store or embedder not initialized")

    try:
        # Crawl the page
        result = await web_crawler.crawl_page(request.url, use_js=request.use_js)

        if not result.success:
            return {
                "success": False,
                "url": request.url,
                "error": result.error
            }

        # Split content into chunks (simple sentence-based chunking)
        content = result.content
        chunks = []

        # Split by paragraphs, then combine into chunks of ~500 words
        paragraphs = content.split("\n\n")
        current_chunk = ""

        for para in paragraphs:
            if len(current_chunk.split()) + len(para.split()) < 500:
                current_chunk += para + "\n\n"
            else:
                if current_chunk.strip():
                    chunks.append(current_chunk.strip())
                current_chunk = para + "\n\n"

        if current_chunk.strip():
            chunks.append(current_chunk.strip())

        if not chunks:
            chunks = [content[:2000]]  # Fallback: use first 2000 chars

        # Create a document entry in the database so it appears in the document list
        import hashlib
        doc_id = None
        if DB_AVAILABLE and DocumentRepository:
            try:
                # Create a hash from the URL and content
                content_hash = hashlib.sha256((request.url + content[:1000]).encode()).hexdigest()

                # Check if already exists
                existing = await DocumentRepository.get_by_hash(content_hash)
                if existing:
                    doc_id = existing.id
                    logger.info(f"Web import already exists: {result.title}")
                else:
                    # Create document entry
                    doc = await DocumentRepository.create(
                        filename=result.title or request.url,
                        file_hash=content_hash,
                        file_size=len(content.encode('utf-8')),
                        file_type="web",
                        metadata={
                            "source": "web_import",
                            "url": request.url,
                            "title": result.title,
                            "word_count": result.word_count
                        }
                    )
                    doc_id = doc.id
                    logger.info(f"Created document entry for web import: {result.title} (id={doc_id})")
            except Exception as e:
                logger.warning(f"Failed to create document entry: {e}")

        # Generate embeddings for each chunk
        embeddings = get_embedder().encode(chunks, normalize_embeddings=True).tolist()

        # Prepare metadata for each chunk
        metadata_list = [
            {
                "content": chunk,
                "source": "web_crawl",
                "url": request.url,
                "title": result.title,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "document_id": doc_id
            }
            for i, chunk in enumerate(chunks)
        ]

        # Index to vector store using document_id if available
        if doc_id:
            # Use upsert with document_id for proper linking
            chunk_dicts = [{"content": chunk, "metadata": meta} for chunk, meta in zip(chunks, metadata_list)]
            await vector_store.upsert(doc_id, chunk_dicts, embeddings)
        else:
            # Fallback to add_vectors
            await vector_store.add_vectors(
                vectors=embeddings,
                metadata=metadata_list
            )

        # Update document status to completed
        if doc_id and DB_AVAILABLE and DocumentRepository:
            try:
                await DocumentRepository.update_status(
                    doc_id,
                    DocumentStatus.COMPLETED,
                    chunk_count=len(chunks)
                )
            except Exception as e:
                logger.warning(f"Failed to update document status: {e}")

        # Extract entities for knowledge graph
        entities_extracted = 0
        if knowledge_graph and hybrid_pipeline:
            try:
                entities = await hybrid_pipeline.extract_entities(content)
                for entity in entities:
                    knowledge_graph.add_entity(
                        name=entity.name,
                        entity_type=entity.entity_type,
                        properties={"source_url": result.url}
                    )
                entities_extracted = len(entities)
            except Exception as e:
                logger.warning(f"Entity extraction failed: {e}")

        return {
            "success": True,
            "url": result.url,
            "title": result.title,
            "word_count": result.word_count,
            "chunks_indexed": len(chunks),
            "entities_extracted": entities_extracted,
            "document_id": doc_id
        }

    except Exception as e:
        logger.error(f"Index URL failed for {request.url}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/crawler/batch-index")
async def batch_index_urls(urls: List[str], use_js: bool = True):
    """
    Crawl and index multiple URLs to the RAG system.

    Processes URLs in parallel with rate limiting.
    """
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    if vector_store is None or embedder is None:
        raise HTTPException(status_code=503, detail="Vector store or embedder not initialized")

    try:
        results = await web_crawler.crawl_urls(urls, use_js=use_js, parallel=3)

        indexed = []
        failed = []

        for result in results:
            if not result.success:
                failed.append({"url": result.url, "error": result.error})
                continue

            # Simple chunking
            content = result.content
            if len(content) < 100:
                failed.append({"url": result.url, "error": "Content too short"})
                continue

            # Generate embedding for full content (simplified)
            embedding = get_embedder().encode([content[:4000]], normalize_embeddings=True)[0].tolist()

            await vector_store.add_vectors(
                vectors=[embedding],
                metadata=[{
                    "content": content[:4000],
                    "source": "web_crawl_batch",
                    "url": result.url,
                    "title": result.title
                }]
            )

            indexed.append({
                "url": result.url,
                "title": result.title,
                "word_count": result.word_count
            })

        return {
            "success": True,
            "total_urls": len(urls),
            "indexed": len(indexed),
            "failed": len(failed),
            "indexed_results": indexed,
            "failed_results": failed
        }

    except Exception as e:
        logger.error(f"Batch index failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/crawler/cache/stats")
async def crawler_cache_stats():
    """Get web crawler cache statistics"""
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    return {
        "cache_size": web_crawler.cache.size(),
        "stats": web_crawler.get_stats()
    }


@app.delete("/crawler/cache/clear")
async def clear_crawler_cache():
    """Clear the web crawler cache"""
    if web_crawler is None:
        raise HTTPException(status_code=503, detail="Web crawler not initialized")

    web_crawler.cache.clear()

    return {
        "success": True,
        "message": "Crawler cache cleared"
    }


# =============================================================================
# MULTI-AGENT PIPELINE ENDPOINTS
# =============================================================================

class AgenticQueryRequest(BaseModel):
    """Request for agentic RAG query"""
    query: str
    user_id: Optional[int] = None
    conversation_id: Optional[str] = None
    max_iterations: Optional[int] = 3
    quality_threshold: Optional[float] = 0.75
    stream: bool = False


class AgenticQueryResponse(BaseModel):
    """Response from agentic RAG query"""
    success: bool
    response: str
    citations: List[Dict[str, Any]]
    metrics: Dict[str, Any]
    error: Optional[str] = None


@app.get("/agents/status")
async def agent_pipeline_status():
    """Get status of the multi-agent pipeline"""
    return {
        "available": agent_pipeline is not None,
        "agent_pipeline_module": AGENT_PIPELINE_AVAILABLE,
        "dependencies": {
            "ollama_service": ollama_service is not None,
            "vector_store": vector_store is not None,
            "reranker": neural_reranker is not None,
        },
        "config": agent_pipeline.get_pipeline_stats() if agent_pipeline else None,
    }


@app.post("/agents/query", response_model=AgenticQueryResponse)
async def agentic_query(request: AgenticQueryRequest):
    """
    Execute an agentic RAG query with multi-agent pipeline.

    The pipeline:
    1. Query Planner: Decomposes complex queries into sub-queries
    2. Retrieval Agent: Searches for relevant document chunks
    3. Synthesis Agent: Generates coherent response with citations
    4. Reflection Agent: Evaluates quality and iterates if needed

    Returns response with citations and quality metrics.
    """
    if agent_pipeline is None:
        raise HTTPException(
            status_code=503,
            detail="Agent pipeline not initialized. Check /agents/status for requirements."
        )

    try:
        result = await agent_pipeline.execute(
            query=request.query,
            user_id=request.user_id,
            conversation_id=request.conversation_id,
            config={
                "max_iterations": request.max_iterations,
                "quality_threshold": request.quality_threshold,
            }
        )

        return AgenticQueryResponse(
            success=result.success,
            response=result.response,
            citations=result.citations,
            metrics={
                "total_duration_ms": result.total_duration_ms,
                "iterations": result.iterations,
                "quality_score": result.quality_score,
            },
            error=result.error,
        )

    except Exception as e:
        logger.error(f"Agentic query error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/agents/query/stream")
async def agentic_query_stream(request: AgenticQueryRequest):
    """
    Stream agentic RAG query with real-time updates.

    Returns Server-Sent Events with:
    - status updates from each agent
    - streaming tokens during synthesis
    - final result with citations
    """
    if agent_pipeline is None:
        raise HTTPException(
            status_code=503,
            detail="Agent pipeline not initialized"
        )

    async def event_generator():
        try:
            async for update in agent_pipeline.execute_stream(
                query=request.query,
                user_id=request.user_id,
                conversation_id=request.conversation_id,
            ):
                event_data = {
                    "type": update.type,
                    "agent": update.agent,
                    "data": update.data,
                    "timestamp": update.timestamp,
                }
                yield f"data: {json.dumps(event_data)}\n\n"

        except Exception as e:
            error_event = {
                "type": "error",
                "agent": "orchestrator",
                "data": str(e),
            }
            yield f"data: {json.dumps(error_event)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        }
    )


@app.get("/agents/trace/{conversation_id}")
async def get_agent_trace(conversation_id: str):
    """
    Get execution trace for debugging.

    Returns the full trace of agent actions for a given conversation.
    """
    # This would need to be implemented with trace storage
    # For now, return a placeholder
    return {
        "conversation_id": conversation_id,
        "message": "Trace storage not yet implemented",
        "note": "Traces are available in the response during execution"
    }


# =============================================================================
# TOOL ENDPOINTS
# =============================================================================

class ToolExecuteRequest(BaseModel):
    """Request to execute a specific tool"""
    tool_name: str
    query: str
    user_id: Optional[int] = None


class ToolExecuteResponse(BaseModel):
    """Response from tool execution"""
    success: bool
    tool_name: str
    data: Optional[Dict[str, Any]] = None
    html: Optional[str] = None
    image_base64: Optional[str] = None
    formatted_output: Optional[str] = None
    error: Optional[str] = None


@app.get("/tools/list")
async def list_tools():
    """
    List all available AI tools for data analysis.
    """
    if agent_pipeline is None:
        return {"tools": [], "message": "Agent pipeline not initialized"}

    tools = agent_pipeline.tool_handlers
    return {
        "tools": [
            {
                "name": name,
                "description": getattr(handler, 'description', 'No description'),
                "available": True,
            }
            for name, handler in tools.items()
        ],
        "count": len(tools),
    }


@app.post("/tools/execute", response_model=ToolExecuteResponse)
async def execute_tool(request: ToolExecuteRequest):
    """
    Execute a specific AI tool directly.

    Available tools:
    - calculator: Mathematical operations on data
    - chart: Create interactive visualizations (bar, line, pie charts)
    - sql: Query user's data in PostgreSQL
    - code: Execute Python code in sandbox (numpy, pandas, math, statistics)
    - insights: Generate actionable insights from data
    """
    if agent_pipeline is None:
        raise HTTPException(
            status_code=503,
            detail="Agent pipeline not initialized"
        )

    if request.tool_name not in agent_pipeline.tool_handlers:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown tool: {request.tool_name}. Available: {list(agent_pipeline.tool_handlers.keys())}"
        )

    try:
        # Import AgentContext for tool execution
        from app.agents.base_agent import AgentContext

        # Create context for tool
        context = AgentContext(
            query=request.query,
            user_id=request.user_id,
        )

        # Get and execute the tool
        tool = agent_pipeline.tool_handlers[request.tool_name]
        result = await tool.execute(context)

        # Format output if available
        formatted = None
        if hasattr(tool, 'format_for_response'):
            formatted = tool.format_for_response(result)

        return ToolExecuteResponse(
            success=result.success,
            tool_name=request.tool_name,
            data=result.data,
            html=result.html,
            image_base64=getattr(result, 'image_base64', None),
            formatted_output=formatted,
            error=result.error,
        )

    except Exception as e:
        logger.error(f"Tool execution error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/tools/insights")
async def get_data_insights(user_id: int):
    """
    Generate comprehensive data insights for a user.

    Combines multiple tools to provide:
    - Document statistics
    - Usage patterns
    - Trend analysis
    - Actionable recommendations
    """
    if agent_pipeline is None or 'insights' not in agent_pipeline.tool_handlers:
        raise HTTPException(
            status_code=503,
            detail="Insights tool not available"
        )

    try:
        from app.agents.base_agent import AgentContext

        context = AgentContext(
            query="generate insights",
            user_id=user_id,
        )

        tool = agent_pipeline.tool_handlers['insights']
        result = await tool.execute(context)

        return {
            "success": result.success,
            "insights": result.data.get("insights", []) if result.data else [],
            "charts": result.data.get("charts", []) if result.data else [],
            "formatted": tool.format_for_response(result) if hasattr(tool, 'format_for_response') else None,
            "error": result.error,
        }

    except Exception as e:
        logger.error(f"Insights error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# AUDIT LOG ENDPOINTS
# =============================================================================

# Import audit repositories
try:
    from assemblyline_common.database import AuditLogRepository, ErrorLogRepository
    AUDIT_AVAILABLE = True
except ImportError:
    AUDIT_AVAILABLE = False
    logger.warning("Audit logging repositories not available")


class AuditLogFilter(BaseModel):
    """Filter parameters for audit log queries"""
    user_id: Optional[int] = None
    agent_used: Optional[str] = None
    success: Optional[bool] = None
    request_type: Optional[str] = None
    min_quality_score: Optional[int] = None


class ErrorLogFilter(BaseModel):
    """Filter parameters for error log queries"""
    severity: Optional[str] = None
    service_name: Optional[str] = None
    resolved: Optional[bool] = None


class ResolveErrorRequest(BaseModel):
    """Request to resolve an error"""
    resolution_notes: str
    resolved_by: int


@app.get("/audit/logs")
async def list_audit_logs(
    request: Request,
    page: int = 1,
    page_size: int = 50,
    user_id: Optional[int] = None,
    agent_used: Optional[str] = None,
    success: Optional[bool] = None,
    request_type: Optional[str] = None,
    min_quality_score: Optional[int] = None
):
    """
    List agent audit logs - ADMIN ONLY

    Supports filtering by:
    - user_id: Filter by specific user
    - agent_used: Filter by agent type (fast, quality, deep, guardrails)
    - success: Filter by success/failure status
    - request_type: Filter by request type (chat, rag_workflow, swarm_routing, etc.)
    - min_quality_score: Minimum quality score (0-100)
    """
    require_admin(request)  # Permission check
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        # Convert page/page_size to limit/offset
        offset = (page - 1) * page_size

        logs = await AuditLogRepository.list_all(
            user_id=user_id,
            agent_used=agent_used,
            success=success,
            limit=page_size,
            offset=offset
        )

        total = await AuditLogRepository.count(
            user_id=user_id,
            success=success
        )

        return {
            "logs": logs,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size
        }

    except Exception as e:
        logger.error(f"Failed to list audit logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/logs/{log_id}")
async def get_audit_log(log_id: int):
    """Get a specific audit log entry with full details"""
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        log = await AuditLogRepository.get_by_id(log_id)
        if not log:
            raise HTTPException(status_code=404, detail="Audit log not found")
        return log

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get audit log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/errors")
async def list_error_logs(
    page: int = 1,
    page_size: int = 50,
    severity: Optional[str] = None,
    service_name: Optional[str] = None,
    resolved: Optional[bool] = None
):
    """
    List system error logs with pagination and filters.

    Supports filtering by:
    - severity: debug, info, warning, error, critical
    - service_name: orchestrator, rag, chat, auth, etc.
    - resolved: Filter by resolution status
    """
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        # Convert page/page_size to limit/offset
        offset = (page - 1) * page_size

        errors = await ErrorLogRepository.list_all(
            service_name=service_name,
            severity=severity,
            resolved=resolved,
            limit=page_size,
            offset=offset
        )

        total = await ErrorLogRepository.count(
            severity=severity,
            resolved=resolved
        )

        return {
            "errors": errors,
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": (total + page_size - 1) // page_size
        }

    except Exception as e:
        logger.error(f"Failed to list error logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/errors/{error_id}")
async def get_error_log(error_id: int):
    """Get a specific error log entry with full details"""
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        error = await ErrorLogRepository.get_by_id(error_id)
        if not error:
            raise HTTPException(status_code=404, detail="Error log not found")
        return error

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get error log: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/audit/errors/{error_id}/resolve")
async def resolve_error(error_id: int, resolve_request: ResolveErrorRequest, request: Request):
    """
    Mark an error as resolved - ADMIN ONLY
    """
    require_admin(request)  # Permission check
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        success = await ErrorLogRepository.resolve_error(
            error_id=error_id,
            resolution_notes=resolve_request.resolution_notes,
            resolved_by=resolve_request.resolved_by
        )

        if not success:
            raise HTTPException(status_code=404, detail="Error log not found")

        return {"success": True, "error_id": error_id, "status": "resolved"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to resolve error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/stats")
async def get_audit_stats(days: int = 7):
    """
    Get aggregate audit statistics.

    Returns:
    - Total requests and success rate
    - Average quality score and latency
    - Agent usage distribution
    - Quality grade distribution
    - Error severity distribution
    """
    if not AUDIT_AVAILABLE:
        raise HTTPException(status_code=503, detail="Audit logging not available")

    try:
        from datetime import timedelta
        start_date = datetime.now() - timedelta(days=days)

        audit_stats = await AuditLogRepository.get_stats(start_date=start_date)
        error_stats = await ErrorLogRepository.get_stats(start_date=start_date)

        return {
            "period_days": days,
            "audit": audit_stats,
            "errors": error_stats
        }

    except Exception as e:
        logger.error(f"Failed to get audit stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/audit/status")
async def get_audit_status():
    """Check audit logging availability and status"""
    return {
        "audit_available": AUDIT_AVAILABLE,
        "database_available": DB_AVAILABLE
    }


# =============================================================================
# SEMANTIC CLUSTERING ENDPOINTS
# =============================================================================

@app.get("/clustering/status")
async def get_clustering_status():
    """Check semantic clustering availability and status"""
    return {
        "clustering_available": SEMANTIC_CLUSTERING_AVAILABLE,
        "database_available": DB_AVAILABLE
    }


@app.post("/clustering/cluster")
async def cluster_user_documents(
    request: Request,
    algorithm: Optional[str] = "kmeans"
):
    """
    Perform semantic clustering on all user's documents.

    Groups documents by semantic similarity using embeddings.
    """
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1  # Default for testing

    try:
        # Get or create clustering engine
        pool = await get_db_pool()
        engine = await get_clustering_engine(db_pool=pool)

        # Parse algorithm
        algo = ClusteringAlgorithm.KMEANS
        if algorithm.lower() == "hdbscan":
            algo = ClusteringAlgorithm.HDBSCAN

        # Perform clustering
        result = await engine.cluster_documents(user_id=user_id, algorithm=algo)

        # Save clusters to database
        if result.clusters:
            await engine.save_clusters(user_id=user_id, result=result)

        return {
            "success": True,
            "total_documents": result.total_documents,
            "clusters_created": len(result.clusters),
            "unclustered_documents": len(result.unclustered_docs),
            "execution_time_ms": result.execution_time_ms,
            "algorithm": result.algorithm_used,
            "clusters": [
                {
                    "name": c.name,
                    "description": c.description,
                    "document_count": len(c.document_ids),
                    "coherence_score": c.coherence_score,
                    "document_ids": c.document_ids
                }
                for c in result.clusters
            ]
        }

    except Exception as e:
        logger.error(f"Clustering error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/clustering/clusters")
async def get_user_clusters(request: Request):
    """Get all semantic clusters for the current user"""
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()
        engine = await get_clustering_engine(db_pool=pool)

        clusters = await engine.get_user_clusters(user_id=user_id)

        return {
            "user_id": user_id,
            "clusters": clusters,
            "total_clusters": len(clusters)
        }

    except Exception as e:
        logger.error(f"Error fetching clusters: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/clustering/clusters/{cluster_id}/rename")
async def rename_cluster(
    cluster_id: int,
    new_name: str,
    new_description: Optional[str] = None
):
    """Manually rename a cluster"""
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    try:
        pool = await get_db_pool()
        engine = await get_clustering_engine(db_pool=pool)

        success = await engine.rename_cluster(
            cluster_id=cluster_id,
            new_name=new_name,
            new_description=new_description
        )

        return {"success": success, "cluster_id": cluster_id, "new_name": new_name}

    except Exception as e:
        logger.error(f"Error renaming cluster: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/clustering/assign")
async def assign_document_to_cluster(
    request: Request,
    document_id: int,
    cluster_id: int
):
    """Manually assign a document to a cluster"""
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    try:
        pool = await get_db_pool()
        engine = await get_clustering_engine(db_pool=pool)

        success = await engine.assign_document_to_cluster(
            document_id=document_id,
            cluster_id=cluster_id,
            membership_score=1.0  # Manual assignment has full score
        )

        return {"success": success, "document_id": document_id, "cluster_id": cluster_id}

    except Exception as e:
        logger.error(f"Error assigning document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/clustering/document/{document_id}")
async def get_document_cluster(
    request: Request,
    document_id: int
):
    """Get cluster information for a specific document"""
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    if not DB_AVAILABLE:
        raise HTTPException(status_code=503, detail="Database not available")

    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT
                    sc.id, sc.cluster_name, sc.cluster_description,
                    dcm.membership_score, dcm.is_primary_cluster
                FROM document_cluster_membership dcm
                JOIN semantic_clusters sc ON dcm.cluster_id = sc.id
                WHERE dcm.document_id = $1
                ORDER BY dcm.is_primary_cluster DESC, dcm.membership_score DESC
                LIMIT 1
            """, document_id)

            if row:
                return {
                    "document_id": document_id,
                    "cluster_id": row['id'],
                    "cluster_name": row['cluster_name'],
                    "cluster_description": row['cluster_description'],
                    "membership_score": row['membership_score'],
                    "is_primary_cluster": row['is_primary_cluster']
                }
            else:
                return {
                    "document_id": document_id,
                    "cluster_id": None,
                    "message": "Document not assigned to any cluster"
                }

    except Exception as e:
        logger.error(f"Error fetching document cluster: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/clustering/recluster")
async def trigger_reclustering(
    request: Request,
    force: bool = False
):
    """
    Trigger reclustering if needed (based on time threshold or force flag).

    Checks if enough time has passed since last clustering before running.
    """
    if not SEMANTIC_CLUSTERING_AVAILABLE:
        raise HTTPException(status_code=503, detail="Semantic clustering not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()

        # Check last clustering time
        async with pool.acquire() as conn:
            last_cluster = await conn.fetchval("""
                SELECT MAX(last_reclustered_at)
                FROM semantic_clusters
                WHERE user_id = $1
            """, user_id)

            # Only recluster if forced or > 24 hours since last clustering
            from datetime import datetime, timedelta
            should_recluster = force or last_cluster is None or \
                (datetime.now() - last_cluster) > timedelta(hours=24)

            if not should_recluster:
                return {
                    "success": False,
                    "message": "Clustering not needed yet",
                    "last_clustered": last_cluster.isoformat() if last_cluster else None,
                    "next_clustering": (last_cluster + timedelta(hours=24)).isoformat() if last_cluster else None
                }

        # Perform clustering
        engine = await get_clustering_engine(db_pool=pool)
        result = await engine.cluster_documents(user_id=user_id)

        if result.clusters:
            await engine.save_clusters(user_id=user_id, result=result)

        return {
            "success": True,
            "clusters_created": len(result.clusters),
            "total_documents": result.total_documents,
            "execution_time_ms": result.execution_time_ms
        }

    except Exception as e:
        logger.error(f"Reclustering error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/search/cluster-enhanced")
async def cluster_enhanced_search(
    request: Request,
    query: str,
    top_k: int = 10,
    include_related_clusters: bool = True
):
    """
    Search with cluster-based context enhancement.

    Finds relevant documents and expands to include related documents
    from the same clusters for better context.
    """
    if vector_store is None:
        raise HTTPException(status_code=503, detail="Vector store not initialized")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        # Generate query embedding
        query_embedding = get_embedder().encode(query, normalize_embeddings=True).tolist()

        # Basic search
        search_results = await vector_store.search(
            query_embedding=query_embedding,
            top_k=top_k,
            user_id=user_id
        )

        if not search_results:
            return {
                "results": [],
                "cluster_context": [],
                "message": "No results found"
            }

        # Get cluster information for top results
        cluster_context = []
        if SEMANTIC_CLUSTERING_AVAILABLE and include_related_clusters:
            pool = await get_db_pool()
            async with pool.acquire() as conn:
                # Get document IDs from results
                doc_ids = list(set(r.get('document_id') for r in search_results if r.get('document_id')))

                if doc_ids:
                    # Get clusters for these documents
                    cluster_rows = await conn.fetch("""
                        SELECT DISTINCT
                            sc.id, sc.cluster_name, sc.cluster_description,
                            ARRAY_AGG(DISTINCT dcm.document_id) as doc_ids
                        FROM semantic_clusters sc
                        JOIN document_cluster_membership dcm ON sc.id = dcm.cluster_id
                        WHERE dcm.document_id = ANY($1)
                        GROUP BY sc.id
                    """, doc_ids)

                    for row in cluster_rows:
                        # Get additional documents from same cluster (not in original results)
                        related_docs = await conn.fetch("""
                            SELECT d.id, d.filename
                            FROM document_cluster_membership dcm
                            JOIN documents d ON dcm.document_id = d.id
                            WHERE dcm.cluster_id = $1
                            AND dcm.document_id != ALL($2)
                            ORDER BY dcm.membership_score DESC
                            LIMIT 5
                        """, row['id'], doc_ids)

                        cluster_context.append({
                            "cluster_id": row['id'],
                            "cluster_name": row['cluster_name'],
                            "cluster_description": row['cluster_description'],
                            "matched_documents": list(row['doc_ids']),
                            "related_documents": [
                                {"id": d['id'], "filename": d['filename']}
                                for d in related_docs
                            ]
                        })

        return {
            "query": query,
            "results": search_results,
            "cluster_context": cluster_context,
            "total_results": len(search_results),
            "clusters_matched": len(cluster_context)
        }

    except Exception as e:
        logger.error(f"Cluster-enhanced search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# PREDICTIVE CACHING ENDPOINTS
# =============================================================================

@app.get("/cache/status")
async def get_predictive_cache_status():
    """Check predictive caching availability and status"""
    return {
        "predictive_cache_available": PREDICTIVE_CACHE_AVAILABLE,
        "database_available": DB_AVAILABLE
    }


@app.get("/cache/stats")
async def get_cache_statistics(
    request: Request,
    days: int = 7
):
    """Get predictive cache performance statistics"""
    if not PREDICTIVE_CACHE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Predictive caching not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()
        cache = await get_predictive_cache(db_pool=pool)
        stats = await cache.get_stats(user_id=user_id, days=days)

        return {
            "user_id": user_id,
            "days": days,
            "total_queries": stats.total_queries,
            "cache_hits": stats.cache_hits,
            "cache_misses": stats.cache_misses,
            "hit_rate": round(stats.hit_rate * 100, 2),
            "time_saved_ms": stats.time_saved_ms,
            "pattern_accuracy": round(stats.pattern_accuracy * 100, 2)
        }

    except Exception as e:
        logger.error(f"Error fetching cache stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cache/patterns")
async def get_learned_patterns(request: Request):
    """Get learned behavior patterns for current user"""
    if not PREDICTIVE_CACHE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Predictive caching not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()
        learner = await get_pattern_learner(db_pool=pool)
        patterns = await learner.get_patterns(user_id=user_id)

        return {
            "user_id": user_id,
            "total_patterns": len(patterns),
            "patterns": [
                {
                    "id": p.id,
                    "type": p.pattern_type.value,
                    "definition": p.pattern_definition,
                    "confidence": round(p.confidence * 100, 2),
                    "hit_count": p.hit_count,
                    "correct_predictions": p.correct_predictions,
                    "last_triggered": p.last_triggered_at.isoformat() if p.last_triggered_at else None
                }
                for p in patterns
            ]
        }

    except Exception as e:
        logger.error(f"Error fetching patterns: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/record-query")
async def record_query_for_learning(
    request: Request,
    query: str,
    query_type: str = "rag_search",
    response_time_ms: int = 0
):
    """Record a query for pattern learning"""
    if not PREDICTIVE_CACHE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Predictive caching not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()
        learner = await get_pattern_learner(db_pool=pool)

        # Generate embedding for query
        query_embedding = None
        if get_embedder() is not None:
            query_embedding = get_embedder().encode(query, normalize_embeddings=True).tolist()

        await learner.record_query(
            user_id=user_id,
            query=query,
            query_type=query_type,
            response_time_ms=response_time_ms,
            query_embedding=query_embedding
        )

        return {"success": True, "message": "Query recorded for pattern learning"}

    except Exception as e:
        logger.error(f"Error recording query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/check")
async def check_predictive_cache(
    request: Request,
    query: str
):
    """Check if a query result is available in the predictive cache"""
    if not PREDICTIVE_CACHE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Predictive caching not available")

    try:
        user_id = require_user_id(request)
    except Exception:
        user_id = 1

    try:
        pool = await get_db_pool()
        cache = await get_predictive_cache(db_pool=pool)
        result = await cache.check_cache(query=query, user_id=user_id)

        if result:
            return {
                "cache_hit": True,
                "confidence": result['confidence'],
                "result": result['result']
            }
        else:
            return {
                "cache_hit": False,
                "message": "Query not in predictive cache"
            }

    except Exception as e:
        logger.error(f"Cache check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/cache/cleanup")
async def cleanup_expired_cache():
    """Clean up expired cache entries"""
    if not PREDICTIVE_CACHE_AVAILABLE:
        raise HTTPException(status_code=503, detail="Predictive caching not available")

    try:
        pool = await get_db_pool()
        cache = await get_predictive_cache(db_pool=pool)
        deleted_count = await cache.cleanup_expired()

        return {"success": True, "deleted_entries": deleted_count}

    except Exception as e:
        logger.error(f"Cache cleanup error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7002)
