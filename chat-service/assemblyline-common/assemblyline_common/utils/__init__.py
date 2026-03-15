"""
T.A.L.O.S. Shared Utilities
Based on Memobytes patterns

Standard modules:
- redis_cache: Redis caching layer
- neo4j_knowledge_graph: Neo4j knowledge graph
- celery_tasks: Celery task queue
- crawl4ai_crawler: Web crawling with crawl4ai
- mem0_memory: AI memory layer with mem0

Performance modules (recommended):
- arq_tasks: ARQ task queue (10x less memory than Celery)
- dragonfly_cache: DragonflyDB cache (25x faster than Redis)
- infinity_embeddings: Infinity embeddings (10,000+ vs 100-500/sec)
- fast_llm: vLLM/llama.cpp (5x faster than Ollama)
- pgbouncer_pool: PostgreSQL connection pooling (<1ms overhead)
"""

# =============================================================================
# STANDARD UTILITIES
# =============================================================================

from assemblyline_common.utils.redis_cache import (
    RedisCache,
    AsyncRedisCache,
    AIResponseCache,
    OCRCache,
    SessionCache,
    RateLimiter,
    ChatStateCache,
    get_redis_cache,
    get_async_redis_cache,
    is_redis_available,
    cached
)

from assemblyline_common.utils.neo4j_knowledge_graph import (
    KnowledgeGraphService,
    Neo4jConfig,
    get_knowledge_graph,
    is_neo4j_available
)

from assemblyline_common.utils.celery_tasks import (
    celery_app,
    TALOSTask,
    TaskManager,
    is_celery_available,
    get_celery_app,
    get_task_manager
)

from assemblyline_common.utils.crawl4ai_crawler import (
    WebCrawlerService,
    ResearchCrawler,
    CrawlerConfig,
    CrawlResult,
    get_web_crawler,
    is_crawl4ai_available,
    quick_crawl
)

from assemblyline_common.utils.mem0_memory import (
    MemoryService,
    ConversationMemory,
    UserPreferencesMemory,
    LearningMemory,
    MemoryConfig,
    MemoryItem,
    get_memory_service,
    is_mem0_available,
    quick_add_memory,
    quick_search_memories
)

# =============================================================================
# PERFORMANCE UTILITIES
# =============================================================================

from assemblyline_common.utils.arq_tasks import (
    ARQTaskManager,
    ARQConfig,
    WorkerSettings as ARQWorkerSettings,
    get_task_manager as get_arq_manager,
    is_arq_available,
    enqueue_task
)

from assemblyline_common.utils.dragonfly_cache import (
    AsyncDragonflyCache,
    DragonflyConfig,
    AIResponseCache as DragonflyAICache,
    EmbeddingCache,
    RateLimiter as DragonflyRateLimiter,
    get_cache as get_dragonfly_cache,
    is_dragonfly_available
)

from assemblyline_common.utils.infinity_embeddings import (
    InfinityEmbeddings,
    InfinityConfig,
    SmartEmbeddings,
    SentenceTransformersFallback,
    get_embeddings,
    quick_embed,
    quick_embed_batch
)

from assemblyline_common.utils.fast_llm import (
    SmartLLM,
    VLLMClient,
    LlamaCppClient,
    OllamaClient,
    LLMConfig,
    LLMBackend,
    get_llm,
    quick_generate,
    quick_chat
)

from assemblyline_common.utils.pgbouncer_pool import (
    AsyncPGPool,
    VectorPool,
    PostgresConfig,
    get_pool,
    get_vector_pool,
    is_asyncpg_available
)

# =============================================================================
# RESPONSE POST-PROCESSING
# =============================================================================

from assemblyline_common.utils.response_postprocessor import (
    ResponseFormatter,
    CleanerType,
    CleanerResult,
    MarkdownCleaner,
    HeaderRemover,
    WhitespaceCleaner,
    CodeBlockProcessor,
    LinkCleaner,
    ListProcessor,
    HTMLCleaner,
    EmojiCleaner,
    CitationCleaner,
    clean_response,
    clean_for_tts,
    clean_for_display,
    create_tts_formatter,
    create_plain_text_formatter,
    create_chat_formatter,
)


__all__ = [
    # Redis (standard)
    "RedisCache",
    "AsyncRedisCache",
    "AIResponseCache",
    "OCRCache",
    "SessionCache",
    "RateLimiter",
    "ChatStateCache",
    "get_redis_cache",
    "get_async_redis_cache",
    "is_redis_available",
    "cached",
    # Neo4j
    "KnowledgeGraphService",
    "Neo4jConfig",
    "get_knowledge_graph",
    "is_neo4j_available",
    # Celery (standard)
    "celery_app",
    "TALOSTask",
    "TaskManager",
    "is_celery_available",
    "get_celery_app",
    "get_task_manager",
    # crawl4ai
    "WebCrawlerService",
    "ResearchCrawler",
    "CrawlerConfig",
    "CrawlResult",
    "get_web_crawler",
    "is_crawl4ai_available",
    "quick_crawl",
    # mem0
    "MemoryService",
    "ConversationMemory",
    "UserPreferencesMemory",
    "LearningMemory",
    "MemoryConfig",
    "MemoryItem",
    "get_memory_service",
    "is_mem0_available",
    "quick_add_memory",
    "quick_search_memories",
    # ARQ (performance - replaces Celery)
    "ARQTaskManager",
    "ARQConfig",
    "ARQWorkerSettings",
    "get_arq_manager",
    "is_arq_available",
    "enqueue_task",
    # DragonflyDB (performance - replaces Redis)
    "AsyncDragonflyCache",
    "DragonflyConfig",
    "DragonflyAICache",
    "EmbeddingCache",
    "DragonflyRateLimiter",
    "get_dragonfly_cache",
    "is_dragonfly_available",
    # Infinity (performance - replaces sentence-transformers)
    "InfinityEmbeddings",
    "InfinityConfig",
    "SmartEmbeddings",
    "SentenceTransformersFallback",
    "get_embeddings",
    "quick_embed",
    "quick_embed_batch",
    # Fast LLM (performance - replaces Ollama)
    "SmartLLM",
    "VLLMClient",
    "LlamaCppClient",
    "OllamaClient",
    "LLMConfig",
    "LLMBackend",
    "get_llm",
    "quick_generate",
    "quick_chat",
    # pgbouncer (performance - enhances asyncpg)
    "AsyncPGPool",
    "VectorPool",
    "PostgresConfig",
    "get_pool",
    "get_vector_pool",
    "is_asyncpg_available",
    # Response post-processing
    "ResponseFormatter",
    "CleanerType",
    "CleanerResult",
    "MarkdownCleaner",
    "HeaderRemover",
    "WhitespaceCleaner",
    "CodeBlockProcessor",
    "LinkCleaner",
    "ListProcessor",
    "HTMLCleaner",
    "EmojiCleaner",
    "CitationCleaner",
    "clean_response",
    "clean_for_tts",
    "clean_for_display",
    "create_tts_formatter",
    "create_plain_text_formatter",
    "create_chat_formatter",
]
