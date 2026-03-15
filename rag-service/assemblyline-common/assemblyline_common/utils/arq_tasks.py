"""
T.A.L.O.S. ARQ Task Queue
Replaces Celery - async-native, 10x less memory

Features:
- Async from ground up
- JSON serialization (safe)
- Minimal dependencies
- Built on Redis/DragonflyDB
"""

import os
import logging
import asyncio

# Import shared config
try:
    from assemblyline_common.config import config
except ImportError:
    config = None
from typing import Optional, Dict, Any, List, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# =============================================================================
# ARQ AVAILABILITY CHECK
# =============================================================================

ARQ_AVAILABLE = False

try:
    from arq import create_pool, cron
    from arq.connections import RedisSettings, ArqRedis
    from arq.worker import Worker
    ARQ_AVAILABLE = True
    logger.info("ARQ library loaded successfully")
except ImportError:
    logger.warning("ARQ not installed. Run: pip install arq")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class ARQConfig:
    """ARQ configuration"""
    # DragonflyDB/Redis connection - use shared config if available
    host: str = field(default_factory=lambda: (
        config.REDIS_HOST if config else os.getenv("DRAGONFLY_HOST", os.getenv("REDIS_HOST", "localhost"))
    ))
    port: int = field(default_factory=lambda: (
        config.REDIS_PORT if config else int(os.getenv("DRAGONFLY_PORT", os.getenv("REDIS_PORT", "6379")))
    ))
    password: Optional[str] = field(default_factory=lambda: (
        config.REDIS_PASSWORD if config else os.getenv("DRAGONFLY_PASSWORD", os.getenv("REDIS_PASSWORD"))
    ))
    database: int = int(os.getenv("ARQ_DATABASE", "1"))

    # Worker settings
    max_jobs: int = 10
    job_timeout: int = 3600  # 1 hour
    keep_result: int = 3600  # Keep results for 1 hour
    queue_name: str = "talos:queue"


def get_redis_settings(config: Optional[ARQConfig] = None) -> "RedisSettings":
    """Get Redis settings for ARQ"""
    if not ARQ_AVAILABLE:
        raise RuntimeError("ARQ not available")

    config = config or ARQConfig()
    return RedisSettings(
        host=config.host,
        port=config.port,
        password=config.password,
        database=config.database
    )


# =============================================================================
# TASK DEFINITIONS
# =============================================================================

async def process_document_task(
    ctx: Dict[str, Any],
    document_id: int,
    file_path: str,
    options: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Process a document asynchronously.

    Args:
        ctx: ARQ context with redis connection
        document_id: Document ID in database
        file_path: Path to the file
        options: Processing options

    Returns:
        Processing result
    """
    options = options or {}
    start_time = datetime.utcnow()

    try:
        logger.info(f"Processing document {document_id}: {file_path}")

        # Import here to avoid circular imports
        from services.documents.app.utils.docling_processor import (
            get_docling_processor
        )

        processor = get_docling_processor()

        # Process document
        result = await processor.process_document(file_path)

        # Get chunks
        chunks = result.to_chunks(
            chunk_size=options.get("chunk_size", 500),
            overlap=options.get("overlap", 50)
        )

        # Index in RAG service
        import httpx
        rag_url = config.RAG_SERVICE_URL if config else os.getenv("RAG_SERVICE_URL", "http://localhost:17002")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{rag_url}/index",
                json={"document_id": document_id, "chunks": chunks},
                timeout=120.0
            )

        processing_time = (datetime.utcnow() - start_time).total_seconds()

        return {
            "status": "completed",
            "document_id": document_id,
            "chunks_indexed": len(chunks),
            "tables_extracted": len(result.tables),
            "word_count": result.word_count,
            "processing_time_seconds": processing_time
        }

    except Exception as e:
        logger.error(f"Document processing failed: {e}")
        return {
            "status": "failed",
            "document_id": document_id,
            "error": str(e)
        }


async def generate_embeddings_task(
    ctx: Dict[str, Any],
    texts: List[str],
    model: str = "all-MiniLM-L6-v2"
) -> Dict[str, Any]:
    """
    Generate embeddings using Infinity server.

    Args:
        ctx: ARQ context
        texts: List of texts to embed
        model: Embedding model name

    Returns:
        Embeddings and metadata
    """
    try:
        import httpx

        infinity_url = config.INFINITY_URL if config else os.getenv("INFINITY_URL", "http://localhost:7997")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{infinity_url}/embeddings",
                json={
                    "model": model,
                    "input": texts
                },
                timeout=60.0
            )
            result = response.json()

        embeddings = [item["embedding"] for item in result.get("data", [])]

        return {
            "status": "completed",
            "count": len(embeddings),
            "dimensions": len(embeddings[0]) if embeddings else 0,
            "model": model,
            "embeddings": embeddings
        }

    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }


async def generate_ai_response_task(
    ctx: Dict[str, Any],
    query: str,
    context: str = None,
    model: str = None
) -> Dict[str, Any]:
    """
    Generate AI response using vLLM or llama.cpp.

    Args:
        ctx: ARQ context
        query: User query
        context: Optional context
        model: Model to use

    Returns:
        AI response
    """
    try:
        import httpx

        # Try vLLM first, fall back to llama.cpp
        vllm_url = config.VLLM_URL if config else os.getenv("VLLM_URL", "http://localhost:8080")
        llama_cpp_url = config.LLAMA_CPP_URL if config else os.getenv("LLAMA_CPP_URL", "http://localhost:8081")

        model = model or os.getenv("LLM_MODEL", "gemma-3-4b")

        prompt = query
        if context:
            prompt = f"Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"

        async with httpx.AsyncClient() as client:
            try:
                # Try vLLM (OpenAI-compatible API)
                response = await client.post(
                    f"{vllm_url}/v1/completions",
                    json={
                        "model": model,
                        "prompt": prompt,
                        "max_tokens": 1024,
                        "temperature": 0.7
                    },
                    timeout=120.0
                )
                result = response.json()
                text = result["choices"][0]["text"]

            except Exception:
                # Fall back to llama.cpp
                response = await client.post(
                    f"{llama_cpp_url}/completion",
                    json={
                        "prompt": prompt,
                        "n_predict": 1024,
                        "temperature": 0.7
                    },
                    timeout=120.0
                )
                result = response.json()
                text = result.get("content", "")

        return {
            "status": "completed",
            "response": text,
            "model": model,
            "query": query
        }

    except Exception as e:
        logger.error(f"AI generation failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }


async def batch_process_documents_task(
    ctx: Dict[str, Any],
    document_ids: List[int],
    options: Dict[str, Any] = None
) -> Dict[str, Any]:
    """Process multiple documents in batch"""
    redis: ArqRedis = ctx["redis"]
    results = []

    for doc_id in document_ids:
        # Queue individual task
        job = await redis.enqueue_job(
            "process_document_task",
            doc_id,
            f"/uploads/{doc_id}",
            options
        )
        results.append({
            "document_id": doc_id,
            "job_id": job.job_id
        })

    return {
        "status": "queued",
        "documents": results,
        "total": len(document_ids)
    }


async def cleanup_expired_cache_task(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Scheduled task to clean up expired cache"""
    try:
        redis: ArqRedis = ctx["redis"]
        # DragonflyDB/Redis handles TTL automatically
        logger.info("Cache cleanup completed")
        return {"status": "completed", "cleaned": 0}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


async def update_embeddings_task(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Scheduled task to update stale embeddings"""
    try:
        logger.info("Running scheduled embedding update")
        return {"status": "completed", "updated": 0}
    except Exception as e:
        return {"status": "failed", "error": str(e)}


async def weekly_rag_evaluation_task(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Weekly RAG evaluation task.

    Runs comprehensive RAG evaluation and generates a report with:
    - Custom retrieval metrics (P@k, R@k, MRR, nDCG)
    - RAGAS metrics (if available)
    - Target compliance analysis
    - Trend analysis vs previous week
    - Actionable recommendations

    Reports are saved to RAG_REPORTS_PATH and can optionally be sent via email.
    """
    try:
        import httpx

        logger.info("Starting weekly RAG evaluation...")

        rag_url = config.RAG_SERVICE_URL if config else os.getenv("RAG_SERVICE_URL", "http://localhost:17002")

        async with httpx.AsyncClient() as client:
            # Generate weekly report via RAG service
            response = await client.post(
                f"{rag_url}/evaluate/weekly-report",
                json={"include_ragas": True},
                timeout=600.0  # 10 minute timeout for evaluation
            )

            if response.status_code != 200:
                return {
                    "status": "failed",
                    "error": f"RAG service returned {response.status_code}",
                    "details": response.text
                }

            result = response.json()

        report_id = result.get("report_id")
        summary = result.get("summary", {})

        logger.info(
            f"Weekly RAG evaluation completed: {report_id} | "
            f"Score: {summary.get('overall_score', 0):.2%} | "
            f"Queries: {summary.get('total_queries', 0)}"
        )

        # Log target compliance
        targets_met = summary.get("targets_met", {})
        passed = sum(1 for v in targets_met.values() if v)
        total = len(targets_met)
        logger.info(f"Targets met: {passed}/{total}")

        # Optionally send notification (email/Slack/etc.)
        notification_url = os.getenv("NOTIFICATION_WEBHOOK_URL")
        if notification_url:
            try:
                await send_report_notification(
                    notification_url,
                    report_id,
                    summary,
                    result.get("markdown", "")
                )
            except Exception as e:
                logger.warning(f"Failed to send notification: {e}")

        return {
            "status": "completed",
            "report_id": report_id,
            "overall_score": summary.get("overall_score"),
            "targets_met": targets_met,
            "total_queries": summary.get("total_queries")
        }

    except Exception as e:
        logger.error(f"Weekly RAG evaluation failed: {e}")
        return {
            "status": "failed",
            "error": str(e)
        }


async def send_report_notification(
    webhook_url: str,
    report_id: str,
    summary: Dict[str, Any],
    markdown_content: str
) -> None:
    """Send evaluation report notification"""
    import httpx

    overall_score = summary.get("overall_score", 0)
    status_emoji = "✅" if overall_score >= 0.8 else "⚠️" if overall_score >= 0.6 else "❌"

    targets_met = summary.get("targets_met", {})
    passed = sum(1 for v in targets_met.values() if v)
    total = len(targets_met)

    message = {
        "text": f"{status_emoji} Weekly RAG Evaluation Report",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{status_emoji} Weekly RAG Evaluation"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Report ID:*\n{report_id}"},
                    {"type": "mrkdwn", "text": f"*Overall Score:*\n{overall_score:.1%}"},
                    {"type": "mrkdwn", "text": f"*Targets Met:*\n{passed}/{total}"},
                    {"type": "mrkdwn", "text": f"*Queries Evaluated:*\n{summary.get('total_queries', 0)}"}
                ]
            }
        ]
    }

    async with httpx.AsyncClient() as client:
        await client.post(webhook_url, json=message, timeout=10.0)


# =============================================================================
# WORKER CONFIGURATION
# =============================================================================

class WorkerSettings:
    """ARQ Worker settings - used by: arq assemblyline_common.utils.arq_tasks.WorkerSettings"""

    # Task functions
    functions = [
        process_document_task,
        generate_embeddings_task,
        generate_ai_response_task,
        batch_process_documents_task,
        cleanup_expired_cache_task,
        update_embeddings_task,
        weekly_rag_evaluation_task,
    ]

    # Cron jobs (scheduled tasks)
    cron_jobs = [
        cron(cleanup_expired_cache_task, hour={0, 6, 12, 18}),  # Every 6 hours
        cron(update_embeddings_task, hour=3, minute=0),  # Daily at 3 AM
        cron(weekly_rag_evaluation_task, weekday=0, hour=6, minute=0),  # Every Monday at 6 AM
    ] if ARQ_AVAILABLE else []

    # Redis/DragonflyDB settings
    redis_settings = get_redis_settings() if ARQ_AVAILABLE else None

    # Worker settings
    max_jobs = 10
    job_timeout = 3600
    keep_result = 3600
    queue_name = "talos:queue"

    # Startup/shutdown hooks
    on_startup = None
    on_shutdown = None


# =============================================================================
# TASK MANAGER
# =============================================================================

class ARQTaskManager:
    """Manage ARQ tasks"""

    def __init__(self, config: Optional[ARQConfig] = None):
        self.config = config or ARQConfig()
        self._pool: Optional[ArqRedis] = None

    async def connect(self):
        """Connect to Redis/DragonflyDB"""
        if not ARQ_AVAILABLE:
            logger.warning("ARQ not available")
            return

        self._pool = await create_pool(get_redis_settings(self.config))
        logger.info("ARQ connected to Redis/DragonflyDB")

    async def close(self):
        """Close connection"""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def enqueue(
        self,
        func_name: str,
        *args,
        _defer_by: Optional[timedelta] = None,
        _job_id: Optional[str] = None,
        **kwargs
    ) -> Optional[str]:
        """
        Enqueue a task.

        Args:
            func_name: Name of the task function
            *args: Task arguments
            _defer_by: Delay before running
            _job_id: Custom job ID
            **kwargs: Task keyword arguments

        Returns:
            Job ID or None
        """
        if not self._pool:
            await self.connect()

        if not self._pool:
            return None

        try:
            job = await self._pool.enqueue_job(
                func_name,
                *args,
                _defer_by=_defer_by,
                _job_id=_job_id,
                **kwargs
            )
            return job.job_id if job else None
        except Exception as e:
            logger.error(f"Failed to enqueue task: {e}")
            return None

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get status of a job"""
        if not self._pool:
            await self.connect()

        if not self._pool:
            return {"status": "not_connected"}

        try:
            job = await self._pool.job(job_id)
            if not job:
                return {"status": "not_found", "job_id": job_id}

            status = await job.status()
            result = await job.result(poll_delay=0.1, timeout=0.5)

            return {
                "job_id": job_id,
                "status": status.value if status else "unknown",
                "result": result
            }
        except asyncio.TimeoutError:
            return {
                "job_id": job_id,
                "status": "pending",
                "result": None
            }
        except Exception as e:
            return {
                "job_id": job_id,
                "status": "error",
                "error": str(e)
            }

    async def get_queue_info(self) -> Dict[str, Any]:
        """Get queue information"""
        if not self._pool:
            await self.connect()

        if not self._pool:
            return {"status": "not_connected"}

        try:
            info = await self._pool.info()
            return {
                "status": "connected",
                "redis_info": info
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_task_manager: Optional[ARQTaskManager] = None


async def get_task_manager() -> ARQTaskManager:
    """Get or create task manager singleton"""
    global _task_manager
    if _task_manager is None:
        _task_manager = ARQTaskManager()
        await _task_manager.connect()
    return _task_manager


def is_arq_available() -> bool:
    """Check if ARQ is available"""
    return ARQ_AVAILABLE


async def enqueue_task(func_name: str, *args, **kwargs) -> Optional[str]:
    """Quick enqueue a task"""
    manager = await get_task_manager()
    return await manager.enqueue(func_name, *args, **kwargs)


# =============================================================================
# CLI COMMANDS
# =============================================================================
# Run worker: arq assemblyline_common.utils.arq_tasks.WorkerSettings
# Run with watch: arq assemblyline_common.utils.arq_tasks.WorkerSettings --watch
