"""
T.A.L.O.S. Celery Task Queue
Based on Memobytes patterns

Features:
- Distributed background processing
- Document processing tasks
- AI generation tasks
- Scheduled tasks
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

# =============================================================================
# CELERY AVAILABILITY CHECK
# =============================================================================

CELERY_AVAILABLE = False

try:
    from celery import Celery, Task
    from celery.result import AsyncResult
    CELERY_AVAILABLE = True
    logger.info("Celery library loaded successfully")
except ImportError:
    logger.warning("Celery not installed. Run: pip install celery[redis]")


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")


# =============================================================================
# CELERY APP
# =============================================================================

if CELERY_AVAILABLE:
    celery_app = Celery(
        "talos",
        broker=REDIS_URL,
        backend=RESULT_BACKEND,
        include=[
            "assemblyline_common.utils.celery_tasks"
        ]
    )

    # Configuration
    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_time_limit=3600,  # 1 hour max
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
    )

    # Task routes
    celery_app.conf.task_routes = {
        "talos.document.*": {"queue": "documents"},
        "talos.ai.*": {"queue": "ai"},
        "talos.email.*": {"queue": "email"},
    }

    # Scheduled tasks (beat)
    celery_app.conf.beat_schedule = {
        "cleanup-expired-cache": {
            "task": "talos.cleanup.expired_cache",
            "schedule": 3600.0,  # Every hour
        },
        "update-embeddings": {
            "task": "talos.ai.update_embeddings",
            "schedule": 86400.0,  # Daily
        },
    }
else:
    celery_app = None


# =============================================================================
# BASE TASK CLASS
# =============================================================================

if CELERY_AVAILABLE:
    class TALOSTask(Task):
        """Base task class with error handling and logging"""

        def on_success(self, retval, task_id, args, kwargs):
            logger.info(f"Task {task_id} completed successfully")

        def on_failure(self, exc, task_id, args, kwargs, einfo):
            logger.error(f"Task {task_id} failed: {exc}")

        def on_retry(self, exc, task_id, args, kwargs, einfo):
            logger.warning(f"Task {task_id} retrying: {exc}")
else:
    TALOSTask = None


# =============================================================================
# DOCUMENT PROCESSING TASKS
# =============================================================================

if CELERY_AVAILABLE:
    @celery_app.task(base=TALOSTask, bind=True, name="talos.document.process")
    def process_document_task(
        self,
        document_id: int,
        file_path: str,
        options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """
        Process a document asynchronously.

        Args:
            document_id: Document ID in database
            file_path: Path to the file
            options: Processing options

        Returns:
            Processing result
        """
        options = options or {}
        start_time = datetime.utcnow()

        try:
            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.1, "stage": "reading"}
            )

            # Import here to avoid circular imports
            from services.documents.app.utils.docling_processor import (
                get_docling_processor
            )

            processor = get_docling_processor()

            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.3, "stage": "extracting"}
            )

            # Process document
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            result = loop.run_until_complete(
                processor.process_document(file_path)
            )

            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.7, "stage": "chunking"}
            )

            # Get chunks
            chunks = result.to_chunks(
                chunk_size=options.get("chunk_size", 500),
                overlap=options.get("overlap", 50)
            )

            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.9, "stage": "indexing"}
            )

            # Index in RAG service
            import httpx
            rag_url = os.getenv("RAG_SERVICE_URL", "http://localhost:8002")

            response = httpx.post(
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

    @celery_app.task(base=TALOSTask, bind=True, name="talos.document.batch_process")
    def batch_process_documents_task(
        self,
        document_ids: List[int],
        options: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Process multiple documents in batch"""
        results = []
        total = len(document_ids)

        for i, doc_id in enumerate(document_ids):
            self.update_state(
                state="PROCESSING",
                meta={
                    "progress": i / total,
                    "current": doc_id,
                    "completed": i,
                    "total": total
                }
            )

            # Queue individual task
            result = process_document_task.delay(doc_id, f"/uploads/{doc_id}", options)
            results.append({"document_id": doc_id, "task_id": result.id})

        return {
            "status": "queued",
            "documents": results,
            "total": total
        }


# =============================================================================
# AI GENERATION TASKS
# =============================================================================

if CELERY_AVAILABLE:
    @celery_app.task(base=TALOSTask, bind=True, name="talos.ai.generate_embeddings")
    def generate_embeddings_task(
        self,
        texts: List[str],
        model: str = "all-MiniLM-L6-v2"
    ) -> Dict[str, Any]:
        """
        Generate embeddings for texts.

        Args:
            texts: List of texts to embed
            model: Embedding model name

        Returns:
            Embeddings and metadata
        """
        try:
            from sentence_transformers import SentenceTransformer

            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.1, "stage": "loading_model"}
            )

            embedder = SentenceTransformer(model)

            self.update_state(
                state="PROCESSING",
                meta={"progress": 0.3, "stage": "generating"}
            )

            embeddings = embedder.encode(
                texts,
                normalize_embeddings=True,
                show_progress_bar=False
            ).tolist()

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

    @celery_app.task(base=TALOSTask, bind=True, name="talos.ai.update_embeddings")
    def update_embeddings_task(self) -> Dict[str, Any]:
        """Scheduled task to update stale embeddings"""
        try:
            # This would query for documents needing embedding updates
            logger.info("Running scheduled embedding update")
            return {"status": "completed", "updated": 0}
        except Exception as e:
            return {"status": "failed", "error": str(e)}

    @celery_app.task(base=TALOSTask, bind=True, name="talos.ai.generate_response")
    def generate_ai_response_task(
        self,
        query: str,
        context: str = None,
        model: str = None
    ) -> Dict[str, Any]:
        """
        Generate AI response asynchronously.

        Useful for long-running AI generations.
        """
        try:
            import httpx

            ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
            model = model or os.getenv("OLLAMA_MODEL", "gemma3:4b")

            prompt = query
            if context:
                prompt = f"Context:\n{context}\n\nQuestion: {query}\n\nAnswer:"

            response = httpx.post(
                f"{ollama_url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False
                },
                timeout=120.0
            )

            result = response.json()

            return {
                "status": "completed",
                "response": result.get("response", ""),
                "model": model,
                "query": query
            }

        except Exception as e:
            return {
                "status": "failed",
                "error": str(e)
            }


# =============================================================================
# CLEANUP TASKS
# =============================================================================

if CELERY_AVAILABLE:
    @celery_app.task(base=TALOSTask, name="talos.cleanup.expired_cache")
    def cleanup_expired_cache_task() -> Dict[str, Any]:
        """Clean up expired cache entries"""
        try:
            import redis

            client = redis.Redis(host="localhost", port=6379)

            # Get all databases
            cleaned = 0
            for db in range(5):
                client.select(db)
                # Redis handles TTL automatically, but we can force cleanup
                # of any orphaned keys here

            logger.info(f"Cache cleanup completed: {cleaned} keys removed")
            return {"status": "completed", "cleaned": cleaned}

        except Exception as e:
            return {"status": "failed", "error": str(e)}


# =============================================================================
# TASK MANAGEMENT
# =============================================================================

class TaskManager:
    """Manage Celery tasks"""

    @staticmethod
    def get_task_status(task_id: str) -> Dict[str, Any]:
        """Get status of a task"""
        if not CELERY_AVAILABLE:
            return {"status": "celery_not_available"}

        result = AsyncResult(task_id, app=celery_app)

        return {
            "task_id": task_id,
            "status": result.status,
            "result": result.result if result.ready() else None,
            "info": result.info if result.status == "PROCESSING" else None
        }

    @staticmethod
    def revoke_task(task_id: str, terminate: bool = False) -> bool:
        """Revoke/cancel a task"""
        if not CELERY_AVAILABLE:
            return False

        try:
            celery_app.control.revoke(task_id, terminate=terminate)
            return True
        except Exception as e:
            logger.error(f"Failed to revoke task: {e}")
            return False

    @staticmethod
    def get_active_tasks() -> List[Dict[str, Any]]:
        """Get list of active tasks"""
        if not CELERY_AVAILABLE:
            return []

        try:
            inspect = celery_app.control.inspect()
            active = inspect.active() or {}

            tasks = []
            for worker, worker_tasks in active.items():
                for task in worker_tasks:
                    tasks.append({
                        "id": task["id"],
                        "name": task["name"],
                        "worker": worker,
                        "args": task.get("args"),
                        "started": task.get("time_start")
                    })
            return tasks

        except Exception as e:
            logger.error(f"Failed to get active tasks: {e}")
            return []

    @staticmethod
    def get_queue_length(queue_name: str = "celery") -> int:
        """Get number of tasks in queue"""
        if not CELERY_AVAILABLE:
            return 0

        try:
            import redis
            client = redis.Redis(host="localhost", port=6379)
            return client.llen(queue_name)
        except Exception:
            return 0


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def is_celery_available() -> bool:
    """Check if Celery is available"""
    return CELERY_AVAILABLE


def get_celery_app():
    """Get Celery app instance"""
    return celery_app


def get_task_manager() -> TaskManager:
    """Get task manager instance"""
    return TaskManager()


# =============================================================================
# CLI COMMANDS (for running workers)
# =============================================================================
# Run worker: celery -A assemblyline_common.utils.celery_tasks worker -l info
# Run beat: celery -A assemblyline_common.utils.celery_tasks beat -l info
# Run flower: celery -A assemblyline_common.utils.celery_tasks flower
