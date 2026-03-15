"""
Continuous RAGAS Evaluation Runner

Automatically runs RAG evaluation metrics on a random sample of queries
at scheduled intervals, storing results for trend analysis.

Features:
- APScheduler-based continuous evaluation
- Random sampling from recent queries
- Stores results in common.rag_eval_results
- Configurable schedule interval
"""

import os
import json
import logging
import asyncio
import random
from datetime import datetime
from typing import Optional, Dict, Any, List

logger = logging.getLogger(__name__)

# Try to import APScheduler
try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.interval import IntervalTrigger
    SCHEDULER_AVAILABLE = True
except ImportError:
    SCHEDULER_AVAILABLE = False
    logger.info("APScheduler not available — continuous eval disabled")

# Try to import database
try:
    from assemblyline_common.database import get_connection
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False


class ContinuousEvalRunner:
    """
    Runs RAGAS evaluation metrics on a scheduled basis.

    Samples recent queries, evaluates retrieval + generation quality,
    and stores results for dashboards and trend analysis.
    """

    def __init__(
        self,
        interval_hours: int = 6,
        sample_size: int = 10,
        enabled: bool = True,
    ):
        """
        Initialize the eval runner.

        Args:
            interval_hours: Hours between evaluation runs
            sample_size: Number of queries to sample per run
            enabled: Whether to enable scheduled evaluation
        """
        self.interval_hours = int(os.getenv("EVAL_INTERVAL_HOURS", str(interval_hours)))
        self.sample_size = int(os.getenv("EVAL_SAMPLE_SIZE", str(sample_size)))
        self.enabled = enabled and SCHEDULER_AVAILABLE and DB_AVAILABLE
        self._scheduler: Optional['AsyncIOScheduler'] = None
        self._running = False

    async def start(self):
        """Start the scheduled evaluation runner"""
        if not self.enabled:
            logger.info("Continuous eval runner disabled (missing dependencies or config)")
            return

        if self._running:
            return

        try:
            # Ensure results table exists
            await self._ensure_table()

            self._scheduler = AsyncIOScheduler()
            self._scheduler.add_job(
                self._run_evaluation,
                trigger=IntervalTrigger(hours=self.interval_hours),
                id="ragas_eval",
                name="RAGAS Continuous Evaluation",
                replace_existing=True,
            )
            self._scheduler.start()
            self._running = True
            logger.info(
                f"Continuous eval runner started (every {self.interval_hours}h, "
                f"sample_size={self.sample_size})"
            )
        except Exception as e:
            logger.error(f"Failed to start eval runner: {e}")

    async def stop(self):
        """Stop the scheduled runner"""
        if self._scheduler and self._running:
            self._scheduler.shutdown(wait=False)
            self._running = False
            logger.info("Continuous eval runner stopped")

    async def _ensure_table(self):
        """Create the results table if it doesn't exist"""
        if not DB_AVAILABLE:
            return

        try:
            async with get_connection() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS common.rag_eval_results (
                        id SERIAL PRIMARY KEY,
                        run_id VARCHAR(64) NOT NULL,
                        run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        sample_size INTEGER NOT NULL,
                        avg_faithfulness FLOAT,
                        avg_answer_relevancy FLOAT,
                        avg_context_relevancy FLOAT,
                        avg_context_precision FLOAT,
                        avg_retrieval_score FLOAT,
                        hallucination_rate FLOAT,
                        individual_results JSONB DEFAULT '[]',
                        metadata JSONB DEFAULT '{}',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                # Index for time-series queries
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_rag_eval_results_timestamp
                    ON common.rag_eval_results (run_timestamp DESC)
                """)
        except Exception as e:
            logger.error(f"Failed to create eval results table: {e}")

    async def _run_evaluation(self):
        """Execute a single evaluation run"""
        run_id = f"eval_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting evaluation run: {run_id}")

        try:
            # Sample recent queries from the database
            queries = await self._sample_recent_queries()
            if not queries:
                logger.info("No recent queries to evaluate")
                return

            # Evaluate each query
            individual_results = []
            metrics_sum = {
                "faithfulness": 0.0,
                "answer_relevancy": 0.0,
                "context_relevancy": 0.0,
                "context_precision": 0.0,
                "retrieval_score": 0.0,
            }

            for query_data in queries:
                result = await self._evaluate_single(query_data)
                if result:
                    individual_results.append(result)
                    for key in metrics_sum:
                        metrics_sum[key] += result.get(key, 0.0)

            count = len(individual_results) or 1

            # Calculate averages
            avg_metrics = {k: v / count for k, v in metrics_sum.items()}

            # Calculate hallucination rate
            hallucination_count = sum(
                1 for r in individual_results
                if r.get("hallucination_detected", False)
            )
            hallucination_rate = hallucination_count / count

            # Store results
            await self._store_results(
                run_id=run_id,
                sample_size=len(individual_results),
                avg_metrics=avg_metrics,
                hallucination_rate=hallucination_rate,
                individual_results=individual_results,
            )

            logger.info(
                f"Evaluation run {run_id} complete: {len(individual_results)} queries, "
                f"faithfulness={avg_metrics['faithfulness']:.3f}, "
                f"relevancy={avg_metrics['answer_relevancy']:.3f}, "
                f"hallucination_rate={hallucination_rate:.3f}"
            )

        except Exception as e:
            logger.error(f"Evaluation run {run_id} failed: {e}")

    async def _sample_recent_queries(self) -> List[Dict[str, Any]]:
        """Sample recent queries from chat history"""
        if not DB_AVAILABLE:
            return []

        try:
            async with get_connection() as conn:
                # Get recent queries that have RAG context
                rows = await conn.fetch("""
                    SELECT
                        m.content as query,
                        m.metadata->>'rag_context' as rag_context,
                        m.metadata->>'rag_sources' as rag_sources,
                        r.content as response
                    FROM common.messages m
                    LEFT JOIN common.messages r ON r.conversation_id = m.conversation_id
                        AND r.role = 'assistant'
                        AND r.created_at > m.created_at
                    WHERE m.role = 'user'
                        AND m.metadata->>'rag_context' IS NOT NULL
                    ORDER BY m.created_at DESC
                    LIMIT 100
                """)

                if not rows:
                    return []

                # Random sample
                sample = random.sample(
                    [dict(r) for r in rows],
                    min(self.sample_size, len(rows))
                )
                return sample

        except Exception as e:
            logger.warning(f"Failed to sample queries: {e}")
            return []

    async def _evaluate_single(self, query_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluate a single query-response pair"""
        try:
            query = query_data.get("query", "")
            context = query_data.get("rag_context", "")
            response = query_data.get("response", "")

            if not query or not response:
                return None

            # Simple heuristic evaluation (no LLM call needed)
            # Faithfulness: check if response terms appear in context
            response_words = set(response.lower().split())
            context_words = set(context.lower().split()) if context else set()
            overlap = len(response_words & context_words) / max(len(response_words), 1)
            faithfulness = min(overlap * 2, 1.0)  # Scale up, cap at 1.0

            # Answer relevancy: check if query terms appear in response
            query_words = set(query.lower().split())
            query_overlap = len(query_words & response_words) / max(len(query_words), 1)
            answer_relevancy = min(query_overlap * 1.5, 1.0)

            # Context relevancy: check if query terms appear in context
            context_overlap = len(query_words & context_words) / max(len(query_words), 1) if context_words else 0
            context_relevancy = min(context_overlap * 1.5, 1.0)

            # Context precision: how much of context is relevant
            context_precision = overlap

            # Retrieval score: combined metric
            retrieval_score = (faithfulness + context_relevancy) / 2

            # Hallucination detection: response claims not in context
            hallucination_detected = faithfulness < 0.3 and len(response) > 100

            return {
                "query": query[:200],
                "faithfulness": round(faithfulness, 4),
                "answer_relevancy": round(answer_relevancy, 4),
                "context_relevancy": round(context_relevancy, 4),
                "context_precision": round(context_precision, 4),
                "retrieval_score": round(retrieval_score, 4),
                "hallucination_detected": hallucination_detected,
            }

        except Exception as e:
            logger.warning(f"Failed to evaluate query: {e}")
            return None

    async def _store_results(
        self,
        run_id: str,
        sample_size: int,
        avg_metrics: Dict[str, float],
        hallucination_rate: float,
        individual_results: List[Dict[str, Any]],
    ):
        """Store evaluation results in the database"""
        if not DB_AVAILABLE:
            return

        try:
            async with get_connection() as conn:
                await conn.execute("""
                    INSERT INTO common.rag_eval_results (
                        run_id, run_timestamp, sample_size,
                        avg_faithfulness, avg_answer_relevancy,
                        avg_context_relevancy, avg_context_precision,
                        avg_retrieval_score, hallucination_rate,
                        individual_results
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                    run_id,
                    datetime.utcnow(),
                    sample_size,
                    avg_metrics["faithfulness"],
                    avg_metrics["answer_relevancy"],
                    avg_metrics["context_relevancy"],
                    avg_metrics["context_precision"],
                    avg_metrics["retrieval_score"],
                    hallucination_rate,
                    json.dumps(individual_results),
                )
        except Exception as e:
            logger.error(f"Failed to store eval results: {e}")

    async def get_recent_results(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent evaluation results"""
        if not DB_AVAILABLE:
            return []

        try:
            async with get_connection() as conn:
                rows = await conn.fetch("""
                    SELECT run_id, run_timestamp, sample_size,
                           avg_faithfulness, avg_answer_relevancy,
                           avg_context_relevancy, avg_retrieval_score,
                           hallucination_rate
                    FROM common.rag_eval_results
                    ORDER BY run_timestamp DESC
                    LIMIT $1
                """, limit)
                return [dict(r) for r in rows]
        except Exception as e:
            logger.warning(f"Failed to get eval results: {e}")
            return []

    def get_info(self) -> dict:
        """Get runner info"""
        return {
            "enabled": self.enabled,
            "scheduler_available": SCHEDULER_AVAILABLE,
            "running": self._running,
            "interval_hours": self.interval_hours,
            "sample_size": self.sample_size,
        }


# Singleton
_eval_runner: Optional[ContinuousEvalRunner] = None


def get_eval_runner() -> ContinuousEvalRunner:
    """Get or create singleton eval runner"""
    global _eval_runner
    if _eval_runner is None:
        _eval_runner = ContinuousEvalRunner()
    return _eval_runner
