"""
Predictive Caching System

Learns user behavior patterns and pre-computes likely queries
to reduce response latency.

Features:
- Time-based patterns (user queries X at Y time)
- Query sequence patterns (after query X, user often asks Y)
- Pre-computed cache with automatic expiration
- Cache hit rate tracking
"""

import asyncio
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import json
import hashlib

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

class PatternType(str, Enum):
    """Types of learned behavior patterns"""
    TIME_BASED = "time_based"          # User queries at specific times
    SEQUENCE = "sequence"              # Query A often followed by query B
    TOPIC_BASED = "topic_based"        # Related queries on same topic
    CALENDAR_TRIGGER = "calendar_trigger"  # Queries before calendar events


@dataclass
class BehaviorPattern:
    """A learned user behavior pattern"""
    id: Optional[int]
    user_id: int
    pattern_type: PatternType
    pattern_definition: Dict[str, Any]
    confidence: float = 0.5
    hit_count: int = 1
    correct_predictions: int = 0
    last_triggered_at: Optional[datetime] = None


@dataclass
class CacheEntry:
    """A cached prediction entry"""
    id: Optional[int]
    user_id: int
    cache_key: str
    predicted_query: str
    cache_value: Dict[str, Any]
    pattern_id: Optional[int]
    prediction_confidence: float
    expires_at: datetime
    was_hit: bool = False


@dataclass
class CacheStats:
    """Cache performance statistics"""
    total_queries: int
    cache_hits: int
    cache_misses: int
    hit_rate: float
    time_saved_ms: int
    pattern_accuracy: float


# =============================================================================
# PATTERN LEARNER
# =============================================================================

class PatternLearner:
    """Learns user behavior patterns from query history"""

    def __init__(self, db_pool=None):
        self.db_pool = db_pool
        self._min_observations = 3  # Min observations before creating pattern
        self._sequence_window_minutes = 5  # Max time between queries for sequence

    async def initialize(self, db_pool=None):
        """Initialize with database pool"""
        if db_pool:
            self.db_pool = db_pool
        logger.info("PatternLearner initialized")

    async def record_query(
        self,
        user_id: int,
        query: str,
        query_type: str = "rag_search",
        response_time_ms: int = 0,
        query_embedding: Optional[List[float]] = None
    ) -> None:
        """Record a query for pattern learning"""
        if not self.db_pool:
            return

        try:
            now = datetime.now()
            async with self.db_pool.acquire() as conn:
                # Get previous query for sequence learning
                prev_query = await conn.fetchrow("""
                    SELECT id FROM query_history
                    WHERE user_id = $1
                    AND created_at > NOW() - INTERVAL '%s minutes'
                    ORDER BY created_at DESC
                    LIMIT 1
                """ % self._sequence_window_minutes, user_id)

                prev_query_id = prev_query['id'] if prev_query else None

                # Insert query history
                # Convert embedding list to string for pgvector
                embedding_str = str(query_embedding) if query_embedding else None
                await conn.execute("""
                    INSERT INTO query_history
                    (user_id, query, query_embedding, query_type, response_time_ms,
                     day_of_week, hour_of_day, previous_query_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                    user_id, query,
                    embedding_str,
                    query_type, response_time_ms,
                    now.weekday(), now.hour,
                    prev_query_id
                )

                # Trigger pattern learning asynchronously
                asyncio.create_task(self._learn_patterns(user_id))

        except Exception as e:
            logger.warning(f"Failed to record query: {e}")

    async def _learn_patterns(self, user_id: int) -> None:
        """Learn patterns from recent query history"""
        try:
            await self._learn_time_patterns(user_id)
            await self._learn_sequence_patterns(user_id)
        except Exception as e:
            logger.warning(f"Pattern learning failed: {e}")

    async def _learn_time_patterns(self, user_id: int) -> None:
        """Learn time-based patterns (user queries X at Y time)"""
        if not self.db_pool:
            return

        async with self.db_pool.acquire() as conn:
            # Find recurring queries at specific times
            patterns = await conn.fetch("""
                SELECT
                    day_of_week, hour_of_day, query,
                    COUNT(*) as occurrence_count
                FROM query_history
                WHERE user_id = $1
                AND created_at > NOW() - INTERVAL '30 days'
                GROUP BY day_of_week, hour_of_day, query
                HAVING COUNT(*) >= $2
                ORDER BY occurrence_count DESC
                LIMIT 20
            """, user_id, self._min_observations)

            for pattern in patterns:
                pattern_def = {
                    "day_of_week": pattern['day_of_week'],
                    "hour": pattern['hour_of_day'],
                    "typical_query": pattern['query']
                }

                # Check if pattern already exists
                existing = await conn.fetchrow("""
                    SELECT id, hit_count FROM user_behavior_patterns
                    WHERE user_id = $1
                    AND pattern_type = 'time_based'
                    AND pattern_definition->>'day_of_week' = $2
                    AND pattern_definition->>'hour' = $3
                """, user_id, str(pattern['day_of_week']), str(pattern['hour_of_day']))

                if existing:
                    # Update existing pattern
                    await conn.execute("""
                        UPDATE user_behavior_patterns
                        SET hit_count = $1, updated_at = NOW()
                        WHERE id = $2
                    """, pattern['occurrence_count'], existing['id'])
                else:
                    # Create new pattern
                    await conn.execute("""
                        INSERT INTO user_behavior_patterns
                        (user_id, pattern_type, pattern_definition, hit_count)
                        VALUES ($1, 'time_based', $2, $3)
                    """, user_id, json.dumps(pattern_def), pattern['occurrence_count'])

    async def _learn_sequence_patterns(self, user_id: int) -> None:
        """Learn query sequence patterns (A followed by B)"""
        if not self.db_pool:
            return

        async with self.db_pool.acquire() as conn:
            # Find common query sequences
            sequences = await conn.fetch("""
                SELECT
                    q1.query as trigger_query,
                    q2.query as follow_up_query,
                    COUNT(*) as sequence_count
                FROM query_history q1
                JOIN query_history q2 ON q2.previous_query_id = q1.id
                WHERE q1.user_id = $1
                AND q1.created_at > NOW() - INTERVAL '30 days'
                GROUP BY q1.query, q2.query
                HAVING COUNT(*) >= $2
                ORDER BY sequence_count DESC
                LIMIT 20
            """, user_id, self._min_observations)

            for seq in sequences:
                pattern_def = {
                    "trigger_query": seq['trigger_query'],
                    "follow_up_query": seq['follow_up_query'],
                    "sequence_count": seq['sequence_count']
                }

                # Check if pattern exists
                existing = await conn.fetchrow("""
                    SELECT id FROM user_behavior_patterns
                    WHERE user_id = $1
                    AND pattern_type = 'sequence'
                    AND pattern_definition->>'trigger_query' = $2
                    AND pattern_definition->>'follow_up_query' = $3
                """, user_id, seq['trigger_query'], seq['follow_up_query'])

                if existing:
                    await conn.execute("""
                        UPDATE user_behavior_patterns
                        SET hit_count = $1, updated_at = NOW()
                        WHERE id = $2
                    """, seq['sequence_count'], existing['id'])
                else:
                    await conn.execute("""
                        INSERT INTO user_behavior_patterns
                        (user_id, pattern_type, pattern_definition, hit_count)
                        VALUES ($1, 'sequence', $2, $3)
                    """, user_id, json.dumps(pattern_def), seq['sequence_count'])

    async def get_patterns(self, user_id: int) -> List[BehaviorPattern]:
        """Get all learned patterns for a user"""
        if not self.db_pool:
            return []

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, user_id, pattern_type, pattern_definition,
                           confidence, hit_count, correct_predictions, last_triggered_at
                    FROM user_behavior_patterns
                    WHERE user_id = $1
                    ORDER BY confidence DESC
                """, user_id)

                return [
                    BehaviorPattern(
                        id=row['id'],
                        user_id=row['user_id'],
                        pattern_type=PatternType(row['pattern_type']),
                        pattern_definition=row['pattern_definition'],
                        confidence=row['confidence'],
                        hit_count=row['hit_count'],
                        correct_predictions=row['correct_predictions'],
                        last_triggered_at=row['last_triggered_at']
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Error fetching patterns: {e}")
            return []


# =============================================================================
# PREDICTIVE CACHE
# =============================================================================

class PredictiveCache:
    """Predictive caching system for pre-computed query results"""

    def __init__(self, db_pool=None, pattern_learner: Optional[PatternLearner] = None):
        self.db_pool = db_pool
        self.pattern_learner = pattern_learner
        self._default_ttl_hours = 24
        self._min_confidence_threshold = 0.3

    async def initialize(self, db_pool=None):
        """Initialize with database pool"""
        if db_pool:
            self.db_pool = db_pool
            if self.pattern_learner:
                await self.pattern_learner.initialize(db_pool)
        logger.info("PredictiveCache initialized")

    def _generate_cache_key(self, query: str, user_id: int) -> str:
        """Generate unique cache key for a query"""
        content = f"{user_id}:{query.lower().strip()}"
        return hashlib.md5(content.encode()).hexdigest()

    async def check_cache(
        self,
        query: str,
        user_id: int
    ) -> Optional[Dict[str, Any]]:
        """Check if query result is in cache"""
        if not self.db_pool:
            return None

        cache_key = self._generate_cache_key(query, user_id)

        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT id, cache_value, prediction_confidence
                    FROM predictive_cache
                    WHERE user_id = $1
                    AND cache_key = $2
                    AND expires_at > NOW()
                """, user_id, cache_key)

                if row:
                    # Record cache hit
                    await conn.execute("""
                        UPDATE predictive_cache
                        SET was_hit = TRUE, hit_at = NOW()
                        WHERE id = $1
                    """, row['id'])

                    # Update daily stats
                    await self._record_cache_hit(conn, user_id)

                    logger.info(f"Cache HIT for query: {query[:50]}...")
                    return {
                        "cached": True,
                        "confidence": row['prediction_confidence'],
                        "result": row['cache_value']
                    }

                # Record cache miss
                await self._record_cache_miss(conn, user_id)
                return None

        except Exception as e:
            logger.warning(f"Cache check failed: {e}")
            return None

    async def predict_and_cache(
        self,
        user_id: int,
        current_query: str,
        rag_search_func
    ) -> None:
        """Predict next queries and pre-cache results"""
        if not self.db_pool or not self.pattern_learner:
            return

        try:
            predictions = await self._get_predictions(user_id, current_query)

            for pred in predictions:
                if pred['confidence'] < self._min_confidence_threshold:
                    continue

                # Pre-compute RAG search for predicted query
                try:
                    result = await rag_search_func(pred['query'], user_id)

                    # Cache the result
                    await self._store_cache(
                        user_id=user_id,
                        query=pred['query'],
                        result=result,
                        pattern_id=pred.get('pattern_id'),
                        confidence=pred['confidence']
                    )

                    logger.info(f"Pre-cached query: {pred['query'][:50]}... (confidence: {pred['confidence']:.2f})")

                except Exception as e:
                    logger.debug(f"Failed to pre-cache query: {e}")

        except Exception as e:
            logger.warning(f"Prediction failed: {e}")

    async def _get_predictions(
        self,
        user_id: int,
        current_query: str
    ) -> List[Dict[str, Any]]:
        """Get predicted next queries based on patterns"""
        predictions = []

        if not self.db_pool:
            return predictions

        try:
            now = datetime.now()
            async with self.db_pool.acquire() as conn:
                # Check sequence patterns (what comes after current query)
                sequence_preds = await conn.fetch("""
                    SELECT id, pattern_definition, confidence
                    FROM user_behavior_patterns
                    WHERE user_id = $1
                    AND pattern_type = 'sequence'
                    AND pattern_definition->>'trigger_query' ILIKE $2
                    AND confidence >= $3
                    ORDER BY confidence DESC
                    LIMIT 3
                """, user_id, f"%{current_query[:50]}%", self._min_confidence_threshold)

                for row in sequence_preds:
                    predictions.append({
                        "query": row['pattern_definition']['follow_up_query'],
                        "confidence": row['confidence'],
                        "pattern_id": row['id'],
                        "reason": "sequence_pattern"
                    })

                # Check time-based patterns (what's typically asked at this time)
                time_preds = await conn.fetch("""
                    SELECT id, pattern_definition, confidence
                    FROM user_behavior_patterns
                    WHERE user_id = $1
                    AND pattern_type = 'time_based'
                    AND (pattern_definition->>'day_of_week')::int = $2
                    AND ABS((pattern_definition->>'hour')::int - $3) <= 1
                    AND confidence >= $4
                    ORDER BY confidence DESC
                    LIMIT 3
                """, user_id, now.weekday(), now.hour, self._min_confidence_threshold)

                for row in time_preds:
                    query = row['pattern_definition'].get('typical_query', '')
                    if query and query.lower() != current_query.lower():
                        predictions.append({
                            "query": query,
                            "confidence": row['confidence'],
                            "pattern_id": row['id'],
                            "reason": "time_pattern"
                        })

        except Exception as e:
            logger.warning(f"Failed to get predictions: {e}")

        return predictions

    async def _store_cache(
        self,
        user_id: int,
        query: str,
        result: Dict[str, Any],
        pattern_id: Optional[int],
        confidence: float
    ) -> None:
        """Store result in predictive cache"""
        if not self.db_pool:
            return

        cache_key = self._generate_cache_key(query, user_id)
        expires_at = datetime.now() + timedelta(hours=self._default_ttl_hours)

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO predictive_cache
                    (user_id, cache_key, predicted_query, cache_value,
                     pattern_id, prediction_confidence, expires_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (user_id, cache_key) DO UPDATE
                    SET cache_value = $4, prediction_confidence = $6, expires_at = $7
                """,
                    user_id, cache_key, query, json.dumps(result),
                    pattern_id, confidence, expires_at
                )

        except Exception as e:
            logger.warning(f"Failed to store cache: {e}")

    async def _record_cache_hit(self, conn, user_id: int) -> None:
        """Record cache hit in daily stats"""
        try:
            await conn.execute("""
                INSERT INTO predictive_cache_stats (user_id, date, cache_hits, total_queries)
                VALUES ($1, CURRENT_DATE, 1, 1)
                ON CONFLICT (user_id, date) DO UPDATE
                SET cache_hits = predictive_cache_stats.cache_hits + 1,
                    total_queries = predictive_cache_stats.total_queries + 1,
                    updated_at = NOW()
            """, user_id)
        except Exception:
            pass

    async def _record_cache_miss(self, conn, user_id: int) -> None:
        """Record cache miss in daily stats"""
        try:
            await conn.execute("""
                INSERT INTO predictive_cache_stats (user_id, date, cache_misses, total_queries)
                VALUES ($1, CURRENT_DATE, 1, 1)
                ON CONFLICT (user_id, date) DO UPDATE
                SET cache_misses = predictive_cache_stats.cache_misses + 1,
                    total_queries = predictive_cache_stats.total_queries + 1,
                    updated_at = NOW()
            """, user_id)
        except Exception:
            pass

    async def get_stats(self, user_id: int, days: int = 7) -> CacheStats:
        """Get cache performance statistics"""
        if not self.db_pool:
            return CacheStats(0, 0, 0, 0.0, 0, 0.0)

        try:
            async with self.db_pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT
                        COALESCE(SUM(total_queries), 0) as total_queries,
                        COALESCE(SUM(cache_hits), 0) as cache_hits,
                        COALESCE(SUM(cache_misses), 0) as cache_misses,
                        COALESCE(SUM(time_saved_ms), 0) as time_saved_ms
                    FROM predictive_cache_stats
                    WHERE user_id = $1
                    AND date > CURRENT_DATE - INTERVAL '1 day' * $2
                """, user_id, days)

                total = row['total_queries'] or 0
                hits = row['cache_hits'] or 0
                misses = row['cache_misses'] or 0

                # Get pattern accuracy
                pattern_row = await conn.fetchrow("""
                    SELECT
                        COALESCE(SUM(hit_count), 0) as total_predictions,
                        COALESCE(SUM(correct_predictions), 0) as correct
                    FROM user_behavior_patterns
                    WHERE user_id = $1
                """, user_id)

                pattern_total = pattern_row['total_predictions'] or 0
                pattern_correct = pattern_row['correct'] or 0
                pattern_accuracy = pattern_correct / max(pattern_total, 1)

                return CacheStats(
                    total_queries=total,
                    cache_hits=hits,
                    cache_misses=misses,
                    hit_rate=hits / max(total, 1),
                    time_saved_ms=row['time_saved_ms'] or 0,
                    pattern_accuracy=pattern_accuracy
                )

        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return CacheStats(0, 0, 0, 0.0, 0, 0.0)

    async def cleanup_expired(self) -> int:
        """Clean up expired cache entries"""
        if not self.db_pool:
            return 0

        try:
            async with self.db_pool.acquire() as conn:
                result = await conn.execute("""
                    DELETE FROM predictive_cache
                    WHERE expires_at < NOW()
                    AND was_hit = FALSE
                """)
                count = int(result.split()[-1])
                if count > 0:
                    logger.info(f"Cleaned up {count} expired cache entries")
                return count

        except Exception as e:
            logger.warning(f"Cache cleanup failed: {e}")
            return 0


# =============================================================================
# MODULE-LEVEL FUNCTIONS
# =============================================================================

_pattern_learner: Optional[PatternLearner] = None
_predictive_cache: Optional[PredictiveCache] = None


async def get_pattern_learner(db_pool=None) -> PatternLearner:
    """Get or create the pattern learner singleton"""
    global _pattern_learner

    if _pattern_learner is None:
        _pattern_learner = PatternLearner(db_pool=db_pool)
        if db_pool:
            await _pattern_learner.initialize(db_pool)
    elif db_pool:
        await _pattern_learner.initialize(db_pool)

    return _pattern_learner


async def get_predictive_cache(db_pool=None) -> PredictiveCache:
    """Get or create the predictive cache singleton"""
    global _predictive_cache

    if _predictive_cache is None:
        learner = await get_pattern_learner(db_pool)
        _predictive_cache = PredictiveCache(db_pool=db_pool, pattern_learner=learner)
        if db_pool:
            await _predictive_cache.initialize(db_pool)
    elif db_pool:
        await _predictive_cache.initialize(db_pool)

    return _predictive_cache
