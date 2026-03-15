"""
ML Result Cache - TTL-Based In-Memory Cache for ML Results

Provides caching for ML/AI responses to reduce redundant API calls:
- SHA256-based cache key generation
- Configurable TTL per entry
- Thread-safe operations
- Automatic expiry cleanup
"""

import hashlib
import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Default TTLs (in seconds)
DEFAULT_SUGGESTION_TTL = 3600       # 1 hour for suggestions
DEFAULT_FIELD_MAPPING_TTL = 86400   # 24 hours for field mappings


def make_key(*parts: Any) -> str:
    """
    Generate a SHA256-based cache key from variable arguments.

    Args:
        *parts: Any hashable/serializable values to include in the key.

    Returns:
        A hex SHA256 digest string.
    """
    raw = "|".join(
        json.dumps(p, sort_keys=True, default=str) if not isinstance(p, str) else p
        for p in parts
    )
    return hashlib.sha256(raw.encode()).hexdigest()


@dataclass
class CacheEntry:
    """A single cache entry with value and expiration."""
    value: Any
    expires_at: float
    created_at: float = field(default_factory=time.time)

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expires_at


class MLResultCache:
    """
    TTL-based in-memory cache for ML/AI results.

    Usage:
        cache = MLResultCache()

        # Cache a mapping result for 24 hours
        key = make_key("auto-map", json_data, esl_source)
        cache.set(key, mapping_result, ttl=86400)

        # Retrieve
        result = cache.get(key)
        if result is not None:
            return result  # Cache hit

        # Otherwise compute and cache
        result = await compute_mapping(...)
        cache.set(key, result)
    """

    def __init__(self, default_ttl: int = DEFAULT_SUGGESTION_TTL, max_size: int = 1000):
        """
        Args:
            default_ttl: Default TTL in seconds when not specified per-entry.
            max_size: Maximum number of entries before eviction.
        """
        self._store: Dict[str, CacheEntry] = {}
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve a cached value by key.

        Returns None if key not found or expired.
        """
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return None
            if entry.is_expired:
                del self._store[key]
                self._misses += 1
                return None
            self._hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Store a value in the cache.

        Args:
            key: Cache key (use make_key() to generate).
            value: Value to cache.
            ttl: TTL in seconds (uses default if not specified).
        """
        effective_ttl = ttl if ttl is not None else self._default_ttl
        with self._lock:
            # Evict expired entries if at capacity
            if len(self._store) >= self._max_size:
                self._evict_expired()
            # If still at capacity, evict oldest
            if len(self._store) >= self._max_size:
                self._evict_oldest()

            self._store[key] = CacheEntry(
                value=value,
                expires_at=time.time() + effective_ttl,
            )

    def invalidate(self, key: str) -> bool:
        """Remove a specific key from cache. Returns True if key existed."""
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._store.clear()
            self._hits = 0
            self._misses = 0

    @property
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            return {
                "size": len(self._store),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total > 0 else 0.0,
                "default_ttl": self._default_ttl,
            }

    def _evict_expired(self) -> None:
        """Remove all expired entries (caller holds lock)."""
        now = time.time()
        expired_keys = [k for k, v in self._store.items() if now > v.expires_at]
        for k in expired_keys:
            del self._store[k]
        if expired_keys:
            logger.debug(f"Evicted {len(expired_keys)} expired cache entries")

    def _evict_oldest(self) -> None:
        """Remove the oldest entry (caller holds lock)."""
        if not self._store:
            return
        oldest_key = min(self._store, key=lambda k: self._store[k].created_at)
        del self._store[oldest_key]


# Singleton instances for different cache categories
_suggestion_cache: Optional[MLResultCache] = None
_mapping_cache: Optional[MLResultCache] = None


def get_suggestion_cache() -> MLResultCache:
    """Get singleton cache for ML suggestions (1hr TTL)."""
    global _suggestion_cache
    if _suggestion_cache is None:
        _suggestion_cache = MLResultCache(default_ttl=DEFAULT_SUGGESTION_TTL)
    return _suggestion_cache


def get_mapping_cache() -> MLResultCache:
    """Get singleton cache for field mappings (24hr TTL)."""
    global _mapping_cache
    if _mapping_cache is None:
        _mapping_cache = MLResultCache(default_ttl=DEFAULT_FIELD_MAPPING_TTL)
    return _mapping_cache


# =============================================================================
# Mapping Feedback Store
# =============================================================================

@dataclass
class FeedbackEntry:
    """A single feedback entry recording a user's confirmation or rejection."""
    input_key: str
    target: str
    confirmed: bool  # True = user confirmed mapping, False = user rejected
    created_at: float = field(default_factory=time.time)


class MappingFeedbackStore:
    """
    In-memory store for user feedback on ML mapping suggestions.

    Stores confirmed/rejected mappings per tenant + context key (e.g., ESL source
    or message type). Used to provide few-shot examples in future ML prompts.

    Usage:
        store = get_feedback_store()
        store.add_feedback("tenant1", "auto-map", "epic", "patient_name", "PID.5", True)
        confirmed = store.get_confirmed_mappings("tenant1", "auto-map", "epic")
    """

    def __init__(self, max_entries_per_key: int = 100):
        self._store: Dict[str, List[FeedbackEntry]] = {}
        self._lock = threading.Lock()
        self._max_entries = max_entries_per_key

    def _make_store_key(self, tenant_id: str, feature: str, context: str) -> str:
        """Create a composite key for the store."""
        return f"{tenant_id}|{feature}|{context}"

    def add_feedback(
        self,
        tenant_id: str,
        feature: str,
        context: str,
        input_key: str,
        target: str,
        confirmed: bool,
    ) -> None:
        """
        Record a user feedback entry.

        Args:
            tenant_id: Tenant identifier.
            feature: Feature name ("auto-map" or "smart-extract").
            context: Context key (ESL source or message type).
            input_key: The input key or segment being mapped.
            target: The target field or segment.
            confirmed: True if user confirmed, False if rejected.
        """
        store_key = self._make_store_key(tenant_id, feature, context)
        entry = FeedbackEntry(
            input_key=input_key,
            target=target,
            confirmed=confirmed,
        )
        with self._lock:
            if store_key not in self._store:
                self._store[store_key] = []
            entries = self._store[store_key]
            # Remove existing feedback for same input_key (latest wins)
            entries[:] = [e for e in entries if e.input_key != input_key]
            entries.append(entry)
            # Trim to max
            if len(entries) > self._max_entries:
                entries[:] = entries[-self._max_entries:]

    def get_confirmed_mappings(
        self, tenant_id: str, feature: str, context: str
    ) -> List[Dict[str, str]]:
        """
        Get all confirmed mappings for a given tenant/feature/context.

        Returns:
            List of dicts with 'input_key' and 'target' for confirmed mappings.
        """
        store_key = self._make_store_key(tenant_id, feature, context)
        with self._lock:
            entries = self._store.get(store_key, [])
            return [
                {"input_key": e.input_key, "target": e.target}
                for e in entries
                if e.confirmed
            ]

    def get_rejected_mappings(
        self, tenant_id: str, feature: str, context: str
    ) -> List[Dict[str, str]]:
        """
        Get all rejected mappings for a given tenant/feature/context.

        Returns:
            List of dicts with 'input_key' and 'target' for rejected mappings.
        """
        store_key = self._make_store_key(tenant_id, feature, context)
        with self._lock:
            entries = self._store.get(store_key, [])
            return [
                {"input_key": e.input_key, "target": e.target}
                for e in entries
                if not e.confirmed
            ]

    def clear(self, tenant_id: str = None, feature: str = None, context: str = None) -> None:
        """Clear feedback entries. If all args are None, clears everything."""
        with self._lock:
            if tenant_id is None and feature is None and context is None:
                self._store.clear()
                return
            if tenant_id and feature and context:
                store_key = self._make_store_key(tenant_id, feature, context)
                self._store.pop(store_key, None)
                return
            # Partial clear: remove matching keys
            prefix_parts = []
            if tenant_id:
                prefix_parts.append(tenant_id)
            keys_to_remove = [
                k for k in self._store
                if all(part in k for part in prefix_parts)
            ]
            for k in keys_to_remove:
                del self._store[k]


# Singleton feedback store
_feedback_store: Optional[MappingFeedbackStore] = None


def get_feedback_store() -> MappingFeedbackStore:
    """Get singleton MappingFeedbackStore instance."""
    global _feedback_store
    if _feedback_store is None:
        _feedback_store = MappingFeedbackStore()
    return _feedback_store


# =============================================================================
# Ignore List Store
# =============================================================================

class IgnoreListStore:
    """
    Persistent in-memory store for segment/field ignore lists.

    Stores per tenant + message type which segments or fields to always skip.

    Usage:
        store = get_ignore_list_store()
        store.add_ignored("tenant1", "ADT_A01", segments=["GT1", "PD1"])
        store.add_ignored("tenant1", "ADT_A01", fields=["PID.27", "PID.30"])
        ignored = store.get_ignored("tenant1", "ADT_A01")
        # → {"segments": ["GT1", "PD1"], "fields": ["PID.27", "PID.30"]}
    """

    def __init__(self):
        # Key: "tenant_id|message_type" → {"segments": set, "fields": set}
        self._store: Dict[str, Dict[str, set]] = {}
        self._lock = threading.Lock()

    def _make_key(self, tenant_id: str, message_type: str) -> str:
        return f"{tenant_id}|{message_type}"

    def add_ignored(
        self,
        tenant_id: str,
        message_type: str,
        segments: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ) -> None:
        """Add segments or fields to the ignore list."""
        key = self._make_key(tenant_id, message_type)
        with self._lock:
            if key not in self._store:
                self._store[key] = {"segments": set(), "fields": set()}
            if segments:
                self._store[key]["segments"].update(segments)
            if fields:
                self._store[key]["fields"].update(fields)

    def remove_ignored(
        self,
        tenant_id: str,
        message_type: str,
        segments: Optional[List[str]] = None,
        fields: Optional[List[str]] = None,
    ) -> None:
        """Remove segments or fields from the ignore list."""
        key = self._make_key(tenant_id, message_type)
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return
            if segments:
                entry["segments"].difference_update(segments)
            if fields:
                entry["fields"].difference_update(fields)

    def get_ignored(self, tenant_id: str, message_type: str) -> Dict[str, List[str]]:
        """Get the full ignore list for a tenant + message type."""
        key = self._make_key(tenant_id, message_type)
        with self._lock:
            entry = self._store.get(key, {"segments": set(), "fields": set()})
            return {
                "segments": sorted(entry["segments"]),
                "fields": sorted(entry["fields"]),
            }

    def is_segment_ignored(self, tenant_id: str, message_type: str, segment: str) -> bool:
        """Check if a specific segment is ignored."""
        key = self._make_key(tenant_id, message_type)
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return False
            return segment in entry["segments"]

    def is_field_ignored(self, tenant_id: str, message_type: str, field: str) -> bool:
        """Check if a specific field (e.g., PID.27) is ignored."""
        key = self._make_key(tenant_id, message_type)
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return False
            return field in entry["fields"]

    def clear(self, tenant_id: str = None, message_type: str = None) -> None:
        """Clear ignore list entries."""
        with self._lock:
            if tenant_id is None and message_type is None:
                self._store.clear()
                return
            if tenant_id and message_type:
                key = self._make_key(tenant_id, message_type)
                self._store.pop(key, None)
                return
            # Partial match
            keys_to_remove = [
                k for k in self._store
                if (tenant_id and k.startswith(tenant_id + "|")) or
                   (message_type and k.endswith("|" + message_type))
            ]
            for k in keys_to_remove:
                del self._store[k]


# Singleton ignore list store
_ignore_list_store: Optional[IgnoreListStore] = None


def get_ignore_list_store() -> IgnoreListStore:
    """Get singleton IgnoreListStore instance."""
    global _ignore_list_store
    if _ignore_list_store is None:
        _ignore_list_store = IgnoreListStore()
    return _ignore_list_store
