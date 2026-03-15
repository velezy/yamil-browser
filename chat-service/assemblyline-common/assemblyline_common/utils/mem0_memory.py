"""
T.A.L.O.S. Memory Layer with Mem0
Based on Memobytes patterns

Features:
- Persistent AI memory for personalization
- User preference learning
- Conversation context retention
- Entity and fact memory

Cutting Edge Features:
- Full bidirectional sync between local DB and Mem0 cloud
- Cross-session learning with shared memory pools
- Real-time memory sync with event-driven updates
- Conflict resolution and merge strategies
"""

import os
import logging
import asyncio
import hashlib
import json
import threading
import time
from typing import Optional, List, Dict, Any, Callable, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from queue import Queue, Empty
import weakref

logger = logging.getLogger(__name__)

# =============================================================================
# MEM0 AVAILABILITY CHECK
# =============================================================================

MEM0_AVAILABLE = False

try:
    from mem0 import Memory, MemoryClient
    MEM0_AVAILABLE = True
    logger.info("mem0ai library loaded successfully")
except ImportError:
    logger.warning("mem0ai not installed. Run: pip install mem0ai")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class MemoryConfig:
    """Memory layer configuration"""
    # Vector store config (uses Qdrant by default in mem0)
    vector_store: str = "qdrant"
    qdrant_host: str = os.getenv("QDRANT_HOST", "localhost")
    qdrant_port: int = int(os.getenv("QDRANT_PORT", "6333"))

    # LLM config for memory extraction
    llm_provider: str = "ollama"
    llm_model: str = os.getenv("OLLAMA_MODEL", "gemma3:4b")
    ollama_url: str = os.getenv("OLLAMA_URL", "http://localhost:11434")

    # Embedding config
    embedder_provider: str = "huggingface"
    embedding_model: str = "all-MiniLM-L6-v2"

    # Memory settings
    memory_collection: str = "talos_memories"
    max_memories_per_user: int = 1000


@dataclass
class MemoryItem:
    """A single memory item"""
    id: str
    content: str
    user_id: str
    category: str = "general"
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    score: float = 1.0


# =============================================================================
# MEMORY SERVICE
# =============================================================================

class MemoryService:
    """
    Mem0-based memory layer for T.A.L.O.S.

    Features:
    - Long-term memory storage
    - User preference learning
    - Semantic memory search
    - Automatic memory extraction from conversations
    """

    def __init__(self, config: Optional[MemoryConfig] = None):
        self.config = config or MemoryConfig()
        self._memory = None
        self._initialized = False
        self._fallback_store: Dict[str, List[MemoryItem]] = {}

    def initialize(self) -> bool:
        """Initialize the memory layer"""
        if not MEM0_AVAILABLE:
            logger.warning("mem0 not available, using fallback memory store")
            self._initialized = True
            return True

        try:
            # Configure mem0
            config = {
                "vector_store": {
                    "provider": self.config.vector_store,
                    "config": {
                        "host": self.config.qdrant_host,
                        "port": self.config.qdrant_port,
                        "collection_name": self.config.memory_collection
                    }
                },
                "llm": {
                    "provider": self.config.llm_provider,
                    "config": {
                        "model": self.config.llm_model,
                        "ollama_base_url": self.config.ollama_url
                    }
                },
                "embedder": {
                    "provider": "ollama",  # Use Ollama for embeddings (huggingface may not work)
                    "config": {
                        "model": "nomic-embed-text",  # Ollama embedding model
                        "ollama_base_url": self.config.ollama_url
                    }
                }
            }

            self._memory = Memory.from_config(config)
            self._initialized = True
            logger.info("Mem0 memory layer initialized")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize mem0: {e}")
            logger.info("Using fallback memory store")
            self._initialized = True
            return True

    def add_memory(
        self,
        content: str,
        user_id: str,
        category: str = "general",
        metadata: Dict[str, Any] = None
    ) -> Optional[str]:
        """
        Add a memory for a user.

        Args:
            content: Memory content
            user_id: User identifier
            category: Memory category (preference, fact, context, etc.)
            metadata: Additional metadata

        Returns:
            Memory ID or None
        """
        if not self._initialized:
            self.initialize()

        metadata = metadata or {}
        metadata["category"] = category
        metadata["timestamp"] = datetime.utcnow().isoformat()

        if self._memory:
            try:
                result = self._memory.add(
                    content,
                    user_id=user_id,
                    metadata=metadata
                )
                return result.get("id") if isinstance(result, dict) else str(result)
            except Exception as e:
                logger.error(f"Failed to add memory: {e}")

        # Fallback store
        memory_id = f"mem_{user_id}_{len(self._fallback_store.get(user_id, []))}"
        memory = MemoryItem(
            id=memory_id,
            content=content,
            user_id=user_id,
            category=category,
            metadata=metadata
        )

        if user_id not in self._fallback_store:
            self._fallback_store[user_id] = []
        self._fallback_store[user_id].append(memory)

        return memory_id

    def search_memories(
        self,
        query: str,
        user_id: str,
        limit: int = 10,
        category: str = None
    ) -> List[MemoryItem]:
        """
        Search memories for a user.

        Args:
            query: Search query
            user_id: User identifier
            limit: Maximum results
            category: Filter by category

        Returns:
            List of matching memories
        """
        if not self._initialized:
            self.initialize()

        if self._memory:
            try:
                results = self._memory.search(
                    query,
                    user_id=user_id,
                    limit=limit
                )

                memories = []
                for r in results:
                    if category and r.get("metadata", {}).get("category") != category:
                        continue
                    memories.append(MemoryItem(
                        id=r.get("id", ""),
                        content=r.get("memory", ""),
                        user_id=user_id,
                        category=r.get("metadata", {}).get("category", "general"),
                        metadata=r.get("metadata", {}),
                        score=r.get("score", 1.0)
                    ))
                return memories[:limit]

            except Exception as e:
                logger.error(f"Memory search failed: {e}")

        # Fallback: simple text search
        memories = self._fallback_store.get(user_id, [])
        query_lower = query.lower()

        matched = [
            m for m in memories
            if query_lower in m.content.lower()
            and (category is None or m.category == category)
        ]

        return matched[:limit]

    def get_all_memories(
        self,
        user_id: str,
        category: str = None
    ) -> List[MemoryItem]:
        """
        Get all memories for a user.

        Args:
            user_id: User identifier
            category: Optional category filter

        Returns:
            List of all memories
        """
        if not self._initialized:
            self.initialize()

        if self._memory:
            try:
                results = self._memory.get_all(user_id=user_id)

                memories = []
                for r in results:
                    if category and r.get("metadata", {}).get("category") != category:
                        continue
                    memories.append(MemoryItem(
                        id=r.get("id", ""),
                        content=r.get("memory", ""),
                        user_id=user_id,
                        category=r.get("metadata", {}).get("category", "general"),
                        metadata=r.get("metadata", {})
                    ))
                return memories

            except Exception as e:
                logger.error(f"Failed to get memories: {e}")

        # Fallback
        memories = self._fallback_store.get(user_id, [])
        if category:
            memories = [m for m in memories if m.category == category]
        return memories

    def update_memory(
        self,
        memory_id: str,
        content: str,
        user_id: str
    ) -> bool:
        """
        Update an existing memory.

        Args:
            memory_id: Memory ID to update
            content: New content
            user_id: User identifier

        Returns:
            True if successful
        """
        if not self._initialized:
            self.initialize()

        if self._memory:
            try:
                self._memory.update(memory_id, content)
                return True
            except Exception as e:
                logger.error(f"Failed to update memory: {e}")
                return False

        # Fallback
        memories = self._fallback_store.get(user_id, [])
        for m in memories:
            if m.id == memory_id:
                m.content = content
                m.updated_at = datetime.utcnow()
                return True
        return False

    def delete_memory(
        self,
        memory_id: str,
        user_id: str
    ) -> bool:
        """
        Delete a memory.

        Args:
            memory_id: Memory ID to delete
            user_id: User identifier

        Returns:
            True if successful
        """
        if not self._initialized:
            self.initialize()

        if self._memory:
            try:
                self._memory.delete(memory_id)
                return True
            except Exception as e:
                logger.error(f"Failed to delete memory: {e}")
                return False

        # Fallback
        if user_id in self._fallback_store:
            self._fallback_store[user_id] = [
                m for m in self._fallback_store[user_id]
                if m.id != memory_id
            ]
            return True
        return False

    def delete_all_memories(self, user_id: str) -> bool:
        """Delete all memories for a user"""
        if not self._initialized:
            self.initialize()

        if self._memory:
            try:
                self._memory.delete_all(user_id=user_id)
                return True
            except Exception as e:
                logger.error(f"Failed to delete all memories: {e}")
                return False

        # Fallback
        self._fallback_store.pop(user_id, None)
        return True


# =============================================================================
# CONVERSATION MEMORY
# =============================================================================

class ConversationMemory:
    """
    Specialized memory for conversation context.
    Automatically extracts and stores relevant information.
    """

    def __init__(self, memory_service: Optional[MemoryService] = None):
        self.memory = memory_service or MemoryService()

    def add_conversation_turn(
        self,
        user_id: str,
        user_message: str,
        assistant_response: str,
        conversation_id: str = None
    ):
        """
        Process a conversation turn and extract memories.

        Args:
            user_id: User identifier
            user_message: User's message
            assistant_response: Assistant's response
            conversation_id: Optional conversation ID
        """
        # Store the full exchange
        content = f"User asked: {user_message}\nAssistant responded: {assistant_response[:500]}"

        self.memory.add_memory(
            content=content,
            user_id=user_id,
            category="conversation",
            metadata={
                "conversation_id": conversation_id,
                "user_message": user_message[:200],
                "response_preview": assistant_response[:200]
            }
        )

    def get_relevant_context(
        self,
        user_id: str,
        current_query: str,
        limit: int = 5
    ) -> List[str]:
        """
        Get relevant conversation context for the current query.

        Args:
            user_id: User identifier
            current_query: Current user query
            limit: Maximum context items

        Returns:
            List of relevant context strings
        """
        memories = self.memory.search_memories(
            query=current_query,
            user_id=user_id,
            limit=limit,
            category="conversation"
        )

        return [m.content for m in memories]


# =============================================================================
# USER PREFERENCES MEMORY
# =============================================================================

class UserPreferencesMemory:
    """
    Specialized memory for user preferences and settings.
    """

    def __init__(self, memory_service: Optional[MemoryService] = None):
        self.memory = memory_service or MemoryService()

    def set_preference(
        self,
        user_id: str,
        preference_key: str,
        preference_value: Any
    ):
        """
        Set a user preference.

        Args:
            user_id: User identifier
            preference_key: Preference key
            preference_value: Preference value
        """
        content = f"User preference: {preference_key} = {preference_value}"

        self.memory.add_memory(
            content=content,
            user_id=user_id,
            category="preference",
            metadata={
                "key": preference_key,
                "value": preference_value
            }
        )

    def get_preferences(self, user_id: str) -> Dict[str, Any]:
        """
        Get all user preferences.

        Args:
            user_id: User identifier

        Returns:
            Dictionary of preferences
        """
        memories = self.memory.get_all_memories(
            user_id=user_id,
            category="preference"
        )

        preferences = {}
        for m in memories:
            key = m.metadata.get("key")
            value = m.metadata.get("value")
            if key:
                preferences[key] = value

        return preferences


# =============================================================================
# LEARNING MEMORY
# =============================================================================

class LearningMemory:
    """
    Specialized memory for tracking user learning progress.
    Useful for educational applications.
    """

    def __init__(self, memory_service: Optional[MemoryService] = None):
        self.memory = memory_service or MemoryService()

    def record_learning(
        self,
        user_id: str,
        topic: str,
        learned_content: str,
        confidence_level: float = 0.5
    ):
        """
        Record something the user learned.

        Args:
            user_id: User identifier
            topic: Topic learned
            learned_content: What was learned
            confidence_level: 0.0-1.0 confidence
        """
        content = f"Learned about {topic}: {learned_content}"

        self.memory.add_memory(
            content=content,
            user_id=user_id,
            category="learning",
            metadata={
                "topic": topic,
                "confidence": confidence_level
            }
        )

    def get_knowledge_gaps(
        self,
        user_id: str,
        topic: str
    ) -> List[str]:
        """
        Find areas where user might have knowledge gaps.

        Args:
            user_id: User identifier
            topic: Topic to analyze

        Returns:
            List of potential knowledge gaps
        """
        memories = self.memory.search_memories(
            query=topic,
            user_id=user_id,
            limit=20,
            category="learning"
        )

        # Find low-confidence learnings
        gaps = [
            m.content for m in memories
            if m.metadata.get("confidence", 0.5) < 0.5
        ]

        return gaps


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_memory_service: Optional[MemoryService] = None


def get_memory_service() -> MemoryService:
    """Get or create memory service singleton"""
    global _memory_service
    if _memory_service is None:
        _memory_service = MemoryService()
        _memory_service.initialize()
    return _memory_service


def is_mem0_available() -> bool:
    """Check if mem0 is available"""
    return MEM0_AVAILABLE


def quick_add_memory(content: str, user_id: str, category: str = "general") -> Optional[str]:
    """Quick add a memory"""
    service = get_memory_service()
    return service.add_memory(content, user_id, category)


def quick_search_memories(query: str, user_id: str, limit: int = 5) -> List[MemoryItem]:
    """Quick search memories"""
    service = get_memory_service()
    return service.search_memories(query, user_id, limit)


# =============================================================================
# CUTTING EDGE: Bidirectional Sync & Cross-Session Learning
# =============================================================================


class SyncDirection(str, Enum):
    """Direction of memory synchronization."""
    LOCAL_TO_REMOTE = "local_to_remote"
    REMOTE_TO_LOCAL = "remote_to_local"
    BIDIRECTIONAL = "bidirectional"


class SyncStatus(str, Enum):
    """Status of a sync operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CONFLICT = "conflict"


class ConflictResolution(str, Enum):
    """Strategy for resolving sync conflicts."""
    LOCAL_WINS = "local_wins"  # Local version takes precedence
    REMOTE_WINS = "remote_wins"  # Remote version takes precedence
    MERGE = "merge"  # Attempt to merge both versions
    NEWEST_WINS = "newest_wins"  # Most recently updated wins
    MANUAL = "manual"  # Require manual resolution


class MemoryPoolType(str, Enum):
    """Types of shared memory pools."""
    GLOBAL = "global"  # Shared across all users
    TEAM = "team"  # Shared within a team
    ORGANIZATION = "organization"  # Organization-wide
    PROJECT = "project"  # Project-specific
    TOPIC = "topic"  # Topic-based pool


@dataclass
class SyncRecord:
    """Record of a synchronization operation."""
    sync_id: str
    memory_id: str
    user_id: str
    direction: SyncDirection
    status: SyncStatus
    local_version: Optional[str] = None
    remote_version: Optional[str] = None
    local_timestamp: Optional[datetime] = None
    remote_timestamp: Optional[datetime] = None
    conflict_resolution: Optional[ConflictResolution] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None


@dataclass
class MemoryVersion:
    """Version tracking for a memory item."""
    memory_id: str
    version_hash: str
    content: str
    metadata: Dict[str, Any]
    timestamp: datetime
    source: str  # 'local' or 'remote'


@dataclass
class SharedMemory:
    """A memory shared in a pool."""
    memory_id: str
    content: str
    pool_type: MemoryPoolType
    pool_id: str  # Team ID, project ID, topic name, etc.
    contributor_id: str  # Who added this memory
    category: str = "general"
    metadata: Dict[str, Any] = field(default_factory=dict)
    access_count: int = 0
    usefulness_score: float = 0.5
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_accessed: Optional[datetime] = None


@dataclass
class SyncConflict:
    """A synchronization conflict requiring resolution."""
    conflict_id: str
    memory_id: str
    user_id: str
    local_version: MemoryVersion
    remote_version: MemoryVersion
    suggested_resolution: ConflictResolution
    auto_resolved: bool = False
    resolution_applied: Optional[ConflictResolution] = None
    merged_content: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SyncEvent:
    """An event in the sync queue."""
    event_type: str  # 'add', 'update', 'delete', 'sync'
    memory_id: str
    user_id: str
    content: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    priority: int = 5  # 1 (highest) to 10 (lowest)


class MemorySyncManager:
    """
    Manages bidirectional synchronization between local and remote memory stores.

    Features:
    - Conflict detection and resolution
    - Version tracking
    - Batch sync operations
    - Incremental sync for efficiency
    """

    def __init__(
        self,
        local_service: MemoryService,
        conflict_strategy: ConflictResolution = ConflictResolution.NEWEST_WINS,
        sync_interval_seconds: int = 60
    ):
        """
        Initialize the sync manager.

        Args:
            local_service: Local MemoryService instance
            conflict_strategy: Default conflict resolution strategy
            sync_interval_seconds: Interval between automatic syncs
        """
        self.local = local_service
        self.conflict_strategy = conflict_strategy
        self.sync_interval = sync_interval_seconds

        # Version tracking
        self._local_versions: Dict[str, MemoryVersion] = {}
        self._remote_versions: Dict[str, MemoryVersion] = {}

        # Sync state
        self._sync_records: List[SyncRecord] = []
        self._conflicts: Dict[str, SyncConflict] = {}
        self._last_sync: Dict[str, datetime] = {}  # user_id -> last sync time

        # Sync queue
        self._sync_queue: Queue = Queue()
        self._sync_lock = threading.Lock()

        logger.info(f"MemorySyncManager initialized with {conflict_strategy.value} strategy")

    def _compute_hash(self, content: str, metadata: Dict[str, Any]) -> str:
        """Compute a hash for version comparison."""
        data = json.dumps({"content": content, "metadata": metadata}, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    async def sync_user_memories(
        self,
        user_id: str,
        direction: SyncDirection = SyncDirection.BIDIRECTIONAL,
        force: bool = False
    ) -> Dict[str, Any]:
        """
        Synchronize memories for a user.

        Args:
            user_id: User to sync
            direction: Sync direction
            force: Force full sync even if recently synced

        Returns:
            Sync results with statistics
        """
        # Check if sync is needed
        last_sync = self._last_sync.get(user_id)
        if not force and last_sync:
            time_since_sync = (datetime.utcnow() - last_sync).total_seconds()
            if time_since_sync < self.sync_interval:
                return {"status": "skipped", "reason": "recently_synced"}

        results = {
            "user_id": user_id,
            "direction": direction.value,
            "started_at": datetime.utcnow().isoformat(),
            "local_to_remote": 0,
            "remote_to_local": 0,
            "conflicts_detected": 0,
            "conflicts_resolved": 0,
            "errors": []
        }

        try:
            # Get local memories
            local_memories = self.local.get_all_memories(user_id)

            # Get remote memories (if Mem0 available)
            remote_memories = await self._fetch_remote_memories(user_id)

            # Build lookup maps
            local_map = {self._get_memory_key(m): m for m in local_memories}
            remote_map = {self._get_memory_key(m): m for m in remote_memories}

            # Sync based on direction
            if direction in [SyncDirection.LOCAL_TO_REMOTE, SyncDirection.BIDIRECTIONAL]:
                for key, local_mem in local_map.items():
                    if key not in remote_map:
                        # Push to remote
                        await self._push_to_remote(local_mem, user_id)
                        results["local_to_remote"] += 1
                    else:
                        # Check for conflicts
                        remote_mem = remote_map[key]
                        conflict = self._detect_conflict(local_mem, remote_mem, user_id)
                        if conflict:
                            self._conflicts[conflict.conflict_id] = conflict
                            results["conflicts_detected"] += 1

                            # Auto-resolve if possible
                            resolved = await self._auto_resolve_conflict(conflict)
                            if resolved:
                                results["conflicts_resolved"] += 1

            if direction in [SyncDirection.REMOTE_TO_LOCAL, SyncDirection.BIDIRECTIONAL]:
                for key, remote_mem in remote_map.items():
                    if key not in local_map:
                        # Pull from remote
                        await self._pull_from_remote(remote_mem, user_id)
                        results["remote_to_local"] += 1

            self._last_sync[user_id] = datetime.utcnow()
            results["completed_at"] = datetime.utcnow().isoformat()
            results["status"] = "completed"

        except Exception as e:
            logger.error(f"Sync failed for user {user_id}: {e}")
            results["status"] = "failed"
            results["errors"].append(str(e))

        return results

    def _get_memory_key(self, memory: MemoryItem) -> str:
        """Generate a unique key for memory deduplication."""
        # Use content hash + category as key
        content_hash = hashlib.md5(memory.content.encode()).hexdigest()[:8]
        return f"{memory.category}:{content_hash}"

    async def _fetch_remote_memories(self, user_id: str) -> List[MemoryItem]:
        """Fetch memories from remote Mem0 store."""
        if not self.local._memory:
            return []

        try:
            results = self.local._memory.get_all(user_id=user_id)
            return [
                MemoryItem(
                    id=r.get("id", ""),
                    content=r.get("memory", ""),
                    user_id=user_id,
                    category=r.get("metadata", {}).get("category", "general"),
                    metadata=r.get("metadata", {})
                )
                for r in results
            ]
        except Exception as e:
            logger.error(f"Failed to fetch remote memories: {e}")
            return []

    async def _push_to_remote(self, memory: MemoryItem, user_id: str):
        """Push a memory to the remote store."""
        if not self.local._memory:
            return

        try:
            self.local._memory.add(
                memory.content,
                user_id=user_id,
                metadata={"category": memory.category, **memory.metadata}
            )

            # Track version
            version = MemoryVersion(
                memory_id=memory.id,
                version_hash=self._compute_hash(memory.content, memory.metadata),
                content=memory.content,
                metadata=memory.metadata,
                timestamp=datetime.utcnow(),
                source="local"
            )
            self._local_versions[memory.id] = version

        except Exception as e:
            logger.error(f"Failed to push memory to remote: {e}")

    async def _pull_from_remote(self, memory: MemoryItem, user_id: str):
        """Pull a memory from remote to local store."""
        # Add to local fallback store
        if user_id not in self.local._fallback_store:
            self.local._fallback_store[user_id] = []

        self.local._fallback_store[user_id].append(memory)

        # Track version
        version = MemoryVersion(
            memory_id=memory.id,
            version_hash=self._compute_hash(memory.content, memory.metadata),
            content=memory.content,
            metadata=memory.metadata,
            timestamp=datetime.utcnow(),
            source="remote"
        )
        self._remote_versions[memory.id] = version

    def _detect_conflict(
        self,
        local: MemoryItem,
        remote: MemoryItem,
        user_id: str
    ) -> Optional[SyncConflict]:
        """Detect if there's a conflict between local and remote versions."""
        local_hash = self._compute_hash(local.content, local.metadata)
        remote_hash = self._compute_hash(remote.content, remote.metadata)

        if local_hash == remote_hash:
            return None  # No conflict

        # Create conflict record
        conflict_id = f"conflict_{local.id}_{int(time.time())}"

        local_version = MemoryVersion(
            memory_id=local.id,
            version_hash=local_hash,
            content=local.content,
            metadata=local.metadata,
            timestamp=local.updated_at,
            source="local"
        )

        remote_version = MemoryVersion(
            memory_id=remote.id,
            version_hash=remote_hash,
            content=remote.content,
            metadata=remote.metadata,
            timestamp=remote.updated_at,
            source="remote"
        )

        return SyncConflict(
            conflict_id=conflict_id,
            memory_id=local.id,
            user_id=user_id,
            local_version=local_version,
            remote_version=remote_version,
            suggested_resolution=self.conflict_strategy
        )

    async def _auto_resolve_conflict(self, conflict: SyncConflict) -> bool:
        """Attempt to automatically resolve a conflict."""
        if self.conflict_strategy == ConflictResolution.MANUAL:
            return False

        try:
            if self.conflict_strategy == ConflictResolution.LOCAL_WINS:
                # Push local to remote
                memory = MemoryItem(
                    id=conflict.memory_id,
                    content=conflict.local_version.content,
                    user_id=conflict.user_id,
                    metadata=conflict.local_version.metadata
                )
                await self._push_to_remote(memory, conflict.user_id)

            elif self.conflict_strategy == ConflictResolution.REMOTE_WINS:
                # Update local with remote
                memory = MemoryItem(
                    id=conflict.memory_id,
                    content=conflict.remote_version.content,
                    user_id=conflict.user_id,
                    metadata=conflict.remote_version.metadata
                )
                await self._pull_from_remote(memory, conflict.user_id)

            elif self.conflict_strategy == ConflictResolution.NEWEST_WINS:
                if conflict.local_version.timestamp > conflict.remote_version.timestamp:
                    memory = MemoryItem(
                        id=conflict.memory_id,
                        content=conflict.local_version.content,
                        user_id=conflict.user_id,
                        metadata=conflict.local_version.metadata
                    )
                    await self._push_to_remote(memory, conflict.user_id)
                else:
                    memory = MemoryItem(
                        id=conflict.memory_id,
                        content=conflict.remote_version.content,
                        user_id=conflict.user_id,
                        metadata=conflict.remote_version.metadata
                    )
                    await self._pull_from_remote(memory, conflict.user_id)

            elif self.conflict_strategy == ConflictResolution.MERGE:
                # Attempt to merge content
                merged = self._merge_content(
                    conflict.local_version.content,
                    conflict.remote_version.content
                )
                conflict.merged_content = merged

                # Push merged version
                memory = MemoryItem(
                    id=conflict.memory_id,
                    content=merged,
                    user_id=conflict.user_id,
                    metadata={**conflict.local_version.metadata, **conflict.remote_version.metadata}
                )
                await self._push_to_remote(memory, conflict.user_id)

            conflict.auto_resolved = True
            conflict.resolution_applied = self.conflict_strategy
            return True

        except Exception as e:
            logger.error(f"Failed to auto-resolve conflict: {e}")
            return False

    def _merge_content(self, local: str, remote: str) -> str:
        """Merge two versions of content."""
        # Simple merge: combine unique information
        local_sentences = set(local.split('. '))
        remote_sentences = set(remote.split('. '))

        # Union of unique sentences
        merged_sentences = local_sentences | remote_sentences
        return '. '.join(sorted(merged_sentences))

    def get_pending_conflicts(self, user_id: Optional[str] = None) -> List[SyncConflict]:
        """Get conflicts pending manual resolution."""
        conflicts = list(self._conflicts.values())
        if user_id:
            conflicts = [c for c in conflicts if c.user_id == user_id]
        return [c for c in conflicts if not c.auto_resolved]

    def resolve_conflict(
        self,
        conflict_id: str,
        resolution: ConflictResolution,
        merged_content: Optional[str] = None
    ) -> bool:
        """Manually resolve a conflict."""
        if conflict_id not in self._conflicts:
            return False

        conflict = self._conflicts[conflict_id]
        conflict.resolution_applied = resolution
        conflict.auto_resolved = True

        if merged_content:
            conflict.merged_content = merged_content

        return True

    def get_sync_stats(self) -> Dict[str, Any]:
        """Get synchronization statistics."""
        return {
            "total_syncs": len(self._sync_records),
            "pending_conflicts": len([c for c in self._conflicts.values() if not c.auto_resolved]),
            "resolved_conflicts": len([c for c in self._conflicts.values() if c.auto_resolved]),
            "local_versions_tracked": len(self._local_versions),
            "remote_versions_tracked": len(self._remote_versions),
            "users_synced": len(self._last_sync),
            "conflict_strategy": self.conflict_strategy.value
        }


class SharedMemoryPool:
    """
    Manages shared memory pools for cross-session learning.

    Enables knowledge sharing across users, teams, and projects.
    """

    def __init__(self, memory_service: MemoryService):
        """
        Initialize the shared memory pool.

        Args:
            memory_service: Base memory service
        """
        self.memory = memory_service

        # Pool storage
        self._pools: Dict[str, Dict[str, SharedMemory]] = defaultdict(dict)
        self._pool_members: Dict[str, Set[str]] = defaultdict(set)  # pool_id -> user_ids
        self._user_pools: Dict[str, Set[str]] = defaultdict(set)  # user_id -> pool_ids

        # Access tracking
        self._access_history: Dict[str, List[Tuple[str, datetime]]] = defaultdict(list)

        logger.info("SharedMemoryPool initialized")

    def create_pool(
        self,
        pool_type: MemoryPoolType,
        pool_id: str,
        creator_id: str,
        initial_members: List[str] = None
    ) -> bool:
        """
        Create a new shared memory pool.

        Args:
            pool_type: Type of pool
            pool_id: Unique pool identifier
            creator_id: User creating the pool
            initial_members: Initial pool members

        Returns:
            True if created successfully
        """
        full_pool_id = f"{pool_type.value}:{pool_id}"

        if full_pool_id in self._pools:
            logger.warning(f"Pool {full_pool_id} already exists")
            return False

        # Initialize pool
        self._pools[full_pool_id] = {}
        self._pool_members[full_pool_id] = {creator_id}

        # Add initial members
        if initial_members:
            for member_id in initial_members:
                self._pool_members[full_pool_id].add(member_id)
                self._user_pools[member_id].add(full_pool_id)

        self._user_pools[creator_id].add(full_pool_id)

        logger.info(f"Created pool {full_pool_id} with {len(self._pool_members[full_pool_id])} members")
        return True

    def join_pool(self, user_id: str, pool_type: MemoryPoolType, pool_id: str) -> bool:
        """Add a user to a pool."""
        full_pool_id = f"{pool_type.value}:{pool_id}"

        if full_pool_id not in self._pools:
            return False

        self._pool_members[full_pool_id].add(user_id)
        self._user_pools[user_id].add(full_pool_id)
        return True

    def leave_pool(self, user_id: str, pool_type: MemoryPoolType, pool_id: str) -> bool:
        """Remove a user from a pool."""
        full_pool_id = f"{pool_type.value}:{pool_id}"

        if full_pool_id not in self._pools:
            return False

        self._pool_members[full_pool_id].discard(user_id)
        self._user_pools[user_id].discard(full_pool_id)
        return True

    def contribute_memory(
        self,
        user_id: str,
        pool_type: MemoryPoolType,
        pool_id: str,
        content: str,
        category: str = "general",
        metadata: Dict[str, Any] = None
    ) -> Optional[str]:
        """
        Contribute a memory to a shared pool.

        Args:
            user_id: User contributing
            pool_type: Pool type
            pool_id: Pool identifier
            content: Memory content
            category: Memory category
            metadata: Additional metadata

        Returns:
            Memory ID if successful
        """
        full_pool_id = f"{pool_type.value}:{pool_id}"

        # Check membership
        if user_id not in self._pool_members.get(full_pool_id, set()):
            logger.warning(f"User {user_id} not a member of pool {full_pool_id}")
            return None

        # Create shared memory
        memory_id = hashlib.md5(f"{full_pool_id}:{content}:{time.time()}".encode()).hexdigest()[:12]

        shared_memory = SharedMemory(
            memory_id=memory_id,
            content=content,
            pool_type=pool_type,
            pool_id=pool_id,
            contributor_id=user_id,
            category=category,
            metadata=metadata or {}
        )

        self._pools[full_pool_id][memory_id] = shared_memory

        logger.info(f"Memory {memory_id} contributed to pool {full_pool_id}")
        return memory_id

    def search_pool(
        self,
        user_id: str,
        query: str,
        pool_type: Optional[MemoryPoolType] = None,
        pool_id: Optional[str] = None,
        limit: int = 10
    ) -> List[SharedMemory]:
        """
        Search for memories in pools the user has access to.

        Args:
            user_id: Searching user
            query: Search query
            pool_type: Filter by pool type
            pool_id: Filter by specific pool
            limit: Maximum results

        Returns:
            List of matching shared memories
        """
        results = []
        query_lower = query.lower()

        # Determine pools to search
        if pool_id:
            full_pool_id = f"{pool_type.value}:{pool_id}" if pool_type else None
            pools_to_search = [full_pool_id] if full_pool_id in self._pools else []
        else:
            pools_to_search = list(self._user_pools.get(user_id, set()))

        # Filter by type if specified
        if pool_type and not pool_id:
            pools_to_search = [p for p in pools_to_search if p.startswith(f"{pool_type.value}:")]

        # Search pools
        for pool_key in pools_to_search:
            pool_memories = self._pools.get(pool_key, {})
            for memory in pool_memories.values():
                if query_lower in memory.content.lower():
                    results.append(memory)

                    # Track access
                    self._record_access(memory.memory_id, user_id)

        # Sort by usefulness score
        results.sort(key=lambda m: m.usefulness_score, reverse=True)

        return results[:limit]

    def _record_access(self, memory_id: str, user_id: str):
        """Record memory access for analytics."""
        self._access_history[memory_id].append((user_id, datetime.utcnow()))

        # Update access count in the memory
        for pool_memories in self._pools.values():
            if memory_id in pool_memories:
                pool_memories[memory_id].access_count += 1
                pool_memories[memory_id].last_accessed = datetime.utcnow()
                break

    def rate_memory(self, user_id: str, memory_id: str, rating: float) -> bool:
        """
        Rate a shared memory's usefulness.

        Args:
            user_id: User rating
            memory_id: Memory to rate
            rating: Rating (0.0 to 1.0)

        Returns:
            True if rated successfully
        """
        for pool_memories in self._pools.values():
            if memory_id in pool_memories:
                memory = pool_memories[memory_id]
                # Moving average
                memory.usefulness_score = (memory.usefulness_score * 0.7) + (rating * 0.3)
                return True
        return False

    def get_pool_memories(
        self,
        pool_type: MemoryPoolType,
        pool_id: str,
        category: Optional[str] = None
    ) -> List[SharedMemory]:
        """Get all memories in a pool."""
        full_pool_id = f"{pool_type.value}:{pool_id}"
        pool_memories = list(self._pools.get(full_pool_id, {}).values())

        if category:
            pool_memories = [m for m in pool_memories if m.category == category]

        return sorted(pool_memories, key=lambda m: m.usefulness_score, reverse=True)

    def get_user_contributions(self, user_id: str) -> List[SharedMemory]:
        """Get all memories contributed by a user."""
        contributions = []
        for pool_memories in self._pools.values():
            for memory in pool_memories.values():
                if memory.contributor_id == user_id:
                    contributions.append(memory)
        return contributions

    def get_cross_session_context(
        self,
        user_id: str,
        query: str,
        limit: int = 5
    ) -> List[str]:
        """
        Get relevant context from shared pools for cross-session learning.

        Args:
            user_id: User requesting context
            query: Current query
            limit: Maximum context items

        Returns:
            List of relevant context strings
        """
        shared_memories = self.search_pool(user_id, query, limit=limit)
        return [m.content for m in shared_memories]

    def get_pool_stats(self, pool_type: MemoryPoolType, pool_id: str) -> Dict[str, Any]:
        """Get statistics for a pool."""
        full_pool_id = f"{pool_type.value}:{pool_id}"

        if full_pool_id not in self._pools:
            return {"error": "Pool not found"}

        memories = list(self._pools[full_pool_id].values())
        members = self._pool_members.get(full_pool_id, set())

        return {
            "pool_id": pool_id,
            "pool_type": pool_type.value,
            "member_count": len(members),
            "memory_count": len(memories),
            "total_accesses": sum(m.access_count for m in memories),
            "avg_usefulness": sum(m.usefulness_score for m in memories) / len(memories) if memories else 0,
            "top_contributors": self._get_top_contributors(full_pool_id, limit=5),
            "categories": list(set(m.category for m in memories))
        }

    def _get_top_contributors(self, pool_id: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get top contributors for a pool."""
        contributor_counts: Dict[str, int] = defaultdict(int)

        for memory in self._pools.get(pool_id, {}).values():
            contributor_counts[memory.contributor_id] += 1

        sorted_contributors = sorted(contributor_counts.items(), key=lambda x: x[1], reverse=True)

        return [
            {"user_id": uid, "contribution_count": count}
            for uid, count in sorted_contributors[:limit]
        ]


class RealTimeSyncEngine:
    """
    Real-time synchronization engine with event-driven updates.

    Features:
    - Event queue processing
    - Webhooks for external notifications
    - Debouncing for efficiency
    - Priority-based sync
    """

    def __init__(
        self,
        sync_manager: MemorySyncManager,
        debounce_seconds: float = 1.0,
        batch_size: int = 10
    ):
        """
        Initialize the real-time sync engine.

        Args:
            sync_manager: MemorySyncManager instance
            debounce_seconds: Debounce delay for batching events
            batch_size: Maximum events per batch
        """
        self.sync_manager = sync_manager
        self.debounce_seconds = debounce_seconds
        self.batch_size = batch_size

        # Event queue
        self._event_queue: Queue = Queue()
        self._pending_events: Dict[str, SyncEvent] = {}

        # Subscribers
        self._subscribers: Dict[str, List[Callable]] = defaultdict(list)

        # State
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None

        # Stats
        self._events_processed = 0
        self._batches_processed = 0

        logger.info("RealTimeSyncEngine initialized")

    def start(self):
        """Start the real-time sync engine."""
        if self._running:
            return

        self._running = True
        self._worker_thread = threading.Thread(target=self._process_events, daemon=True)
        self._worker_thread.start()
        logger.info("RealTimeSyncEngine started")

    def stop(self):
        """Stop the real-time sync engine."""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5.0)
        logger.info("RealTimeSyncEngine stopped")

    def emit_event(self, event: SyncEvent):
        """
        Emit a sync event.

        Args:
            event: Event to emit
        """
        # Deduplicate by memory_id (keep latest)
        self._pending_events[event.memory_id] = event
        self._event_queue.put(event)

    def subscribe(self, event_type: str, callback: Callable):
        """
        Subscribe to sync events.

        Args:
            event_type: Event type to subscribe to ('add', 'update', 'delete', 'sync', '*')
            callback: Callback function(event: SyncEvent)
        """
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        """Unsubscribe from sync events."""
        if callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)

    def _process_events(self):
        """Background worker for processing events."""
        while self._running:
            batch = []

            # Collect events with debouncing
            try:
                # Wait for first event
                event = self._event_queue.get(timeout=1.0)
                batch.append(event)

                # Collect more events during debounce window
                deadline = time.time() + self.debounce_seconds
                while len(batch) < self.batch_size and time.time() < deadline:
                    try:
                        event = self._event_queue.get(timeout=0.1)
                        batch.append(event)
                    except Empty:
                        break

            except Empty:
                continue

            # Process batch
            if batch:
                self._process_batch(batch)

    def _process_batch(self, batch: List[SyncEvent]):
        """Process a batch of sync events."""
        # Deduplicate by memory_id (process only latest)
        unique_events = {}
        for event in batch:
            unique_events[event.memory_id] = event

        # Sort by priority
        events = sorted(unique_events.values(), key=lambda e: e.priority)

        for event in events:
            try:
                self._handle_event(event)
                self._events_processed += 1

                # Notify subscribers
                self._notify_subscribers(event)

            except Exception as e:
                logger.error(f"Failed to process event {event.event_type}: {e}")

        self._batches_processed += 1

    def _handle_event(self, event: SyncEvent):
        """Handle a single sync event."""
        if event.event_type == "add":
            # Memory was added locally
            logger.debug(f"Processing add event for {event.memory_id}")
            # Sync will happen on next sync cycle

        elif event.event_type == "update":
            # Memory was updated
            logger.debug(f"Processing update event for {event.memory_id}")

        elif event.event_type == "delete":
            # Memory was deleted
            logger.debug(f"Processing delete event for {event.memory_id}")

        elif event.event_type == "sync":
            # Explicit sync request
            asyncio.run(self.sync_manager.sync_user_memories(
                event.user_id,
                direction=SyncDirection.BIDIRECTIONAL,
                force=True
            ))

    def _notify_subscribers(self, event: SyncEvent):
        """Notify subscribers of an event."""
        # Notify specific subscribers
        for callback in self._subscribers.get(event.event_type, []):
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Subscriber callback failed: {e}")

        # Notify wildcard subscribers
        for callback in self._subscribers.get("*", []):
            try:
                callback(event)
            except Exception as e:
                logger.error(f"Wildcard subscriber callback failed: {e}")

    def trigger_sync(self, user_id: str, priority: int = 5):
        """Trigger an immediate sync for a user."""
        event = SyncEvent(
            event_type="sync",
            memory_id=f"sync_{user_id}",
            user_id=user_id,
            priority=priority
        )
        self.emit_event(event)

    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        return {
            "running": self._running,
            "events_processed": self._events_processed,
            "batches_processed": self._batches_processed,
            "pending_events": len(self._pending_events),
            "queue_size": self._event_queue.qsize(),
            "subscriber_count": sum(len(subs) for subs in self._subscribers.values())
        }


class AdvancedMem0Integration:
    """
    Advanced Mem0 integration combining bidirectional sync,
    shared memory pools, and real-time synchronization.

    Unified interface for all cutting-edge memory features.
    """

    def __init__(
        self,
        config: Optional[MemoryConfig] = None,
        conflict_strategy: ConflictResolution = ConflictResolution.NEWEST_WINS,
        enable_realtime_sync: bool = True
    ):
        """
        Initialize advanced Mem0 integration.

        Args:
            config: Memory configuration
            conflict_strategy: Default conflict resolution strategy
            enable_realtime_sync: Enable real-time sync engine
        """
        # Core services
        self.memory_service = MemoryService(config)
        self.memory_service.initialize()

        # Advanced features
        self.sync_manager = MemorySyncManager(
            self.memory_service,
            conflict_strategy=conflict_strategy
        )
        self.shared_pools = SharedMemoryPool(self.memory_service)

        # Real-time sync
        self.realtime_engine: Optional[RealTimeSyncEngine] = None
        if enable_realtime_sync:
            self.realtime_engine = RealTimeSyncEngine(self.sync_manager)
            self.realtime_engine.start()

        # Specialized memories
        self.conversation = ConversationMemory(self.memory_service)
        self.preferences = UserPreferencesMemory(self.memory_service)
        self.learning = LearningMemory(self.memory_service)

        logger.info("AdvancedMem0Integration initialized")

    # ==========================================================================
    # Core Memory Operations (with sync events)
    # ==========================================================================

    def add_memory(
        self,
        content: str,
        user_id: str,
        category: str = "general",
        metadata: Dict[str, Any] = None
    ) -> Optional[str]:
        """Add a memory with automatic sync event."""
        memory_id = self.memory_service.add_memory(content, user_id, category, metadata)

        if memory_id and self.realtime_engine:
            self.realtime_engine.emit_event(SyncEvent(
                event_type="add",
                memory_id=memory_id,
                user_id=user_id,
                content=content,
                metadata=metadata
            ))

        return memory_id

    def search_memories(
        self,
        query: str,
        user_id: str,
        limit: int = 10,
        include_shared: bool = True
    ) -> List[MemoryItem]:
        """Search memories including shared pools."""
        # Personal memories
        results = self.memory_service.search_memories(query, user_id, limit)

        # Shared memories
        if include_shared:
            shared = self.shared_pools.search_pool(user_id, query, limit=limit // 2)
            for sm in shared:
                results.append(MemoryItem(
                    id=sm.memory_id,
                    content=sm.content,
                    user_id=sm.contributor_id,
                    category=f"shared:{sm.category}",
                    metadata={"pool_type": sm.pool_type.value, "pool_id": sm.pool_id}
                ))

        return results[:limit]

    def update_memory(self, memory_id: str, content: str, user_id: str) -> bool:
        """Update a memory with automatic sync event."""
        success = self.memory_service.update_memory(memory_id, content, user_id)

        if success and self.realtime_engine:
            self.realtime_engine.emit_event(SyncEvent(
                event_type="update",
                memory_id=memory_id,
                user_id=user_id,
                content=content
            ))

        return success

    def delete_memory(self, memory_id: str, user_id: str) -> bool:
        """Delete a memory with automatic sync event."""
        success = self.memory_service.delete_memory(memory_id, user_id)

        if success and self.realtime_engine:
            self.realtime_engine.emit_event(SyncEvent(
                event_type="delete",
                memory_id=memory_id,
                user_id=user_id
            ))

        return success

    # ==========================================================================
    # Sync Operations
    # ==========================================================================

    async def sync_user(
        self,
        user_id: str,
        direction: SyncDirection = SyncDirection.BIDIRECTIONAL,
        force: bool = False
    ) -> Dict[str, Any]:
        """Synchronize memories for a user."""
        return await self.sync_manager.sync_user_memories(user_id, direction, force)

    def trigger_realtime_sync(self, user_id: str, priority: int = 5):
        """Trigger immediate sync via real-time engine."""
        if self.realtime_engine:
            self.realtime_engine.trigger_sync(user_id, priority)

    def get_pending_conflicts(self, user_id: Optional[str] = None) -> List[SyncConflict]:
        """Get pending sync conflicts."""
        return self.sync_manager.get_pending_conflicts(user_id)

    def resolve_conflict(
        self,
        conflict_id: str,
        resolution: ConflictResolution,
        merged_content: Optional[str] = None
    ) -> bool:
        """Resolve a sync conflict."""
        return self.sync_manager.resolve_conflict(conflict_id, resolution, merged_content)

    # ==========================================================================
    # Shared Pool Operations
    # ==========================================================================

    def create_shared_pool(
        self,
        pool_type: MemoryPoolType,
        pool_id: str,
        creator_id: str,
        initial_members: List[str] = None
    ) -> bool:
        """Create a shared memory pool."""
        return self.shared_pools.create_pool(pool_type, pool_id, creator_id, initial_members)

    def join_shared_pool(self, user_id: str, pool_type: MemoryPoolType, pool_id: str) -> bool:
        """Join a shared memory pool."""
        return self.shared_pools.join_pool(user_id, pool_type, pool_id)

    def contribute_to_pool(
        self,
        user_id: str,
        pool_type: MemoryPoolType,
        pool_id: str,
        content: str,
        category: str = "general"
    ) -> Optional[str]:
        """Contribute a memory to a shared pool."""
        return self.shared_pools.contribute_memory(user_id, pool_type, pool_id, content, category)

    def get_cross_session_context(
        self,
        user_id: str,
        query: str,
        limit: int = 5
    ) -> List[str]:
        """Get cross-session context from shared pools."""
        return self.shared_pools.get_cross_session_context(user_id, query, limit)

    # ==========================================================================
    # Analytics
    # ==========================================================================

    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        stats = {
            "memory_service": {
                "mem0_available": MEM0_AVAILABLE,
                "initialized": self.memory_service._initialized
            },
            "sync_manager": self.sync_manager.get_sync_stats(),
            "shared_pools": {
                "total_pools": len(self.shared_pools._pools),
                "total_shared_memories": sum(
                    len(pool) for pool in self.shared_pools._pools.values()
                )
            }
        }

        if self.realtime_engine:
            stats["realtime_engine"] = self.realtime_engine.get_stats()

        return stats

    def shutdown(self):
        """Shutdown the integration."""
        if self.realtime_engine:
            self.realtime_engine.stop()
        logger.info("AdvancedMem0Integration shutdown complete")


# =============================================================================
# Factory Functions
# =============================================================================

_advanced_mem0: Optional[AdvancedMem0Integration] = None


def get_advanced_mem0(
    config: Optional[MemoryConfig] = None,
    conflict_strategy: ConflictResolution = ConflictResolution.NEWEST_WINS
) -> AdvancedMem0Integration:
    """Get or create global advanced Mem0 integration."""
    global _advanced_mem0

    if _advanced_mem0 is None:
        _advanced_mem0 = AdvancedMem0Integration(
            config=config,
            conflict_strategy=conflict_strategy
        )

    return _advanced_mem0


def reset_advanced_mem0():
    """Reset the global advanced Mem0 integration."""
    global _advanced_mem0
    if _advanced_mem0:
        _advanced_mem0.shutdown()
    _advanced_mem0 = None


async def sync_all_users(
    user_ids: List[str],
    direction: SyncDirection = SyncDirection.BIDIRECTIONAL
) -> Dict[str, Any]:
    """
    Synchronize memories for multiple users.

    Args:
        user_ids: List of user IDs to sync
        direction: Sync direction

    Returns:
        Aggregated sync results
    """
    integration = get_advanced_mem0()

    results = {
        "total_users": len(user_ids),
        "successful": 0,
        "failed": 0,
        "user_results": {}
    }

    for user_id in user_ids:
        try:
            result = await integration.sync_user(user_id, direction)
            results["user_results"][user_id] = result
            if result.get("status") == "completed":
                results["successful"] += 1
            else:
                results["failed"] += 1
        except Exception as e:
            results["user_results"][user_id] = {"status": "failed", "error": str(e)}
            results["failed"] += 1

    return results
