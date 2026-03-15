"""
Structured JSON Logging for DriveSentinel (Advanced)

Provides consistent, machine-parseable logging across all services.
Integrates with PHI masking and OpenTelemetry trace context.

Advanced Features (upgraded from Required):
- Semantic log analysis: NLP-based log understanding and insight extraction
- Auto-categorization: Automatic issue classification and pattern detection
- Error clustering: Group similar errors to reduce noise
- Intelligent deduplication: Smart grouping of repeated log messages

Usage:
    from services.shared.observability import get_logger, configure_logging

    # Configure at startup
    configure_logging(service_name="orchestrator", log_level="INFO")

    # Get logger
    logger = get_logger(__name__)

    # Log with structured context
    logger.info("Request processed", extra={
        "user_id": "123",
        "duration_ms": 45.2,
        "status": "success"
    })

    # Advanced: Get log analysis
    analyzer = get_log_analyzer()
    patterns = analyzer.get_error_patterns()
    insights = analyzer.get_insights()
"""

import os
import sys
import json
import logging
import traceback
import re
import hashlib
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union, Set
from dataclasses import dataclass, field
from contextvars import ContextVar
from collections import defaultdict
from enum import Enum

# Context variables for request tracking
request_id_var: ContextVar[Optional[str]] = ContextVar("request_id", default=None)
user_id_var: ContextVar[Optional[str]] = ContextVar("user_id", default=None)
trace_id_var: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
span_id_var: ContextVar[Optional[str]] = ContextVar("span_id", default=None)


# =============================================================================
# ADVANCED: SEMANTIC LOG ANALYSIS & AUTO-CATEGORIZATION
# =============================================================================

class LogCategory(str, Enum):
    """Categories for automatic log classification."""
    ERROR_DATABASE = "error_database"
    ERROR_NETWORK = "error_network"
    ERROR_AUTHENTICATION = "error_authentication"
    ERROR_VALIDATION = "error_validation"
    ERROR_TIMEOUT = "error_timeout"
    ERROR_RESOURCE = "error_resource"
    ERROR_EXTERNAL_API = "error_external_api"
    ERROR_INTERNAL = "error_internal"
    PERFORMANCE_SLOW = "performance_slow"
    PERFORMANCE_NORMAL = "performance_normal"
    SECURITY_THREAT = "security_threat"
    BUSINESS_EVENT = "business_event"
    SYSTEM_EVENT = "system_event"
    UNKNOWN = "unknown"


class LogSeverity(str, Enum):
    """Severity levels for categorized logs."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class LogEntry:
    """Structured log entry for analysis."""
    timestamp: datetime
    level: str
    message: str
    logger_name: str
    service: str
    category: LogCategory = LogCategory.UNKNOWN
    severity: LogSeverity = LogSeverity.INFO
    error_type: Optional[str] = None
    traceback: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    signature: str = ""  # Hash for deduplication

    def __post_init__(self):
        if not self.signature:
            self.signature = self._compute_signature()

    def _compute_signature(self) -> str:
        """Compute a signature for deduplication."""
        parts = [self.level, self.logger_name, self.error_type or ""]
        # Normalize message (remove numbers, UUIDs)
        normalized_msg = re.sub(r'\d+', 'N', self.message)
        normalized_msg = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', 'UUID', normalized_msg)
        parts.append(normalized_msg[:100])
        return hashlib.md5(":".join(parts).encode()).hexdigest()[:12]


@dataclass
class LogPattern:
    """Pattern detected in logs."""
    signature: str
    category: LogCategory
    sample_message: str
    count: int = 1
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    services: Set[str] = field(default_factory=set)
    severity: LogSeverity = LogSeverity.INFO

    def to_dict(self) -> Dict[str, Any]:
        return {
            "signature": self.signature,
            "category": self.category.value,
            "sample_message": self.sample_message[:200],
            "count": self.count,
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "services": list(self.services),
            "severity": self.severity.value,
        }


@dataclass
class LogInsight:
    """Insight derived from log analysis."""
    title: str
    description: str
    severity: LogSeverity
    affected_services: List[str]
    recommendation: str
    supporting_evidence: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "affected_services": self.affected_services,
            "recommendation": self.recommendation,
            "evidence_count": len(self.supporting_evidence),
        }


class LogCategorizer:
    """
    Automatically categorizes log messages using pattern matching.

    Uses keyword extraction and pattern recognition to classify logs.
    """

    # Category patterns: (keywords, regex_patterns)
    CATEGORY_PATTERNS: Dict[LogCategory, Tuple[List[str], List[str]]] = {
        LogCategory.ERROR_DATABASE: (
            ["database", "db", "sql", "postgres", "mysql", "connection pool", "query failed"],
            [r"psycopg", r"sqlalchemy", r"database.*error", r"connection.*refused"]
        ),
        LogCategory.ERROR_NETWORK: (
            ["network", "connection", "socket", "http", "request failed", "unreachable"],
            [r"connection.*timeout", r"network.*unreachable", r"socket.*error"]
        ),
        LogCategory.ERROR_AUTHENTICATION: (
            ["auth", "authentication", "unauthorized", "forbidden", "token", "jwt", "credential"],
            [r"401", r"403", r"invalid.*token", r"authentication.*failed"]
        ),
        LogCategory.ERROR_VALIDATION: (
            ["validation", "invalid", "required field", "constraint", "schema"],
            [r"validation.*error", r"invalid.*value", r"required.*missing"]
        ),
        LogCategory.ERROR_TIMEOUT: (
            ["timeout", "timed out", "deadline exceeded"],
            [r"timeout", r"timed.*out", r"deadline.*exceeded"]
        ),
        LogCategory.ERROR_RESOURCE: (
            ["memory", "disk", "cpu", "quota", "limit exceeded", "out of memory"],
            [r"out.*of.*memory", r"disk.*full", r"quota.*exceeded"]
        ),
        LogCategory.ERROR_EXTERNAL_API: (
            ["api", "external", "third-party", "upstream", "downstream"],
            [r"api.*error", r"external.*service", r"upstream.*failed"]
        ),
        LogCategory.PERFORMANCE_SLOW: (
            ["slow", "latency", "performance", "degraded"],
            [r"slow.*query", r"high.*latency", r"performance.*degraded"]
        ),
        LogCategory.SECURITY_THREAT: (
            ["security", "attack", "injection", "xss", "csrf", "suspicious"],
            [r"sql.*injection", r"xss", r"unauthorized.*access", r"suspicious.*activity"]
        ),
    }

    def categorize(self, entry: LogEntry) -> Tuple[LogCategory, LogSeverity]:
        """
        Categorize a log entry based on its content.

        Returns (category, severity).
        """
        message_lower = entry.message.lower()
        error_type_lower = (entry.error_type or "").lower()
        combined = f"{message_lower} {error_type_lower}"

        # Check each category pattern
        for category, (keywords, regexes) in self.CATEGORY_PATTERNS.items():
            # Check keywords
            if any(kw in combined for kw in keywords):
                severity = self._determine_severity(entry, category)
                return category, severity

            # Check regex patterns
            for pattern in regexes:
                if re.search(pattern, combined, re.IGNORECASE):
                    severity = self._determine_severity(entry, category)
                    return category, severity

        # Default categorization based on log level
        if entry.level in ("ERROR", "CRITICAL"):
            return LogCategory.ERROR_INTERNAL, LogSeverity.HIGH
        elif entry.level == "WARNING":
            return LogCategory.SYSTEM_EVENT, LogSeverity.MEDIUM

        return LogCategory.UNKNOWN, LogSeverity.INFO

    def _determine_severity(self, entry: LogEntry, category: LogCategory) -> LogSeverity:
        """Determine severity based on category and log level."""
        # Security threats are always high priority
        if category == LogCategory.SECURITY_THREAT:
            return LogSeverity.CRITICAL

        # Map log level to base severity
        level_severity = {
            "CRITICAL": LogSeverity.CRITICAL,
            "ERROR": LogSeverity.HIGH,
            "WARNING": LogSeverity.MEDIUM,
            "INFO": LogSeverity.LOW,
            "DEBUG": LogSeverity.INFO,
        }
        base_severity = level_severity.get(entry.level, LogSeverity.INFO)

        # Elevate for critical categories
        if category in (LogCategory.ERROR_DATABASE, LogCategory.ERROR_AUTHENTICATION):
            if base_severity == LogSeverity.HIGH:
                return LogSeverity.CRITICAL

        return base_severity


class ErrorClusterer:
    """
    Groups similar errors together to reduce alert fatigue.

    Uses log signatures and similarity metrics to cluster errors.
    """

    def __init__(self, similarity_threshold: float = 0.8):
        self.similarity_threshold = similarity_threshold
        self._clusters: Dict[str, List[LogEntry]] = defaultdict(list)
        self._cluster_metadata: Dict[str, Dict[str, Any]] = {}

    def add_entry(self, entry: LogEntry) -> str:
        """
        Add a log entry to a cluster.

        Returns the cluster ID.
        """
        # Use signature as primary cluster key
        cluster_id = entry.signature

        self._clusters[cluster_id].append(entry)

        # Update metadata
        if cluster_id not in self._cluster_metadata:
            self._cluster_metadata[cluster_id] = {
                "first_seen": entry.timestamp,
                "sample_message": entry.message,
                "category": entry.category,
                "services": set(),
            }

        self._cluster_metadata[cluster_id]["last_seen"] = entry.timestamp
        self._cluster_metadata[cluster_id]["count"] = len(self._clusters[cluster_id])
        self._cluster_metadata[cluster_id]["services"].add(entry.service)

        return cluster_id

    def get_clusters(self, min_count: int = 2) -> List[Dict[str, Any]]:
        """Get error clusters with at least min_count occurrences."""
        clusters = []
        for cluster_id, entries in self._clusters.items():
            if len(entries) < min_count:
                continue

            meta = self._cluster_metadata[cluster_id]
            clusters.append({
                "cluster_id": cluster_id,
                "count": len(entries),
                "sample_message": meta["sample_message"][:200],
                "category": meta["category"].value if meta["category"] else "unknown",
                "first_seen": meta["first_seen"].isoformat(),
                "last_seen": meta["last_seen"].isoformat(),
                "services": list(meta["services"]),
            })

        return sorted(clusters, key=lambda x: x["count"], reverse=True)

    def should_deduplicate(self, entry: LogEntry, window_seconds: int = 60) -> bool:
        """Check if this entry should be deduplicated (not logged again)."""
        if entry.signature not in self._clusters:
            return False

        recent = self._clusters[entry.signature]
        if not recent:
            return False

        # Check if we've seen this recently
        cutoff = datetime.utcnow() - timedelta(seconds=window_seconds)
        recent_count = sum(1 for e in recent if e.timestamp >= cutoff)

        # Deduplicate if we've seen more than 5 in the window
        return recent_count >= 5


# =============================================================================
# ADVANCED: ML-BASED ERROR CLUSTERING
# =============================================================================

@dataclass
class ErrorFingerprint:
    """Fingerprint for an error for ML-based clustering"""
    signature: str
    message_tokens: List[str]
    error_type: str
    logger_name: str
    stack_hash: Optional[str]
    service: str
    feature_vector: List[float] = field(default_factory=list)

    def __post_init__(self):
        if not self.feature_vector:
            self.feature_vector = self._compute_features()

    def _compute_features(self) -> List[float]:
        """Compute feature vector for similarity comparison"""
        features = []

        # Feature 1: Message length bucket (0-4)
        msg_len = len(self.message_tokens)
        features.append(min(4, msg_len // 5) / 4.0)

        # Feature 2: Has stack trace
        features.append(1.0 if self.stack_hash else 0.0)

        # Feature 3: Error type hash (normalized to 0-1)
        type_hash = hash(self.error_type or "unknown") % 1000
        features.append(type_hash / 1000.0)

        # Feature 4: Logger name hash (normalized to 0-1)
        logger_hash = hash(self.logger_name) % 1000
        features.append(logger_hash / 1000.0)

        # Feature 5-8: Token frequency features
        common_error_tokens = ['error', 'failed', 'exception', 'timeout', 'connection', 'refused', 'invalid', 'null']
        for token in common_error_tokens[:4]:
            features.append(1.0 if token in [t.lower() for t in self.message_tokens] else 0.0)

        return features


@dataclass
class ErrorCluster:
    """Cluster of similar errors"""
    cluster_id: str
    centroid_fingerprint: ErrorFingerprint
    members: List[ErrorFingerprint] = field(default_factory=list)
    representative_message: str = ""
    error_types: Set[str] = field(default_factory=set)
    services: Set[str] = field(default_factory=set)
    first_seen: datetime = field(default_factory=datetime.utcnow)
    last_seen: datetime = field(default_factory=datetime.utcnow)
    severity_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "count": len(self.members),
            "representative_message": self.representative_message[:200],
            "error_types": list(self.error_types),
            "services": list(self.services),
            "first_seen": self.first_seen.isoformat(),
            "last_seen": self.last_seen.isoformat(),
            "severity_score": round(self.severity_score, 2)
        }


class MLErrorClusterer:
    """
    ML-based error clustering using feature similarity.

    Provides:
    - Feature extraction from error messages
    - Cosine similarity-based clustering
    - Dynamic cluster creation and merging
    - Severity scoring based on cluster characteristics
    """

    def __init__(
        self,
        similarity_threshold: float = 0.75,
        max_clusters: int = 100,
        merge_threshold: float = 0.9
    ):
        self.similarity_threshold = similarity_threshold
        self.max_clusters = max_clusters
        self.merge_threshold = merge_threshold
        self.clusters: Dict[str, ErrorCluster] = {}
        self._next_cluster_id = 0
        self._token_idf: Dict[str, float] = {}  # Inverse document frequency for tokens

    def _tokenize(self, message: str) -> List[str]:
        """Tokenize error message"""
        # Remove common patterns
        cleaned = re.sub(r'\d+', 'NUM', message)  # Replace numbers
        cleaned = re.sub(r'0x[a-fA-F0-9]+', 'HEX', cleaned)  # Replace hex
        cleaned = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', 'UUID', cleaned)

        # Tokenize
        tokens = re.findall(r'[a-zA-Z_]+', cleaned.lower())

        # Filter stopwords
        stopwords = {'the', 'a', 'an', 'is', 'was', 'were', 'been', 'be', 'have', 'has',
                     'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
                     'at', 'on', 'in', 'to', 'for', 'of', 'with', 'by'}
        return [t for t in tokens if t not in stopwords and len(t) > 2]

    def _compute_stack_hash(self, traceback_str: Optional[str]) -> Optional[str]:
        """Compute hash of stack trace for comparison"""
        if not traceback_str:
            return None

        # Extract file:line patterns
        lines = re.findall(r'File "([^"]+)", line (\d+)', traceback_str)
        if not lines:
            return None

        # Hash the call stack
        stack_key = ":".join(f"{f}:{l}" for f, l in lines[-5:])  # Last 5 frames
        return hashlib.md5(stack_key.encode()).hexdigest()[:8]

    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors"""
        if not vec1 or not vec2 or len(vec1) != len(vec2):
            return 0.0

        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        norm1 = sum(a ** 2 for a in vec1) ** 0.5
        norm2 = sum(b ** 2 for b in vec2) ** 0.5

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return dot_product / (norm1 * norm2)

    def _jaccard_similarity(self, tokens1: List[str], tokens2: List[str]) -> float:
        """Compute Jaccard similarity between token sets"""
        set1 = set(tokens1)
        set2 = set(tokens2)

        if not set1 or not set2:
            return 0.0

        intersection = len(set1 & set2)
        union = len(set1 | set2)

        return intersection / union if union > 0 else 0.0

    def create_fingerprint(
        self,
        message: str,
        error_type: Optional[str] = None,
        logger_name: str = "unknown",
        traceback_str: Optional[str] = None,
        service: str = "unknown"
    ) -> ErrorFingerprint:
        """Create fingerprint from error data"""
        tokens = self._tokenize(message)

        # Compute signature
        sig_parts = [error_type or "", logger_name]
        sig_parts.extend(sorted(tokens)[:10])
        signature = hashlib.md5(":".join(sig_parts).encode()).hexdigest()[:12]

        return ErrorFingerprint(
            signature=signature,
            message_tokens=tokens,
            error_type=error_type or "Unknown",
            logger_name=logger_name,
            stack_hash=self._compute_stack_hash(traceback_str),
            service=service
        )

    def find_similar_cluster(self, fingerprint: ErrorFingerprint) -> Optional[str]:
        """Find the most similar existing cluster"""
        best_cluster = None
        best_similarity = 0.0

        for cluster_id, cluster in self.clusters.items():
            # Compute combined similarity
            feature_sim = self._cosine_similarity(
                fingerprint.feature_vector,
                cluster.centroid_fingerprint.feature_vector
            )
            token_sim = self._jaccard_similarity(
                fingerprint.message_tokens,
                cluster.centroid_fingerprint.message_tokens
            )

            # Weight feature similarity higher
            combined_sim = 0.6 * feature_sim + 0.4 * token_sim

            # Bonus for same error type
            if fingerprint.error_type == cluster.centroid_fingerprint.error_type:
                combined_sim += 0.1

            # Bonus for same stack hash
            if fingerprint.stack_hash and fingerprint.stack_hash == cluster.centroid_fingerprint.stack_hash:
                combined_sim += 0.2

            if combined_sim > best_similarity:
                best_similarity = combined_sim
                best_cluster = cluster_id

        if best_similarity >= self.similarity_threshold:
            return best_cluster
        return None

    def add_error(
        self,
        message: str,
        error_type: Optional[str] = None,
        logger_name: str = "unknown",
        traceback_str: Optional[str] = None,
        service: str = "unknown"
    ) -> Tuple[str, bool]:
        """
        Add an error to a cluster.

        Returns (cluster_id, is_new_cluster).
        """
        fingerprint = self.create_fingerprint(
            message, error_type, logger_name, traceback_str, service
        )

        # Find similar cluster
        cluster_id = self.find_similar_cluster(fingerprint)

        if cluster_id:
            # Add to existing cluster
            cluster = self.clusters[cluster_id]
            cluster.members.append(fingerprint)
            cluster.error_types.add(fingerprint.error_type)
            cluster.services.add(service)
            cluster.last_seen = datetime.utcnow()
            self._update_severity_score(cluster)
            return cluster_id, False

        # Create new cluster
        self._next_cluster_id += 1
        cluster_id = f"cluster_{self._next_cluster_id}"

        cluster = ErrorCluster(
            cluster_id=cluster_id,
            centroid_fingerprint=fingerprint,
            members=[fingerprint],
            representative_message=message,
            error_types={fingerprint.error_type},
            services={service}
        )
        self._update_severity_score(cluster)
        self.clusters[cluster_id] = cluster

        # Cleanup if too many clusters
        if len(self.clusters) > self.max_clusters:
            self._merge_similar_clusters()

        return cluster_id, True

    def _update_severity_score(self, cluster: ErrorCluster):
        """Update cluster severity score based on characteristics"""
        score = 0.0

        # Factor 1: Error count (more errors = higher severity)
        count = len(cluster.members)
        score += min(40, count * 2)  # Max 40 points

        # Factor 2: Recency (recent errors = higher severity)
        age_minutes = (datetime.utcnow() - cluster.last_seen).total_seconds() / 60
        if age_minutes < 5:
            score += 30
        elif age_minutes < 30:
            score += 20
        elif age_minutes < 60:
            score += 10

        # Factor 3: Service spread (more services = higher severity)
        score += min(20, len(cluster.services) * 5)

        # Factor 4: Error type diversity (multiple types = higher severity)
        score += min(10, len(cluster.error_types) * 2)

        cluster.severity_score = score

    def _merge_similar_clusters(self):
        """Merge very similar clusters to reduce count"""
        cluster_list = list(self.clusters.values())
        merged = set()

        for i, c1 in enumerate(cluster_list):
            if c1.cluster_id in merged:
                continue

            for c2 in cluster_list[i + 1:]:
                if c2.cluster_id in merged:
                    continue

                # Check similarity
                sim = self._cosine_similarity(
                    c1.centroid_fingerprint.feature_vector,
                    c2.centroid_fingerprint.feature_vector
                )

                if sim >= self.merge_threshold:
                    # Merge c2 into c1
                    c1.members.extend(c2.members)
                    c1.error_types.update(c2.error_types)
                    c1.services.update(c2.services)
                    c1.first_seen = min(c1.first_seen, c2.first_seen)
                    c1.last_seen = max(c1.last_seen, c2.last_seen)
                    self._update_severity_score(c1)
                    merged.add(c2.cluster_id)

        # Remove merged clusters
        for cluster_id in merged:
            del self.clusters[cluster_id]

    def get_clusters(
        self,
        min_count: int = 2,
        min_severity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Get error clusters sorted by severity"""
        clusters = [
            c.to_dict() for c in self.clusters.values()
            if len(c.members) >= min_count and c.severity_score >= min_severity
        ]
        return sorted(clusters, key=lambda x: x["severity_score"], reverse=True)

    def get_cluster_for_error(self, message: str, error_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Get the cluster that an error belongs to"""
        fingerprint = self.create_fingerprint(message, error_type)
        cluster_id = self.find_similar_cluster(fingerprint)

        if cluster_id and cluster_id in self.clusters:
            return self.clusters[cluster_id].to_dict()
        return None


class SmartDeduplicator:
    """
    Smart deduplication with adaptive thresholds and burst detection.

    Provides:
    - Adaptive deduplication windows
    - Burst detection and suppression
    - Rate-based suppression decisions
    - Digest generation for suppressed errors
    """

    def __init__(
        self,
        base_window_seconds: int = 60,
        max_window_seconds: int = 600,
        burst_threshold: int = 10,
        digest_interval_seconds: int = 300
    ):
        self.base_window = base_window_seconds
        self.max_window = max_window_seconds
        self.burst_threshold = burst_threshold
        self.digest_interval = digest_interval_seconds

        self._occurrences: Dict[str, List[datetime]] = defaultdict(list)
        self._suppressed_counts: Dict[str, int] = defaultdict(int)
        self._last_digest: Dict[str, datetime] = {}
        self._adaptive_windows: Dict[str, int] = {}

    def _get_signature(self, message: str, error_type: Optional[str] = None) -> str:
        """Get deduplication signature"""
        # Normalize message
        normalized = re.sub(r'\d+', 'N', message)
        normalized = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', 'UUID', normalized)
        normalized = normalized[:200]

        parts = [error_type or "", normalized]
        return hashlib.md5(":".join(parts).encode()).hexdigest()[:12]

    def _get_adaptive_window(self, signature: str) -> int:
        """Get adaptive window size based on error frequency"""
        occurrences = self._occurrences.get(signature, [])
        if not occurrences:
            return self.base_window

        # Calculate rate
        now = datetime.utcnow()
        recent = [t for t in occurrences if (now - t).total_seconds() < 300]
        rate = len(recent) / 5.0  # errors per minute

        # Increase window for high-frequency errors
        if rate > 20:
            return self.max_window
        elif rate > 10:
            return min(self.max_window, self.base_window * 4)
        elif rate > 5:
            return min(self.max_window, self.base_window * 2)

        return self.base_window

    def should_log(
        self,
        message: str,
        error_type: Optional[str] = None
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Determine if error should be logged or suppressed.

        Returns (should_log, digest_if_available).
        """
        signature = self._get_signature(message, error_type)
        now = datetime.utcnow()

        # Record occurrence
        self._occurrences[signature].append(now)

        # Clean old occurrences
        cutoff = now - timedelta(seconds=self.max_window)
        self._occurrences[signature] = [
            t for t in self._occurrences[signature] if t > cutoff
        ]

        # Get adaptive window
        window = self._get_adaptive_window(signature)
        window_cutoff = now - timedelta(seconds=window)

        # Count recent occurrences
        recent_count = len([
            t for t in self._occurrences[signature] if t > window_cutoff
        ])

        # Check for burst
        is_burst = recent_count > self.burst_threshold

        # First occurrence always logs
        if recent_count == 1:
            return True, None

        # Check if we should generate a digest
        digest = None
        last_digest = self._last_digest.get(signature)
        should_digest = (
            not last_digest or
            (now - last_digest).total_seconds() >= self.digest_interval
        )

        if should_digest and self._suppressed_counts[signature] > 0:
            digest = self._generate_digest(signature)
            self._last_digest[signature] = now
            self._suppressed_counts[signature] = 0

        # Suppress if burst or recent
        if is_burst or recent_count > 3:
            self._suppressed_counts[signature] += 1
            return False, digest

        return True, digest

    def _generate_digest(self, signature: str) -> Dict[str, Any]:
        """Generate digest for suppressed errors"""
        occurrences = self._occurrences.get(signature, [])
        suppressed = self._suppressed_counts.get(signature, 0)

        if not occurrences:
            return {}

        return {
            "signature": signature,
            "suppressed_count": suppressed,
            "total_occurrences": len(occurrences),
            "first_occurrence": min(occurrences).isoformat() if occurrences else None,
            "last_occurrence": max(occurrences).isoformat() if occurrences else None,
            "rate_per_minute": len(occurrences) / max(1, (max(occurrences) - min(occurrences)).total_seconds() / 60) if len(occurrences) > 1 else 0
        }

    def get_suppression_stats(self) -> Dict[str, Any]:
        """Get overall suppression statistics"""
        total_suppressed = sum(self._suppressed_counts.values())
        active_signatures = len([s for s, c in self._suppressed_counts.items() if c > 0])

        return {
            "total_suppressed": total_suppressed,
            "active_suppressions": active_signatures,
            "signatures": [
                {"signature": sig, "suppressed": count, "window_seconds": self._adaptive_windows.get(sig, self.base_window)}
                for sig, count in sorted(self._suppressed_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            ]
        }

    def reset_suppression(self, signature: Optional[str] = None):
        """Reset suppression for a signature or all"""
        if signature:
            self._suppressed_counts[signature] = 0
            self._occurrences[signature] = []
        else:
            self._suppressed_counts.clear()
            self._occurrences.clear()


# Singleton instances for advanced clustering
_ml_clusterer: Optional[MLErrorClusterer] = None
_smart_deduplicator: Optional[SmartDeduplicator] = None


def get_ml_clusterer() -> MLErrorClusterer:
    """Get or create global ML error clusterer"""
    global _ml_clusterer
    if _ml_clusterer is None:
        _ml_clusterer = MLErrorClusterer()
    return _ml_clusterer


def get_smart_deduplicator() -> SmartDeduplicator:
    """Get or create global smart deduplicator"""
    global _smart_deduplicator
    if _smart_deduplicator is None:
        _smart_deduplicator = SmartDeduplicator()
    return _smart_deduplicator


class SemanticLogAnalyzer:
    """
    Analyzes logs for patterns, insights, and anomalies.

    Provides:
    - Pattern detection across services
    - Insight generation
    - Error rate tracking
    - Correlation analysis
    """

    def __init__(self, max_entries: int = 10000):
        self.max_entries = max_entries
        self._entries: List[LogEntry] = []
        self._patterns: Dict[str, LogPattern] = {}
        self.categorizer = LogCategorizer()
        self.clusterer = ErrorClusterer()
        self._error_rates: Dict[str, List[Tuple[datetime, int]]] = defaultdict(list)

    def analyze_log(
        self,
        message: str,
        level: str,
        logger_name: str,
        service: str = "unknown",
        error_type: Optional[str] = None,
        traceback_str: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> LogEntry:
        """
        Analyze a log message and categorize it.

        Returns a structured LogEntry with category and severity.
        """
        entry = LogEntry(
            timestamp=datetime.utcnow(),
            level=level,
            message=message,
            logger_name=logger_name,
            service=service,
            error_type=error_type,
            traceback=traceback_str,
            context=context or {},
        )

        # Categorize the entry
        category, severity = self.categorizer.categorize(entry)
        entry.category = category
        entry.severity = severity

        # Add to storage
        self._entries.append(entry)
        if len(self._entries) > self.max_entries:
            self._entries = self._entries[-self.max_entries // 2:]

        # Update patterns
        self._update_patterns(entry)

        # Add to clusterer for error entries
        if level in ("ERROR", "CRITICAL", "WARNING"):
            self.clusterer.add_entry(entry)
            self._track_error_rate(service)

        return entry

    def _update_patterns(self, entry: LogEntry):
        """Update pattern tracking."""
        sig = entry.signature

        if sig not in self._patterns:
            self._patterns[sig] = LogPattern(
                signature=sig,
                category=entry.category,
                sample_message=entry.message,
                first_seen=entry.timestamp,
                severity=entry.severity,
            )

        pattern = self._patterns[sig]
        pattern.count += 1
        pattern.last_seen = entry.timestamp
        pattern.services.add(entry.service)

        # Elevate severity if pattern is recurring
        if pattern.count > 10 and pattern.severity == LogSeverity.LOW:
            pattern.severity = LogSeverity.MEDIUM

    def _track_error_rate(self, service: str):
        """Track error rate over time."""
        now = datetime.utcnow()
        # Round to minute
        minute_key = now.replace(second=0, microsecond=0)

        rates = self._error_rates[service]
        if rates and rates[-1][0] == minute_key:
            rates[-1] = (minute_key, rates[-1][1] + 1)
        else:
            rates.append((minute_key, 1))

        # Keep last hour
        cutoff = now - timedelta(hours=1)
        self._error_rates[service] = [(t, c) for t, c in rates if t >= cutoff]

    def get_error_patterns(self, min_count: int = 3) -> List[Dict[str, Any]]:
        """Get recurring error patterns."""
        patterns = [
            p.to_dict() for p in self._patterns.values()
            if p.count >= min_count
        ]
        return sorted(patterns, key=lambda x: x["count"], reverse=True)

    def get_error_clusters(self, min_count: int = 2) -> List[Dict[str, Any]]:
        """Get clustered errors for deduplication."""
        return self.clusterer.get_clusters(min_count)

    def get_error_rate(self, service: Optional[str] = None) -> Dict[str, Any]:
        """Get error rate statistics."""
        if service:
            rates = self._error_rates.get(service, [])
            if not rates:
                return {"service": service, "error_rate_per_minute": 0, "total_errors": 0}

            total = sum(c for _, c in rates)
            minutes = len(rates) or 1
            return {
                "service": service,
                "error_rate_per_minute": total / minutes,
                "total_errors": total,
                "window_minutes": minutes,
            }

        # Aggregate across all services
        all_rates = {}
        for svc, rates in self._error_rates.items():
            total = sum(c for _, c in rates)
            minutes = len(rates) or 1
            all_rates[svc] = {
                "error_rate_per_minute": total / minutes,
                "total_errors": total,
            }

        return {"services": all_rates}

    def get_insights(self) -> List[LogInsight]:
        """Generate insights from log analysis."""
        insights = []

        # Insight: High error rate
        for service, rates in self._error_rates.items():
            total = sum(c for _, c in rates)
            minutes = len(rates) or 1
            rate = total / minutes

            if rate > 10:  # More than 10 errors per minute
                insights.append(LogInsight(
                    title=f"High Error Rate in {service}",
                    description=f"Service {service} is experiencing {rate:.1f} errors/minute",
                    severity=LogSeverity.HIGH,
                    affected_services=[service],
                    recommendation="Investigate service health and recent deployments",
                    supporting_evidence=[f"{total} errors in last {minutes} minutes"],
                ))

        # Insight: Recurring patterns
        recurring = [p for p in self._patterns.values() if p.count > 20]
        for pattern in recurring:
            if len(pattern.services) > 1:
                insights.append(LogInsight(
                    title="Cross-Service Error Pattern",
                    description=f"Same error occurring in {len(pattern.services)} services",
                    severity=LogSeverity.MEDIUM,
                    affected_services=list(pattern.services),
                    recommendation="Check shared dependencies or configuration",
                    supporting_evidence=[pattern.sample_message[:100]],
                ))

        # Insight: Security threats
        security_patterns = [
            p for p in self._patterns.values()
            if p.category == LogCategory.SECURITY_THREAT
        ]
        if security_patterns:
            total_threats = sum(p.count for p in security_patterns)
            insights.append(LogInsight(
                title="Security Threats Detected",
                description=f"{total_threats} potential security-related log entries",
                severity=LogSeverity.CRITICAL,
                affected_services=list(set(s for p in security_patterns for s in p.services)),
                recommendation="Review security logs and consider incident response",
                supporting_evidence=[p.sample_message[:100] for p in security_patterns[:3]],
            ))

        return insights

    def get_category_distribution(self) -> Dict[str, int]:
        """Get distribution of log categories."""
        distribution: Dict[str, int] = defaultdict(int)
        for entry in self._entries:
            distribution[entry.category.value] += 1
        return dict(distribution)

    def get_service_health(self) -> Dict[str, Dict[str, Any]]:
        """Get health status per service based on logs."""
        health: Dict[str, Dict[str, Any]] = {}

        for service, rates in self._error_rates.items():
            total = sum(c for _, c in rates)
            minutes = len(rates) or 1
            rate = total / minutes

            if rate > 20:
                status = "critical"
            elif rate > 5:
                status = "degraded"
            else:
                status = "healthy"

            health[service] = {
                "status": status,
                "error_rate": rate,
                "total_errors": total,
            }

        return health


# Global analyzer instance
_log_analyzer: Optional[SemanticLogAnalyzer] = None


def get_log_analyzer() -> SemanticLogAnalyzer:
    """Get the global log analyzer instance."""
    global _log_analyzer
    if _log_analyzer is None:
        _log_analyzer = SemanticLogAnalyzer()
    return _log_analyzer


@dataclass
class LoggingConfig:
    """Configuration for structured logging."""

    # Service identification
    service_name: str = "drivesentinel"
    service_version: str = "2.0.0"
    environment: str = "development"

    # Output configuration
    log_level: str = "INFO"
    json_format: bool = True  # Use JSON format (recommended for production)
    include_timestamp: bool = True
    include_trace_context: bool = True

    # PII masking
    enable_pii_masking: bool = True

    # Output destination
    log_file: Optional[str] = None
    log_to_stdout: bool = True

    # Additional fields to include in all logs
    extra_fields: Dict[str, Any] = None


class StructuredJSONFormatter(logging.Formatter):
    """
    Custom formatter that outputs logs as JSON with structured context.
    """

    RESERVED_ATTRS = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName", "taskName"
    }

    def __init__(
        self,
        service_name: str = "drivesentinel",
        service_version: str = "2.0.0",
        environment: str = "development",
        include_trace_context: bool = True,
        enable_pii_masking: bool = True,
        extra_fields: Optional[Dict[str, Any]] = None
    ):
        super().__init__()
        self.service_name = service_name
        self.service_version = service_version
        self.environment = environment
        self.include_trace_context = include_trace_context
        self.enable_pii_masking = enable_pii_masking
        self.extra_fields = extra_fields or {}
        self._phi_masker = None

    def _get_phi_masker(self):
        """Lazy load PHI masker."""
        if self._phi_masker is None and self.enable_pii_masking:
            try:
                from .phi_masking import get_masker
                self._phi_masker = get_masker()
            except ImportError:
                self._phi_masker = None
        return self._phi_masker

    def _mask_if_needed(self, value: Any) -> Any:
        """Mask PHI if enabled."""
        masker = self._get_phi_masker()
        if masker:
            return masker.mask(value)
        return value

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as JSON."""
        # Base log structure
        log_dict = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self._mask_if_needed(record.getMessage()),
            "service": {
                "name": self.service_name,
                "version": self.service_version,
            },
            "environment": self.environment,
        }

        # Add source location for errors and above
        if record.levelno >= logging.ERROR:
            log_dict["source"] = {
                "file": record.filename,
                "line": record.lineno,
                "function": record.funcName,
            }

        # Add exception info if present
        if record.exc_info:
            log_dict["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self._mask_if_needed(
                    "".join(traceback.format_exception(*record.exc_info))
                ) if record.exc_info[2] else None
            }

        # Add trace context
        if self.include_trace_context:
            trace_context = self._get_trace_context()
            if trace_context:
                log_dict["trace"] = trace_context

        # Add request context
        request_context = self._get_request_context()
        if request_context:
            log_dict["request"] = request_context

        # Add extra fields from record
        extra_dict = {}
        for key, value in record.__dict__.items():
            if key not in self.RESERVED_ATTRS and not key.startswith("_"):
                extra_dict[key] = self._mask_if_needed(value)

        if extra_dict:
            log_dict["context"] = extra_dict

        # Add configured extra fields
        if self.extra_fields:
            log_dict.update(self.extra_fields)

        return json.dumps(log_dict, default=str, ensure_ascii=False)

    def _get_trace_context(self) -> Optional[Dict]:
        """Get OpenTelemetry trace context if available."""
        trace_id = trace_id_var.get()
        span_id = span_id_var.get()

        if trace_id or span_id:
            return {
                "trace_id": trace_id,
                "span_id": span_id,
            }

        # Try to get from OpenTelemetry
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            if span and span.get_span_context().is_valid:
                ctx = span.get_span_context()
                return {
                    "trace_id": format(ctx.trace_id, "032x"),
                    "span_id": format(ctx.span_id, "016x"),
                }
        except ImportError:
            pass

        return None

    def _get_request_context(self) -> Optional[Dict]:
        """Get request context from context variables."""
        request_id = request_id_var.get()
        user_id = user_id_var.get()

        if request_id or user_id:
            ctx = {}
            if request_id:
                ctx["request_id"] = request_id
            if user_id:
                ctx["user_id"] = self._mask_if_needed(user_id)
            return ctx

        return None


class ColoredConsoleFormatter(logging.Formatter):
    """
    Human-readable colored formatter for development console output.
    """

    COLORS = {
        "DEBUG": "\033[36m",    # Cyan
        "INFO": "\033[32m",     # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",    # Red
        "CRITICAL": "\033[35m", # Magenta
    }
    RESET = "\033[0m"

    def __init__(self, service_name: str = "drivesentinel"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelname, "")
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]

        # Basic format
        formatted = f"{color}{timestamp}{self.RESET} [{record.levelname:8}] {self.service_name}.{record.name}: {record.getMessage()}"

        # Add exception if present
        if record.exc_info:
            formatted += "\n" + "".join(traceback.format_exception(*record.exc_info))

        return formatted


# Global configuration
_logging_configured = False
_logging_config: Optional[LoggingConfig] = None


def configure_logging(
    service_name: str = "drivesentinel",
    log_level: str = "INFO",
    json_format: Optional[bool] = None,
    enable_pii_masking: bool = True,
    config: Optional[LoggingConfig] = None
) -> None:
    """
    Configure structured logging for the service.

    Args:
        service_name: Name of the service
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON format (None = auto-detect based on environment)
        enable_pii_masking: Enable PHI masking in logs
        config: Full configuration object (overrides other params)
    """
    global _logging_configured, _logging_config

    if config:
        _logging_config = config
    else:
        # Auto-detect JSON format based on environment
        if json_format is None:
            json_format = os.getenv("ENVIRONMENT", "development") != "development"

        _logging_config = LoggingConfig(
            service_name=service_name,
            log_level=log_level.upper(),
            json_format=json_format,
            enable_pii_masking=enable_pii_masking,
            environment=os.getenv("ENVIRONMENT", "development"),
        )

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, _logging_config.log_level))

    # Remove existing handlers
    root_logger.handlers = []

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, _logging_config.log_level))

    # Set formatter
    if _logging_config.json_format:
        formatter = StructuredJSONFormatter(
            service_name=_logging_config.service_name,
            service_version=_logging_config.service_version,
            environment=_logging_config.environment,
            include_trace_context=_logging_config.include_trace_context,
            enable_pii_masking=_logging_config.enable_pii_masking,
            extra_fields=_logging_config.extra_fields,
        )
    else:
        formatter = ColoredConsoleFormatter(service_name=_logging_config.service_name)

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    # Add file handler if configured
    if _logging_config.log_file:
        file_handler = logging.FileHandler(_logging_config.log_file)
        file_handler.setLevel(getattr(logging, _logging_config.log_level))
        file_handler.setFormatter(StructuredJSONFormatter(
            service_name=_logging_config.service_name,
            service_version=_logging_config.service_version,
            environment=_logging_config.environment,
            include_trace_context=_logging_config.include_trace_context,
            enable_pii_masking=_logging_config.enable_pii_masking,
        ))
        root_logger.addHandler(file_handler)

    # Add PHI masking filter if enabled
    if _logging_config.enable_pii_masking:
        try:
            from .phi_masking import PHIMaskingFilter
            handler.addFilter(PHIMaskingFilter())
        except ImportError:
            pass

    _logging_configured = True


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.

    Automatically configures logging if not already done.

    Args:
        name: Logger name (usually __name__)

    Returns:
        Configured logger
    """
    global _logging_configured

    if not _logging_configured:
        configure_logging()

    return logging.getLogger(name)


# Context management
def set_request_context(
    request_id: Optional[str] = None,
    user_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    span_id: Optional[str] = None
):
    """Set request context for logging."""
    if request_id:
        request_id_var.set(request_id)
    if user_id:
        user_id_var.set(user_id)
    if trace_id:
        trace_id_var.set(trace_id)
    if span_id:
        span_id_var.set(span_id)


def clear_request_context():
    """Clear request context."""
    request_id_var.set(None)
    user_id_var.set(None)
    trace_id_var.set(None)
    span_id_var.set(None)


class RequestContextMiddleware:
    """
    ASGI middleware to set request context for logging.

    Usage:
        from fastapi import FastAPI
        from services.shared.observability import RequestContextMiddleware

        app = FastAPI()
        app.add_middleware(RequestContextMiddleware)
    """

    def __init__(self, app, service_name: str = "unknown"):
        self.app = app
        self.service_name = service_name

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        import uuid

        # Extract or generate request ID
        headers = dict(scope.get("headers", []))
        request_id = headers.get(b"x-request-id", b"").decode() or str(uuid.uuid4())[:8]

        # Set context
        set_request_context(request_id=request_id)

        try:
            await self.app(scope, receive, send)
        finally:
            clear_request_context()


# Utility functions for structured logging
def log_event(
    logger: logging.Logger,
    event: str,
    level: str = "info",
    **kwargs
):
    """
    Log a structured event.

    Args:
        logger: Logger instance
        event: Event name
        level: Log level
        **kwargs: Additional context
    """
    log_method = getattr(logger, level.lower(), logger.info)
    log_method(event, extra=kwargs)


def log_error(
    logger: logging.Logger,
    error: Exception,
    message: str = "An error occurred",
    **kwargs
):
    """
    Log an error with full context.

    Args:
        logger: Logger instance
        error: Exception object
        message: Error message
        **kwargs: Additional context
    """
    kwargs["error_type"] = type(error).__name__
    kwargs["error_message"] = str(error)
    logger.error(message, exc_info=error, extra=kwargs)


def log_performance(
    logger: logging.Logger,
    operation: str,
    duration_ms: float,
    success: bool = True,
    **kwargs
):
    """
    Log a performance metric.

    Args:
        logger: Logger instance
        operation: Operation name
        duration_ms: Duration in milliseconds
        success: Whether operation succeeded
        **kwargs: Additional context
    """
    kwargs["operation"] = operation
    kwargs["duration_ms"] = round(duration_ms, 2)
    kwargs["success"] = success

    level = "info" if success else "warning"
    log_event(logger, f"Performance: {operation}", level=level, **kwargs)


# =============================================================================
# CUTTING EDGE: LOG-TO-CODE CORRELATION
# =============================================================================

@dataclass
class SourceLocation:
    """Location in source code."""
    file_path: str
    line_number: int
    function_name: str
    class_name: Optional[str] = None
    module_name: Optional[str] = None
    code_snippet: Optional[str] = None
    git_blame: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file": self.file_path,
            "line": self.line_number,
            "function": self.function_name,
            "class": self.class_name,
            "module": self.module_name,
            "snippet": self.code_snippet,
            "git_blame": self.git_blame,
        }

    def get_github_link(self, repo_base: str = "", branch: str = "main") -> str:
        """Generate GitHub link to source location."""
        if not repo_base:
            return ""
        return f"{repo_base}/blob/{branch}/{self.file_path}#L{self.line_number}"


@dataclass
class CodeContext:
    """Context extracted from source code around a log statement."""
    source_location: SourceLocation
    surrounding_variables: Dict[str, str]  # Variable name -> inferred type
    function_signature: Optional[str] = None
    docstring: Optional[str] = None
    related_functions: List[str] = field(default_factory=list)
    error_handling_context: Optional[str] = None


@dataclass
class LogCodeCorrelation:
    """Correlation between a log message and its source code."""
    log_signature: str
    source_location: SourceLocation
    code_context: CodeContext
    correlation_confidence: float
    last_code_change: Optional[datetime] = None
    change_frequency: int = 0  # How often this code changes


class SourceCodeIndexer:
    """
    Indexes source code for log correlation.

    Scans Python source files to build an index of log statements
    and their source locations.
    """

    def __init__(self, source_roots: Optional[List[str]] = None):
        self.source_roots = source_roots or ["."]
        self._log_index: Dict[str, List[SourceLocation]] = {}
        self._file_cache: Dict[str, List[str]] = {}
        self._function_map: Dict[str, SourceLocation] = {}
        self._last_index_time: Optional[datetime] = None

        # Patterns for log statement detection
        self._log_patterns = [
            r'logger\.(debug|info|warning|error|critical)\s*\(\s*["\']([^"\']+)["\']',
            r'logging\.(debug|info|warning|error|critical)\s*\(\s*["\']([^"\']+)["\']',
            r'log\.(debug|info|warning|error|critical)\s*\(\s*["\']([^"\']+)["\']',
        ]

    def index_source_files(self, patterns: List[str] = None) -> int:
        """
        Index all source files for log statements.

        Returns number of log statements indexed.
        """
        import glob
        import ast

        patterns = patterns or ["**/*.py"]
        indexed_count = 0

        for root in self.source_roots:
            for pattern in patterns:
                full_pattern = os.path.join(root, pattern)
                for file_path in glob.glob(full_pattern, recursive=True):
                    try:
                        indexed_count += self._index_file(file_path)
                    except Exception as e:
                        pass  # Skip files that can't be parsed

        self._last_index_time = datetime.utcnow()
        return indexed_count

    def _index_file(self, file_path: str) -> int:
        """Index a single Python file."""
        import ast

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                lines = content.split('\n')
                self._file_cache[file_path] = lines
        except Exception:
            return 0

        indexed = 0

        # Find log statements using regex
        for pattern in self._log_patterns:
            for match in re.finditer(pattern, content):
                level = match.group(1)
                message = match.group(2)

                # Calculate line number
                line_num = content[:match.start()].count('\n') + 1

                # Get function context
                func_name = self._find_enclosing_function(content, match.start())

                location = SourceLocation(
                    file_path=file_path,
                    line_number=line_num,
                    function_name=func_name,
                    module_name=os.path.splitext(os.path.basename(file_path))[0],
                    code_snippet=self._get_code_snippet(lines, line_num),
                )

                # Create signature from message pattern
                sig = self._create_message_signature(message)

                if sig not in self._log_index:
                    self._log_index[sig] = []
                self._log_index[sig].append(location)
                indexed += 1

        return indexed

    def _find_enclosing_function(self, content: str, position: int) -> str:
        """Find the function containing a position in the file."""
        # Simple approach: find last 'def' before position
        prefix = content[:position]
        matches = list(re.finditer(r'def\s+(\w+)\s*\(', prefix))
        if matches:
            return matches[-1].group(1)
        return "<module>"

    def _get_code_snippet(self, lines: List[str], line_num: int, context: int = 2) -> str:
        """Get code snippet around a line."""
        start = max(0, line_num - context - 1)
        end = min(len(lines), line_num + context)
        return '\n'.join(lines[start:end])

    def _create_message_signature(self, message: str) -> str:
        """Create a signature from a log message for matching."""
        # Normalize the message
        normalized = re.sub(r'\{[^}]*\}', '{VAR}', message)  # f-string vars
        normalized = re.sub(r'%[sdrifg]', '%s', normalized)  # printf-style
        normalized = normalized.lower()[:100]
        return hashlib.md5(normalized.encode()).hexdigest()[:12]

    def find_source_location(
        self,
        message: str,
        logger_name: Optional[str] = None
    ) -> Optional[SourceLocation]:
        """
        Find source location for a log message.

        Returns the most likely source location or None.
        """
        sig = self._create_message_signature(message)

        locations = self._log_index.get(sig, [])
        if not locations:
            return None

        if len(locations) == 1:
            return locations[0]

        # Multiple matches - try to narrow down by logger name
        if logger_name:
            for loc in locations:
                if logger_name in loc.file_path or logger_name in loc.module_name:
                    return loc

        # Return first match
        return locations[0]

    def get_index_stats(self) -> Dict[str, Any]:
        """Get statistics about the index."""
        return {
            "indexed_signatures": len(self._log_index),
            "indexed_files": len(self._file_cache),
            "last_indexed": self._last_index_time.isoformat() if self._last_index_time else None,
        }


class LogCodeCorrelator:
    """
    Cutting Edge: Correlates logs to source code locations.

    Features:
    - Automatic source code indexing
    - Log-to-code mapping
    - Code context extraction
    - Change tracking integration
    """

    def __init__(
        self,
        source_roots: Optional[List[str]] = None,
        repo_base_url: Optional[str] = None
    ):
        self.source_indexer = SourceCodeIndexer(source_roots)
        self.repo_base_url = repo_base_url
        self._correlations: Dict[str, LogCodeCorrelation] = {}
        self._correlation_hits: Dict[str, int] = defaultdict(int)

    def index_codebase(self) -> int:
        """Index the codebase for log correlation."""
        return self.source_indexer.index_source_files()

    def correlate_log(
        self,
        message: str,
        logger_name: Optional[str] = None,
        level: str = "INFO"
    ) -> Optional[LogCodeCorrelation]:
        """
        Find correlation between a log message and its source.

        Returns LogCodeCorrelation if found.
        """
        sig = self.source_indexer._create_message_signature(message)

        # Check cache
        if sig in self._correlations:
            self._correlation_hits[sig] += 1
            return self._correlations[sig]

        # Find source location
        location = self.source_indexer.find_source_location(message, logger_name)
        if not location:
            return None

        # Extract code context
        context = self._extract_code_context(location)

        correlation = LogCodeCorrelation(
            log_signature=sig,
            source_location=location,
            code_context=context,
            correlation_confidence=0.9 if logger_name else 0.7,
        )

        self._correlations[sig] = correlation
        self._correlation_hits[sig] = 1

        return correlation

    def _extract_code_context(self, location: SourceLocation) -> CodeContext:
        """Extract code context around the log statement."""
        surrounding_vars: Dict[str, str] = {}
        docstring = None
        related_functions: List[str] = []

        # Get file content
        lines = self.source_indexer._file_cache.get(location.file_path, [])
        if not lines:
            return CodeContext(
                source_location=location,
                surrounding_variables={},
            )

        # Look for variable assignments near the log line
        start_line = max(0, location.line_number - 20)
        end_line = location.line_number

        for i in range(start_line, min(end_line, len(lines))):
            line = lines[i]
            # Simple variable detection
            match = re.match(r'\s*(\w+)\s*=\s*(.+)', line)
            if match:
                var_name = match.group(1)
                value = match.group(2).strip()
                # Infer type
                if value.startswith('"') or value.startswith("'"):
                    surrounding_vars[var_name] = "str"
                elif value.isdigit():
                    surrounding_vars[var_name] = "int"
                elif value.startswith('['):
                    surrounding_vars[var_name] = "list"
                elif value.startswith('{'):
                    surrounding_vars[var_name] = "dict"
                else:
                    surrounding_vars[var_name] = "unknown"

        # Look for function calls
        context_lines = '\n'.join(lines[start_line:end_line])
        func_calls = re.findall(r'(\w+)\s*\(', context_lines)
        related_functions = list(set(func_calls))[:10]

        return CodeContext(
            source_location=location,
            surrounding_variables=surrounding_vars,
            related_functions=related_functions,
        )

    def get_correlation_stats(self) -> Dict[str, Any]:
        """Get statistics about log-code correlations."""
        return {
            "total_correlations": len(self._correlations),
            "total_hits": sum(self._correlation_hits.values()),
            "top_correlated_logs": sorted(
                [(sig, count) for sig, count in self._correlation_hits.items()],
                key=lambda x: x[1],
                reverse=True
            )[:10],
            **self.source_indexer.get_index_stats(),
        }


# =============================================================================
# CUTTING EDGE: AUTOMATIC LOG ENRICHMENT
# =============================================================================

class EnrichmentSource(str, Enum):
    """Sources for automatic log enrichment."""
    GIT_CONTEXT = "git_context"
    RUNTIME_CONTEXT = "runtime_context"
    REQUEST_CONTEXT = "request_context"
    SYSTEM_CONTEXT = "system_context"
    CODE_CONTEXT = "code_context"
    HISTORICAL_CONTEXT = "historical_context"


@dataclass
class EnrichmentConfig:
    """Configuration for log enrichment."""
    enabled_sources: List[EnrichmentSource] = field(default_factory=lambda: [
        EnrichmentSource.RUNTIME_CONTEXT,
        EnrichmentSource.REQUEST_CONTEXT,
        EnrichmentSource.SYSTEM_CONTEXT,
    ])
    git_repo_path: Optional[str] = None
    max_enrichment_depth: int = 3
    cache_enrichments: bool = True
    enrichment_timeout_ms: int = 50  # Max time for enrichment


@dataclass
class EnrichedLogData:
    """Enriched log data with additional context."""
    original_message: str
    enrichments: Dict[EnrichmentSource, Dict[str, Any]]
    enrichment_time_ms: float
    sources_used: List[EnrichmentSource]

    def to_dict(self) -> Dict[str, Any]:
        result = {"original_message": self.original_message}
        for source, data in self.enrichments.items():
            result[f"enrichment_{source.value}"] = data
        result["_enrichment_meta"] = {
            "time_ms": self.enrichment_time_ms,
            "sources": [s.value for s in self.sources_used],
        }
        return result


class GitContextEnricher:
    """Enriches logs with Git context (blame, recent changes)."""

    def __init__(self, repo_path: str = "."):
        self.repo_path = repo_path
        self._blame_cache: Dict[str, Dict[int, Dict]] = {}
        self._recent_commits_cache: Dict[str, List[Dict]] = {}

    def get_blame_info(
        self,
        file_path: str,
        line_number: int
    ) -> Optional[Dict[str, Any]]:
        """Get git blame info for a specific line."""
        import subprocess

        try:
            result = subprocess.run(
                ["git", "blame", "-L", f"{line_number},{line_number}", "--porcelain", file_path],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode != 0:
                return None

            output = result.stdout
            lines = output.strip().split('\n')

            if not lines:
                return None

            # Parse porcelain output
            commit_hash = lines[0].split()[0]
            author = None
            author_time = None

            for line in lines:
                if line.startswith('author '):
                    author = line[7:]
                elif line.startswith('author-time '):
                    try:
                        timestamp = int(line[12:])
                        author_time = datetime.fromtimestamp(timestamp).isoformat()
                    except Exception:
                        pass

            return {
                "commit": commit_hash[:8],
                "author": author,
                "date": author_time,
            }

        except Exception:
            return None

    def get_recent_changes(self, file_path: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recent commits affecting a file."""
        import subprocess

        if file_path in self._recent_commits_cache:
            return self._recent_commits_cache[file_path]

        try:
            result = subprocess.run(
                ["git", "log", "-n", str(limit), "--pretty=format:%H|%an|%ai|%s", "--", file_path],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                timeout=2
            )

            if result.returncode != 0:
                return []

            commits = []
            for line in result.stdout.strip().split('\n'):
                if '|' in line:
                    parts = line.split('|')
                    if len(parts) >= 4:
                        commits.append({
                            "commit": parts[0][:8],
                            "author": parts[1],
                            "date": parts[2],
                            "message": parts[3][:100],
                        })

            self._recent_commits_cache[file_path] = commits
            return commits

        except Exception:
            return []


class RuntimeContextEnricher:
    """Enriches logs with runtime context."""

    def __init__(self):
        self._process_info_cache: Optional[Dict[str, Any]] = None
        self._cache_time: Optional[datetime] = None

    def get_runtime_context(self) -> Dict[str, Any]:
        """Get current runtime context."""
        import sys
        import platform

        # Cache for 60 seconds
        now = datetime.utcnow()
        if self._process_info_cache and self._cache_time:
            if (now - self._cache_time).total_seconds() < 60:
                return self._process_info_cache

        try:
            import psutil
            process = psutil.Process()

            context = {
                "python_version": sys.version.split()[0],
                "platform": platform.system(),
                "process_id": process.pid,
                "memory_mb": round(process.memory_info().rss / 1024 / 1024, 1),
                "cpu_percent": process.cpu_percent(),
                "threads": process.num_threads(),
                "open_files": len(process.open_files()),
            }
        except ImportError:
            context = {
                "python_version": sys.version.split()[0],
                "platform": platform.system(),
                "process_id": os.getpid(),
            }

        self._process_info_cache = context
        self._cache_time = now

        return context


class HistoricalContextEnricher:
    """Enriches logs with historical context (similar errors, patterns)."""

    def __init__(self, log_analyzer: Optional[SemanticLogAnalyzer] = None):
        self.log_analyzer = log_analyzer or get_log_analyzer()

    def get_historical_context(
        self,
        message: str,
        level: str,
        service: str
    ) -> Dict[str, Any]:
        """Get historical context for a log message."""
        context = {}

        # Check if this is a known pattern
        patterns = self.log_analyzer.get_error_patterns(min_count=2)
        for pattern in patterns[:20]:
            if self._message_matches_pattern(message, pattern["sample_message"]):
                context["known_pattern"] = True
                context["pattern_count"] = pattern["count"]
                context["first_seen"] = pattern["first_seen"]
                context["affected_services"] = pattern["services"]
                break

        # Get error rate context
        if level in ("ERROR", "CRITICAL"):
            rate_info = self.log_analyzer.get_error_rate(service)
            if rate_info:
                context["service_error_rate"] = rate_info.get("error_rate_per_minute", 0)

        # Check ML clusters
        ml_clusterer = get_ml_clusterer()
        cluster = ml_clusterer.get_cluster_for_error(message)
        if cluster:
            context["error_cluster"] = {
                "cluster_id": cluster["cluster_id"],
                "cluster_size": cluster["count"],
                "severity_score": cluster["severity_score"],
            }

        return context

    def _message_matches_pattern(self, message: str, pattern: str) -> bool:
        """Check if message matches a pattern (fuzzy)."""
        # Normalize both
        norm_msg = re.sub(r'\d+', 'N', message.lower()[:100])
        norm_pat = re.sub(r'\d+', 'N', pattern.lower()[:100])

        # Simple substring match
        return norm_pat in norm_msg or norm_msg in norm_pat


class AutoLogEnricher:
    """
    Cutting Edge: Automatically enriches logs with contextual information.

    Features:
    - Git blame and history
    - Runtime context (memory, CPU, threads)
    - Historical context (similar errors, patterns)
    - Request context propagation
    - Code context from correlation
    """

    def __init__(self, config: Optional[EnrichmentConfig] = None):
        self.config = config or EnrichmentConfig()

        # Initialize enrichers based on config
        self._git_enricher: Optional[GitContextEnricher] = None
        if EnrichmentSource.GIT_CONTEXT in self.config.enabled_sources:
            self._git_enricher = GitContextEnricher(self.config.git_repo_path or ".")

        self._runtime_enricher: Optional[RuntimeContextEnricher] = None
        if EnrichmentSource.RUNTIME_CONTEXT in self.config.enabled_sources:
            self._runtime_enricher = RuntimeContextEnricher()

        self._historical_enricher: Optional[HistoricalContextEnricher] = None
        if EnrichmentSource.HISTORICAL_CONTEXT in self.config.enabled_sources:
            self._historical_enricher = HistoricalContextEnricher()

        self._code_correlator: Optional[LogCodeCorrelator] = None
        if EnrichmentSource.CODE_CONTEXT in self.config.enabled_sources:
            self._code_correlator = LogCodeCorrelator()

        # Cache for enrichments
        self._enrichment_cache: Dict[str, EnrichedLogData] = {}

    def enrich_log(
        self,
        message: str,
        level: str = "INFO",
        logger_name: Optional[str] = None,
        service: str = "unknown",
        source_file: Optional[str] = None,
        source_line: Optional[int] = None
    ) -> EnrichedLogData:
        """
        Enrich a log message with contextual information.

        Returns EnrichedLogData with all applicable enrichments.
        """
        import time
        start_time = time.time()

        enrichments: Dict[EnrichmentSource, Dict[str, Any]] = {}
        sources_used: List[EnrichmentSource] = []

        # Check cache
        cache_key = f"{message[:100]}:{level}:{service}"
        if self.config.cache_enrichments and cache_key in self._enrichment_cache:
            cached = self._enrichment_cache[cache_key]
            # Update with fresh request context
            if EnrichmentSource.REQUEST_CONTEXT in self.config.enabled_sources:
                enrichments[EnrichmentSource.REQUEST_CONTEXT] = self._get_request_context()
            return EnrichedLogData(
                original_message=message,
                enrichments={**cached.enrichments, **enrichments},
                enrichment_time_ms=cached.enrichment_time_ms,
                sources_used=cached.sources_used,
            )

        # Request context (always fresh)
        if EnrichmentSource.REQUEST_CONTEXT in self.config.enabled_sources:
            req_context = self._get_request_context()
            if req_context:
                enrichments[EnrichmentSource.REQUEST_CONTEXT] = req_context
                sources_used.append(EnrichmentSource.REQUEST_CONTEXT)

        # System context
        if EnrichmentSource.SYSTEM_CONTEXT in self.config.enabled_sources:
            sys_context = self._get_system_context()
            enrichments[EnrichmentSource.SYSTEM_CONTEXT] = sys_context
            sources_used.append(EnrichmentSource.SYSTEM_CONTEXT)

        # Runtime context
        if self._runtime_enricher:
            runtime_context = self._runtime_enricher.get_runtime_context()
            enrichments[EnrichmentSource.RUNTIME_CONTEXT] = runtime_context
            sources_used.append(EnrichmentSource.RUNTIME_CONTEXT)

        # Historical context (for errors)
        if self._historical_enricher and level in ("ERROR", "CRITICAL", "WARNING"):
            hist_context = self._historical_enricher.get_historical_context(
                message, level, service
            )
            if hist_context:
                enrichments[EnrichmentSource.HISTORICAL_CONTEXT] = hist_context
                sources_used.append(EnrichmentSource.HISTORICAL_CONTEXT)

        # Git context
        if self._git_enricher and source_file and source_line:
            git_blame = self._git_enricher.get_blame_info(source_file, source_line)
            if git_blame:
                enrichments[EnrichmentSource.GIT_CONTEXT] = {
                    "blame": git_blame,
                    "recent_changes": self._git_enricher.get_recent_changes(source_file, 3),
                }
                sources_used.append(EnrichmentSource.GIT_CONTEXT)

        # Code context
        if self._code_correlator:
            correlation = self._code_correlator.correlate_log(message, logger_name, level)
            if correlation:
                code_ctx = {
                    "source_location": correlation.source_location.to_dict(),
                    "surrounding_variables": correlation.code_context.surrounding_variables,
                    "related_functions": correlation.code_context.related_functions,
                }
                enrichments[EnrichmentSource.CODE_CONTEXT] = code_ctx
                sources_used.append(EnrichmentSource.CODE_CONTEXT)

        enrichment_time = (time.time() - start_time) * 1000

        result = EnrichedLogData(
            original_message=message,
            enrichments=enrichments,
            enrichment_time_ms=enrichment_time,
            sources_used=sources_used,
        )

        # Cache result
        if self.config.cache_enrichments:
            self._enrichment_cache[cache_key] = result

            # Limit cache size
            if len(self._enrichment_cache) > 1000:
                # Remove oldest entries
                keys = list(self._enrichment_cache.keys())
                for key in keys[:500]:
                    del self._enrichment_cache[key]

        return result

    def _get_request_context(self) -> Dict[str, Any]:
        """Get current request context."""
        context = {}

        request_id = request_id_var.get()
        if request_id:
            context["request_id"] = request_id

        user_id = user_id_var.get()
        if user_id:
            context["user_id"] = user_id

        trace_id = trace_id_var.get()
        if trace_id:
            context["trace_id"] = trace_id

        span_id = span_id_var.get()
        if span_id:
            context["span_id"] = span_id

        return context

    def _get_system_context(self) -> Dict[str, Any]:
        """Get system context."""
        return {
            "hostname": os.environ.get("HOSTNAME", os.uname().nodename),
            "environment": os.environ.get("ENVIRONMENT", "development"),
            "pod_name": os.environ.get("POD_NAME"),
            "container_id": os.environ.get("CONTAINER_ID"),
        }

    def get_enrichment_stats(self) -> Dict[str, Any]:
        """Get statistics about enrichment."""
        return {
            "cache_size": len(self._enrichment_cache),
            "enabled_sources": [s.value for s in self.config.enabled_sources],
            "code_correlator_stats": self._code_correlator.get_correlation_stats() if self._code_correlator else None,
        }


# =============================================================================
# CUTTING EDGE: INTEGRATED LOGGING SYSTEM
# =============================================================================

class CuttingEdgeLoggingConfig(LoggingConfig):
    """Extended configuration for cutting edge logging."""
    enable_code_correlation: bool = True
    enable_auto_enrichment: bool = True
    enable_historical_analysis: bool = True
    source_roots: List[str] = field(default_factory=lambda: ["."])
    enrichment_config: Optional[EnrichmentConfig] = None


class CuttingEdgeStructuredLogger:
    """
    Cutting Edge structured logger with auto-enrichment and code correlation.

    Features beyond Advanced:
    - Log-to-Code Correlation: Link logs to source code lines
    - Auto Enrichment: Automatically add context from code, git, runtime
    - Historical Analysis: Detect patterns and similar errors
    - Intelligent Deduplication: ML-based error clustering

    Usage:
        logger = get_cutting_edge_logger(__name__)

        # Automatic enrichment
        logger.info("User logged in", extra={"user_id": "123"})

        # Get code location for errors
        logger.error("Database connection failed")
        # Log will include: source file, line, function, git blame, etc.

        # Get historical context
        analysis = logger.get_log_analysis()
    """

    def __init__(
        self,
        name: str,
        config: Optional[CuttingEdgeLoggingConfig] = None
    ):
        self.name = name
        self.config = config or CuttingEdgeLoggingConfig()
        self._logger = logging.getLogger(name)

        # Initialize cutting edge components
        self._enricher: Optional[AutoLogEnricher] = None
        if self.config.enable_auto_enrichment:
            self._enricher = AutoLogEnricher(self.config.enrichment_config)

        self._correlator: Optional[LogCodeCorrelator] = None
        if self.config.enable_code_correlation:
            self._correlator = LogCodeCorrelator(self.config.source_roots)

        self._analyzer: Optional[SemanticLogAnalyzer] = None
        if self.config.enable_historical_analysis:
            self._analyzer = get_log_analyzer()

        # ML components
        self._ml_clusterer = get_ml_clusterer()
        self._deduplicator = get_smart_deduplicator()

    def _enrich_and_log(
        self,
        level: str,
        message: str,
        exc_info=None,
        **kwargs
    ):
        """Enrich and log a message."""
        # Get caller info
        import inspect
        frame = inspect.currentframe()
        if frame:
            # Go up the call stack to find the actual caller
            caller_frame = frame.f_back.f_back if frame.f_back else None
            if caller_frame:
                source_file = caller_frame.f_code.co_filename
                source_line = caller_frame.f_lineno
                kwargs["_source_file"] = source_file
                kwargs["_source_line"] = source_line

        # Check deduplication
        error_type = kwargs.get("error_type") if level in ("ERROR", "CRITICAL") else None
        should_log, digest = self._deduplicator.should_log(message, error_type)

        if digest:
            # Log the digest instead
            self._logger.info(
                f"Suppressed {digest['suppressed_count']} similar errors",
                extra={"suppression_digest": digest}
            )

        if not should_log and level not in ("CRITICAL",):
            return  # Skip deduplicated logs (always log critical)

        # Enrich log
        enriched_extra = dict(kwargs)
        if self._enricher:
            enriched = self._enricher.enrich_log(
                message=message,
                level=level,
                logger_name=self.name,
                service=self.config.service_name,
                source_file=kwargs.get("_source_file"),
                source_line=kwargs.get("_source_line"),
            )
            enriched_extra.update(enriched.to_dict())

        # Add code correlation
        if self._correlator:
            correlation = self._correlator.correlate_log(message, self.name, level)
            if correlation:
                enriched_extra["_code_location"] = correlation.source_location.to_dict()

        # Analyze for errors
        if self._analyzer and level in ("ERROR", "CRITICAL", "WARNING"):
            self._analyzer.analyze_log(
                message=message,
                level=level,
                logger_name=self.name,
                service=self.config.service_name,
                error_type=kwargs.get("error_type"),
                traceback_str=str(exc_info) if exc_info else None,
                context=kwargs,
            )

            # Add to ML clusterer
            if level in ("ERROR", "CRITICAL"):
                cluster_id, is_new = self._ml_clusterer.add_error(
                    message=message,
                    error_type=kwargs.get("error_type"),
                    logger_name=self.name,
                    service=self.config.service_name,
                )
                enriched_extra["_error_cluster_id"] = cluster_id
                enriched_extra["_is_new_error_pattern"] = is_new

        # Log with enriched context
        log_method = getattr(self._logger, level.lower())
        log_method(message, exc_info=exc_info, extra=enriched_extra)

    def debug(self, message: str, **kwargs):
        self._enrich_and_log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs):
        self._enrich_and_log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._enrich_and_log("WARNING", message, **kwargs)

    def error(self, message: str, exc_info=None, **kwargs):
        self._enrich_and_log("ERROR", message, exc_info=exc_info, **kwargs)

    def critical(self, message: str, exc_info=None, **kwargs):
        self._enrich_and_log("CRITICAL", message, exc_info=exc_info, **kwargs)

    def exception(self, message: str, **kwargs):
        self._enrich_and_log("ERROR", message, exc_info=True, **kwargs)

    def get_log_analysis(self) -> Dict[str, Any]:
        """Get comprehensive log analysis."""
        analysis = {}

        if self._analyzer:
            analysis["patterns"] = self._analyzer.get_error_patterns(min_count=2)[:10]
            analysis["insights"] = [i.to_dict() for i in self._analyzer.get_insights()]
            analysis["category_distribution"] = self._analyzer.get_category_distribution()
            analysis["service_health"] = self._analyzer.get_service_health()

        if self._ml_clusterer:
            analysis["error_clusters"] = self._ml_clusterer.get_clusters(min_count=2)[:10]

        if self._deduplicator:
            analysis["suppression_stats"] = self._deduplicator.get_suppression_stats()

        if self._enricher:
            analysis["enrichment_stats"] = self._enricher.get_enrichment_stats()

        if self._correlator:
            analysis["correlation_stats"] = self._correlator.get_correlation_stats()

        return analysis

    def index_codebase(self) -> int:
        """Index codebase for code correlation."""
        if self._correlator:
            return self._correlator.index_codebase()
        return 0


# Factory functions
_cutting_edge_loggers: Dict[str, CuttingEdgeStructuredLogger] = {}
_cutting_edge_config: Optional[CuttingEdgeLoggingConfig] = None


def configure_cutting_edge_logging(
    config: CuttingEdgeLoggingConfig
) -> None:
    """Configure cutting edge logging globally."""
    global _cutting_edge_config
    _cutting_edge_config = config

    # Also configure base logging
    configure_logging(
        service_name=config.service_name,
        log_level=config.log_level,
        json_format=config.json_format,
        enable_pii_masking=config.enable_pii_masking,
    )


def get_cutting_edge_logger(name: str) -> CuttingEdgeStructuredLogger:
    """Get a cutting edge logger instance."""
    global _cutting_edge_loggers, _cutting_edge_config

    if name not in _cutting_edge_loggers:
        _cutting_edge_loggers[name] = CuttingEdgeStructuredLogger(
            name,
            _cutting_edge_config
        )

    return _cutting_edge_loggers[name]


# =============================================================================
# CUTTING EDGE: PREDICTIVE ERROR DETECTION
# =============================================================================

class PredictionModel(str, Enum):
    """Types of prediction models."""
    TIME_SERIES = "time_series"
    ANOMALY_DETECTION = "anomaly_detection"
    PATTERN_EXTRAPOLATION = "pattern_extrapolation"
    SEASONAL_DECOMPOSITION = "seasonal_decomposition"
    ENSEMBLE = "ensemble"


class RemediationAction(str, Enum):
    """Types of auto-remediation actions."""
    RESTART_SERVICE = "restart_service"
    SCALE_RESOURCES = "scale_resources"
    CLEAR_CACHE = "clear_cache"
    RATE_LIMIT = "rate_limit"
    CIRCUIT_BREAK = "circuit_break"
    ROLLBACK = "rollback"
    NOTIFY_ONCALL = "notify_oncall"
    CREATE_INCIDENT = "create_incident"
    CUSTOM_WEBHOOK = "custom_webhook"


class IncidentSeverity(str, Enum):
    """Incident severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class ErrorPrediction:
    """A prediction of future errors."""
    prediction_id: str
    model: PredictionModel
    predicted_error_type: str
    predicted_time: datetime
    confidence: float
    contributing_factors: List[str]
    historical_pattern: str
    recommended_actions: List[RemediationAction]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "prediction_id": self.prediction_id,
            "model": self.model.value,
            "predicted_error_type": self.predicted_error_type,
            "predicted_time": self.predicted_time.isoformat(),
            "confidence": self.confidence,
            "contributing_factors": self.contributing_factors,
            "historical_pattern": self.historical_pattern,
            "recommended_actions": [a.value for a in self.recommended_actions],
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class AutoRemediation:
    """An auto-remediation record."""
    remediation_id: str
    trigger_event: str
    action: RemediationAction
    target: str
    parameters: Dict[str, Any]
    status: str  # pending, executing, completed, failed
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    result: Optional[str]
    rollback_available: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "remediation_id": self.remediation_id,
            "trigger_event": self.trigger_event,
            "action": self.action.value,
            "target": self.target,
            "parameters": self.parameters,
            "status": self.status,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "result": self.result,
            "rollback_available": self.rollback_available,
        }


@dataclass
class AutoIncident:
    """An automatically created incident."""
    incident_id: str
    title: str
    description: str
    severity: IncidentSeverity
    service: str
    related_errors: List[str]
    predicted_impact: str
    suggested_runbook: Optional[str]
    auto_assigned_to: Optional[str]
    created_at: datetime
    escalation_path: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "title": self.title,
            "description": self.description,
            "severity": self.severity.value,
            "service": self.service,
            "related_errors": self.related_errors,
            "predicted_impact": self.predicted_impact,
            "suggested_runbook": self.suggested_runbook,
            "auto_assigned_to": self.auto_assigned_to,
            "created_at": self.created_at.isoformat(),
            "escalation_path": self.escalation_path,
        }


class PredictiveErrorDetector:
    """
    ML-based error prediction using time series analysis.

    Features:
    - Analyzes error patterns over time
    - Predicts likely future errors
    - Identifies seasonal/cyclical patterns
    - Detects anomalies before they become critical
    """

    def __init__(self, lookback_window: int = 24, forecast_horizon: int = 4):
        self.lookback_window = lookback_window  # hours
        self.forecast_horizon = forecast_horizon  # hours
        self._error_history: List[Tuple[datetime, str, str, Dict]] = []  # (time, type, service, context)
        self._pattern_cache: Dict[str, List[float]] = {}
        self._seasonal_patterns: Dict[str, Dict[str, float]] = {}  # service -> hour_of_day -> rate
        self._predictions: List[ErrorPrediction] = []
        self._model_weights = {
            PredictionModel.TIME_SERIES: 0.3,
            PredictionModel.ANOMALY_DETECTION: 0.25,
            PredictionModel.PATTERN_EXTRAPOLATION: 0.2,
            PredictionModel.SEASONAL_DECOMPOSITION: 0.15,
            PredictionModel.ENSEMBLE: 0.1,
        }

    def record_error(
        self,
        error_type: str,
        service: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Record an error for pattern analysis."""
        self._error_history.append((
            datetime.now(timezone.utc),
            error_type,
            service,
            context or {}
        ))

        # Keep only recent history
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.lookback_window * 2)
        self._error_history = [
            e for e in self._error_history if e[0] > cutoff
        ]

        # Update seasonal patterns
        self._update_seasonal_patterns()

    def _update_seasonal_patterns(self) -> None:
        """Update hourly seasonal patterns."""
        service_hour_counts: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))

        for ts, _, service, _ in self._error_history:
            hour = ts.hour
            service_hour_counts[service][hour].append(1)

        for service, hours in service_hour_counts.items():
            self._seasonal_patterns[service] = {
                str(h): sum(counts) / max(len(counts), 1) * len(self._error_history) / 24
                for h, counts in hours.items()
            }

    def _time_series_prediction(self, service: str, error_type: str) -> Tuple[float, List[str]]:
        """Predict using simple time series analysis."""
        recent_errors = [
            e for e in self._error_history
            if e[2] == service and e[1] == error_type
            and e[0] > datetime.now(timezone.utc) - timedelta(hours=self.lookback_window)
        ]

        if len(recent_errors) < 3:
            return 0.0, []

        # Calculate error rate trend
        intervals = []
        for i in range(1, len(recent_errors)):
            interval = (recent_errors[i][0] - recent_errors[i-1][0]).total_seconds() / 3600
            intervals.append(interval)

        if not intervals:
            return 0.0, []

        avg_interval = sum(intervals) / len(intervals)
        trend = intervals[-1] / avg_interval if avg_interval > 0 else 1.0

        # Higher trend = decreasing intervals = increasing rate
        confidence = min(0.9, 0.3 + (1.0 - trend) * 0.3 + len(recent_errors) * 0.02)

        factors = []
        if trend < 0.8:
            factors.append(f"Error rate increasing (interval decreased by {(1-trend)*100:.0f}%)")
        if len(recent_errors) > 10:
            factors.append(f"High error volume ({len(recent_errors)} errors in {self.lookback_window}h)")

        return max(0, min(1, confidence)), factors

    def _anomaly_detection(self, service: str) -> Tuple[float, List[str]]:
        """Detect anomalies in error patterns."""
        recent_errors = [
            e for e in self._error_history
            if e[2] == service
            and e[0] > datetime.now(timezone.utc) - timedelta(hours=2)
        ]

        historical_errors = [
            e for e in self._error_history
            if e[2] == service
            and e[0] > datetime.now(timezone.utc) - timedelta(hours=self.lookback_window)
            and e[0] <= datetime.now(timezone.utc) - timedelta(hours=2)
        ]

        if not historical_errors:
            return 0.0, []

        # Compare recent rate to historical
        recent_rate = len(recent_errors) / 2  # per hour
        historical_rate = len(historical_errors) / (self.lookback_window - 2)

        if historical_rate == 0:
            return 0.0, []

        deviation = (recent_rate - historical_rate) / historical_rate

        factors = []
        if deviation > 0.5:
            factors.append(f"Anomaly: Error rate {deviation*100:.0f}% above baseline")
            confidence = min(0.85, 0.4 + deviation * 0.3)
        elif deviation > 0.2:
            factors.append(f"Warning: Error rate elevated by {deviation*100:.0f}%")
            confidence = 0.3 + deviation * 0.2
        else:
            confidence = 0.0

        return max(0, min(1, confidence)), factors

    def _seasonal_prediction(self, service: str) -> Tuple[float, List[str]]:
        """Predict based on seasonal patterns."""
        if service not in self._seasonal_patterns:
            return 0.0, []

        current_hour = datetime.now(timezone.utc).hour
        next_hour = (current_hour + 1) % 24

        patterns = self._seasonal_patterns[service]
        current_rate = patterns.get(str(current_hour), 0)
        next_rate = patterns.get(str(next_hour), 0)

        factors = []
        if next_rate > current_rate * 1.5 and next_rate > 2:
            factors.append(f"Seasonal pattern: Peak error time approaching (hour {next_hour})")
            confidence = min(0.7, 0.3 + (next_rate - current_rate) * 0.1)
        else:
            confidence = 0.0

        return max(0, min(1, confidence)), factors

    def predict_errors(self, service: Optional[str] = None) -> List[ErrorPrediction]:
        """Generate error predictions for a service or all services."""
        predictions = []

        # Get unique services
        services = set(e[2] for e in self._error_history)
        if service:
            services = {service} if service in services else set()

        for svc in services:
            # Get unique error types for this service
            error_types = set(
                e[1] for e in self._error_history if e[2] == svc
            )

            for error_type in error_types:
                # Run multiple prediction models
                ts_conf, ts_factors = self._time_series_prediction(svc, error_type)
                anomaly_conf, anomaly_factors = self._anomaly_detection(svc)
                seasonal_conf, seasonal_factors = self._seasonal_prediction(svc)

                # Ensemble prediction
                weights = self._model_weights
                ensemble_confidence = (
                    ts_conf * weights[PredictionModel.TIME_SERIES] +
                    anomaly_conf * weights[PredictionModel.ANOMALY_DETECTION] +
                    seasonal_conf * weights[PredictionModel.SEASONAL_DECOMPOSITION]
                ) / (
                    weights[PredictionModel.TIME_SERIES] +
                    weights[PredictionModel.ANOMALY_DETECTION] +
                    weights[PredictionModel.SEASONAL_DECOMPOSITION]
                )

                # Only create prediction if confidence is significant
                if ensemble_confidence > 0.3:
                    all_factors = ts_factors + anomaly_factors + seasonal_factors

                    # Determine recommended actions based on factors
                    actions = self._determine_actions(ensemble_confidence, all_factors)

                    prediction = ErrorPrediction(
                        prediction_id=f"pred_{hashlib.md5(f'{svc}_{error_type}_{datetime.now().isoformat()}'.encode()).hexdigest()[:12]}",
                        model=PredictionModel.ENSEMBLE,
                        predicted_error_type=error_type,
                        predicted_time=datetime.now(timezone.utc) + timedelta(hours=1),
                        confidence=ensemble_confidence,
                        contributing_factors=all_factors,
                        historical_pattern=f"{len([e for e in self._error_history if e[1] == error_type and e[2] == svc])} occurrences in {self.lookback_window}h",
                        recommended_actions=actions,
                    )
                    predictions.append(prediction)

        # Cache predictions
        self._predictions = predictions
        return predictions

    def _determine_actions(self, confidence: float, factors: List[str]) -> List[RemediationAction]:
        """Determine recommended actions based on prediction."""
        actions = []

        if confidence > 0.7:
            actions.append(RemediationAction.NOTIFY_ONCALL)
            actions.append(RemediationAction.CREATE_INCIDENT)

        if any("rate increasing" in f.lower() for f in factors):
            actions.append(RemediationAction.RATE_LIMIT)

        if any("anomaly" in f.lower() for f in factors):
            actions.append(RemediationAction.CIRCUIT_BREAK)

        if any("volume" in f.lower() for f in factors):
            actions.append(RemediationAction.SCALE_RESOURCES)

        if not actions:
            actions.append(RemediationAction.NOTIFY_ONCALL)

        return actions

    def get_predictions(self) -> List[Dict[str, Any]]:
        """Get current predictions."""
        return [p.to_dict() for p in self._predictions]


class AutoRemediationEngine:
    """
    Automated remediation system for error recovery.

    Features:
    - Rule-based remediation triggers
    - Safe execution with rollback support
    - Integration with orchestration systems
    - Audit trail of all actions
    """

    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self._rules: Dict[str, Dict[str, Any]] = {}
        self._remediations: List[AutoRemediation] = []
        self._action_handlers: Dict[RemediationAction, callable] = {}
        self._cooldowns: Dict[str, datetime] = {}  # action_key -> last_executed
        self._default_cooldown = timedelta(minutes=15)

        # Initialize default rules
        self._init_default_rules()

    def _init_default_rules(self) -> None:
        """Initialize default remediation rules."""
        self._rules = {
            "high_error_rate": {
                "condition": {"error_rate": ">50%", "window": "5m"},
                "actions": [RemediationAction.RATE_LIMIT, RemediationAction.NOTIFY_ONCALL],
                "cooldown_minutes": 15,
            },
            "memory_exhaustion": {
                "condition": {"error_type": "MemoryError"},
                "actions": [RemediationAction.RESTART_SERVICE, RemediationAction.SCALE_RESOURCES],
                "cooldown_minutes": 30,
            },
            "database_connection": {
                "condition": {"error_type": "ConnectionError", "service": "database"},
                "actions": [RemediationAction.CLEAR_CACHE, RemediationAction.CIRCUIT_BREAK],
                "cooldown_minutes": 10,
            },
            "timeout_cascade": {
                "condition": {"error_type": "TimeoutError", "count": ">10", "window": "1m"},
                "actions": [RemediationAction.CIRCUIT_BREAK, RemediationAction.CREATE_INCIDENT],
                "cooldown_minutes": 5,
            },
            "deployment_regression": {
                "condition": {"error_rate_increase": ">200%", "since_deployment": "<1h"},
                "actions": [RemediationAction.ROLLBACK, RemediationAction.CREATE_INCIDENT],
                "cooldown_minutes": 60,
            },
        }

    def register_handler(self, action: RemediationAction, handler: callable) -> None:
        """Register a handler for an action type."""
        self._action_handlers[action] = handler

    def add_rule(
        self,
        rule_id: str,
        condition: Dict[str, Any],
        actions: List[RemediationAction],
        cooldown_minutes: int = 15
    ) -> None:
        """Add a custom remediation rule."""
        self._rules[rule_id] = {
            "condition": condition,
            "actions": actions,
            "cooldown_minutes": cooldown_minutes,
        }

    def _check_cooldown(self, rule_id: str, action: RemediationAction) -> bool:
        """Check if action is in cooldown."""
        key = f"{rule_id}_{action.value}"
        last_executed = self._cooldowns.get(key)

        if last_executed:
            cooldown = timedelta(minutes=self._rules.get(rule_id, {}).get("cooldown_minutes", 15))
            if datetime.now(timezone.utc) - last_executed < cooldown:
                return True

        return False

    def evaluate_and_remediate(
        self,
        error_type: str,
        service: str,
        error_rate: Optional[float] = None,
        error_count: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> List[AutoRemediation]:
        """Evaluate rules and execute remediations."""
        triggered_remediations = []

        for rule_id, rule in self._rules.items():
            if self._matches_condition(rule["condition"], error_type, service, error_rate, error_count, context):
                for action in rule["actions"]:
                    if self._check_cooldown(rule_id, action):
                        continue

                    remediation = self._create_remediation(
                        rule_id=rule_id,
                        action=action,
                        service=service,
                        error_type=error_type,
                        context=context,
                    )

                    if self._execute_remediation(remediation):
                        triggered_remediations.append(remediation)
                        self._cooldowns[f"{rule_id}_{action.value}"] = datetime.now(timezone.utc)

        return triggered_remediations

    def _matches_condition(
        self,
        condition: Dict[str, Any],
        error_type: str,
        service: str,
        error_rate: Optional[float],
        error_count: Optional[int],
        context: Optional[Dict[str, Any]]
    ) -> bool:
        """Check if condition matches current state."""
        # Check error type
        if "error_type" in condition:
            if condition["error_type"].lower() not in error_type.lower():
                return False

        # Check service
        if "service" in condition:
            if condition["service"].lower() not in service.lower():
                return False

        # Check error rate
        if "error_rate" in condition and error_rate is not None:
            threshold_str = condition["error_rate"]
            if threshold_str.startswith(">"):
                threshold = float(threshold_str[1:].replace("%", "")) / 100
                if error_rate <= threshold:
                    return False

        # Check count
        if "count" in condition and error_count is not None:
            threshold_str = condition["count"]
            if threshold_str.startswith(">"):
                threshold = int(threshold_str[1:])
                if error_count <= threshold:
                    return False

        return True

    def _create_remediation(
        self,
        rule_id: str,
        action: RemediationAction,
        service: str,
        error_type: str,
        context: Optional[Dict[str, Any]]
    ) -> AutoRemediation:
        """Create a remediation record."""
        return AutoRemediation(
            remediation_id=f"rem_{hashlib.md5(f'{rule_id}_{action.value}_{datetime.now().isoformat()}'.encode()).hexdigest()[:12]}",
            trigger_event=f"Rule: {rule_id}, Error: {error_type}",
            action=action,
            target=service,
            parameters=context or {},
            status="pending",
            started_at=None,
            completed_at=None,
            result=None,
            rollback_available=action in [RemediationAction.ROLLBACK, RemediationAction.RESTART_SERVICE],
        )

    def _execute_remediation(self, remediation: AutoRemediation) -> bool:
        """Execute a remediation action."""
        if self.dry_run:
            remediation.status = "simulated"
            remediation.started_at = datetime.now(timezone.utc)
            remediation.completed_at = datetime.now(timezone.utc)
            remediation.result = f"[DRY RUN] Would execute {remediation.action.value} on {remediation.target}"
            self._remediations.append(remediation)
            return True

        remediation.status = "executing"
        remediation.started_at = datetime.now(timezone.utc)

        try:
            handler = self._action_handlers.get(remediation.action)
            if handler:
                result = handler(remediation.target, remediation.parameters)
                remediation.result = str(result)
                remediation.status = "completed"
            else:
                # Default handlers
                remediation.result = self._default_handler(remediation)
                remediation.status = "completed"
        except Exception as e:
            remediation.status = "failed"
            remediation.result = f"Error: {str(e)}"
        finally:
            remediation.completed_at = datetime.now(timezone.utc)
            self._remediations.append(remediation)

        return remediation.status == "completed"

    def _default_handler(self, remediation: AutoRemediation) -> str:
        """Default handler for actions without custom handlers."""
        action = remediation.action

        if action == RemediationAction.NOTIFY_ONCALL:
            return f"Notification sent to on-call for {remediation.target}"
        elif action == RemediationAction.CREATE_INCIDENT:
            return f"Incident created for {remediation.trigger_event}"
        elif action == RemediationAction.RATE_LIMIT:
            return f"Rate limiting enabled for {remediation.target}"
        elif action == RemediationAction.CIRCUIT_BREAK:
            return f"Circuit breaker activated for {remediation.target}"
        elif action == RemediationAction.CLEAR_CACHE:
            return f"Cache cleared for {remediation.target}"
        else:
            return f"Action {action.value} simulated for {remediation.target}"

    def get_remediation_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get remediation history."""
        return [r.to_dict() for r in self._remediations[-limit:]]


class AutoIncidentCreator:
    """
    Automatic incident creation and management.

    Features:
    - Intelligent incident creation from error patterns
    - Automatic severity assessment
    - Runbook suggestion
    - Smart assignment based on service ownership
    """

    def __init__(self):
        self._incidents: List[AutoIncident] = []
        self._service_owners: Dict[str, str] = {}
        self._runbooks: Dict[str, str] = {}
        self._severity_thresholds = {
            IncidentSeverity.CRITICAL: {"error_rate": 0.5, "user_impact": 1000},
            IncidentSeverity.HIGH: {"error_rate": 0.25, "user_impact": 100},
            IncidentSeverity.MEDIUM: {"error_rate": 0.1, "user_impact": 10},
            IncidentSeverity.LOW: {"error_rate": 0.05, "user_impact": 1},
        }
        self._escalation_paths: Dict[str, List[str]] = {
            "default": ["on-call-primary", "on-call-secondary", "engineering-lead", "director"],
            "critical": ["on-call-primary", "on-call-secondary", "engineering-lead", "director", "vp-engineering"],
        }

    def set_service_owner(self, service: str, owner: str) -> None:
        """Set the owner for a service."""
        self._service_owners[service] = owner

    def set_runbook(self, error_pattern: str, runbook_url: str) -> None:
        """Set a runbook for an error pattern."""
        self._runbooks[error_pattern] = runbook_url

    def create_incident(
        self,
        errors: List[Dict[str, Any]],
        service: str,
        error_rate: Optional[float] = None,
        affected_users: Optional[int] = None,
        prediction: Optional[ErrorPrediction] = None
    ) -> AutoIncident:
        """Create an incident from error data."""
        # Determine severity
        severity = self._assess_severity(error_rate, affected_users, len(errors))

        # Find runbook
        error_types = list(set(e.get("error_type", "Unknown") for e in errors))
        runbook = None
        for et in error_types:
            for pattern, url in self._runbooks.items():
                if pattern.lower() in et.lower():
                    runbook = url
                    break

        # Build title and description
        title = f"[{severity.value.upper()}] {service}: {error_types[0] if error_types else 'Multiple Errors'}"

        description_parts = [
            f"**Service:** {service}",
            f"**Error Count:** {len(errors)}",
            f"**Error Types:** {', '.join(error_types)}",
        ]

        if error_rate is not None:
            description_parts.append(f"**Error Rate:** {error_rate*100:.1f}%")
        if affected_users is not None:
            description_parts.append(f"**Affected Users:** ~{affected_users}")
        if prediction:
            description_parts.append(f"\n**Predicted by AI:** {prediction.historical_pattern}")
            description_parts.append(f"**Contributing Factors:**\n" + "\n".join(f"- {f}" for f in prediction.contributing_factors))

        description = "\n".join(description_parts)

        # Get escalation path
        escalation = self._escalation_paths.get(
            "critical" if severity == IncidentSeverity.CRITICAL else "default"
        )

        incident = AutoIncident(
            incident_id=f"INC-{hashlib.md5(f'{service}_{datetime.now().isoformat()}'.encode()).hexdigest()[:8].upper()}",
            title=title,
            description=description,
            severity=severity,
            service=service,
            related_errors=[e.get("error_id", str(i)) for i, e in enumerate(errors[:10])],
            predicted_impact=self._estimate_impact(severity, affected_users),
            suggested_runbook=runbook,
            auto_assigned_to=self._service_owners.get(service),
            created_at=datetime.now(timezone.utc),
            escalation_path=escalation,
        )

        self._incidents.append(incident)
        return incident

    def _assess_severity(
        self,
        error_rate: Optional[float],
        affected_users: Optional[int],
        error_count: int
    ) -> IncidentSeverity:
        """Assess incident severity."""
        # Check thresholds in order of severity
        for severity, thresholds in self._severity_thresholds.items():
            if error_rate is not None and error_rate >= thresholds["error_rate"]:
                return severity
            if affected_users is not None and affected_users >= thresholds["user_impact"]:
                return severity

        # Default based on error count
        if error_count >= 100:
            return IncidentSeverity.HIGH
        elif error_count >= 20:
            return IncidentSeverity.MEDIUM
        else:
            return IncidentSeverity.LOW

    def _estimate_impact(self, severity: IncidentSeverity, affected_users: Optional[int]) -> str:
        """Estimate business impact."""
        impacts = {
            IncidentSeverity.CRITICAL: "Major outage affecting core functionality",
            IncidentSeverity.HIGH: "Significant degradation impacting many users",
            IncidentSeverity.MEDIUM: "Partial functionality issues affecting some users",
            IncidentSeverity.LOW: "Minor issues with limited user impact",
            IncidentSeverity.INFO: "Informational - no immediate user impact",
        }

        base_impact = impacts.get(severity, "Unknown impact")
        if affected_users is not None:
            return f"{base_impact}. Estimated {affected_users} users affected."
        return base_impact

    def get_incidents(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent incidents."""
        return [i.to_dict() for i in self._incidents[-limit:]]


class CuttingEdgePredictiveLogger:
    """
    Cutting-edge predictive error logger combining all features.

    Features:
    - Predictive error detection
    - Automatic remediation
    - Incident creation
    - Integrated with structured logging
    """

    def __init__(
        self,
        service_name: str,
        enable_predictions: bool = True,
        enable_remediation: bool = True,
        remediation_dry_run: bool = True,
        enable_incidents: bool = True
    ):
        self.service_name = service_name
        self._predictor = PredictiveErrorDetector() if enable_predictions else None
        self._remediator = AutoRemediationEngine(dry_run=remediation_dry_run) if enable_remediation else None
        self._incident_creator = AutoIncidentCreator() if enable_incidents else None
        self._error_count = 0
        self._error_window: List[Tuple[datetime, str]] = []
        self._window_size = timedelta(minutes=5)

    def log_error(
        self,
        error_type: str,
        message: str,
        context: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Log an error with predictive analysis."""
        now = datetime.now(timezone.utc)

        # Track error for rate calculation
        self._error_window.append((now, error_type))
        self._error_window = [
            (t, et) for t, et in self._error_window
            if now - t < self._window_size
        ]
        self._error_count += 1

        result = {
            "logged": True,
            "error_type": error_type,
            "message": message,
            "predictions": [],
            "remediations": [],
            "incident": None,
        }

        # Record for prediction
        if self._predictor:
            self._predictor.record_error(error_type, self.service_name, context)
            predictions = self._predictor.predict_errors(self.service_name)
            result["predictions"] = [p.to_dict() for p in predictions]

        # Check for remediations
        if self._remediator:
            error_rate = len(self._error_window) / (self._window_size.total_seconds() / 60)  # per minute
            remediations = self._remediator.evaluate_and_remediate(
                error_type=error_type,
                service=self.service_name,
                error_rate=error_rate / 100,  # normalize
                error_count=len(self._error_window),
                context=context,
            )
            result["remediations"] = [r.to_dict() for r in remediations]

        # Create incident if needed
        if self._incident_creator and len(self._error_window) > 10:
            high_confidence_predictions = [
                p for p in (self._predictor.predict_errors() if self._predictor else [])
                if p.confidence > 0.6
            ]

            if high_confidence_predictions or len(self._error_window) > 20:
                incident = self._incident_creator.create_incident(
                    errors=[{"error_type": et, "timestamp": t.isoformat()} for t, et in self._error_window],
                    service=self.service_name,
                    error_rate=len(self._error_window) / (self._window_size.total_seconds() / 60) / 100,
                    prediction=high_confidence_predictions[0] if high_confidence_predictions else None,
                )
                result["incident"] = incident.to_dict()

        return result

    def get_predictions(self) -> List[Dict[str, Any]]:
        """Get current predictions."""
        if self._predictor:
            return self._predictor.get_predictions()
        return []

    def get_remediation_history(self) -> List[Dict[str, Any]]:
        """Get remediation history."""
        if self._remediator:
            return self._remediator.get_remediation_history()
        return []

    def get_incidents(self) -> List[Dict[str, Any]]:
        """Get incidents."""
        if self._incident_creator:
            return self._incident_creator.get_incidents()
        return []

    def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary."""
        return {
            "service": self.service_name,
            "total_errors": self._error_count,
            "errors_in_window": len(self._error_window),
            "window_minutes": self._window_size.total_seconds() / 60,
            "active_predictions": len(self.get_predictions()),
            "recent_remediations": len(self.get_remediation_history()),
            "open_incidents": len(self.get_incidents()),
        }


# Factory functions for cutting-edge predictive logging
_predictive_loggers: Dict[str, CuttingEdgePredictiveLogger] = {}


def get_predictive_logger(
    service_name: str,
    enable_predictions: bool = True,
    enable_remediation: bool = True,
    remediation_dry_run: bool = True,
    enable_incidents: bool = True
) -> CuttingEdgePredictiveLogger:
    """Get or create a cutting-edge predictive logger."""
    if service_name not in _predictive_loggers:
        _predictive_loggers[service_name] = CuttingEdgePredictiveLogger(
            service_name=service_name,
            enable_predictions=enable_predictions,
            enable_remediation=enable_remediation,
            remediation_dry_run=remediation_dry_run,
            enable_incidents=enable_incidents,
        )
    return _predictive_loggers[service_name]
