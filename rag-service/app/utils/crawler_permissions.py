"""
Crawler Permission System

User-controlled permission management for web crawling.
Implements security best practices for URL access control.

Features:
- Domain whitelisting
- URL pattern matching
- Rate limiting
- Audit logging
- Robots.txt compliance
- User consent tracking
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse
from collections import defaultdict

logger = logging.getLogger(__name__)

# =============================================================================
# LIBRARY AVAILABILITY
# =============================================================================

AIOHTTP_AVAILABLE = False

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    logger.warning("aiohttp not installed. Robots.txt checking disabled.")


# =============================================================================
# DATA MODELS
# =============================================================================

class PermissionLevel(str, Enum):
    """Permission levels for domains"""
    BLOCKED = "blocked"          # Explicitly blocked
    ASK = "ask"                  # Ask user before crawling
    ALLOWED = "allowed"          # Allowed for this session
    TRUSTED = "trusted"          # Always allowed (user whitelist)


class CrawlAction(str, Enum):
    """Types of crawl actions"""
    FETCH = "fetch"              # Simple page fetch
    CRAWL = "crawl"              # Multi-page crawl
    DOWNLOAD = "download"        # File download
    SCREENSHOT = "screenshot"    # Take screenshot
    EXTRACT = "extract"          # Extract data


@dataclass
class DomainPermission:
    """Permission record for a domain"""
    domain: str
    level: PermissionLevel
    granted_by: str  # user_id
    granted_at: datetime = field(default_factory=datetime.now)
    expires_at: Optional[datetime] = None
    allowed_paths: Set[str] = field(default_factory=set)
    blocked_paths: Set[str] = field(default_factory=set)
    rate_limit: int = 10  # requests per minute
    notes: Optional[str] = None

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return datetime.now() > self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "level": self.level.value,
            "granted_by": self.granted_by,
            "granted_at": self.granted_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "allowed_paths": list(self.allowed_paths),
            "blocked_paths": list(self.blocked_paths),
            "rate_limit": self.rate_limit,
            "is_expired": self.is_expired,
        }


@dataclass
class CrawlRequest:
    """A request to crawl a URL"""
    url: str
    action: CrawlAction
    user_id: str
    requested_at: datetime = field(default_factory=datetime.now)
    approved: bool = False
    approved_at: Optional[datetime] = None
    denied_reason: Optional[str] = None


@dataclass
class AuditLogEntry:
    """Audit log entry for crawl actions"""
    timestamp: datetime
    user_id: str
    action: CrawlAction
    url: str
    domain: str
    status: str  # allowed, denied, rate_limited
    reason: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# RATE LIMITER
# =============================================================================

class RateLimiter:
    """
    Per-domain rate limiting.
    Uses sliding window algorithm.
    """

    def __init__(self, default_limit: int = 10, window_seconds: int = 60):
        self.default_limit = default_limit
        self.window_seconds = window_seconds
        self._requests: Dict[str, List[datetime]] = defaultdict(list)
        self._limits: Dict[str, int] = {}

    def set_limit(self, domain: str, limit: int):
        """Set rate limit for a domain."""
        self._limits[domain] = limit

    def get_limit(self, domain: str) -> int:
        """Get rate limit for a domain."""
        return self._limits.get(domain, self.default_limit)

    def is_allowed(self, domain: str) -> Tuple[bool, int]:
        """
        Check if request is allowed under rate limit.

        Returns:
            Tuple of (allowed, remaining_requests)
        """
        now = datetime.now()
        window_start = now - timedelta(seconds=self.window_seconds)

        # Clean old requests
        self._requests[domain] = [
            t for t in self._requests[domain]
            if t > window_start
        ]

        limit = self.get_limit(domain)
        current_count = len(self._requests[domain])
        remaining = max(0, limit - current_count)

        if current_count >= limit:
            return False, 0

        return True, remaining

    def record_request(self, domain: str):
        """Record a request for rate limiting."""
        self._requests[domain].append(datetime.now())

    def get_wait_time(self, domain: str) -> int:
        """Get seconds to wait before next request is allowed."""
        if not self._requests[domain]:
            return 0

        limit = self.get_limit(domain)
        if len(self._requests[domain]) < limit:
            return 0

        # Time until oldest request expires
        oldest = min(self._requests[domain])
        wait = (oldest + timedelta(seconds=self.window_seconds) - datetime.now()).total_seconds()
        return max(0, int(wait))


# =============================================================================
# ROBOTS.TXT PARSER
# =============================================================================

class RobotsTxtChecker:
    """
    Checks robots.txt compliance.
    Caches robots.txt files for efficiency.
    """

    def __init__(self, user_agent: str = "DriveSentinel-Crawler/1.0"):
        self.user_agent = user_agent
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 3600  # 1 hour

    async def is_allowed(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt."""
        if not AIOHTTP_AVAILABLE:
            return True  # Allow if we can't check

        parsed = urlparse(url)
        domain = parsed.netloc
        path = parsed.path or "/"

        # Get robots.txt rules
        rules = await self._get_rules(domain, parsed.scheme)

        if rules is None:
            return True  # No robots.txt = allowed

        # Check disallow rules
        for disallow in rules.get("disallow", []):
            if path.startswith(disallow):
                # Check for specific allow
                for allow in rules.get("allow", []):
                    if path.startswith(allow):
                        return True
                return False

        return True

    async def _get_rules(self, domain: str, scheme: str) -> Optional[Dict[str, List[str]]]:
        """Get robots.txt rules for domain."""
        # Check cache
        if domain in self._cache:
            cache_entry = self._cache[domain]
            if datetime.now() < cache_entry["expires"]:
                return cache_entry["rules"]

        # Fetch robots.txt
        robots_url = f"{scheme}://{domain}/robots.txt"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status != 200:
                        self._cache[domain] = {
                            "rules": None,
                            "expires": datetime.now() + timedelta(seconds=self._cache_ttl)
                        }
                        return None

                    content = await response.text()
                    rules = self._parse_robots(content)

                    self._cache[domain] = {
                        "rules": rules,
                        "expires": datetime.now() + timedelta(seconds=self._cache_ttl)
                    }

                    return rules

        except Exception as e:
            logger.warning(f"Failed to fetch robots.txt for {domain}: {e}")
            return None

    def _parse_robots(self, content: str) -> Dict[str, List[str]]:
        """Parse robots.txt content."""
        rules = {"allow": [], "disallow": []}
        current_agent = None

        for line in content.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if line.lower().startswith("user-agent:"):
                agent = line.split(":", 1)[1].strip()
                if agent == "*" or agent.lower() == self.user_agent.lower():
                    current_agent = agent

            elif current_agent and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    rules["disallow"].append(path)

            elif current_agent and line.lower().startswith("allow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    rules["allow"].append(path)

        return rules


# =============================================================================
# PERMISSION MANAGER
# =============================================================================

class CrawlerPermissionManager:
    """
    Manages crawl permissions with security controls.

    Security features:
    - Explicit user consent required
    - Domain whitelisting
    - Rate limiting
    - Robots.txt compliance
    - Comprehensive audit logging
    """

    # Blocked TLDs and patterns
    BLOCKED_PATTERNS = [
        r"\.gov$",          # Government sites (unless explicitly allowed)
        r"\.mil$",          # Military sites
        r"\.edu$",          # Educational (often have restrictions)
        r"localhost",       # Local resources
        r"127\.0\.0\.1",
        r"192\.168\.",
        r"10\.",
        r"172\.(1[6-9]|2[0-9]|3[01])\.",
    ]

    # Always blocked (security/privacy)
    ALWAYS_BLOCKED = [
        "facebook.com",
        "google.com/accounts",
        "bank",
        "paypal.com",
        "stripe.com",
    ]

    def __init__(self, user_id: str):
        self.user_id = user_id
        self._permissions: Dict[str, DomainPermission] = {}
        self._rate_limiter = RateLimiter()
        self._robots_checker = RobotsTxtChecker()
        self._audit_log: List[AuditLogEntry] = []
        self._pending_requests: Dict[str, CrawlRequest] = {}

    # =========================================================================
    # PERMISSION MANAGEMENT
    # =========================================================================

    def add_permission(
        self,
        domain: str,
        level: PermissionLevel = PermissionLevel.ALLOWED,
        allowed_paths: Optional[Set[str]] = None,
        blocked_paths: Optional[Set[str]] = None,
        rate_limit: int = 10,
        ttl: Optional[int] = None,
        notes: Optional[str] = None
    ) -> DomainPermission:
        """
        Add permission for a domain.

        Args:
            domain: Target domain
            level: Permission level
            allowed_paths: Specific paths to allow
            blocked_paths: Specific paths to block
            rate_limit: Requests per minute
            ttl: Time-to-live in seconds
            notes: Optional notes

        Returns:
            Created permission
        """
        expires_at = None
        if ttl:
            expires_at = datetime.now() + timedelta(seconds=ttl)

        permission = DomainPermission(
            domain=domain,
            level=level,
            granted_by=self.user_id,
            expires_at=expires_at,
            allowed_paths=allowed_paths or set(),
            blocked_paths=blocked_paths or set(),
            rate_limit=rate_limit,
            notes=notes,
        )

        self._permissions[domain] = permission
        self._rate_limiter.set_limit(domain, rate_limit)

        logger.info(f"Added {level.value} permission for {domain}")
        return permission

    def revoke_permission(self, domain: str):
        """Revoke permission for a domain."""
        if domain in self._permissions:
            del self._permissions[domain]
            logger.info(f"Revoked permission for {domain}")

    def get_permission(self, domain: str) -> Optional[DomainPermission]:
        """Get permission for a domain."""
        perm = self._permissions.get(domain)
        if perm and not perm.is_expired:
            return perm
        return None

    def list_permissions(self) -> List[Dict[str, Any]]:
        """List all permissions."""
        return [
            perm.to_dict()
            for perm in self._permissions.values()
            if not perm.is_expired
        ]

    # =========================================================================
    # ACCESS CONTROL
    # =========================================================================

    async def check_permission(
        self,
        url: str,
        action: CrawlAction = CrawlAction.FETCH
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if crawling a URL is permitted.

        Args:
            url: Target URL
            action: Type of action

        Returns:
            Tuple of (allowed, reason_if_denied)
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        path = parsed.path or "/"

        # Check always blocked
        for blocked in self.ALWAYS_BLOCKED:
            if blocked in domain.lower():
                self._log_action(action, url, "denied", "Always blocked domain")
                return False, f"Domain {domain} is blocked for security reasons"

        # Check blocked patterns
        for pattern in self.BLOCKED_PATTERNS:
            if re.search(pattern, domain, re.IGNORECASE):
                self._log_action(action, url, "denied", "Blocked pattern match")
                return False, f"Domain matches blocked pattern: {pattern}"

        # Check explicit permission
        permission = self.get_permission(domain)

        if permission is None:
            self._log_action(action, url, "denied", "No permission")
            return False, f"No permission granted for {domain}. Please add it to your whitelist."

        if permission.level == PermissionLevel.BLOCKED:
            self._log_action(action, url, "denied", "Explicitly blocked")
            return False, f"Domain {domain} is explicitly blocked"

        if permission.level == PermissionLevel.ASK:
            # Create pending request
            request = CrawlRequest(
                url=url,
                action=action,
                user_id=self.user_id,
            )
            self._pending_requests[url] = request
            self._log_action(action, url, "pending", "Awaiting user approval")
            return False, "Permission required. Please approve this request."

        # Check path restrictions
        if permission.blocked_paths:
            for blocked_path in permission.blocked_paths:
                if path.startswith(blocked_path):
                    self._log_action(action, url, "denied", "Blocked path")
                    return False, f"Path {path} is blocked"

        if permission.allowed_paths:
            path_allowed = any(path.startswith(p) for p in permission.allowed_paths)
            if not path_allowed:
                self._log_action(action, url, "denied", "Path not in allowlist")
                return False, f"Path {path} is not in the allowed paths list"

        # Check rate limit
        allowed, remaining = self._rate_limiter.is_allowed(domain)
        if not allowed:
            wait_time = self._rate_limiter.get_wait_time(domain)
            self._log_action(action, url, "rate_limited", f"Wait {wait_time}s")
            return False, f"Rate limit exceeded. Please wait {wait_time} seconds."

        # Check robots.txt
        robots_allowed = await self._robots_checker.is_allowed(url)
        if not robots_allowed:
            self._log_action(action, url, "denied", "Blocked by robots.txt")
            return False, f"URL is blocked by robots.txt"

        # All checks passed
        self._rate_limiter.record_request(domain)
        self._log_action(action, url, "allowed")
        return True, None

    def approve_pending_request(self, url: str) -> bool:
        """Approve a pending crawl request."""
        if url in self._pending_requests:
            request = self._pending_requests[url]
            request.approved = True
            request.approved_at = datetime.now()

            # Add temporary permission
            domain = urlparse(url).netloc
            self.add_permission(domain, PermissionLevel.ALLOWED, ttl=3600)

            del self._pending_requests[url]
            return True
        return False

    def deny_pending_request(self, url: str, reason: str = "User denied"):
        """Deny a pending crawl request."""
        if url in self._pending_requests:
            request = self._pending_requests[url]
            request.denied_reason = reason
            del self._pending_requests[url]

    def get_pending_requests(self) -> List[Dict[str, Any]]:
        """Get list of pending requests."""
        return [
            {
                "url": req.url,
                "action": req.action.value,
                "requested_at": req.requested_at.isoformat(),
                "domain": urlparse(req.url).netloc,
            }
            for req in self._pending_requests.values()
        ]

    # =========================================================================
    # AUDIT LOGGING
    # =========================================================================

    def _log_action(
        self,
        action: CrawlAction,
        url: str,
        status: str,
        reason: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log a crawl action."""
        entry = AuditLogEntry(
            timestamp=datetime.now(),
            user_id=self.user_id,
            action=action,
            url=url,
            domain=urlparse(url).netloc,
            status=status,
            reason=reason,
            metadata=metadata or {},
        )
        self._audit_log.append(entry)

        # Keep only last 1000 entries
        if len(self._audit_log) > 1000:
            self._audit_log = self._audit_log[-1000:]

    def get_audit_log(
        self,
        limit: int = 100,
        domain: Optional[str] = None,
        status: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get audit log entries."""
        entries = self._audit_log

        if domain:
            entries = [e for e in entries if e.domain == domain]

        if status:
            entries = [e for e in entries if e.status == status]

        return [
            {
                "timestamp": e.timestamp.isoformat(),
                "action": e.action.value,
                "url": e.url,
                "domain": e.domain,
                "status": e.status,
                "reason": e.reason,
            }
            for e in entries[-limit:]
        ]

    # =========================================================================
    # UTILITY
    # =========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Get permission statistics."""
        return {
            "total_permissions": len(self._permissions),
            "allowed_domains": len([p for p in self._permissions.values()
                                   if p.level in [PermissionLevel.ALLOWED, PermissionLevel.TRUSTED]]),
            "blocked_domains": len([p for p in self._permissions.values()
                                   if p.level == PermissionLevel.BLOCKED]),
            "pending_requests": len(self._pending_requests),
            "audit_log_entries": len(self._audit_log),
        }


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Global manager cache
_permission_managers: Dict[str, CrawlerPermissionManager] = {}


def get_permission_manager(user_id: str) -> CrawlerPermissionManager:
    """Get or create permission manager for a user."""
    if user_id not in _permission_managers:
        _permission_managers[user_id] = CrawlerPermissionManager(user_id)
    return _permission_managers[user_id]


async def check_crawl_permission(
    url: str,
    user_id: str,
    action: CrawlAction = CrawlAction.FETCH
) -> Tuple[bool, Optional[str]]:
    """
    Check if user has permission to crawl a URL.

    Args:
        url: Target URL
        user_id: User ID
        action: Crawl action type

    Returns:
        Tuple of (allowed, reason_if_denied)
    """
    manager = get_permission_manager(user_id)
    return await manager.check_permission(url, action)


__all__ = [
    'CrawlerPermissionManager',
    'DomainPermission',
    'CrawlRequest',
    'CrawlAction',
    'PermissionLevel',
    'RateLimiter',
    'RobotsTxtChecker',
    'get_permission_manager',
    'check_crawl_permission',
]
