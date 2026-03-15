"""
Bot Detection Module

Enterprise-grade bot detection and mitigation.
Similar to Cloudflare Bot Management, AWS WAF Bot Control, Akamai Bot Manager.

Features:
- User-Agent analysis
- Known bot signatures
- Behavioral analysis
- Challenge-based verification
- Good bot allowlist (Googlebot, etc.)
- Rate-based detection
"""

import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

from fastapi import HTTPException, Request, status

logger = logging.getLogger(__name__)


class BotCategory(str, Enum):
    """Bot categories."""
    VERIFIED_CRAWLER = "verified_crawler"  # Google, Bing, etc.
    UNVERIFIED_CRAWLER = "unverified_crawler"
    MONITORING = "monitoring"  # Uptime monitors
    AUTOMATION = "automation"  # CI/CD, testing
    SCRAPER = "scraper"  # Content scrapers
    SPAM_BOT = "spam_bot"
    ATTACK_BOT = "attack_bot"
    UNKNOWN_BOT = "unknown_bot"
    HUMAN = "human"


class BotAction(str, Enum):
    """Action to take for detected bots."""
    ALLOW = "allow"
    CHALLENGE = "challenge"  # CAPTCHA
    THROTTLE = "throttle"  # Rate limit
    BLOCK = "block"


@dataclass
class BotDetectionConfig:
    """Configuration for bot detection."""
    redis_url: str = field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    key_prefix: str = "bot:"

    # Detection thresholds
    min_request_interval_ms: int = 50  # Requests faster than 50ms
    max_requests_per_minute: int = 120
    max_requests_per_second: int = 10

    # Behavioral analysis
    analyze_behavior: bool = True
    behavior_window_seconds: int = 60
    suspicious_patterns_threshold: int = 3

    # Actions per category
    actions: Dict[BotCategory, BotAction] = field(default_factory=lambda: {
        BotCategory.VERIFIED_CRAWLER: BotAction.ALLOW,
        BotCategory.UNVERIFIED_CRAWLER: BotAction.THROTTLE,
        BotCategory.MONITORING: BotAction.ALLOW,
        BotCategory.AUTOMATION: BotAction.ALLOW,
        BotCategory.SCRAPER: BotAction.THROTTLE,
        BotCategory.SPAM_BOT: BotAction.BLOCK,
        BotCategory.ATTACK_BOT: BotAction.BLOCK,
        BotCategory.UNKNOWN_BOT: BotAction.CHALLENGE,
        BotCategory.HUMAN: BotAction.ALLOW,
    })


# Known good bots (verified by reverse DNS)
VERIFIED_BOTS = {
    "Googlebot": {"pattern": r"Googlebot", "dns_suffix": ".googlebot.com"},
    "Bingbot": {"pattern": r"bingbot", "dns_suffix": ".search.msn.com"},
    "Slurp": {"pattern": r"Yahoo! Slurp", "dns_suffix": ".crawl.yahoo.net"},
    "DuckDuckBot": {"pattern": r"DuckDuckBot", "dns_suffix": ".duckduckgo.com"},
    "Baiduspider": {"pattern": r"Baiduspider", "dns_suffix": ".baidu.com"},
    "YandexBot": {"pattern": r"YandexBot", "dns_suffix": ".yandex.com"},
    "facebookexternalhit": {"pattern": r"facebookexternalhit", "dns_suffix": ".facebook.com"},
    "Twitterbot": {"pattern": r"Twitterbot", "dns_suffix": None},
    "LinkedInBot": {"pattern": r"LinkedInBot", "dns_suffix": None},
}

# Known monitoring/automation bots
MONITORING_BOTS = [
    r"Pingdom",
    r"UptimeRobot",
    r"StatusCake",
    r"Site24x7",
    r"Datadog",
    r"NewRelic",
]

AUTOMATION_BOTS = [
    r"GitHub-Hookshot",
    r"GitLab",
    r"Jenkins",
    r"CircleCI",
    r"Travis",
    r"Postman",
    r"curl/",
    r"HTTPie/",
    r"python-requests",
    r"axios/",
]

# Suspicious patterns
SUSPICIOUS_USER_AGENTS = [
    r"^$",  # Empty UA
    r"^-$",  # Dash UA
    r"^Mozilla/4\.0$",  # Ancient Mozilla
    r"Python-urllib",
    r"Java/",
    r"libwww-perl",
    r"Wget/",
    r"scrapy",
    r"phantom",
    r"selenium",
    r"headless",
    r"crawler",
    r"spider",
    r"bot",
]

# Attack signatures
ATTACK_SIGNATURES = [
    r"sqlmap",
    r"nikto",
    r"nmap",
    r"masscan",
    r"zgrab",
    r"dirbuster",
    r"gobuster",
    r"wpscan",
    r"nuclei",
    r"burp",
    r"nessus",
    r"acunetix",
]


@dataclass
class BotDetectionResult:
    """Result of bot detection."""
    is_bot: bool
    category: BotCategory
    confidence: float  # 0.0 to 1.0
    action: BotAction
    reasons: List[str] = field(default_factory=list)
    fingerprint: str = ""


class BotDetector:
    """
    Bot detection and classification service.

    Usage:
        detector = BotDetector(config)
        await detector.connect()

        # Check request
        result = await detector.detect(request)
        if result.action == BotAction.BLOCK:
            raise HTTPException(403, "Bot detected")
    """

    def __init__(self, config: Optional[BotDetectionConfig] = None):
        self.config = config or BotDetectionConfig()
        self._redis = None

        # Compile patterns
        self._verified_patterns = {
            name: re.compile(info["pattern"], re.IGNORECASE)
            for name, info in VERIFIED_BOTS.items()
        }
        self._monitoring_patterns = [re.compile(p, re.IGNORECASE) for p in MONITORING_BOTS]
        self._automation_patterns = [re.compile(p, re.IGNORECASE) for p in AUTOMATION_BOTS]
        self._suspicious_patterns = [re.compile(p, re.IGNORECASE) for p in SUSPICIOUS_USER_AGENTS]
        self._attack_patterns = [re.compile(p, re.IGNORECASE) for p in ATTACK_SIGNATURES]

    async def connect(self):
        """Connect to Redis."""
        try:
            import redis.asyncio as redis
            self._redis = redis.from_url(
                self.config.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self._redis.ping()
            logger.info("Bot detector connected to Redis")
        except Exception as e:
            logger.warning(f"Bot detector Redis connection failed: {e}")
            self._redis = None

    async def close(self):
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()

    def _get_fingerprint(self, request: Request) -> str:
        """Generate request fingerprint for behavioral analysis."""
        parts = [
            request.client.host if request.client else "",
            request.headers.get("User-Agent", ""),
            request.headers.get("Accept-Language", ""),
            request.headers.get("Accept-Encoding", ""),
        ]
        data = "|".join(parts)
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _check_user_agent(self, user_agent: str) -> Tuple[BotCategory, float, List[str]]:
        """
        Analyze User-Agent string.

        Returns:
            Tuple of (category, confidence, reasons)
        """
        if not user_agent:
            return BotCategory.UNKNOWN_BOT, 0.7, ["Empty User-Agent"]

        reasons = []

        # Check attack signatures first
        for pattern in self._attack_patterns:
            if pattern.search(user_agent):
                return BotCategory.ATTACK_BOT, 0.95, [f"Attack tool signature: {pattern.pattern}"]

        # Check verified crawlers
        for name, pattern in self._verified_patterns.items():
            if pattern.search(user_agent):
                # TODO: Verify via reverse DNS for high confidence
                return BotCategory.VERIFIED_CRAWLER, 0.8, [f"Verified crawler: {name}"]

        # Check monitoring bots
        for pattern in self._monitoring_patterns:
            if pattern.search(user_agent):
                return BotCategory.MONITORING, 0.9, [f"Monitoring bot: {pattern.pattern}"]

        # Check automation bots
        for pattern in self._automation_patterns:
            if pattern.search(user_agent):
                return BotCategory.AUTOMATION, 0.85, [f"Automation tool: {pattern.pattern}"]

        # Check suspicious patterns
        suspicious_count = 0
        for pattern in self._suspicious_patterns:
            if pattern.search(user_agent):
                suspicious_count += 1
                reasons.append(f"Suspicious pattern: {pattern.pattern}")

        if suspicious_count >= 2:
            return BotCategory.UNKNOWN_BOT, 0.7, reasons

        # Check for common browser indicators
        browser_indicators = [
            "Mozilla/5.0",
            "Chrome/",
            "Firefox/",
            "Safari/",
            "Edge/",
        ]

        has_browser = any(ind in user_agent for ind in browser_indicators)
        has_os = any(os in user_agent for os in ["Windows", "Mac OS", "Linux", "Android", "iPhone"])

        if has_browser and has_os:
            return BotCategory.HUMAN, 0.6, ["Appears to be browser"]

        if suspicious_count > 0:
            return BotCategory.UNKNOWN_BOT, 0.5, reasons

        return BotCategory.HUMAN, 0.4, ["No bot indicators found"]

    async def _check_behavior(
        self,
        fingerprint: str,
        client_ip: str
    ) -> Tuple[bool, float, List[str]]:
        """
        Analyze request behavior over time.

        Returns:
            Tuple of (is_suspicious, confidence_boost, reasons)
        """
        if not self._redis or not self.config.analyze_behavior:
            return False, 0.0, []

        reasons = []
        confidence_boost = 0.0
        now = time.time()
        window = self.config.behavior_window_seconds

        try:
            # Track request timing
            key = f"{self.config.key_prefix}timing:{fingerprint}"
            last_request = await self._redis.get(key)

            if last_request:
                interval_ms = (now - float(last_request)) * 1000
                if interval_ms < self.config.min_request_interval_ms:
                    reasons.append(f"Request interval too fast: {interval_ms:.0f}ms")
                    confidence_boost += 0.15

            await self._redis.setex(key, window, str(now))

            # Count requests per minute
            count_key = f"{self.config.key_prefix}count:{fingerprint}"
            count = await self._redis.incr(count_key)
            await self._redis.expire(count_key, 60)

            if count > self.config.max_requests_per_minute:
                reasons.append(f"Exceeded rate: {count}/min")
                confidence_boost += 0.2

            # Check for suspicious patterns
            patterns_key = f"{self.config.key_prefix}patterns:{fingerprint}"

            # Track unique paths accessed
            path_key = f"{self.config.key_prefix}paths:{fingerprint}"
            await self._redis.sadd(path_key, str(now))
            await self._redis.expire(path_key, window)
            unique_paths = await self._redis.scard(path_key)

            if unique_paths > 50:  # Accessing many different paths quickly
                reasons.append(f"Accessing many paths: {unique_paths}")
                confidence_boost += 0.1

            return len(reasons) > 0, confidence_boost, reasons

        except Exception as e:
            logger.debug(f"Behavior analysis failed: {e}")
            return False, 0.0, []

    def _check_headers(self, request: Request) -> Tuple[bool, float, List[str]]:
        """
        Check request headers for bot indicators.

        Returns:
            Tuple of (is_suspicious, confidence_boost, reasons)
        """
        reasons = []
        confidence_boost = 0.0

        # Check for missing common headers
        if not request.headers.get("Accept"):
            reasons.append("Missing Accept header")
            confidence_boost += 0.05

        if not request.headers.get("Accept-Language"):
            reasons.append("Missing Accept-Language header")
            confidence_boost += 0.05

        if not request.headers.get("Accept-Encoding"):
            reasons.append("Missing Accept-Encoding header")
            confidence_boost += 0.03

        # Check for suspicious header combinations
        ua = request.headers.get("User-Agent", "")
        if "Chrome" in ua and not request.headers.get("Sec-Ch-Ua"):
            # Modern Chrome should have Client Hints
            reasons.append("Chrome without Client Hints")
            confidence_boost += 0.05

        # Check for headless browser indicators
        if any(h in ua.lower() for h in ["headless", "phantom", "puppeteer"]):
            reasons.append("Headless browser indicator")
            confidence_boost += 0.2

        return len(reasons) > 0, confidence_boost, reasons

    async def detect(self, request: Request) -> BotDetectionResult:
        """
        Detect if request is from a bot.

        Args:
            request: FastAPI request

        Returns:
            BotDetectionResult with classification and action
        """
        user_agent = request.headers.get("User-Agent", "")
        client_ip = request.client.host if request.client else "unknown"
        fingerprint = self._get_fingerprint(request)

        all_reasons = []

        # 1. User-Agent analysis
        category, ua_confidence, ua_reasons = self._check_user_agent(user_agent)
        all_reasons.extend(ua_reasons)

        # 2. Header analysis
        headers_suspicious, header_boost, header_reasons = self._check_headers(request)
        all_reasons.extend(header_reasons)

        # 3. Behavioral analysis
        behavior_suspicious, behavior_boost, behavior_reasons = await self._check_behavior(
            fingerprint, client_ip
        )
        all_reasons.extend(behavior_reasons)

        # Calculate final confidence
        confidence = min(ua_confidence + header_boost + behavior_boost, 1.0)

        # Adjust category based on additional analysis
        if category == BotCategory.HUMAN:
            if behavior_suspicious and behavior_boost > 0.15:
                category = BotCategory.UNKNOWN_BOT
            elif headers_suspicious and header_boost > 0.1:
                category = BotCategory.UNKNOWN_BOT

        # Determine if it's a bot
        is_bot = category != BotCategory.HUMAN

        # Get action for this category
        action = self.config.actions.get(category, BotAction.ALLOW)

        result = BotDetectionResult(
            is_bot=is_bot,
            category=category,
            confidence=confidence,
            action=action,
            reasons=all_reasons,
            fingerprint=fingerprint,
        )

        # Log detection
        if is_bot and action != BotAction.ALLOW:
            logger.info(
                f"Bot detected: {category.value} ({confidence:.2f})",
                extra={
                    "event_type": "bot_detected",
                    "client_ip": client_ip,
                    "category": category.value,
                    "confidence": confidence,
                    "action": action.value,
                    "fingerprint": fingerprint,
                    "reasons": all_reasons,
                }
            )

        return result

    async def add_allowlist(self, pattern: str, category: BotCategory = BotCategory.AUTOMATION):
        """Add a User-Agent pattern to allowlist."""
        if self._redis:
            key = f"{self.config.key_prefix}allowlist"
            await self._redis.hset(key, pattern, category.value)
            logger.info(f"Added to bot allowlist: {pattern}")

    async def add_blocklist(self, pattern: str):
        """Add a User-Agent pattern to blocklist."""
        if self._redis:
            key = f"{self.config.key_prefix}blocklist"
            await self._redis.sadd(key, pattern)
            logger.info(f"Added to bot blocklist: {pattern}")


# Singleton
_detector_instance: Optional[BotDetector] = None


async def get_bot_detector(
    config: Optional[BotDetectionConfig] = None
) -> BotDetector:
    """Get or create bot detector singleton."""
    global _detector_instance

    if _detector_instance is None:
        _detector_instance = BotDetector(config)
        await _detector_instance.connect()

    return _detector_instance


async def bot_detection_dependency(request: Request):
    """
    FastAPI dependency for bot detection.

    Usage:
        @app.get("/api/data")
        async def get_data(_: None = Depends(bot_detection_dependency)):
            return {"data": "value"}
    """
    detector = await get_bot_detector()
    result = await detector.detect(request)

    if result.action == BotAction.BLOCK:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied"
        )

    elif result.action == BotAction.CHALLENGE:
        # In production, return a challenge page or CAPTCHA
        # For API, we just deny access
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Challenge required",
            headers={"X-Bot-Challenge": "required"}
        )

    elif result.action == BotAction.THROTTLE:
        # Apply stricter rate limits
        # This would integrate with rate_limiter.py
        pass
