"""
Web Crawler Module for DriveSentinel RAG System
Based on Crawl4AI integration for web content extraction
"""

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse
import re

logger = logging.getLogger(__name__)

# Try to import crawl4ai - optional dependency
try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
    from crawl4ai.extraction_strategy import LLMExtractionStrategy
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False
    logger.warning("crawl4ai not installed. Run: pip install crawl4ai")

# Try to import BeautifulSoup for fallback
try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    BS4_AVAILABLE = False

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


@dataclass
class CrawlConfig:
    """Configuration for web crawling"""
    max_depth: int = 2
    max_pages: int = 50
    delay_between_requests: float = 1.0
    timeout: int = 30
    user_agent: str = "DriveSentinel-Bot/1.0"
    respect_robots_txt: bool = True
    extract_links: bool = True
    extract_images: bool = False
    wait_for_js: bool = True
    js_wait_time: int = 2000
    allowed_domains: List[str] = field(default_factory=list)
    blocked_patterns: List[str] = field(default_factory=lambda: [
        r'\.pdf$', r'\.zip$', r'\.exe$', r'\.dmg$',
        r'/login', r'/signup', r'/auth', r'/admin'
    ])


@dataclass
class CrawlResult:
    """Result from a single page crawl"""
    url: str
    title: str
    content: str
    markdown: str
    links: List[str]
    metadata: Dict[str, Any]
    crawled_at: datetime
    success: bool
    error: Optional[str] = None
    word_count: int = 0
    content_hash: str = ""

    def __post_init__(self):
        if not self.content_hash and self.content:
            self.content_hash = hashlib.sha256(self.content.encode()).hexdigest()[:16]
        if not self.word_count and self.content:
            self.word_count = len(self.content.split())


@dataclass
class CrawlSession:
    """Tracks state for a crawl session"""
    session_id: str
    start_url: str
    config: CrawlConfig
    visited_urls: Set[str] = field(default_factory=set)
    pending_urls: List[str] = field(default_factory=list)
    results: List[CrawlResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.utcnow)
    status: str = "pending"
    error: Optional[str] = None

    def __post_init__(self):
        if not self.session_id:
            self.session_id = hashlib.sha256(
                f"{self.start_url}{time.time()}".encode()
            ).hexdigest()[:12]


class CrawlCache:
    """In-memory cache for crawl results"""

    def __init__(self, ttl_minutes: int = 60):
        self._cache: Dict[str, tuple[CrawlResult, datetime]] = {}
        self._ttl = timedelta(minutes=ttl_minutes)

    def get(self, url: str) -> Optional[CrawlResult]:
        if url in self._cache:
            result, cached_at = self._cache[url]
            if datetime.utcnow() - cached_at < self._ttl:
                return result
            del self._cache[url]
        return None

    def set(self, url: str, result: CrawlResult):
        self._cache[url] = (result, datetime.utcnow())

    def clear(self):
        self._cache.clear()

    def size(self) -> int:
        return len(self._cache)


class WebCrawler:
    """
    Web crawler for DriveSentinel RAG system.
    Supports both Crawl4AI (for JS-heavy sites) and simple HTTP crawling.
    """

    def __init__(self, config: Optional[CrawlConfig] = None):
        self.config = config or CrawlConfig()
        self.cache = CrawlCache()
        self._crawler: Optional[Any] = None
        self._http_client: Optional[Any] = None
        self._sessions: Dict[str, CrawlSession] = {}

    async def _init_crawl4ai(self):
        """Initialize Crawl4AI crawler"""
        if not CRAWL4AI_AVAILABLE:
            raise ImportError("crawl4ai not installed")

        if self._crawler is None:
            browser_config = BrowserConfig(
                headless=True,
                verbose=False,
            )
            self._crawler = AsyncWebCrawler(config=browser_config)
            await self._crawler.start()
        return self._crawler

    async def _init_http_client(self):
        """Initialize httpx client for simple crawling"""
        if not HTTPX_AVAILABLE:
            raise ImportError("httpx not installed")

        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=self.config.timeout,
                follow_redirects=True,
                headers={"User-Agent": self.config.user_agent}
            )
        return self._http_client

    async def close(self):
        """Close crawler and HTTP client"""
        if self._crawler:
            await self._crawler.close()
            self._crawler = None
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

    def _should_crawl(self, url: str) -> bool:
        """Check if URL should be crawled based on config"""
        parsed = urlparse(url)

        # Check domain restrictions
        if self.config.allowed_domains:
            if parsed.netloc not in self.config.allowed_domains:
                return False

        # Check blocked patterns
        for pattern in self.config.blocked_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False

        return True

    def _normalize_url(self, url: str, base_url: str) -> str:
        """Normalize and resolve relative URLs"""
        if url.startswith(('http://', 'https://')):
            return url
        return urljoin(base_url, url)

    def _extract_links(self, html: str, base_url: str) -> List[str]:
        """Extract links from HTML content"""
        links = []
        if not BS4_AVAILABLE:
            # Fallback regex extraction
            href_pattern = r'href=["\']([^"\']+)["\']'
            for match in re.finditer(href_pattern, html):
                href = match.group(1)
                if href and not href.startswith(('#', 'javascript:', 'mailto:')):
                    normalized = self._normalize_url(href, base_url)
                    if self._should_crawl(normalized):
                        links.append(normalized)
        else:
            soup = BeautifulSoup(html, 'html.parser')
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if href and not href.startswith(('#', 'javascript:', 'mailto:')):
                    normalized = self._normalize_url(href, base_url)
                    if self._should_crawl(normalized):
                        links.append(normalized)
        return list(set(links))

    def _html_to_markdown(self, html: str) -> str:
        """Convert HTML to markdown-like text"""
        if not BS4_AVAILABLE:
            # Simple regex-based extraction
            text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
            text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
            text = re.sub(r'<[^>]+>', '\n', text)
            text = re.sub(r'\n\s*\n', '\n\n', text)
            return text.strip()

        soup = BeautifulSoup(html, 'html.parser')

        # Remove scripts, styles, nav, footer
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            tag.decompose()

        # Process content
        lines = []
        for element in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'li', 'pre', 'code']):
            tag_name = element.name
            text = element.get_text(strip=True)

            if not text:
                continue

            if tag_name == 'h1':
                lines.append(f"# {text}\n")
            elif tag_name == 'h2':
                lines.append(f"## {text}\n")
            elif tag_name == 'h3':
                lines.append(f"### {text}\n")
            elif tag_name in ('h4', 'h5', 'h6'):
                lines.append(f"#### {text}\n")
            elif tag_name == 'li':
                lines.append(f"- {text}")
            elif tag_name in ('pre', 'code'):
                lines.append(f"```\n{text}\n```")
            else:
                lines.append(f"{text}\n")

        return '\n'.join(lines)

    def _extract_title(self, html: str) -> str:
        """Extract page title from HTML"""
        if BS4_AVAILABLE:
            soup = BeautifulSoup(html, 'html.parser')
            title_tag = soup.find('title')
            if title_tag:
                return title_tag.get_text(strip=True)
            h1_tag = soup.find('h1')
            if h1_tag:
                return h1_tag.get_text(strip=True)
        else:
            # Regex fallback
            match = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        return "Untitled"

    async def crawl_page_simple(self, url: str) -> CrawlResult:
        """Crawl a single page using simple HTTP request"""
        try:
            client = await self._init_http_client()
            response = await client.get(url)
            response.raise_for_status()

            html = response.text
            title = self._extract_title(html)
            markdown = self._html_to_markdown(html)
            links = self._extract_links(html, url) if self.config.extract_links else []

            return CrawlResult(
                url=url,
                title=title,
                content=markdown,
                markdown=markdown,
                links=links,
                metadata={
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "crawl_method": "httpx"
                },
                crawled_at=datetime.utcnow(),
                success=True
            )
        except Exception as e:
            logger.error(f"Simple crawl failed for {url}: {e}")
            return CrawlResult(
                url=url,
                title="",
                content="",
                markdown="",
                links=[],
                metadata={"crawl_method": "httpx"},
                crawled_at=datetime.utcnow(),
                success=False,
                error=str(e)
            )

    async def crawl_page_js(self, url: str) -> CrawlResult:
        """Crawl a single page with JavaScript rendering using Crawl4AI"""
        if not CRAWL4AI_AVAILABLE:
            logger.warning("Crawl4AI not available, falling back to simple crawl")
            return await self.crawl_page_simple(url)

        try:
            crawler = await self._init_crawl4ai()

            run_config = CrawlerRunConfig(
                wait_until="networkidle",
                delay_before_return_html=self.config.js_wait_time / 1000,
            )

            result = await crawler.arun(url=url, config=run_config)

            if result.success:
                links = self._extract_links(result.html, url) if self.config.extract_links else []

                return CrawlResult(
                    url=url,
                    title=result.metadata.get("title", self._extract_title(result.html)),
                    content=result.markdown or self._html_to_markdown(result.html),
                    markdown=result.markdown or self._html_to_markdown(result.html),
                    links=links,
                    metadata={
                        "crawl_method": "crawl4ai",
                        **result.metadata
                    },
                    crawled_at=datetime.utcnow(),
                    success=True
                )
            else:
                raise Exception(result.error_message or "Crawl failed")

        except Exception as e:
            logger.error(f"JS crawl failed for {url}: {e}")
            # Fallback to simple crawl
            return await self.crawl_page_simple(url)

    async def crawl_page(self, url: str, use_js: bool = True) -> CrawlResult:
        """
        Crawl a single page with caching.

        Args:
            url: URL to crawl
            use_js: Whether to use JavaScript rendering
        """
        # Check cache first
        cached = self.cache.get(url)
        if cached:
            logger.debug(f"Cache hit for {url}")
            return cached

        # Crawl the page
        if use_js and CRAWL4AI_AVAILABLE:
            result = await self.crawl_page_js(url)
        else:
            result = await self.crawl_page_simple(url)

        # Cache successful results
        if result.success:
            self.cache.set(url, result)

        return result

    async def crawl_site(
        self,
        start_url: str,
        max_depth: Optional[int] = None,
        max_pages: Optional[int] = None,
        use_js: bool = True
    ) -> AsyncIterator[CrawlResult]:
        """
        Crawl a website starting from the given URL.

        Args:
            start_url: Starting URL
            max_depth: Maximum crawl depth (default from config)
            max_pages: Maximum pages to crawl (default from config)
            use_js: Whether to use JavaScript rendering

        Yields:
            CrawlResult for each crawled page
        """
        max_depth = max_depth or self.config.max_depth
        max_pages = max_pages or self.config.max_pages

        visited: Set[str] = set()
        queue: List[tuple[str, int]] = [(start_url, 0)]  # (url, depth)
        pages_crawled = 0

        while queue and pages_crawled < max_pages:
            url, depth = queue.pop(0)

            if url in visited:
                continue

            if not self._should_crawl(url):
                continue

            visited.add(url)

            logger.info(f"Crawling [{pages_crawled + 1}/{max_pages}] depth={depth}: {url}")

            result = await self.crawl_page(url, use_js=use_js)
            pages_crawled += 1

            yield result

            # Add discovered links to queue if within depth limit
            if result.success and depth < max_depth:
                for link in result.links:
                    if link not in visited:
                        queue.append((link, depth + 1))

            # Delay between requests
            if self.config.delay_between_requests > 0:
                await asyncio.sleep(self.config.delay_between_requests)

    async def crawl_urls(
        self,
        urls: List[str],
        use_js: bool = True,
        parallel: int = 3
    ) -> List[CrawlResult]:
        """
        Crawl multiple URLs in parallel.

        Args:
            urls: List of URLs to crawl
            use_js: Whether to use JavaScript rendering
            parallel: Number of concurrent crawls

        Returns:
            List of CrawlResults
        """
        results = []
        semaphore = asyncio.Semaphore(parallel)

        async def crawl_with_semaphore(url: str) -> CrawlResult:
            async with semaphore:
                result = await self.crawl_page(url, use_js=use_js)
                await asyncio.sleep(self.config.delay_between_requests)
                return result

        tasks = [crawl_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks)

        return list(results)

    def create_session(self, start_url: str, config: Optional[CrawlConfig] = None) -> str:
        """Create a new crawl session"""
        session = CrawlSession(
            session_id="",
            start_url=start_url,
            config=config or self.config
        )
        self._sessions[session.session_id] = session
        return session.session_id

    def get_session(self, session_id: str) -> Optional[CrawlSession]:
        """Get a crawl session by ID"""
        return self._sessions.get(session_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get crawler statistics"""
        return {
            "cache_size": self.cache.size(),
            "active_sessions": len(self._sessions),
            "crawl4ai_available": CRAWL4AI_AVAILABLE,
            "bs4_available": BS4_AVAILABLE,
            "httpx_available": HTTPX_AVAILABLE
        }


# Singleton instance
_crawler_instance: Optional[WebCrawler] = None


def get_crawler(config: Optional[CrawlConfig] = None) -> WebCrawler:
    """Get or create singleton crawler instance"""
    global _crawler_instance
    if _crawler_instance is None:
        _crawler_instance = WebCrawler(config)
    return _crawler_instance


async def cleanup_crawler():
    """Cleanup crawler resources"""
    global _crawler_instance
    if _crawler_instance:
        await _crawler_instance.close()
        _crawler_instance = None
