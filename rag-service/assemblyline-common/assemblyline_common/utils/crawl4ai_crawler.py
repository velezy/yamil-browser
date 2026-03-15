"""
T.A.L.O.S. Web Crawler with crawl4ai
Based on Memobytes patterns

Features:
- LLM-optimized async web crawling
- JavaScript rendering with Playwright
- Intelligent content extraction
- Rate limiting and caching
"""

import os
import logging
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# =============================================================================
# CRAWL4AI AVAILABILITY CHECK
# =============================================================================

CRAWL4AI_AVAILABLE = False

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
    from crawl4ai.extraction_strategy import LLMExtractionStrategy
    CRAWL4AI_AVAILABLE = True
    logger.info("crawl4ai library loaded successfully")
except ImportError:
    logger.warning("crawl4ai not installed. Run: pip install crawl4ai")

PLAYWRIGHT_AVAILABLE = False

try:
    from playwright.async_api import async_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    logger.warning("Playwright not installed. Run: pip install playwright && playwright install chromium")


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class CrawlerConfig:
    """Web crawler configuration"""
    headless: bool = True
    timeout: int = 30000  # 30 seconds
    max_pages: int = 100
    delay_between_requests: float = 1.0
    respect_robots_txt: bool = True
    user_agent: str = "TALOS-Crawler/2.0 (Research Bot)"
    cache_enabled: bool = True
    cache_ttl: int = 3600  # 1 hour


@dataclass
class CrawlResult:
    """Result of a crawl operation"""
    url: str
    title: str = ""
    content: str = ""
    markdown: str = ""
    links: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    crawled_at: datetime = field(default_factory=datetime.utcnow)
    success: bool = True
    error: Optional[str] = None


# =============================================================================
# WEB CRAWLER SERVICE
# =============================================================================

class WebCrawlerService:
    """
    crawl4ai-based web crawler for T.A.L.O.S.

    Features:
    - Async web crawling with JavaScript rendering
    - LLM-optimized content extraction
    - Intelligent markdown conversion
    - Link and image extraction
    """

    def __init__(self, config: Optional[CrawlerConfig] = None):
        self.config = config or CrawlerConfig()
        self._crawler = None
        self._cache: Dict[str, CrawlResult] = {}

    async def initialize(self):
        """Initialize the crawler"""
        if not CRAWL4AI_AVAILABLE:
            logger.warning("crawl4ai not available, using fallback crawler")
            return False

        try:
            browser_config = BrowserConfig(
                headless=self.config.headless,
                verbose=False
            )
            self._crawler = AsyncWebCrawler(config=browser_config)
            await self._crawler.start()
            logger.info("Web crawler initialized")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize crawler: {e}")
            return False

    async def close(self):
        """Close the crawler"""
        if self._crawler:
            await self._crawler.close()
            self._crawler = None

    async def crawl_url(
        self,
        url: str,
        extract_content: bool = True,
        extract_links: bool = True,
        extract_images: bool = False,
        use_cache: bool = True
    ) -> CrawlResult:
        """
        Crawl a single URL.

        Args:
            url: URL to crawl
            extract_content: Extract main content
            extract_links: Extract all links
            extract_images: Extract image URLs
            use_cache: Use cached results if available

        Returns:
            CrawlResult with extracted content
        """
        # Check cache
        if use_cache and self.config.cache_enabled and url in self._cache:
            cached = self._cache[url]
            age = (datetime.utcnow() - cached.crawled_at).total_seconds()
            if age < self.config.cache_ttl:
                logger.debug(f"Cache hit for {url}")
                return cached

        if CRAWL4AI_AVAILABLE and self._crawler:
            return await self._crawl_with_crawl4ai(
                url, extract_content, extract_links, extract_images
            )
        else:
            return await self._crawl_with_fallback(
                url, extract_content, extract_links, extract_images
            )

    async def _crawl_with_crawl4ai(
        self,
        url: str,
        extract_content: bool,
        extract_links: bool,
        extract_images: bool
    ) -> CrawlResult:
        """Crawl using crawl4ai library"""
        try:
            run_config = CrawlerRunConfig(
                word_count_threshold=10,
                remove_overlay_elements=True,
                process_iframes=True
            )

            result = await self._crawler.arun(
                url=url,
                config=run_config
            )

            if not result.success:
                return CrawlResult(
                    url=url,
                    success=False,
                    error=result.error_message or "Crawl failed"
                )

            crawl_result = CrawlResult(
                url=url,
                title=result.metadata.get("title", "") if result.metadata else "",
                content=result.cleaned_html or "",
                markdown=result.markdown or "",
                links=result.links.get("internal", []) + result.links.get("external", []) if result.links else [],
                images=result.media.get("images", []) if result.media else [],
                metadata=result.metadata or {},
                success=True
            )

            # Cache result
            if self.config.cache_enabled:
                self._cache[url] = crawl_result

            return crawl_result

        except Exception as e:
            logger.error(f"crawl4ai error for {url}: {e}")
            return CrawlResult(url=url, success=False, error=str(e))

    async def _crawl_with_fallback(
        self,
        url: str,
        extract_content: bool,
        extract_links: bool,
        extract_images: bool
    ) -> CrawlResult:
        """Fallback crawler using httpx and BeautifulSoup"""
        try:
            import httpx
            from bs4 import BeautifulSoup

            async with httpx.AsyncClient(
                timeout=self.config.timeout / 1000,
                follow_redirects=True,
                headers={"User-Agent": self.config.user_agent}
            ) as client:
                response = await client.get(url)
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # Extract title
            title = ""
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

            # Extract main content
            content = ""
            markdown = ""
            if extract_content:
                # Remove scripts and styles
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.decompose()

                # Get main content
                main = soup.find("main") or soup.find("article") or soup.find("body")
                if main:
                    content = main.get_text(separator="\n", strip=True)
                    markdown = self._html_to_markdown(main)

            # Extract links
            links = []
            if extract_links:
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("http"):
                        links.append(href)

            # Extract images
            images = []
            if extract_images:
                for img in soup.find_all("img", src=True):
                    src = img["src"]
                    if src.startswith("http"):
                        images.append(src)

            crawl_result = CrawlResult(
                url=url,
                title=title,
                content=content,
                markdown=markdown,
                links=links,
                images=images,
                metadata={"method": "fallback"},
                success=True
            )

            if self.config.cache_enabled:
                self._cache[url] = crawl_result

            return crawl_result

        except Exception as e:
            logger.error(f"Fallback crawler error for {url}: {e}")
            return CrawlResult(url=url, success=False, error=str(e))

    def _html_to_markdown(self, element) -> str:
        """Convert HTML element to markdown"""
        lines = []

        for child in element.descendants:
            if child.name == "h1":
                lines.append(f"# {child.get_text(strip=True)}")
            elif child.name == "h2":
                lines.append(f"## {child.get_text(strip=True)}")
            elif child.name == "h3":
                lines.append(f"### {child.get_text(strip=True)}")
            elif child.name == "p":
                text = child.get_text(strip=True)
                if text:
                    lines.append(text)
            elif child.name == "li":
                lines.append(f"- {child.get_text(strip=True)}")
            elif child.name == "code":
                lines.append(f"`{child.get_text(strip=True)}`")
            elif child.name == "pre":
                lines.append(f"```\n{child.get_text()}\n```")

        return "\n\n".join(lines)

    async def crawl_sitemap(
        self,
        sitemap_url: str,
        max_urls: int = None
    ) -> List[CrawlResult]:
        """
        Crawl URLs from a sitemap.

        Args:
            sitemap_url: URL to sitemap.xml
            max_urls: Maximum URLs to crawl

        Returns:
            List of CrawlResults
        """
        max_urls = max_urls or self.config.max_pages

        try:
            import httpx
            from bs4 import BeautifulSoup

            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(sitemap_url)
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "xml")
            urls = [loc.get_text() for loc in soup.find_all("loc")][:max_urls]

            results = []
            for url in urls:
                result = await self.crawl_url(url)
                results.append(result)
                await asyncio.sleep(self.config.delay_between_requests)

            return results

        except Exception as e:
            logger.error(f"Sitemap crawl failed: {e}")
            return []

    async def extract_structured_data(
        self,
        url: str,
        schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract structured data from a page using LLM.

        Args:
            url: URL to crawl
            schema: JSON schema for extraction

        Returns:
            Extracted structured data
        """
        if not CRAWL4AI_AVAILABLE:
            logger.warning("LLM extraction requires crawl4ai")
            return {}

        try:
            # Use LLM extraction strategy
            extraction_strategy = LLMExtractionStrategy(
                provider="ollama/gemma3:4b",
                schema=schema,
                instruction="Extract the requested information from this webpage."
            )

            run_config = CrawlerRunConfig(
                extraction_strategy=extraction_strategy
            )

            result = await self._crawler.arun(url=url, config=run_config)

            if result.success and result.extracted_content:
                return result.extracted_content

            return {}

        except Exception as e:
            logger.error(f"Structured extraction failed: {e}")
            return {}

    def clear_cache(self):
        """Clear the URL cache"""
        self._cache.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "cached_urls": len(self._cache),
            "cache_enabled": self.config.cache_enabled,
            "cache_ttl": self.config.cache_ttl
        }


# =============================================================================
# RESEARCH CRAWLER (for RAG)
# =============================================================================

class ResearchCrawler:
    """
    Specialized crawler for research and knowledge gathering.
    Optimized for RAG document ingestion.
    """

    def __init__(self, crawler: Optional[WebCrawlerService] = None):
        self.crawler = crawler or WebCrawlerService()

    async def research_topic(
        self,
        query: str,
        sources: List[str],
        max_pages_per_source: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Research a topic across multiple sources.

        Args:
            query: Research query
            sources: List of base URLs to crawl
            max_pages_per_source: Max pages per source

        Returns:
            List of research documents
        """
        await self.crawler.initialize()

        try:
            documents = []

            for source in sources:
                result = await self.crawler.crawl_url(source)

                if result.success:
                    documents.append({
                        "url": result.url,
                        "title": result.title,
                        "content": result.markdown or result.content,
                        "source": source,
                        "query": query,
                        "crawled_at": result.crawled_at.isoformat()
                    })

            return documents

        finally:
            await self.crawler.close()

    async def crawl_documentation(
        self,
        base_url: str,
        max_depth: int = 2
    ) -> List[Dict[str, Any]]:
        """
        Crawl documentation site for knowledge base.

        Args:
            base_url: Documentation base URL
            max_depth: Maximum crawl depth

        Returns:
            List of documentation pages
        """
        await self.crawler.initialize()

        try:
            visited = set()
            to_visit = [(base_url, 0)]
            documents = []

            while to_visit and len(documents) < self.crawler.config.max_pages:
                url, depth = to_visit.pop(0)

                if url in visited or depth > max_depth:
                    continue

                visited.add(url)

                result = await self.crawler.crawl_url(
                    url,
                    extract_links=True,
                    extract_content=True
                )

                if result.success:
                    documents.append({
                        "url": result.url,
                        "title": result.title,
                        "content": result.markdown or result.content,
                        "depth": depth
                    })

                    # Add child links
                    for link in result.links:
                        if link.startswith(base_url) and link not in visited:
                            to_visit.append((link, depth + 1))

                await asyncio.sleep(self.crawler.config.delay_between_requests)

            return documents

        finally:
            await self.crawler.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

_web_crawler: Optional[WebCrawlerService] = None


async def get_web_crawler() -> WebCrawlerService:
    """Get or create web crawler singleton"""
    global _web_crawler
    if _web_crawler is None:
        _web_crawler = WebCrawlerService()
        await _web_crawler.initialize()
    return _web_crawler


def is_crawl4ai_available() -> bool:
    """Check if crawl4ai is available"""
    return CRAWL4AI_AVAILABLE


async def quick_crawl(url: str) -> CrawlResult:
    """Quick crawl a single URL"""
    crawler = WebCrawlerService()
    await crawler.initialize()
    try:
        return await crawler.crawl_url(url)
    finally:
        await crawler.close()
