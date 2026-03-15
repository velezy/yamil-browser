"""
Agentic Web Crawler for DriveSentinel RAG System
Uses LLM agents for intelligent URL planning, content evaluation, and synthesis
"""

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from urllib.parse import urlparse
import re

logger = logging.getLogger(__name__)

# Import base crawler
from .web_crawler import WebCrawler, CrawlConfig, CrawlResult, get_crawler

# Try to import Ollama for LLM integration
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


@dataclass
class CrawlPlan:
    """Plan for crawling a topic"""
    topic: str
    seed_urls: List[str]
    url_patterns: List[str]
    max_pages: int
    priority_keywords: List[str]
    depth_strategy: str = "breadth_first"  # or "depth_first"
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ContentEvaluation:
    """Evaluation of crawled content"""
    url: str
    relevance_score: float  # 0.0 to 1.0
    quality_score: float
    key_topics: List[str]
    summary: str
    should_index: bool
    follow_links: List[str]
    reasoning: str


@dataclass
class SynthesizedContent:
    """Content synthesized from multiple crawled pages"""
    topic: str
    summary: str
    key_concepts: List[Dict[str, Any]]
    source_urls: List[str]
    entity_graph: Dict[str, List[str]]  # entity -> related entities
    confidence: float
    created_at: datetime = field(default_factory=datetime.utcnow)


class OllamaAgent:
    """Simple agent powered by Ollama LLM"""

    def __init__(
        self,
        name: str,
        model: str = "llama3.2",
        system_prompt: str = "",
        ollama_url: str = "http://localhost:11434"
    ):
        self.name = name
        self.model = model
        self.system_prompt = system_prompt
        self.ollama_url = ollama_url
        self._client: Optional[Any] = None

    async def _get_client(self):
        if self._client is None:
            if not HTTPX_AVAILABLE:
                raise ImportError("httpx not installed")
            self._client = httpx.AsyncClient(timeout=120)
        return self._client

    async def run(self, prompt: str, context: Optional[Dict[str, Any]] = None) -> str:
        """Run the agent with a prompt"""
        client = await self._get_client()

        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})

        # Add context if provided
        if context:
            context_str = json.dumps(context, indent=2, default=str)
            messages.append({
                "role": "user",
                "content": f"Context:\n{context_str}\n\nTask:\n{prompt}"
            })
        else:
            messages.append({"role": "user", "content": prompt})

        try:
            response = await client.post(
                f"{self.ollama_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                    "options": {"temperature": 0.3}
                }
            )
            response.raise_for_status()
            result = response.json()
            return result.get("message", {}).get("content", "")
        except Exception as e:
            logger.error(f"Ollama agent {self.name} failed: {e}")
            return ""

    async def run_json(self, prompt: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Run agent and parse JSON response"""
        response = await self.run(prompt + "\n\nRespond with valid JSON only.", context)

        # Extract JSON from response
        try:
            # Try direct parsing
            return json.loads(response)
        except json.JSONDecodeError:
            # Try to find JSON in response
            json_match = re.search(r'\{[\s\S]*\}', response)
            if json_match:
                try:
                    return json.loads(json_match.group())
                except json.JSONDecodeError:
                    pass

            # Try array
            json_match = re.search(r'\[[\s\S]*\]', response)
            if json_match:
                try:
                    return {"items": json.loads(json_match.group())}
                except json.JSONDecodeError:
                    pass

        logger.warning(f"Failed to parse JSON from agent response: {response[:200]}")
        return {}

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None


class AgenticCrawler:
    """
    Intelligent web crawler that uses LLM agents for:
    - URL planning and prioritization
    - Content relevance evaluation
    - Cross-URL content synthesis
    - Entity extraction and relationship building
    """

    def __init__(
        self,
        ollama_url: str = "http://localhost:11434",
        model: str = "llama3.2",
        crawler_config: Optional[CrawlConfig] = None
    ):
        self.ollama_url = ollama_url
        self.model = model
        self.crawler = get_crawler(crawler_config)

        # Initialize specialized agents
        self.url_planner = OllamaAgent(
            name="URL Planner",
            model=model,
            ollama_url=ollama_url,
            system_prompt="""You are a web research URL planner. Your job is to:
1. Generate relevant seed URLs for a given topic
2. Identify URL patterns that indicate relevant content
3. Prioritize URLs based on likely relevance

Always respond with valid JSON in this format:
{
    "seed_urls": ["url1", "url2", ...],
    "url_patterns": ["pattern1", "pattern2", ...],
    "priority_keywords": ["keyword1", "keyword2", ...],
    "reasoning": "explanation"
}"""
        )

        self.content_evaluator = OllamaAgent(
            name="Content Evaluator",
            model=model,
            ollama_url=ollama_url,
            system_prompt="""You are a content relevance evaluator. Your job is to:
1. Assess content relevance to a given topic (0.0-1.0 score)
2. Evaluate content quality and informativeness
3. Extract key topics and entities
4. Identify which links should be followed

Always respond with valid JSON in this format:
{
    "relevance_score": 0.85,
    "quality_score": 0.75,
    "key_topics": ["topic1", "topic2"],
    "summary": "brief summary",
    "should_index": true,
    "follow_links": ["url1", "url2"],
    "reasoning": "explanation"
}"""
        )

        self.synthesizer = OllamaAgent(
            name="Content Synthesizer",
            model=model,
            ollama_url=ollama_url,
            system_prompt="""You are a content synthesizer. Your job is to:
1. Combine information from multiple sources
2. Identify key concepts and their relationships
3. Create a coherent summary
4. Build an entity relationship graph

Always respond with valid JSON in this format:
{
    "summary": "comprehensive summary",
    "key_concepts": [{"concept": "name", "description": "...", "sources": ["url1"]}],
    "entity_graph": {"entity1": ["related1", "related2"], ...},
    "confidence": 0.85,
    "reasoning": "explanation"
}"""
        )

    async def plan_crawl(
        self,
        topic: str,
        existing_urls: Optional[List[str]] = None,
        max_pages: int = 20
    ) -> CrawlPlan:
        """
        Use LLM agent to plan crawl strategy for a topic.

        Args:
            topic: Topic to research
            existing_urls: Optional list of known relevant URLs
            max_pages: Maximum pages to crawl

        Returns:
            CrawlPlan with seed URLs and strategy
        """
        context = {
            "topic": topic,
            "existing_urls": existing_urls or [],
            "max_pages": max_pages
        }

        prompt = f"""Create a crawl plan for researching: "{topic}"

Consider:
1. What are the most authoritative sources for this topic?
2. What URL patterns indicate relevant content?
3. What keywords should we prioritize?

Generate 5-10 high-quality seed URLs from authoritative sources."""

        result = await self.url_planner.run_json(prompt, context)

        return CrawlPlan(
            topic=topic,
            seed_urls=result.get("seed_urls", []),
            url_patterns=result.get("url_patterns", []),
            max_pages=max_pages,
            priority_keywords=result.get("priority_keywords", [])
        )

    async def evaluate_content(
        self,
        crawl_result: CrawlResult,
        topic: str,
        context: Optional[Dict[str, Any]] = None
    ) -> ContentEvaluation:
        """
        Use LLM agent to evaluate crawled content relevance.

        Args:
            crawl_result: Result from web crawler
            topic: Research topic for relevance scoring
            context: Optional additional context

        Returns:
            ContentEvaluation with scores and recommendations
        """
        # Truncate content for LLM context
        content_preview = crawl_result.content[:4000] if crawl_result.content else ""

        eval_context = {
            "topic": topic,
            "url": crawl_result.url,
            "title": crawl_result.title,
            "content_preview": content_preview,
            "word_count": crawl_result.word_count,
            "available_links": crawl_result.links[:20],
            **(context or {})
        }

        prompt = f"""Evaluate this web page for the topic: "{topic}"

Title: {crawl_result.title}
URL: {crawl_result.url}
Word Count: {crawl_result.word_count}

Content:
{content_preview}

Assess relevance, quality, and which links (if any) should be followed."""

        result = await self.content_evaluator.run_json(prompt, eval_context)

        return ContentEvaluation(
            url=crawl_result.url,
            relevance_score=float(result.get("relevance_score", 0.5)),
            quality_score=float(result.get("quality_score", 0.5)),
            key_topics=result.get("key_topics", []),
            summary=result.get("summary", ""),
            should_index=result.get("should_index", True),
            follow_links=result.get("follow_links", []),
            reasoning=result.get("reasoning", "")
        )

    async def synthesize_content(
        self,
        crawl_results: List[CrawlResult],
        evaluations: List[ContentEvaluation],
        topic: str
    ) -> SynthesizedContent:
        """
        Use LLM agent to synthesize content from multiple pages.

        Args:
            crawl_results: List of crawl results
            evaluations: List of content evaluations
            topic: Research topic

        Returns:
            SynthesizedContent with summary and entity graph
        """
        # Prepare content summaries
        content_items = []
        for result, eval in zip(crawl_results, evaluations):
            if eval.should_index:
                content_items.append({
                    "url": result.url,
                    "title": result.title,
                    "summary": eval.summary,
                    "key_topics": eval.key_topics,
                    "relevance": eval.relevance_score
                })

        context = {
            "topic": topic,
            "source_count": len(content_items),
            "content_items": content_items
        }

        prompt = f"""Synthesize information about "{topic}" from {len(content_items)} sources.

Create:
1. A comprehensive summary combining all relevant information
2. A list of key concepts with descriptions and sources
3. An entity relationship graph showing how concepts relate

Sources:
{json.dumps(content_items, indent=2)}"""

        result = await self.synthesizer.run_json(prompt, context)

        return SynthesizedContent(
            topic=topic,
            summary=result.get("summary", ""),
            key_concepts=result.get("key_concepts", []),
            source_urls=[r.url for r in crawl_results if any(
                e.should_index and e.url == r.url for e in evaluations
            )],
            entity_graph=result.get("entity_graph", {}),
            confidence=float(result.get("confidence", 0.5))
        )

    async def research_topic(
        self,
        topic: str,
        seed_urls: Optional[List[str]] = None,
        max_pages: int = 10,
        min_relevance: float = 0.5,
        use_js: bool = True,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Dict[str, Any]:
        """
        Perform agentic research on a topic.

        Args:
            topic: Topic to research
            seed_urls: Optional starting URLs (will generate if not provided)
            max_pages: Maximum pages to crawl
            min_relevance: Minimum relevance score to include content
            use_js: Whether to use JavaScript rendering
            progress_callback: Optional callback(status, current, total)

        Returns:
            Research results with crawled content, evaluations, and synthesis
        """
        results = {
            "topic": topic,
            "crawl_plan": None,
            "crawl_results": [],
            "evaluations": [],
            "synthesis": None,
            "stats": {
                "pages_crawled": 0,
                "pages_indexed": 0,
                "avg_relevance": 0.0
            }
        }

        # Step 1: Plan crawl (if no seed URLs provided)
        if not seed_urls:
            if progress_callback:
                progress_callback("Planning crawl strategy", 0, max_pages)

            plan = await self.plan_crawl(topic, max_pages=max_pages)
            results["crawl_plan"] = {
                "seed_urls": plan.seed_urls,
                "url_patterns": plan.url_patterns,
                "priority_keywords": plan.priority_keywords
            }
            seed_urls = plan.seed_urls

        if not seed_urls:
            logger.warning(f"No seed URLs generated for topic: {topic}")
            return results

        # Step 2: Crawl pages with evaluation
        crawl_results = []
        evaluations = []
        urls_to_crawl = list(seed_urls[:max_pages])
        crawled_urls = set()

        page_num = 0
        while urls_to_crawl and page_num < max_pages:
            url = urls_to_crawl.pop(0)

            if url in crawled_urls:
                continue

            crawled_urls.add(url)
            page_num += 1

            if progress_callback:
                progress_callback(f"Crawling: {url[:50]}...", page_num, max_pages)

            # Crawl the page
            crawl_result = await self.crawler.crawl_page(url, use_js=use_js)

            if not crawl_result.success:
                logger.warning(f"Failed to crawl {url}: {crawl_result.error}")
                continue

            crawl_results.append(crawl_result)

            # Evaluate content
            evaluation = await self.evaluate_content(crawl_result, topic)
            evaluations.append(evaluation)

            # Add recommended links to queue
            if evaluation.relevance_score >= min_relevance:
                for link in evaluation.follow_links:
                    if link not in crawled_urls and link not in urls_to_crawl:
                        urls_to_crawl.append(link)

            # Rate limiting
            await asyncio.sleep(self.crawler.config.delay_between_requests)

        results["crawl_results"] = [
            {
                "url": r.url,
                "title": r.title,
                "content": r.content[:500] + "..." if len(r.content) > 500 else r.content,
                "word_count": r.word_count,
                "success": r.success
            }
            for r in crawl_results
        ]

        results["evaluations"] = [
            {
                "url": e.url,
                "relevance_score": e.relevance_score,
                "quality_score": e.quality_score,
                "key_topics": e.key_topics,
                "summary": e.summary,
                "should_index": e.should_index
            }
            for e in evaluations
        ]

        # Step 3: Synthesize content
        if progress_callback:
            progress_callback("Synthesizing content", max_pages, max_pages)

        indexed_results = [r for r, e in zip(crawl_results, evaluations) if e.should_index]
        indexed_evals = [e for e in evaluations if e.should_index]

        if indexed_results:
            synthesis = await self.synthesize_content(
                indexed_results, indexed_evals, topic
            )
            results["synthesis"] = {
                "summary": synthesis.summary,
                "key_concepts": synthesis.key_concepts,
                "entity_graph": synthesis.entity_graph,
                "confidence": synthesis.confidence,
                "source_count": len(synthesis.source_urls)
            }

        # Calculate stats
        results["stats"] = {
            "pages_crawled": len(crawl_results),
            "pages_indexed": len(indexed_results),
            "avg_relevance": sum(e.relevance_score for e in evaluations) / len(evaluations) if evaluations else 0.0
        }

        return results

    async def close(self):
        """Close all agents and crawler"""
        await self.url_planner.close()
        await self.content_evaluator.close()
        await self.synthesizer.close()
        await self.crawler.close()


# Singleton instance
_agentic_crawler: Optional[AgenticCrawler] = None


def get_agentic_crawler(
    ollama_url: str = "http://localhost:11434",
    model: str = "llama3.2"
) -> AgenticCrawler:
    """Get or create singleton agentic crawler instance"""
    global _agentic_crawler
    if _agentic_crawler is None:
        _agentic_crawler = AgenticCrawler(ollama_url=ollama_url, model=model)
    return _agentic_crawler


async def cleanup_agentic_crawler():
    """Cleanup agentic crawler resources"""
    global _agentic_crawler
    if _agentic_crawler:
        await _agentic_crawler.close()
        _agentic_crawler = None
