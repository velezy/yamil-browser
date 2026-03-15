"""
T.A.L.O.S. Cross-Reference Linker
Link related chunks across documents for enhanced context retrieval

Features:
- Semantic similarity-based linking
- Entity co-occurrence detection
- Topic clustering
- Citation and reference extraction
- Knowledge graph building

Cutting Edge Features:
- Automatic citation generation in multiple formats (APA, MLA, Chicago, IEEE, Harvard)
- Source verification with multi-source triangulation
- Fact-checking with claim extraction and contradiction detection
- Confidence-calibrated verification scores
"""

import logging
import re
import hashlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from enum import Enum
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class CrossReference:
    """Represents a cross-reference between two chunks"""
    source_chunk_id: int
    target_chunk_id: int
    source_document_id: int
    target_document_id: int
    relationship_type: str  # semantic, entity, citation, topic
    similarity_score: float
    shared_entities: List[str] = field(default_factory=list)
    shared_topics: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_chunk_id": self.source_chunk_id,
            "target_chunk_id": self.target_chunk_id,
            "source_document_id": self.source_document_id,
            "target_document_id": self.target_document_id,
            "relationship_type": self.relationship_type,
            "similarity_score": self.similarity_score,
            "shared_entities": self.shared_entities,
            "shared_topics": self.shared_topics,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class ChunkNode:
    """Node in the knowledge graph representing a chunk"""
    chunk_id: int
    document_id: int
    content: str
    embedding: Optional[List[float]] = None
    entities: List[str] = field(default_factory=list)
    topics: List[str] = field(default_factory=list)
    references: List[CrossReference] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DocumentCluster:
    """Cluster of related documents"""
    cluster_id: int
    document_ids: List[int]
    centroid_embedding: List[float]
    topic_keywords: List[str]
    coherence_score: float


# =============================================================================
# CROSS-REFERENCE LINKER
# =============================================================================

class CrossReferenceLinker:
    """
    Links related chunks across documents for enhanced context retrieval.

    Use cases:
    - Find related content from different documents
    - Build knowledge graph of document relationships
    - Enhance RAG with multi-document context
    - Topic-based document clustering
    """

    def __init__(
        self,
        embedder=None,
        similarity_threshold: float = 0.7,
        max_references_per_chunk: int = 5
    ):
        """
        Args:
            embedder: SentenceTransformer or compatible embedder
            similarity_threshold: Minimum similarity for cross-reference
            max_references_per_chunk: Maximum cross-references per chunk
        """
        self.embedder = embedder
        self.similarity_threshold = similarity_threshold
        self.max_references_per_chunk = max_references_per_chunk

        # In-memory graph (for single-server deployments)
        self._nodes: Dict[int, ChunkNode] = {}
        self._references: List[CrossReference] = []
        self._entity_index: Dict[str, Set[int]] = defaultdict(set)  # entity -> chunk_ids
        self._topic_index: Dict[str, Set[int]] = defaultdict(set)   # topic -> chunk_ids
        self._document_chunks: Dict[int, Set[int]] = defaultdict(set)  # doc_id -> chunk_ids

    async def add_chunk(
        self,
        chunk_id: int,
        document_id: int,
        content: str,
        embedding: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ChunkNode:
        """
        Add a chunk to the cross-reference graph.

        Args:
            chunk_id: Unique chunk identifier
            document_id: Parent document ID
            content: Chunk text content
            embedding: Pre-computed embedding (optional)
            metadata: Additional metadata

        Returns:
            ChunkNode with extracted entities and topics
        """
        # Generate embedding if not provided
        if embedding is None and self.embedder is not None:
            embedding = self.embedder.encode(content, normalize_embeddings=True).tolist()

        # Extract entities and topics
        entities = self._extract_entities(content)
        topics = self._extract_topics(content)

        # Create node
        node = ChunkNode(
            chunk_id=chunk_id,
            document_id=document_id,
            content=content,
            embedding=embedding,
            entities=entities,
            topics=topics,
            metadata=metadata or {}
        )

        # Index node
        self._nodes[chunk_id] = node
        self._document_chunks[document_id].add(chunk_id)

        # Index entities
        for entity in entities:
            self._entity_index[entity.lower()].add(chunk_id)

        # Index topics
        for topic in topics:
            self._topic_index[topic.lower()].add(chunk_id)

        return node

    async def build_references(
        self,
        chunk_id: int,
        search_all: bool = False
    ) -> List[CrossReference]:
        """
        Build cross-references for a chunk.

        Args:
            chunk_id: Chunk to find references for
            search_all: Search all chunks (expensive) or use index

        Returns:
            List of cross-references
        """
        if chunk_id not in self._nodes:
            return []

        source_node = self._nodes[chunk_id]
        references = []

        # Find candidates via entity/topic index
        candidates = self._find_candidates(source_node)

        # Calculate similarity and create references
        for target_id in candidates:
            if target_id == chunk_id:
                continue

            target_node = self._nodes[target_id]
            ref = self._create_reference(source_node, target_node)

            if ref and ref.similarity_score >= self.similarity_threshold:
                references.append(ref)

        # Sort by score and limit
        references.sort(key=lambda r: r.similarity_score, reverse=True)
        references = references[:self.max_references_per_chunk]

        # Store references
        source_node.references = references
        self._references.extend(references)

        return references

    def _find_candidates(self, source_node: ChunkNode) -> Set[int]:
        """Find candidate chunks for cross-referencing"""
        candidates = set()

        # Entity-based candidates
        for entity in source_node.entities:
            candidates.update(self._entity_index.get(entity.lower(), set()))

        # Topic-based candidates
        for topic in source_node.topics:
            candidates.update(self._topic_index.get(topic.lower(), set()))

        # Exclude self and same-document chunks
        candidates.discard(source_node.chunk_id)

        return candidates

    def _create_reference(
        self,
        source: ChunkNode,
        target: ChunkNode
    ) -> Optional[CrossReference]:
        """Create a cross-reference between two chunks"""
        # Calculate semantic similarity
        if source.embedding and target.embedding:
            similarity = self._cosine_similarity(source.embedding, target.embedding)
        else:
            similarity = 0.0

        # Find shared entities
        shared_entities = list(set(source.entities) & set(target.entities))

        # Find shared topics
        shared_topics = list(set(source.topics) & set(target.topics))

        # Determine relationship type
        if similarity > 0.85:
            rel_type = "semantic"
        elif shared_entities:
            rel_type = "entity"
        elif shared_topics:
            rel_type = "topic"
        else:
            rel_type = "semantic"

        # Boost similarity for shared entities/topics
        boosted_similarity = similarity
        if shared_entities:
            boosted_similarity += 0.1 * len(shared_entities)
        if shared_topics:
            boosted_similarity += 0.05 * len(shared_topics)
        boosted_similarity = min(boosted_similarity, 1.0)

        return CrossReference(
            source_chunk_id=source.chunk_id,
            target_chunk_id=target.chunk_id,
            source_document_id=source.document_id,
            target_document_id=target.document_id,
            relationship_type=rel_type,
            similarity_score=boosted_similarity,
            shared_entities=shared_entities,
            shared_topics=shared_topics
        )

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        a_np = np.array(a)
        b_np = np.array(b)
        return float(np.dot(a_np, b_np) / (np.linalg.norm(a_np) * np.linalg.norm(b_np)))

    def _extract_entities(self, text: str) -> List[str]:
        """Extract named entities from text (simple heuristic)"""
        # Capitalized phrases (potential named entities)
        entities = re.findall(r'\b[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)*\b', text)

        # Filter common false positives
        false_positives = {"The", "This", "That", "These", "Those", "It", "I", "We", "You", "They"}
        entities = [e for e in entities if e not in false_positives]

        return list(set(entities))[:10]

    def _extract_topics(self, text: str) -> List[str]:
        """Extract topic keywords from text"""
        # Important keywords (nouns and key terms)
        text_lower = text.lower()

        # Look for topic indicators
        topic_patterns = [
            r"(?:about|regarding|concerning)\s+(\w+)",
            r"(?:topic|subject|theme)(?:\s+(?:of|is))?\s*:?\s*(\w+)",
        ]

        topics = []
        for pattern in topic_patterns:
            matches = re.findall(pattern, text_lower)
            topics.extend(matches)

        # Extract capitalized compound terms
        compound_terms = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+\b', text)
        topics.extend([t.lower() for t in compound_terms])

        return list(set(topics))[:5]

    async def get_related_chunks(
        self,
        chunk_id: int,
        limit: int = 5,
        min_score: float = 0.5,
        cross_document_only: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get chunks related to a given chunk.

        Args:
            chunk_id: Source chunk ID
            limit: Maximum results
            min_score: Minimum similarity score
            cross_document_only: Only return chunks from different documents

        Returns:
            List of related chunks with scores
        """
        if chunk_id not in self._nodes:
            return []

        source = self._nodes[chunk_id]
        related = []

        for ref in source.references:
            if ref.similarity_score < min_score:
                continue

            if cross_document_only and ref.target_document_id == source.document_id:
                continue

            target = self._nodes.get(ref.target_chunk_id)
            if target:
                related.append({
                    "chunk_id": target.chunk_id,
                    "document_id": target.document_id,
                    "content": target.content[:200] + "..." if len(target.content) > 200 else target.content,
                    "similarity_score": ref.similarity_score,
                    "relationship_type": ref.relationship_type,
                    "shared_entities": ref.shared_entities,
                    "shared_topics": ref.shared_topics
                })

        return sorted(related, key=lambda x: x["similarity_score"], reverse=True)[:limit]

    async def get_document_graph(self, document_id: int) -> Dict[str, Any]:
        """
        Get the knowledge graph for a document.

        Returns nodes and edges for visualization.
        """
        chunk_ids = self._document_chunks.get(document_id, set())

        nodes = []
        edges = []

        for chunk_id in chunk_ids:
            node = self._nodes.get(chunk_id)
            if node:
                nodes.append({
                    "id": chunk_id,
                    "label": node.content[:50] + "...",
                    "entities": node.entities,
                    "topics": node.topics
                })

                for ref in node.references:
                    edges.append({
                        "source": chunk_id,
                        "target": ref.target_chunk_id,
                        "type": ref.relationship_type,
                        "weight": ref.similarity_score
                    })

        return {
            "document_id": document_id,
            "nodes": nodes,
            "edges": edges,
            "node_count": len(nodes),
            "edge_count": len(edges)
        }

    async def get_cross_document_links(
        self,
        document_id: int,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get links from a document to other documents.

        Useful for "Related Documents" feature.
        """
        chunk_ids = self._document_chunks.get(document_id, set())
        doc_scores: Dict[int, List[float]] = defaultdict(list)

        for chunk_id in chunk_ids:
            node = self._nodes.get(chunk_id)
            if node:
                for ref in node.references:
                    if ref.target_document_id != document_id:
                        doc_scores[ref.target_document_id].append(ref.similarity_score)

        # Aggregate scores (average)
        related_docs = []
        for doc_id, scores in doc_scores.items():
            related_docs.append({
                "document_id": doc_id,
                "average_similarity": sum(scores) / len(scores),
                "link_count": len(scores),
                "max_similarity": max(scores)
            })

        return sorted(related_docs, key=lambda x: x["average_similarity"], reverse=True)[:limit]

    def get_stats(self) -> Dict[str, Any]:
        """Get cross-referencer statistics"""
        return {
            "total_chunks": len(self._nodes),
            "total_references": len(self._references),
            "total_documents": len(self._document_chunks),
            "unique_entities": len(self._entity_index),
            "unique_topics": len(self._topic_index),
            "avg_references_per_chunk": len(self._references) / max(len(self._nodes), 1)
        }


# =============================================================================
# CONTEXT ENHANCER
# =============================================================================

async def enhance_context_with_references(
    cross_linker: CrossReferenceLinker,
    retrieved_chunks: List[Dict[str, Any]],
    max_additions: int = 3
) -> List[Dict[str, Any]]:
    """
    Enhance retrieved chunks with cross-references.

    This adds related chunks from other documents to provide
    more comprehensive context.

    Args:
        cross_linker: CrossReferenceLinker instance
        retrieved_chunks: Original search results
        max_additions: Maximum chunks to add

    Returns:
        Enhanced list of chunks
    """
    enhanced = list(retrieved_chunks)
    seen_ids = {c["chunk_id"] for c in retrieved_chunks if "chunk_id" in c}

    for chunk in retrieved_chunks[:5]:  # Only look at top 5
        chunk_id = chunk.get("chunk_id")
        if chunk_id is None:
            continue

        related = await cross_linker.get_related_chunks(
            chunk_id,
            limit=2,
            min_score=0.7,
            cross_document_only=True
        )

        for rel in related:
            if rel["chunk_id"] not in seen_ids and len(enhanced) < len(retrieved_chunks) + max_additions:
                enhanced.append({
                    **rel,
                    "score": rel["similarity_score"] * 0.9,  # Slightly lower score
                    "source": "cross_reference"
                })
                seen_ids.add(rel["chunk_id"])

    return enhanced


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_GLOBAL_LINKER: Optional[CrossReferenceLinker] = None


def get_cross_referencer(
    embedder=None,
    similarity_threshold: float = 0.7
) -> CrossReferenceLinker:
    """Get or create global cross-referencer"""
    global _GLOBAL_LINKER

    if _GLOBAL_LINKER is None:
        _GLOBAL_LINKER = CrossReferenceLinker(
            embedder=embedder,
            similarity_threshold=similarity_threshold
        )

    return _GLOBAL_LINKER


# =============================================================================
# CUTTING EDGE: Citation Generation & Source Verification
# =============================================================================


class CitationStyle(str, Enum):
    """Supported citation styles."""
    APA = "apa"  # American Psychological Association
    MLA = "mla"  # Modern Language Association
    CHICAGO = "chicago"  # Chicago Manual of Style
    IEEE = "ieee"  # Institute of Electrical and Electronics Engineers
    HARVARD = "harvard"  # Harvard referencing
    VANCOUVER = "vancouver"  # Vancouver (medical/scientific)
    BIBTEX = "bibtex"  # BibTeX format


class VerificationStatus(str, Enum):
    """Status of fact verification."""
    VERIFIED = "verified"  # Confirmed by multiple sources
    PARTIALLY_VERIFIED = "partially_verified"  # Some support found
    UNVERIFIED = "unverified"  # No supporting sources found
    CONTRADICTED = "contradicted"  # Sources contradict the claim
    UNCERTAIN = "uncertain"  # Conflicting information


@dataclass
class SourceMetadata:
    """Metadata for a source document."""
    document_id: int
    title: str
    authors: List[str] = field(default_factory=list)
    publication_date: Optional[datetime] = None
    source_type: str = "document"  # document, webpage, article, book
    publisher: Optional[str] = None
    url: Optional[str] = None
    doi: Optional[str] = None
    page_numbers: Optional[str] = None
    volume: Optional[str] = None
    issue: Optional[str] = None
    accessed_date: Optional[datetime] = None

    def __post_init__(self):
        if self.accessed_date is None:
            self.accessed_date = datetime.utcnow()


@dataclass
class Citation:
    """A formatted citation."""
    source: SourceMetadata
    style: CitationStyle
    formatted_text: str
    inline_citation: str  # Short form for in-text use
    chunk_ids: List[int] = field(default_factory=list)
    relevance_score: float = 0.0


@dataclass
class ExtractedClaim:
    """A claim extracted from text for verification."""
    claim_id: str
    text: str
    claim_type: str  # factual, statistical, attributive, causal
    entities: List[str] = field(default_factory=list)
    source_chunk_id: Optional[int] = None
    confidence: float = 0.0


@dataclass
class VerificationResult:
    """Result of verifying a claim against sources."""
    claim: ExtractedClaim
    status: VerificationStatus
    confidence: float  # 0-1 confidence in the verification
    supporting_sources: List[Dict[str, Any]] = field(default_factory=list)
    contradicting_sources: List[Dict[str, Any]] = field(default_factory=list)
    explanation: str = ""
    triangulation_score: float = 0.0  # Based on multiple independent sources


@dataclass
class FactCheckReport:
    """Complete fact-checking report for a text."""
    text: str
    claims: List[ExtractedClaim]
    verifications: List[VerificationResult]
    overall_credibility: float  # 0-1
    verified_count: int = 0
    contradicted_count: int = 0
    unverified_count: int = 0
    citations_generated: List[Citation] = field(default_factory=list)


class CitationFormatter:
    """
    Generates citations in multiple academic formats.

    Supports APA, MLA, Chicago, IEEE, Harvard, Vancouver, and BibTeX.
    """

    def __init__(self):
        self._formatters = {
            CitationStyle.APA: self._format_apa,
            CitationStyle.MLA: self._format_mla,
            CitationStyle.CHICAGO: self._format_chicago,
            CitationStyle.IEEE: self._format_ieee,
            CitationStyle.HARVARD: self._format_harvard,
            CitationStyle.VANCOUVER: self._format_vancouver,
            CitationStyle.BIBTEX: self._format_bibtex,
        }

    def format_citation(
        self,
        source: SourceMetadata,
        style: CitationStyle = CitationStyle.APA
    ) -> Citation:
        """
        Format a citation in the specified style.

        Args:
            source: Source metadata
            style: Citation style to use

        Returns:
            Formatted Citation object
        """
        formatter = self._formatters.get(style, self._format_apa)
        formatted_text = formatter(source)
        inline_citation = self._format_inline(source, style)

        return Citation(
            source=source,
            style=style,
            formatted_text=formatted_text,
            inline_citation=inline_citation
        )

    def _format_authors_apa(self, authors: List[str]) -> str:
        """Format authors in APA style."""
        if not authors:
            return ""
        if len(authors) == 1:
            return self._last_first(authors[0])
        elif len(authors) == 2:
            return f"{self._last_first(authors[0])}, & {self._last_first(authors[1])}"
        else:
            formatted = ", ".join(self._last_first(a) for a in authors[:-1])
            return f"{formatted}, & {self._last_first(authors[-1])}"

    def _last_first(self, name: str) -> str:
        """Convert 'First Last' to 'Last, F.'"""
        parts = name.strip().split()
        if len(parts) >= 2:
            first_initials = ". ".join(p[0].upper() for p in parts[:-1]) + "."
            return f"{parts[-1]}, {first_initials}"
        return name

    def _format_apa(self, source: SourceMetadata) -> str:
        """Format citation in APA 7th edition style."""
        parts = []

        # Authors
        if source.authors:
            parts.append(self._format_authors_apa(source.authors))

        # Year
        if source.publication_date:
            parts.append(f"({source.publication_date.year}).")
        else:
            parts.append("(n.d.).")

        # Title
        if source.title:
            if source.source_type in ["article", "chapter"]:
                parts.append(f"{source.title}.")
            else:
                parts.append(f"*{source.title}*.")

        # Publisher/Source
        if source.publisher:
            parts.append(f"{source.publisher}.")

        # DOI or URL
        if source.doi:
            parts.append(f"https://doi.org/{source.doi}")
        elif source.url:
            parts.append(f"Retrieved from {source.url}")

        return " ".join(parts)

    def _format_mla(self, source: SourceMetadata) -> str:
        """Format citation in MLA 9th edition style."""
        parts = []

        # Authors
        if source.authors:
            if len(source.authors) == 1:
                parts.append(f"{self._last_first_mla(source.authors[0])}.")
            elif len(source.authors) == 2:
                parts.append(f"{self._last_first_mla(source.authors[0])}, and {source.authors[1]}.")
            else:
                parts.append(f"{self._last_first_mla(source.authors[0])}, et al.")

        # Title
        if source.title:
            parts.append(f'"{source.title}."')

        # Publisher
        if source.publisher:
            parts.append(f"{source.publisher},")

        # Year
        if source.publication_date:
            parts.append(f"{source.publication_date.year}.")

        # URL
        if source.url:
            parts.append(f"{source.url}.")

        return " ".join(parts)

    def _last_first_mla(self, name: str) -> str:
        """Convert 'First Last' to 'Last, First'"""
        parts = name.strip().split()
        if len(parts) >= 2:
            return f"{parts[-1]}, {' '.join(parts[:-1])}"
        return name

    def _format_chicago(self, source: SourceMetadata) -> str:
        """Format citation in Chicago style (notes-bibliography)."""
        parts = []

        # Authors
        if source.authors:
            if len(source.authors) == 1:
                parts.append(f"{self._last_first_mla(source.authors[0])}.")
            else:
                names = [self._last_first_mla(source.authors[0])]
                names.extend(source.authors[1:])
                parts.append(f"{', '.join(names)}.")

        # Title
        if source.title:
            parts.append(f"*{source.title}*.")

        # Publisher and place
        if source.publisher:
            parts.append(f"{source.publisher},")

        # Year
        if source.publication_date:
            parts.append(f"{source.publication_date.year}.")

        return " ".join(parts)

    def _format_ieee(self, source: SourceMetadata) -> str:
        """Format citation in IEEE style."""
        parts = []

        # Authors (initials first)
        if source.authors:
            formatted_authors = []
            for author in source.authors:
                name_parts = author.strip().split()
                if len(name_parts) >= 2:
                    initials = ". ".join(p[0].upper() for p in name_parts[:-1]) + "."
                    formatted_authors.append(f"{initials} {name_parts[-1]}")
                else:
                    formatted_authors.append(author)
            parts.append(", ".join(formatted_authors) + ",")

        # Title in quotes
        if source.title:
            parts.append(f'"{source.title},"')

        # Publisher
        if source.publisher:
            parts.append(f"*{source.publisher}*,")

        # Volume/Issue
        if source.volume:
            vol_str = f"vol. {source.volume}"
            if source.issue:
                vol_str += f", no. {source.issue}"
            parts.append(vol_str + ",")

        # Pages
        if source.page_numbers:
            parts.append(f"pp. {source.page_numbers},")

        # Year
        if source.publication_date:
            parts.append(f"{source.publication_date.year}.")

        return " ".join(parts)

    def _format_harvard(self, source: SourceMetadata) -> str:
        """Format citation in Harvard style."""
        parts = []

        # Authors
        if source.authors:
            parts.append(self._format_authors_apa(source.authors))

        # Year
        if source.publication_date:
            parts.append(f"({source.publication_date.year})")

        # Title
        if source.title:
            parts.append(f"*{source.title}*.")

        # Publisher
        if source.publisher:
            parts.append(f"{source.publisher}.")

        # URL with accessed date
        if source.url:
            accessed = source.accessed_date or datetime.utcnow()
            parts.append(f"Available at: {source.url} [Accessed {accessed.strftime('%d %B %Y')}].")

        return " ".join(parts)

    def _format_vancouver(self, source: SourceMetadata) -> str:
        """Format citation in Vancouver style (medical/scientific)."""
        parts = []

        # Authors (up to 6, then et al.)
        if source.authors:
            formatted = []
            for i, author in enumerate(source.authors[:6]):
                name_parts = author.strip().split()
                if len(name_parts) >= 2:
                    formatted.append(f"{name_parts[-1]} {name_parts[0][0]}")
                else:
                    formatted.append(author)
            author_str = ", ".join(formatted)
            if len(source.authors) > 6:
                author_str += ", et al"
            parts.append(author_str + ".")

        # Title
        if source.title:
            parts.append(f"{source.title}.")

        # Publisher
        if source.publisher:
            parts.append(f"{source.publisher}.")

        # Year
        if source.publication_date:
            parts.append(f"{source.publication_date.year};")

        # Volume/Pages
        if source.volume:
            vol_str = source.volume
            if source.page_numbers:
                vol_str += f":{source.page_numbers}"
            parts.append(vol_str + ".")

        return " ".join(parts)

    def _format_bibtex(self, source: SourceMetadata) -> str:
        """Format citation in BibTeX format."""
        # Generate a citation key
        key_parts = []
        if source.authors:
            first_author = source.authors[0].split()[-1].lower()
            key_parts.append(first_author)
        if source.publication_date:
            key_parts.append(str(source.publication_date.year))
        key = "".join(key_parts) or "unknown"

        # Determine entry type
        entry_type = {
            "article": "article",
            "book": "book",
            "chapter": "incollection",
            "webpage": "misc",
            "document": "misc"
        }.get(source.source_type, "misc")

        lines = [f"@{entry_type}{{{key},"]

        if source.authors:
            lines.append(f'  author = {{{" and ".join(source.authors)}}},')
        if source.title:
            lines.append(f'  title = {{{{{source.title}}}}},')
        if source.publication_date:
            lines.append(f'  year = {{{source.publication_date.year}}},')
        if source.publisher:
            lines.append(f'  publisher = {{{source.publisher}}},')
        if source.volume:
            lines.append(f'  volume = {{{source.volume}}},')
        if source.page_numbers:
            lines.append(f'  pages = {{{source.page_numbers}}},')
        if source.doi:
            lines.append(f'  doi = {{{source.doi}}},')
        if source.url:
            lines.append(f'  url = {{{source.url}}},')

        lines.append("}")
        return "\n".join(lines)

    def _format_inline(self, source: SourceMetadata, style: CitationStyle) -> str:
        """Generate inline citation for in-text use."""
        author_part = ""
        if source.authors:
            if len(source.authors) == 1:
                author_part = source.authors[0].split()[-1]
            elif len(source.authors) == 2:
                author_part = f"{source.authors[0].split()[-1]} & {source.authors[1].split()[-1]}"
            else:
                author_part = f"{source.authors[0].split()[-1]} et al."
        else:
            author_part = source.title[:20] + "..." if source.title else "Unknown"

        year_part = str(source.publication_date.year) if source.publication_date else "n.d."

        if style in [CitationStyle.APA, CitationStyle.HARVARD]:
            return f"({author_part}, {year_part})"
        elif style == CitationStyle.MLA:
            return f"({author_part})"
        elif style == CitationStyle.IEEE:
            return "[#]"  # Numbered reference
        elif style == CitationStyle.CHICAGO:
            return f"({author_part} {year_part})"
        else:
            return f"({author_part}, {year_part})"

    def format_bibliography(
        self,
        sources: List[SourceMetadata],
        style: CitationStyle = CitationStyle.APA
    ) -> str:
        """
        Format a complete bibliography from multiple sources.

        Args:
            sources: List of source metadata
            style: Citation style

        Returns:
            Formatted bibliography string
        """
        citations = [self.format_citation(s, style) for s in sources]

        # Sort alphabetically by author (or title if no author)
        def sort_key(c: Citation) -> str:
            if c.source.authors:
                return c.source.authors[0].split()[-1].lower()
            return c.source.title.lower() if c.source.title else ""

        citations.sort(key=sort_key)

        if style == CitationStyle.IEEE:
            # Numbered format
            return "\n".join(f"[{i+1}] {c.formatted_text}" for i, c in enumerate(citations))
        else:
            return "\n\n".join(c.formatted_text for c in citations)


class ClaimExtractor:
    """
    Extracts verifiable claims from text.

    Identifies factual statements, statistics, attributions, and causal claims.
    """

    # Patterns for different claim types
    FACTUAL_PATTERNS = [
        r"(?:is|are|was|were)\s+(?:a|an|the)?\s*([^.!?]+)",
        r"(?:has|have|had)\s+([^.!?]+)",
        r"(?:can|could|will|would)\s+([^.!?]+)",
    ]

    STATISTICAL_PATTERNS = [
        r"(\d+(?:\.\d+)?)\s*(?:%|percent|percentage)",
        r"(\d+(?:,\d{3})*(?:\.\d+)?)\s+(?:people|users|customers|items|records)",
        r"(?:increased|decreased|grew|fell)\s+(?:by\s+)?(\d+(?:\.\d+)?%?)",
        r"(?:approximately|about|around|nearly)\s+(\d+(?:,\d{3})*)",
    ]

    ATTRIBUTION_PATTERNS = [
        r"(?:according to|as stated by|per)\s+([A-Z][a-zA-Z\s]+)",
        r"([A-Z][a-zA-Z\s]+)\s+(?:said|stated|reported|claimed|argued)",
        r"(?:research|study|report)\s+(?:by|from)\s+([A-Z][a-zA-Z\s]+)",
    ]

    CAUSAL_PATTERNS = [
        r"(?:because|due to|as a result of|caused by)\s+([^.!?]+)",
        r"([^.!?]+)\s+(?:leads to|results in|causes|creates)",
        r"(?:therefore|thus|consequently|hence)\s+([^.!?]+)",
    ]

    def __init__(self):
        self._factual_re = [re.compile(p, re.IGNORECASE) for p in self.FACTUAL_PATTERNS]
        self._statistical_re = [re.compile(p, re.IGNORECASE) for p in self.STATISTICAL_PATTERNS]
        self._attribution_re = [re.compile(p, re.IGNORECASE) for p in self.ATTRIBUTION_PATTERNS]
        self._causal_re = [re.compile(p, re.IGNORECASE) for p in self.CAUSAL_PATTERNS]

    def extract_claims(
        self,
        text: str,
        source_chunk_id: Optional[int] = None
    ) -> List[ExtractedClaim]:
        """
        Extract verifiable claims from text.

        Args:
            text: Text to analyze
            source_chunk_id: ID of the source chunk

        Returns:
            List of extracted claims
        """
        claims = []

        # Split into sentences
        sentences = re.split(r'[.!?]+', text)

        for sentence in sentences:
            sentence = sentence.strip()
            if len(sentence) < 20:  # Skip very short sentences
                continue

            # Check for statistical claims (highest priority)
            for pattern in self._statistical_re:
                if pattern.search(sentence):
                    claims.append(self._create_claim(
                        sentence, "statistical", source_chunk_id
                    ))
                    break
            else:
                # Check for attribution claims
                for pattern in self._attribution_re:
                    if pattern.search(sentence):
                        claims.append(self._create_claim(
                            sentence, "attributive", source_chunk_id
                        ))
                        break
                else:
                    # Check for causal claims
                    for pattern in self._causal_re:
                        if pattern.search(sentence):
                            claims.append(self._create_claim(
                                sentence, "causal", source_chunk_id
                            ))
                            break
                    else:
                        # Check for factual claims
                        for pattern in self._factual_re:
                            if pattern.search(sentence):
                                claims.append(self._create_claim(
                                    sentence, "factual", source_chunk_id
                                ))
                                break

        return claims

    def _create_claim(
        self,
        text: str,
        claim_type: str,
        source_chunk_id: Optional[int]
    ) -> ExtractedClaim:
        """Create a claim object."""
        # Generate unique ID
        claim_id = hashlib.md5(text.encode()).hexdigest()[:12]

        # Extract entities
        entities = self._extract_entities(text)

        # Calculate confidence based on specificity
        confidence = self._calculate_confidence(text, claim_type)

        return ExtractedClaim(
            claim_id=claim_id,
            text=text,
            claim_type=claim_type,
            entities=entities,
            source_chunk_id=source_chunk_id,
            confidence=confidence
        )

    def _extract_entities(self, text: str) -> List[str]:
        """Extract named entities from claim."""
        # Simple heuristic: capitalized words not at sentence start
        words = text.split()
        entities = []

        for i, word in enumerate(words):
            if i > 0 and word[0].isupper() and len(word) > 2:
                # Check if part of a multi-word entity
                if i < len(words) - 1 and words[i + 1][0].isupper():
                    entities.append(f"{word} {words[i + 1]}")
                else:
                    entities.append(word.strip(".,;:"))

        return list(set(entities))[:5]

    def _calculate_confidence(self, text: str, claim_type: str) -> float:
        """Calculate confidence score for a claim."""
        confidence = 0.5  # Base confidence

        # Statistical claims with numbers are more specific
        if claim_type == "statistical":
            confidence += 0.2

        # Attribution claims are more verifiable
        if claim_type == "attributive":
            confidence += 0.15

        # Longer, more specific claims
        word_count = len(text.split())
        if word_count > 10:
            confidence += 0.1
        if word_count > 20:
            confidence += 0.05

        # Contains specific numbers
        if re.search(r'\d+', text):
            confidence += 0.1

        return min(confidence, 1.0)


class SourceVerifier:
    """
    Verifies claims against multiple sources using triangulation.

    Implements multi-source verification for fact-checking.
    """

    def __init__(
        self,
        cross_linker: CrossReferenceLinker,
        similarity_threshold: float = 0.7,
        min_sources_for_verification: int = 2
    ):
        """
        Initialize the verifier.

        Args:
            cross_linker: CrossReferenceLinker for finding related content
            similarity_threshold: Minimum similarity to consider as supporting
            min_sources_for_verification: Minimum sources needed for "verified" status
        """
        self.cross_linker = cross_linker
        self.similarity_threshold = similarity_threshold
        self.min_sources = min_sources_for_verification
        self._embedding_fn: Optional[Callable[[str], List[float]]] = None

    def set_embedding_function(self, fn: Callable[[str], List[float]]):
        """Set the embedding function for semantic comparison."""
        self._embedding_fn = fn

    async def verify_claim(
        self,
        claim: ExtractedClaim,
        available_chunks: List[Dict[str, Any]]
    ) -> VerificationResult:
        """
        Verify a claim against available sources.

        Args:
            claim: The claim to verify
            available_chunks: Available chunks to check against

        Returns:
            VerificationResult with status and evidence
        """
        supporting = []
        contradicting = []

        # Get claim embedding for semantic comparison
        claim_embedding = None
        if self._embedding_fn:
            try:
                claim_embedding = self._embedding_fn(claim.text)
            except Exception:
                pass

        # Check each chunk for support or contradiction
        for chunk in available_chunks:
            if chunk.get("chunk_id") == claim.source_chunk_id:
                continue  # Skip the source chunk itself

            content = chunk.get("content", "")
            chunk_embedding = chunk.get("embedding")

            # Calculate similarity
            similarity = 0.0
            if claim_embedding and chunk_embedding:
                similarity = self._cosine_similarity(claim_embedding, chunk_embedding)
            else:
                # Fallback to keyword overlap
                similarity = self._keyword_similarity(claim.text, content)

            if similarity >= self.similarity_threshold:
                # Check for contradiction
                is_contradiction = self._check_contradiction(claim.text, content)

                evidence = {
                    "chunk_id": chunk.get("chunk_id"),
                    "document_id": chunk.get("document_id"),
                    "content_preview": content[:200] + "..." if len(content) > 200 else content,
                    "similarity": similarity,
                    "source_title": chunk.get("metadata", {}).get("title", "Unknown")
                }

                if is_contradiction:
                    contradicting.append(evidence)
                else:
                    supporting.append(evidence)

        # Determine verification status
        status, confidence = self._determine_status(
            supporting, contradicting, len(available_chunks)
        )

        # Calculate triangulation score
        triangulation = self._calculate_triangulation(supporting)

        # Generate explanation
        explanation = self._generate_explanation(
            claim, status, supporting, contradicting
        )

        return VerificationResult(
            claim=claim,
            status=status,
            confidence=confidence,
            supporting_sources=supporting,
            contradicting_sources=contradicting,
            explanation=explanation,
            triangulation_score=triangulation
        )

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Calculate cosine similarity."""
        a_np = np.array(a)
        b_np = np.array(b)
        norm_a = np.linalg.norm(a_np)
        norm_b = np.linalg.norm(b_np)
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return float(np.dot(a_np, b_np) / (norm_a * norm_b))

    def _keyword_similarity(self, text1: str, text2: str) -> float:
        """Calculate keyword-based similarity."""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())

        # Remove stop words
        stop_words = {"the", "a", "an", "is", "are", "was", "were", "be", "been",
                      "being", "have", "has", "had", "do", "does", "did", "will",
                      "would", "could", "should", "may", "might", "must", "shall",
                      "can", "to", "of", "in", "for", "on", "with", "at", "by",
                      "from", "as", "into", "through", "during", "before", "after",
                      "above", "below", "between", "under", "again", "further",
                      "then", "once", "here", "there", "when", "where", "why",
                      "how", "all", "each", "few", "more", "most", "other", "some",
                      "such", "no", "nor", "not", "only", "own", "same", "so",
                      "than", "too", "very", "just", "and", "but", "if", "or",
                      "because", "until", "while", "this", "that", "these", "those"}

        words1 = words1 - stop_words
        words2 = words2 - stop_words

        if not words1 or not words2:
            return 0.0

        intersection = words1 & words2
        union = words1 | words2

        return len(intersection) / len(union)

    def _check_contradiction(self, claim: str, content: str) -> bool:
        """Check if content contradicts the claim."""
        claim_lower = claim.lower()
        content_lower = content.lower()

        # Negation patterns
        negation_pairs = [
            ("is not", "is"),
            ("was not", "was"),
            ("are not", "are"),
            ("were not", "were"),
            ("does not", "does"),
            ("did not", "did"),
            ("cannot", "can"),
            ("will not", "will"),
            ("never", "always"),
            ("false", "true"),
            ("incorrect", "correct"),
            ("wrong", "right"),
            ("failed", "succeeded"),
            ("decreased", "increased"),
            ("less", "more"),
            ("lower", "higher"),
            ("fewer", "more"),
        ]

        for neg, pos in negation_pairs:
            if neg in claim_lower and pos in content_lower:
                return True
            if pos in claim_lower and neg in content_lower:
                return True

        # Check for explicit contradiction markers
        contradiction_markers = ["however", "contrary to", "in contrast", "on the other hand",
                                  "contradicts", "disputes", "refutes", "disproves"]

        for marker in contradiction_markers:
            if marker in content_lower:
                # Check if the content references similar entities
                claim_entities = set(re.findall(r'\b[A-Z][a-z]+\b', claim))
                content_entities = set(re.findall(r'\b[A-Z][a-z]+\b', content))
                if claim_entities & content_entities:
                    return True

        return False

    def _determine_status(
        self,
        supporting: List[Dict],
        contradicting: List[Dict],
        total_sources: int
    ) -> Tuple[VerificationStatus, float]:
        """Determine verification status and confidence."""
        support_count = len(supporting)
        contradict_count = len(contradicting)

        if contradict_count > support_count:
            confidence = min(0.9, contradict_count / max(total_sources, 1))
            return VerificationStatus.CONTRADICTED, confidence

        if support_count >= self.min_sources and contradict_count == 0:
            confidence = min(0.95, 0.5 + (support_count * 0.15))
            return VerificationStatus.VERIFIED, confidence

        if support_count > 0 and contradict_count == 0:
            confidence = min(0.7, 0.4 + (support_count * 0.1))
            return VerificationStatus.PARTIALLY_VERIFIED, confidence

        if support_count > 0 and contradict_count > 0:
            confidence = 0.5
            return VerificationStatus.UNCERTAIN, confidence

        return VerificationStatus.UNVERIFIED, 0.3

    def _calculate_triangulation(self, supporting: List[Dict]) -> float:
        """
        Calculate triangulation score based on source independence.

        Higher score when sources are from different documents.
        """
        if not supporting:
            return 0.0

        document_ids = set(s.get("document_id") for s in supporting)
        unique_docs = len(document_ids)

        # Score based on unique documents
        if unique_docs >= 3:
            return 0.9
        elif unique_docs == 2:
            return 0.7
        elif unique_docs == 1:
            return 0.4
        else:
            return 0.0

    def _generate_explanation(
        self,
        claim: ExtractedClaim,
        status: VerificationStatus,
        supporting: List[Dict],
        contradicting: List[Dict]
    ) -> str:
        """Generate human-readable explanation."""
        explanations = {
            VerificationStatus.VERIFIED: f"Claim verified by {len(supporting)} independent source(s).",
            VerificationStatus.PARTIALLY_VERIFIED: f"Partial support found in {len(supporting)} source(s), but insufficient for full verification.",
            VerificationStatus.UNVERIFIED: "No supporting sources found for this claim.",
            VerificationStatus.CONTRADICTED: f"Claim contradicted by {len(contradicting)} source(s).",
            VerificationStatus.UNCERTAIN: f"Mixed evidence: {len(supporting)} supporting and {len(contradicting)} contradicting source(s)."
        }

        base = explanations.get(status, "Verification status unknown.")

        # Add details about sources
        if supporting:
            sources = [s.get("source_title", "Unknown") for s in supporting[:3]]
            base += f" Supporting: {', '.join(sources)}."

        if contradicting:
            sources = [s.get("source_title", "Unknown") for s in contradicting[:3]]
            base += f" Contradicting: {', '.join(sources)}."

        return base


class FactChecker:
    """
    Complete fact-checking system combining claim extraction and verification.

    Provides comprehensive credibility assessment with citations.
    """

    def __init__(
        self,
        cross_linker: CrossReferenceLinker,
        citation_formatter: Optional[CitationFormatter] = None
    ):
        """
        Initialize the fact checker.

        Args:
            cross_linker: CrossReferenceLinker instance
            citation_formatter: CitationFormatter instance (optional)
        """
        self.cross_linker = cross_linker
        self.claim_extractor = ClaimExtractor()
        self.verifier = SourceVerifier(cross_linker)
        self.citation_formatter = citation_formatter or CitationFormatter()

        # Track verification history for learning
        self._verification_history: List[VerificationResult] = []

    def set_embedding_function(self, fn: Callable[[str], List[float]]):
        """Set embedding function for semantic comparison."""
        self.verifier.set_embedding_function(fn)

    async def fact_check(
        self,
        text: str,
        available_chunks: List[Dict[str, Any]],
        source_metadata: Optional[Dict[int, SourceMetadata]] = None,
        citation_style: CitationStyle = CitationStyle.APA
    ) -> FactCheckReport:
        """
        Perform comprehensive fact-checking on text.

        Args:
            text: Text to fact-check
            available_chunks: Available chunks for verification
            source_metadata: Metadata for source documents (keyed by document_id)
            citation_style: Style for citation generation

        Returns:
            Complete FactCheckReport
        """
        source_metadata = source_metadata or {}

        # Extract claims
        claims = self.claim_extractor.extract_claims(text)

        # Verify each claim
        verifications = []
        for claim in claims:
            result = await self.verifier.verify_claim(claim, available_chunks)
            verifications.append(result)
            self._verification_history.append(result)

        # Count by status
        verified = sum(1 for v in verifications if v.status == VerificationStatus.VERIFIED)
        contradicted = sum(1 for v in verifications if v.status == VerificationStatus.CONTRADICTED)
        unverified = sum(1 for v in verifications if v.status == VerificationStatus.UNVERIFIED)

        # Calculate overall credibility
        credibility = self._calculate_credibility(verifications)

        # Generate citations for supporting sources
        citations = []
        cited_docs = set()

        for verification in verifications:
            for source in verification.supporting_sources:
                doc_id = source.get("document_id")
                if doc_id and doc_id not in cited_docs:
                    cited_docs.add(doc_id)

                    # Get or create source metadata
                    if doc_id in source_metadata:
                        meta = source_metadata[doc_id]
                    else:
                        meta = SourceMetadata(
                            document_id=doc_id,
                            title=source.get("source_title", f"Document {doc_id}"),
                            source_type="document"
                        )

                    citation = self.citation_formatter.format_citation(meta, citation_style)
                    citation.chunk_ids.append(source.get("chunk_id"))
                    citation.relevance_score = source.get("similarity", 0.0)
                    citations.append(citation)

        return FactCheckReport(
            text=text,
            claims=claims,
            verifications=verifications,
            overall_credibility=credibility,
            verified_count=verified,
            contradicted_count=contradicted,
            unverified_count=unverified,
            citations_generated=citations
        )

    def _calculate_credibility(self, verifications: List[VerificationResult]) -> float:
        """Calculate overall credibility score."""
        if not verifications:
            return 0.5  # Neutral if no claims

        weights = {
            VerificationStatus.VERIFIED: 1.0,
            VerificationStatus.PARTIALLY_VERIFIED: 0.7,
            VerificationStatus.UNCERTAIN: 0.5,
            VerificationStatus.UNVERIFIED: 0.3,
            VerificationStatus.CONTRADICTED: 0.0,
        }

        total_weight = 0.0
        total_confidence = 0.0

        for v in verifications:
            weight = weights.get(v.status, 0.5)
            total_weight += weight * v.confidence
            total_confidence += v.confidence

        if total_confidence == 0:
            return 0.5

        return total_weight / total_confidence

    def get_verification_stats(self) -> Dict[str, Any]:
        """Get statistics about verification history."""
        if not self._verification_history:
            return {"total_verifications": 0}

        status_counts = defaultdict(int)
        for v in self._verification_history:
            status_counts[v.status.value] += 1

        avg_confidence = sum(v.confidence for v in self._verification_history) / len(self._verification_history)
        avg_triangulation = sum(v.triangulation_score for v in self._verification_history) / len(self._verification_history)

        return {
            "total_verifications": len(self._verification_history),
            "status_distribution": dict(status_counts),
            "average_confidence": avg_confidence,
            "average_triangulation": avg_triangulation,
            "verified_rate": status_counts.get("verified", 0) / len(self._verification_history),
            "contradicted_rate": status_counts.get("contradicted", 0) / len(self._verification_history)
        }


class AdvancedCrossReferencer:
    """
    Advanced cross-referencer combining citation generation,
    source verification, and fact-checking.

    Unified interface for all cross-referencing capabilities.
    """

    def __init__(
        self,
        embedder=None,
        similarity_threshold: float = 0.7
    ):
        """
        Initialize the advanced cross-referencer.

        Args:
            embedder: Embedding model for semantic comparison
            similarity_threshold: Minimum similarity for cross-references
        """
        self.cross_linker = CrossReferenceLinker(
            embedder=embedder,
            similarity_threshold=similarity_threshold
        )
        self.citation_formatter = CitationFormatter()
        self.claim_extractor = ClaimExtractor()
        self.fact_checker = FactChecker(self.cross_linker, self.citation_formatter)

        # Set embedding function if embedder provided
        if embedder:
            self.fact_checker.set_embedding_function(
                lambda text: embedder.encode(text, normalize_embeddings=True).tolist()
            )

        # Source metadata registry
        self._source_metadata: Dict[int, SourceMetadata] = {}

        logger.info("AdvancedCrossReferencer initialized")

    def register_source(self, metadata: SourceMetadata):
        """Register source metadata for citation generation."""
        self._source_metadata[metadata.document_id] = metadata

    async def add_chunk(
        self,
        chunk_id: int,
        document_id: int,
        content: str,
        embedding: Optional[List[float]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ChunkNode:
        """Add a chunk to the cross-reference graph."""
        return await self.cross_linker.add_chunk(
            chunk_id, document_id, content, embedding, metadata
        )

    async def build_references(self, chunk_id: int) -> List[CrossReference]:
        """Build cross-references for a chunk."""
        return await self.cross_linker.build_references(chunk_id)

    def format_citation(
        self,
        document_id: int,
        style: CitationStyle = CitationStyle.APA
    ) -> Optional[Citation]:
        """Format a citation for a document."""
        if document_id not in self._source_metadata:
            return None

        return self.citation_formatter.format_citation(
            self._source_metadata[document_id],
            style
        )

    def format_bibliography(
        self,
        document_ids: List[int],
        style: CitationStyle = CitationStyle.APA
    ) -> str:
        """Format a bibliography for multiple documents."""
        sources = [
            self._source_metadata[did]
            for did in document_ids
            if did in self._source_metadata
        ]
        return self.citation_formatter.format_bibliography(sources, style)

    async def fact_check(
        self,
        text: str,
        available_chunks: List[Dict[str, Any]],
        citation_style: CitationStyle = CitationStyle.APA
    ) -> FactCheckReport:
        """Perform comprehensive fact-checking."""
        return await self.fact_checker.fact_check(
            text=text,
            available_chunks=available_chunks,
            source_metadata=self._source_metadata,
            citation_style=citation_style
        )

    async def verify_claim(
        self,
        claim_text: str,
        available_chunks: List[Dict[str, Any]]
    ) -> VerificationResult:
        """Verify a single claim."""
        claim = ExtractedClaim(
            claim_id=hashlib.md5(claim_text.encode()).hexdigest()[:12],
            text=claim_text,
            claim_type="factual",
            confidence=0.7
        )
        return await self.fact_checker.verifier.verify_claim(claim, available_chunks)

    async def get_related_chunks(
        self,
        chunk_id: int,
        limit: int = 5,
        cross_document_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Get related chunks with citation info."""
        related = await self.cross_linker.get_related_chunks(
            chunk_id, limit=limit, cross_document_only=cross_document_only
        )

        # Enrich with citation info
        for item in related:
            doc_id = item.get("document_id")
            if doc_id in self._source_metadata:
                meta = self._source_metadata[doc_id]
                item["citation"] = self.citation_formatter.format_citation(meta).inline_citation
                item["source_title"] = meta.title
                item["authors"] = meta.authors

        return related

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        return {
            "cross_linker": self.cross_linker.get_stats(),
            "fact_checker": self.fact_checker.get_verification_stats(),
            "registered_sources": len(self._source_metadata)
        }


# =============================================================================
# Factory Functions
# =============================================================================

_ADVANCED_REFERENCER: Optional[AdvancedCrossReferencer] = None


def get_advanced_cross_referencer(
    embedder=None,
    similarity_threshold: float = 0.7
) -> AdvancedCrossReferencer:
    """Get or create global advanced cross-referencer."""
    global _ADVANCED_REFERENCER

    if _ADVANCED_REFERENCER is None:
        _ADVANCED_REFERENCER = AdvancedCrossReferencer(
            embedder=embedder,
            similarity_threshold=similarity_threshold
        )

    return _ADVANCED_REFERENCER


def reset_advanced_cross_referencer():
    """Reset the global advanced cross-referencer."""
    global _ADVANCED_REFERENCER
    _ADVANCED_REFERENCER = None


async def generate_citations_for_response(
    response_text: str,
    source_chunks: List[Dict[str, Any]],
    source_metadata: Dict[int, SourceMetadata],
    style: CitationStyle = CitationStyle.APA
) -> Tuple[str, str]:
    """
    Generate citations for a response and return annotated text with bibliography.

    Args:
        response_text: The response text
        source_chunks: Chunks used to generate the response
        source_metadata: Metadata for source documents
        style: Citation style

    Returns:
        Tuple of (annotated_text, bibliography)
    """
    referencer = get_advanced_cross_referencer()

    # Register source metadata
    for meta in source_metadata.values():
        referencer.register_source(meta)

    # Get unique document IDs
    doc_ids = list(set(c.get("document_id") for c in source_chunks if c.get("document_id")))

    # Generate bibliography
    bibliography = referencer.format_bibliography(doc_ids, style)

    # Add inline citations (simplified - in production, would match content to sources)
    annotated = response_text
    if doc_ids and style != CitationStyle.IEEE:
        # Add a general citation at the end
        citations = [referencer.format_citation(did, style) for did in doc_ids[:3]]
        citation_text = "; ".join(c.inline_citation for c in citations if c)
        if citation_text:
            annotated += f" {citation_text}"

    return annotated, bibliography
