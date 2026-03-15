"""
Parent-Child Chunking for RAG

Search on small, precise chunks but return parent context for better understanding.
This improves retrieval precision while maintaining context completeness.

Architecture:
- Parent chunks: 512-1024 tokens (for context)
- Child chunks: 128-256 tokens (for precise matching)
- Child chunks store reference to parent
- Search returns parent when child matches

Cutting Edge Features:
- Dynamic chunk sizing based on content density and type
- Content-aware boundaries using semantic signals
- ML-based chunk boundary detection with learned models
"""

import hashlib
import logging
import re
import math
import statistics
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum
from collections import defaultdict
import numpy as np

logger = logging.getLogger(__name__)


class ChunkLevel(Enum):
    """Chunk hierarchy levels"""
    DOCUMENT = "document"
    PARENT = "parent"
    CHILD = "child"


@dataclass
class Chunk:
    """Represents a text chunk with hierarchy information"""
    id: str
    content: str
    level: ChunkLevel
    parent_id: Optional[str] = None
    document_id: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    start_char: int = 0
    end_char: int = 0

    def __post_init__(self):
        if not self.id:
            self.id = self._generate_id()

    def _generate_id(self) -> str:
        """Generate unique ID based on content hash"""
        content_hash = hashlib.md5(self.content.encode()).hexdigest()[:12]
        return f"{self.level.value}_{content_hash}"


class ParentChildChunker:
    """
    Implements parent-child chunking strategy.

    - Creates large parent chunks for context
    - Splits parents into smaller child chunks for search
    - Maintains parent-child relationships
    """

    def __init__(
        self,
        parent_chunk_size: int = 1024,
        parent_chunk_overlap: int = 128,
        child_chunk_size: int = 256,
        child_chunk_overlap: int = 32,
        min_chunk_size: int = 50
    ):
        """
        Initialize the chunker.

        Args:
            parent_chunk_size: Size of parent chunks in characters
            parent_chunk_overlap: Overlap between parent chunks
            child_chunk_size: Size of child chunks in characters
            child_chunk_overlap: Overlap between child chunks
            min_chunk_size: Minimum chunk size to keep
        """
        self.parent_chunk_size = parent_chunk_size
        self.parent_chunk_overlap = parent_chunk_overlap
        self.child_chunk_size = child_chunk_size
        self.child_chunk_overlap = child_chunk_overlap
        self.min_chunk_size = min_chunk_size

        # Storage for parent-child relationships
        self.parent_store: dict[str, Chunk] = {}
        self.child_to_parent: dict[str, str] = {}

        logger.info(
            f"ParentChildChunker initialized: "
            f"parent_size={parent_chunk_size}, child_size={child_chunk_size}"
        )

    def chunk_document(
        self,
        text: str,
        document_id: str,
        metadata: Optional[dict] = None
    ) -> tuple[list[Chunk], list[Chunk]]:
        """
        Chunk a document into parent and child chunks.

        Args:
            text: The document text to chunk
            document_id: Unique identifier for the document
            metadata: Optional metadata to attach to chunks

        Returns:
            Tuple of (parent_chunks, child_chunks)
        """
        metadata = metadata or {}

        # First, create parent chunks
        parent_chunks = self._create_chunks(
            text=text,
            chunk_size=self.parent_chunk_size,
            overlap=self.parent_chunk_overlap,
            level=ChunkLevel.PARENT,
            document_id=document_id,
            metadata=metadata
        )

        # Store parents
        for parent in parent_chunks:
            self.parent_store[parent.id] = parent

        # Then, create child chunks from each parent
        all_child_chunks = []
        for parent in parent_chunks:
            child_chunks = self._create_chunks(
                text=parent.content,
                chunk_size=self.child_chunk_size,
                overlap=self.child_chunk_overlap,
                level=ChunkLevel.CHILD,
                document_id=document_id,
                parent_id=parent.id,
                metadata={**metadata, "parent_id": parent.id},
                base_offset=parent.start_char
            )

            # Map children to parent
            for child in child_chunks:
                self.child_to_parent[child.id] = parent.id

            all_child_chunks.extend(child_chunks)

        logger.info(
            f"Chunked document {document_id}: "
            f"{len(parent_chunks)} parents, {len(all_child_chunks)} children"
        )

        return parent_chunks, all_child_chunks

    def _create_chunks(
        self,
        text: str,
        chunk_size: int,
        overlap: int,
        level: ChunkLevel,
        document_id: str,
        metadata: dict,
        parent_id: Optional[str] = None,
        base_offset: int = 0
    ) -> list[Chunk]:
        """
        Create chunks from text with specified parameters.

        Uses sentence-aware splitting when possible.
        """
        chunks = []

        if len(text) <= chunk_size:
            # Text fits in single chunk
            chunk = Chunk(
                id="",
                content=text.strip(),
                level=level,
                parent_id=parent_id,
                document_id=document_id,
                metadata=metadata,
                start_char=base_offset,
                end_char=base_offset + len(text)
            )
            if len(chunk.content) >= self.min_chunk_size:
                chunks.append(chunk)
            return chunks

        # Split into chunks with overlap
        start = 0
        while start < len(text):
            end = start + chunk_size

            # Try to break at sentence boundary
            if end < len(text):
                # Look for sentence endings near the chunk boundary
                search_start = max(start + chunk_size - 100, start)
                search_end = min(start + chunk_size + 50, len(text))
                search_text = text[search_start:search_end]

                # Find best break point
                best_break = self._find_sentence_break(search_text)
                if best_break > 0:
                    end = search_start + best_break

            chunk_text = text[start:end].strip()

            if len(chunk_text) >= self.min_chunk_size:
                chunk = Chunk(
                    id="",
                    content=chunk_text,
                    level=level,
                    parent_id=parent_id,
                    document_id=document_id,
                    metadata=metadata,
                    start_char=base_offset + start,
                    end_char=base_offset + end
                )
                chunks.append(chunk)

            # Move to next chunk with overlap
            start = end - overlap
            if start >= len(text) - self.min_chunk_size:
                break

        return chunks

    def _find_sentence_break(self, text: str) -> int:
        """Find the best sentence break point in text."""
        # Sentence ending patterns (prioritized)
        endings = ['. ', '.\n', '? ', '?\n', '! ', '!\n', '\n\n']

        best_pos = -1
        for ending in endings:
            pos = text.rfind(ending)
            if pos > 0:
                best_pos = pos + len(ending)
                break

        return best_pos

    def get_parent_for_child(self, child_id: str) -> Optional[Chunk]:
        """
        Get the parent chunk for a given child chunk ID.

        Args:
            child_id: The ID of the child chunk

        Returns:
            The parent Chunk if found, None otherwise
        """
        parent_id = self.child_to_parent.get(child_id)
        if parent_id:
            return self.parent_store.get(parent_id)
        return None

    def get_parent_by_id(self, parent_id: str) -> Optional[Chunk]:
        """Get a parent chunk by its ID."""
        return self.parent_store.get(parent_id)

    def expand_to_parent(
        self,
        child_chunks: list[dict],
        deduplicate: bool = True
    ) -> list[dict]:
        """
        Expand child chunk search results to include parent context.

        Args:
            child_chunks: List of child chunk search results with 'id' field
            deduplicate: Whether to deduplicate parents

        Returns:
            List of parent chunks with combined scores
        """
        parent_scores: dict[str, float] = {}
        parent_metadata: dict[str, dict] = {}

        for child in child_chunks:
            child_id = child.get("id") or child.get("chunk_id")
            score = child.get("score", 0.0)

            parent_id = self.child_to_parent.get(child_id)
            if parent_id:
                # Aggregate scores for same parent
                if parent_id in parent_scores:
                    # Use max score or sum (configurable)
                    parent_scores[parent_id] = max(parent_scores[parent_id], score)
                else:
                    parent_scores[parent_id] = score
                    parent_metadata[parent_id] = child.get("metadata", {})

        # Build result list
        results = []
        for parent_id, score in sorted(
            parent_scores.items(),
            key=lambda x: x[1],
            reverse=True
        ):
            parent = self.parent_store.get(parent_id)
            if parent:
                results.append({
                    "id": parent_id,
                    "content": parent.content,
                    "score": score,
                    "level": "parent",
                    "document_id": parent.document_id,
                    "metadata": {**parent.metadata, **parent_metadata.get(parent_id, {})},
                    "start_char": parent.start_char,
                    "end_char": parent.end_char
                })

        return results

    def clear(self):
        """Clear all stored chunks and mappings."""
        self.parent_store.clear()
        self.child_to_parent.clear()
        logger.info("ParentChildChunker cleared")


class ParentChildRetriever:
    """
    High-level retriever that combines parent-child chunking with vector search.

    Workflow:
    1. Index child chunks in vector store
    2. Search returns matching children
    3. Expand to parent chunks for context
    """

    def __init__(
        self,
        chunker: ParentChildChunker,
        vector_store=None,
        embedding_model=None
    ):
        """
        Initialize the retriever.

        Args:
            chunker: ParentChildChunker instance
            vector_store: Vector store for embeddings (optional, can be set later)
            embedding_model: Model for generating embeddings (optional)
        """
        self.chunker = chunker
        self.vector_store = vector_store
        self.embedding_model = embedding_model
        self._child_embeddings: dict[str, list[float]] = {}

    def index_document(
        self,
        text: str,
        document_id: str,
        metadata: Optional[dict] = None
    ) -> dict:
        """
        Index a document using parent-child chunking.

        Args:
            text: Document text
            document_id: Unique document ID
            metadata: Optional metadata

        Returns:
            Indexing statistics
        """
        # Create chunks
        parent_chunks, child_chunks = self.chunker.chunk_document(
            text=text,
            document_id=document_id,
            metadata=metadata
        )

        # Index child chunks in vector store
        indexed_count = 0
        if self.vector_store and self.embedding_model:
            for child in child_chunks:
                try:
                    # Generate embedding
                    embedding = self.embedding_model.embed(child.content)
                    self._child_embeddings[child.id] = embedding

                    # Add to vector store
                    self.vector_store.add(
                        id=child.id,
                        embedding=embedding,
                        metadata={
                            "content": child.content,
                            "parent_id": child.parent_id,
                            "document_id": document_id,
                            **child.metadata
                        }
                    )
                    indexed_count += 1
                except Exception as e:
                    logger.error(f"Failed to index child chunk {child.id}: {e}")

        return {
            "document_id": document_id,
            "parent_chunks": len(parent_chunks),
            "child_chunks": len(child_chunks),
            "indexed": indexed_count
        }

    def search(
        self,
        query: str,
        top_k: int = 5,
        return_parents: bool = True,
        include_children: bool = False
    ) -> list[dict]:
        """
        Search for relevant chunks.

        Args:
            query: Search query
            top_k: Number of results to return
            return_parents: Whether to expand results to parent chunks
            include_children: Whether to include child chunks in results

        Returns:
            List of matching chunks (parents if return_parents=True)
        """
        if not self.vector_store or not self.embedding_model:
            logger.warning("Vector store or embedding model not configured")
            return []

        try:
            # Generate query embedding
            query_embedding = self.embedding_model.embed(query)

            # Search child chunks (get more to allow for deduplication)
            child_k = top_k * 3 if return_parents else top_k
            child_results = self.vector_store.search(
                embedding=query_embedding,
                top_k=child_k
            )

            if return_parents:
                # Expand to parent chunks
                parent_results = self.chunker.expand_to_parent(
                    child_chunks=child_results,
                    deduplicate=True
                )

                results = parent_results[:top_k]

                if include_children:
                    for result in results:
                        result["matched_children"] = [
                            c for c in child_results
                            if c.get("metadata", {}).get("parent_id") == result["id"]
                        ]

                return results
            else:
                return child_results[:top_k]

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def get_stats(self) -> dict:
        """Get retriever statistics."""
        return {
            "parent_chunks": len(self.chunker.parent_store),
            "child_chunks": len(self.chunker.child_to_parent),
            "indexed_embeddings": len(self._child_embeddings)
        }


# =============================================================================
# CUTTING EDGE: Dynamic Chunk Sizing & ML-Based Boundary Detection
# =============================================================================


class ContentType(str, Enum):
    """Types of content for adaptive chunking."""
    PROSE = "prose"  # Regular text paragraphs
    CODE = "code"  # Source code
    TABLE = "table"  # Tabular data
    LIST = "list"  # Bullet points, numbered lists
    HEADER = "header"  # Section headers
    QUOTE = "quote"  # Block quotes
    FORMULA = "formula"  # Mathematical formulas
    MIXED = "mixed"  # Multiple types combined


@dataclass
class ContentAnalysis:
    """Results of content analysis for a text segment."""
    content_type: ContentType
    density_score: float  # 0-1, higher = more information dense
    complexity_score: float  # 0-1, higher = more complex
    coherence_score: float  # 0-1, higher = more coherent
    recommended_chunk_size: int
    features: dict = field(default_factory=dict)


@dataclass
class BoundarySignal:
    """A detected boundary signal in text."""
    position: int
    signal_type: str  # 'semantic', 'structural', 'topic_shift', 'code_block'
    confidence: float  # 0-1
    context: str  # Text around the boundary


@dataclass
class ChunkBoundary:
    """A recommended chunk boundary with ML confidence."""
    position: int
    confidence: float
    signals: list[BoundarySignal]
    chunk_type: ContentType
    reasoning: str


class ContentAnalyzer:
    """
    Analyzes content to determine type, density, and optimal chunking parameters.

    Uses heuristics and pattern matching to understand content structure.
    """

    # Patterns for content type detection
    CODE_PATTERNS = [
        r'^\s*(def |class |function |const |let |var |import |from |#include)',
        r'[{}\[\]();]',
        r'=>|->|\|\||&&',
        r'^\s*(@\w+|#\w+)',  # Decorators, preprocessor
        r'^\s*(if|else|for|while|return|try|catch)\s*[\({:]',
    ]

    TABLE_PATTERNS = [
        r'\|.*\|.*\|',  # Markdown tables
        r'^\s*[\+\-]{3,}',  # ASCII table borders
        r'\t.*\t.*\t',  # Tab-separated
    ]

    LIST_PATTERNS = [
        r'^\s*[-*•]\s+',  # Bullet points
        r'^\s*\d+[.)]\s+',  # Numbered lists
        r'^\s*[a-z][.)]\s+',  # Letter lists
    ]

    HEADER_PATTERNS = [
        r'^#{1,6}\s+',  # Markdown headers
        r'^[A-Z][^.!?]*:$',  # Title-like headers
        r'^={3,}$|^-{3,}$',  # Underline headers
    ]

    QUOTE_PATTERNS = [
        r'^\s*>',  # Markdown quotes
        r'^"[^"]{50,}"$',  # Long quotes
    ]

    FORMULA_PATTERNS = [
        r'\$\$.*\$\$',  # LaTeX display math
        r'\$[^$]+\$',  # LaTeX inline math
        r'\\frac|\\sum|\\int|\\sqrt',  # LaTeX commands
    ]

    def __init__(
        self,
        base_chunk_size: int = 512,
        min_chunk_size: int = 128,
        max_chunk_size: int = 2048
    ):
        self.base_chunk_size = base_chunk_size
        self.min_chunk_size = min_chunk_size
        self.max_chunk_size = max_chunk_size

        # Compile patterns for efficiency
        self._code_re = [re.compile(p, re.MULTILINE) for p in self.CODE_PATTERNS]
        self._table_re = [re.compile(p, re.MULTILINE) for p in self.TABLE_PATTERNS]
        self._list_re = [re.compile(p, re.MULTILINE) for p in self.LIST_PATTERNS]
        self._header_re = [re.compile(p, re.MULTILINE) for p in self.HEADER_PATTERNS]
        self._quote_re = [re.compile(p, re.MULTILINE) for p in self.QUOTE_PATTERNS]
        self._formula_re = [re.compile(p, re.MULTILINE) for p in self.FORMULA_PATTERNS]

    def analyze(self, text: str) -> ContentAnalysis:
        """
        Analyze text content to determine type and optimal chunking parameters.

        Args:
            text: Text to analyze

        Returns:
            ContentAnalysis with recommendations
        """
        # Detect content type
        content_type = self._detect_content_type(text)

        # Calculate density score (information per character)
        density_score = self._calculate_density(text)

        # Calculate complexity score
        complexity_score = self._calculate_complexity(text)

        # Calculate coherence score
        coherence_score = self._calculate_coherence(text)

        # Determine recommended chunk size
        recommended_size = self._calculate_recommended_size(
            content_type, density_score, complexity_score, coherence_score, len(text)
        )

        # Extract features for ML model
        features = self._extract_features(text)

        return ContentAnalysis(
            content_type=content_type,
            density_score=density_score,
            complexity_score=complexity_score,
            coherence_score=coherence_score,
            recommended_chunk_size=recommended_size,
            features=features
        )

    def _detect_content_type(self, text: str) -> ContentType:
        """Detect the primary content type of text."""
        scores = {
            ContentType.CODE: sum(1 for p in self._code_re if p.search(text)),
            ContentType.TABLE: sum(1 for p in self._table_re if p.search(text)),
            ContentType.LIST: sum(1 for p in self._list_re if p.search(text)),
            ContentType.HEADER: sum(1 for p in self._header_re if p.search(text)),
            ContentType.QUOTE: sum(1 for p in self._quote_re if p.search(text)),
            ContentType.FORMULA: sum(1 for p in self._formula_re if p.search(text)),
        }

        max_score = max(scores.values())
        if max_score == 0:
            return ContentType.PROSE

        # Check for mixed content
        high_scores = sum(1 for s in scores.values() if s >= max_score * 0.5)
        if high_scores > 1:
            return ContentType.MIXED

        for content_type, score in scores.items():
            if score == max_score:
                return content_type

        return ContentType.PROSE

    def _calculate_density(self, text: str) -> float:
        """
        Calculate information density score.

        Higher density = more unique words, entities, technical terms.
        """
        if not text:
            return 0.0

        words = text.split()
        if not words:
            return 0.0

        # Unique word ratio
        unique_ratio = len(set(words)) / len(words)

        # Average word length (longer words often carry more information)
        avg_word_len = sum(len(w) for w in words) / len(words)
        word_len_score = min(avg_word_len / 8.0, 1.0)  # Normalize to ~8 char average

        # Technical term detection (camelCase, snake_case, acronyms)
        technical_count = sum(1 for w in words if (
            '_' in w or
            any(c.isupper() for c in w[1:]) or
            w.isupper() and len(w) > 2
        ))
        technical_ratio = technical_count / len(words) if words else 0

        # Combine scores
        density = (unique_ratio * 0.4 + word_len_score * 0.3 + technical_ratio * 0.3)
        return min(density, 1.0)

    def _calculate_complexity(self, text: str) -> float:
        """
        Calculate text complexity score.

        Based on sentence length, nesting depth, vocabulary diversity.
        """
        if not text:
            return 0.0

        # Sentence complexity
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]

        if not sentences:
            return 0.0

        avg_sentence_len = sum(len(s.split()) for s in sentences) / len(sentences)
        sentence_complexity = min(avg_sentence_len / 25.0, 1.0)  # 25 words = complex

        # Nesting depth (parentheses, brackets)
        max_depth = 0
        current_depth = 0
        for char in text:
            if char in '([{':
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char in ')]}':
                current_depth = max(0, current_depth - 1)
        nesting_complexity = min(max_depth / 5.0, 1.0)

        # Vocabulary diversity (type-token ratio)
        words = text.lower().split()
        ttr = len(set(words)) / len(words) if words else 0

        return (sentence_complexity * 0.4 + nesting_complexity * 0.3 + ttr * 0.3)

    def _calculate_coherence(self, text: str) -> float:
        """
        Calculate text coherence score.

        Based on transition words, topic consistency, paragraph structure.
        """
        if not text:
            return 0.0

        # Transition word presence
        transition_words = [
            'however', 'therefore', 'furthermore', 'moreover', 'additionally',
            'consequently', 'nevertheless', 'meanwhile', 'similarly', 'likewise',
            'in contrast', 'on the other hand', 'as a result', 'for example',
            'in addition', 'first', 'second', 'third', 'finally', 'next'
        ]
        text_lower = text.lower()
        transition_count = sum(1 for tw in transition_words if tw in text_lower)
        transition_score = min(transition_count / 5.0, 1.0)

        # Paragraph structure
        paragraphs = text.split('\n\n')
        para_count = len([p for p in paragraphs if p.strip()])
        para_score = 1.0 if para_count > 0 else 0.0

        # Sentence connectivity (pronouns, references)
        reference_words = ['this', 'that', 'these', 'those', 'it', 'they', 'which']
        reference_count = sum(text_lower.count(w) for w in reference_words)
        words = text.split()
        reference_ratio = reference_count / len(words) if words else 0
        reference_score = min(reference_ratio * 10, 1.0)  # ~10% reference is good

        return (transition_score * 0.4 + para_score * 0.3 + reference_score * 0.3)

    def _calculate_recommended_size(
        self,
        content_type: ContentType,
        density: float,
        complexity: float,
        coherence: float,
        text_length: int
    ) -> int:
        """Calculate recommended chunk size based on analysis."""
        # Base size adjustments by content type
        type_multipliers = {
            ContentType.PROSE: 1.0,
            ContentType.CODE: 0.75,  # Smaller chunks for code
            ContentType.TABLE: 1.5,  # Keep tables together
            ContentType.LIST: 0.8,
            ContentType.HEADER: 0.5,  # Headers are short
            ContentType.QUOTE: 1.2,
            ContentType.FORMULA: 0.6,  # Formulas need precision
            ContentType.MIXED: 1.0
        }

        base = self.base_chunk_size * type_multipliers.get(content_type, 1.0)

        # Adjust for density (high density = smaller chunks for precision)
        density_adjustment = 1.0 - (density * 0.3)

        # Adjust for complexity (high complexity = smaller chunks)
        complexity_adjustment = 1.0 - (complexity * 0.2)

        # Adjust for coherence (high coherence = can use larger chunks)
        coherence_adjustment = 1.0 + (coherence * 0.2)

        # Calculate final size
        recommended = int(base * density_adjustment * complexity_adjustment * coherence_adjustment)

        # Clamp to bounds
        return max(self.min_chunk_size, min(self.max_chunk_size, recommended))

    def _extract_features(self, text: str) -> dict:
        """Extract numerical features for ML model."""
        words = text.split()
        sentences = re.split(r'[.!?]+', text)

        return {
            "char_count": len(text),
            "word_count": len(words),
            "sentence_count": len([s for s in sentences if s.strip()]),
            "avg_word_length": sum(len(w) for w in words) / len(words) if words else 0,
            "unique_word_ratio": len(set(words)) / len(words) if words else 0,
            "newline_ratio": text.count('\n') / len(text) if text else 0,
            "punctuation_ratio": sum(1 for c in text if c in '.,;:!?') / len(text) if text else 0,
            "uppercase_ratio": sum(1 for c in text if c.isupper()) / len(text) if text else 0,
            "digit_ratio": sum(1 for c in text if c.isdigit()) / len(text) if text else 0,
            "special_char_ratio": sum(1 for c in text if c in '{}[]()<>@#$%^&*') / len(text) if text else 0,
        }


class SemanticBoundaryDetector:
    """
    Detects semantic boundaries using sentence embeddings.

    Identifies topic shifts by comparing embedding similarity between
    adjacent text segments.
    """

    def __init__(
        self,
        embedding_fn: Optional[Callable[[str], list[float]]] = None,
        similarity_threshold: float = 0.5,
        window_size: int = 3
    ):
        """
        Initialize the detector.

        Args:
            embedding_fn: Function to generate embeddings (text -> vector)
            similarity_threshold: Below this similarity = topic shift
            window_size: Number of sentences to consider together
        """
        self.embedding_fn = embedding_fn
        self.similarity_threshold = similarity_threshold
        self.window_size = window_size

        # Cache for embeddings
        self._embedding_cache: dict[str, list[float]] = {}

    def detect_boundaries(self, text: str) -> list[BoundarySignal]:
        """
        Detect semantic boundaries in text.

        Args:
            text: Text to analyze

        Returns:
            List of boundary signals sorted by position
        """
        boundaries = []

        # Split into sentences
        sentence_pattern = r'(?<=[.!?])\s+(?=[A-Z])'
        sentences = re.split(sentence_pattern, text)

        if len(sentences) < 2:
            return boundaries

        # Calculate sentence positions
        positions = []
        current_pos = 0
        for sent in sentences:
            positions.append(current_pos)
            current_pos += len(sent) + 1  # +1 for space

        # Get embeddings for each sentence
        embeddings = []
        for sent in sentences:
            emb = self._get_embedding(sent)
            embeddings.append(emb)

        # Detect topic shifts using sliding window
        for i in range(1, len(sentences)):
            # Compare current sentence to previous window
            prev_window = self._average_embeddings(
                embeddings[max(0, i - self.window_size):i]
            )
            curr_window = self._average_embeddings(
                embeddings[i:min(len(embeddings), i + self.window_size)]
            )

            if prev_window is not None and curr_window is not None:
                similarity = self._cosine_similarity(prev_window, curr_window)

                if similarity < self.similarity_threshold:
                    # Topic shift detected
                    confidence = 1.0 - similarity  # Lower similarity = higher confidence
                    boundaries.append(BoundarySignal(
                        position=positions[i],
                        signal_type="topic_shift",
                        confidence=confidence,
                        context=sentences[i][:100]
                    ))

        return sorted(boundaries, key=lambda b: b.position)

    def _get_embedding(self, text: str) -> Optional[list[float]]:
        """Get embedding for text, using cache."""
        if text in self._embedding_cache:
            return self._embedding_cache[text]

        if self.embedding_fn is None:
            # Fallback: simple bag-of-words vector
            return self._simple_embedding(text)

        try:
            emb = self.embedding_fn(text)
            self._embedding_cache[text] = emb
            return emb
        except Exception as e:
            logger.warning(f"Embedding failed: {e}")
            return self._simple_embedding(text)

    def _simple_embedding(self, text: str) -> list[float]:
        """Simple fallback embedding using word hashing."""
        # Hash words to fixed-size vector
        vector_size = 128
        vector = [0.0] * vector_size

        words = text.lower().split()
        for word in words:
            idx = hash(word) % vector_size
            vector[idx] += 1.0

        # Normalize
        norm = math.sqrt(sum(v * v for v in vector))
        if norm > 0:
            vector = [v / norm for v in vector]

        return vector

    def _average_embeddings(self, embeddings: list[Optional[list[float]]]) -> Optional[list[float]]:
        """Average multiple embeddings."""
        valid = [e for e in embeddings if e is not None]
        if not valid:
            return None

        result = [0.0] * len(valid[0])
        for emb in valid:
            for i, v in enumerate(emb):
                result[i] += v

        return [v / len(valid) for v in result]

    def _cosine_similarity(self, a: list[float], b: list[float]) -> float:
        """Calculate cosine similarity between two vectors."""
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))

        if norm_a == 0 or norm_b == 0:
            return 0.0

        return dot / (norm_a * norm_b)

    def clear_cache(self):
        """Clear the embedding cache."""
        self._embedding_cache.clear()


class StructuralBoundaryDetector:
    """
    Detects structural boundaries in text.

    Identifies boundaries based on formatting, headers, code blocks, etc.
    """

    # Structural patterns
    PATTERNS = {
        "markdown_header": (r'^#{1,6}\s+.+$', 0.9),
        "underline_header": (r'^[^\n]+\n[=-]{3,}$', 0.85),
        "code_block_start": (r'^```\w*$', 0.95),
        "code_block_end": (r'^```$', 0.95),
        "horizontal_rule": (r'^[-*_]{3,}$', 0.8),
        "list_start": (r'^[-*•]\s+', 0.6),
        "numbered_list": (r'^\d+[.)]\s+', 0.6),
        "blockquote": (r'^>\s+', 0.5),
        "blank_lines": (r'\n\n+', 0.4),
        "section_marker": (r'^(Section|Chapter|Part)\s+\d+', 0.9),
    }

    def __init__(self):
        self._compiled = {
            name: (re.compile(pattern, re.MULTILINE), confidence)
            for name, (pattern, confidence) in self.PATTERNS.items()
        }

    def detect_boundaries(self, text: str) -> list[BoundarySignal]:
        """
        Detect structural boundaries in text.

        Args:
            text: Text to analyze

        Returns:
            List of boundary signals
        """
        boundaries = []

        for signal_type, (pattern, base_confidence) in self._compiled.items():
            for match in pattern.finditer(text):
                boundaries.append(BoundarySignal(
                    position=match.start(),
                    signal_type=signal_type,
                    confidence=base_confidence,
                    context=match.group()[:50]
                ))

        return sorted(boundaries, key=lambda b: b.position)


@dataclass
class BoundaryPrediction:
    """ML model prediction for a potential boundary."""
    position: int
    probability: float
    features: dict


class MLChunkBoundaryModel:
    """
    ML-based model for predicting optimal chunk boundaries.

    Uses a combination of features to learn where to split text:
    - Semantic similarity drops
    - Structural patterns
    - Content type transitions
    - Historical chunk performance
    """

    def __init__(self):
        # Feature weights (learned from feedback)
        self.weights = {
            "semantic_drop": 0.3,
            "structural_signal": 0.25,
            "content_type_change": 0.2,
            "sentence_boundary": 0.15,
            "paragraph_boundary": 0.1,
        }

        # Performance tracking for online learning
        self.boundary_outcomes: list[dict] = []
        self.learning_rate = 0.01

    def predict_boundaries(
        self,
        text: str,
        semantic_signals: list[BoundarySignal],
        structural_signals: list[BoundarySignal],
        content_analysis: ContentAnalysis,
        target_chunk_size: int
    ) -> list[ChunkBoundary]:
        """
        Predict optimal chunk boundaries using ML.

        Args:
            text: Text to chunk
            semantic_signals: Signals from semantic detector
            structural_signals: Signals from structural detector
            content_analysis: Content analysis results
            target_chunk_size: Target size for chunks

        Returns:
            List of recommended boundaries
        """
        # Merge all signals
        all_signals = semantic_signals + structural_signals
        signal_map = defaultdict(list)
        for signal in all_signals:
            signal_map[signal.position].append(signal)

        # Generate candidate boundaries
        candidates = self._generate_candidates(text, target_chunk_size, signal_map)

        # Score each candidate
        scored = []
        for pos, signals in candidates.items():
            features = self._extract_boundary_features(text, pos, signals, content_analysis)
            score = self._calculate_boundary_score(features)

            if score > 0.3:  # Minimum threshold
                scored.append(ChunkBoundary(
                    position=pos,
                    confidence=score,
                    signals=signals,
                    chunk_type=content_analysis.content_type,
                    reasoning=self._generate_reasoning(features, signals)
                ))

        # Select optimal boundaries
        return self._select_boundaries(scored, text, target_chunk_size)

    def _generate_candidates(
        self,
        text: str,
        target_size: int,
        signal_map: dict[int, list[BoundarySignal]]
    ) -> dict[int, list[BoundarySignal]]:
        """Generate candidate boundary positions."""
        candidates = dict(signal_map)

        # Add sentence boundaries
        for match in re.finditer(r'[.!?]\s+', text):
            pos = match.end()
            if pos not in candidates:
                candidates[pos] = []
            candidates[pos].append(BoundarySignal(
                position=pos,
                signal_type="sentence_boundary",
                confidence=0.5,
                context=""
            ))

        # Add paragraph boundaries
        for match in re.finditer(r'\n\n+', text):
            pos = match.start()
            if pos not in candidates:
                candidates[pos] = []
            candidates[pos].append(BoundarySignal(
                position=pos,
                signal_type="paragraph_boundary",
                confidence=0.6,
                context=""
            ))

        # Add periodic boundaries at target size intervals
        for i in range(target_size, len(text), target_size):
            # Find nearest sentence boundary
            search_start = max(0, i - 100)
            search_end = min(len(text), i + 100)
            search_text = text[search_start:search_end]

            best_pos = None
            for match in re.finditer(r'[.!?]\s+', search_text):
                candidate_pos = search_start + match.end()
                if best_pos is None or abs(candidate_pos - i) < abs(best_pos - i):
                    best_pos = candidate_pos

            if best_pos and best_pos not in candidates:
                candidates[best_pos] = [BoundarySignal(
                    position=best_pos,
                    signal_type="periodic_boundary",
                    confidence=0.4,
                    context=""
                )]

        return candidates

    def _extract_boundary_features(
        self,
        text: str,
        position: int,
        signals: list[BoundarySignal],
        analysis: ContentAnalysis
    ) -> dict:
        """Extract features for a boundary position."""
        # Context around boundary
        context_before = text[max(0, position - 100):position]
        context_after = text[position:min(len(text), position + 100)]

        return {
            "position_ratio": position / len(text) if text else 0,
            "semantic_signal_strength": max(
                (s.confidence for s in signals if s.signal_type == "topic_shift"),
                default=0
            ),
            "structural_signal_strength": max(
                (s.confidence for s in signals if s.signal_type not in ["topic_shift", "sentence_boundary", "paragraph_boundary"]),
                default=0
            ),
            "is_sentence_boundary": any(s.signal_type == "sentence_boundary" for s in signals),
            "is_paragraph_boundary": any(s.signal_type == "paragraph_boundary" for s in signals),
            "content_density": analysis.density_score,
            "content_complexity": analysis.complexity_score,
            "signal_count": len(signals),
            "before_ends_sentence": context_before.rstrip().endswith(('.', '!', '?')),
            "after_starts_capital": context_after.lstrip()[:1].isupper() if context_after.strip() else False,
        }

    def _calculate_boundary_score(self, features: dict) -> float:
        """Calculate boundary score using weighted features."""
        score = 0.0

        # Semantic signals are strong indicators
        score += features["semantic_signal_strength"] * self.weights["semantic_drop"]

        # Structural signals
        score += features["structural_signal_strength"] * self.weights["structural_signal"]

        # Sentence boundaries
        if features["is_sentence_boundary"]:
            score += 0.3 * self.weights["sentence_boundary"]

        # Paragraph boundaries
        if features["is_paragraph_boundary"]:
            score += 0.5 * self.weights["paragraph_boundary"]

        # Proper formatting
        if features["before_ends_sentence"] and features["after_starts_capital"]:
            score += 0.2

        # Multiple signals reinforce each other
        if features["signal_count"] > 1:
            score *= 1.0 + (features["signal_count"] - 1) * 0.1

        return min(score, 1.0)

    def _generate_reasoning(self, features: dict, signals: list[BoundarySignal]) -> str:
        """Generate human-readable reasoning for boundary."""
        reasons = []

        if features["semantic_signal_strength"] > 0.5:
            reasons.append("topic shift detected")
        if features["structural_signal_strength"] > 0.5:
            signal_types = [s.signal_type for s in signals if s.signal_type not in ["topic_shift", "sentence_boundary"]]
            if signal_types:
                reasons.append(f"structural: {signal_types[0]}")
        if features["is_paragraph_boundary"]:
            reasons.append("paragraph break")
        if features["is_sentence_boundary"]:
            reasons.append("sentence end")

        return "; ".join(reasons) if reasons else "periodic boundary"

    def _select_boundaries(
        self,
        candidates: list[ChunkBoundary],
        text: str,
        target_size: int
    ) -> list[ChunkBoundary]:
        """Select optimal set of boundaries."""
        if not candidates:
            return []

        # Sort by position
        candidates = sorted(candidates, key=lambda b: b.position)

        # Greedy selection to ensure reasonable chunk sizes
        selected = []
        last_pos = 0
        min_chunk = target_size // 3
        max_chunk = target_size * 2

        for boundary in candidates:
            chunk_size = boundary.position - last_pos

            # Too small - skip
            if chunk_size < min_chunk:
                continue

            # Too large - we need a boundary somewhere
            if chunk_size > max_chunk:
                # Find best boundary in this range
                range_candidates = [
                    b for b in candidates
                    if last_pos + min_chunk <= b.position <= last_pos + max_chunk
                ]
                if range_candidates:
                    best = max(range_candidates, key=lambda b: b.confidence)
                    selected.append(best)
                    last_pos = best.position
                else:
                    selected.append(boundary)
                    last_pos = boundary.position
            elif boundary.confidence > 0.5:
                selected.append(boundary)
                last_pos = boundary.position

        return selected

    def record_outcome(self, boundary: ChunkBoundary, success: bool, metrics: dict):
        """
        Record boundary performance for online learning.

        Args:
            boundary: The boundary that was used
            success: Whether the resulting chunk performed well
            metrics: Performance metrics (retrieval score, user feedback, etc.)
        """
        self.boundary_outcomes.append({
            "boundary": boundary,
            "success": success,
            "metrics": metrics
        })

        # Online weight update
        if len(self.boundary_outcomes) >= 10:
            self._update_weights()

    def _update_weights(self):
        """Update weights based on recorded outcomes."""
        # Simple online learning: adjust weights based on success rate
        recent = self.boundary_outcomes[-100:]  # Last 100 outcomes

        for signal_type in self.weights:
            successes = [
                o for o in recent
                if o["success"] and any(
                    s.signal_type == signal_type or signal_type in s.signal_type
                    for s in o["boundary"].signals
                )
            ]
            failures = [
                o for o in recent
                if not o["success"] and any(
                    s.signal_type == signal_type or signal_type in s.signal_type
                    for s in o["boundary"].signals
                )
            ]

            if successes or failures:
                success_rate = len(successes) / (len(successes) + len(failures))
                # Adjust weight toward success rate
                current = self.weights[signal_type]
                self.weights[signal_type] = current + self.learning_rate * (success_rate - current)

        # Normalize weights
        total = sum(self.weights.values())
        if total > 0:
            self.weights = {k: v / total for k, v in self.weights.items()}

    def get_learning_stats(self) -> dict:
        """Get statistics about model learning."""
        if not self.boundary_outcomes:
            return {"total_samples": 0}

        recent = self.boundary_outcomes[-100:]
        success_rate = sum(1 for o in recent if o["success"]) / len(recent)

        return {
            "total_samples": len(self.boundary_outcomes),
            "recent_success_rate": success_rate,
            "current_weights": dict(self.weights)
        }


class DynamicChunkSizer:
    """
    Dynamically adjusts chunk sizes based on content characteristics.

    Combines content analysis with ML predictions to determine
    optimal chunk sizes for different parts of a document.
    """

    def __init__(
        self,
        content_analyzer: ContentAnalyzer,
        base_parent_size: int = 1024,
        base_child_size: int = 256,
        size_variance: float = 0.5  # Allow 50% variance from base
    ):
        self.content_analyzer = content_analyzer
        self.base_parent_size = base_parent_size
        self.base_child_size = base_child_size
        self.size_variance = size_variance

        # Track sizing performance
        self.sizing_history: list[dict] = []

    def get_chunk_sizes(
        self,
        text: str,
        position: int = 0,
        context_analysis: Optional[ContentAnalysis] = None
    ) -> tuple[int, int]:
        """
        Get recommended parent and child chunk sizes for text.

        Args:
            text: Text to chunk
            position: Position in document (for context)
            context_analysis: Pre-computed analysis (optional)

        Returns:
            Tuple of (parent_size, child_size)
        """
        # Analyze content if not provided
        if context_analysis is None:
            context_analysis = self.content_analyzer.analyze(text)

        # Calculate adjustments
        parent_size = self._adjust_size(
            self.base_parent_size,
            context_analysis,
            is_parent=True
        )

        child_size = self._adjust_size(
            self.base_child_size,
            context_analysis,
            is_parent=False
        )

        # Ensure child is smaller than parent
        if child_size >= parent_size:
            child_size = parent_size // 4

        return parent_size, child_size

    def _adjust_size(
        self,
        base_size: int,
        analysis: ContentAnalysis,
        is_parent: bool
    ) -> int:
        """Adjust size based on content analysis."""
        # Start with recommended size from analysis
        size = analysis.recommended_chunk_size

        # Scale for parent vs child
        if is_parent:
            size = int(size * (self.base_parent_size / self.content_analyzer.base_chunk_size))
        else:
            size = int(size * (self.base_child_size / self.content_analyzer.base_chunk_size))

        # Apply variance limits
        min_size = int(base_size * (1 - self.size_variance))
        max_size = int(base_size * (1 + self.size_variance))

        return max(min_size, min(max_size, size))

    def record_performance(
        self,
        parent_size: int,
        child_size: int,
        content_type: ContentType,
        retrieval_score: float,
        user_feedback: Optional[float] = None
    ):
        """Record chunk size performance for learning."""
        self.sizing_history.append({
            "parent_size": parent_size,
            "child_size": child_size,
            "content_type": content_type,
            "retrieval_score": retrieval_score,
            "user_feedback": user_feedback
        })

    def get_optimal_sizes_for_type(self, content_type: ContentType) -> tuple[int, int]:
        """Get historically optimal sizes for a content type."""
        type_history = [
            h for h in self.sizing_history
            if h["content_type"] == content_type and h.get("retrieval_score", 0) > 0.5
        ]

        if not type_history:
            return self.base_parent_size, self.base_child_size

        # Use sizes from high-performing chunks
        avg_parent = statistics.mean(h["parent_size"] for h in type_history)
        avg_child = statistics.mean(h["child_size"] for h in type_history)

        return int(avg_parent), int(avg_child)


class AdaptiveParentChildChunker:
    """
    Advanced parent-child chunker with ML-based boundary detection
    and dynamic chunk sizing.

    Combines:
    - Content analysis for understanding document structure
    - Semantic boundary detection for topic-aware splitting
    - Structural boundary detection for format-aware splitting
    - ML model for optimal boundary prediction
    - Dynamic sizing based on content characteristics
    """

    def __init__(
        self,
        embedding_fn: Optional[Callable[[str], list[float]]] = None,
        base_parent_size: int = 1024,
        base_child_size: int = 256,
        min_chunk_size: int = 50,
        enable_ml_boundaries: bool = True,
        enable_dynamic_sizing: bool = True
    ):
        """
        Initialize the adaptive chunker.

        Args:
            embedding_fn: Function to generate embeddings for semantic detection
            base_parent_size: Base size for parent chunks
            base_child_size: Base size for child chunks
            min_chunk_size: Minimum chunk size
            enable_ml_boundaries: Whether to use ML boundary detection
            enable_dynamic_sizing: Whether to use dynamic sizing
        """
        # Core components
        self.content_analyzer = ContentAnalyzer(
            base_chunk_size=(base_parent_size + base_child_size) // 2
        )
        self.semantic_detector = SemanticBoundaryDetector(
            embedding_fn=embedding_fn
        )
        self.structural_detector = StructuralBoundaryDetector()
        self.boundary_model = MLChunkBoundaryModel()
        self.dynamic_sizer = DynamicChunkSizer(
            content_analyzer=self.content_analyzer,
            base_parent_size=base_parent_size,
            base_child_size=base_child_size
        )

        # Configuration
        self.base_parent_size = base_parent_size
        self.base_child_size = base_child_size
        self.min_chunk_size = min_chunk_size
        self.enable_ml_boundaries = enable_ml_boundaries
        self.enable_dynamic_sizing = enable_dynamic_sizing

        # Storage
        self.parent_store: dict[str, Chunk] = {}
        self.child_to_parent: dict[str, str] = {}

        # Analytics
        self.chunking_stats: list[dict] = []

        logger.info(
            f"AdaptiveParentChildChunker initialized: "
            f"ml_boundaries={enable_ml_boundaries}, dynamic_sizing={enable_dynamic_sizing}"
        )

    def chunk_document(
        self,
        text: str,
        document_id: str,
        metadata: Optional[dict] = None
    ) -> tuple[list[Chunk], list[Chunk]]:
        """
        Chunk a document using adaptive strategies.

        Args:
            text: Document text
            document_id: Unique document ID
            metadata: Optional metadata

        Returns:
            Tuple of (parent_chunks, child_chunks)
        """
        metadata = metadata or {}

        # Analyze content
        analysis = self.content_analyzer.analyze(text)

        # Get dynamic sizes
        if self.enable_dynamic_sizing:
            parent_size, child_size = self.dynamic_sizer.get_chunk_sizes(text, 0, analysis)
        else:
            parent_size = self.base_parent_size
            child_size = self.base_child_size

        # Detect boundaries
        if self.enable_ml_boundaries:
            semantic_signals = self.semantic_detector.detect_boundaries(text)
            structural_signals = self.structural_detector.detect_boundaries(text)

            boundaries = self.boundary_model.predict_boundaries(
                text=text,
                semantic_signals=semantic_signals,
                structural_signals=structural_signals,
                content_analysis=analysis,
                target_chunk_size=parent_size
            )
        else:
            boundaries = []

        # Create parent chunks using boundaries
        if boundaries:
            parent_chunks = self._create_chunks_from_boundaries(
                text=text,
                boundaries=boundaries,
                level=ChunkLevel.PARENT,
                document_id=document_id,
                metadata=metadata,
                target_size=parent_size
            )
        else:
            # Fallback to traditional chunking
            parent_chunks = self._create_chunks_traditional(
                text=text,
                chunk_size=parent_size,
                level=ChunkLevel.PARENT,
                document_id=document_id,
                metadata=metadata
            )

        # Store parents
        for parent in parent_chunks:
            self.parent_store[parent.id] = parent

        # Create child chunks from each parent
        all_child_chunks = []
        for parent in parent_chunks:
            # Analyze parent content for child sizing
            parent_analysis = self.content_analyzer.analyze(parent.content)

            if self.enable_dynamic_sizing:
                _, child_chunk_size = self.dynamic_sizer.get_chunk_sizes(
                    parent.content, parent.start_char, parent_analysis
                )
            else:
                child_chunk_size = child_size

            child_chunks = self._create_chunks_traditional(
                text=parent.content,
                chunk_size=child_chunk_size,
                level=ChunkLevel.CHILD,
                document_id=document_id,
                parent_id=parent.id,
                metadata={**metadata, "parent_id": parent.id},
                base_offset=parent.start_char
            )

            for child in child_chunks:
                self.child_to_parent[child.id] = parent.id

            all_child_chunks.extend(child_chunks)

        # Record stats
        self.chunking_stats.append({
            "document_id": document_id,
            "content_type": analysis.content_type.value,
            "parent_size_used": parent_size,
            "child_size_used": child_size,
            "parent_count": len(parent_chunks),
            "child_count": len(all_child_chunks),
            "boundaries_detected": len(boundaries),
            "analysis": {
                "density": analysis.density_score,
                "complexity": analysis.complexity_score,
                "coherence": analysis.coherence_score
            }
        })

        logger.info(
            f"Adaptive chunking for {document_id}: "
            f"type={analysis.content_type.value}, "
            f"parents={len(parent_chunks)}, children={len(all_child_chunks)}, "
            f"boundaries={len(boundaries)}"
        )

        return parent_chunks, all_child_chunks

    def _create_chunks_from_boundaries(
        self,
        text: str,
        boundaries: list[ChunkBoundary],
        level: ChunkLevel,
        document_id: str,
        metadata: dict,
        target_size: int,
        parent_id: Optional[str] = None,
        base_offset: int = 0
    ) -> list[Chunk]:
        """Create chunks using ML-detected boundaries."""
        chunks = []

        # Add start and end positions
        positions = [0] + [b.position for b in boundaries] + [len(text)]

        for i in range(len(positions) - 1):
            start = positions[i]
            end = positions[i + 1]

            chunk_text = text[start:end].strip()

            if len(chunk_text) >= self.min_chunk_size:
                chunks.append(Chunk(
                    id="",
                    content=chunk_text,
                    level=level,
                    parent_id=parent_id,
                    document_id=document_id,
                    metadata={
                        **metadata,
                        "boundary_confidence": boundaries[i].confidence if i < len(boundaries) else 1.0,
                        "chunk_type": boundaries[i].chunk_type.value if i < len(boundaries) else "default"
                    },
                    start_char=base_offset + start,
                    end_char=base_offset + end
                ))

        return chunks

    def _create_chunks_traditional(
        self,
        text: str,
        chunk_size: int,
        level: ChunkLevel,
        document_id: str,
        metadata: dict,
        parent_id: Optional[str] = None,
        base_offset: int = 0
    ) -> list[Chunk]:
        """Create chunks using traditional overlap-based method."""
        chunks = []
        overlap = chunk_size // 8  # 12.5% overlap

        if len(text) <= chunk_size:
            chunk = Chunk(
                id="",
                content=text.strip(),
                level=level,
                parent_id=parent_id,
                document_id=document_id,
                metadata=metadata,
                start_char=base_offset,
                end_char=base_offset + len(text)
            )
            if len(chunk.content) >= self.min_chunk_size:
                chunks.append(chunk)
            return chunks

        start = 0
        while start < len(text):
            end = start + chunk_size

            # Try to break at sentence boundary
            if end < len(text):
                search_start = max(start + chunk_size - 100, start)
                search_end = min(start + chunk_size + 50, len(text))
                search_text = text[search_start:search_end]

                best_break = self._find_sentence_break(search_text)
                if best_break > 0:
                    end = search_start + best_break

            chunk_text = text[start:end].strip()

            if len(chunk_text) >= self.min_chunk_size:
                chunks.append(Chunk(
                    id="",
                    content=chunk_text,
                    level=level,
                    parent_id=parent_id,
                    document_id=document_id,
                    metadata=metadata,
                    start_char=base_offset + start,
                    end_char=base_offset + end
                ))

            start = end - overlap
            if start >= len(text) - self.min_chunk_size:
                break

        return chunks

    def _find_sentence_break(self, text: str) -> int:
        """Find the best sentence break point."""
        endings = ['. ', '.\n', '? ', '?\n', '! ', '!\n', '\n\n']

        best_pos = -1
        for ending in endings:
            pos = text.rfind(ending)
            if pos > 0:
                best_pos = pos + len(ending)
                break

        return best_pos

    def get_parent_for_child(self, child_id: str) -> Optional[Chunk]:
        """Get parent chunk for a child."""
        parent_id = self.child_to_parent.get(child_id)
        if parent_id:
            return self.parent_store.get(parent_id)
        return None

    def expand_to_parent(
        self,
        child_chunks: list[dict],
        deduplicate: bool = True
    ) -> list[dict]:
        """Expand child results to parent context."""
        parent_scores: dict[str, float] = {}
        parent_metadata: dict[str, dict] = {}

        for child in child_chunks:
            child_id = child.get("id") or child.get("chunk_id")
            score = child.get("score", 0.0)

            parent_id = self.child_to_parent.get(child_id)
            if parent_id:
                if parent_id in parent_scores:
                    parent_scores[parent_id] = max(parent_scores[parent_id], score)
                else:
                    parent_scores[parent_id] = score
                    parent_metadata[parent_id] = child.get("metadata", {})

        results = []
        for parent_id, score in sorted(parent_scores.items(), key=lambda x: x[1], reverse=True):
            parent = self.parent_store.get(parent_id)
            if parent:
                results.append({
                    "id": parent_id,
                    "content": parent.content,
                    "score": score,
                    "level": "parent",
                    "document_id": parent.document_id,
                    "metadata": {**parent.metadata, **parent_metadata.get(parent_id, {})},
                    "start_char": parent.start_char,
                    "end_char": parent.end_char
                })

        return results

    def record_retrieval_feedback(
        self,
        chunk_id: str,
        retrieval_score: float,
        user_feedback: Optional[float] = None
    ):
        """Record feedback for learning."""
        parent_id = self.child_to_parent.get(chunk_id, chunk_id)
        parent = self.parent_store.get(parent_id)

        if parent:
            # Find the stats for this document
            stats = next(
                (s for s in self.chunking_stats if s["document_id"] == parent.document_id),
                None
            )

            if stats:
                self.dynamic_sizer.record_performance(
                    parent_size=stats["parent_size_used"],
                    child_size=stats["child_size_used"],
                    content_type=ContentType(stats["content_type"]),
                    retrieval_score=retrieval_score,
                    user_feedback=user_feedback
                )

    def get_analytics(self) -> dict:
        """Get chunking analytics."""
        if not self.chunking_stats:
            return {"total_documents": 0}

        return {
            "total_documents": len(self.chunking_stats),
            "total_parents": len(self.parent_store),
            "total_children": len(self.child_to_parent),
            "content_type_distribution": {
                ct.value: sum(1 for s in self.chunking_stats if s["content_type"] == ct.value)
                for ct in ContentType
            },
            "avg_boundaries_per_doc": statistics.mean(
                s["boundaries_detected"] for s in self.chunking_stats
            ) if self.chunking_stats else 0,
            "ml_model_stats": self.boundary_model.get_learning_stats(),
            "recent_stats": self.chunking_stats[-5:] if len(self.chunking_stats) >= 5 else self.chunking_stats
        }

    def clear(self):
        """Clear all stored data."""
        self.parent_store.clear()
        self.child_to_parent.clear()
        self.semantic_detector.clear_cache()
        logger.info("AdaptiveParentChildChunker cleared")


# =============================================================================
# Factory Functions and Singletons
# =============================================================================

_adaptive_chunker: Optional[AdaptiveParentChildChunker] = None


def get_adaptive_chunker(
    embedding_fn: Optional[Callable[[str], list[float]]] = None,
    **kwargs
) -> AdaptiveParentChildChunker:
    """
    Get or create the singleton adaptive chunker.

    Args:
        embedding_fn: Optional embedding function for semantic detection
        **kwargs: Additional arguments for AdaptiveParentChildChunker

    Returns:
        AdaptiveParentChildChunker instance
    """
    global _adaptive_chunker

    if _adaptive_chunker is None:
        _adaptive_chunker = AdaptiveParentChildChunker(
            embedding_fn=embedding_fn,
            **kwargs
        )

    return _adaptive_chunker


def reset_adaptive_chunker():
    """Reset the singleton adaptive chunker."""
    global _adaptive_chunker
    if _adaptive_chunker:
        _adaptive_chunker.clear()
    _adaptive_chunker = None


async def chunk_document_adaptive(
    text: str,
    document_id: str,
    embedding_fn: Optional[Callable[[str], list[float]]] = None,
    metadata: Optional[dict] = None
) -> tuple[list[dict], list[dict]]:
    """
    Convenience function to chunk a document with adaptive strategies.

    Args:
        text: Document text
        document_id: Unique document ID
        embedding_fn: Optional embedding function
        metadata: Optional metadata

    Returns:
        Tuple of (parent_chunks_as_dicts, child_chunks_as_dicts)
    """
    chunker = get_adaptive_chunker(embedding_fn=embedding_fn)

    parents, children = chunker.chunk_document(
        text=text,
        document_id=document_id,
        metadata=metadata
    )

    parent_dicts = [
        {
            "id": p.id,
            "content": p.content,
            "level": p.level.value,
            "document_id": p.document_id,
            "start_char": p.start_char,
            "end_char": p.end_char,
            "metadata": p.metadata
        }
        for p in parents
    ]

    child_dicts = [
        {
            "id": c.id,
            "content": c.content,
            "level": c.level.value,
            "parent_id": c.parent_id,
            "document_id": c.document_id,
            "start_char": c.start_char,
            "end_char": c.end_char,
            "metadata": c.metadata
        }
        for c in children
    ]

    return parent_dicts, child_dicts


# Convenience function for simple usage
def create_parent_child_chunks(
    text: str,
    document_id: str,
    parent_size: int = 1024,
    child_size: int = 256,
    metadata: Optional[dict] = None
) -> tuple[list[dict], list[dict]]:
    """
    Simple function to create parent-child chunks from text.

    Args:
        text: Text to chunk
        document_id: Document identifier
        parent_size: Parent chunk size in characters
        child_size: Child chunk size in characters
        metadata: Optional metadata

    Returns:
        Tuple of (parent_chunks_as_dicts, child_chunks_as_dicts)
    """
    chunker = ParentChildChunker(
        parent_chunk_size=parent_size,
        child_chunk_size=child_size
    )

    parents, children = chunker.chunk_document(
        text=text,
        document_id=document_id,
        metadata=metadata
    )

    parent_dicts = [
        {
            "id": p.id,
            "content": p.content,
            "level": p.level.value,
            "document_id": p.document_id,
            "start_char": p.start_char,
            "end_char": p.end_char,
            "metadata": p.metadata
        }
        for p in parents
    ]

    child_dicts = [
        {
            "id": c.id,
            "content": c.content,
            "level": c.level.value,
            "parent_id": c.parent_id,
            "document_id": c.document_id,
            "start_char": c.start_char,
            "end_char": c.end_char,
            "metadata": c.metadata
        }
        for c in children
    ]

    return parent_dicts, child_dicts
