"""
GraphRAG - Hybrid Graph-Based Retrieval Augmented Generation for DriveSentinel

This module implements a hybrid RAG system that combines:
1. Knowledge Graph (NetworkX) for entity relationships
2. Vector Embeddings (pgvector) for semantic similarity
3. Multi-Agent AI (Ollama) for entity extraction and generation

Features:
- Entity & Relationship Extraction using Ollama models
- In-Memory Knowledge Graph with NetworkX
- Graph-Enhanced Document Analysis
- Prerequisite ordering via topological sort
- Community detection for topic grouping
- Hybrid RAG pipeline combining graph + vector queries

Based on FlashCards/Memobyte implementation patterns.
"""

import logging
import asyncio
import json
import re
import hashlib
import os
from typing import Dict, List, Any, Optional, Tuple, Set, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict, Counter

# NetworkX for graph operations
try:
    import networkx as nx
    from networkx.algorithms import community as nx_community
    NETWORKX_AVAILABLE = True
except ImportError:
    NETWORKX_AVAILABLE = False
    nx = None

# Numpy for numerical operations
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    np = None

# Sentence transformers for embeddings - lazy loaded to avoid OOM at startup
# EMBEDDING_AVAILABLE will be set when first needed
EMBEDDING_AVAILABLE = None  # None = not yet checked
_sentence_transformer_class = None
_cosine_similarity_func = None

def _ensure_embedding_available() -> bool:
    """Lazily check/load embedding dependencies"""
    global EMBEDDING_AVAILABLE, _sentence_transformer_class, _cosine_similarity_func
    if EMBEDDING_AVAILABLE is not None:
        return EMBEDDING_AVAILABLE
    try:
        from sentence_transformers import SentenceTransformer
        from sklearn.metrics.pairwise import cosine_similarity
        _sentence_transformer_class = SentenceTransformer
        _cosine_similarity_func = cosine_similarity
        EMBEDDING_AVAILABLE = True
    except ImportError:
        EMBEDDING_AVAILABLE = False
    return EMBEDDING_AVAILABLE

# Async HTTP for Ollama API
import aiohttp

# spaCy for fast entity extraction
try:
    import spacy
    from spacy.matcher import Matcher
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False
    spacy = None

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

class RelationshipType(Enum):
    """Types of relationships between entities."""
    CAUSES = "causes"
    LEADS_TO = "leads_to"
    PART_OF = "part_of"
    CONTRASTS_WITH = "contrasts_with"
    PREREQUISITE_OF = "prerequisite_of"
    EXAMPLE_OF = "example_of"
    DEFINES = "defines"
    CONTAINS = "contains"
    SIMILAR_TO = "similar_to"
    DEPENDS_ON = "depends_on"
    RELATED_TO = "related_to"


@dataclass
class Entity:
    """Represents an entity extracted from content."""
    name: str
    entity_type: str  # concept, term, process, person, organization, etc.
    description: str = ""
    source_text: str = ""
    importance_score: float = 0.5
    embedding: Optional[List[float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def __hash__(self):
        return hash(self.name.lower())
    
    def __eq__(self, other):
        if isinstance(other, Entity):
            return self.name.lower() == other.name.lower()
        return False


@dataclass
class Relationship:
    """Represents a relationship between two entities."""
    source: str
    target: str
    relationship_type: RelationshipType
    description: str = ""
    confidence: float = 0.8
    source_text: str = ""
    
    def to_triple(self) -> Tuple[str, str, str]:
        """Return as (subject, predicate, object) triple."""
        return (self.source, self.relationship_type.value, self.target)


@dataclass
class ExtractionResult:
    """Result of entity/relationship extraction."""
    entities: List[Entity]
    relationships: List[Relationship]
    extraction_time: float
    model_used: str
    content_hash: str


# =============================================================================
# CONTENT KNOWLEDGE GRAPH
# =============================================================================

class ContentKnowledgeGraph:
    """
    In-memory knowledge graph for content analysis.
    
    Uses NetworkX for graph operations including:
    - Entity and relationship storage
    - Prerequisite ordering (topological sort)
    - Community detection for topic grouping
    - Path finding for related concepts
    """
    
    def __init__(self):
        """Initialize the knowledge graph."""
        if not NETWORKX_AVAILABLE:
            raise ImportError("NetworkX is required. Install with: pip install networkx")
        
        self.graph = nx.DiGraph()
        self.entities: Dict[str, Entity] = {}
        self.relationships: List[Relationship] = []
        self._embedding_model = None
        self._communities: Optional[List[Set[str]]] = None
        
        logger.info("ContentKnowledgeGraph initialized")
    
    @property
    def embedding_model(self):
        """Lazy-load embedding model."""
        if self._embedding_model is None and _ensure_embedding_available():
            try:
                ml_device = os.environ.get('ML_DEVICE', 'cpu')
                os.environ['HF_HUB_OFFLINE'] = '1'
                os.environ['TRANSFORMERS_OFFLINE'] = '1'
                self._embedding_model = _sentence_transformer_class(
                    'all-MiniLM-L6-v2',
                    device=ml_device
                )
                logger.info("Embedding model loaded (offline/cached mode)")
            except Exception as e:
                logger.warning(f"Failed to load embedding model: {e}")
                self._embedding_model = None
        return self._embedding_model
    
    def clear(self):
        """Clear the graph."""
        self.graph.clear()
        self.entities.clear()
        self.relationships.clear()
        self._communities = None
        logger.info("Knowledge graph cleared")
    
    # =========================================================================
    # ENTITY MANAGEMENT
    # =========================================================================
    
    def add_entity(self, entity: Entity) -> bool:
        """Add an entity to the graph."""
        key = entity.name.lower()

        if key in self.entities:
            # Merge metadata and update timestamp
            existing = self.entities[key]
            existing.importance_score = max(existing.importance_score, entity.importance_score)
            if entity.description and not existing.description:
                existing.description = entity.description
            existing.metadata.update(entity.metadata)
            existing.updated_at = datetime.now()  # Track last update
            return False
        
        self.entities[key] = entity
        self.graph.add_node(
            key,
            name=entity.name,
            type=entity.entity_type,
            description=entity.description,
            importance=entity.importance_score,
            created_at=entity.created_at.isoformat(),
            updated_at=entity.updated_at.isoformat(),
            **entity.metadata
        )
        
        return True
    
    def get_entity(self, name: str) -> Optional[Entity]:
        """Get an entity by name."""
        return self.entities.get(name.lower())
    
    def get_all_entities(self) -> List[Entity]:
        """Get all entities."""
        return list(self.entities.values())
    
    def get_entities_by_type(self, entity_type: str) -> List[Entity]:
        """Get entities of a specific type."""
        return [e for e in self.entities.values() if e.entity_type == entity_type]
    
    # =========================================================================
    # RELATIONSHIP MANAGEMENT
    # =========================================================================
    
    def add_relationship(self, relationship: Relationship) -> bool:
        """Add a relationship between entities."""
        source_key = relationship.source.lower()
        target_key = relationship.target.lower()
        
        # Auto-create entities if they don't exist
        if source_key not in self.entities:
            self.add_entity(Entity(
                name=relationship.source,
                entity_type="concept",
                importance_score=0.3
            ))
        
        if target_key not in self.entities:
            self.add_entity(Entity(
                name=relationship.target,
                entity_type="concept",
                importance_score=0.3
            ))
        
        self.relationships.append(relationship)
        self.graph.add_edge(
            source_key,
            target_key,
            relationship=relationship.relationship_type.value,
            description=relationship.description,
            confidence=relationship.confidence
        )
        
        self._communities = None  # Invalidate community cache
        return True
    
    def get_relationships(self, entity_name: str) -> List[Relationship]:
        """Get all relationships involving an entity."""
        key = entity_name.lower()
        return [r for r in self.relationships 
                if r.source.lower() == key or r.target.lower() == key]
    
    # =========================================================================
    # GRAPH QUERIES
    # =========================================================================
    
    def get_related_concepts(self, entity: str, depth: int = 2) -> nx.DiGraph:
        """Get subgraph of concepts related to an entity within given depth."""
        key = entity.lower()
        if key not in self.graph:
            return nx.DiGraph()
        
        return nx.ego_graph(self.graph, key, radius=depth)
    
    def get_prerequisite_order(self) -> List[str]:
        """Get entities ordered by prerequisites (topological sort)."""
        prereq_graph = nx.DiGraph()
        
        for rel in self.relationships:
            if rel.relationship_type in [RelationshipType.PREREQUISITE_OF, 
                                         RelationshipType.DEPENDS_ON]:
                if rel.relationship_type == RelationshipType.PREREQUISITE_OF:
                    prereq_graph.add_edge(rel.source.lower(), rel.target.lower())
                else:
                    prereq_graph.add_edge(rel.target.lower(), rel.source.lower())
        
        for entity_key in self.entities:
            if entity_key not in prereq_graph:
                prereq_graph.add_node(entity_key)
        
        try:
            order = list(nx.topological_sort(prereq_graph))
            return [self.entities[key].name for key in order if key in self.entities]
        except nx.NetworkXUnfeasible:
            logger.warning("Cycle detected in prerequisite graph, using importance ordering")
            sorted_entities = sorted(
                self.entities.values(),
                key=lambda e: e.importance_score,
                reverse=True
            )
            return [e.name for e in sorted_entities]
    
    def get_communities(self) -> List[Set[str]]:
        """Detect communities (topic clusters) in the graph."""
        if self._communities is not None:
            return self._communities
        
        if len(self.graph.nodes()) < 2:
            self._communities = [set(self.entities.keys())]
            return self._communities
        
        try:
            undirected = self.graph.to_undirected()
            communities_gen = nx_community.greedy_modularity_communities(undirected)
            self._communities = [set(c) for c in communities_gen]
            logger.info(f"Detected {len(self._communities)} communities")
            return self._communities
        except Exception as e:
            logger.warning(f"Community detection failed: {e}")
            self._communities = [set(self.entities.keys())]
            return self._communities
    
    def get_entity_community(self, entity_name: str) -> int:
        """Get the community ID for an entity."""
        key = entity_name.lower()
        communities = self.get_communities()
        
        for idx, community in enumerate(communities):
            if key in community:
                return idx
        return 0
    
    def get_shortest_path(self, source: str, target: str) -> List[str]:
        """Get shortest path between two entities."""
        try:
            path = nx.shortest_path(self.graph, source.lower(), target.lower())
            return [self.entities[k].name for k in path if k in self.entities]
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            return []
    
    def get_central_entities(self, top_n: int = 10) -> List[Tuple[str, float]]:
        """Get the most central entities using PageRank."""
        if len(self.graph.nodes()) == 0:
            return []
        
        try:
            pagerank = nx.pagerank(self.graph)
            sorted_pr = sorted(pagerank.items(), key=lambda x: x[1], reverse=True)
            return [
                (self.entities[k].name, score) 
                for k, score in sorted_pr[:top_n] 
                if k in self.entities
            ]
        except Exception as e:
            logger.warning(f"PageRank failed: {e}")
            return []
    
    # =========================================================================
    # SIMILARITY QUERIES
    # =========================================================================
    
    def find_similar_entities(
        self, 
        query: str, 
        top_n: int = 5,
        threshold: float = 0.5
    ) -> List[Tuple[Entity, float]]:
        """Find entities similar to a query using embeddings."""
        if not _ensure_embedding_available() or self.embedding_model is None:
            return self._find_similar_string_match(query, top_n)

        try:
            query_embedding = self.embedding_model.encode(query, convert_to_numpy=True)

            similarities = []
            for entity in self.entities.values():
                if entity.embedding is None:
                    entity_text = f"{entity.name} {entity.description}"
                    entity.embedding = self.embedding_model.encode(
                        entity_text, convert_to_numpy=True
                    ).tolist()

                sim = _cosine_similarity_func(
                    [query_embedding],
                    [entity.embedding]
                )[0][0]

                if sim >= threshold:
                    similarities.append((entity, float(sim)))
            
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:top_n]
            
        except Exception as e:
            logger.error(f"Similarity search failed: {e}")
            return self._find_similar_string_match(query, top_n)
    
    def _find_similar_string_match(self, query: str, top_n: int) -> List[Tuple[Entity, float]]:
        """Fallback string matching for similarity."""
        query_lower = query.lower()
        results = []
        
        for entity in self.entities.values():
            name_match = query_lower in entity.name.lower()
            desc_match = query_lower in entity.description.lower()
            
            if name_match:
                results.append((entity, 0.9))
            elif desc_match:
                results.append((entity, 0.6))
        
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_n]
    
    # =========================================================================
    # EXPORT / IMPORT
    # =========================================================================
    
    def to_dict(self) -> Dict[str, Any]:
        """Export graph to dictionary."""
        return {
            'entities': [
                {
                    'name': e.name,
                    'entity_type': e.entity_type,
                    'description': e.description,
                    'importance_score': e.importance_score,
                    'metadata': e.metadata,
                    'created_at': e.created_at.isoformat() if hasattr(e, 'created_at') else None,
                    'updated_at': e.updated_at.isoformat() if hasattr(e, 'updated_at') else None
                }
                for e in self.entities.values()
            ],
            'relationships': [
                {
                    'source': r.source,
                    'target': r.target,
                    'type': r.relationship_type.value,
                    'description': r.description,
                    'confidence': r.confidence
                }
                for r in self.relationships
            ],
            'node_count': len(self.graph.nodes()),
            'edge_count': len(self.graph.edges())
        }
    
    def from_dict(self, data: Dict[str, Any]):
        """Import graph from dictionary."""
        self.clear()
        
        for e_data in data.get('entities', []):
            self.add_entity(Entity(
                name=e_data['name'],
                entity_type=e_data['entity_type'],
                description=e_data.get('description', ''),
                importance_score=e_data.get('importance_score', 0.5),
                metadata=e_data.get('metadata', {})
            ))
        
        for r_data in data.get('relationships', []):
            rel_type = RelationshipType(r_data['type'])
            self.add_relationship(Relationship(
                source=r_data['source'],
                target=r_data['target'],
                relationship_type=rel_type,
                description=r_data.get('description', ''),
                confidence=r_data.get('confidence', 0.8)
            ))
    
    def get_stats(self) -> Dict[str, Any]:
        """Get graph statistics."""
        communities = self.get_communities()
        
        return {
            'entity_count': len(self.entities),
            'relationship_count': len(self.relationships),
            'node_count': len(self.graph.nodes()),
            'edge_count': len(self.graph.edges()),
            'community_count': len(communities),
            'is_connected': nx.is_weakly_connected(self.graph) if len(self.graph) > 0 else True,
            'density': nx.density(self.graph) if len(self.graph) > 0 else 0,
            'entity_types': dict(Counter(e.entity_type for e in self.entities.values())),
            'relationship_types': dict(Counter(r.relationship_type.value for r in self.relationships))
        }


# =============================================================================
# MULTI-HOP REASONING & PATH-BASED INFERENCE (CUTTING EDGE)
# =============================================================================

class ReasoningStrategy(Enum):
    """Strategies for multi-hop reasoning."""
    BREADTH_FIRST = "breadth_first"  # Explore all neighbors at each depth
    DEPTH_FIRST = "depth_first"  # Follow paths to completion
    WEIGHTED = "weighted"  # Prioritize high-confidence relationships
    SEMANTIC = "semantic"  # Use embedding similarity to guide traversal


@dataclass
class ReasoningPath:
    """Represents a reasoning path through the knowledge graph."""
    entities: List[str]  # Ordered list of entities in path
    relationships: List[str]  # Relationship types between entities
    confidence: float  # Overall path confidence (product of edge confidences)
    depth: int  # Number of hops
    evidence: List[str]  # Supporting evidence/descriptions

    def __str__(self) -> str:
        """Human-readable path representation."""
        if not self.entities:
            return "Empty path"
        parts = []
        for i, entity in enumerate(self.entities):
            parts.append(entity)
            if i < len(self.relationships):
                parts.append(f" --[{self.relationships[i]}]--> ")
        return "".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'path': str(self),
            'entities': self.entities,
            'relationships': self.relationships,
            'confidence': self.confidence,
            'depth': self.depth,
            'evidence': self.evidence
        }


@dataclass
class InferredRelationship:
    """A relationship inferred through path analysis."""
    source: str
    target: str
    inferred_type: str  # The inferred relationship type
    confidence: float  # Confidence based on path analysis
    supporting_paths: List[ReasoningPath]  # Paths that support this inference
    reasoning: str  # Explanation of the inference

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source': self.source,
            'target': self.target,
            'inferred_type': self.inferred_type,
            'confidence': self.confidence,
            'supporting_paths': [p.to_dict() for p in self.supporting_paths],
            'reasoning': self.reasoning
        }


@dataclass
class MultiHopReasoningResult:
    """Result of multi-hop reasoning over the graph."""
    query_entity: str
    target_entity: Optional[str]
    reasoning_paths: List[ReasoningPath]
    inferred_relationships: List[InferredRelationship]
    visited_entities: Set[str]
    total_hops: int
    reasoning_strategy: str
    execution_time: float
    confidence_score: float  # Overall reasoning confidence
    answer_entities: List[str]  # Entities that answer the query

    def to_dict(self) -> Dict[str, Any]:
        return {
            'query_entity': self.query_entity,
            'target_entity': self.target_entity,
            'paths': [p.to_dict() for p in self.reasoning_paths],
            'inferred_relationships': [r.to_dict() for r in self.inferred_relationships],
            'visited_count': len(self.visited_entities),
            'total_hops': self.total_hops,
            'strategy': self.reasoning_strategy,
            'execution_time': self.execution_time,
            'confidence': self.confidence_score,
            'answer_entities': self.answer_entities
        }


@dataclass
class EntityRepresentation:
    """GNN-style entity representation with propagated features."""
    entity_name: str
    base_embedding: Optional[List[float]]  # Original embedding
    propagated_embedding: Optional[List[float]]  # After message passing
    aggregated_features: Dict[str, float]  # Features from neighbors
    neighborhood_context: List[str]  # Nearby entities
    importance_score: float  # Centrality-based importance
    layer_representations: List[List[float]]  # Representations at each GNN layer


# Inference rules for deriving new relationships from paths
INFERENCE_RULES = {
    # Transitivity rules: A->B->C implies A->C
    ('causes', 'causes'): ('causes', 0.7),  # A causes B causes C => A causes C
    ('causes', 'leads_to'): ('causes', 0.6),
    ('leads_to', 'leads_to'): ('leads_to', 0.7),
    ('leads_to', 'causes'): ('leads_to', 0.6),
    ('part_of', 'part_of'): ('part_of', 0.8),  # Transitive
    ('contains', 'contains'): ('contains', 0.8),
    ('prerequisite_of', 'prerequisite_of'): ('prerequisite_of', 0.9),  # Strong transitivity
    ('depends_on', 'depends_on'): ('depends_on', 0.85),
    ('similar_to', 'similar_to'): ('similar_to', 0.5),  # Weak transitivity
    ('defines', 'part_of'): ('related_to', 0.5),
    ('example_of', 'part_of'): ('example_of', 0.6),
    ('part_of', 'causes'): ('contributes_to', 0.5),
    ('depends_on', 'causes'): ('indirectly_causes', 0.5),
}


class GraphReasoner:
    """
    Multi-hop reasoning and path-based inference engine for knowledge graphs.

    Cutting-edge features:
    1. Multi-hop reasoning - Iteratively traverse to answer complex queries
    2. Path-based inference - Derive implicit relationships from paths
    3. GNN-style message passing - Propagate information through the graph
    4. Confidence propagation - Track uncertainty through reasoning chains

    Example usage:
        reasoner = GraphReasoner(knowledge_graph)

        # Multi-hop reasoning
        result = reasoner.multi_hop_reason("machine learning", max_hops=3)

        # Find paths between entities
        paths = reasoner.find_all_paths("AI", "automation", max_depth=4)

        # Infer new relationships
        inferred = reasoner.infer_relationships("deep learning")

        # GNN-style representation
        repr = reasoner.compute_entity_representation("neural networks", layers=2)
    """

    def __init__(
        self,
        graph: 'ContentKnowledgeGraph',
        default_strategy: ReasoningStrategy = ReasoningStrategy.WEIGHTED,
        max_paths: int = 50,
        min_confidence: float = 0.1
    ):
        """
        Initialize the graph reasoner.

        Args:
            graph: The knowledge graph to reason over
            default_strategy: Default reasoning strategy
            max_paths: Maximum paths to explore
            min_confidence: Minimum confidence threshold
        """
        self.graph = graph
        self.default_strategy = default_strategy
        self.max_paths = max_paths
        self.min_confidence = min_confidence
        self._path_cache: Dict[str, List[ReasoningPath]] = {}

        logger.info(f"GraphReasoner initialized (strategy: {default_strategy.value})")

    # =========================================================================
    # MULTI-HOP REASONING
    # =========================================================================

    def multi_hop_reason(
        self,
        query_entity: str,
        target_entity: Optional[str] = None,
        max_hops: int = 3,
        strategy: Optional[ReasoningStrategy] = None,
        relationship_filter: Optional[List[str]] = None,
        min_path_confidence: float = 0.1
    ) -> MultiHopReasoningResult:
        """
        Perform multi-hop reasoning starting from a query entity.

        Iteratively explores the graph following relationship chains to:
        - Find paths to a target entity (if specified)
        - Discover related entities through chain reasoning
        - Build evidence for complex queries

        Args:
            query_entity: Starting entity for reasoning
            target_entity: Optional target to find paths to
            max_hops: Maximum reasoning depth
            strategy: Reasoning strategy to use
            relationship_filter: Only follow these relationship types
            min_path_confidence: Minimum confidence for valid paths

        Returns:
            MultiHopReasoningResult with all reasoning paths and inferences
        """
        start_time = datetime.now()
        strategy = strategy or self.default_strategy

        query_key = query_entity.lower()
        if query_key not in self.graph.entities:
            # Try to find similar entity
            similar = self.graph.find_similar_entities(query_entity, top_n=1)
            if similar:
                query_key = similar[0][0].name.lower()
                logger.info(f"Mapped query '{query_entity}' to '{query_key}'")
            else:
                logger.warning(f"Entity '{query_entity}' not found in graph")
                return self._empty_reasoning_result(query_entity, target_entity, strategy, start_time)

        target_key = target_entity.lower() if target_entity else None

        # Track visited entities and paths
        visited: Set[str] = set()
        all_paths: List[ReasoningPath] = []
        answer_entities: List[str] = []

        # BFS/DFS based on strategy
        if strategy == ReasoningStrategy.BREADTH_FIRST:
            all_paths = self._bfs_reasoning(
                query_key, target_key, max_hops,
                relationship_filter, min_path_confidence, visited
            )
        elif strategy == ReasoningStrategy.DEPTH_FIRST:
            all_paths = self._dfs_reasoning(
                query_key, target_key, max_hops,
                relationship_filter, min_path_confidence, visited
            )
        elif strategy == ReasoningStrategy.WEIGHTED:
            all_paths = self._weighted_reasoning(
                query_key, target_key, max_hops,
                relationship_filter, min_path_confidence, visited
            )
        elif strategy == ReasoningStrategy.SEMANTIC:
            all_paths = self._semantic_reasoning(
                query_key, target_key, max_hops,
                relationship_filter, min_path_confidence, visited
            )

        # Sort paths by confidence
        all_paths.sort(key=lambda p: p.confidence, reverse=True)
        all_paths = all_paths[:self.max_paths]

        # Extract answer entities (endpoints of high-confidence paths)
        seen_answers = set()
        for path in all_paths:
            if path.confidence >= min_path_confidence and path.entities:
                endpoint = path.entities[-1]
                if endpoint not in seen_answers and endpoint != query_key:
                    answer_entities.append(self.graph.entities[endpoint].name if endpoint in self.graph.entities else endpoint)
                    seen_answers.add(endpoint)

        # Infer new relationships from paths
        inferred = self._infer_from_paths(all_paths, query_key)

        # Calculate overall confidence
        if all_paths:
            confidence = sum(p.confidence for p in all_paths[:5]) / min(5, len(all_paths))
        else:
            confidence = 0.0

        execution_time = (datetime.now() - start_time).total_seconds()

        result = MultiHopReasoningResult(
            query_entity=self.graph.entities[query_key].name if query_key in self.graph.entities else query_entity,
            target_entity=target_entity,
            reasoning_paths=all_paths,
            inferred_relationships=inferred,
            visited_entities=visited,
            total_hops=max(p.depth for p in all_paths) if all_paths else 0,
            reasoning_strategy=strategy.value,
            execution_time=execution_time,
            confidence_score=confidence,
            answer_entities=answer_entities[:10]
        )

        logger.info(
            f"Multi-hop reasoning: {len(all_paths)} paths, "
            f"{len(inferred)} inferences, {len(visited)} entities visited "
            f"in {execution_time:.3f}s"
        )

        return result

    def _bfs_reasoning(
        self,
        start: str,
        target: Optional[str],
        max_hops: int,
        rel_filter: Optional[List[str]],
        min_conf: float,
        visited: Set[str]
    ) -> List[ReasoningPath]:
        """Breadth-first multi-hop reasoning."""
        paths: List[ReasoningPath] = []

        # Queue: (current_entity, path_entities, path_rels, confidence, evidence)
        queue = [(start, [start], [], 1.0, [])]
        visited.add(start)

        while queue:
            current, path_entities, path_rels, conf, evidence = queue.pop(0)

            # Get all outgoing edges
            if current not in self.graph.graph:
                continue

            for neighbor in self.graph.graph.successors(current):
                edge_data = self.graph.graph.edges[current, neighbor]
                rel_type = edge_data.get('relationship', 'related_to')
                edge_conf = edge_data.get('confidence', 0.8)
                edge_desc = edge_data.get('description', '')

                # Apply relationship filter
                if rel_filter and rel_type not in rel_filter:
                    continue

                new_conf = conf * edge_conf
                if new_conf < min_conf:
                    continue

                new_path_entities = path_entities + [neighbor]
                new_path_rels = path_rels + [rel_type]
                new_evidence = evidence + [edge_desc] if edge_desc else evidence

                # Record path
                path = ReasoningPath(
                    entities=new_path_entities,
                    relationships=new_path_rels,
                    confidence=new_conf,
                    depth=len(new_path_rels),
                    evidence=new_evidence
                )
                paths.append(path)

                # Check if we reached target
                if target and neighbor == target:
                    continue  # Don't extend past target

                # Continue BFS if within depth limit
                if len(new_path_rels) < max_hops:
                    if neighbor not in visited:
                        visited.add(neighbor)
                        queue.append((neighbor, new_path_entities, new_path_rels, new_conf, new_evidence))

        return paths

    def _dfs_reasoning(
        self,
        start: str,
        target: Optional[str],
        max_hops: int,
        rel_filter: Optional[List[str]],
        min_conf: float,
        visited: Set[str]
    ) -> List[ReasoningPath]:
        """Depth-first multi-hop reasoning - follows paths to completion."""
        paths: List[ReasoningPath] = []

        def dfs(current: str, path_entities: List[str], path_rels: List[str],
                conf: float, evidence: List[str], local_visited: Set[str]):
            if current not in self.graph.graph:
                return

            for neighbor in self.graph.graph.successors(current):
                if neighbor in local_visited:
                    continue

                edge_data = self.graph.graph.edges[current, neighbor]
                rel_type = edge_data.get('relationship', 'related_to')
                edge_conf = edge_data.get('confidence', 0.8)
                edge_desc = edge_data.get('description', '')

                if rel_filter and rel_type not in rel_filter:
                    continue

                new_conf = conf * edge_conf
                if new_conf < min_conf:
                    continue

                new_path_entities = path_entities + [neighbor]
                new_path_rels = path_rels + [rel_type]
                new_evidence = evidence + [edge_desc] if edge_desc else evidence

                path = ReasoningPath(
                    entities=new_path_entities,
                    relationships=new_path_rels,
                    confidence=new_conf,
                    depth=len(new_path_rels),
                    evidence=new_evidence
                )
                paths.append(path)
                visited.add(neighbor)

                if target and neighbor == target:
                    continue

                if len(new_path_rels) < max_hops:
                    local_visited.add(neighbor)
                    dfs(neighbor, new_path_entities, new_path_rels, new_conf, new_evidence, local_visited)
                    local_visited.remove(neighbor)

        visited.add(start)
        dfs(start, [start], [], 1.0, [], {start})
        return paths

    def _weighted_reasoning(
        self,
        start: str,
        target: Optional[str],
        max_hops: int,
        rel_filter: Optional[List[str]],
        min_conf: float,
        visited: Set[str]
    ) -> List[ReasoningPath]:
        """Weighted reasoning - prioritizes high-confidence paths using priority queue."""
        import heapq

        paths: List[ReasoningPath] = []

        # Priority queue: (-confidence, path_entities, path_rels, evidence)
        # Negative confidence for max-heap behavior
        pq = [(-1.0, [start], [], [])]
        visited.add(start)

        while pq and len(paths) < self.max_paths:
            neg_conf, path_entities, path_rels, evidence = heapq.heappop(pq)
            conf = -neg_conf
            current = path_entities[-1]

            if current not in self.graph.graph:
                continue

            # Get neighbors sorted by edge confidence
            neighbors_with_conf = []
            for neighbor in self.graph.graph.successors(current):
                edge_data = self.graph.graph.edges[current, neighbor]
                edge_conf = edge_data.get('confidence', 0.8)
                neighbors_with_conf.append((neighbor, edge_conf, edge_data))

            neighbors_with_conf.sort(key=lambda x: x[1], reverse=True)

            for neighbor, edge_conf, edge_data in neighbors_with_conf:
                rel_type = edge_data.get('relationship', 'related_to')
                edge_desc = edge_data.get('description', '')

                if rel_filter and rel_type not in rel_filter:
                    continue

                new_conf = conf * edge_conf
                if new_conf < min_conf:
                    continue

                new_path_entities = path_entities + [neighbor]
                new_path_rels = path_rels + [rel_type]
                new_evidence = evidence + [edge_desc] if edge_desc else evidence

                path = ReasoningPath(
                    entities=new_path_entities,
                    relationships=new_path_rels,
                    confidence=new_conf,
                    depth=len(new_path_rels),
                    evidence=new_evidence
                )
                paths.append(path)
                visited.add(neighbor)

                if target and neighbor == target:
                    continue

                if len(new_path_rels) < max_hops and neighbor not in path_entities[:-1]:
                    heapq.heappush(pq, (-new_conf, new_path_entities, new_path_rels, new_evidence))

        return paths

    def _semantic_reasoning(
        self,
        start: str,
        target: Optional[str],
        max_hops: int,
        rel_filter: Optional[List[str]],
        min_conf: float,
        visited: Set[str]
    ) -> List[ReasoningPath]:
        """Semantic reasoning - uses embedding similarity to guide traversal."""
        if not _ensure_embedding_available() or self.graph.embedding_model is None:
            logger.warning("Embeddings not available, falling back to weighted reasoning")
            return self._weighted_reasoning(start, target, max_hops, rel_filter, min_conf, visited)

        import heapq

        paths: List[ReasoningPath] = []

        # Get target embedding if specified
        target_embedding = None
        if target and target in self.graph.entities:
            target_entity = self.graph.entities[target]
            if target_entity.embedding:
                target_embedding = np.array(target_entity.embedding)
            else:
                target_text = f"{target_entity.name} {target_entity.description}"
                target_embedding = self.graph.embedding_model.encode(target_text, convert_to_numpy=True)

        # Priority queue: (-score, path_entities, path_rels, conf, evidence)
        pq = [(0.0, [start], [], 1.0, [])]
        visited.add(start)

        while pq and len(paths) < self.max_paths:
            neg_score, path_entities, path_rels, conf, evidence = heapq.heappop(pq)
            current = path_entities[-1]

            if current not in self.graph.graph:
                continue

            for neighbor in self.graph.graph.successors(current):
                if neighbor in path_entities[:-1]:  # Avoid cycles
                    continue

                edge_data = self.graph.graph.edges[current, neighbor]
                rel_type = edge_data.get('relationship', 'related_to')
                edge_conf = edge_data.get('confidence', 0.8)
                edge_desc = edge_data.get('description', '')

                if rel_filter and rel_type not in rel_filter:
                    continue

                new_conf = conf * edge_conf
                if new_conf < min_conf:
                    continue

                # Calculate semantic score
                semantic_score = 0.5
                if target_embedding is not None and neighbor in self.graph.entities:
                    neighbor_entity = self.graph.entities[neighbor]
                    if neighbor_entity.embedding is None:
                        neighbor_text = f"{neighbor_entity.name} {neighbor_entity.description}"
                        neighbor_entity.embedding = self.graph.embedding_model.encode(
                            neighbor_text, convert_to_numpy=True
                        ).tolist()
                    neighbor_emb = np.array(neighbor_entity.embedding)
                    semantic_score = float(_cosine_similarity_func([target_embedding], [neighbor_emb])[0][0])

                # Combined score: confidence * semantic similarity
                combined_score = new_conf * (0.5 + 0.5 * semantic_score)

                new_path_entities = path_entities + [neighbor]
                new_path_rels = path_rels + [rel_type]
                new_evidence = evidence + [edge_desc] if edge_desc else evidence

                path = ReasoningPath(
                    entities=new_path_entities,
                    relationships=new_path_rels,
                    confidence=new_conf,
                    depth=len(new_path_rels),
                    evidence=new_evidence
                )
                paths.append(path)
                visited.add(neighbor)

                if target and neighbor == target:
                    continue

                if len(new_path_rels) < max_hops:
                    heapq.heappush(pq, (-combined_score, new_path_entities, new_path_rels, new_conf, new_evidence))

        return paths

    def _empty_reasoning_result(
        self,
        query: str,
        target: Optional[str],
        strategy: ReasoningStrategy,
        start_time: datetime
    ) -> MultiHopReasoningResult:
        """Return empty result when entity not found."""
        return MultiHopReasoningResult(
            query_entity=query,
            target_entity=target,
            reasoning_paths=[],
            inferred_relationships=[],
            visited_entities=set(),
            total_hops=0,
            reasoning_strategy=strategy.value,
            execution_time=(datetime.now() - start_time).total_seconds(),
            confidence_score=0.0,
            answer_entities=[]
        )

    # =========================================================================
    # PATH-BASED INFERENCE
    # =========================================================================

    def find_all_paths(
        self,
        source: str,
        target: str,
        max_depth: int = 4,
        min_confidence: float = 0.1
    ) -> List[ReasoningPath]:
        """
        Find all paths between two entities.

        Uses modified DFS to find all simple paths (no cycles) between
        source and target within the depth limit.

        Args:
            source: Source entity name
            target: Target entity name
            max_depth: Maximum path length
            min_confidence: Minimum path confidence

        Returns:
            List of ReasoningPaths connecting source to target
        """
        source_key = source.lower()
        target_key = target.lower()

        if source_key not in self.graph.entities or target_key not in self.graph.entities:
            return []

        paths: List[ReasoningPath] = []

        def find_paths_dfs(current: str, path_entities: List[str], path_rels: List[str],
                          conf: float, evidence: List[str], local_visited: Set[str]):
            if current == target_key:
                paths.append(ReasoningPath(
                    entities=path_entities,
                    relationships=path_rels,
                    confidence=conf,
                    depth=len(path_rels),
                    evidence=evidence
                ))
                return

            if len(path_rels) >= max_depth or current not in self.graph.graph:
                return

            for neighbor in self.graph.graph.successors(current):
                if neighbor in local_visited:
                    continue

                edge_data = self.graph.graph.edges[current, neighbor]
                edge_conf = edge_data.get('confidence', 0.8)
                new_conf = conf * edge_conf

                if new_conf < min_confidence:
                    continue

                rel_type = edge_data.get('relationship', 'related_to')
                edge_desc = edge_data.get('description', '')

                local_visited.add(neighbor)
                find_paths_dfs(
                    neighbor,
                    path_entities + [neighbor],
                    path_rels + [rel_type],
                    new_conf,
                    evidence + [edge_desc] if edge_desc else evidence,
                    local_visited
                )
                local_visited.remove(neighbor)

        find_paths_dfs(source_key, [source_key], [], 1.0, [], {source_key})

        # Sort by confidence
        paths.sort(key=lambda p: p.confidence, reverse=True)

        logger.info(f"Found {len(paths)} paths from '{source}' to '{target}'")
        return paths

    def infer_relationships(
        self,
        entity: str,
        max_hops: int = 2,
        min_confidence: float = 0.3
    ) -> List[InferredRelationship]:
        """
        Infer new relationships for an entity based on path analysis.

        Applies inference rules to derive implicit relationships from
        existing paths in the graph.

        Args:
            entity: Entity to infer relationships for
            max_hops: Maximum path length for inference
            min_confidence: Minimum confidence for inferred relationships

        Returns:
            List of inferred relationships with supporting evidence
        """
        entity_key = entity.lower()
        if entity_key not in self.graph.entities:
            return []

        # Get all paths from this entity
        result = self.multi_hop_reason(
            entity, max_hops=max_hops,
            strategy=ReasoningStrategy.WEIGHTED,
            min_path_confidence=min_confidence
        )

        return result.inferred_relationships

    def _infer_from_paths(
        self,
        paths: List[ReasoningPath],
        source_key: str
    ) -> List[InferredRelationship]:
        """Apply inference rules to paths to derive new relationships."""
        inferred: List[InferredRelationship] = []
        inferred_pairs: Dict[Tuple[str, str], List[Tuple[str, float, ReasoningPath]]] = defaultdict(list)

        for path in paths:
            if len(path.relationships) < 2:
                continue

            # Apply two-hop inference rules
            for i in range(len(path.relationships) - 1):
                rel1 = path.relationships[i]
                rel2 = path.relationships[i + 1]

                rule_key = (rel1, rel2)
                if rule_key in INFERENCE_RULES:
                    inferred_type, conf_factor = INFERENCE_RULES[rule_key]
                    source = path.entities[i]
                    target = path.entities[i + 2]

                    # Calculate confidence
                    path_conf_segment = path.confidence  # Use full path confidence
                    inferred_conf = path_conf_segment * conf_factor

                    # Skip if this would duplicate an existing relationship
                    existing_edge = self.graph.graph.edges.get((source, target))
                    if existing_edge and existing_edge.get('relationship') == inferred_type:
                        continue

                    inferred_pairs[(source, target)].append((inferred_type, inferred_conf, path))

        # Aggregate inferences for each pair
        for (source, target), inferences in inferred_pairs.items():
            # Group by inferred type
            by_type: Dict[str, List[Tuple[float, ReasoningPath]]] = defaultdict(list)
            for inf_type, conf, path in inferences:
                by_type[inf_type].append((conf, path))

            for inf_type, conf_paths in by_type.items():
                # Average confidence, boost for multiple supporting paths
                confs = [c for c, _ in conf_paths]
                supporting_paths = [p for _, p in conf_paths]

                avg_conf = sum(confs) / len(confs)
                boost = min(1.0, 1.0 + 0.1 * (len(confs) - 1))  # Up to 1.5x for many paths
                final_conf = min(0.95, avg_conf * boost)

                if final_conf >= self.min_confidence:
                    source_name = self.graph.entities[source].name if source in self.graph.entities else source
                    target_name = self.graph.entities[target].name if target in self.graph.entities else target

                    inferred.append(InferredRelationship(
                        source=source_name,
                        target=target_name,
                        inferred_type=inf_type,
                        confidence=final_conf,
                        supporting_paths=supporting_paths[:3],  # Keep top 3 paths
                        reasoning=f"Inferred from {len(supporting_paths)} path(s) using transitivity rules"
                    ))

        # Sort by confidence
        inferred.sort(key=lambda x: x.confidence, reverse=True)

        return inferred

    # =========================================================================
    # GNN-STYLE REASONING
    # =========================================================================

    def compute_entity_representation(
        self,
        entity: str,
        layers: int = 2,
        aggregation: str = 'mean'
    ) -> EntityRepresentation:
        """
        Compute GNN-style entity representation through message passing.

        Performs iterative neighborhood aggregation similar to graph neural
        networks to create rich entity representations.

        Args:
            entity: Entity name
            layers: Number of message passing layers
            aggregation: Aggregation method ('mean', 'max', 'attention')

        Returns:
            EntityRepresentation with propagated features
        """
        entity_key = entity.lower()
        if entity_key not in self.graph.entities:
            return EntityRepresentation(
                entity_name=entity,
                base_embedding=None,
                propagated_embedding=None,
                aggregated_features={},
                neighborhood_context=[],
                importance_score=0.0,
                layer_representations=[]
            )

        entity_obj = self.graph.entities[entity_key]

        # Get base embedding
        base_embedding = None
        if _ensure_embedding_available() and self.graph.embedding_model is not None:
            if entity_obj.embedding:
                base_embedding = entity_obj.embedding
            else:
                entity_text = f"{entity_obj.name} {entity_obj.description}"
                base_embedding = self.graph.embedding_model.encode(
                    entity_text, convert_to_numpy=True
                ).tolist()
                entity_obj.embedding = base_embedding

        # Message passing layers
        layer_representations: List[List[float]] = []
        current_repr = np.array(base_embedding) if base_embedding else None

        if current_repr is not None:
            layer_representations.append(current_repr.tolist())

            for layer in range(layers):
                neighbor_reprs = []
                neighbor_weights = []

                # Get neighbors at this layer
                if entity_key in self.graph.graph:
                    for neighbor in self.graph.graph.successors(entity_key):
                        if neighbor in self.graph.entities:
                            neighbor_entity = self.graph.entities[neighbor]

                            # Get neighbor embedding
                            if neighbor_entity.embedding:
                                neighbor_emb = np.array(neighbor_entity.embedding)
                            elif self.graph.embedding_model is not None:
                                neighbor_text = f"{neighbor_entity.name} {neighbor_entity.description}"
                                neighbor_emb = self.graph.embedding_model.encode(
                                    neighbor_text, convert_to_numpy=True
                                )
                                neighbor_entity.embedding = neighbor_emb.tolist()
                            else:
                                continue

                            # Get edge weight
                            edge_data = self.graph.graph.edges[entity_key, neighbor]
                            weight = edge_data.get('confidence', 0.8)

                            neighbor_reprs.append(neighbor_emb)
                            neighbor_weights.append(weight)

                    # Also include predecessors (incoming edges)
                    for neighbor in self.graph.graph.predecessors(entity_key):
                        if neighbor in self.graph.entities:
                            neighbor_entity = self.graph.entities[neighbor]

                            if neighbor_entity.embedding:
                                neighbor_emb = np.array(neighbor_entity.embedding)
                            elif self.graph.embedding_model is not None:
                                neighbor_text = f"{neighbor_entity.name} {neighbor_entity.description}"
                                neighbor_emb = self.graph.embedding_model.encode(
                                    neighbor_text, convert_to_numpy=True
                                )
                                neighbor_entity.embedding = neighbor_emb.tolist()
                            else:
                                continue

                            edge_data = self.graph.graph.edges[neighbor, entity_key]
                            weight = edge_data.get('confidence', 0.8)

                            neighbor_reprs.append(neighbor_emb)
                            neighbor_weights.append(weight)

                # Aggregate neighbor representations
                if neighbor_reprs:
                    neighbor_reprs = np.array(neighbor_reprs)
                    neighbor_weights = np.array(neighbor_weights)

                    if aggregation == 'mean':
                        # Weighted mean
                        weights_normalized = neighbor_weights / neighbor_weights.sum()
                        aggregated = np.average(neighbor_reprs, axis=0, weights=weights_normalized)
                    elif aggregation == 'max':
                        aggregated = np.max(neighbor_reprs, axis=0)
                    elif aggregation == 'attention':
                        # Simple attention based on similarity to current representation
                        similarities = _cosine_similarity_func([current_repr], neighbor_reprs)[0]
                        attention_weights = np.exp(similarities) / np.exp(similarities).sum()
                        aggregated = np.average(neighbor_reprs, axis=0, weights=attention_weights)
                    else:
                        aggregated = np.mean(neighbor_reprs, axis=0)

                    # Combine: new_repr = 0.5 * self + 0.5 * neighbors
                    current_repr = 0.5 * current_repr + 0.5 * aggregated

                    # Normalize
                    norm = np.linalg.norm(current_repr)
                    if norm > 0:
                        current_repr = current_repr / norm

                    layer_representations.append(current_repr.tolist())

        # Compute aggregated features
        aggregated_features = self._compute_aggregated_features(entity_key)

        # Get neighborhood context
        neighborhood_context = self._get_neighborhood_context(entity_key, depth=2)

        # Get importance score (PageRank)
        importance = 0.0
        try:
            pagerank = nx.pagerank(self.graph.graph)
            importance = pagerank.get(entity_key, 0.0)
        except Exception:
            importance = entity_obj.importance_score

        propagated_embedding = current_repr.tolist() if current_repr is not None else None

        return EntityRepresentation(
            entity_name=entity_obj.name,
            base_embedding=base_embedding,
            propagated_embedding=propagated_embedding,
            aggregated_features=aggregated_features,
            neighborhood_context=neighborhood_context,
            importance_score=importance,
            layer_representations=layer_representations
        )

    def _compute_aggregated_features(self, entity_key: str) -> Dict[str, float]:
        """Compute aggregated features from neighborhood."""
        features = {
            'in_degree': 0,
            'out_degree': 0,
            'avg_neighbor_importance': 0.0,
            'relationship_diversity': 0.0,
            'clustering_coefficient': 0.0
        }

        if entity_key not in self.graph.graph:
            return features

        # Degrees
        features['in_degree'] = self.graph.graph.in_degree(entity_key)
        features['out_degree'] = self.graph.graph.out_degree(entity_key)

        # Average neighbor importance
        neighbors = list(self.graph.graph.successors(entity_key)) + list(self.graph.graph.predecessors(entity_key))
        if neighbors:
            importances = [
                self.graph.entities[n].importance_score
                for n in neighbors
                if n in self.graph.entities
            ]
            if importances:
                features['avg_neighbor_importance'] = sum(importances) / len(importances)

        # Relationship diversity (unique relationship types)
        rel_types = set()
        for neighbor in self.graph.graph.successors(entity_key):
            edge_data = self.graph.graph.edges[entity_key, neighbor]
            rel_types.add(edge_data.get('relationship', 'related_to'))
        for neighbor in self.graph.graph.predecessors(entity_key):
            edge_data = self.graph.graph.edges[neighbor, entity_key]
            rel_types.add(edge_data.get('relationship', 'related_to'))
        features['relationship_diversity'] = len(rel_types) / max(1, len(RelationshipType))

        # Clustering coefficient
        try:
            undirected = self.graph.graph.to_undirected()
            features['clustering_coefficient'] = nx.clustering(undirected, entity_key)
        except Exception:
            pass

        return features

    def _get_neighborhood_context(self, entity_key: str, depth: int = 2) -> List[str]:
        """Get entity names in the neighborhood."""
        if entity_key not in self.graph.graph:
            return []

        visited = {entity_key}
        current_level = {entity_key}

        for _ in range(depth):
            next_level = set()
            for node in current_level:
                if node in self.graph.graph:
                    for neighbor in self.graph.graph.successors(node):
                        if neighbor not in visited:
                            next_level.add(neighbor)
                            visited.add(neighbor)
                    for neighbor in self.graph.graph.predecessors(node):
                        if neighbor not in visited:
                            next_level.add(neighbor)
                            visited.add(neighbor)
            current_level = next_level

        # Return entity names (excluding the query entity)
        context = [
            self.graph.entities[k].name
            for k in visited
            if k != entity_key and k in self.graph.entities
        ]
        return context[:20]  # Limit to 20

    # =========================================================================
    # QUERY ANSWERING
    # =========================================================================

    def answer_multi_hop_query(
        self,
        query: str,
        query_entities: Optional[List[str]] = None,
        max_hops: int = 3
    ) -> Dict[str, Any]:
        """
        Answer a complex query using multi-hop reasoning.

        This method combines entity detection, multi-hop reasoning, and
        path-based inference to answer complex questions that require
        traversing the knowledge graph.

        Args:
            query: Natural language query
            query_entities: Pre-extracted entities from the query
            max_hops: Maximum reasoning depth

        Returns:
            Answer with supporting evidence and reasoning chains
        """
        start_time = datetime.now()

        # Find relevant entities in query
        if query_entities is None:
            similar = self.graph.find_similar_entities(query, top_n=3, threshold=0.3)
            query_entities = [e.name for e, _ in similar]

        if not query_entities:
            return {
                'success': False,
                'answer': 'Could not identify relevant entities in the query',
                'entities': [],
                'reasoning_paths': [],
                'confidence': 0.0
            }

        # Perform multi-hop reasoning from each entity
        all_paths: List[ReasoningPath] = []
        all_inferred: List[InferredRelationship] = []
        all_answers: List[str] = []

        for entity in query_entities:
            result = self.multi_hop_reason(
                entity, max_hops=max_hops,
                strategy=ReasoningStrategy.SEMANTIC
            )
            all_paths.extend(result.reasoning_paths)
            all_inferred.extend(result.inferred_relationships)
            all_answers.extend(result.answer_entities)

        # Deduplicate and rank paths
        seen_paths = set()
        unique_paths = []
        for path in sorted(all_paths, key=lambda p: p.confidence, reverse=True):
            path_key = tuple(path.entities)
            if path_key not in seen_paths:
                unique_paths.append(path)
                seen_paths.add(path_key)

        # Deduplicate answer entities
        answer_counts = Counter(all_answers)
        ranked_answers = [ans for ans, _ in answer_counts.most_common(10)]

        # Calculate overall confidence
        if unique_paths:
            confidence = sum(p.confidence for p in unique_paths[:5]) / min(5, len(unique_paths))
        else:
            confidence = 0.0

        execution_time = (datetime.now() - start_time).total_seconds()

        return {
            'success': True,
            'query': query,
            'query_entities': query_entities,
            'answer_entities': ranked_answers,
            'reasoning_paths': [p.to_dict() for p in unique_paths[:10]],
            'inferred_relationships': [r.to_dict() for r in all_inferred[:5]],
            'confidence': confidence,
            'execution_time': execution_time
        }

    def clear_cache(self):
        """Clear the path cache."""
        self._path_cache.clear()


# Global reasoner instance
_global_reasoner: Optional[GraphReasoner] = None


def get_graph_reasoner(graph: Optional['ContentKnowledgeGraph'] = None) -> GraphReasoner:
    """Get or create global graph reasoner."""
    global _global_reasoner
    if _global_reasoner is None:
        if graph is None:
            graph = get_knowledge_graph()
        _global_reasoner = GraphReasoner(graph)
    return _global_reasoner


# =============================================================================
# ENTITY EXTRACTOR (OLLAMA AI-POWERED)
# =============================================================================

class OllamaEntityExtractor:
    """
    AI-powered entity and relationship extractor using Ollama.
    Uses local LLMs for deep semantic extraction.
    """
    
    EXTRACTION_PROMPT = """Analyze the following content and extract entities and relationships.

CONTENT:
{content}

INSTRUCTIONS:
1. Extract key concepts, terms, processes, definitions, people, organizations, and important entities
2. Identify relationships between entities using these types:
   - causes: A causes B
   - leads_to: A leads to B
   - part_of: A is part of B
   - contrasts_with: A contrasts with B
   - prerequisite_of: A must be understood before B
   - example_of: A is an example of B
   - defines: A defines/describes B
   - contains: A contains B
   - similar_to: A is similar to B
   - depends_on: A depends on B

3. Return your analysis in this EXACT JSON format:
{{
  "entities": [
    {{"name": "Entity Name", "type": "concept|term|process|person|organization|event|location|other", "description": "Brief description", "importance": 0.0-1.0}}
  ],
  "relationships": [
    {{"source": "Entity A", "target": "Entity B", "type": "causes|leads_to|part_of|contrasts_with|prerequisite_of|example_of|defines|contains|similar_to|depends_on", "description": "Why this relationship exists"}}
  ]
}}

Important:
- Extract 5-15 key entities depending on content length
- Include 3-10 important relationships
- Focus on document analysis and knowledge extraction value
- Ensure entity names are consistent across relationships

JSON Response:"""

    def __init__(
        self,
        model: str = None,
        ollama_url: str = "http://localhost:11434"
    ):
        self.model = model or os.environ.get('OLLAMA_MODEL', 'gemma3:4b')
        self.ollama_url = ollama_url
        self._session: Optional[aiohttp.ClientSession] = None
        
        logger.info(f"OllamaEntityExtractor initialized with model: {self.model}")
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session
    
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def extract(
        self,
        content: str,
        max_entities: int = 15,
        min_confidence: float = 0.5
    ) -> ExtractionResult:
        """Extract entities and relationships from content."""
        start_time = datetime.now()
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]
        
        # Truncate content if too long
        max_content_length = 4000
        if len(content) > max_content_length:
            content = content[:max_content_length] + "..."
        
        prompt = self.EXTRACTION_PROMPT.format(content=content)
        
        try:
            session = await self._get_session()
            
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,
                    "num_predict": 2000
                }
            }
            
            async with session.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                if response.status != 200:
                    logger.error(f"Ollama API error: {response.status}")
                    return self._empty_result(content_hash, start_time)
                
                result = await response.json()
                response_text = result.get('response', '')
                
                entities, relationships = self._parse_extraction_response(response_text)
                
                entities = [e for e in entities if e.importance_score >= min_confidence]
                relationships = [r for r in relationships if r.confidence >= min_confidence]
                
                entities = sorted(entities, key=lambda e: e.importance_score, reverse=True)[:max_entities]
                
                extraction_time = (datetime.now() - start_time).total_seconds()
                
                logger.info(f"Extracted {len(entities)} entities and {len(relationships)} relationships in {extraction_time:.2f}s")
                
                return ExtractionResult(
                    entities=entities,
                    relationships=relationships,
                    extraction_time=extraction_time,
                    model_used=self.model,
                    content_hash=content_hash
                )
                
        except asyncio.TimeoutError:
            logger.error("Entity extraction timed out")
            return self._empty_result(content_hash, start_time)
        except Exception as e:
            logger.error(f"Entity extraction failed: {e}")
            return self._empty_result(content_hash, start_time)
    
    def _parse_extraction_response(self, response: str) -> Tuple[List[Entity], List[Relationship]]:
        """Parse the JSON response from Ollama."""
        entities = []
        relationships = []
        
        try:
            json_match = re.search(r'\{[\s\S]*\}', response)
            if not json_match:
                logger.warning("No JSON found in extraction response")
                return entities, relationships
            
            data = json.loads(json_match.group())
            
            for e_data in data.get('entities', []):
                try:
                    entities.append(Entity(
                        name=e_data.get('name', ''),
                        entity_type=e_data.get('type', 'concept'),
                        description=e_data.get('description', ''),
                        importance_score=float(e_data.get('importance', 0.5))
                    ))
                except (KeyError, ValueError) as e:
                    logger.debug(f"Skipping malformed entity: {e}")
            
            rel_type_map = {
                'causes': RelationshipType.CAUSES,
                'leads_to': RelationshipType.LEADS_TO,
                'part_of': RelationshipType.PART_OF,
                'contrasts_with': RelationshipType.CONTRASTS_WITH,
                'prerequisite_of': RelationshipType.PREREQUISITE_OF,
                'example_of': RelationshipType.EXAMPLE_OF,
                'defines': RelationshipType.DEFINES,
                'contains': RelationshipType.CONTAINS,
                'similar_to': RelationshipType.SIMILAR_TO,
                'depends_on': RelationshipType.DEPENDS_ON
            }
            
            for r_data in data.get('relationships', []):
                try:
                    rel_type_str = r_data.get('type', 'related_to')
                    rel_type = rel_type_map.get(rel_type_str, RelationshipType.SIMILAR_TO)
                    
                    relationships.append(Relationship(
                        source=r_data.get('source', ''),
                        target=r_data.get('target', ''),
                        relationship_type=rel_type,
                        description=r_data.get('description', ''),
                        confidence=float(r_data.get('confidence', 0.8))
                    ))
                except (KeyError, ValueError) as e:
                    logger.debug(f"Skipping malformed relationship: {e}")
            
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse extraction JSON: {e}")
        
        return entities, relationships
    
    def _empty_result(self, content_hash: str, start_time: datetime) -> ExtractionResult:
        return ExtractionResult(
            entities=[],
            relationships=[],
            extraction_time=(datetime.now() - start_time).total_seconds(),
            model_used=self.model,
            content_hash=content_hash
        )


# =============================================================================
# SPACY ENTITY EXTRACTOR (FAST, RULE-BASED)
# =============================================================================

class SpaCyEntityExtractor:
    """
    Fast entity extraction using spaCy NLP pipeline.
    Much faster than LLM-based extraction (~100x speedup).
    """

    RELATIONSHIP_PATTERNS = {
        'causes': ['cause', 'causes', 'caused by', 'results in', 'leads to'],
        'part_of': ['part of', 'component of', 'element of', 'belongs to'],
        'contains': ['contains', 'includes', 'consists of', 'comprises'],
        'defines': ['is defined as', 'means', 'refers to', 'is called'],
        'example_of': ['example of', 'instance of', 'such as', 'like'],
        'prerequisite_of': ['required for', 'needed for', 'before', 'prerequisite'],
        'contrasts_with': ['unlike', 'whereas', 'in contrast to', 'differs from'],
        'similar_to': ['similar to', 'like', 'resembles', 'same as'],
        'depends_on': ['depends on', 'relies on', 'requires'],
        'leads_to': ['leads to', 'results in', 'produces', 'generates']
    }

    ENTITY_TYPE_MAP = {
        'PERSON': 'person',
        'ORG': 'organization',
        'GPE': 'location',
        'DATE': 'event',
        'EVENT': 'event',
        'WORK_OF_ART': 'term',
        'LAW': 'definition',
        'PRODUCT': 'concept',
        'NORP': 'concept',
        'FAC': 'location',
        'LOC': 'location',
        'LANGUAGE': 'term',
    }

    def __init__(self, model_name: str = "en_core_web_sm"):
        if not SPACY_AVAILABLE:
            raise ImportError("spaCy is required. Install with: pip install spacy && python -m spacy download en_core_web_sm")

        self._nlp = None
        self._model_name = model_name
        self._matcher = None

        logger.info(f"SpaCyEntityExtractor initialized (model: {model_name})")

    @property
    def nlp(self):
        """Lazy-load spaCy model."""
        if self._nlp is None:
            try:
                self._nlp = spacy.load(self._model_name)
                self._setup_matcher()
                logger.info(f"spaCy model loaded: {self._model_name}")
            except OSError:
                logger.info(f"Downloading spaCy model: {self._model_name}")
                import subprocess
                subprocess.run(["python", "-m", "spacy", "download", self._model_name], check=True)
                self._nlp = spacy.load(self._model_name)
                self._setup_matcher()
        return self._nlp

    def _setup_matcher(self):
        """Setup phrase matcher for relationship detection."""
        self._matcher = Matcher(self._nlp.vocab)

        for rel_type, phrases in self.RELATIONSHIP_PATTERNS.items():
            patterns = []
            for phrase in phrases:
                pattern = [{"LOWER": word.lower()} for word in phrase.split()]
                patterns.append(pattern)
            self._matcher.add(rel_type, patterns)

    def extract(
        self,
        content: str,
        max_entities: int = 20,
        min_confidence: float = 0.3
    ) -> ExtractionResult:
        """Extract entities and relationships using spaCy."""
        start_time = datetime.now()
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        entities = []
        relationships = []
        seen_entities = set()

        try:
            doc = self.nlp(content[:50000])

            # Extract named entities
            for ent in doc.ents:
                if ent.text.lower() not in seen_entities and len(ent.text) > 2:
                    entity_type = self.ENTITY_TYPE_MAP.get(ent.label_, 'concept')
                    entities.append(Entity(
                        name=ent.text,
                        entity_type=entity_type,
                        description=f"A {entity_type} mentioned in the content",
                        source_text=ent.sent.text if ent.sent else "",
                        importance_score=0.7
                    ))
                    seen_entities.add(ent.text.lower())

            # Extract important noun chunks as concepts
            noun_chunks = list(doc.noun_chunks)
            chunk_freq = Counter(chunk.root.lemma_.lower() for chunk in noun_chunks)

            for chunk in noun_chunks:
                chunk_text = chunk.text.strip()
                if (chunk_text.lower() not in seen_entities and
                    len(chunk_text) > 3 and
                    chunk_freq[chunk.root.lemma_.lower()] >= 2):

                    freq = chunk_freq[chunk.root.lemma_.lower()]
                    importance = min(0.9, 0.3 + (freq * 0.1))

                    entities.append(Entity(
                        name=chunk_text,
                        entity_type='concept',
                        description=f"Key concept appearing {freq} times",
                        source_text=chunk.sent.text if hasattr(chunk, 'sent') else "",
                        importance_score=importance
                    ))
                    seen_entities.add(chunk_text.lower())

            # Find relationships using matcher
            matches = self._matcher(doc)
            for match_id, start, end in matches:
                rel_type = self._nlp.vocab.strings[match_id]
                span = doc[start:end]

                sent = span.sent if hasattr(span, 'sent') else doc[max(0, start-20):min(len(doc), end+20)]
                sent_ents = [e for e in entities if e.name.lower() in sent.text.lower()]

                if len(sent_ents) >= 2:
                    source = sent_ents[0]
                    target = sent_ents[1]

                    rel_type_enum = self._map_relationship_type(rel_type)

                    relationships.append(Relationship(
                        source=source.name,
                        target=target.name,
                        relationship_type=rel_type_enum,
                        description="Found via pattern matching",
                        confidence=0.6,
                        source_text=sent.text
                    ))

            # Sort and limit
            entities = sorted(entities, key=lambda e: e.importance_score, reverse=True)[:max_entities]
            entities = [e for e in entities if e.importance_score >= min_confidence]
            relationships = [r for r in relationships if r.confidence >= min_confidence]

            # Remove duplicate relationships
            seen_rels = set()
            unique_rels = []
            for r in relationships:
                key = (r.source.lower(), r.target.lower(), r.relationship_type)
                if key not in seen_rels:
                    unique_rels.append(r)
                    seen_rels.add(key)
            relationships = unique_rels

            extraction_time = (datetime.now() - start_time).total_seconds()

            logger.info(f"spaCy extracted {len(entities)} entities and {len(relationships)} relationships in {extraction_time:.3f}s")

            return ExtractionResult(
                entities=entities,
                relationships=relationships,
                extraction_time=extraction_time,
                model_used=f"spacy:{self._model_name}",
                content_hash=content_hash
            )

        except Exception as e:
            logger.error(f"spaCy extraction failed: {e}")
            return ExtractionResult(
                entities=[],
                relationships=[],
                extraction_time=(datetime.now() - start_time).total_seconds(),
                model_used=f"spacy:{self._model_name}",
                content_hash=content_hash
            )

    def _map_relationship_type(self, rel_str: str) -> RelationshipType:
        """Map string to RelationshipType enum."""
        mapping = {
            'causes': RelationshipType.CAUSES,
            'leads_to': RelationshipType.LEADS_TO,
            'part_of': RelationshipType.PART_OF,
            'contains': RelationshipType.CONTAINS,
            'defines': RelationshipType.DEFINES,
            'example_of': RelationshipType.EXAMPLE_OF,
            'prerequisite_of': RelationshipType.PREREQUISITE_OF,
            'contrasts_with': RelationshipType.CONTRASTS_WITH,
            'similar_to': RelationshipType.SIMILAR_TO,
            'depends_on': RelationshipType.DEPENDS_ON
        }
        return mapping.get(rel_str.lower(), RelationshipType.SIMILAR_TO)


# =============================================================================
# HYBRID ENTITY EXTRACTOR
# =============================================================================

class HybridEntityExtractor:
    """
    Hybrid entity extractor combining spaCy (fast) and Ollama (deep).

    Strategy:
    1. Use spaCy for fast initial extraction
    2. Optionally enhance with Ollama for complex content
    3. Merge results intelligently
    """

    def __init__(
        self,
        use_spacy: bool = True,
        use_ollama: bool = False,
        ollama_model: str = None,
        ollama_url: str = "http://localhost:11434"
    ):
        self.use_spacy = use_spacy and SPACY_AVAILABLE
        self.use_ollama = use_ollama

        self._spacy_extractor = None
        self._ollama_extractor = None

        if self.use_spacy:
            try:
                self._spacy_extractor = SpaCyEntityExtractor()
            except Exception as e:
                logger.warning(f"spaCy initialization failed: {e}")
                self.use_spacy = False

        if self.use_ollama:
            self._ollama_extractor = OllamaEntityExtractor(
                model=ollama_model,
                ollama_url=ollama_url
            )

        logger.info(f"HybridEntityExtractor initialized (spaCy: {self.use_spacy}, Ollama: {self.use_ollama})")

    async def extract(
        self,
        content: str,
        max_entities: int = 20,
        min_confidence: float = 0.3,
        enhance_with_ollama: bool = False
    ) -> ExtractionResult:
        """Extract entities using hybrid approach."""
        start_time = datetime.now()
        content_hash = hashlib.sha256(content.encode()).hexdigest()[:16]

        all_entities = []
        all_relationships = []
        models_used = []

        # Fast spaCy extraction
        if self.use_spacy and self._spacy_extractor:
            try:
                spacy_result = self._spacy_extractor.extract(content, max_entities, min_confidence)
                all_entities.extend(spacy_result.entities)
                all_relationships.extend(spacy_result.relationships)
                models_used.append(spacy_result.model_used)
            except Exception as e:
                logger.error(f"spaCy extraction error: {e}")

        # Deep Ollama extraction (optional)
        if (self.use_ollama and self._ollama_extractor and
            (enhance_with_ollama or not all_entities)):
            try:
                ollama_result = await self._ollama_extractor.extract(content, max_entities, min_confidence)

                # Merge with existing (avoid duplicates)
                existing_names = {e.name.lower() for e in all_entities}
                for entity in ollama_result.entities:
                    if entity.name.lower() not in existing_names:
                        all_entities.append(entity)
                        existing_names.add(entity.name.lower())

                # Merge relationships
                existing_rels = {
                    (r.source.lower(), r.target.lower(), r.relationship_type)
                    for r in all_relationships
                }
                for rel in ollama_result.relationships:
                    key = (rel.source.lower(), rel.target.lower(), rel.relationship_type)
                    if key not in existing_rels:
                        all_relationships.append(rel)
                        existing_rels.add(key)

                models_used.append(ollama_result.model_used)

            except Exception as e:
                logger.error(f"Ollama extraction error: {e}")

        # Sort and limit
        all_entities = sorted(all_entities, key=lambda e: e.importance_score, reverse=True)[:max_entities]

        extraction_time = (datetime.now() - start_time).total_seconds()

        logger.info(f"Hybrid extraction: {len(all_entities)} entities, {len(all_relationships)} relationships in {extraction_time:.2f}s")

        return ExtractionResult(
            entities=all_entities,
            relationships=all_relationships,
            extraction_time=extraction_time,
            model_used="+".join(models_used) if models_used else "none",
            content_hash=content_hash
        )

    async def close(self):
        """Close resources."""
        if self._ollama_extractor:
            await self._ollama_extractor.close()


# =============================================================================
# HYBRID RAG PIPELINE
# =============================================================================

class HybridRAGPipeline:
    """
    Hybrid RAG pipeline combining graph queries with vector embeddings.
    
    Pipeline:
    1. Content → Entity Extraction → Build Graph
    2. Vector Embeddings (pgvector) + Graph Queries
    3. Enhanced Context for generation
    """
    
    def __init__(
        self,
        extractor: Optional[HybridEntityExtractor] = None,
        graph: Optional[ContentKnowledgeGraph] = None,
        use_spacy: bool = True,
        use_ollama: bool = False
    ):
        """Initialize the pipeline."""
        self.extractor = extractor or HybridEntityExtractor(
            use_spacy=use_spacy,
            use_ollama=use_ollama
        )
        self.graph = graph or ContentKnowledgeGraph()
        
        logger.info("HybridRAGPipeline initialized")
    
    async def process_content(
        self,
        content: str,
        document_id: Optional[int] = None,
        extract_entities: bool = True,
        max_entities: int = 15,
        enhance_with_ollama: bool = False
    ) -> Dict[str, Any]:
        """
        Process content through the hybrid pipeline.
        
        Args:
            content: Source content
            document_id: Optional document ID for linking
            extract_entities: Whether to extract entities
            max_entities: Maximum entities to extract
            enhance_with_ollama: Use Ollama for deeper extraction
            
        Returns:
            Processing result with graph statistics
        """
        result = {
            'success': True,
            'entities_extracted': 0,
            'relationships_extracted': 0,
            'graph_stats': {},
            'document_id': document_id
        }
        
        if extract_entities:
            extraction = await self.extractor.extract(
                content, 
                max_entities,
                enhance_with_ollama=enhance_with_ollama
            )
            
            # Add to graph
            for entity in extraction.entities:
                self.graph.add_entity(entity)
            
            for relationship in extraction.relationships:
                self.graph.add_relationship(relationship)
            
            result['entities_extracted'] = len(extraction.entities)
            result['relationships_extracted'] = len(extraction.relationships)
            result['extraction_time'] = extraction.extraction_time
            result['entities'] = [
                {'name': e.name, 'type': e.entity_type, 'importance': e.importance_score}
                for e in extraction.entities
            ]
            result['relationships'] = [
                {'source': r.source, 'target': r.target, 'type': r.relationship_type.value}
                for r in extraction.relationships
            ]
        
        result['graph_stats'] = self.graph.get_stats()
        
        return result
    
    def get_enhanced_context(
        self,
        query: str,
        top_k: int = 5
    ) -> Dict[str, Any]:
        """
        Get enhanced context for a query using graph + similarity.
        
        Combines:
        - Graph neighbors for structural context
        - Embedding similarity for semantic context
        
        Args:
            query: Query text
            top_k: Number of results
            
        Returns:
            Enhanced context dictionary
        """
        context = {
            'similar_entities': [],
            'related_concepts': [],
            'prerequisite_chain': [],
            'community_context': [],
            'central_entities': []
        }
        
        # Find similar entities
        similar = self.graph.find_similar_entities(query, top_n=top_k)
        context['similar_entities'] = [
            {'name': e.name, 'type': e.entity_type, 'similarity': sim, 'description': e.description}
            for e, sim in similar
        ]
        
        if similar:
            main_entity = similar[0][0].name
            
            # Get related concepts from graph
            related_graph = self.graph.get_related_concepts(main_entity, depth=2)
            context['related_concepts'] = list(related_graph.nodes())[:10]
            
            # Get prerequisite chain
            prereq_order = self.graph.get_prerequisite_order()
            main_idx = prereq_order.index(main_entity) if main_entity in prereq_order else -1
            if main_idx >= 0:
                context['prerequisite_chain'] = prereq_order[:main_idx + 1][-5:]
            
            # Get community members
            community_id = self.graph.get_entity_community(main_entity)
            communities = self.graph.get_communities()
            if community_id < len(communities):
                context['community_context'] = list(communities[community_id])[:10]
        
        # Get central entities
        central = self.graph.get_central_entities(top_n=5)
        context['central_entities'] = [
            {'name': name, 'centrality': score}
            for name, score in central
        ]
        
        return context
    
    def build_rag_context(self, query: str, vector_results: List[Dict] = None) -> str:
        """
        Build enriched RAG context combining vector results with graph knowledge.
        
        Args:
            query: User query
            vector_results: Results from vector similarity search
            
        Returns:
            Enriched context string for LLM
        """
        context_parts = []
        
        # Get graph-enhanced context
        graph_context = self.get_enhanced_context(query)
        
        # Add similar entities
        if graph_context['similar_entities']:
            entities_text = []
            for e in graph_context['similar_entities'][:3]:
                entities_text.append(f"- {e['name']} ({e['type']}): {e['description']}")
            if entities_text:
                context_parts.append("Related Concepts:\n" + "\n".join(entities_text))
        
        # Add vector search results if provided
        if vector_results:
            docs_text = []
            for i, doc in enumerate(vector_results[:5], 1):
                content = doc.get('content', doc.get('text', ''))[:500]
                source = doc.get('source', doc.get('title', f'Document {i}'))
                docs_text.append(f"[{i}] {source}:\n{content}")
            if docs_text:
                context_parts.append("Document Context:\n" + "\n\n".join(docs_text))
        
        # Add relationship context
        if graph_context['related_concepts']:
            context_parts.append(f"Related Topics: {', '.join(graph_context['related_concepts'][:5])}")
        
        return "\n\n---\n\n".join(context_parts) if context_parts else ""
    
    async def close(self):
        """Clean up resources."""
        await self.extractor.close()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

# Global instances
_global_graph: Optional[ContentKnowledgeGraph] = None
_global_pipeline: Optional[HybridRAGPipeline] = None


def get_knowledge_graph() -> ContentKnowledgeGraph:
    """Get or create global knowledge graph."""
    global _global_graph
    if _global_graph is None:
        _global_graph = ContentKnowledgeGraph()
    return _global_graph


def get_hybrid_pipeline(use_spacy: bool = True, use_ollama: bool = False) -> HybridRAGPipeline:
    """Get or create global hybrid pipeline."""
    global _global_pipeline
    if _global_pipeline is None:
        _global_pipeline = HybridRAGPipeline(
            graph=get_knowledge_graph(),
            use_spacy=use_spacy,
            use_ollama=use_ollama
        )
    return _global_pipeline


async def extract_and_build_graph(
    content: str,
    document_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Extract entities from content and build knowledge graph.
    
    Args:
        content: Source content
        document_id: Optional document ID
        
    Returns:
        Extraction and graph statistics
    """
    pipeline = get_hybrid_pipeline()
    return await pipeline.process_content(content, document_id=document_id)


def get_graph_enhanced_context(query: str, vector_results: List[Dict] = None) -> str:
    """
    Get graph-enhanced context for RAG.
    
    Args:
        query: User query
        vector_results: Vector search results
        
    Returns:
        Enhanced context string
    """
    pipeline = get_hybrid_pipeline()
    return pipeline.build_rag_context(query, vector_results)


# =============================================================================
# EXPORT
# =============================================================================

__all__ = [
    # Enums
    'RelationshipType',
    'ReasoningStrategy',

    # Data classes
    'Entity',
    'Relationship',
    'ExtractionResult',
    'ReasoningPath',
    'InferredRelationship',
    'MultiHopReasoningResult',
    'EntityRepresentation',

    # Core classes
    'ContentKnowledgeGraph',
    'GraphReasoner',
    'OllamaEntityExtractor',
    'SpaCyEntityExtractor',
    'HybridEntityExtractor',
    'HybridRAGPipeline',

    # Convenience functions
    'get_knowledge_graph',
    'get_graph_reasoner',
    'get_hybrid_pipeline',
    'extract_and_build_graph',
    'get_graph_enhanced_context',

    # Inference rules
    'INFERENCE_RULES',

    # Availability flags
    'NETWORKX_AVAILABLE',
    'EMBEDDING_AVAILABLE',
    'SPACY_AVAILABLE',
    '_ensure_embedding_available'  # Lazy loading function
]
