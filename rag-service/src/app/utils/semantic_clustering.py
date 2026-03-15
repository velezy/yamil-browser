"""
Semantic Clustering for Documents

Automatically groups documents by semantic similarity using embeddings.
Uses K-Means or HDBSCAN for clustering, with LLM-powered cluster naming.
"""

import asyncio
import logging
import numpy as np
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger(__name__)


class ClusteringAlgorithm(str, Enum):
    """Supported clustering algorithms"""
    KMEANS = "kmeans"
    HDBSCAN = "hdbscan"
    AGGLOMERATIVE = "agglomerative"


@dataclass
class ClusterConfig:
    """Configuration for clustering"""
    min_cluster_size: int = 3
    max_clusters: int = 20
    similarity_threshold: float = 0.7
    algorithm: ClusteringAlgorithm = ClusteringAlgorithm.KMEANS
    auto_recluster_enabled: bool = True
    recluster_interval_hours: int = 24


@dataclass
class DocumentVector:
    """Document with its embedding"""
    document_id: int
    filename: str
    embedding: np.ndarray
    content_preview: str = ""


@dataclass
class SemanticCluster:
    """A cluster of semantically similar documents"""
    cluster_id: int
    name: str
    description: str
    centroid: np.ndarray
    document_ids: List[int] = field(default_factory=list)
    coherence_score: float = 0.0
    is_auto_generated: bool = True


@dataclass
class ClusteringResult:
    """Result of clustering operation"""
    clusters: List[SemanticCluster]
    unclustered_docs: List[int]
    total_documents: int
    execution_time_ms: float
    algorithm_used: str


class SemanticClusteringEngine:
    """
    Engine for semantic clustering of documents.

    Features:
    - Multiple clustering algorithms (K-Means, HDBSCAN)
    - LLM-powered cluster naming
    - Incremental clustering for new documents
    - Coherence scoring
    - Centroid-based cluster representation
    """

    def __init__(
        self,
        db_pool=None,
        embedding_model=None,
        llm_client=None,
        config: Optional[ClusterConfig] = None
    ):
        self.db_pool = db_pool
        self.embedding_model = embedding_model
        self.llm_client = llm_client
        self.config = config or ClusterConfig()

    async def initialize(self, db_pool=None):
        """Initialize with database pool"""
        if db_pool:
            self.db_pool = db_pool
        logger.info("SemanticClusteringEngine initialized")

    async def get_document_embeddings(
        self,
        user_id: int,
        document_ids: Optional[List[int]] = None
    ) -> List[DocumentVector]:
        """Fetch document embeddings from database"""
        if not self.db_pool:
            logger.warning("No database pool available")
            return []

        try:
            async with self.db_pool.acquire() as conn:
                if document_ids:
                    query = """
                        SELECT e.document_id, d.filename, e.embedding,
                               LEFT(e.content, 200) as content_preview
                        FROM embeddings e
                        JOIN documents d ON e.document_id = d.id
                        WHERE d.user_id = $1 AND e.document_id = ANY($2)
                        AND e.embedding IS NOT NULL
                    """
                    rows = await conn.fetch(query, user_id, document_ids)
                else:
                    query = """
                        SELECT e.document_id, d.filename, e.embedding,
                               LEFT(e.content, 200) as content_preview
                        FROM embeddings e
                        JOIN documents d ON e.document_id = d.id
                        WHERE d.user_id = $1
                        AND e.embedding IS NOT NULL
                    """
                    rows = await conn.fetch(query, user_id)

                documents = []
                for row in rows:
                    try:
                        # Parse embedding from pgvector format
                        raw_embedding = row['embedding']
                        if isinstance(raw_embedding, str):
                            # Handle string representation (e.g., "[0.1,0.2,...]")
                            import json
                            embedding = np.array(json.loads(raw_embedding.replace("'", '"')))
                        elif isinstance(raw_embedding, (list, tuple)):
                            embedding = np.array(raw_embedding)
                        else:
                            # Assume it's already array-like
                            embedding = np.array(raw_embedding)
                        documents.append(DocumentVector(
                            document_id=row['document_id'],
                            filename=row['filename'],
                            embedding=embedding,
                            content_preview=row['content_preview'] or ""
                        ))
                    except Exception as e:
                        logger.warning(f"Error parsing embedding for doc {row['document_id']}: {e}")

                logger.info(f"Fetched {len(documents)} document embeddings for user {user_id}")
                return documents

        except Exception as e:
            logger.error(f"Error fetching document embeddings: {e}")
            return []

    def _determine_optimal_k(self, n_documents: int) -> int:
        """Determine optimal number of clusters using rule of thumb"""
        if n_documents < self.config.min_cluster_size:
            return 0

        # Rule of thumb: sqrt(n/2), bounded by config
        import math
        k = int(math.sqrt(n_documents / 2))
        k = max(2, min(k, self.config.max_clusters))

        # Ensure we don't have more clusters than documents
        max_possible = n_documents // self.config.min_cluster_size
        k = min(k, max_possible)

        return k

    def _cluster_kmeans(
        self,
        embeddings: np.ndarray,
        n_clusters: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Perform K-Means clustering"""
        try:
            from sklearn.cluster import KMeans

            kmeans = KMeans(
                n_clusters=n_clusters,
                random_state=42,
                n_init=10,
                max_iter=300
            )
            labels = kmeans.fit_predict(embeddings)
            centroids = kmeans.cluster_centers_

            return labels, centroids

        except ImportError:
            logger.warning("scikit-learn not available, using simple clustering")
            return self._cluster_simple(embeddings, n_clusters)

    def _cluster_hdbscan(
        self,
        embeddings: np.ndarray
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Perform HDBSCAN clustering (density-based)"""
        try:
            import hdbscan

            clusterer = hdbscan.HDBSCAN(
                min_cluster_size=self.config.min_cluster_size,
                min_samples=2,
                metric='cosine'
            )
            labels = clusterer.fit_predict(embeddings)

            # Calculate centroids for each cluster
            unique_labels = set(labels) - {-1}  # Exclude noise
            centroids = []
            for label in sorted(unique_labels):
                mask = labels == label
                centroid = embeddings[mask].mean(axis=0)
                centroids.append(centroid)

            return labels, np.array(centroids) if centroids else np.array([])

        except ImportError:
            logger.warning("hdbscan not available, falling back to K-Means")
            n_clusters = self._determine_optimal_k(len(embeddings))
            return self._cluster_kmeans(embeddings, n_clusters)

    def _cluster_simple(
        self,
        embeddings: np.ndarray,
        n_clusters: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Simple clustering fallback without sklearn"""
        # Random initialization
        np.random.seed(42)
        indices = np.random.choice(len(embeddings), n_clusters, replace=False)
        centroids = embeddings[indices].copy()

        for _ in range(50):  # Max iterations
            # Assign points to nearest centroid
            distances = np.zeros((len(embeddings), n_clusters))
            for i, centroid in enumerate(centroids):
                distances[:, i] = np.linalg.norm(embeddings - centroid, axis=1)
            labels = np.argmin(distances, axis=1)

            # Update centroids
            new_centroids = np.zeros_like(centroids)
            for i in range(n_clusters):
                mask = labels == i
                if mask.sum() > 0:
                    new_centroids[i] = embeddings[mask].mean(axis=0)
                else:
                    new_centroids[i] = centroids[i]

            if np.allclose(centroids, new_centroids):
                break
            centroids = new_centroids

        return labels, centroids

    def _calculate_coherence(
        self,
        embeddings: np.ndarray,
        labels: np.ndarray,
        cluster_id: int
    ) -> float:
        """Calculate coherence score for a cluster (average pairwise similarity)"""
        mask = labels == cluster_id
        cluster_embeddings = embeddings[mask]

        if len(cluster_embeddings) < 2:
            return 1.0

        # Calculate pairwise cosine similarities
        norms = np.linalg.norm(cluster_embeddings, axis=1, keepdims=True)
        normalized = cluster_embeddings / (norms + 1e-8)
        similarities = np.dot(normalized, normalized.T)

        # Average of upper triangle (excluding diagonal)
        n = len(cluster_embeddings)
        upper_triangle = similarities[np.triu_indices(n, k=1)]

        return float(np.mean(upper_triangle)) if len(upper_triangle) > 0 else 1.0

    async def _generate_cluster_name(
        self,
        documents: List[DocumentVector],
        centroid: np.ndarray
    ) -> Tuple[str, str]:
        """Generate cluster name and description using LLM or keywords"""
        # Collect filenames and content previews
        filenames = [doc.filename for doc in documents[:10]]
        previews = [doc.content_preview for doc in documents[:5] if doc.content_preview]

        # Try LLM-based naming first
        if self.llm_client:
            try:
                prompt = f"""Based on these document filenames and content previews, suggest a short cluster name (2-4 words) and a one-sentence description.

Filenames: {', '.join(filenames)}

Content samples:
{chr(10).join(previews[:3])}

Respond in this format:
Name: <cluster name>
Description: <one sentence description>"""

                response = await self.llm_client.generate(prompt, max_tokens=100)

                # Parse response
                lines = response.strip().split('\n')
                name = "Unnamed Cluster"
                description = ""

                for line in lines:
                    if line.startswith("Name:"):
                        name = line.replace("Name:", "").strip()
                    elif line.startswith("Description:"):
                        description = line.replace("Description:", "").strip()

                return name, description

            except Exception as e:
                logger.warning(f"LLM naming failed: {e}")

        # Fallback: keyword-based naming
        return self._keyword_based_naming(filenames, previews)

    def _keyword_based_naming(
        self,
        filenames: List[str],
        previews: List[str]
    ) -> Tuple[str, str]:
        """Generate cluster name from keywords in filenames"""
        import re

        # Extract words from filenames
        words = []
        for filename in filenames:
            # Remove extension and split
            name = re.sub(r'\.[^.]+$', '', filename)
            # Split on non-alphanumeric
            parts = re.split(r'[^a-zA-Z0-9]+', name)
            words.extend([w.lower() for w in parts if len(w) > 2])

        # Find most common words
        from collections import Counter
        word_counts = Counter(words)
        common_words = [w for w, c in word_counts.most_common(3) if c > 1]

        if common_words:
            name = " ".join(w.title() for w in common_words[:2]) + " Documents"
        else:
            name = f"Document Group ({len(filenames)} files)"

        description = f"Contains {len(filenames)} documents"

        return name, description

    async def cluster_documents(
        self,
        user_id: int,
        document_ids: Optional[List[int]] = None,
        algorithm: Optional[ClusteringAlgorithm] = None
    ) -> ClusteringResult:
        """
        Perform semantic clustering on user's documents.

        Args:
            user_id: User ID
            document_ids: Optional specific documents to cluster
            algorithm: Clustering algorithm to use

        Returns:
            ClusteringResult with clusters and statistics
        """
        import time
        start_time = time.time()

        algorithm = algorithm or self.config.algorithm

        # Fetch document embeddings
        documents = await self.get_document_embeddings(user_id, document_ids)

        if len(documents) < self.config.min_cluster_size:
            logger.info(f"Not enough documents for clustering: {len(documents)}")
            return ClusteringResult(
                clusters=[],
                unclustered_docs=[d.document_id for d in documents],
                total_documents=len(documents),
                execution_time_ms=(time.time() - start_time) * 1000,
                algorithm_used=algorithm.value
            )

        # Stack embeddings into matrix
        embeddings = np.vstack([d.embedding for d in documents])

        # Perform clustering
        if algorithm == ClusteringAlgorithm.HDBSCAN:
            labels, centroids = self._cluster_hdbscan(embeddings)
        else:
            n_clusters = self._determine_optimal_k(len(documents))
            labels, centroids = self._cluster_kmeans(embeddings, n_clusters)

        # Build cluster objects
        clusters = []
        unclustered = []
        unique_labels = set(labels)

        for label in sorted(unique_labels):
            if label == -1:
                # Noise points (HDBSCAN)
                unclustered.extend([
                    documents[i].document_id
                    for i in range(len(labels))
                    if labels[i] == -1
                ])
                continue

            # Get documents in this cluster
            cluster_docs = [
                documents[i]
                for i in range(len(labels))
                if labels[i] == label
            ]

            if len(cluster_docs) < self.config.min_cluster_size:
                unclustered.extend([d.document_id for d in cluster_docs])
                continue

            # Calculate centroid
            centroid = centroids[label] if label < len(centroids) else embeddings[labels == label].mean(axis=0)

            # Calculate coherence
            coherence = self._calculate_coherence(embeddings, labels, label)

            # Generate name
            name, description = await self._generate_cluster_name(cluster_docs, centroid)

            cluster = SemanticCluster(
                cluster_id=label,
                name=name,
                description=description,
                centroid=centroid,
                document_ids=[d.document_id for d in cluster_docs],
                coherence_score=coherence,
                is_auto_generated=True
            )
            clusters.append(cluster)

        execution_time = (time.time() - start_time) * 1000
        logger.info(f"Clustering complete: {len(clusters)} clusters from {len(documents)} documents in {execution_time:.0f}ms")

        return ClusteringResult(
            clusters=clusters,
            unclustered_docs=unclustered,
            total_documents=len(documents),
            execution_time_ms=execution_time,
            algorithm_used=algorithm.value
        )

    async def save_clusters(
        self,
        user_id: int,
        result: ClusteringResult
    ) -> bool:
        """Save clustering result to database"""
        if not self.db_pool:
            logger.warning("No database pool available")
            return False

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    # Delete existing clusters for user
                    await conn.execute(
                        "DELETE FROM semantic_clusters WHERE user_id = $1",
                        user_id
                    )

                    for cluster in result.clusters:
                        # Insert cluster
                        # Convert centroid to string format for pgvector
                        centroid_str = str(cluster.centroid.tolist())
                        cluster_db_id = await conn.fetchval("""
                            INSERT INTO semantic_clusters
                            (user_id, cluster_name, cluster_description, centroid_embedding,
                             document_count, coherence_score, is_auto_generated, last_reclustered_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                            RETURNING id
                        """,
                            user_id,
                            cluster.name,
                            cluster.description,
                            centroid_str,
                            len(cluster.document_ids),
                            cluster.coherence_score,
                            cluster.is_auto_generated
                        )

                        # Insert document memberships
                        for i, doc_id in enumerate(cluster.document_ids):
                            is_primary = (i == 0)  # First doc is primary
                            await conn.execute("""
                                INSERT INTO document_cluster_membership
                                (document_id, cluster_id, membership_score, is_primary_cluster)
                                VALUES ($1, $2, $3, $4)
                                ON CONFLICT (document_id, cluster_id) DO UPDATE
                                SET membership_score = $3, is_primary_cluster = $4
                            """,
                                doc_id,
                                cluster_db_id,
                                cluster.coherence_score,
                                is_primary
                            )

                    logger.info(f"Saved {len(result.clusters)} clusters for user {user_id}")
                    return True

        except Exception as e:
            logger.error(f"Error saving clusters: {e}")
            return False

    async def get_user_clusters(self, user_id: int) -> List[Dict[str, Any]]:
        """Get all clusters for a user"""
        if not self.db_pool:
            return []

        try:
            async with self.db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT
                        sc.id, sc.cluster_name, sc.cluster_description,
                        sc.document_count, sc.coherence_score, sc.is_auto_generated,
                        sc.created_at, sc.last_reclustered_at,
                        ARRAY_AGG(dcm.document_id) as document_ids
                    FROM semantic_clusters sc
                    LEFT JOIN document_cluster_membership dcm ON sc.id = dcm.cluster_id
                    WHERE sc.user_id = $1
                    GROUP BY sc.id
                    ORDER BY sc.document_count DESC
                """, user_id)

                return [
                    {
                        "id": row['id'],
                        "name": row['cluster_name'],
                        "description": row['cluster_description'],
                        "document_count": row['document_count'],
                        "coherence_score": row['coherence_score'],
                        "is_auto_generated": row['is_auto_generated'],
                        "created_at": row['created_at'].isoformat() if row['created_at'] else None,
                        "document_ids": [d for d in row['document_ids'] if d is not None]
                    }
                    for row in rows
                ]

        except Exception as e:
            logger.error(f"Error fetching clusters: {e}")
            return []

    async def find_cluster_for_document(
        self,
        user_id: int,
        document_embedding: np.ndarray
    ) -> Optional[Dict[str, Any]]:
        """Find the best cluster for a new document"""
        if not self.db_pool:
            return None

        try:
            async with self.db_pool.acquire() as conn:
                # Find most similar cluster centroid
                row = await conn.fetchrow("""
                    SELECT
                        id, cluster_name,
                        1 - (centroid_embedding <=> $2::vector) as similarity
                    FROM semantic_clusters
                    WHERE user_id = $1
                    ORDER BY centroid_embedding <=> $2::vector
                    LIMIT 1
                """, user_id, document_embedding.tolist())

                if row and row['similarity'] >= self.config.similarity_threshold:
                    return {
                        "cluster_id": row['id'],
                        "cluster_name": row['cluster_name'],
                        "similarity": row['similarity']
                    }

                return None

        except Exception as e:
            logger.error(f"Error finding cluster: {e}")
            return None

    async def assign_document_to_cluster(
        self,
        document_id: int,
        cluster_id: int,
        membership_score: float
    ) -> bool:
        """Assign a document to a cluster"""
        if not self.db_pool:
            return False

        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO document_cluster_membership
                    (document_id, cluster_id, membership_score, is_primary_cluster)
                    VALUES ($1, $2, $3, TRUE)
                    ON CONFLICT (document_id, cluster_id) DO UPDATE
                    SET membership_score = $3
                """, document_id, cluster_id, membership_score)

                return True

        except Exception as e:
            logger.error(f"Error assigning document to cluster: {e}")
            return False

    async def rename_cluster(
        self,
        cluster_id: int,
        new_name: str,
        new_description: Optional[str] = None
    ) -> bool:
        """Manually rename a cluster"""
        if not self.db_pool:
            return False

        try:
            async with self.db_pool.acquire() as conn:
                # Get old name for history
                old_row = await conn.fetchrow(
                    "SELECT cluster_name FROM semantic_clusters WHERE id = $1",
                    cluster_id
                )

                if old_row:
                    # Log name change
                    await conn.execute("""
                        INSERT INTO cluster_naming_history
                        (cluster_id, old_name, new_name, name_source)
                        VALUES ($1, $2, $3, 'user_edit')
                    """, cluster_id, old_row['cluster_name'], new_name)

                # Update cluster
                if new_description:
                    await conn.execute("""
                        UPDATE semantic_clusters
                        SET cluster_name = $1, cluster_description = $2,
                            is_auto_generated = FALSE, updated_at = NOW()
                        WHERE id = $3
                    """, new_name, new_description, cluster_id)
                else:
                    await conn.execute("""
                        UPDATE semantic_clusters
                        SET cluster_name = $1, is_auto_generated = FALSE, updated_at = NOW()
                        WHERE id = $2
                    """, new_name, cluster_id)

                return True

        except Exception as e:
            logger.error(f"Error renaming cluster: {e}")
            return False


# Module-level instance
_clustering_engine: Optional[SemanticClusteringEngine] = None


async def get_clustering_engine(
    db_pool=None,
    embedding_model=None,
    llm_client=None
) -> SemanticClusteringEngine:
    """Get or create the clustering engine singleton"""
    global _clustering_engine

    if _clustering_engine is None:
        _clustering_engine = SemanticClusteringEngine(
            db_pool=db_pool,
            embedding_model=embedding_model,
            llm_client=llm_client
        )
    elif db_pool:
        await _clustering_engine.initialize(db_pool)

    return _clustering_engine
