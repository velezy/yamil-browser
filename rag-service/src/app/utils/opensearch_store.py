"""
OpenSearch Store

Full-text search and hybrid search using OpenSearch.
Provides BM25 search, highlights, faceted search, and aggregations.

Features:
- Document indexing with metadata
- BM25 full-text search
- Highlighted results
- Faceted search by file type, status
- Aggregation queries
"""

import os
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Try to import OpenSearch
try:
    from opensearchpy import OpenSearch, AsyncOpenSearch
    OPENSEARCH_AVAILABLE = True
except ImportError:
    OPENSEARCH_AVAILABLE = False
    logger.info("OpenSearch client not available — full-text search disabled")

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX", "drivesentinel-documents")


@dataclass
class SearchResult:
    """Result from OpenSearch query"""
    chunk_id: int
    document_id: int
    content: str
    score: float
    highlights: List[str]
    metadata: Dict[str, Any]


class OpenSearchStore:
    """
    OpenSearch integration for BM25 full-text search.

    Complements pgvector semantic search with lexical search capabilities.
    """

    def __init__(
        self,
        url: Optional[str] = None,
        index_name: Optional[str] = None,
    ):
        self.url = url or OPENSEARCH_URL
        self.index_name = index_name or OPENSEARCH_INDEX
        self._client: Optional['OpenSearch'] = None
        self._initialized = False

    def _get_client(self) -> 'OpenSearch':
        """Get or create OpenSearch client"""
        if self._client is None:
            if not OPENSEARCH_AVAILABLE:
                raise RuntimeError("opensearch-py not installed")

            self._client = OpenSearch(
                hosts=[self.url],
                use_ssl=False,
                verify_certs=False,
                timeout=30,
            )
        return self._client

    async def initialize(self):
        """Create index if it doesn't exist"""
        if self._initialized:
            return

        client = self._get_client()

        if not client.indices.exists(self.index_name):
            client.indices.create(
                self.index_name,
                body={
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0,
                        "analysis": {
                            "analyzer": {
                                "content_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "standard",
                                    "filter": ["lowercase", "stop", "snowball"],
                                }
                            }
                        },
                    },
                    "mappings": {
                        "properties": {
                            "chunk_id": {"type": "integer"},
                            "document_id": {"type": "integer"},
                            "content": {
                                "type": "text",
                                "analyzer": "content_analyzer",
                                "fields": {
                                    "exact": {"type": "keyword"},
                                },
                            },
                            "filename": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                            "file_type": {"type": "keyword"},
                            "status": {"type": "keyword"},
                            "tenant_id": {"type": "keyword"},
                            "metadata": {"type": "object", "enabled": True},
                            "created_at": {"type": "date"},
                        }
                    },
                },
            )
            logger.info(f"Created OpenSearch index: {self.index_name}")

        self._initialized = True

    def index_document(
        self,
        chunk_id: int,
        document_id: int,
        content: str,
        filename: str = "",
        file_type: str = "",
        tenant_id: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Index a document chunk"""
        client = self._get_client()
        doc = {
            "chunk_id": chunk_id,
            "document_id": document_id,
            "content": content,
            "filename": filename,
            "file_type": file_type,
            "tenant_id": tenant_id,
            "metadata": metadata or {},
        }
        client.index(
            index=self.index_name,
            body=doc,
            id=f"{document_id}_{chunk_id}",
        )

    def index_documents_bulk(self, documents: List[Dict[str, Any]]):
        """Bulk index documents"""
        client = self._get_client()
        actions = []
        for doc in documents:
            actions.append({"index": {"_index": self.index_name, "_id": f"{doc['document_id']}_{doc['chunk_id']}"}})
            actions.append(doc)

        if actions:
            client.bulk(body=actions)
            logger.info(f"Bulk indexed {len(documents)} documents")

    def search(
        self,
        query: str,
        top_k: int = 10,
        tenant_id: Optional[str] = None,
        file_type: Optional[str] = None,
        document_ids: Optional[List[int]] = None,
    ) -> List[SearchResult]:
        """
        BM25 full-text search with highlights.

        Args:
            query: Search query
            top_k: Number of results
            tenant_id: Filter by tenant
            file_type: Filter by file type
            document_ids: Filter by document IDs

        Returns:
            List of SearchResult
        """
        client = self._get_client()

        # Build query
        must = [{"match": {"content": {"query": query, "operator": "or"}}}]
        filters = []

        if tenant_id:
            filters.append({"term": {"tenant_id": tenant_id}})
        if file_type:
            filters.append({"term": {"file_type": file_type}})
        if document_ids:
            filters.append({"terms": {"document_id": document_ids}})

        body = {
            "size": top_k,
            "query": {
                "bool": {
                    "must": must,
                    "filter": filters,
                }
            },
            "highlight": {
                "fields": {
                    "content": {
                        "fragment_size": 200,
                        "number_of_fragments": 3,
                    }
                }
            },
        }

        response = client.search(index=self.index_name, body=body)

        results = []
        for hit in response["hits"]["hits"]:
            source = hit["_source"]
            highlights = hit.get("highlight", {}).get("content", [])
            results.append(SearchResult(
                chunk_id=source.get("chunk_id", 0),
                document_id=source.get("document_id", 0),
                content=source.get("content", ""),
                score=hit["_score"],
                highlights=highlights,
                metadata=source.get("metadata", {}),
            ))

        return results

    def get_facets(
        self,
        query: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get faceted search aggregations"""
        client = self._get_client()

        body: Dict[str, Any] = {
            "size": 0,
            "aggs": {
                "file_types": {
                    "terms": {"field": "file_type", "size": 20}
                },
                "status": {
                    "terms": {"field": "status", "size": 10}
                },
                "doc_count": {
                    "cardinality": {"field": "document_id"}
                },
            },
        }

        if query:
            body["query"] = {"match": {"content": query}}
        if tenant_id:
            body.setdefault("query", {}).setdefault("bool", {}).setdefault("filter", []).append(
                {"term": {"tenant_id": tenant_id}}
            )

        response = client.search(index=self.index_name, body=body)
        aggs = response.get("aggregations", {})

        return {
            "total_hits": response["hits"]["total"]["value"],
            "file_types": [
                {"type": b["key"], "count": b["doc_count"]}
                for b in aggs.get("file_types", {}).get("buckets", [])
            ],
            "statuses": [
                {"status": b["key"], "count": b["doc_count"]}
                for b in aggs.get("status", {}).get("buckets", [])
            ],
            "unique_documents": aggs.get("doc_count", {}).get("value", 0),
        }

    def delete_document(self, document_id: int):
        """Delete all chunks for a document"""
        client = self._get_client()
        client.delete_by_query(
            index=self.index_name,
            body={"query": {"term": {"document_id": document_id}}},
        )

    def get_info(self) -> Dict[str, Any]:
        """Get store info"""
        info = {
            "available": OPENSEARCH_AVAILABLE,
            "url": self.url,
            "index": self.index_name,
            "initialized": self._initialized,
        }

        if self._initialized and OPENSEARCH_AVAILABLE:
            try:
                client = self._get_client()
                stats = client.indices.stats(self.index_name)
                index_stats = stats["indices"].get(self.index_name, {})
                info["doc_count"] = index_stats.get("total", {}).get("docs", {}).get("count", 0)
                info["store_size"] = index_stats.get("total", {}).get("store", {}).get("size_in_bytes", 0)
            except Exception:
                pass

        return info


# Singleton
_opensearch_store: Optional[OpenSearchStore] = None


def get_opensearch_store() -> OpenSearchStore:
    """Get or create singleton OpenSearch store"""
    global _opensearch_store
    if _opensearch_store is None:
        _opensearch_store = OpenSearchStore()
    return _opensearch_store
