# RAG Service

Enterprise Retrieval-Augmented Generation microservice. Hybrid search, HyDE query expansion, reranking, and RAGAS evaluation. 118 endpoints.

## Quick Start

```bash
docker compose up
```

The service will be available at `http://localhost:8022`. Health check: `GET /health`

## Features

- Hybrid search (BM25 + pgvector semantic)
- HyDE (Hypothetical Document Embedding) for query expansion
- Query reranking with semantic scoring
- RAGAS evaluation framework (RAG quality metrics)
- Vector store management with pgvector
- Knowledge base metadata
- Text-to-SQL (natural language to SQL)
- Batch indexing
- Multi-modal embeddings

## Configuration

Copy `.env.example` to `.env` and configure. See `.env.example` for all available options.

Full OpenAPI docs available at `http://localhost:8022/docs`

## Database

**Tables**: `document_chunks`, `embeddings`, `vector_index_registry`, `rag_eval_results`

## Dependencies

- **PostgreSQL 17** (pgvector) — vector storage, document chunks
- **assemblyline-common** — database
