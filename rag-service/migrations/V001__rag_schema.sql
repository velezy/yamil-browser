-- RAG Service Schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS common;

CREATE TABLE IF NOT EXISTS common.document_chunks (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    document_id uuid NOT NULL,
    tenant_id uuid NOT NULL,
    chunk_index integer NOT NULL,
    content text NOT NULL,
    embedding vector(384),
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS common.vector_index_registry (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    tenant_id uuid NOT NULL,
    name character varying(255) NOT NULL,
    dimensions integer NOT NULL,
    model_name character varying(255),
    document_count integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS common.rag_eval_results (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    tenant_id uuid NOT NULL,
    query text NOT NULL,
    answer text,
    contexts jsonb DEFAULT '[]'::jsonb,
    scores jsonb DEFAULT '{}'::jsonb,
    model character varying(255),
    created_at timestamp with time zone DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chunks_document ON common.document_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_chunks_tenant ON common.document_chunks(tenant_id);
