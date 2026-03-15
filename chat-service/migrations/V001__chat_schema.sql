-- Chat Service Schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS common;

CREATE TABLE IF NOT EXISTS common.conversations (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    tenant_id uuid NOT NULL,
    user_id uuid NOT NULL,
    title character varying(500),
    metadata jsonb DEFAULT '{}'::jsonb,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);

CREATE TABLE IF NOT EXISTS common.messages (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    conversation_id uuid NOT NULL REFERENCES common.conversations(id) ON DELETE CASCADE,
    role character varying(20) NOT NULL,
    content text NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_conversations_tenant ON common.conversations(tenant_id);
CREATE INDEX IF NOT EXISTS idx_conversations_user ON common.conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_messages_conversation ON common.messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_messages_created ON common.messages(created_at DESC);
