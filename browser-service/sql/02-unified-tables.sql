-- Unified YAMIL Browser Schema — Phase 1
-- Consolidates chat, RAG, learning config, and MemoByte sync tables
-- into the single yamil_browser database.
-- Runs AFTER init.sql (01-browser.sql) which creates browser_knowledge, browser_actions, browser_credentials.

-- ═══════════════════════════════════════════════════════════════════════
-- Chat tables (migrated from yamil_chat / common schema)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS chat_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    user_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    title VARCHAR(500),
    metadata JSONB DEFAULT '{}'::jsonb,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id UUID NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_conv_tenant ON chat_conversations(tenant_id);
CREATE INDEX IF NOT EXISTS idx_chat_conv_user ON chat_conversations(user_id);
CREATE INDEX IF NOT EXISTS idx_chat_msg_conversation ON chat_messages(conversation_id);
CREATE INDEX IF NOT EXISTS idx_chat_msg_created ON chat_messages(created_at DESC);

-- ═══════════════════════════════════════════════════════════════════════
-- RAG tables (migrated from yamil_rag / common schema)
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS rag_document_chunks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL,
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector(768),
    metadata JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rag_vector_index_registry (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    name VARCHAR(255) NOT NULL,
    dimensions INTEGER NOT NULL,
    model_name VARCHAR(255),
    document_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS rag_eval_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    query TEXT NOT NULL,
    answer TEXT,
    contexts JSONB DEFAULT '[]'::jsonb,
    scores JSONB DEFAULT '{}'::jsonb,
    model VARCHAR(255),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rag_chunks_document ON rag_document_chunks(document_id);
CREATE INDEX IF NOT EXISTS idx_rag_chunks_tenant ON rag_document_chunks(tenant_id);

-- ═══════════════════════════════════════════════════════════════════════
-- Learning config — persists learning on/off + sync on/off across restarts
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS learning_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Defaults: learning ON, sync OFF
INSERT INTO learning_config (key, value) VALUES ('learning_enabled', 'true')
    ON CONFLICT (key) DO NOTHING;
INSERT INTO learning_config (key, value) VALUES ('sync_enabled', 'false')
    ON CONFLICT (key) DO NOTHING;

-- ═══════════════════════════════════════════════════════════════════════
-- MemoByte sync log — tracks what got synced to MemoByte episodic memory
-- ═══════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS memobyte_sync_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    knowledge_id UUID REFERENCES browser_knowledge(id) ON DELETE SET NULL,
    episode_id TEXT,
    status TEXT NOT NULL DEFAULT 'pending',  -- pending, synced, failed
    error TEXT,
    synced_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sync_log_knowledge ON memobyte_sync_log(knowledge_id);
CREATE INDEX IF NOT EXISTS idx_sync_log_status ON memobyte_sync_log(status);
