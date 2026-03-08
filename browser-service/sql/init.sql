-- YAMIL Browser Knowledge Store
-- pgvector extension for embedding similarity search

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS browser_knowledge (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  domain TEXT NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  content JSONB NOT NULL,
  source_goal TEXT,
  source_url TEXT,
  embedding vector(768),
  confidence FLOAT DEFAULT 1.0,
  access_count INT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Cosine similarity index for fast vector search
CREATE INDEX IF NOT EXISTS idx_knowledge_embedding
  ON browser_knowledge USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 20);

CREATE INDEX IF NOT EXISTS idx_knowledge_domain ON browser_knowledge (domain);
CREATE INDEX IF NOT EXISTS idx_knowledge_category ON browser_knowledge (category);
CREATE INDEX IF NOT EXISTS idx_knowledge_created ON browser_knowledge (created_at DESC);

-- Passive action log — raw actions before distillation
CREATE TABLE IF NOT EXISTS browser_actions (
  id BIGSERIAL PRIMARY KEY,
  session_id TEXT NOT NULL,
  action TEXT NOT NULL,
  selector TEXT,
  value TEXT,
  page_url TEXT,
  domain TEXT,
  result TEXT DEFAULT 'ok',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_actions_session ON browser_actions (session_id);
CREATE INDEX IF NOT EXISTS idx_actions_domain ON browser_actions (domain);
CREATE INDEX IF NOT EXISTS idx_actions_created ON browser_actions (created_at DESC);

-- AI-managed credential store — passwords encrypted via Electron safeStorage (OS keychain)
CREATE TABLE IF NOT EXISTS browser_credentials (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  domain TEXT NOT NULL,
  username TEXT NOT NULL,
  password_encrypted TEXT NOT NULL,
  label TEXT,
  form_url TEXT,
  notes TEXT,
  last_used TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(domain, username)
);

CREATE INDEX IF NOT EXISTS idx_credentials_domain ON browser_credentials (domain);
