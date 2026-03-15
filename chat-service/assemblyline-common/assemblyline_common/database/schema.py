"""
DriveSentinel Database Schema
PostgreSQL 17 + pgvector schema definition
"""

import os
import logging
from .asyncpg_connection import get_db_pool, get_connection

logger = logging.getLogger(__name__)

# Set SKIP_SEED_DATA=true in production to skip test/demo data
SKIP_SEED_DATA = os.getenv("SKIP_SEED_DATA", "false").lower() == "true"

# Embedding dimension for all-MiniLM-L6-v2
EMBEDDING_DIM = 384

# =============================================================================
# SCHEMA SQL
# =============================================================================

SCHEMA_SQL = f"""
-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- =============================================================================
-- ORGANIZATIONS TABLE (Multi-tenant support)
-- =============================================================================
CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    license_key VARCHAR(24) UNIQUE,
    license_tier VARCHAR(50) DEFAULT 'pro' CHECK (license_tier IN ('free', 'pro', 'enterprise', 'consumer', 'consumer_plus', 'enterprise_s', 'enterprise_m', 'enterprise_l', 'enterprise_unlimited', 'developer')),
    max_users INTEGER DEFAULT 2,
    domain VARCHAR(255),
    settings JSONB DEFAULT '{{}}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Migration: Add missing columns to organizations table (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'organizations' AND column_name = 'license_key') THEN
        ALTER TABLE organizations ADD COLUMN license_key VARCHAR(24) UNIQUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'organizations' AND column_name = 'domain') THEN
        ALTER TABLE organizations ADD COLUMN domain VARCHAR(255);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'organizations' AND column_name = 'settings') THEN
        ALTER TABLE organizations ADD COLUMN settings JSONB DEFAULT '{{}}';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'organizations' AND column_name = 'max_users') THEN
        ALTER TABLE organizations ADD COLUMN max_users INTEGER DEFAULT 2;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_organizations_license_key ON organizations(license_key) WHERE license_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_organizations_domain ON organizations(domain) WHERE domain IS NOT NULL;

-- =============================================================================
-- DEPARTMENTS TABLE (Enterprise department management)
-- =============================================================================
CREATE TABLE IF NOT EXISTS departments (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_by INTEGER,  -- Will reference users after users table is created
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(organization_id, name)
);

CREATE INDEX IF NOT EXISTS idx_departments_organization ON departments(organization_id);

-- =============================================================================
-- USERS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    hashed_password VARCHAR(255) NOT NULL,
    role VARCHAR(50) DEFAULT 'user' CHECK (role IN ('user', 'manager', 'admin', 'superadmin')),
    organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
    department VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    must_change_password BOOLEAN DEFAULT FALSE,
    sso_only BOOLEAN DEFAULT FALSE,
    sso_provider VARCHAR(50),
    last_sso_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Migration: Add missing columns to users table (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'department') THEN
        ALTER TABLE users ADD COLUMN department VARCHAR(100);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'must_change_password') THEN
        ALTER TABLE users ADD COLUMN must_change_password BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'sso_only') THEN
        ALTER TABLE users ADD COLUMN sso_only BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'sso_provider') THEN
        ALTER TABLE users ADD COLUMN sso_provider VARCHAR(50);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'users' AND column_name = 'last_sso_login') THEN
        ALTER TABLE users ADD COLUMN last_sso_login TIMESTAMP;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_organization ON users(organization_id);
CREATE INDEX IF NOT EXISTS idx_users_department ON users(department) WHERE department IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_users_sso ON users(sso_only) WHERE sso_only = TRUE;

-- =============================================================================
-- DEPARTMENT ACCESS GRANTS TABLE (Enterprise cross-department access)
-- =============================================================================
CREATE TABLE IF NOT EXISTS department_access_grants (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    department VARCHAR(100) NOT NULL,
    granted_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP DEFAULT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(user_id, organization_id, department)
);

CREATE INDEX IF NOT EXISTS idx_access_grants_user ON department_access_grants(user_id);
CREATE INDEX IF NOT EXISTS idx_access_grants_org_dept ON department_access_grants(organization_id, department);

-- =============================================================================
-- ORGANIZATION EMAIL CONFIG TABLE (Enterprise email sending configuration)
-- =============================================================================
CREATE TABLE IF NOT EXISTS org_email_config (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER UNIQUE NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider VARCHAR(50) NOT NULL DEFAULT 'disabled' CHECK (provider IN ('smtp', 'microsoft_graph', 'disabled')),

    -- SMTP Settings (credentials encrypted)
    smtp_host VARCHAR(255),
    smtp_port INTEGER DEFAULT 587,
    smtp_username VARCHAR(255),
    smtp_password_encrypted TEXT,
    smtp_use_tls BOOLEAN DEFAULT TRUE,

    -- Microsoft Graph Settings (credentials encrypted)
    ms_tenant_id VARCHAR(255),
    ms_client_id VARCHAR(255),
    ms_client_secret_encrypted TEXT,

    -- Common Settings
    from_address VARCHAR(255),
    from_name VARCHAR(255) DEFAULT 'DriveSentinel',

    -- Metadata
    is_verified BOOLEAN DEFAULT FALSE,
    last_test_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_org_email_config_org ON org_email_config(organization_id);

-- =============================================================================
-- ORGANIZATION AI PROVIDER CONFIG TABLE (Enterprise LLM configuration)
-- =============================================================================
CREATE TABLE IF NOT EXISTS org_ai_config (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER UNIQUE NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,

    -- Active provider selection
    active_provider VARCHAR(50) NOT NULL DEFAULT 'ollama' CHECK (active_provider IN ('ollama', 'openai', 'anthropic', 'google', 'azure', 'aws_bedrock')),

    -- OpenAI Configuration
    openai_api_key_encrypted TEXT,
    openai_model VARCHAR(100) DEFAULT 'gpt-4o',
    openai_base_url VARCHAR(500),

    -- Anthropic (Claude) Configuration
    anthropic_api_key_encrypted TEXT,
    anthropic_model VARCHAR(100) DEFAULT 'claude-sonnet-4-20250514',

    -- Google (Gemini) Configuration
    google_api_key_encrypted TEXT,
    google_model VARCHAR(100) DEFAULT 'gemini-2.0-flash',
    google_project_id VARCHAR(255),

    -- Azure OpenAI Configuration
    azure_api_key_encrypted TEXT,
    azure_endpoint VARCHAR(500),
    azure_deployment VARCHAR(255),
    azure_api_version VARCHAR(50) DEFAULT '2024-02-15-preview',

    -- AWS Bedrock Configuration
    aws_access_key_encrypted TEXT,
    aws_secret_key_encrypted TEXT,
    aws_region VARCHAR(50) DEFAULT 'us-east-1',
    aws_bedrock_model VARCHAR(100) DEFAULT 'anthropic.claude-3-sonnet-20240229-v1:0',

    -- Local Ollama (fallback)
    ollama_url VARCHAR(500) DEFAULT 'http://localhost:11434',
    ollama_model VARCHAR(100) DEFAULT 'llama3.2',

    -- Metadata
    is_verified BOOLEAN DEFAULT FALSE,
    last_test_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_org_ai_config_org ON org_ai_config(organization_id);

-- =============================================================================
-- ORGANIZATION SSO CONFIG TABLE (Enterprise SSO Configuration)
-- =============================================================================
CREATE TABLE IF NOT EXISTS org_sso_config (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER UNIQUE NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,

    -- SSO Type Selection
    sso_enabled BOOLEAN DEFAULT FALSE,
    sso_provider VARCHAR(50) NOT NULL DEFAULT 'disabled'
        CHECK (sso_provider IN ('disabled', 'oidc', 'saml2')),

    -- OIDC Configuration
    oidc_issuer_url VARCHAR(500),
    oidc_client_id VARCHAR(255),
    oidc_client_secret_encrypted TEXT,
    oidc_scopes VARCHAR(500) DEFAULT 'openid email profile',
    oidc_authorization_endpoint VARCHAR(500),
    oidc_token_endpoint VARCHAR(500),
    oidc_userinfo_endpoint VARCHAR(500),
    oidc_jwks_uri VARCHAR(500),

    -- SAML2 Configuration (SP-initiated)
    saml_entity_id VARCHAR(500),
    saml_idp_entity_id VARCHAR(500),
    saml_idp_sso_url VARCHAR(500),
    saml_idp_slo_url VARCHAR(500),
    saml_idp_certificate TEXT,
    saml_sp_certificate_encrypted TEXT,
    saml_sp_private_key_encrypted TEXT,
    saml_sign_requests BOOLEAN DEFAULT TRUE,
    saml_want_signed_responses BOOLEAN DEFAULT TRUE,
    saml_name_id_format VARCHAR(100) DEFAULT 'urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress',

    -- Domain Enforcement
    enforce_sso_for_domain BOOLEAN DEFAULT FALSE,
    sso_domains TEXT[],

    -- User Provisioning Settings
    auto_provision_users BOOLEAN DEFAULT TRUE,
    default_role VARCHAR(50) DEFAULT 'user',

    -- Claim/Attribute Mapping (stored as JSONB for flexibility)
    attribute_mapping JSONB DEFAULT '{{
        "email": "email",
        "name": "name",
        "given_name": "given_name",
        "family_name": "family_name",
        "groups": "groups",
        "department": "department"
    }}',

    -- Group to Role/Department Mapping
    group_role_mapping JSONB DEFAULT '{{}}',
    group_department_mapping JSONB DEFAULT '{{}}',

    -- Metadata
    is_verified BOOLEAN DEFAULT FALSE,
    last_test_at TIMESTAMP,
    metadata_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_org_sso_config_org ON org_sso_config(organization_id);
CREATE INDEX IF NOT EXISTS idx_org_sso_config_domains ON org_sso_config USING GIN(sso_domains);

-- =============================================================================
-- USER SSO IDENTITIES TABLE (Link users to IdP identities)
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_sso_identities (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,

    -- Identity Provider Info
    sso_provider VARCHAR(50) NOT NULL,
    idp_subject VARCHAR(500) NOT NULL,
    idp_issuer VARCHAR(500) NOT NULL,

    -- Cached IdP Attributes
    idp_email VARCHAR(255),
    idp_name VARCHAR(255),
    idp_groups JSONB DEFAULT '[]',
    raw_attributes JSONB DEFAULT '{{}}',

    -- Session Info
    last_login_at TIMESTAMP,
    session_id VARCHAR(255),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(organization_id, sso_provider, idp_subject)
);

CREATE INDEX IF NOT EXISTS idx_user_sso_identities_user ON user_sso_identities(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sso_identities_subject ON user_sso_identities(idp_subject);
CREATE INDEX IF NOT EXISTS idx_user_sso_identities_org ON user_sso_identities(organization_id);

-- =============================================================================
-- DOCUMENTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS documents (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(500) NOT NULL,
    file_hash VARCHAR(64) NOT NULL,
    file_size INTEGER NOT NULL,
    file_type VARCHAR(50) NOT NULL,
    file_path VARCHAR(1000),
    status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'no_content', 'needs_reindex')),
    chunk_count INTEGER DEFAULT 0,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    organization_id INTEGER REFERENCES organizations(id) ON DELETE SET NULL,
    department VARCHAR(100),  -- Department for cross-department access filtering
    metadata JSONB DEFAULT '{{}}',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,  -- Soft delete support

    -- Document flagging (Enterprise feature)
    flagged_at TIMESTAMP DEFAULT NULL,
    flagged_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    flag_reason TEXT DEFAULT NULL,

    -- Delete protection (Enterprise feature)
    is_protected BOOLEAN DEFAULT FALSE,
    protected_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    protected_at TIMESTAMP DEFAULT NULL,

    -- Visibility control
    visibility VARCHAR(20) DEFAULT 'private' CHECK (visibility IN ('private', 'department', 'organization')),

    -- Folder assignment
    folder_id UUID,

    UNIQUE(file_hash, user_id)
);

-- Migration: Add missing columns to documents table (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'file_hash') THEN
        ALTER TABLE documents ADD COLUMN file_hash VARCHAR(64);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'status') THEN
        ALTER TABLE documents ADD COLUMN status VARCHAR(50) DEFAULT 'pending';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'chunk_count') THEN
        ALTER TABLE documents ADD COLUMN chunk_count INTEGER DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'error_message') THEN
        ALTER TABLE documents ADD COLUMN error_message TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'processed_at') THEN
        ALTER TABLE documents ADD COLUMN processed_at TIMESTAMP;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'department') THEN
        ALTER TABLE documents ADD COLUMN department VARCHAR(100);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'deleted_at') THEN
        ALTER TABLE documents ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'flagged_at') THEN
        ALTER TABLE documents ADD COLUMN flagged_at TIMESTAMP DEFAULT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'flagged_by') THEN
        ALTER TABLE documents ADD COLUMN flagged_by INTEGER;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'flag_reason') THEN
        ALTER TABLE documents ADD COLUMN flag_reason TEXT DEFAULT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'is_protected') THEN
        ALTER TABLE documents ADD COLUMN is_protected BOOLEAN DEFAULT FALSE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'protected_by') THEN
        ALTER TABLE documents ADD COLUMN protected_by INTEGER;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'protected_at') THEN
        ALTER TABLE documents ADD COLUMN protected_at TIMESTAMP DEFAULT NULL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'visibility') THEN
        ALTER TABLE documents ADD COLUMN visibility VARCHAR(20) DEFAULT 'private';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_documents_user ON documents(user_id);
CREATE INDEX IF NOT EXISTS idx_documents_organization ON documents(organization_id);
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_hash ON documents(file_hash);
CREATE INDEX IF NOT EXISTS idx_documents_deleted_at ON documents(deleted_at) WHERE deleted_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_department ON documents(organization_id, department);
CREATE INDEX IF NOT EXISTS idx_documents_flagged ON documents(flagged_at) WHERE flagged_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_documents_visibility ON documents(visibility);
CREATE INDEX IF NOT EXISTS idx_documents_protected ON documents(is_protected) WHERE is_protected = TRUE;

-- =============================================================================
-- DOCUMENT CHUNKS TABLE (with pgvector embeddings)
-- =============================================================================
CREATE TABLE IF NOT EXISTS document_chunks (
    id SERIAL PRIMARY KEY,
    document_id INTEGER NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content TEXT NOT NULL,
    embedding vector({EMBEDDING_DIM}),
    metadata JSONB DEFAULT '{{}}',
    token_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(document_id, chunk_index)
);

-- HNSW index for fast vector similarity search
CREATE INDEX IF NOT EXISTS idx_chunks_embedding_hnsw ON document_chunks
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 128);

-- Index for document lookups
CREATE INDEX IF NOT EXISTS idx_chunks_document ON document_chunks(document_id);

-- GIN index for full-text search on content
CREATE INDEX IF NOT EXISTS idx_chunks_content_fts ON document_chunks
    USING gin(to_tsvector('english', content));

-- =============================================================================
-- CONVERSATIONS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS conversations (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500),
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    model_used VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversations_user ON conversations(user_id);

-- =============================================================================
-- MESSAGES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL CHECK (role IN ('user', 'assistant', 'system')),
    content TEXT NOT NULL,
    sources JSONB,
    agent_used VARCHAR(100),
    model_used VARCHAR(100),
    processing_time_ms INTEGER,
    quality_score FLOAT,  -- 0.0-1.0 score from MonitorAgent
    quality_grade VARCHAR(2),  -- A, B, C, D, F grade
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id);

-- Migration: Add quality columns if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'messages' AND column_name = 'quality_score') THEN
        ALTER TABLE messages ADD COLUMN quality_score FLOAT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'messages' AND column_name = 'quality_grade') THEN
        ALTER TABLE messages ADD COLUMN quality_grade VARCHAR(2);
    END IF;
END $$;

-- =============================================================================
-- PROMPTS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    content TEXT NOT NULL,
    category VARCHAR(50) DEFAULT 'custom' CHECK (category IN ('system', 'guardrails', 'templates', 'rag', 'custom')),
    is_default BOOLEAN DEFAULT FALSE,
    version INTEGER DEFAULT 1,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL,  -- Soft delete support
    CONSTRAINT prompts_name_category_unique UNIQUE (name, category)
);

CREATE INDEX IF NOT EXISTS idx_prompts_category ON prompts(category);
CREATE INDEX IF NOT EXISTS idx_prompts_user ON prompts(user_id);
CREATE INDEX IF NOT EXISTS idx_prompts_deleted_at ON prompts(deleted_at) WHERE deleted_at IS NOT NULL;

-- =============================================================================
-- PROMPT VERSIONS TABLE (Version History)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompt_versions (
    id SERIAL PRIMARY KEY,
    prompt_id INTEGER NOT NULL REFERENCES prompts(id) ON DELETE CASCADE,
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    change_summary TEXT,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(prompt_id, version)
);

CREATE INDEX IF NOT EXISTS idx_prompt_versions_prompt ON prompt_versions(prompt_id);

-- =============================================================================
-- GUARDRAILS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS guardrails (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(50) NOT NULL CHECK (category IN ('safety', 'privacy', 'compliance', 'brand', 'accuracy')),
    pattern TEXT,  -- Regex pattern
    keywords JSONB DEFAULT '[]',  -- Array of keywords
    action VARCHAR(50) DEFAULT 'block' CHECK (action IN ('block', 'warn', 'modify', 'log')),
    replacement_text TEXT,  -- For 'modify' action
    is_active BOOLEAN DEFAULT TRUE,
    priority INTEGER DEFAULT 0,
    apply_to_input BOOLEAN DEFAULT TRUE,
    apply_to_output BOOLEAN DEFAULT TRUE,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL  -- Soft delete support
);

CREATE INDEX IF NOT EXISTS idx_guardrails_category ON guardrails(category);
CREATE INDEX IF NOT EXISTS idx_guardrails_active ON guardrails(is_active);
CREATE INDEX IF NOT EXISTS idx_guardrails_priority ON guardrails(priority DESC);
CREATE INDEX IF NOT EXISTS idx_guardrails_deleted_at ON guardrails(deleted_at) WHERE deleted_at IS NOT NULL;

-- =============================================================================
-- GUARDRAIL VIOLATIONS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS guardrail_violations (
    id SERIAL PRIMARY KEY,
    guardrail_id INTEGER NOT NULL REFERENCES guardrails(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
    original_content TEXT NOT NULL,
    action_taken VARCHAR(50) NOT NULL,
    modified_content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_violations_guardrail ON guardrail_violations(guardrail_id);
CREATE INDEX IF NOT EXISTS idx_violations_user ON guardrail_violations(user_id);
CREATE INDEX IF NOT EXISTS idx_violations_created ON guardrail_violations(created_at);

-- =============================================================================
-- JAILBREAK EMBEDDINGS TABLE (ML-based jailbreak detection)
-- =============================================================================
CREATE TABLE IF NOT EXISTS jailbreak_embeddings (
    id SERIAL PRIMARY KEY,
    text_hash VARCHAR(64) UNIQUE NOT NULL,  -- SHA256 hash of normalized text
    embedding JSONB NOT NULL,  -- Embedding vector stored as JSON array
    category VARCHAR(50) NOT NULL,  -- Jailbreak category (dan_mode, roleplay_bypass, etc.)
    example_text TEXT,  -- Original text (for reference, may be null for privacy)
    is_seed BOOLEAN DEFAULT FALSE,  -- TRUE if from initial seed dataset
    is_active BOOLEAN DEFAULT TRUE,  -- Can disable without deleting
    detection_count INTEGER DEFAULT 0,  -- How many times this pattern matched
    last_matched_at TIMESTAMP,  -- When it was last matched
    added_by INTEGER REFERENCES users(id) ON DELETE SET NULL,  -- Who added it (if manually)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jailbreak_category ON jailbreak_embeddings(category);
CREATE INDEX IF NOT EXISTS idx_jailbreak_active ON jailbreak_embeddings(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_jailbreak_seed ON jailbreak_embeddings(is_seed);

-- =============================================================================
-- JAILBREAK DETECTION LOG TABLE (Track detected attempts)
-- =============================================================================
CREATE TABLE IF NOT EXISTS jailbreak_detections (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
    input_text_hash VARCHAR(64) NOT NULL,  -- Hash of the input (for privacy)
    input_preview VARCHAR(200),  -- First 200 chars for review
    matched_embedding_id INTEGER REFERENCES jailbreak_embeddings(id) ON DELETE SET NULL,
    category VARCHAR(50) NOT NULL,
    confidence DECIMAL(4,3) NOT NULL,  -- 0.000 to 1.000
    similarity_score DECIMAL(4,3) NOT NULL,
    action_taken VARCHAR(50) DEFAULT 'blocked',  -- blocked, warned, logged
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jailbreak_det_user ON jailbreak_detections(user_id);
CREATE INDEX IF NOT EXISTS idx_jailbreak_det_category ON jailbreak_detections(category);
CREATE INDEX IF NOT EXISTS idx_jailbreak_det_confidence ON jailbreak_detections(confidence DESC);
CREATE INDEX IF NOT EXISTS idx_jailbreak_det_created ON jailbreak_detections(created_at);

-- =============================================================================
-- USER SETTINGS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_settings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    theme VARCHAR(50) DEFAULT 'system',
    high_contrast BOOLEAN DEFAULT FALSE,
    notifications BOOLEAN DEFAULT TRUE,
    stream_responses BOOLEAN DEFAULT TRUE,
    ollama_url VARCHAR(500) DEFAULT 'http://localhost:11434',
    model VARCHAR(100) DEFAULT 'gemma3:4b',
    embedding_model VARCHAR(100) DEFAULT 'nomic-embed-text',
    temperature DECIMAL(3,2) DEFAULT 0.7,
    max_tokens INTEGER DEFAULT 2000,
    chunk_size INTEGER DEFAULT 500,
    chunk_overlap INTEGER DEFAULT 50,
    top_k INTEGER DEFAULT 5,
    min_score DECIMAL(3,2) DEFAULT 0.5,
    voice VARCHAR(50) DEFAULT 'af_heart',
    voice_speed DECIMAL(3,2) DEFAULT 1.0,
    voice_volume DECIMAL(3,2) DEFAULT 1.0,
    auto_read_aloud BOOLEAN DEFAULT FALSE,
    tts_provider VARCHAR(50) DEFAULT 'kokoro',
    tts_streaming_enabled BOOLEAN DEFAULT TRUE,
    tts_wait_for_complete BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Migration: Add missing TTS columns to user_settings (for existing databases)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_settings' AND column_name = 'tts_provider') THEN
        ALTER TABLE user_settings ADD COLUMN tts_provider VARCHAR(50) DEFAULT 'kokoro';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_settings' AND column_name = 'tts_streaming_enabled') THEN
        ALTER TABLE user_settings ADD COLUMN tts_streaming_enabled BOOLEAN DEFAULT TRUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_settings' AND column_name = 'tts_wait_for_complete') THEN
        ALTER TABLE user_settings ADD COLUMN tts_wait_for_complete BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- =============================================================================
-- USER SECRET VAULT TABLE
-- =============================================================================
-- Encrypted storage for sensitive user credentials (OAuth tokens, API keys, etc.)
-- Each user has their own vault with AES-256-GCM encrypted values
CREATE TABLE IF NOT EXISTS user_secret_vault (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    secret_key VARCHAR(100) NOT NULL,  -- e.g., 'email.gmail.access_token'
    encrypted_value TEXT NOT NULL,  -- AES-256-GCM encrypted
    encryption_metadata JSONB NOT NULL,  -- salt, nonce, algorithm info
    category VARCHAR(50) NOT NULL DEFAULT 'general',  -- email, oauth, api_key, etc.
    description VARCHAR(255),  -- Optional human-readable description
    expires_at TIMESTAMP,  -- Optional expiration for tokens
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, secret_key)
);

CREATE INDEX IF NOT EXISTS idx_secret_vault_user ON user_secret_vault(user_id);
CREATE INDEX IF NOT EXISTS idx_secret_vault_category ON user_secret_vault(user_id, category);
CREATE INDEX IF NOT EXISTS idx_secret_vault_expires ON user_secret_vault(expires_at) WHERE expires_at IS NOT NULL;

-- =============================================================================
-- API KEYS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    key_hash VARCHAR(255) NOT NULL,
    key_prefix VARCHAR(20) NOT NULL,
    expires_at TIMESTAMP,
    last_used TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);

-- =============================================================================
-- AI PERFORMANCE METRICS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS ai_metrics (
    id SERIAL PRIMARY KEY,
    query_id VARCHAR(100),
    query_text TEXT,
    retrieval_time_ms INTEGER,
    generation_time_ms INTEGER,
    total_time_ms INTEGER,
    chunks_retrieved INTEGER,
    model_used VARCHAR(100),
    agent_used VARCHAR(100),
    precision_score DECIMAL(5,4),
    recall_score DECIMAL(5,4),
    faithfulness_score DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_created ON ai_metrics(created_at);

-- =============================================================================
-- AUDIT LOG TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS audit_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(100),
    resource_id VARCHAR(100),
    details JSONB,
    ip_address VARCHAR(45),
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_log(action);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_log(created_at);

-- =============================================================================
-- COMPANY SETTINGS TABLE (Super Admin Configuration)
-- =============================================================================
CREATE TABLE IF NOT EXISTS company_settings (
    id SERIAL PRIMARY KEY,
    setting_key VARCHAR(255) UNIQUE NOT NULL,
    setting_value JSONB NOT NULL,
    description TEXT,
    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_settings_key ON company_settings(setting_key);

-- =============================================================================
-- PROMPT SUGGESTIONS TABLE (AI-Generated Prompt Suggestions)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompt_suggestions (
    id SERIAL PRIMARY KEY,
    suggested_prompt TEXT NOT NULL,
    source_query TEXT NOT NULL,
    category VARCHAR(50) DEFAULT 'custom' CHECK (category IN ('system', 'guardrails', 'templates', 'rag', 'custom')),
    risk_level VARCHAR(20) DEFAULT 'low' CHECK (risk_level IN ('low', 'medium', 'high')),
    suggested_by VARCHAR(100) DEFAULT 'orchestrator',
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    reviewed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    review_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP
);

-- Migration: Add missing columns to prompt_suggestions table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'prompt_suggestions' AND column_name = 'status') THEN
        ALTER TABLE prompt_suggestions ADD COLUMN status VARCHAR(20) DEFAULT 'pending';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'prompt_suggestions' AND column_name = 'category') THEN
        ALTER TABLE prompt_suggestions ADD COLUMN category VARCHAR(50) DEFAULT 'custom';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'prompt_suggestions' AND column_name = 'risk_level') THEN
        ALTER TABLE prompt_suggestions ADD COLUMN risk_level VARCHAR(20) DEFAULT 'low';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'prompt_suggestions' AND column_name = 'suggested_by') THEN
        ALTER TABLE prompt_suggestions ADD COLUMN suggested_by VARCHAR(100) DEFAULT 'orchestrator';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_prompt_suggestions_status ON prompt_suggestions(status);
CREATE INDEX IF NOT EXISTS idx_prompt_suggestions_created ON prompt_suggestions(created_at);

-- =============================================================================
-- PROMPT CHANGE REQUESTS TABLE (Approval Workflow)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompt_change_requests (
    id SERIAL PRIMARY KEY,
    request_type VARCHAR(50) NOT NULL CHECK (request_type IN ('create', 'update', 'delete')),
    target_type VARCHAR(50) NOT NULL CHECK (target_type IN ('prompt', 'guardrail')),
    target_id INTEGER,
    proposed_content JSONB NOT NULL,
    risk_level VARCHAR(20) DEFAULT 'low' CHECK (risk_level IN ('low', 'medium', 'high')),
    status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    requested_by INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    reviewed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    review_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_change_requests_status ON prompt_change_requests(status);
CREATE INDEX IF NOT EXISTS idx_change_requests_type ON prompt_change_requests(target_type);
CREATE INDEX IF NOT EXISTS idx_change_requests_requester ON prompt_change_requests(requested_by);

-- =============================================================================
-- PROMPT TESTS TABLE (Prompt Testing Sandbox)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompt_tests (
    id SERIAL PRIMARY KEY,
    prompt_id INTEGER REFERENCES prompts(id) ON DELETE SET NULL,
    prompt_content TEXT NOT NULL,
    test_query TEXT NOT NULL,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prompt_tests_prompt ON prompt_tests(prompt_id);
CREATE INDEX IF NOT EXISTS idx_prompt_tests_user ON prompt_tests(created_by);

-- =============================================================================
-- PROMPT TEST RESULTS TABLE (Multi-Model Comparison)
-- =============================================================================
CREATE TABLE IF NOT EXISTS prompt_test_results (
    id SERIAL PRIMARY KEY,
    test_id INTEGER NOT NULL REFERENCES prompt_tests(id) ON DELETE CASCADE,
    model VARCHAR(100) NOT NULL,
    response TEXT,
    quality_score INTEGER CHECK (quality_score >= 0 AND quality_score <= 100),
    relevance_score INTEGER CHECK (relevance_score >= 0 AND relevance_score <= 100),
    latency_ms INTEGER,
    token_count INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_test_results_test ON prompt_test_results(test_id);
CREATE INDEX IF NOT EXISTS idx_test_results_model ON prompt_test_results(model);

-- =============================================================================
-- AI MODELS TABLE (Ollama Model Management)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ai_models (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    tier VARCHAR(50) DEFAULT 'quality' CHECK (tier IN ('fast', 'quality', 'deep', 'vision', 'embedding', 'math', 'coder')),
    role VARCHAR(100) DEFAULT 'general',
    description TEXT,
    is_default BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    parameters JSONB DEFAULT '{{}}',
    capabilities JSONB DEFAULT '[]',
    context_length INTEGER DEFAULT 4096,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_ai_models_tier ON ai_models(tier);
CREATE INDEX IF NOT EXISTS idx_ai_models_role ON ai_models(role);
CREATE INDEX IF NOT EXISTS idx_ai_models_active ON ai_models(is_active);

-- Migration: Add 'math' and 'coder' tiers to ai_models CHECK constraint
DO $$
BEGIN
    -- Drop old constraint and add updated one with math/coder tiers
    IF EXISTS (
        SELECT 1 FROM information_schema.check_constraints
        WHERE constraint_name = 'ai_models_tier_check'
    ) THEN
        ALTER TABLE ai_models DROP CONSTRAINT ai_models_tier_check;
        ALTER TABLE ai_models ADD CONSTRAINT ai_models_tier_check
            CHECK (tier IN ('fast', 'quality', 'deep', 'vision', 'embedding', 'math', 'coder'));
    END IF;
END $$;

-- =============================================================================
-- USER MEMORIES TABLE (Mem0-style memory storage)
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_memories (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    source VARCHAR(100) DEFAULT 'chat',
    category VARCHAR(50) DEFAULT 'general',
    confidence DECIMAL(3,2) DEFAULT 0.8,
    metadata JSONB DEFAULT '{{}}',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP DEFAULT NULL  -- Soft delete support
);

-- Migration: Add missing columns to user_memories table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_memories' AND column_name = 'category') THEN
        ALTER TABLE user_memories ADD COLUMN category VARCHAR(50) DEFAULT 'general';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_memories' AND column_name = 'source') THEN
        ALTER TABLE user_memories ADD COLUMN source VARCHAR(100) DEFAULT 'chat';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_memories' AND column_name = 'confidence') THEN
        ALTER TABLE user_memories ADD COLUMN confidence DECIMAL(3,2) DEFAULT 0.8;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_memories' AND column_name = 'is_active') THEN
        ALTER TABLE user_memories ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'user_memories' AND column_name = 'deleted_at') THEN
        ALTER TABLE user_memories ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_user_memories_user ON user_memories(user_id);
CREATE INDEX IF NOT EXISTS idx_user_memories_category ON user_memories(category);
CREATE INDEX IF NOT EXISTS idx_user_memories_created ON user_memories(created_at);
CREATE INDEX IF NOT EXISTS idx_user_memories_deleted_at ON user_memories(deleted_at) WHERE deleted_at IS NOT NULL;

-- =============================================================================
-- TTS FILTERS TABLE (Admin-configurable TTS text filters)
-- =============================================================================
CREATE TABLE IF NOT EXISTS tts_filters (
    id SERIAL PRIMARY KEY,
    pattern TEXT NOT NULL,
    filter_type VARCHAR(20) DEFAULT 'text' CHECK (filter_type IN ('text', 'regex')),
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tts_filters_active ON tts_filters(is_active);

-- =============================================================================
-- QUALITY METRICS TABLE (RAGAS quality metrics)
-- =============================================================================
CREATE TABLE IF NOT EXISTS quality_metrics (
    id SERIAL PRIMARY KEY,
    query_id VARCHAR(100),
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,

    -- RAGAS Core Metrics
    faithfulness DECIMAL(5,4),
    answer_relevancy DECIMAL(5,4),
    context_precision DECIMAL(5,4),
    context_recall DECIMAL(5,4),
    overall_score DECIMAL(5,4),

    -- Performance Metrics
    response_time_ms INTEGER,
    chunks_retrieved INTEGER,
    model_used VARCHAR(100),
    agent_used VARCHAR(100),

    -- Status
    was_successful BOOLEAN DEFAULT TRUE,
    error_message TEXT,

    -- Metadata
    metadata JSONB DEFAULT '{{}}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_metrics_user ON quality_metrics(user_id);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_created ON quality_metrics(created_at);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_model ON quality_metrics(model_used);

-- =============================================================================
-- AGENT AUDIT LOG TABLE (Industry-standard agentic AI observability)
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent_audit_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,

    -- Request details
    query TEXT NOT NULL,
    request_type VARCHAR(50) DEFAULT 'chat',

    -- Agent pipeline info
    agent_used VARCHAR(100),
    model_tier VARCHAR(50),
    model_used VARCHAR(100),
    complexity VARCHAR(50),

    -- Response details
    response TEXT,
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,

    -- Quality metrics (RAGAS)
    quality_score INTEGER,
    quality_grade VARCHAR(2),
    issues JSONB DEFAULT '[]',

    -- RAG metrics
    context_used BOOLEAN DEFAULT FALSE,
    context_quality FLOAT,
    sources JSONB DEFAULT '[]',

    -- Tool usage
    tool_used VARCHAR(100),
    tool_result JSONB,

    -- Token metrics
    query_tokens INTEGER,
    context_tokens INTEGER,
    response_tokens INTEGER,
    efficiency_grade VARCHAR(1),

    -- Timing (OpenTelemetry-style spans)
    total_latency_ms INTEGER,
    rag_latency_ms INTEGER,
    generation_latency_ms INTEGER,
    quality_latency_ms INTEGER,

    -- Reflection/regeneration tracking
    regeneration_count INTEGER DEFAULT 0,
    reflection_improved BOOLEAN DEFAULT FALSE,

    -- Memory agent tracking
    memory_learned BOOLEAN DEFAULT FALSE,
    personalization_applied BOOLEAN DEFAULT FALSE,

    -- Request metadata
    ip_address VARCHAR(45),
    user_agent TEXT,
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_agent_audit_user ON agent_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_agent_audit_created ON agent_audit_log(created_at);
CREATE INDEX IF NOT EXISTS idx_agent_audit_agent ON agent_audit_log(agent_used);
CREATE INDEX IF NOT EXISTS idx_agent_audit_success ON agent_audit_log(success);
CREATE INDEX IF NOT EXISTS idx_agent_audit_quality ON agent_audit_log(quality_score);
CREATE INDEX IF NOT EXISTS idx_agent_audit_conversation ON agent_audit_log(conversation_id);

-- =============================================================================
-- SYSTEM ERROR LOG TABLE (Centralized error management)
-- =============================================================================
CREATE TABLE IF NOT EXISTS system_error_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    agent_audit_id INTEGER REFERENCES agent_audit_log(id) ON DELETE SET NULL,

    -- Error details
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    stack_trace TEXT,

    -- Context
    service_name VARCHAR(100),
    agent_name VARCHAR(100),
    model_used VARCHAR(100),
    request_context JSONB,

    -- Severity (debug, info, warning, error, critical)
    severity VARCHAR(20) DEFAULT 'error',

    -- Resolution tracking
    resolved BOOLEAN DEFAULT FALSE,
    resolution_notes TEXT,
    resolved_at TIMESTAMP,
    resolved_by INTEGER REFERENCES users(id),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_error_log_created ON system_error_log(created_at);
CREATE INDEX IF NOT EXISTS idx_error_log_severity ON system_error_log(severity);
CREATE INDEX IF NOT EXISTS idx_error_log_resolved ON system_error_log(resolved);
CREATE INDEX IF NOT EXISTS idx_error_log_service ON system_error_log(service_name);

-- =============================================================================
-- LRM SELF-LEARNING TABLES
-- =============================================================================

-- Knowledge gaps identified by DiscoveryAgent
CREATE TABLE IF NOT EXISTS knowledge_gaps (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    gap_type VARCHAR(50) CHECK (gap_type IN ('missing_topic', 'low_coverage', 'outdated', 'conflicting', 'ambiguous')),
    description TEXT,
    confidence FLOAT,
    suggested_action TEXT,
    related_queries JSONB DEFAULT '[]',
    status VARCHAR(20) DEFAULT 'open' CHECK (status IN ('open', 'addressed', 'ignored', 'investigating')),
    addressed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    addressed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_knowledge_gaps_status ON knowledge_gaps(status);
CREATE INDEX IF NOT EXISTS idx_knowledge_gaps_type ON knowledge_gaps(gap_type);
CREATE INDEX IF NOT EXISTS idx_knowledge_gaps_created ON knowledge_gaps(created_at);

-- Feedback examples for contrastive learning
CREATE TABLE IF NOT EXISTS feedback_examples (
    id SERIAL PRIMARY KEY,
    query TEXT NOT NULL,
    bad_response TEXT,
    good_response TEXT,
    issue_type VARCHAR(50),
    improvement_notes TEXT,
    feedback_source VARCHAR(50) CHECK (feedback_source IN ('monitor', 'reflection', 'user', 'discovery')),
    quality_before INTEGER,
    quality_after INTEGER,
    used_for_training BOOLEAN DEFAULT FALSE,
    training_batch_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_feedback_examples_source ON feedback_examples(feedback_source);
CREATE INDEX IF NOT EXISTS idx_feedback_examples_issue ON feedback_examples(issue_type);
CREATE INDEX IF NOT EXISTS idx_feedback_examples_training ON feedback_examples(used_for_training);

-- Reasoning traces for chain-of-thought debugging and learning
CREATE TABLE IF NOT EXISTS reasoning_traces (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE SET NULL,
    user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    query TEXT NOT NULL,

    -- Reasoning steps (hidden scratchpad)
    reasoning_steps JSONB DEFAULT '[]',
    sub_questions JSONB DEFAULT '[]',

    -- Verification and backtracking
    retrieval_attempts INTEGER DEFAULT 1,
    backtrack_reasons JSONB DEFAULT '[]',
    verification_scores JSONB DEFAULT '[]',

    -- Final metrics
    final_confidence FLOAT,
    iterations_used INTEGER DEFAULT 1,
    backtrack_count INTEGER DEFAULT 0,
    total_reasoning_time_ms INTEGER,

    -- Intent-based routing
    detected_intent VARCHAR(50),

    -- Success tracking
    was_successful BOOLEAN DEFAULT TRUE,
    failure_reason TEXT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_reasoning_traces_user ON reasoning_traces(user_id);
CREATE INDEX IF NOT EXISTS idx_reasoning_traces_conversation ON reasoning_traces(conversation_id);
CREATE INDEX IF NOT EXISTS idx_reasoning_traces_success ON reasoning_traces(was_successful);
CREATE INDEX IF NOT EXISTS idx_reasoning_traces_created ON reasoning_traces(created_at);

-- Concept taxonomy for ontology management
CREATE TABLE IF NOT EXISTS concept_taxonomy (
    id SERIAL PRIMARY KEY,
    concept VARCHAR(255) NOT NULL,
    normalized_concept VARCHAR(255),
    parent_concept VARCHAR(255),
    related_concepts JSONB DEFAULT '[]',
    synonyms JSONB DEFAULT '[]',
    definition TEXT,
    document_count INTEGER DEFAULT 0,
    query_count INTEGER DEFAULT 0,
    confidence FLOAT DEFAULT 0.5,
    source VARCHAR(50) DEFAULT 'extracted' CHECK (source IN ('extracted', 'manual', 'inferred')),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_concept_taxonomy_concept ON concept_taxonomy(concept);
CREATE INDEX IF NOT EXISTS idx_concept_taxonomy_parent ON concept_taxonomy(parent_concept);
CREATE INDEX IF NOT EXISTS idx_concept_taxonomy_active ON concept_taxonomy(is_active);

-- Retrieval strategies for adaptive search
CREATE TABLE IF NOT EXISTS retrieval_strategies (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    query_pattern TEXT,
    parameters JSONB DEFAULT '{{}}',
    success_rate FLOAT DEFAULT 0.5,
    usage_count INTEGER DEFAULT 0,
    avg_quality_score FLOAT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_retrieval_strategies_name ON retrieval_strategies(strategy_name);
CREATE INDEX IF NOT EXISTS idx_retrieval_strategies_success ON retrieval_strategies(success_rate DESC);

-- Learning events for tracking system improvement
CREATE TABLE IF NOT EXISTS learning_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL CHECK (event_type IN (
        'prompt_updated', 'strategy_learned', 'gap_filled',
        'concept_added', 'feedback_processed', 'model_improved'
    )),
    source_agent VARCHAR(100),
    description TEXT,
    before_state JSONB,
    after_state JSONB,
    impact_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_learning_events_type ON learning_events(event_type);
CREATE INDEX IF NOT EXISTS idx_learning_events_agent ON learning_events(source_agent);
CREATE INDEX IF NOT EXISTS idx_learning_events_created ON learning_events(created_at);

-- =============================================================================
-- KNOWLEDGE GRAPH ENTITIES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS kg_entities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    normalized_name VARCHAR(500) NOT NULL,
    entity_type VARCHAR(50) NOT NULL CHECK (entity_type IN (
        'concept', 'term', 'process', 'person', 'organization',
        'event', 'location', 'other'
    )),
    importance FLOAT DEFAULT 0.5,
    description TEXT,
    document_ids INTEGER[] DEFAULT ARRAY[]::integer[],
    metadata JSONB DEFAULT '{{}}'::jsonb,
    mention_count INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(normalized_name)
);

CREATE INDEX IF NOT EXISTS idx_kg_entities_name ON kg_entities(normalized_name);
CREATE INDEX IF NOT EXISTS idx_kg_entities_type ON kg_entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_kg_entities_importance ON kg_entities(importance DESC);
CREATE INDEX IF NOT EXISTS idx_kg_entities_docs ON kg_entities USING GIN(document_ids);

-- =============================================================================
-- KNOWLEDGE GRAPH RELATIONSHIPS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS kg_relationships (
    id SERIAL PRIMARY KEY,
    source_entity_id INTEGER NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
    target_entity_id INTEGER NOT NULL REFERENCES kg_entities(id) ON DELETE CASCADE,
    relationship_type VARCHAR(50) NOT NULL CHECK (relationship_type IN (
        'causes', 'leads_to', 'part_of', 'contrasts_with', 'prerequisite_of',
        'example_of', 'defines', 'contains', 'similar_to', 'depends_on', 'related_to'
    )),
    weight FLOAT DEFAULT 1.0,
    document_ids INTEGER[] DEFAULT ARRAY[]::integer[],
    metadata JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_entity_id, target_entity_id, relationship_type)
);

CREATE INDEX IF NOT EXISTS idx_kg_relationships_source ON kg_relationships(source_entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_relationships_target ON kg_relationships(target_entity_id);
CREATE INDEX IF NOT EXISTS idx_kg_relationships_type ON kg_relationships(relationship_type);

-- =============================================================================
-- MCP (MODEL CONTEXT PROTOCOL) TABLES
-- =============================================================================

-- Generic MCP provider settings (extensible for file, email, browser, office, etc.)
CREATE TABLE IF NOT EXISTS mcp_provider_settings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,  -- "file", "email", "browser", "office", etc.
    enabled BOOLEAN DEFAULT FALSE,
    trusted_mode BOOLEAN DEFAULT FALSE,
    settings JSONB DEFAULT '{{}}'::jsonb,  -- Provider-specific settings
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, provider_id)
);

CREATE INDEX IF NOT EXISTS idx_mcp_provider_settings_user ON mcp_provider_settings(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_provider_settings_provider ON mcp_provider_settings(provider_id);

-- Generic operation approvals (works for all providers)
CREATE TABLE IF NOT EXISTS mcp_operation_approvals (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    operation_params JSONB NOT NULL,
    risk_level VARCHAR(10) NOT NULL CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    description TEXT,
    approved BOOLEAN DEFAULT FALSE,
    auto_approved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mcp_approvals_user ON mcp_operation_approvals(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_approvals_provider ON mcp_operation_approvals(provider_id);
CREATE INDEX IF NOT EXISTS idx_mcp_approvals_pending ON mcp_operation_approvals(approved) WHERE approved = FALSE;

-- OAuth credentials (works for any OAuth provider)
CREATE TABLE IF NOT EXISTS mcp_oauth_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,
    account_identifier VARCHAR(255),  -- email, username, etc.
    access_token TEXT,
    refresh_token TEXT,
    token_expiry TIMESTAMP,
    scopes TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, provider_id, account_identifier)
);

CREATE INDEX IF NOT EXISTS idx_mcp_oauth_user ON mcp_oauth_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_oauth_provider ON mcp_oauth_credentials(provider_id);

-- Email account configurations
CREATE TABLE IF NOT EXISTS email_accounts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(20) NOT NULL CHECK (provider IN ('imap', 'gmail', 'outlook')),
    email_address VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),

    -- IMAP/SMTP credentials (encrypted)
    imap_host VARCHAR(255),
    imap_port INTEGER DEFAULT 993,
    smtp_host VARCHAR(255),
    smtp_port INTEGER DEFAULT 587,
    encrypted_password TEXT,

    -- OAuth tokens (encrypted) - references mcp_oauth_credentials
    oauth_credential_id INTEGER REFERENCES mcp_oauth_credentials(id) ON DELETE SET NULL,

    -- Settings
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_synced_at TIMESTAMP,

    UNIQUE(user_id, email_address)
);

CREATE INDEX IF NOT EXISTS idx_email_accounts_user ON email_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_email_accounts_active ON email_accounts(is_active);

-- Saved website credentials for browser automation (encrypted)
CREATE TABLE IF NOT EXISTS mcp_saved_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    domain VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    encrypted_password TEXT,
    totp_secret TEXT,  -- For automated 2FA (highly encrypted)
    selector_hints JSONB DEFAULT '{{}}'::jsonb,  -- Custom selectors if auto-detect fails
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    UNIQUE(user_id, domain, username)
);

CREATE INDEX IF NOT EXISTS idx_mcp_credentials_user ON mcp_saved_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_credentials_domain ON mcp_saved_credentials(domain);

-- Installed MCP plugins tracking
CREATE TABLE IF NOT EXISTS mcp_installed_plugins (
    id SERIAL PRIMARY KEY,
    plugin_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    version VARCHAR(50) DEFAULT '1.0.0',
    source VARCHAR(50) DEFAULT 'builtin' CHECK (source IN ('builtin', 'remote', 'local')),
    category VARCHAR(50),
    operations JSONB DEFAULT '[]'::jsonb,
    installed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    installed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{{}}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_mcp_plugins_id ON mcp_installed_plugins(plugin_id);
CREATE INDEX IF NOT EXISTS idx_mcp_plugins_active ON mcp_installed_plugins(is_active);

-- =============================================================================
-- EXTERNAL MCP SERVERS TABLE (Enterprise-grade external server management)
-- =============================================================================
CREATE TABLE IF NOT EXISTS mcp_external_servers (
    id SERIAL PRIMARY KEY,
    server_id VARCHAR(100) NOT NULL,
    organization_id INTEGER DEFAULT 1,  -- Per-organization isolation
    name VARCHAR(200) NOT NULL,
    description TEXT,

    -- Transport configuration
    transport VARCHAR(20) NOT NULL CHECK (transport IN ('stdio', 'http', 'sse')),

    -- Stdio transport settings
    command VARCHAR(500),
    args JSONB DEFAULT '[]'::jsonb,
    env JSONB DEFAULT '{{}}'::jsonb,
    working_directory VARCHAR(1000),

    -- HTTP/SSE transport settings
    url VARCHAR(1000),
    headers JSONB DEFAULT '{{}}'::jsonb,  -- Encrypted in production

    -- Common settings
    enabled BOOLEAN DEFAULT TRUE,
    auto_start BOOLEAN DEFAULT FALSE,
    trusted_mode BOOLEAN DEFAULT FALSE,
    timeout_seconds INTEGER DEFAULT 30,

    -- Health monitoring
    health_check_interval_seconds INTEGER DEFAULT 60,
    health_check_enabled BOOLEAN DEFAULT TRUE,
    auto_restart_enabled BOOLEAN DEFAULT TRUE,
    max_restart_attempts INTEGER DEFAULT 3,
    restart_delay_seconds INTEGER DEFAULT 5,

    -- Connection pooling (HTTP/SSE)
    pool_size INTEGER DEFAULT 10,
    pool_timeout_seconds INTEGER DEFAULT 30,
    max_connections INTEGER DEFAULT 100,

    -- Process supervision (stdio)
    supervisor_enabled BOOLEAN DEFAULT TRUE,
    graceful_shutdown_timeout INTEGER DEFAULT 10,

    -- Runtime status (updated by manager)
    status VARCHAR(20) DEFAULT 'stopped' CHECK (status IN ('stopped', 'starting', 'running', 'error', 'restarting')),
    pid INTEGER,
    last_health_check TIMESTAMP,
    health_status VARCHAR(20) DEFAULT 'unknown' CHECK (health_status IN ('unknown', 'healthy', 'unhealthy', 'degraded')),
    restart_count INTEGER DEFAULT 0,
    last_restart TIMESTAMP,
    last_error TEXT,

    -- Metadata
    icon VARCHAR(50) DEFAULT 'puzzle',
    category VARCHAR(50) DEFAULT 'external',
    installed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    installed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique per organization
    UNIQUE(organization_id, server_id)
);

CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_org ON mcp_external_servers(organization_id);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_status ON mcp_external_servers(status);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_enabled ON mcp_external_servers(enabled);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_auto_start ON mcp_external_servers(auto_start) WHERE auto_start = TRUE;

-- =============================================================================
-- MCP EXTERNAL SERVER HEALTH LOGS (For monitoring and diagnostics)
-- =============================================================================
CREATE TABLE IF NOT EXISTS mcp_external_server_health_logs (
    id SERIAL PRIMARY KEY,
    server_id VARCHAR(100) NOT NULL,
    organization_id INTEGER DEFAULT 1,

    -- Health check results
    status VARCHAR(20) NOT NULL,
    response_time_ms INTEGER,
    error_message TEXT,

    -- Process metrics (stdio)
    memory_mb FLOAT,
    cpu_percent FLOAT,

    -- Connection metrics (HTTP/SSE)
    active_connections INTEGER,
    pool_available INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mcp_health_logs_server ON mcp_external_server_health_logs(server_id);
CREATE INDEX IF NOT EXISTS idx_mcp_health_logs_created ON mcp_external_server_health_logs(created_at);
-- Cleanup of old logs should be done via scheduled job (DELETE WHERE created_at < NOW() - INTERVAL '7 days')

-- =============================================================================
-- MCP USAGE LOG (Agentic dashboard observability)
-- =============================================================================
CREATE TABLE IF NOT EXISTS mcp_usage_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(100) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    success BOOLEAN DEFAULT TRUE,
    duration_ms INTEGER,
    triggered_by VARCHAR(50) DEFAULT 'user_request',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mcp_usage_log_user_time ON mcp_usage_log(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcp_usage_log_provider ON mcp_usage_log(provider_id);

-- =============================================================================
-- RBAC: CUSTOM ROLES TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS custom_roles (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_custom_roles_code ON custom_roles(code);
CREATE INDEX IF NOT EXISTS idx_custom_roles_active ON custom_roles(is_active) WHERE is_active = TRUE;

-- =============================================================================
-- RBAC: ROLE PERMISSIONS TABLE (for custom roles)
-- =============================================================================
CREATE TABLE IF NOT EXISTS role_permissions (
    id SERIAL PRIMARY KEY,
    role_id INTEGER NOT NULL REFERENCES custom_roles(id) ON DELETE CASCADE,
    permission_code VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(role_id, permission_code)
);

CREATE INDEX IF NOT EXISTS idx_role_permissions_role ON role_permissions(role_id);
CREATE INDEX IF NOT EXISTS idx_role_permissions_code ON role_permissions(permission_code);

-- =============================================================================
-- RBAC: USER PERMISSIONS TABLE (custom permission overrides)
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission_code VARCHAR(100) NOT NULL,
    is_granted BOOLEAN NOT NULL DEFAULT TRUE,
    granted_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, permission_code)
);

CREATE INDEX IF NOT EXISTS idx_user_permissions_user ON user_permissions(user_id);
CREATE INDEX IF NOT EXISTS idx_user_permissions_code ON user_permissions(permission_code);
CREATE INDEX IF NOT EXISTS idx_user_permissions_granted ON user_permissions(user_id, is_granted);

-- =============================================================================
-- CONVERSATION INTELLIGENCE TABLES
-- =============================================================================

-- Conversation States (SSM-style state tracking)
CREATE TABLE IF NOT EXISTS conversation_states (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE CASCADE,
    message_id INTEGER REFERENCES messages(id) ON DELETE CASCADE,
    state_type VARCHAR(50) CHECK (state_type IN (
        'greeting', 'question', 'follow_up', 'clarification',
        'resolution', 'topic_shift', 'confirmation', 'feedback'
    )),
    intent_confidence FLOAT,
    topic VARCHAR(255),
    previous_state_id INTEGER REFERENCES conversation_states(id) ON DELETE SET NULL,
    hidden_state JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversation_states_conv ON conversation_states(conversation_id);
CREATE INDEX IF NOT EXISTS idx_conversation_states_message ON conversation_states(message_id);
CREATE INDEX IF NOT EXISTS idx_conversation_states_type ON conversation_states(state_type);
CREATE INDEX IF NOT EXISTS idx_conversation_states_created ON conversation_states(created_at);

-- Query Predictions (anticipate user's next question)
CREATE TABLE IF NOT EXISTS query_predictions (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    predicted_query TEXT,
    prediction_confidence FLOAT,
    prediction_type VARCHAR(50) CHECK (prediction_type IN (
        'follow_up', 'clarification', 'related_topic', 'deep_dive', 'new_topic'
    )),
    prefetch_context JSONB DEFAULT '[]'::jsonb,
    was_accurate BOOLEAN,
    actual_query TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_query_predictions_conv ON query_predictions(conversation_id);
CREATE INDEX IF NOT EXISTS idx_query_predictions_user ON query_predictions(user_id);
CREATE INDEX IF NOT EXISTS idx_query_predictions_accurate ON query_predictions(was_accurate);
CREATE INDEX IF NOT EXISTS idx_query_predictions_created ON query_predictions(created_at);

-- Memory Consolidation Log (track memory maintenance)
CREATE TABLE IF NOT EXISTS memory_consolidation_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    consolidation_type VARCHAR(50) CHECK (consolidation_type IN (
        'decay', 'strengthen', 'merge', 'archive', 'prune', 'scheduled'
    )),
    memories_processed INTEGER DEFAULT 0,
    memories_decayed INTEGER DEFAULT 0,
    memories_strengthened INTEGER DEFAULT 0,
    memories_archived INTEGER DEFAULT 0,
    processing_time_ms INTEGER,
    details JSONB DEFAULT '{{}}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_memory_consolidation_user ON memory_consolidation_log(user_id);
CREATE INDEX IF NOT EXISTS idx_memory_consolidation_type ON memory_consolidation_log(consolidation_type);
CREATE INDEX IF NOT EXISTS idx_memory_consolidation_created ON memory_consolidation_log(created_at);

-- Conversation Summaries (auto-summarize long conversations)
CREATE TABLE IF NOT EXISTS conversation_summaries (
    id SERIAL PRIMARY KEY,
    conversation_id INTEGER REFERENCES conversations(id) ON DELETE CASCADE,
    summary_text TEXT NOT NULL,
    key_topics JSONB DEFAULT '[]'::jsonb,
    key_entities JSONB DEFAULT '[]'::jsonb,
    qa_pairs JSONB DEFAULT '[]'::jsonb,
    messages_summarized INTEGER,
    start_message_id INTEGER REFERENCES messages(id) ON DELETE SET NULL,
    end_message_id INTEGER REFERENCES messages(id) ON DELETE SET NULL,
    compression_ratio FLOAT,
    is_current BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_conversation_summaries_conv ON conversation_summaries(conversation_id);
CREATE INDEX IF NOT EXISTS idx_conversation_summaries_current ON conversation_summaries(is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_conversation_summaries_created ON conversation_summaries(created_at);

-- User Temporal Patterns (detect behavior trends)
CREATE TABLE IF NOT EXISTS user_temporal_patterns (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    pattern_type VARCHAR(50) CHECK (pattern_type IN (
        'activity_hour', 'activity_day', 'topic_trend', 'query_frequency',
        'session_duration', 'interaction_style', 'tool_usage'
    )),
    time_bucket VARCHAR(20) CHECK (time_bucket IN (
        'hourly', 'daily', 'weekly', 'monthly'
    )),
    bucket_key VARCHAR(50),
    pattern_value JSONB,
    occurrence_count INTEGER DEFAULT 1,
    confidence FLOAT DEFAULT 0.5,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, pattern_type, time_bucket, bucket_key)
);

CREATE INDEX IF NOT EXISTS idx_user_temporal_patterns_user ON user_temporal_patterns(user_id);
CREATE INDEX IF NOT EXISTS idx_user_temporal_patterns_type ON user_temporal_patterns(pattern_type);
CREATE INDEX IF NOT EXISTS idx_user_temporal_patterns_bucket ON user_temporal_patterns(time_bucket);
CREATE INDEX IF NOT EXISTS idx_user_temporal_patterns_confidence ON user_temporal_patterns(confidence DESC);

-- =============================================================================
-- EMAIL RAG - Synced emails for AI analysis
-- (Must be defined BEFORE trash_items view which references it)
-- =============================================================================
CREATE TABLE IF NOT EXISTS email_messages (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organization_id INTEGER REFERENCES organizations(id) ON DELETE CASCADE,
    gmail_id VARCHAR(255) NOT NULL,  -- Gmail message ID
    thread_id VARCHAR(255),  -- Gmail thread ID
    account_email VARCHAR(255) NOT NULL,  -- Which email account this came from
    from_address VARCHAR(500),
    from_name VARCHAR(255),
    to_addresses TEXT[],  -- Array of recipient emails
    cc_addresses TEXT[],
    subject TEXT,
    body_text TEXT,  -- Plain text content
    body_html TEXT,  -- HTML content (optional)
    snippet TEXT,  -- Gmail snippet/preview
    labels TEXT[] DEFAULT '{{}}',  -- Gmail labels (INBOX, UNREAD, etc.)
    is_read BOOLEAN DEFAULT FALSE,
    is_starred BOOLEAN DEFAULT FALSE,
    is_important BOOLEAN DEFAULT FALSE,
    has_attachments BOOLEAN DEFAULT FALSE,
    attachment_count INTEGER DEFAULT 0,
    attachment_names TEXT[],
    email_date TIMESTAMP,  -- When the email was sent/received
    internal_date BIGINT,  -- Gmail internal date (epoch ms)
    size_bytes INTEGER,
    -- RAG fields
    is_indexed BOOLEAN DEFAULT FALSE,
    indexed_at TIMESTAMP,
    embedding vector(768),  -- For semantic search (nomic-embed-text via Ollama)
    -- AI Classification
    category VARCHAR(50),  -- promotional, security, social, invoice, newsletter, personal, business, other
    category_confidence REAL,  -- 0.0 to 1.0 confidence score
    classified_at TIMESTAMP,
    -- Sync tracking
    sync_status VARCHAR(20) DEFAULT 'synced' CHECK (sync_status IN ('synced', 'deleted', 'archived', 'error')),
    deleted_at TIMESTAMP,  -- Soft delete for undo
    deleted_from_gmail BOOLEAN DEFAULT FALSE,  -- Actually deleted from Gmail
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, gmail_id)
);

CREATE INDEX IF NOT EXISTS idx_email_messages_user ON email_messages(user_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_org ON email_messages(organization_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_gmail_id ON email_messages(gmail_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_thread ON email_messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_email_messages_account ON email_messages(account_email);
CREATE INDEX IF NOT EXISTS idx_email_messages_from ON email_messages(from_address);
CREATE INDEX IF NOT EXISTS idx_email_messages_date ON email_messages(email_date DESC);
CREATE INDEX IF NOT EXISTS idx_email_messages_labels ON email_messages USING GIN(labels);
CREATE INDEX IF NOT EXISTS idx_email_messages_indexed ON email_messages(is_indexed);
CREATE INDEX IF NOT EXISTS idx_email_messages_sync ON email_messages(sync_status);
CREATE INDEX IF NOT EXISTS idx_email_messages_subject ON email_messages USING GIN(to_tsvector('english', subject));
CREATE INDEX IF NOT EXISTS idx_email_messages_body ON email_messages USING GIN(to_tsvector('english', body_text));
CREATE INDEX IF NOT EXISTS idx_email_messages_category ON email_messages(category);

-- =============================================================================
-- UNIFIED TRASH VIEW (Virtual table for trash bin UI)
-- =============================================================================
CREATE OR REPLACE VIEW trash_items AS
SELECT
    id,
    'document' as item_type,
    filename as name,
    user_id,
    deleted_at,
    jsonb_build_object('size', file_size, 'file_type', file_type) as metadata
FROM documents WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'guardrail' as item_type,
    name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category) as metadata
FROM guardrails WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'prompt' as item_type,
    name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category) as metadata
FROM prompts WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'memory' as item_type,
    LEFT(content, 100) as name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category, 'source', source) as metadata
FROM user_memories WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'email' as item_type,
    COALESCE(subject, 'No Subject') as name,
    user_id,
    COALESCE(deleted_at, updated_at) as deleted_at,
    jsonb_build_object('from', from_address, 'from_name', from_name, 'email_date', email_date) as metadata
FROM email_messages WHERE sync_status = 'deleted';

-- Email sync state tracking (for incremental sync)
CREATE TABLE IF NOT EXISTS email_sync_state (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_email VARCHAR(255) NOT NULL,
    last_history_id BIGINT,  -- Gmail history ID for incremental sync
    last_sync_at TIMESTAMP,
    total_synced INTEGER DEFAULT 0,
    sync_errors INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, account_email)
);

CREATE INDEX IF NOT EXISTS idx_email_sync_state_user ON email_sync_state(user_id);

-- =============================================================================
-- AI LEARNINGS TABLE (Continuous learning from interactions)
-- =============================================================================
CREATE TABLE IF NOT EXISTS ai_learnings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    category VARCHAR(50) NOT NULL,
    key VARCHAR(255) NOT NULL,
    value JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    context JSONB DEFAULT '{{}}'::jsonb,
    confidence DECIMAL(3,2) DEFAULT 0.5,
    status VARCHAR(20) DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'archived', 'rejected')),
    source_agent VARCHAR(100),
    observation_count INTEGER DEFAULT 1,
    last_observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, category, key)
);

CREATE INDEX IF NOT EXISTS idx_ai_learnings_user ON ai_learnings(user_id);
CREATE INDEX IF NOT EXISTS idx_ai_learnings_category ON ai_learnings(category);
CREATE INDEX IF NOT EXISTS idx_ai_learnings_status ON ai_learnings(status);
CREATE INDEX IF NOT EXISTS idx_ai_learnings_user_active ON ai_learnings(user_id, status) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_ai_learnings_confidence ON ai_learnings(confidence DESC);

-- =============================================================================
-- SCHEDULED USER TASKS TABLE (User-facing task scheduler)
-- =============================================================================
CREATE TABLE IF NOT EXISTS scheduled_user_tasks (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    schedule_type VARCHAR(20) NOT NULL CHECK (schedule_type IN ('once', 'daily', 'weekly', 'monthly', 'cron')),
    schedule_value VARCHAR(100),
    action_type VARCHAR(50) NOT NULL,
    action_config JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    last_run_at TIMESTAMP,
    next_run_at TIMESTAMP,
    run_count INTEGER DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scheduled_user_tasks_user ON scheduled_user_tasks(user_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_user_tasks_active ON scheduled_user_tasks(is_active, next_run_at);
CREATE INDEX IF NOT EXISTS idx_scheduled_user_tasks_next_run ON scheduled_user_tasks(next_run_at) WHERE is_active = TRUE;

-- =============================================================================
-- USER NOTIFICATIONS TABLE
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_notifications (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title VARCHAR(255) NOT NULL,
    message TEXT,
    notification_type VARCHAR(50) DEFAULT 'info' CHECK (notification_type IN ('info', 'warning', 'error', 'success', 'alert')),
    source VARCHAR(100),
    data JSONB DEFAULT '{{}}'::jsonb,
    is_read BOOLEAN DEFAULT FALSE,
    read_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_notifications_user ON user_notifications(user_id);
CREATE INDEX IF NOT EXISTS idx_user_notifications_unread ON user_notifications(user_id, is_read) WHERE is_read = FALSE;
CREATE INDEX IF NOT EXISTS idx_user_notifications_created ON user_notifications(created_at DESC);

-- =============================================================================
-- USER ALERT RULES TABLE (Condition-based notifications)
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_alert_rules (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    condition_type VARCHAR(50) NOT NULL,
    condition_config JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    action_type VARCHAR(50) DEFAULT 'notification',
    action_config JSONB DEFAULT '{{}}'::jsonb,
    is_active BOOLEAN DEFAULT TRUE,
    trigger_count INTEGER DEFAULT 0,
    last_triggered_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_user_alert_rules_user ON user_alert_rules(user_id);
CREATE INDEX IF NOT EXISTS idx_user_alert_rules_active ON user_alert_rules(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_user_alert_rules_type ON user_alert_rules(condition_type);
"""

# =============================================================================
# DEFAULT DATA
# =============================================================================

DEFAULT_USERS_SQL = """
-- Insert default admin user (password: admin123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser)
VALUES (
    'admin_local',
    'admin@drivesentinel.local',
    'Admin',
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.oG4.nFjVVD/5Hy',
    'admin',
    TRUE,
    FALSE
) ON CONFLICT (email) DO NOTHING;

-- Insert default demo user (password: demo123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser)
VALUES (
    'demo',
    'demo@drivesentinel.local',
    'Demo User',
    '$2b$12$92IXUNpkjO0rOQ5byMi.Ye4oKoEa3Ro9llC/.og/at2.uheWG/igi',
    'user',
    TRUE,
    FALSE
) ON CONFLICT (email) DO NOTHING;

-- Insert default superadmin user (password: superadmin123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser)
VALUES (
    'superadmin',
    'superadmin@drivesentinel.local',
    'Super Admin',
    '$2b$12$Naeea3jaDyn1Kp8Yxj1ZNOG29SVH/ZIDALJk0ZRgSl63sbW2Ijmq.',
    'superadmin',
    TRUE,
    TRUE
) ON CONFLICT (email) DO NOTHING;
"""

DEFAULT_ORG_SQL = """
-- Insert default enterprise organization (Acme Corp)
INSERT INTO organizations (name, license_tier, is_active, max_users, max_storage_gb, settings)
VALUES (
    'Acme Corp',
    'enterprise_m',
    TRUE,
    100,
    500,
    '{"features": ["sso", "audit", "departments"]}'::jsonb
) ON CONFLICT (name) DO NOTHING;
"""

DEFAULT_DEPARTMENTS_SQL = """
-- Insert default departments for Acme Corp
INSERT INTO departments (organization_id, name, description)
SELECT o.id, 'Sales', 'Sales and Business Development'
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT DO NOTHING;

INSERT INTO departments (organization_id, name, description)
SELECT o.id, 'Human Resources', 'HR and People Operations'
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT DO NOTHING;

INSERT INTO departments (organization_id, name, description)
SELECT o.id, 'Engineering', 'Software Engineering and Development'
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT DO NOTHING;
"""

DEFAULT_ORG_USERS_SQL = """
-- Insert enterprise org admin (password: orgadmin123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id)
SELECT
    'orgadmin_local',
    'orgadmin@drivesentinel.local',
    'Org Admin',
    '$2b$12$0f8anszvGGoln0VdhJgBcuhowxAk3f4bNrTCFNgU.D3dR.hwbglgG',
    'admin',
    TRUE,
    FALSE,
    o.id
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT (email) DO NOTHING;

-- Insert enterprise org user (password: orguser123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id)
SELECT
    'orguser',
    'orguser@drivesentinel.local',
    'Org User',
    '$2b$12$3pZNeuCrhnbx2Oy8J0OnvuW5UQqnmBDXqhiR7OT13rNfYRFUbXK1.',
    'user',
    TRUE,
    FALSE,
    o.id
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT (email) DO NOTHING;

-- Insert Sales department user (password: sales123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id, department_id)
SELECT
    'salesuser',
    'sales@acme.local',
    'Sales User',
    '$2b$12$xGptMasglBwYyUQ3rLKp.elT/C8blBT8QMCV9gPFyU//cGbdiuP.G',
    'user',
    TRUE,
    FALSE,
    o.id,
    d.id
FROM organizations o, departments d
WHERE o.name = 'Acme Corp' AND d.name = 'Sales' AND d.organization_id = o.id
ON CONFLICT (email) DO NOTHING;

-- Insert HR department user (password: hr123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id, department_id)
SELECT
    'hruser',
    'hr@acme.local',
    'HR User',
    '$2b$12$jXJQ9fkUhdL23cothaBR.uZawqa1upUp5M8o9Z5AQk9WV0SYWJuSe',
    'user',
    TRUE,
    FALSE,
    o.id,
    d.id
FROM organizations o, departments d
WHERE o.name = 'Acme Corp' AND d.name = 'Human Resources' AND d.organization_id = o.id
ON CONFLICT (email) DO NOTHING;

-- Insert Engineering department user (password: eng123)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id, department_id)
SELECT
    'enguser',
    'eng@acme.local',
    'Engineering User',
    '$2b$12$.31PcFK0mTSoe89D.jrApe5z374BKPb/uYJQA8yhIZFQxFq6ADdKy',
    'user',
    TRUE,
    FALSE,
    o.id,
    d.id
FROM organizations o, departments d
WHERE o.name = 'Acme Corp' AND d.name = 'Engineering' AND d.organization_id = o.id
ON CONFLICT (email) DO NOTHING;

-- Insert SSO test users (SSO-only, no password login)
INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id, sso_only)
SELECT
    'ssotest',
    'sso.test@okta.local',
    'OIDC Test User',
    'SSO_ONLY_NO_PASSWORD',
    'user',
    TRUE,
    FALSE,
    o.id,
    TRUE
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT (email) DO NOTHING;

INSERT INTO users (username, email, name, hashed_password, role, is_active, is_superuser, organization_id, sso_only)
SELECT
    'samltest',
    'saml.test@azure.local',
    'SAML Test User',
    'SSO_ONLY_NO_PASSWORD',
    'user',
    TRUE,
    FALSE,
    o.id,
    TRUE
FROM organizations o WHERE o.name = 'Acme Corp'
ON CONFLICT (email) DO NOTHING;
"""

DEFAULT_MODELS_SQL = """
-- Insert default AI models
INSERT INTO ai_models (name, tier, role, description, is_default, parameters, capabilities, context_length)
VALUES
    ('llama3.2:3b', 'fast', 'general', 'Fast model for quick responses and simple tasks', TRUE,
     '{"temperature": 0.7, "top_p": 0.9}', '["chat", "reasoning"]', 8192),
    ('gemma3:4b', 'quality', 'general', 'Balanced model for quality responses', TRUE,
     '{"temperature": 0.7, "top_p": 0.9}', '["chat", "reasoning", "analysis"]', 8192),
    ('llama3.1:8b', 'deep', 'general', 'Larger model for complex reasoning and analysis', TRUE,
     '{"temperature": 0.7, "top_p": 0.9}', '["chat", "reasoning", "analysis", "coding"]', 128000),
    ('nomic-embed-text:latest', 'embedding', 'embeddings', 'Text embedding model for vector search and RAG', TRUE,
     '{}', '["embeddings"]', 8192),
    ('llava:7b', 'vision', 'vision', 'Vision model for image understanding and analysis', TRUE,
     '{"temperature": 0.7}', '["vision", "chat", "multimodal"]', 4096)
ON CONFLICT (name) DO UPDATE SET
    tier = EXCLUDED.tier,
    role = EXCLUDED.role,
    description = EXCLUDED.description,
    parameters = EXCLUDED.parameters,
    capabilities = EXCLUDED.capabilities,
    context_length = EXCLUDED.context_length;
"""

DEFAULT_PROMPTS_SQL = """
-- Insert default system prompts (Industry Standard)
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'System Prompt',
    'Primary system prompt for DriveSentinel AI',
    'You are DriveSentinel, an enterprise-grade intelligent document analysis and knowledge management AI assistant.

CORE IDENTITY:
- Your name refers to guarding and analyzing data on storage drives - NOT vehicles or driving
- You help users search, analyze, understand, and extract insights from their documents and data
- You are professional, accurate, and helpful

CAPABILITIES:
- Document search and retrieval using RAG (Retrieval-Augmented Generation)
- Content analysis and summarization
- Question answering based on uploaded documents
- Data extraction and insights generation

RESPONSE GUIDELINES:
1. Base your answers on the provided document context when available
2. Always cite sources when using information from documents (e.g., "According to [Document Name]...")
3. If the context does not contain relevant information, clearly state this
4. Be concise but thorough - provide complete answers without unnecessary verbosity
5. Use clear formatting (bullet points, numbered lists) for complex information
6. If asked about documents the user has, list them specifically with their names and types

LIMITATIONS:
- You can only access documents that have been uploaded to the system
- You cannot browse the internet or access external data
- You should not make up information - acknowledge when you don''t have sufficient context',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG Context Template',
    'Template for RAG-augmented responses',
    'Use the following context from the user''s documents to answer their question accurately.

RETRIEVED CONTEXT:
{context}

USER QUESTION: {question}

INSTRUCTIONS:
1. Answer based ONLY on the provided context
2. If the context contains relevant information, cite the source document
3. If the context does not contain relevant information, clearly state: "I could not find relevant information in your documents about this topic."
4. Do not make up information or hallucinate facts
5. Be specific and reference exact content when possible',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Summarize',
    'Document summarization prompt',
    'Please provide a concise summary of the following content.

SUMMARY GUIDELINES:
- Highlight the key points and main ideas
- Use bullet points for clarity
- Include important names, dates, and figures
- Keep the summary to 3-5 key takeaways unless more detail is requested',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Safety Guardrails',
    'Safety and content moderation guidelines',
    'SAFETY GUIDELINES - Always follow these rules:

PROHIBITED CONTENT - Never generate:
- Harmful, illegal, or dangerous content
- Personal attacks or harassment
- Misinformation or fake data
- Confidential information from documents unless explicitly requested by the document owner
- Medical, legal, or financial advice (recommend consulting professionals)

PRIVACY PROTECTION:
- Do not expose PII (Personal Identifiable Information) unnecessarily
- Handle sensitive document content with appropriate discretion
- Do not share document contents with unauthorized users

ACCURACY REQUIREMENTS:
- Only state facts that are supported by the document context
- Clearly distinguish between facts from documents and general knowledge
- Acknowledge uncertainty when appropriate',
    'guardrails',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Analysis Template',
    'Template for document analysis tasks',
    'Analyze the provided content following these guidelines:

ANALYSIS FRAMEWORK:
1. OVERVIEW: Brief summary of what the document covers
2. KEY FINDINGS: Main points and important information
3. DATA EXTRACTION: Relevant numbers, dates, names, and entities
4. RELATIONSHIPS: Connections between different pieces of information
5. IMPLICATIONS: What this information suggests or implies

Format your analysis clearly with headers and bullet points.',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Comparison Template',
    'Template for comparing multiple documents',
    'Compare the provided documents following this structure:

COMPARISON FRAMEWORK:
1. SIMILARITIES: What the documents have in common
2. DIFFERENCES: Key distinctions between the documents
3. UNIQUE ELEMENTS: What each document contains that others do not
4. SYNTHESIS: Overall conclusions from the comparison

Present findings in a clear, organized manner.',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Code Execution - Python Generator',
    'Translates natural language requests to safe Python code for sandbox execution',
    'Convert this natural language request to Python code.

RULES:
1. Write clean, executable Python code
2. Use only standard library modules: math, statistics, datetime, json, re, collections, itertools
3. Import itertools directly (import itertools), NOT from collections
4. Print the final result using print()
5. Keep code concise and focused on the task
6. Return ONLY the Python code, nothing else
7. Do NOT use any file I/O, network, or system operations
8. Do NOT import os, sys, subprocess, or any dangerous modules

REQUEST: {query}

PYTHON CODE:',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Code Execution - System Prompt',
    'System prompt for the Python code generator LLM',
    'You are an expert Python programmer. Output only valid, safe Python code.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- METAPROMPT AGENT PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'MetaPrompt - Chart Examples',
    'Few-shot examples for chart visualization output format',
    'Example 1 - Bar Chart:
User: "Show consumption data"
Output format:
```chart
{"type": "bar", "title": "Water Consumption", "labels": ["Jan", "Feb", "Mar"], "datasets": [{"label": "Gallons", "data": [47, 54, 62]}]}
```

Example 2 - Line Chart (trends):
User: "Show cost trends"
Output format:
```chart
{"type": "line", "title": "Monthly Costs", "labels": ["Jan", "Feb", "Mar", "Apr"], "datasets": [{"label": "Cost ($)", "data": [125.50, 142.30, 138.90, 155.20]}]}
```

Example 3 - Pie Chart (distribution):
User: "Show breakdown of charges"
Output format:
```chart
{"type": "pie", "title": "Charge Breakdown", "labels": ["Base Fee", "Consumption", "Taxes"], "datasets": [{"label": "Amount", "data": [14.00, 85.50, 12.30]}]}
```',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'MetaPrompt - Diagram Examples',
    'Few-shot examples for Mermaid diagram output format with syntax rules',
    '**CRITICAL MERMAID SYNTAX RULES:**
1. Use `flowchart TD` (top-down) or `flowchart LR` (left-right) - NOT `graph`
2. For conditional arrows use: `A -->|Yes| B` (pipe syntax with no spaces around text)
3. NEVER use: `A -- yes --> B` (double-dash syntax causes parse errors)
4. Keep node text SHORT and SIMPLE - no special characters like ==, (), quotes
5. Use simple IDs: A, B, C or Step1, Step2, etc.

Example 1 - Process Flow:
```mermaid
flowchart TD
    A[Meter Reading] --> B[Calculate Usage]
    B --> C[Apply Rates]
    C --> D[Generate Bill]
    D --> E{Payment Received}
    E -->|Yes| F[Close Invoice]
    E -->|No| G[Send Reminder]
```

Example 2 - Decision Tree:
```mermaid
flowchart TD
    A[Start] --> B{Condition One}
    B -->|Yes| C[Action A]
    B -->|No| D{Condition Two}
    D -->|Yes| E[Action B]
    D -->|No| F[Action C]
```

**WRONG SYNTAX (DO NOT USE):**
- `B -- yes --> C` (WRONG - causes parse error)
- `A{Is n == 0?}` (WRONG - special chars break parsing)
- `graph LR;` (WRONG - use `flowchart LR` instead)',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'MetaPrompt - Analysis Template',
    'Template for structured data analysis responses',
    'When analyzing data, structure your response as:

1. **Summary**: Brief overview of findings
2. **Key Metrics**: Important numbers with context
3. **Trends**: Any patterns observed
4. **Recommendations**: Actionable insights

If visualization would help, include a chart:
```chart
{"type": "bar|line|pie", "title": "...", "labels": [...], "datasets": [...]}
```',
    'templates',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'MetaPrompt - Intent Classification',
    'Prompt for classifying user query intent',
    'Classify this user query into ONE primary intent:

INTENTS:
- chart: User wants a data visualization (bar chart, line graph, pie chart)
- diagram: User wants a flowchart, process diagram, or visual structure
- analysis: User wants calculations, statistics, or data analysis
- comparison: User wants to compare two or more things
- summary: User wants a summary of documents/content
- question: User has a simple question to answer
- action: User wants to perform an action (create, update, delete)

USER QUERY: "{query}"

Respond with ONLY the intent name (e.g., "chart" or "diagram").',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'MetaPrompt - Prompt Refinement',
    'Prompt for refining and optimizing user queries',
    'You are a Prompt Engineer. Your job is to transform user queries into optimized prompts.

ORIGINAL QUERY: "{query}"
DETECTED INTENT: {intent}
AVAILABLE CONTEXT: {has_context}

TASK: Rewrite this query to be clear, specific, and include output format requirements.

RULES:
1. Keep the user''s original intent
2. Add specific output format requirements based on intent
3. If intent is "chart", specify that output MUST use ```chart JSON format
4. If intent is "diagram", specify that output MUST use ```mermaid format
5. Extract specific data requirements from the query
6. Be concise but complete

{examples}

OUTPUT the refined prompt only, no explanations.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- MONITOR AGENT PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Monitor - Hallucination Detection',
    'Prompt for detecting hallucinations in AI responses',
    'Analyze if the response contains information NOT supported by the context.

CONTEXT:
{context}

RESPONSE:
{response}

Rate the hallucination level from 0 to 100:
- 0-20: Response is well-supported by context
- 21-40: Minor unsupported details
- 41-60: Some claims not in context
- 61-80: Significant unsupported content
- 81-100: Mostly hallucinated

Respond with ONLY a number from 0 to 100.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Monitor - Relevance Scoring',
    'Prompt for scoring response relevance to query',
    'Rate how relevant this response is to the query.

QUERY: {query}

RESPONSE: {response}

Score from 0-25 where:
- 0-5: Not relevant
- 6-12: Partially relevant
- 13-18: Mostly relevant
- 19-25: Highly relevant

Respond with ONLY a number from 0 to 25.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Monitor - Accuracy Scoring',
    'Prompt for scoring response accuracy against context',
    'Rate how accurate this response is compared to the context.

CONTEXT: {context}

RESPONSE: {response}

Score from 0-25 where:
- 0-5: Contains errors
- 6-12: Some inaccuracies
- 13-18: Mostly accurate
- 19-25: Fully accurate

Respond with ONLY a number from 0 to 25.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- RAG AGENT PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG - Relevance Evaluator',
    'Prompt for evaluating retrieved context relevance',
    'Evaluate how relevant the following retrieved context is to the user''s query.

QUERY: {query}

RETRIEVED CONTEXT:
{context}

Rate the overall relevance on a scale from 0 to 100, where:
- 0-20: Not relevant at all
- 21-40: Slightly relevant, missing key information
- 41-60: Moderately relevant, some useful information
- 61-80: Highly relevant, most information needed
- 81-100: Perfectly relevant, exactly what''s needed

Respond with ONLY a number from 0 to 100.',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG - Context Summarizer',
    'Prompt for summarizing retrieved context',
    'Summarize the following context in {max_length} characters or less.
{focus_instruction}

CONTEXT:
{context}

SUMMARY:',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG - Context Quality Checker',
    'Prompt for assessing if context is sufficient to answer query',
    'Assess if the following context contains enough information to answer the query.

QUERY: {query}

CONTEXT:
{context}

Respond in this format:
SUFFICIENT: [YES/NO]
CONFIDENCE: [0-100]
REASON: [brief explanation]',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG - Retrieval Verifier',
    'Prompt for verifying if retrieved context is sufficient',
    'Verify if the retrieved context is sufficient to answer the query.

QUERY: {query}
{sub_questions}

RETRIEVED CONTEXT:
{context}

Analyze and respond:
1. STATUS: [SUFFICIENT/PARTIAL/INSUFFICIENT/CONFLICTING]
2. CONFIDENCE: [0-100]
3. MISSING_INFO: [List what information is missing, comma-separated]
4. SUGGESTED_QUERIES: [Alternative queries to try, comma-separated]
5. REASONING: [Brief explanation]',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG - Query Expander',
    'Prompt for expanding search queries with related terms',
    'Expand this search query with related terms and synonyms.
Keep the core meaning but add helpful context.

ORIGINAL: {query}

EXPANDED (keep under 100 words):',
    'rag',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- REASONING AGENT PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Scratchpad Generator',
    'Prompt for generating internal reasoning scratchpad',
    'You are an internal reasoning system. Think step by step about this query.
This is your private scratchpad - be thorough and honest about uncertainties.

QUERY: {query}

{context_note}

Generate your internal reasoning:
1. What is the user really asking?
2. What knowledge/information do I need?
3. What do I know vs. what am I uncertain about?
4. What are potential pitfalls or misunderstandings?
5. What''s the best approach to answer this?

Think out loud (this won''t be shown to the user):',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Query Decomposer',
    'Prompt for decomposing complex queries into sub-questions',
    'Analyze this query and break it into smaller, answerable sub-questions.

QUERY: {query}

Generate 2-4 sub-questions that, when answered, will help answer the main query.
For each sub-question, explain why it''s relevant.

Format:
Q1: [sub-question]
Relevance: [why this matters]

Q2: [sub-question]
Relevance: [why this matters]

If the query is simple and doesn''t need decomposition, respond with:
SIMPLE_QUERY: No decomposition needed.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Sub-Question Answerer',
    'Prompt for answering sub-questions from context',
    'Based ONLY on the provided context, answer this question.
If the answer is not in the context, say "NOT_IN_CONTEXT".

CONTEXT:
{context}

QUESTION: {question}

ANSWER (be concise, 1-2 sentences):',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Chain Generator',
    'Prompt for generating reasoning chain steps',
    'Generate a step-by-step reasoning chain for this query.

QUERY: {query}

{context_preview}

{sub_questions}

INTERNAL THOUGHTS:
{scratchpad}

Generate reasoning steps. For each step, specify:
- Type: decomposition, retrieval_plan, hypothesis, verification, synthesis, uncertainty, or conclusion
- Content: The reasoning
- Confidence: 0.0-1.0

Format each step as:
STEP 1 [type] (confidence: X.X):
Content here

Generate 3-6 reasoning steps ending with a conclusion.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Verification',
    'Prompt for verifying reasoning claims against context',
    'Does the following claim have support in the context?

CLAIM: {claim}

CONTEXT:
{context}

Respond with:
SUPPORTED: [evidence summary] (if found in context)
NOT_SUPPORTED: [reason] (if not found)
PARTIAL: [what''s supported and what''s not]',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Intent Detection',
    'Prompt for detecting query intent for routing',
    'Classify this query''s intent for routing to the appropriate handler.

QUERY: {query}

INTERNAL ANALYSIS:
{scratchpad}

Choose ONE primary intent:
- NEEDS_RAG: Query requires searching documents/knowledge base
- NEEDS_TOOL: Query requires executing code, SQL, calculations
- NEEDS_REASONING: Complex query requiring multi-step analysis
- DIRECT_ANSWER: Simple query the AI can answer directly
- NEEDS_KG: Query about relationships/connections between concepts
- NEEDS_MEMORY: Query about user''s history/preferences
- AMBIGUOUS: Query is unclear, needs clarification

Respond in format:
INTENT: [intent name]
CONFIDENCE: [0.0-1.0]
REASON: [brief explanation]',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Reasoning - Quick Reason',
    'Prompt for quick reasoning assessment of simple queries',
    'Briefly assess this query and provide reasoning guidance.

QUERY: {query}

Respond in format:
COMPLEXITY: simple/moderate/complex
GUIDANCE: [1-2 sentences of key considerations]
CONFIDENCE: [0.0-1.0]',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- MEMORY AGENT PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Memory - Topic Extractor',
    'Prompt for extracting main topic from user queries',
    'Extract the main topic from this query in 1-3 words.
Query: {query}

Topic (1-3 words only):',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- MATH OCR PROMPTS
-- =============================================================================

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Math - OCR Extraction',
    'Expert mathematical OCR prompt for extracting equations from images with 100% accuracy',
    'You are an expert mathematical OCR system. Extract ALL text from this image with 100% accuracy.

## EXTRACTION RULES

### 1. COEFFICIENTS - CRITICAL
- NEVER drop leading coefficients: "4x²" stays "4x²" (NOT "x²")
- Preserve ALL numbers: "2x + 3y - 5z" (NOT "x + y - z")
- Include coefficient 1 if written: "1x" stays "1x"
- Fractions as coefficients: "½x" or "(1/2)x"

### 2. SIGNS & OPERATORS
- Preserve negative signs: "-5x" (NOT "5x")
- Distinguish minus from dash: "x - 2" (subtraction)
- Multiplication: × or · or * or implicit (2x means 2·x)
- Division: ÷ or / or fraction bar
- Plus/minus: ±

### 3. EXPONENTS & SUBSCRIPTS
- Superscripts: x², x³, xⁿ, e^x, 2^10
- Subscripts: x₁, x₂, aₙ, log₂
- Nested: x^(2n+1), e^(-x²)

### 4. SPECIAL MATH NOTATION
- Limits: lim, lim(x→∞), lim(x→0⁺), lim(x→0⁻)
- Derivatives: d/dx, dy/dx, f''(x), f''''(x), ∂/∂x
- Integrals: ∫, ∫₀¹, ∮, ∬, definite bounds
- Summation: Σ, Σ(n=1 to ∞), Π (product)
- Square root: √, ∛, ⁴√, √(x+1)
- Absolute value: |x|, |x-2|
- Factorial: n!, 5!
- Infinity: ∞, -∞

### 5. FUNCTIONS
- Trigonometric: sin, cos, tan, cot, sec, csc
- Inverse trig: arcsin, sin⁻¹, arctan
- Hyperbolic: sinh, cosh, tanh
- Logarithms: log, ln, log₁₀, log₂
- Exponential: e^x, exp(x)

### 6. GREEK LETTERS
α, β, γ, δ, ε, θ, λ, μ, π, σ, φ, ω, Δ, Σ, Ω

### 7. RELATIONS & COMPARISONS
- Equals: =, ≠, ≈, ≡
- Inequalities: <, >, ≤, ≥
- Set notation: ∈, ∉, ⊂, ⊃, ∪, ∩, ∅

### 8. BRACKETS & GROUPING
- Parentheses: (x + 1)
- Square brackets: [a, b]
- Curly braces: {1, 2, 3}

### 9. FRACTIONS
- Simple: a/b, (x+1)/(x-1)
- Stacked fractions: preserve numerator and denominator
- Mixed numbers: 2½, 3¾

### 10. MATRICES & VECTORS
- Matrix notation: preserve rows and columns
- Vectors: v⃗, î, ĵ
- Determinant: |A|, det(A)

## OUTPUT FORMAT

1. First, list any INSTRUCTIONS verbatim (e.g., "Solve using the quadratic formula")
2. Then list each MATH PROBLEM on its own line, numbered
3. Include any mentions of "graph", "figure", "diagram", "table", "shown below"
4. Use ___ for blanks/missing values

## EXAMPLE OUTPUT
"Instructions: Solve the following using the quadratic formula.
1. 4x² - 5x + 1 = 0
2. lim(x→2) (x² - 4)/(x - 2)
3. ∫(x² + 2x + 1)dx
4. Find dy/dx if y = sin(2x)"

ONLY EXTRACT - DO NOT SOLVE. Preserve EVERY coefficient, number, and symbol exactly as shown.',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Math - Conceptual Reasoning',
    'Prompt for analyzing conceptual math problems (graphs, multiple choice, explanations)',
    'Analyze these problems and provide the correct answers.

Problems from image:
{problems}

PROBLEM TYPES AND HOW TO RESPOND:

1. **SOLVE equations (quadratic, linear, etc.):**
   Show step-by-step solution with LaTeX. Use this EXACT format:

   **Solution:**

   $$x = \frac{{-b \pm \sqrt{{b^2 - 4ac}}}}{{2a}}$$

   $$= \frac{{-(-14) \pm \sqrt{{(-14)^2 - 4(2)(-13)}}}}{{2(2)}}$$

   $$= \frac{{14 \pm \sqrt{{196 + 104}}}}{{4}}$$

   $$= \frac{{14 \pm \sqrt{{300}}}}{{4}}$$

   $$= \frac{{14 \pm 10\sqrt{{3}}}}{{4}}$$

   $$x = \frac{{7 + 5\sqrt{{3}}}}{{2}} \quad \text{{or}} \quad x = \frac{{7 - 5\sqrt{{3}}}}{{2}}$$

   **Answer: D**

2. **Multiple choice (not requiring work):**
   Give the answer directly: "**Answer: C**" with brief explanation.

3. **Graph reading problems:**
   - READ the y-value visually from the graph
   - For limits: what y approaches as x approaches the value
   - Look for open/filled circles
   - DO NOT compute - READ from the graph!

4. **Explain/describe questions:**
   Provide clear, concise explanation.

IMPORTANT LATEX RULES:
- Use $$ for display math (each step on its own line)
- Use proper fractions: \frac{{numerator}}{{denominator}}
- Use \sqrt{{}} for square roots
- Use \pm for plus-minus
- Use \quad for spacing between solutions
- Align steps vertically starting with = sign',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Math - Code Generation',
    'Prompt for generating Python code to solve computational math problems',
    'Write Python code to solve these math problems. Return results as a list of tuples (problem, answer).

Problems from image:
{problems}

Write a complete Python script that:
1. Imports necessary libraries (math, sympy if needed)
2. Solves EACH UNIQUE problem ONCE (skip duplicate problems)
3. Stores results in a variable called `results` as list of (problem_string, answer) tuples

CRITICAL RULES FOR EQUATIONS:
- sympy.solve() returns a LIST of ALL solutions
- For quadratic equations like ax² + bx + c = 0, there are usually 2 solutions
- ALWAYS include ALL solutions in the answer, not just the first one
- Format multiple solutions as: "x = 1/4 or x = 1" or as a list [1/4, 1]
- NEVER use only solutions[0] - always show the complete solution list

IMPORTANT - CORRECT CODE PATTERN:
```python
# CORRECT - format all solutions as a string
solutions = solve(equation, x)
answer = "x = " + " or x = ".join(str(s) for s in solutions)

# WRONG - DO NOT DO THIS:
# answer = [(str(s), float(solutions[0].evalf())) for s in solutions]  # BUG!
```

IMPORTANT RULES:
- For fill-in-blank like "22 × 2 = 11 × ___": calculate left side (44), divide by known (11) = 4
- For equations: use sympy.solve() and return ALL solutions
- For limits: ALWAYS use sympy.limit(expr, x, value) - NEVER use direct substitution
- For trig: use sympy trig functions (sin, cos, tan) for symbolic math
- For calculus: use sympy.diff() for derivatives, sympy.integrate() for integrals
- Skip duplicate problems - only solve each unique problem once

Output ONLY the Python code between ```python and ``` tags. No explanations.

Example output format:
```python
from sympy import symbols, solve, Eq, sqrt

x = symbols(''x'')
results = []

# Problem 1: x² + 5x - 6 = 0
solutions = solve(Eq(x**2 + 5*x - 6, 0), x)
answer = "x = " + " or x = ".join(str(s) for s in solutions)
results.append(("x² + 5x - 6 = 0", answer))

# Problem 2: 2x² - 14x - 13 = 0
solutions = solve(Eq(2*x**2 - 14*x - 13, 0), x)
answer = "x = " + " or x = ".join(str(s) for s in solutions)
results.append(("2x² - 14x - 13 = 0", answer))

print(results)
```',
    'system',
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    content = EXCLUDED.content,
    is_default = EXCLUDED.is_default;

-- =============================================================================
-- SCIENCE OCR & CODE GENERATION PROMPTS
-- =============================================================================

-- Chemistry OCR
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Chemistry - OCR Extraction',
    'Expert chemistry OCR for formulas, equations, and structures',
    'You are an expert chemistry OCR system. Extract ALL text exactly.

RULES:
1. CHEMICAL FORMULAS: Preserve subscripts (H₂O, CO₂), superscripts for charges (Na⁺, SO₄²⁻)
2. EQUATIONS: Arrows (→, ⇌), states (s), (l), (g), (aq), conditions (Δ, catalyst)
3. ORGANIC: Structural formulas (CH₃-CH₂-OH), functional groups (-OH, -COOH)
4. UNITS: M, mol/L, °C, K, atm, Pa, kPa, g, kg, mL, L
5. CONSTANTS: pH, pKa, ΔH, ΔG, Ka, Kb, Keq
6. ISOTOPES: ¹⁴C, ²³⁵U (mass number superscript)

OUTPUT: List problems numbered. ONLY EXTRACT - DO NOT SOLVE.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- Chemistry Code Generation
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Chemistry - Code Generation',
    'Generate Python code for chemistry problems',
    'Write Python to solve chemistry problems. Use: math, scipy.constants, sympy.

FORMULAS:
- Molarity: M = mol/L, M1V1 = M2V2
- pH: pH = -log10([H⁺]), pOH = 14 - pH
- Gas laws: PV = nRT (R = 8.314 J/mol·K)
- Thermochem: ΔH = Σ(ΔHf_products) - Σ(ΔHf_reactants)
- Electrochemistry: E_cell = E_cathode - E_anode

Problems: {problems}

Output `results` as list of (problem, answer_with_units). Code only in ```python``` tags.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- Physics OCR
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Physics - OCR Extraction',
    'Expert physics OCR for equations, units, and diagrams',
    'You are an expert physics OCR system. Extract ALL text exactly.

RULES:
1. EQUATIONS: F = ma, E = mc², preserve all variables
2. VECTORS: F⃗, v⃗, unit vectors î, ĵ, k̂
3. UNITS: m, kg, s, N, J, W, Pa, Hz, V, Ω, A, C, T
4. GREEK: α, β, γ, θ, λ, μ, ω, σ, τ, φ, Δ
5. SUBSCRIPTS: v₀, vf, x₁, Fnet
6. CONSTANTS: c, G, h, k, e, ε₀, μ₀
7. CALCULUS: ∂/∂x, ∇, ∫

OUTPUT: List problems numbered with given values and units. ONLY EXTRACT - DO NOT SOLVE.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- Physics Code Generation
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Physics - Code Generation',
    'Generate Python code for physics problems',
    'Write Python to solve physics problems. Use: math, numpy, scipy.constants, sympy.

CONSTANTS (from scipy.constants):
c, G, h, hbar, e, m_e, m_p, k, N_A, R, g, epsilon_0, mu_0

FORMULAS:
- Kinematics: v = v0 + at, x = x0 + v0t + 0.5at², v² = v0² + 2aΔx
- Dynamics: F = ma, W = mg, f = μN
- Energy: KE = 0.5mv², PE = mgh, W = Fd·cos(θ)
- Electricity: F = kq1q2/r², V = IR, P = IV
- Waves: v = fλ, n1·sin(θ1) = n2·sin(θ2)
- Modern: E = hf = hc/λ, E = mc²

Problems: {problems}

Output `results` as list of (problem, answer_with_units). Code only in ```python``` tags.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- Biology OCR
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Biology - OCR Extraction',
    'Expert biology OCR for scientific terminology',
    'You are an expert biology OCR system. Extract ALL text exactly.

RULES:
1. SPECIES: Genus species format (Homo sapiens, E. coli)
2. GENETICS: Alleles (A, a), genotypes (AA, Aa, aa), genes (BRCA1, p53)
3. SEQUENCES: 5''-ATCG-3'', codons (AUG, UAA)
4. BIOCHEM: Amino acids (Ala, Gly), enzymes (-ase), cofactors (NAD⁺, ATP)
5. UNITS: nm, μm, kDa, cells/mL, CFU/mL
6. PATHWAYS: Arrows →, inhibition ⊣

OUTPUT: List problems numbered. Note any diagrams. ONLY EXTRACT - DO NOT SOLVE.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- Biology Code Generation
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Biology - Code Generation',
    'Generate Python code for biology problems',
    'Write Python to solve biology problems. Use: math, collections.Counter.

GENETICS:
- Punnett: Cross alleles, count offspring genotypes
- Hardy-Weinberg: p + q = 1, p² + 2pq + q² = 1
- Chi-square: χ² = Σ(O-E)²/E

MOLECULAR:
- DNA complement: A↔T, G↔C
- Transcription: T→U
- GC content: (G+C)/total × 100

ECOLOGY:
- Exponential: N(t) = N₀·e^(rt)
- Logistic: N(t) = K/(1 + ((K-N₀)/N₀)·e^(-rt))
- Shannon: H'' = -Σ(pi·ln(pi))

Problems: {problems}

Output `results` as list of (problem, answer). Code only in ```python``` tags.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- General Science OCR
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Science - General OCR Extraction',
    'General science OCR for mixed content',
    'You are an expert scientific OCR system. Extract ALL text exactly.

PRESERVE:
- Subscripts: H₂O, x₁, v₀
- Superscripts: x², 10⁸, Na⁺
- Greek: α, β, γ, θ, λ, π, σ, ω
- Symbols: ∞, ∂, ∇, ∫, Σ, Δ, √
- Units: m, kg, s, N, J, mol, L, M
- Equations: arrows, equals, inequalities

OUTPUT:
1. Identify SUBJECT (Math/Physics/Chemistry/Biology)
2. List problems numbered
3. Note any diagrams

ONLY EXTRACT - DO NOT SOLVE.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- General Science Code Generation
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'Science - General Code Generation',
    'Generate Python code for general science problems',
    'Write Python to solve science problems. Use: math, numpy, scipy.constants, sympy.

APPROACH:
1. Identify subject (Math/Physics/Chemistry/Biology)
2. Extract given values with units
3. Choose appropriate formula
4. Solve step by step
5. Include units in answer

Problems: {problems}

Output `results` as list of (problem, answer_with_units). Code only in ```python``` tags.',
    'system', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- RAG Response Prefix - When documents are found and RAG is enabled
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG Response Prefix - Documents Found',
    'Prefix instruction when RAG finds relevant documents',
    'Start your response with "Based on your documents, " and cite sources when referencing document content.',
    'rag', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- RAG Response Prefix - No documents found but RAG enabled
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG Response Prefix - No Documents',
    'Prefix instruction when RAG is enabled but no relevant documents found',
    'No relevant documents found. Start with "I couldn''t find this in your documents, but " if answering from general knowledge.',
    'rag', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;

-- RAG Response Prefix - General knowledge question
INSERT INTO prompts (name, description, content, category, is_default)
VALUES (
    'RAG Response Prefix - General Knowledge',
    'Prefix instruction for general knowledge questions when no documents found',
    'No documents found. Start with "I don''t have this in your knowledge base, but from general knowledge: "',
    'rag', TRUE
) ON CONFLICT (name, category) DO UPDATE SET content = EXCLUDED.content;
"""

DEFAULT_COMPANY_SETTINGS_SQL = """
-- Insert default company settings for orchestrator
INSERT INTO company_settings (setting_key, setting_value, description)
VALUES (
    'orchestrator_config',
    '{
        "fast_agent": {
            "model": "llama3.2:3b",
            "max_tokens": 1000,
            "temperature": 0.3,
            "use_cases": ["simple_qa", "quick_lookup", "basic_chat"]
        },
        "quality_agent": {
            "model": "gemma3:4b",
            "max_tokens": 2000,
            "temperature": 0.5,
            "use_cases": ["analysis", "explanation", "comparison"]
        },
        "deep_agent": {
            "model": "llama3.1:8b",
            "max_tokens": 4000,
            "temperature": 0.7,
            "use_cases": ["complex_reasoning", "research", "multi_step"]
        },
        "complexity_thresholds": {
            "simple": 1.5,
            "standard": 4.0
        },
        "routing": {
            "default_agent": "quality",
            "fallback_behavior": "degrade",
            "context_upgrade_threshold": 3,
            "enable_parallel": true
        }
    }',
    'Main orchestrator configuration for agent routing and model selection'
) ON CONFLICT (setting_key) DO NOTHING;

-- Insert company PIN setting (default: 123456, bcrypt hashed)
INSERT INTO company_settings (setting_key, setting_value, description)
VALUES (
    'company_pin',
    '{"hash": "$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/X4.oG4.nFjVVD/5Hy", "attempts": 0, "locked_until": null}',
    'Company PIN for super admin access (default: 123456)'
) ON CONFLICT (setting_key) DO NOTHING;

-- Insert audit settings
INSERT INTO company_settings (setting_key, setting_value, description)
VALUES (
    'audit_settings',
    '{"log_superadmin_actions": true, "retention_days": 90}',
    'Audit logging configuration'
) ON CONFLICT (setting_key) DO NOTHING;
"""

DEFAULT_GUARDRAILS_SQL = """
-- Insert default safety guardrails
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active)
VALUES (
    'Harmful Content Prevention',
    'Blocks requests for harmful, violent, or dangerous content',
    'safety',
    '["how to make a bomb", "how to harm", "instructions for violence", "suicide methods"]',
    'block',
    100,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active)
VALUES (
    'Illegal Activity Prevention',
    'Blocks requests related to illegal activities',
    'safety',
    '["how to hack", "steal credit card", "forge documents", "drug synthesis"]',
    'block',
    100,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

-- Privacy guardrails
INSERT INTO guardrails (name, description, category, pattern, action, replacement_text, priority, is_active)
VALUES (
    'SSN Detection',
    'Detects and redacts Social Security Numbers',
    'privacy',
    '\\b\\d{3}-\\d{2}-\\d{4}\\b',
    'modify',
    '[SSN REDACTED]',
    90,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    pattern = EXCLUDED.pattern,
    action = EXCLUDED.action,
    replacement_text = EXCLUDED.replacement_text,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

INSERT INTO guardrails (name, description, category, pattern, action, replacement_text, priority, is_active)
VALUES (
    'Credit Card Detection',
    'Detects and redacts credit card numbers',
    'privacy',
    '\\b\\d{4}[- ]?\\d{4}[- ]?\\d{4}[- ]?\\d{4}\\b',
    'modify',
    '[CARD NUMBER REDACTED]',
    90,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    pattern = EXCLUDED.pattern,
    action = EXCLUDED.action,
    replacement_text = EXCLUDED.replacement_text,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

INSERT INTO guardrails (name, description, category, pattern, action, replacement_text, priority, is_active)
VALUES (
    'Email Detection',
    'Logs when email addresses are shared',
    'privacy',
    '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}',
    'log',
    NULL,
    50,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    pattern = EXCLUDED.pattern,
    action = EXCLUDED.action,
    replacement_text = EXCLUDED.replacement_text,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

-- Brand guardrails
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active, apply_to_output)
VALUES (
    'Professional Tone',
    'Warns when unprofessional language is detected in AI responses',
    'brand',
    '["lol", "lmao", "wtf", "omg"]',
    'warn',
    30,
    TRUE,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active,
    apply_to_output = EXCLUDED.apply_to_output;

-- Accuracy guardrails
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active, apply_to_output)
VALUES (
    'Citation Reminder',
    'Logs when AI makes claims without citing sources',
    'accuracy',
    '["studies show", "research indicates", "according to experts"]',
    'log',
    20,
    TRUE,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active,
    apply_to_output = EXCLUDED.apply_to_output;

-- =============================================================================
-- INDUSTRY COMPLIANCE GUARDRAILS
-- =============================================================================

-- HIPAA - Healthcare compliance
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active)
VALUES (
    'HIPAA Compliance',
    'Protects Protected Health Information (PHI) and ensures healthcare data privacy compliance',
    'compliance',
    '["patient records", "medical history", "health information", "PHI", "diagnosis", "treatment records", "prescription data", "insurance claims", "medical ID", "health conditions", "hospital records", "doctor notes", "lab results", "mental health records"]',
    'warn',
    90,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

-- PCI-DSS - Financial/Payment card compliance
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active)
VALUES (
    'PCI-DSS Compliance',
    'Protects payment card data and ensures financial transaction security compliance',
    'compliance',
    '["credit card number", "CVV", "card verification", "payment card", "cardholder data", "PAN", "primary account number", "card expiration", "PIN", "bank account", "routing number", "financial transaction", "payment processing"]',
    'warn',
    90,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;

-- GDPR - EU Data Protection compliance
INSERT INTO guardrails (name, description, category, keywords, action, priority, is_active)
VALUES (
    'GDPR Compliance',
    'Ensures EU personal data protection and privacy rights compliance',
    'compliance',
    '["personal data", "data subject", "consent", "right to erasure", "data portability", "EU citizen", "processing personal", "data controller", "data processor", "cross-border transfer", "legitimate interest", "special category data", "biometric data", "genetic data"]',
    'warn',
    90,
    TRUE
) ON CONFLICT (name, category) DO UPDATE SET
    description = EXCLUDED.description,
    keywords = EXCLUDED.keywords,
    action = EXCLUDED.action,
    priority = EXCLUDED.priority,
    is_active = EXCLUDED.is_active;
"""


async def initialize_schema():
    """Create all database tables"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            # Create schema
            await conn.execute(SCHEMA_SQL)
            logger.info("✅ Database schema created successfully")

            # Initialize Row-Level Security for multi-tenant isolation
            try:
                from .rls import initialize_rls
                if await initialize_rls(conn):
                    logger.info("✅ Row-Level Security policies initialized")
                else:
                    logger.warning("⚠️ RLS initialization returned False")
            except ImportError:
                logger.warning("⚠️ RLS module not available, skipping RLS initialization")
            except Exception as rls_error:
                logger.warning(f"⚠️ RLS initialization failed: {rls_error}")

            # Insert default data (skip in production with SKIP_SEED_DATA=true)
            if SKIP_SEED_DATA:
                logger.info("⏭️ Skipping seed data (SKIP_SEED_DATA=true)")
            else:
                try:
                    await conn.execute(DEFAULT_USERS_SQL)
                    await conn.execute(DEFAULT_ORG_SQL)
                    await conn.execute(DEFAULT_DEPARTMENTS_SQL)
                    await conn.execute(DEFAULT_ORG_USERS_SQL)
                    await conn.execute(DEFAULT_PROMPTS_SQL)
                    await conn.execute(DEFAULT_GUARDRAILS_SQL)
                    await conn.execute(DEFAULT_COMPANY_SETTINGS_SQL)
                    logger.info("✅ Default data inserted (set SKIP_SEED_DATA=true to skip in production)")
                except Exception as seed_error:
                    logger.warning(f"⚠️ Seed data insertion skipped (data may already exist): {seed_error}")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to initialize schema: {e}")
        raise


async def drop_all_tables():
    """Drop all tables (use with caution!)"""
    try:
        async with get_connection() as conn:
            await conn.execute("""
                -- Drop MCP tables first (have foreign keys)
                DROP VIEW IF EXISTS trash_items CASCADE;
                DROP TABLE IF EXISTS mcp_saved_credentials CASCADE;
                DROP TABLE IF EXISTS email_accounts CASCADE;
                DROP TABLE IF EXISTS mcp_oauth_credentials CASCADE;
                DROP TABLE IF EXISTS mcp_operation_approvals CASCADE;
                DROP TABLE IF EXISTS mcp_provider_settings CASCADE;

                -- Drop existing tables
                DROP TABLE IF EXISTS system_error_log CASCADE;
                DROP TABLE IF EXISTS agent_audit_log CASCADE;
                DROP TABLE IF EXISTS prompt_test_results CASCADE;
                DROP TABLE IF EXISTS prompt_tests CASCADE;
                DROP TABLE IF EXISTS prompt_change_requests CASCADE;
                DROP TABLE IF EXISTS prompt_suggestions CASCADE;
                DROP TABLE IF EXISTS company_settings CASCADE;
                DROP TABLE IF EXISTS guardrail_violations CASCADE;
                DROP TABLE IF EXISTS guardrails CASCADE;
                DROP TABLE IF EXISTS prompt_versions CASCADE;
                DROP TABLE IF EXISTS audit_log CASCADE;
                DROP TABLE IF EXISTS ai_metrics CASCADE;
                DROP TABLE IF EXISTS ai_models CASCADE;
                DROP TABLE IF EXISTS quality_metrics CASCADE;
                DROP TABLE IF EXISTS user_memories CASCADE;
                DROP TABLE IF EXISTS api_keys CASCADE;
                DROP TABLE IF EXISTS user_settings CASCADE;
                DROP TABLE IF EXISTS prompts CASCADE;
                DROP TABLE IF EXISTS messages CASCADE;
                DROP TABLE IF EXISTS conversations CASCADE;
                DROP TABLE IF EXISTS document_chunks CASCADE;
                DROP TABLE IF EXISTS documents CASCADE;
                DROP TABLE IF EXISTS users CASCADE;
            """)
            logger.info("All tables dropped")
        return True
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        raise


# =============================================================================
# MIGRATION FOR EXISTING DATABASES
# =============================================================================

MIGRATION_V2_TRASH_AND_MCP = """
-- =============================================================================
-- MIGRATION: Add soft-delete support and MCP tables
-- =============================================================================

-- Add deleted_at columns to existing tables (if not exists)
DO $$
BEGIN
    -- Documents
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='documents' AND column_name='deleted_at') THEN
        ALTER TABLE documents ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
        CREATE INDEX IF NOT EXISTS idx_documents_deleted_at ON documents(deleted_at) WHERE deleted_at IS NOT NULL;
    END IF;

    -- Prompts
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prompts' AND column_name='deleted_at') THEN
        ALTER TABLE prompts ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
        CREATE INDEX IF NOT EXISTS idx_prompts_deleted_at ON prompts(deleted_at) WHERE deleted_at IS NOT NULL;
    END IF;

    -- Guardrails
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='guardrails' AND column_name='deleted_at') THEN
        ALTER TABLE guardrails ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
        CREATE INDEX IF NOT EXISTS idx_guardrails_deleted_at ON guardrails(deleted_at) WHERE deleted_at IS NOT NULL;
    END IF;

    -- Add user_id to guardrails if missing
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='guardrails' AND column_name='user_id') THEN
        ALTER TABLE guardrails ADD COLUMN user_id INTEGER REFERENCES users(id) ON DELETE SET NULL;
    END IF;

    -- User memories
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='user_memories' AND column_name='deleted_at') THEN
        ALTER TABLE user_memories ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL;
        CREATE INDEX IF NOT EXISTS idx_user_memories_deleted_at ON user_memories(deleted_at) WHERE deleted_at IS NOT NULL;
    END IF;

    -- Memory consolidation columns for user_memories
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='user_memories' AND column_name='access_count') THEN
        ALTER TABLE user_memories ADD COLUMN access_count INTEGER DEFAULT 0;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='user_memories' AND column_name='last_accessed_at') THEN
        ALTER TABLE user_memories ADD COLUMN last_accessed_at TIMESTAMP;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='user_memories' AND column_name='decay_factor') THEN
        ALTER TABLE user_memories ADD COLUMN decay_factor FLOAT DEFAULT 1.0;
    END IF;
END $$;

-- Create MCP tables if they don't exist
CREATE TABLE IF NOT EXISTS mcp_provider_settings (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,
    enabled BOOLEAN DEFAULT FALSE,
    trusted_mode BOOLEAN DEFAULT FALSE,
    settings JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, provider_id)
);

CREATE TABLE IF NOT EXISTS mcp_operation_approvals (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,
    operation_type VARCHAR(50) NOT NULL,
    operation_params JSONB NOT NULL,
    risk_level VARCHAR(10) NOT NULL CHECK (risk_level IN ('low', 'medium', 'high', 'critical')),
    description TEXT,
    approved BOOLEAN DEFAULT FALSE,
    auto_approved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    expires_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mcp_oauth_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(50) NOT NULL,
    account_identifier VARCHAR(255),
    access_token TEXT,
    refresh_token TEXT,
    token_expiry TIMESTAMP,
    scopes TEXT[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, provider_id, account_identifier)
);

CREATE TABLE IF NOT EXISTS email_accounts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(20) NOT NULL CHECK (provider IN ('imap', 'gmail', 'outlook')),
    email_address VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    imap_host VARCHAR(255),
    imap_port INTEGER DEFAULT 993,
    smtp_host VARCHAR(255),
    smtp_port INTEGER DEFAULT 587,
    encrypted_password TEXT,
    oauth_credential_id INTEGER REFERENCES mcp_oauth_credentials(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_synced_at TIMESTAMP,
    UNIQUE(user_id, email_address)
);

CREATE TABLE IF NOT EXISTS mcp_saved_credentials (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    domain VARCHAR(255) NOT NULL,
    username VARCHAR(255),
    encrypted_password TEXT,
    totp_secret TEXT,
    selector_hints JSONB DEFAULT '{}'::jsonb,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMP,
    UNIQUE(user_id, domain, username)
);

-- Create indexes if they don't exist
CREATE INDEX IF NOT EXISTS idx_mcp_provider_settings_user ON mcp_provider_settings(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_provider_settings_provider ON mcp_provider_settings(provider_id);
CREATE INDEX IF NOT EXISTS idx_mcp_approvals_user ON mcp_operation_approvals(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_approvals_provider ON mcp_operation_approvals(provider_id);
CREATE INDEX IF NOT EXISTS idx_mcp_approvals_pending ON mcp_operation_approvals(approved) WHERE approved = FALSE;
CREATE INDEX IF NOT EXISTS idx_mcp_oauth_user ON mcp_oauth_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_oauth_provider ON mcp_oauth_credentials(provider_id);
CREATE INDEX IF NOT EXISTS idx_email_accounts_user ON email_accounts(user_id);
CREATE INDEX IF NOT EXISTS idx_email_accounts_active ON email_accounts(is_active);
CREATE INDEX IF NOT EXISTS idx_mcp_credentials_user ON mcp_saved_credentials(user_id);
CREATE INDEX IF NOT EXISTS idx_mcp_credentials_domain ON mcp_saved_credentials(domain);

-- Installed MCP plugins tracking
CREATE TABLE IF NOT EXISTS mcp_installed_plugins (
    id SERIAL PRIMARY KEY,
    plugin_id VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    version VARCHAR(50) DEFAULT '1.0.0',
    source VARCHAR(50) DEFAULT 'builtin' CHECK (source IN ('builtin', 'remote', 'local')),
    category VARCHAR(50),
    operations JSONB DEFAULT '[]'::jsonb,
    installed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    installed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    metadata JSONB DEFAULT '{{}}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_mcp_plugins_id ON mcp_installed_plugins(plugin_id);
CREATE INDEX IF NOT EXISTS idx_mcp_plugins_active ON mcp_installed_plugins(is_active);

-- Create or replace the trash view
CREATE OR REPLACE VIEW trash_items AS
SELECT
    id,
    'document' as item_type,
    filename as name,
    user_id,
    deleted_at,
    jsonb_build_object('size', file_size, 'file_type', file_type) as metadata
FROM documents WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'guardrail' as item_type,
    name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category) as metadata
FROM guardrails WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'prompt' as item_type,
    name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category) as metadata
FROM prompts WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'memory' as item_type,
    LEFT(content, 100) as name,
    user_id,
    deleted_at,
    jsonb_build_object('category', category, 'source', source) as metadata
FROM user_memories WHERE deleted_at IS NOT NULL
UNION ALL
SELECT
    id,
    'email' as item_type,
    COALESCE(subject, 'No Subject') as name,
    user_id,
    COALESCE(deleted_at, updated_at) as deleted_at,
    jsonb_build_object('from', from_address, 'from_name', from_name, 'email_date', email_date) as metadata
FROM email_messages WHERE sync_status = 'deleted';
"""


async def run_migration_v2():
    """Run migration to add soft-delete and MCP support"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V2_TRASH_AND_MCP)
            logger.info("✅ Migration V2 (Trash + MCP) completed successfully")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run migration V2: {e}")
        raise


# =============================================================================
# MIGRATION V3: External MCP Servers (Enterprise-grade)
# =============================================================================

MIGRATION_V3_EXTERNAL_MCP_SERVERS = """
-- =============================================================================
-- MIGRATION V3: Add enterprise-grade external MCP server tables
-- =============================================================================

-- External MCP Servers Table (Enterprise-grade)
CREATE TABLE IF NOT EXISTS mcp_external_servers (
    id SERIAL PRIMARY KEY,
    server_id VARCHAR(100) NOT NULL,
    organization_id INTEGER DEFAULT 1,
    name VARCHAR(200) NOT NULL,
    description TEXT,

    -- Transport configuration
    transport VARCHAR(20) NOT NULL CHECK (transport IN ('stdio', 'http', 'sse')),

    -- Stdio transport settings
    command VARCHAR(500),
    args JSONB DEFAULT '[]'::jsonb,
    env JSONB DEFAULT '{}'::jsonb,
    working_directory VARCHAR(1000),

    -- HTTP/SSE transport settings
    url VARCHAR(1000),
    headers JSONB DEFAULT '{}'::jsonb,

    -- Common settings
    enabled BOOLEAN DEFAULT TRUE,
    auto_start BOOLEAN DEFAULT FALSE,
    trusted_mode BOOLEAN DEFAULT FALSE,
    timeout_seconds INTEGER DEFAULT 30,

    -- Health monitoring
    health_check_interval_seconds INTEGER DEFAULT 60,
    health_check_enabled BOOLEAN DEFAULT TRUE,
    auto_restart_enabled BOOLEAN DEFAULT TRUE,
    max_restart_attempts INTEGER DEFAULT 3,
    restart_delay_seconds INTEGER DEFAULT 5,

    -- Connection pooling (HTTP/SSE)
    pool_size INTEGER DEFAULT 10,
    pool_timeout_seconds INTEGER DEFAULT 30,
    max_connections INTEGER DEFAULT 100,

    -- Process supervision (stdio)
    supervisor_enabled BOOLEAN DEFAULT TRUE,
    graceful_shutdown_timeout INTEGER DEFAULT 10,

    -- Runtime status
    status VARCHAR(20) DEFAULT 'stopped' CHECK (status IN ('stopped', 'starting', 'running', 'error', 'restarting')),
    pid INTEGER,
    last_health_check TIMESTAMP,
    health_status VARCHAR(20) DEFAULT 'unknown' CHECK (health_status IN ('unknown', 'healthy', 'unhealthy', 'degraded')),
    restart_count INTEGER DEFAULT 0,
    last_restart TIMESTAMP,
    last_error TEXT,

    -- Metadata
    icon VARCHAR(50) DEFAULT 'puzzle',
    category VARCHAR(50) DEFAULT 'external',
    installed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    installed_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(organization_id, server_id)
);

CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_org ON mcp_external_servers(organization_id);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_status ON mcp_external_servers(status);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_enabled ON mcp_external_servers(enabled);
CREATE INDEX IF NOT EXISTS idx_mcp_external_servers_auto_start ON mcp_external_servers(auto_start) WHERE auto_start = TRUE;

-- Health logs table
CREATE TABLE IF NOT EXISTS mcp_external_server_health_logs (
    id SERIAL PRIMARY KEY,
    server_id VARCHAR(100) NOT NULL,
    organization_id INTEGER DEFAULT 1,

    status VARCHAR(20) NOT NULL,
    response_time_ms INTEGER,
    error_message TEXT,

    memory_mb FLOAT,
    cpu_percent FLOAT,

    active_connections INTEGER,
    pool_available INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mcp_health_logs_server ON mcp_external_server_health_logs(server_id);
CREATE INDEX IF NOT EXISTS idx_mcp_health_logs_created ON mcp_external_server_health_logs(created_at);
-- Cleanup of old logs should be done via scheduled job (DELETE WHERE created_at < NOW() - INTERVAL '7 days')
"""


async def run_migration_v3():
    """Run migration to add external MCP server tables (enterprise-grade)"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V3_EXTERNAL_MCP_SERVERS)
            logger.info("✅ Migration V3 (External MCP Servers) completed successfully")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run migration V3: {e}")
        raise


# MIGRATION V4: Add must_change_password to users table
MIGRATION_V4_MUST_CHANGE_PASSWORD = """
-- Add must_change_password column for forced password change on first login
ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN DEFAULT FALSE;
"""


async def run_migration_v4():
    """Run migration to add must_change_password column to users table"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V4_MUST_CHANGE_PASSWORD)
            logger.info("✅ Migration V4 (must_change_password) completed successfully")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run migration V4: {e}")
        raise


# MIGRATION V5: Add SSO tables and columns
MIGRATION_V5_SSO = """
-- Add SSO columns to users table
ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_only BOOLEAN DEFAULT FALSE;
ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_provider VARCHAR(50);
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_sso_login TIMESTAMP;
CREATE INDEX IF NOT EXISTS idx_users_sso ON users(sso_only) WHERE sso_only = TRUE;

-- Create org_sso_config table
CREATE TABLE IF NOT EXISTS org_sso_config (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER UNIQUE NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    sso_enabled BOOLEAN DEFAULT FALSE,
    sso_provider VARCHAR(50) NOT NULL DEFAULT 'disabled'
        CHECK (sso_provider IN ('disabled', 'oidc', 'saml2')),
    oidc_issuer_url VARCHAR(500),
    oidc_client_id VARCHAR(255),
    oidc_client_secret_encrypted TEXT,
    oidc_scopes VARCHAR(500) DEFAULT 'openid email profile',
    oidc_authorization_endpoint VARCHAR(500),
    oidc_token_endpoint VARCHAR(500),
    oidc_userinfo_endpoint VARCHAR(500),
    oidc_jwks_uri VARCHAR(500),
    saml_entity_id VARCHAR(500),
    saml_idp_entity_id VARCHAR(500),
    saml_idp_sso_url VARCHAR(500),
    saml_idp_slo_url VARCHAR(500),
    saml_idp_certificate TEXT,
    saml_sp_certificate_encrypted TEXT,
    saml_sp_private_key_encrypted TEXT,
    saml_sign_requests BOOLEAN DEFAULT TRUE,
    saml_want_signed_responses BOOLEAN DEFAULT TRUE,
    saml_name_id_format VARCHAR(100) DEFAULT 'urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress',
    enforce_sso_for_domain BOOLEAN DEFAULT FALSE,
    sso_domains TEXT[],
    auto_provision_users BOOLEAN DEFAULT TRUE,
    default_role VARCHAR(50) DEFAULT 'user',
    attribute_mapping JSONB DEFAULT '{"email": "email", "name": "name", "given_name": "given_name", "family_name": "family_name", "groups": "groups", "department": "department"}',
    group_role_mapping JSONB DEFAULT '{}',
    group_department_mapping JSONB DEFAULT '{}',
    is_verified BOOLEAN DEFAULT FALSE,
    last_test_at TIMESTAMP,
    metadata_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_org_sso_config_org ON org_sso_config(organization_id);
CREATE INDEX IF NOT EXISTS idx_org_sso_config_domains ON org_sso_config USING GIN(sso_domains);

-- Create user_sso_identities table
CREATE TABLE IF NOT EXISTS user_sso_identities (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    sso_provider VARCHAR(50) NOT NULL,
    idp_subject VARCHAR(500) NOT NULL,
    idp_issuer VARCHAR(500) NOT NULL,
    idp_email VARCHAR(255),
    idp_name VARCHAR(255),
    idp_groups JSONB DEFAULT '[]',
    raw_attributes JSONB DEFAULT '{}',
    last_login_at TIMESTAMP,
    session_id VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(organization_id, sso_provider, idp_subject)
);
CREATE INDEX IF NOT EXISTS idx_user_sso_identities_user ON user_sso_identities(user_id);
CREATE INDEX IF NOT EXISTS idx_user_sso_identities_subject ON user_sso_identities(idp_subject);
CREATE INDEX IF NOT EXISTS idx_user_sso_identities_org ON user_sso_identities(organization_id);
"""


async def run_migration_v5():
    """Run migration to add SSO tables and columns"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V5_SSO)
            logger.info("✅ Migration V5 (SSO tables) completed successfully")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run migration V5: {e}")
        raise


# MIGRATION V6: Multi-tenant MCP policies
MIGRATION_V6_MCP_MULTI_TENANT = """
-- MIGRATION V6: Add multi-tenant MCP policies tables

-- Organization-level MCP policies (Enterprise admins control which MCPs their org can use)
CREATE TABLE IF NOT EXISTS org_mcp_policies (
    id SERIAL PRIMARY KEY,
    organization_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider_id VARCHAR(100) NOT NULL,
    allowed BOOLEAN DEFAULT TRUE,
    default_enabled BOOLEAN DEFAULT FALSE,
    force_trusted_mode BOOLEAN DEFAULT NULL,
    max_operations_per_hour INTEGER DEFAULT NULL,
    restricted_operations JSONB DEFAULT '[]',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    UNIQUE(organization_id, provider_id)
);
CREATE INDEX IF NOT EXISTS idx_org_mcp_policies_org ON org_mcp_policies(organization_id);
CREATE INDEX IF NOT EXISTS idx_org_mcp_policies_provider ON org_mcp_policies(provider_id);

-- Platform-level MCP configuration (Superadmin controls which MCPs are available system-wide)
CREATE TABLE IF NOT EXISTS platform_mcp_config (
    id SERIAL PRIMARY KEY,
    provider_id VARCHAR(100) UNIQUE NOT NULL,
    display_name VARCHAR(255) NOT NULL,
    description TEXT,
    available_tiers JSONB DEFAULT '["consumer", "enterprise", "developer"]',
    is_premium BOOLEAN DEFAULT FALSE,
    default_allowed BOOLEAN DEFAULT TRUE,
    global_enabled BOOLEAN DEFAULT TRUE,
    category VARCHAR(50) DEFAULT 'general',
    risk_level VARCHAR(20) DEFAULT 'low' CHECK (risk_level IN ('low', 'medium', 'high')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_platform_mcp_config_enabled ON platform_mcp_config(global_enabled);
CREATE INDEX IF NOT EXISTS idx_platform_mcp_config_premium ON platform_mcp_config(is_premium);

-- Insert default platform MCP configs for built-in providers
INSERT INTO platform_mcp_config (provider_id, display_name, description, is_premium, risk_level, category)
VALUES
    ('file', 'File System', 'Read, write, and manage local files', FALSE, 'high', 'system'),
    ('browser', 'Web Browser', 'Navigate websites, take screenshots, extract content', FALSE, 'high', 'automation'),
    ('email', 'Email', 'Send, read, and manage emails', FALSE, 'medium', 'communication'),
    ('office', 'Office Suite', 'Read Word, Excel, and PowerPoint files', FALSE, 'low', 'productivity'),
    ('calendar', 'Calendar', 'Manage calendar events and schedules', TRUE, 'medium', 'productivity'),
    ('code', 'Code Executor', 'Run and analyze code in sandboxed environment', TRUE, 'high', 'development'),
    ('math', 'Math Solver', 'Solve mathematical equations and plot graphs', FALSE, 'low', 'education'),
    ('vision', 'Vision AI', 'Analyze images, OCR, and visual understanding', TRUE, 'low', 'ai'),
    ('finance', 'Finance', 'Parse bank statements and categorize transactions', TRUE, 'medium', 'finance'),
    ('credential', 'Credential Vault', 'Securely store and retrieve credentials', TRUE, 'high', 'security')
ON CONFLICT (provider_id) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;
"""


async def run_migration_v6():
    """Run migration to add multi-tenant MCP policies tables"""
    try:
        pool = await get_db_pool()

        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V6_MCP_MULTI_TENANT)
            logger.info("✅ Migration V6 (MCP Multi-Tenant Policies) completed successfully")

        return True

    except Exception as e:
        logger.error(f"❌ Failed to run migration V6: {e}")
        raise


# MIGRATION V7: Fix document status CHECK constraint + document health
MIGRATION_V7_DOCUMENT_STATUS = """
-- MIGRATION V7: Expand document status CHECK constraint to include no_content and needs_reindex
DO $$
BEGIN
    -- Drop the old CHECK constraint (may have different names depending on DB)
    BEGIN
        ALTER TABLE documents DROP CONSTRAINT IF EXISTS documents_status_check;
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Ignore if constraint doesn't exist
    END;

    -- Add the new CHECK constraint with all 6 valid statuses
    BEGIN
        ALTER TABLE documents ADD CONSTRAINT documents_status_check
            CHECK (status IN ('pending', 'processing', 'completed', 'failed', 'no_content', 'needs_reindex'));
    EXCEPTION WHEN OTHERS THEN
        NULL; -- Constraint may already exist with the correct definition
    END;

    -- Flag completed documents with 0 chunks as needs_reindex
    UPDATE documents
    SET status = 'needs_reindex'
    WHERE status = 'completed'
    AND (chunk_count = 0 OR chunk_count IS NULL)
    AND deleted_at IS NULL;
END $$;
"""


async def run_migration_v7():
    """Run migration to fix document status CHECK constraint"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V7_DOCUMENT_STATUS)
            logger.info("✅ Migration V7 (Document Status Fix) completed successfully")
            return True
    except Exception as e:
        logger.error(f"❌ Failed to run migration V7: {e}")
        return False


MIGRATION_V8_DOCUMENT_FOLDERS = """
-- MIGRATION V8: Document folders table + folder_id column on documents
-- Enable ltree extension for hierarchical paths
CREATE EXTENSION IF NOT EXISTS ltree;

-- Create document_folders table
CREATE TABLE IF NOT EXISTS document_folders (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
    name VARCHAR(255) NOT NULL,
    parent_id UUID REFERENCES document_folders(id) ON DELETE CASCADE,
    path ltree,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_document_folders_tenant ON document_folders(tenant_id);
CREATE INDEX IF NOT EXISTS idx_document_folders_parent ON document_folders(parent_id);
CREATE INDEX IF NOT EXISTS idx_document_folders_path ON document_folders USING GIST (path);

-- Add folder_id column to documents table
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'documents' AND column_name = 'folder_id') THEN
        ALTER TABLE documents ADD COLUMN folder_id UUID REFERENCES document_folders(id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_documents_folder ON documents(folder_id) WHERE folder_id IS NOT NULL;
"""


async def run_migration_v8():
    """Run migration to create document_folders table and add folder_id to documents"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V8_DOCUMENT_FOLDERS)
            logger.info("✅ Migration V8 (Document Folders) completed successfully")
            return True
    except Exception as e:
        logger.error(f"❌ Failed to run migration V8: {e}")
        return False


MIGRATION_V9_MCP_USAGE_LOG = """
-- MCP usage log for agentic dashboard observability
CREATE TABLE IF NOT EXISTS mcp_usage_log (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider_id VARCHAR(100) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    success BOOLEAN DEFAULT TRUE,
    duration_ms INTEGER,
    triggered_by VARCHAR(50) DEFAULT 'user_request',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_mcp_usage_log_user_time ON mcp_usage_log(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mcp_usage_log_provider ON mcp_usage_log(provider_id);
"""


async def run_migration_v9():
    """Run migration to create mcp_usage_log table"""
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await conn.execute(MIGRATION_V9_MCP_USAGE_LOG)
            logger.info("✅ Migration V9 (MCP Usage Log) completed successfully")
            return True
    except Exception as e:
        logger.error(f"❌ Failed to run migration V9: {e}")
        return False


async def run_all_migrations():
    """Run all migrations in order"""
    try:
        await run_migration_v2()
        await run_migration_v3()
        await run_migration_v4()
        await run_migration_v5()
        await run_migration_v6()
        await run_migration_v7()
        await run_migration_v8()
        await run_migration_v9()
        logger.info("✅ All migrations completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to run migrations: {e}")
        raise


async def reset_database():
    """Drop and recreate all tables"""
    await drop_all_tables()
    await initialize_schema()
    logger.info("Database reset complete")
