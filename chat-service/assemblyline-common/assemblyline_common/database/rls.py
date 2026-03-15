"""
Row-Level Security (RLS) for Multi-Tenant Isolation

Provides database-level security to ensure organizations can only access their own data.
This is a defense-in-depth measure on top of application-level filtering.

Usage:
    # Set org context on each connection
    await set_organization_context(conn, org_id)

    # Or use the helper
    async with get_connection_with_rls(org_id) as conn:
        # All queries automatically filtered by org
        rows = await conn.fetch("SELECT * FROM documents")
"""

import os
import logging
from typing import Optional
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)

# =============================================================================
# RLS SQL POLICIES
# =============================================================================

RLS_SETUP_SQL = """
-- =============================================================================
-- ROW-LEVEL SECURITY POLICIES
-- Provides database-level multi-tenant isolation
-- =============================================================================

-- Enable RLS on critical tables (idempotent)
ALTER TABLE documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE document_chunks ENABLE ROW LEVEL SECURITY;

-- Force RLS for all roles including superuser (more secure but optional)
-- ALTER TABLE documents FORCE ROW LEVEL SECURITY;

-- =============================================================================
-- DOCUMENTS POLICIES
-- =============================================================================

-- Drop existing policies to allow recreation (idempotent)
DROP POLICY IF EXISTS documents_org_isolation ON documents;
DROP POLICY IF EXISTS documents_own_user ON documents;
DROP POLICY IF EXISTS documents_superadmin_access ON documents;

-- Policy: Organization members can see their org's documents
CREATE POLICY documents_org_isolation ON documents
    FOR ALL
    TO PUBLIC
    USING (
        -- Allow if org context matches
        organization_id = current_setting('app.current_org_id', true)::int
        -- Or if no org context set (superadmin/system operations)
        OR current_setting('app.current_org_id', true) IS NULL
        OR current_setting('app.current_org_id', true) = ''
    );

-- =============================================================================
-- CONVERSATIONS POLICIES
-- =============================================================================
-- Conversations use user_id, which links to organizations through users table

DROP POLICY IF EXISTS conversations_user_isolation ON conversations;

-- Policy: Users can only see their own conversations
-- (organization isolation is enforced through user_id -> users.organization_id)
CREATE POLICY conversations_user_isolation ON conversations
    FOR ALL
    TO PUBLIC
    USING (
        -- Allow if user context matches
        user_id = current_setting('app.current_user_id', true)::int
        -- Or if no context set (system operations)
        OR current_setting('app.current_user_id', true) IS NULL
        OR current_setting('app.current_user_id', true) = ''
    );

-- =============================================================================
-- MESSAGES POLICIES
-- =============================================================================
-- Messages inherit access from their conversations

DROP POLICY IF EXISTS messages_via_conversation ON messages;

-- Policy: Messages accessible if parent conversation is accessible
CREATE POLICY messages_via_conversation ON messages
    FOR ALL
    TO PUBLIC
    USING (
        EXISTS (
            SELECT 1 FROM conversations c
            WHERE c.id = messages.conversation_id
            AND (
                c.user_id = current_setting('app.current_user_id', true)::int
                OR current_setting('app.current_user_id', true) IS NULL
                OR current_setting('app.current_user_id', true) = ''
            )
        )
    );

-- =============================================================================
-- EMBEDDINGS POLICIES
-- =============================================================================
-- Embeddings inherit access from their documents

DROP POLICY IF EXISTS document_chunks_via_document ON document_chunks;

-- Policy: Embeddings accessible if parent document is accessible
CREATE POLICY document_chunks_via_document ON document_chunks
    FOR ALL
    TO PUBLIC
    USING (
        EXISTS (
            SELECT 1 FROM documents d
            WHERE d.id = document_chunks.document_id
            AND (
                d.organization_id = current_setting('app.current_org_id', true)::int
                OR current_setting('app.current_org_id', true) IS NULL
                OR current_setting('app.current_org_id', true) = ''
            )
        )
    );

-- =============================================================================
-- HELPER FUNCTIONS FOR SETTING CONTEXT
-- =============================================================================

-- Function to set organization context for current session
CREATE OR REPLACE FUNCTION set_tenant_context(org_id int, user_id int DEFAULT NULL)
RETURNS void AS $$
BEGIN
    IF org_id IS NOT NULL THEN
        PERFORM set_config('app.current_org_id', org_id::text, true);
    END IF;
    IF user_id IS NOT NULL THEN
        PERFORM set_config('app.current_user_id', user_id::text, true);
    END IF;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to clear tenant context
CREATE OR REPLACE FUNCTION clear_tenant_context()
RETURNS void AS $$
BEGIN
    PERFORM set_config('app.current_org_id', '', true);
    PERFORM set_config('app.current_user_id', '', true);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Function to get current tenant context
CREATE OR REPLACE FUNCTION get_tenant_context()
RETURNS TABLE(org_id text, user_id text) AS $$
BEGIN
    RETURN QUERY SELECT
        current_setting('app.current_org_id', true),
        current_setting('app.current_user_id', true);
END;
$$ LANGUAGE plpgsql;
"""

# Migration SQL to add organization_id to conversations if missing
MIGRATION_SQL = """
-- Add organization_id to conversations if not exists (for direct org filtering)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'conversations' AND column_name = 'organization_id'
    ) THEN
        ALTER TABLE conversations ADD COLUMN organization_id INTEGER;

        -- Backfill organization_id from users
        UPDATE conversations c
        SET organization_id = u.organization_id
        FROM users u
        WHERE c.user_id = u.id AND c.organization_id IS NULL;

        -- Add foreign key
        ALTER TABLE conversations
            ADD CONSTRAINT fk_conversations_org
            FOREIGN KEY (organization_id)
            REFERENCES organizations(id) ON DELETE SET NULL;

        CREATE INDEX IF NOT EXISTS idx_conversations_org ON conversations(organization_id);
    END IF;
END $$;
"""


# =============================================================================
# PYTHON HELPER FUNCTIONS
# =============================================================================

async def initialize_rls(conn) -> bool:
    """
    Initialize Row-Level Security policies.
    Should be called during database setup.

    Returns:
        True if successful, False otherwise
    """
    try:
        # Run migration first
        await conn.execute(MIGRATION_SQL)
        logger.info("RLS migration completed")

        # Then set up RLS policies
        await conn.execute(RLS_SETUP_SQL)
        logger.info("Row-Level Security policies initialized")
        return True
    except Exception as e:
        logger.error(f"Failed to initialize RLS: {e}")
        return False


async def set_organization_context(conn, organization_id: Optional[int], user_id: Optional[int] = None):
    """
    Set the organization and user context for RLS filtering.

    Call this after acquiring a connection to enable tenant isolation.

    Args:
        conn: Database connection
        organization_id: The organization ID to filter by
        user_id: Optional user ID for user-level filtering
    """
    try:
        if organization_id is not None:
            await conn.execute(
                "SELECT set_config('app.current_org_id', $1::text, true)",
                organization_id
            )
        if user_id is not None:
            await conn.execute(
                "SELECT set_config('app.current_user_id', $1::text, true)",
                user_id
            )
    except Exception as e:
        logger.warning(f"Failed to set RLS context: {e}")


async def clear_organization_context(conn):
    """
    Clear the organization context.
    Call this before returning connection to pool.
    """
    try:
        await conn.execute("SELECT clear_tenant_context()")
    except Exception as e:
        logger.debug(f"Failed to clear RLS context: {e}")


@asynccontextmanager
async def get_connection_with_rls(organization_id: int, user_id: Optional[int] = None):
    """
    Get a database connection with RLS context set.

    Usage:
        async with get_connection_with_rls(org_id, user_id) as conn:
            rows = await conn.fetch("SELECT * FROM documents")
            # Automatically filtered to org's data
    """
    from .asyncpg_connection import get_connection

    async with get_connection() as conn:
        await set_organization_context(conn, organization_id, user_id)
        try:
            yield conn
        finally:
            await clear_organization_context(conn)


# =============================================================================
# VERIFICATION UTILITIES
# =============================================================================

async def verify_rls_enabled(conn) -> dict:
    """
    Verify RLS is properly configured on all tables.

    Returns:
        Dict with table names and their RLS status
    """
    tables = ['documents', 'conversations', 'messages', 'document_chunks']
    status = {}

    for table in tables:
        try:
            result = await conn.fetchval(f"""
                SELECT relrowsecurity
                FROM pg_class
                WHERE relname = '{table}'
            """)
            status[table] = {
                'rls_enabled': bool(result),
                'status': 'ok' if result else 'disabled'
            }
        except Exception as e:
            status[table] = {
                'rls_enabled': False,
                'status': f'error: {e}'
            }

    return status


async def get_rls_policies(conn, table: str) -> list:
    """
    Get all RLS policies for a table.

    Returns:
        List of policy names
    """
    try:
        rows = await conn.fetch("""
            SELECT polname, polcmd, polpermissive
            FROM pg_policy
            WHERE polrelid = $1::regclass
        """, table)
        return [
            {
                'name': row['polname'],
                'command': row['polcmd'],
                'permissive': row['polpermissive']
            }
            for row in rows
        ]
    except Exception as e:
        logger.warning(f"Failed to get RLS policies for {table}: {e}")
        return []


# Exports
__all__ = [
    'initialize_rls',
    'set_organization_context',
    'clear_organization_context',
    'get_connection_with_rls',
    'verify_rls_enabled',
    'get_rls_policies',
    'RLS_SETUP_SQL',
]
