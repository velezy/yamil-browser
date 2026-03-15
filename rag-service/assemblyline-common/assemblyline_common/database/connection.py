"""
Database connection utilities for Logic Weaver.

Provides async SQLAlchemy connections with multi-tenant schema support.
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
import logging
import ssl

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy import text, event
from sqlalchemy.pool import NullPool

from assemblyline_common.config import settings, get_database_url_with_encryption

logger = logging.getLogger(__name__)

# Global engine instance
_engine: Optional[AsyncEngine] = None


def get_engine() -> AsyncEngine:
    """Get or create the async database engine.

    Supports three auth modes:
    - IAM auth (USE_IAM_AUTH=true): Short-lived tokens via IAM role. Best for AWS production.
    - Password auth: Traditional DATABASE_URL with credentials (Secrets Manager, encrypted, or .env).

    Pool configuration sized for 50+ concurrent users:
    - pool_size: Base number of connections to maintain
    - max_overflow: Additional connections allowed under load
    - pool_timeout: Seconds to wait before giving up on getting a connection
    - pool_recycle: Seconds before recycling a connection (prevents stale connections)
    """
    global _engine

    if _engine is not None:
        return _engine

    # Build connect_args with statement timeouts to prevent runaway queries
    connect_args = {}
    if settings.DATABASE_STATEMENT_TIMEOUT > 0:
        connect_args["server_settings"] = {
            "statement_timeout": str(settings.DATABASE_STATEMENT_TIMEOUT),
            "idle_in_transaction_session_timeout": str(settings.DATABASE_IDLE_IN_TRANSACTION_TIMEOUT),
        }

    if settings.USE_IAM_AUTH:
        _engine = _create_iam_engine(connect_args)
    else:
        _engine = _create_password_engine(connect_args)

    return _engine


def _create_password_engine(connect_args: dict) -> AsyncEngine:
    """Standard password-based engine (existing behavior)."""
    database_url = get_database_url_with_encryption()

    engine = create_async_engine(
        database_url,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_timeout=settings.DATABASE_POOL_TIMEOUT,
        pool_recycle=settings.DATABASE_POOL_RECYCLE,
        echo=settings.DATABASE_ECHO,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    logger.info(
        "Database engine created (password auth): pool_size=%d, max_overflow=%d, "
        "pool_recycle=%ds, statement_timeout=%dms",
        settings.DATABASE_POOL_SIZE, settings.DATABASE_MAX_OVERFLOW,
        settings.DATABASE_POOL_RECYCLE, settings.DATABASE_STATEMENT_TIMEOUT,
    )
    return engine


def _create_iam_engine(connect_args: dict) -> AsyncEngine:
    """IAM-authenticated engine with dynamic token injection."""
    from assemblyline_common.database.iam_auth import get_rds_iam_token

    # Build URL with placeholder password (replaced at connect time)
    base_url = (
        f"postgresql+asyncpg://{settings.RDS_IAM_USER}:placeholder"
        f"@{settings.RDS_IAM_HOST}:{settings.RDS_IAM_PORT}"
        f"/{settings.RDS_IAM_DBNAME}"
    )

    # IAM tokens expire in 15 min — recycle connections well before that
    pool_recycle = min(settings.DATABASE_POOL_RECYCLE, 600)

    # RDS IAM auth requires SSL — create context that trusts AWS RDS CA
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    iam_connect_args = {**connect_args, "ssl": ssl_context}

    engine = create_async_engine(
        base_url,
        pool_size=settings.DATABASE_POOL_SIZE,
        max_overflow=settings.DATABASE_MAX_OVERFLOW,
        pool_timeout=settings.DATABASE_POOL_TIMEOUT,
        pool_recycle=pool_recycle,
        echo=settings.DATABASE_ECHO,
        pool_pre_ping=True,
        connect_args=iam_connect_args,
    )

    # Inject fresh IAM token on every new physical connection
    @event.listens_for(engine.sync_engine, "do_connect")
    def inject_iam_token(dialect, conn_rec, cargs, cparams):
        cparams["password"] = get_rds_iam_token()

    logger.info(
        "Database engine created (IAM auth): user=%s, host=%s, pool_recycle=%ds",
        settings.RDS_IAM_USER, settings.RDS_IAM_HOST, pool_recycle,
    )
    return engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get the async session factory."""
    return async_sessionmaker(
        bind=get_engine(),
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency that provides a database session.
    
    Use with FastAPI:
        @app.get("/items")
        async def get_items(db: AsyncSession = Depends(get_db)):
            ...
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_tenant_db(
    tenant_schema: str,
    tenant_id: Optional[str] = None,
) -> AsyncGenerator[AsyncSession, None]:
    """
    Context manager that provides a session with tenant schema set.

    Args:
        tenant_schema: The PostgreSQL schema name for the tenant (e.g., "tenant_epic")
        tenant_id: Optional tenant UUID string for RLS enforcement.
                   When provided, sets app.current_tenant_id so PostgreSQL
                   row-level security policies restrict access to this tenant only.

    Usage:
        async with get_tenant_db("tenant_epic", str(tenant.id)) as session:
            result = await session.execute(select(Message))
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            # Set the search path to tenant schema + common schema
            await session.execute(
                text(f"SET search_path TO {tenant_schema}, common, public")
            )
            # Set RLS session variable for row-level security enforcement
            if tenant_id:
                await session.execute(
                    text("SET app.current_tenant_id = :tid"),
                    {"tid": str(tenant_id)},
                )
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            # Reset search path and RLS context
            await session.execute(text("SET search_path TO public"))
            await session.execute(text("RESET app.current_tenant_id"))
            await session.close()


async def get_serializable_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency that provides a SERIALIZABLE isolation level session.

    Use for critical operations where dirty/phantom reads are unacceptable:
    - Financial transactions
    - Clinical data writes
    - Audit log chain verification

    Usage:
        @app.post("/transfer")
        async def transfer(db: AsyncSession = Depends(get_serializable_db)):
            ...
    """
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            await session.execute(
                text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            )
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def create_tenant_schema(tenant_key: str) -> str:
    """
    Create a new schema for a tenant.
    
    Args:
        tenant_key: Unique identifier for the tenant (e.g., "epic", "workday")
    
    Returns:
        The schema name created (e.g., "tenant_epic")
    """
    schema_name = f"tenant_{tenant_key}"
    
    async with get_session_factory()() as session:
        # Create schema if not exists
        await session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
        
        # Create tenant-specific tables
        await session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.inbound_messages (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                correlation_id UUID NOT NULL,
                message_type VARCHAR(50) NOT NULL,
                content_type VARCHAR(100) DEFAULT 'application/json',
                raw_content TEXT NOT NULL,
                parsed_content JSONB,
                source_system VARCHAR(100),
                received_at TIMESTAMPTZ DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                status VARCHAR(20) DEFAULT 'pending',
                error_message TEXT,
                metadata JSONB DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        await session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.outbound_messages (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                correlation_id UUID NOT NULL,
                inbound_message_id UUID REFERENCES {schema_name}.inbound_messages(id),
                destination VARCHAR(100) NOT NULL,
                destination_url TEXT,
                message_type VARCHAR(50),
                content_type VARCHAR(100) DEFAULT 'application/json',
                content TEXT NOT NULL,
                status VARCHAR(20) DEFAULT 'pending',
                retry_count INT DEFAULT 0,
                max_retries INT DEFAULT 3,
                next_retry_at TIMESTAMPTZ,
                sent_at TIMESTAMPTZ,
                response_status INT,
                response_body TEXT,
                error_message TEXT,
                metadata JSONB DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        await session.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {schema_name}.flow_executions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                flow_id UUID NOT NULL,
                flow_version INT NOT NULL,
                correlation_id UUID NOT NULL,
                input_data JSONB NOT NULL,
                output_data JSONB,
                status VARCHAR(20) DEFAULT 'running',
                started_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                duration_ms INT,
                steps_executed INT DEFAULT 0,
                error_message TEXT,
                step_logs JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        
        # Create indexes
        await session.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{schema_name}_inbound_correlation 
            ON {schema_name}.inbound_messages(correlation_id)
        """))
        await session.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{schema_name}_inbound_status 
            ON {schema_name}.inbound_messages(status)
        """))
        await session.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{schema_name}_outbound_correlation 
            ON {schema_name}.outbound_messages(correlation_id)
        """))
        await session.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{schema_name}_outbound_status 
            ON {schema_name}.outbound_messages(status, next_retry_at)
        """))
        
        await session.commit()
        
        logger.info(f"Created tenant schema: {schema_name}")
        return schema_name


async def close_engine() -> None:
    """Close the database engine and all connections."""
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        logger.info("Database engine closed")
