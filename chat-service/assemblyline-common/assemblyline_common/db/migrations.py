"""
Database migration runner for Logic Weaver.

Runs SQL migration files in order on application startup.
"""

import os
import re
import logging
from pathlib import Path
from typing import List, Tuple
import asyncpg

logger = logging.getLogger(__name__)


def normalize_database_url(url: str) -> str:
    """
    Convert SQLAlchemy-style database URL to asyncpg-compatible URL.

    SQLAlchemy uses: postgresql+asyncpg://user@host:port/db
    asyncpg needs:   postgresql://user@host:port/db
    """
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql://")
    return url


async def get_applied_migrations(conn: asyncpg.Connection) -> List[str]:
    """Get list of already applied migration versions."""
    # Create migrations tracking table if it doesn't exist
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS common.schema_migrations (
            version VARCHAR(20) PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW(),
            checksum VARCHAR(64)
        )
    """)

    rows = await conn.fetch("SELECT version FROM common.schema_migrations ORDER BY version")
    return [row['version'] for row in rows]


async def mark_migration_applied(conn: asyncpg.Connection, version: str, checksum: str):
    """Mark a migration as applied."""
    await conn.execute(
        """
        INSERT INTO common.schema_migrations (version, checksum)
        VALUES ($1, $2)
        ON CONFLICT (version) DO NOTHING
        """,
        version, checksum
    )


def get_migration_files(migrations_dir: Path) -> List[Tuple[str, Path]]:
    """Get all migration files sorted by version number."""
    if not migrations_dir.exists():
        logger.warning(f"Migrations directory not found: {migrations_dir}")
        return []

    migrations = []
    pattern = re.compile(r'^V(\d+)_.+\.sql$')

    for file in migrations_dir.iterdir():
        if file.is_file() and file.suffix == '.sql':
            match = pattern.match(file.name)
            if match:
                version = f"V{match.group(1).zfill(3)}"  # Normalize to V001, V002, etc.
                migrations.append((version, file))

    # Sort by version number
    migrations.sort(key=lambda x: int(re.search(r'\d+', x[0]).group()))
    return migrations


def compute_checksum(content: str) -> str:
    """Compute SHA-256 checksum of migration content."""
    import hashlib
    return hashlib.sha256(content.encode()).hexdigest()


async def run_migrations(database_url: str, migrations_dir: Path = None):
    """
    Run all pending migrations.

    Args:
        database_url: PostgreSQL connection URL
        migrations_dir: Path to migrations directory (defaults to project migrations/)
    """
    # Normalize URL for asyncpg
    database_url = normalize_database_url(database_url)

    if migrations_dir is None:
        # Default to project root migrations directory
        # Go up from shared/python/assemblyline_common/db/ to project root
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent.parent.parent.parent
        migrations_dir = project_root / "migrations"

    logger.info(f"Looking for migrations in: {migrations_dir}")

    # Get all migration files
    migration_files = get_migration_files(migrations_dir)
    if not migration_files:
        logger.info("No migration files found")
        return

    logger.info(f"Found {len(migration_files)} migration files")

    # Connect to database
    try:
        conn = await asyncpg.connect(database_url)
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

    try:
        # Ensure common schema exists
        await conn.execute("CREATE SCHEMA IF NOT EXISTS common")

        # Get applied migrations
        applied = await get_applied_migrations(conn)
        logger.info(f"Already applied migrations: {applied}")

        # Run pending migrations
        pending_count = 0
        for version, file_path in migration_files:
            if version in applied:
                continue

            pending_count += 1
            logger.info(f"Applying migration {version}: {file_path.name}")

            try:
                content = file_path.read_text()
                checksum = compute_checksum(content)

                # Execute migration
                await conn.execute(content)

                # Mark as applied
                await mark_migration_applied(conn, version, checksum)
                logger.info(f"Successfully applied {version}")

            except Exception as e:
                logger.error(f"Failed to apply migration {version}: {e}")
                raise

        if pending_count == 0:
            logger.info("Database is up to date - no migrations to apply")
        else:
            logger.info(f"Applied {pending_count} migrations successfully")

    finally:
        await conn.close()


async def run_init_sql(database_url: str, init_sql_path: Path = None):
    """
    Run the init.sql file for fresh database setup.
    Only runs if the common.tenants table doesn't exist.

    Args:
        database_url: PostgreSQL connection URL
        init_sql_path: Path to init.sql file
    """
    # Normalize URL for asyncpg
    database_url = normalize_database_url(database_url)

    if init_sql_path is None:
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent.parent.parent.parent
        init_sql_path = project_root / "infrastructure" / "migrations" / "init.sql"

    if not init_sql_path.exists():
        logger.warning(f"init.sql not found at {init_sql_path}")
        return False

    try:
        conn = await asyncpg.connect(database_url)
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

    try:
        # Check if database is already initialized
        result = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'common'
                AND table_name = 'tenants'
            )
        """)

        if result:
            logger.info("Database already initialized (common.tenants exists)")
            return False

        logger.info("Running init.sql for fresh database setup...")
        content = init_sql_path.read_text()
        await conn.execute(content)
        logger.info("Successfully ran init.sql")
        return True

    finally:
        await conn.close()


async def ensure_database_ready(database_url: str):
    """
    Ensure database is ready by running init.sql if needed, then migrations.

    This is the main entry point for application startup.

    Args:
        database_url: PostgreSQL connection URL (SQLAlchemy or asyncpg format)
    """
    # Normalize URL once at the top level
    database_url = normalize_database_url(database_url)
    logger.info("Checking database state...")

    # Run init.sql if this is a fresh database
    await run_init_sql(database_url)

    # Run any pending migrations
    await run_migrations(database_url)

    logger.info("Database ready")
