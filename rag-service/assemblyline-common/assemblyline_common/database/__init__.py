"""
Database module for Logic Weaver.

Two connection backends:
- SQLAlchemy async (connection.py) — used by most services
- Raw asyncpg pool (asyncpg_connection.py) — used by legacy services (chat, document, etc.)

Pydantic models and repositories are available for services using the asyncpg backend.
"""

from assemblyline_common.database.connection import (
    get_db,
    get_serializable_db,
    get_tenant_db,
    get_engine,
    get_session_factory,
    create_tenant_schema,
    close_engine,
)

# asyncpg-based connection (legacy services)
from assemblyline_common.database.asyncpg_connection import (
    get_db_pool,
    close_db_pool,
    get_connection,
)

__all__ = [
    # SQLAlchemy async
    "get_db",
    "get_serializable_db",
    "get_tenant_db",
    "get_engine",
    "get_session_factory",
    "create_tenant_schema",
    "close_engine",
    # asyncpg pool
    "get_db_pool",
    "close_db_pool",
    "get_connection",
]
