"""
Pytest fixtures for RAG service tests
"""
import pytest
import asyncio
import os
import sys
import numpy as np
from unittest.mock import AsyncMock, MagicMock

# Add project paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))


@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_db_pool():
    """Mock asyncpg database pool with acquire context manager.

    asyncpg pool.acquire() returns an async context manager directly
    (not a coroutine that returns one), so we mock it accordingly.
    """
    pool = MagicMock()
    conn = AsyncMock()

    # pool.acquire() returns an async context manager
    acm = AsyncMock()
    acm.__aenter__.return_value = conn
    acm.__aexit__.return_value = None
    pool.acquire.return_value = acm

    # conn.transaction() must return an async context manager directly.
    # Since conn is AsyncMock, conn.transaction() returns a coroutine by default.
    # Override it with a MagicMock so it returns the context manager synchronously.
    txn = MagicMock()
    txn.__aenter__ = AsyncMock(return_value=None)
    txn.__aexit__ = AsyncMock(return_value=None)
    conn.transaction = MagicMock(return_value=txn)

    return pool, conn


@pytest.fixture
def mock_llm_client():
    """Mock LLM client for cluster naming"""
    client = AsyncMock()
    client.generate = AsyncMock(
        return_value="Name: Technical Reports\nDescription: Documents containing technical analysis and reports"
    )
    return client


@pytest.fixture
def sample_embeddings():
    """Generate sample embedding vectors for testing"""
    np.random.seed(42)

    def make_cluster(center, n_docs, spread=0.1):
        """Generate n_docs embeddings clustered around center"""
        return [center + np.random.randn(128) * spread for _ in range(n_docs)]

    # 3 distinct clusters
    cluster_a = make_cluster(np.ones(128) * 0.5, 5, spread=0.05)
    cluster_b = make_cluster(np.ones(128) * -0.5, 4, spread=0.05)
    cluster_c = make_cluster(np.zeros(128), 3, spread=0.05)

    return {
        "cluster_a": cluster_a,
        "cluster_b": cluster_b,
        "cluster_c": cluster_c,
        "all": cluster_a + cluster_b + cluster_c,
    }
