"""
Logic Weaver Common Library

Shared utilities, models, and functions used across all Logic Weaver services.
"""

__version__ = "1.0.0"
__author__ = "Logic Weaver Team"

from assemblyline_common.config import settings
from assemblyline_common.database.connection import get_db, get_tenant_db
from assemblyline_common.auth.dependencies import get_current_user, get_current_tenant

__all__ = [
    "settings",
    "get_db",
    "get_tenant_db",
    "get_current_user",
    "get_current_tenant",
    # Agentic AI platform clients (lazy imports — use directly from submodules)
    # from assemblyline_common.memory_client import MemoryClient
    # from assemblyline_common.evaluation_client import EvaluationClient
    # from assemblyline_common.hitl_client import HITLClient
    # from assemblyline_common.feedback_client import FeedbackClient
]
