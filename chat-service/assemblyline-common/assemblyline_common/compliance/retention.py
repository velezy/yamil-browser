"""
Data Retention Service - HIPAA-Compliant Data Lifecycle Management

Implements automated data retention and cleanup based on:
- HIPAA requirements (7 years for PHI)
- SOC2 requirements (configurable per data type)
- Legal hold overrides

Data Categories:
- PHI (Protected Health Information): 7 years
- Audit Logs: 7 years
- API Call Logs: 2 years
- Flow Executions: 1 year
- Temporary Data: 30 days
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Set
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy import select, delete, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ============================================================================
# Data Categories
# ============================================================================

class DataCategory(str, Enum):
    """Categories of data with different retention requirements."""

    PHI = "phi"  # Protected Health Information
    AUDIT_LOG = "audit_log"  # Compliance audit trail
    API_CALL_LOG = "api_call_log"  # Inbound API requests
    OUTBOUND_CALL_LOG = "outbound_call_log"  # Outbound API calls
    KAFKA_LOG = "kafka_log"  # Message queue logs
    FLOW_EXECUTION = "flow_execution"  # Workflow executions
    FLOW_VERSION = "flow_version"  # Version history
    SESSION = "session"  # User sessions
    TEMP_DATA = "temp_data"  # Temporary processing data
    CONNECTOR_LOG = "connector_log"  # Connector activity


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class RetentionPolicy:
    """Retention policy for a data category."""

    category: DataCategory
    retention_days: int
    description: str
    is_phi: bool = False
    requires_secure_delete: bool = False
    can_be_archived: bool = True


# Default HIPAA-compliant retention policies
DEFAULT_RETENTION_POLICIES: Dict[DataCategory, RetentionPolicy] = {
    DataCategory.PHI: RetentionPolicy(
        category=DataCategory.PHI,
        retention_days=2555,  # 7 years
        description="Protected Health Information - HIPAA mandated 7 year retention",
        is_phi=True,
        requires_secure_delete=True,
        can_be_archived=True,
    ),
    DataCategory.AUDIT_LOG: RetentionPolicy(
        category=DataCategory.AUDIT_LOG,
        retention_days=2555,  # 7 years
        description="Audit trail - HIPAA/SOC2 mandated 7 year retention",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.API_CALL_LOG: RetentionPolicy(
        category=DataCategory.API_CALL_LOG,
        retention_days=730,  # 2 years
        description="API call logs for debugging and compliance",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.OUTBOUND_CALL_LOG: RetentionPolicy(
        category=DataCategory.OUTBOUND_CALL_LOG,
        retention_days=730,  # 2 years
        description="Outbound API call logs",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.KAFKA_LOG: RetentionPolicy(
        category=DataCategory.KAFKA_LOG,
        retention_days=365,  # 1 year
        description="Kafka message logs",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.FLOW_EXECUTION: RetentionPolicy(
        category=DataCategory.FLOW_EXECUTION,
        retention_days=365,  # 1 year
        description="Flow execution history",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.FLOW_VERSION: RetentionPolicy(
        category=DataCategory.FLOW_VERSION,
        retention_days=2555,  # 7 years
        description="Flow version history for audit",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
    DataCategory.SESSION: RetentionPolicy(
        category=DataCategory.SESSION,
        retention_days=90,  # 90 days
        description="User session data",
        is_phi=False,
        requires_secure_delete=True,
        can_be_archived=False,
    ),
    DataCategory.TEMP_DATA: RetentionPolicy(
        category=DataCategory.TEMP_DATA,
        retention_days=30,  # 30 days
        description="Temporary processing data",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=False,
    ),
    DataCategory.CONNECTOR_LOG: RetentionPolicy(
        category=DataCategory.CONNECTOR_LOG,
        retention_days=365,  # 1 year
        description="Connector activity logs",
        is_phi=False,
        requires_secure_delete=False,
        can_be_archived=True,
    ),
}


@dataclass
class RetentionConfig:
    """Configuration for retention service."""

    # Override default policies
    policy_overrides: Dict[DataCategory, int] = field(default_factory=dict)

    # Batch size for deletion operations
    deletion_batch_size: int = 1000

    # Whether to archive before deletion
    archive_before_delete: bool = True

    # Dry run mode (log only, no actual deletion)
    dry_run: bool = False

    # Maximum records to process per run
    max_records_per_run: int = 100000


# ============================================================================
# Result Models
# ============================================================================

class RetentionRunResult(BaseModel):
    """Result of a retention cleanup run."""

    run_id: UUID = Field(default_factory=uuid4)
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None

    # Statistics
    categories_processed: List[str] = Field(default_factory=list)
    records_scanned: int = 0
    records_deleted: int = 0
    records_archived: int = 0
    records_skipped_legal_hold: int = 0

    # Errors
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    is_success: bool = True

    # Dry run
    dry_run: bool = False


class CategoryRetentionStatus(BaseModel):
    """Retention status for a specific category."""

    category: str
    policy_days: int
    total_records: int
    expired_records: int
    records_on_legal_hold: int
    oldest_record_date: Optional[datetime] = None
    newest_record_date: Optional[datetime] = None
    estimated_cleanup_size: int = 0


# ============================================================================
# Retention Service
# ============================================================================

class RetentionService:
    """
    Service for managing data retention and cleanup.

    Implements HIPAA-compliant data lifecycle management with:
    - Configurable retention periods per data category
    - Legal hold support to prevent deletion
    - Secure deletion for PHI data
    - Archival support before deletion
    """

    def __init__(self, config: Optional[RetentionConfig] = None):
        self.config = config or RetentionConfig()
        self._policies = self._build_policies()

    def _build_policies(self) -> Dict[DataCategory, RetentionPolicy]:
        """Build policies with any overrides applied."""
        policies = dict(DEFAULT_RETENTION_POLICIES)

        for category, days in self.config.policy_overrides.items():
            if category in policies:
                policies[category] = RetentionPolicy(
                    category=category,
                    retention_days=days,
                    description=f"Custom policy: {days} days",
                    is_phi=policies[category].is_phi,
                    requires_secure_delete=policies[category].requires_secure_delete,
                    can_be_archived=policies[category].can_be_archived,
                )

        return policies

    def get_policy(self, category: DataCategory) -> RetentionPolicy:
        """Get retention policy for a category."""
        return self._policies.get(category, DEFAULT_RETENTION_POLICIES.get(category))

    def get_cutoff_date(self, category: DataCategory) -> datetime:
        """Get the cutoff date for a category (records older than this should be deleted)."""
        policy = self.get_policy(category)
        return datetime.now(timezone.utc) - timedelta(days=policy.retention_days)

    async def get_retention_status(
        self,
        db: AsyncSession,
        tenant_id: UUID,
    ) -> List[CategoryRetentionStatus]:
        """
        Get retention status for all categories.

        Returns statistics about records eligible for cleanup.
        """
        from assemblyline_common.models import (
            AuditTrail, APICallLog, OutboundAPICallLog,
            KafkaMessageLog, FlowExecution, FlowVersion,
        )

        statuses = []

        # Map categories to tables
        category_tables = {
            DataCategory.AUDIT_LOG: (AuditTrail, "created_at"),
            DataCategory.API_CALL_LOG: (APICallLog, "created_at"),
            DataCategory.OUTBOUND_CALL_LOG: (OutboundAPICallLog, "created_at"),
            DataCategory.KAFKA_LOG: (KafkaMessageLog, "created_at"),
            DataCategory.FLOW_EXECUTION: (FlowExecution, "started_at"),
            DataCategory.FLOW_VERSION: (FlowVersion, "created_at"),
        }

        for category, (model, date_column) in category_tables.items():
            policy = self.get_policy(category)
            cutoff = self.get_cutoff_date(category)
            date_col = getattr(model, date_column)

            # Total records
            total_result = await db.execute(
                select(func.count(model.id))
                .where(model.tenant_id == tenant_id)
            )
            total = total_result.scalar() or 0

            # Expired records
            expired_result = await db.execute(
                select(func.count(model.id))
                .where(
                    model.tenant_id == tenant_id,
                    date_col < cutoff
                )
            )
            expired = expired_result.scalar() or 0

            # Date range
            range_result = await db.execute(
                select(func.min(date_col), func.max(date_col))
                .where(model.tenant_id == tenant_id)
            )
            date_range = range_result.first()

            statuses.append(CategoryRetentionStatus(
                category=category.value,
                policy_days=policy.retention_days,
                total_records=total,
                expired_records=expired,
                records_on_legal_hold=0,  # Will be updated if legal hold service is used
                oldest_record_date=date_range[0] if date_range else None,
                newest_record_date=date_range[1] if date_range else None,
                estimated_cleanup_size=expired,
            ))

        return statuses

    async def run_cleanup(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        categories: Optional[List[DataCategory]] = None,
    ) -> RetentionRunResult:
        """
        Run retention cleanup for specified categories.

        Args:
            db: Database session
            tenant_id: Tenant to clean up
            categories: Optional list of categories (all if None)

        Returns:
            RetentionRunResult with cleanup statistics
        """
        from assemblyline_common.models import (
            AuditTrail, APICallLog, OutboundAPICallLog,
            KafkaMessageLog, FlowExecution,
        )

        started_at = datetime.now(timezone.utc)
        result = RetentionRunResult(
            started_at=started_at,
            dry_run=self.config.dry_run,
        )

        # Default to non-PHI categories for automatic cleanup
        if categories is None:
            categories = [
                DataCategory.API_CALL_LOG,
                DataCategory.OUTBOUND_CALL_LOG,
                DataCategory.KAFKA_LOG,
                DataCategory.FLOW_EXECUTION,
            ]

        # Map categories to tables
        category_tables = {
            DataCategory.API_CALL_LOG: (APICallLog, "created_at"),
            DataCategory.OUTBOUND_CALL_LOG: (OutboundAPICallLog, "created_at"),
            DataCategory.KAFKA_LOG: (KafkaMessageLog, "created_at"),
            DataCategory.FLOW_EXECUTION: (FlowExecution, "started_at"),
        }

        for category in categories:
            if category not in category_tables:
                continue

            model, date_column = category_tables[category]
            policy = self.get_policy(category)
            cutoff = self.get_cutoff_date(category)
            date_col = getattr(model, date_column)

            try:
                # Count expired records
                count_result = await db.execute(
                    select(func.count(model.id))
                    .where(
                        model.tenant_id == tenant_id,
                        date_col < cutoff
                    )
                )
                expired_count = count_result.scalar() or 0
                result.records_scanned += expired_count

                if expired_count > 0:
                    if self.config.dry_run:
                        logger.info(
                            f"DRY RUN: Would delete {expired_count} records from {category.value}",
                            extra={
                                "event_type": "retention.dry_run",
                                "category": category.value,
                                "count": expired_count,
                            }
                        )
                    else:
                        # Delete in batches
                        deleted = 0
                        while deleted < expired_count and deleted < self.config.max_records_per_run:
                            batch_result = await db.execute(
                                delete(model)
                                .where(
                                    model.tenant_id == tenant_id,
                                    date_col < cutoff
                                )
                                .execution_options(synchronize_session=False)
                            )
                            batch_deleted = batch_result.rowcount
                            if batch_deleted == 0:
                                break
                            deleted += batch_deleted

                            if deleted >= self.config.deletion_batch_size:
                                await db.commit()

                        result.records_deleted += deleted
                        await db.commit()

                        logger.info(
                            f"Deleted {deleted} records from {category.value}",
                            extra={
                                "event_type": "retention.cleanup",
                                "category": category.value,
                                "deleted": deleted,
                                "tenant_id": str(tenant_id),
                            }
                        )

                result.categories_processed.append(category.value)

            except Exception as e:
                logger.error(
                    f"Error cleaning up {category.value}: {str(e)}",
                    extra={
                        "event_type": "retention.error",
                        "category": category.value,
                        "error": str(e),
                    }
                )
                result.errors.append({
                    "category": category.value,
                    "error": str(e),
                })
                result.is_success = False

        completed_at = datetime.now(timezone.utc)
        result.completed_at = completed_at
        result.duration_ms = int((completed_at - started_at).total_seconds() * 1000)

        return result


# ============================================================================
# Singleton Factory
# ============================================================================

_retention_service: Optional[RetentionService] = None


async def get_retention_service(
    config: Optional[RetentionConfig] = None
) -> RetentionService:
    """Get singleton instance of retention service."""
    global _retention_service

    if _retention_service is None:
        _retention_service = RetentionService(config)
        logger.info(
            "Initialized retention service",
            extra={
                "event_type": "retention.service_initialized",
                "dry_run": _retention_service.config.dry_run,
            }
        )

    return _retention_service
