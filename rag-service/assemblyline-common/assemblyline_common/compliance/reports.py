"""
Compliance Reports - HIPAA Access Reports and SOC2 Evidence

Generates compliance reports for:
- HIPAA: PHI access reports, breach notification data
- SOC2 CC6.1: Access control evidence
- Authentication failure reports
- Data retention status
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Optional, List, Dict, Any, Tuple
from uuid import UUID, uuid4

from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, or_, case, distinct
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ============================================================================
# Report Types
# ============================================================================

class ReportType(str, Enum):
    """Types of compliance reports."""
    HIPAA_ACCESS = "hipaa_access"
    SOC2_ACCESS_CONTROL = "soc2_access_control"
    AUTH_FAILURE = "auth_failure"
    DATA_RETENTION = "data_retention"
    PHI_ACCESS_SUMMARY = "phi_access_summary"
    USER_ACTIVITY = "user_activity"


class ReportFormat(str, Enum):
    """Output formats for reports."""
    JSON = "json"
    CSV = "csv"
    PDF = "pdf"


# ============================================================================
# HIPAA Access Report
# ============================================================================

class PHIAccessEntry(BaseModel):
    """Single PHI access event."""
    timestamp: datetime
    user_id: Optional[UUID] = None
    user_email: Optional[str] = None
    api_key_id: Optional[UUID] = None
    ip_address: Optional[str] = None
    action: str
    resource_type: str
    resource_id: Optional[str] = None
    phi_types: List[str] = Field(default_factory=list)
    details: Dict[str, Any] = Field(default_factory=dict)


class HIPAAAccessReport(BaseModel):
    """
    HIPAA Access Report - Required for breach notification and audits.

    Shows all access to Protected Health Information including:
    - Who accessed the data
    - What data was accessed
    - When and from where
    - What action was performed
    """
    report_id: UUID = Field(default_factory=uuid4)
    report_type: str = "hipaa_access"
    tenant_id: UUID
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Report period
    start_date: datetime
    end_date: datetime

    # Summary statistics
    total_phi_accesses: int = 0
    unique_users: int = 0
    unique_resources: int = 0

    # PHI types accessed
    phi_types_accessed: Dict[str, int] = Field(default_factory=dict)

    # Access breakdown by action
    actions_breakdown: Dict[str, int] = Field(default_factory=dict)

    # Access breakdown by user
    user_breakdown: List[Dict[str, Any]] = Field(default_factory=list)

    # Detailed access log
    access_entries: List[PHIAccessEntry] = Field(default_factory=list)


# ============================================================================
# SOC2 Access Control Report
# ============================================================================

class AccessControlEntry(BaseModel):
    """Access control evidence entry."""
    timestamp: datetime
    event_type: str  # login, logout, permission_change, etc.
    user_id: Optional[UUID] = None
    user_email: Optional[str] = None
    ip_address: Optional[str] = None
    success: bool
    details: Dict[str, Any] = Field(default_factory=dict)


class SOC2AccessControlReport(BaseModel):
    """
    SOC2 CC6.1 Access Control Evidence Report.

    Provides evidence for:
    - Logical and physical access controls
    - Access authentication and authorization
    - Access removal procedures
    """
    report_id: UUID = Field(default_factory=uuid4)
    report_type: str = "soc2_access_control"
    tenant_id: UUID
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Report period
    start_date: datetime
    end_date: datetime

    # User management
    total_users: int = 0
    active_users: int = 0
    disabled_users: int = 0
    users_with_mfa: int = 0
    mfa_percentage: float = 0.0

    # Access attempts
    successful_logins: int = 0
    failed_logins: int = 0
    locked_accounts: int = 0

    # Permission changes
    permission_changes: int = 0
    role_changes: int = 0

    # API key management
    total_api_keys: int = 0
    active_api_keys: int = 0
    expired_api_keys: int = 0

    # Detailed entries
    entries: List[AccessControlEntry] = Field(default_factory=list)


# ============================================================================
# Authentication Failure Report
# ============================================================================

class AuthFailureEntry(BaseModel):
    """Authentication failure entry."""
    timestamp: datetime
    email: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    failure_reason: str
    attempt_count: int = 1


class AuthenticationFailureReport(BaseModel):
    """
    Authentication Failure Report.

    Tracks failed login attempts for:
    - Security monitoring
    - Brute force detection
    - Account lockout analysis
    """
    report_id: UUID = Field(default_factory=uuid4)
    report_type: str = "auth_failure"
    tenant_id: UUID
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Report period
    start_date: datetime
    end_date: datetime

    # Summary
    total_failures: int = 0
    unique_ips: int = 0
    unique_emails: int = 0
    accounts_locked: int = 0

    # Failures by reason
    failure_reasons: Dict[str, int] = Field(default_factory=dict)

    # Top offending IPs
    top_ips: List[Dict[str, Any]] = Field(default_factory=list)

    # Detailed entries
    entries: List[AuthFailureEntry] = Field(default_factory=list)


# ============================================================================
# Data Retention Report
# ============================================================================

class DataRetentionReport(BaseModel):
    """
    Data Retention Status Report.

    Shows compliance with retention policies:
    - Current data volumes
    - Expired data pending cleanup
    - Legal holds in effect
    """
    report_id: UUID = Field(default_factory=uuid4)
    report_type: str = "data_retention"
    tenant_id: UUID
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Category status
    categories: List[Dict[str, Any]] = Field(default_factory=list)

    # Legal holds
    active_legal_holds: int = 0
    records_on_hold: int = 0

    # Cleanup history
    last_cleanup_at: Optional[datetime] = None
    records_deleted_30d: int = 0
    records_archived_30d: int = 0


# ============================================================================
# Compliance Report Service
# ============================================================================

@dataclass
class ReportConfig:
    """Configuration for report generation."""
    max_entries: int = 10000
    include_details: bool = True
    anonymize_users: bool = False


class ComplianceReportService:
    """
    Service for generating compliance reports.

    Generates reports required for:
    - HIPAA audits and breach notification
    - SOC2 Type II evidence
    - Internal security monitoring
    """

    def __init__(self, config: Optional[ReportConfig] = None):
        self.config = config or ReportConfig()

    async def generate_hipaa_access_report(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        start_date: datetime,
        end_date: datetime,
    ) -> HIPAAAccessReport:
        """
        Generate HIPAA PHI Access Report.

        Shows all access to Protected Health Information during the period.
        """
        from assemblyline_common.models import AuditTrail, User

        report = HIPAAAccessReport(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
        )

        # Get PHI access entries
        result = await db.execute(
            select(AuditTrail)
            .where(
                AuditTrail.tenant_id == tenant_id,
                AuditTrail.contains_phi == True,
                AuditTrail.created_at >= start_date,
                AuditTrail.created_at <= end_date,
            )
            .order_by(AuditTrail.created_at.desc())
            .limit(self.config.max_entries)
        )
        entries = result.scalars().all()

        # Build summary
        phi_types_count: Dict[str, int] = {}
        actions_count: Dict[str, int] = {}
        user_access: Dict[str, int] = {}
        unique_resources: set = set()

        for entry in entries:
            # Count PHI types
            for phi_type in (entry.phi_types or []):
                phi_types_count[phi_type] = phi_types_count.get(phi_type, 0) + 1

            # Count actions
            actions_count[entry.action] = actions_count.get(entry.action, 0) + 1

            # Count by user
            user_key = str(entry.user_id) if entry.user_id else "api_key"
            user_access[user_key] = user_access.get(user_key, 0) + 1

            # Track unique resources
            if entry.resource_id:
                unique_resources.add(entry.resource_id)

            # Add to entries list
            if self.config.include_details:
                report.access_entries.append(PHIAccessEntry(
                    timestamp=entry.created_at,
                    user_id=entry.user_id,
                    api_key_id=entry.api_key_id,
                    ip_address=str(entry.ip_address) if entry.ip_address else None,
                    action=entry.action,
                    resource_type=entry.resource_type,
                    resource_id=entry.resource_id,
                    phi_types=entry.phi_types or [],
                    details=entry.details or {},
                ))

        report.total_phi_accesses = len(entries)
        report.unique_users = len(user_access)
        report.unique_resources = len(unique_resources)
        report.phi_types_accessed = phi_types_count
        report.actions_breakdown = actions_count

        # Build user breakdown
        for user_key, count in sorted(user_access.items(), key=lambda x: -x[1])[:10]:
            report.user_breakdown.append({
                "user_id": user_key,
                "access_count": count,
            })

        logger.info(
            "Generated HIPAA access report",
            extra={
                "event_type": "compliance.report_generated",
                "report_type": "hipaa_access",
                "tenant_id": str(tenant_id),
                "total_accesses": report.total_phi_accesses,
            }
        )

        return report

    async def generate_soc2_access_control_report(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        start_date: datetime,
        end_date: datetime,
    ) -> SOC2AccessControlReport:
        """
        Generate SOC2 CC6.1 Access Control Evidence Report.

        Provides evidence of access control implementation.
        """
        from assemblyline_common.models import User, APIKey, AuditTrail

        report = SOC2AccessControlReport(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
        )

        # User statistics
        user_result = await db.execute(
            select(
                func.count(User.id).label("total"),
                func.sum(case((User.is_active == True, 1), else_=0)).label("active"),
                func.sum(case((User.is_active == False, 1), else_=0)).label("disabled"),
                func.sum(case((User.mfa_enabled == True, 1), else_=0)).label("mfa"),
            )
            .where(User.tenant_id == tenant_id)
        )
        user_stats = user_result.first()

        report.total_users = user_stats.total or 0
        report.active_users = int(user_stats.active or 0)
        report.disabled_users = int(user_stats.disabled or 0)
        report.users_with_mfa = int(user_stats.mfa or 0)
        report.mfa_percentage = (
            (report.users_with_mfa / report.total_users * 100)
            if report.total_users > 0 else 0.0
        )

        # API key statistics
        key_result = await db.execute(
            select(
                func.count(APIKey.id).label("total"),
                func.sum(case((APIKey.is_active == True, 1), else_=0)).label("active"),
            )
            .where(APIKey.tenant_id == tenant_id)
        )
        key_stats = key_result.first()
        report.total_api_keys = key_stats.total or 0
        report.active_api_keys = int(key_stats.active or 0)

        # Login statistics from audit trail
        login_actions = ["login", "login_failed", "logout"]
        login_result = await db.execute(
            select(
                AuditTrail.action,
                func.count(AuditTrail.id)
            )
            .where(
                AuditTrail.tenant_id == tenant_id,
                AuditTrail.action.in_(login_actions),
                AuditTrail.created_at >= start_date,
                AuditTrail.created_at <= end_date,
            )
            .group_by(AuditTrail.action)
        )
        login_counts = dict(login_result.all())

        report.successful_logins = login_counts.get("login", 0)
        report.failed_logins = login_counts.get("login_failed", 0)

        # Permission changes
        perm_result = await db.execute(
            select(func.count(AuditTrail.id))
            .where(
                AuditTrail.tenant_id == tenant_id,
                AuditTrail.action.in_(["permission_change", "role_change"]),
                AuditTrail.created_at >= start_date,
                AuditTrail.created_at <= end_date,
            )
        )
        report.permission_changes = perm_result.scalar() or 0

        # Get detailed entries
        if self.config.include_details:
            entries_result = await db.execute(
                select(AuditTrail)
                .where(
                    AuditTrail.tenant_id == tenant_id,
                    AuditTrail.action.in_(login_actions + ["permission_change", "role_change", "mfa_enroll"]),
                    AuditTrail.created_at >= start_date,
                    AuditTrail.created_at <= end_date,
                )
                .order_by(AuditTrail.created_at.desc())
                .limit(self.config.max_entries)
            )

            for entry in entries_result.scalars().all():
                report.entries.append(AccessControlEntry(
                    timestamp=entry.created_at,
                    event_type=entry.action,
                    user_id=entry.user_id,
                    ip_address=str(entry.ip_address) if entry.ip_address else None,
                    success=entry.action not in ["login_failed"],
                    details=entry.details or {},
                ))

        logger.info(
            "Generated SOC2 access control report",
            extra={
                "event_type": "compliance.report_generated",
                "report_type": "soc2_access_control",
                "tenant_id": str(tenant_id),
                "total_users": report.total_users,
            }
        )

        return report

    async def generate_auth_failure_report(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        start_date: datetime,
        end_date: datetime,
    ) -> AuthenticationFailureReport:
        """
        Generate Authentication Failure Report.

        Shows all failed login attempts for security analysis.
        """
        from assemblyline_common.models import AuditTrail

        report = AuthenticationFailureReport(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
        )

        # Get failed logins
        result = await db.execute(
            select(AuditTrail)
            .where(
                AuditTrail.tenant_id == tenant_id,
                AuditTrail.action == "login_failed",
                AuditTrail.created_at >= start_date,
                AuditTrail.created_at <= end_date,
            )
            .order_by(AuditTrail.created_at.desc())
            .limit(self.config.max_entries)
        )
        entries = result.scalars().all()

        # Analyze failures
        unique_ips: set = set()
        unique_emails: set = set()
        reason_counts: Dict[str, int] = {}
        ip_counts: Dict[str, int] = {}

        for entry in entries:
            ip = str(entry.ip_address) if entry.ip_address else "unknown"
            unique_ips.add(ip)
            ip_counts[ip] = ip_counts.get(ip, 0) + 1

            email = entry.details.get("email") if entry.details else None
            if email:
                unique_emails.add(email)

            reason = entry.details.get("reason", "unknown") if entry.details else "unknown"
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

            if self.config.include_details:
                report.entries.append(AuthFailureEntry(
                    timestamp=entry.created_at,
                    email=email,
                    ip_address=ip,
                    user_agent=entry.user_agent,
                    failure_reason=reason,
                ))

        report.total_failures = len(entries)
        report.unique_ips = len(unique_ips)
        report.unique_emails = len(unique_emails)
        report.failure_reasons = reason_counts

        # Top offending IPs
        report.top_ips = [
            {"ip": ip, "count": count}
            for ip, count in sorted(ip_counts.items(), key=lambda x: -x[1])[:10]
        ]

        logger.info(
            "Generated authentication failure report",
            extra={
                "event_type": "compliance.report_generated",
                "report_type": "auth_failure",
                "tenant_id": str(tenant_id),
                "total_failures": report.total_failures,
            }
        )

        return report

    async def generate_data_retention_report(
        self,
        db: AsyncSession,
        tenant_id: UUID,
    ) -> DataRetentionReport:
        """
        Generate Data Retention Status Report.

        Shows current data volumes and retention compliance.
        """
        from assemblyline_common.compliance.retention import get_retention_service
        from assemblyline_common.compliance.legal_hold import get_legal_hold_service

        report = DataRetentionReport(tenant_id=tenant_id)

        # Get retention status
        retention_service = await get_retention_service()
        statuses = await retention_service.get_retention_status(db, tenant_id)

        for status in statuses:
            report.categories.append({
                "category": status.category,
                "policy_days": status.policy_days,
                "total_records": status.total_records,
                "expired_records": status.expired_records,
                "oldest_record": status.oldest_record_date.isoformat() if status.oldest_record_date else None,
                "newest_record": status.newest_record_date.isoformat() if status.newest_record_date else None,
            })

        # Get legal holds
        legal_hold_service = await get_legal_hold_service()
        hold_summary = await legal_hold_service.get_summary(tenant_id)
        report.active_legal_holds = hold_summary.active_holds

        logger.info(
            "Generated data retention report",
            extra={
                "event_type": "compliance.report_generated",
                "report_type": "data_retention",
                "tenant_id": str(tenant_id),
                "categories": len(report.categories),
            }
        )

        return report


# ============================================================================
# Singleton Factory
# ============================================================================

_compliance_report_service: Optional[ComplianceReportService] = None


async def get_compliance_report_service(
    config: Optional[ReportConfig] = None
) -> ComplianceReportService:
    """Get singleton instance of compliance report service."""
    global _compliance_report_service

    if _compliance_report_service is None:
        _compliance_report_service = ComplianceReportService(config)
        logger.info(
            "Initialized compliance report service",
            extra={"event_type": "compliance.service_initialized"}
        )

    return _compliance_report_service
