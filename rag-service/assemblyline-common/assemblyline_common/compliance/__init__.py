"""
Compliance Module - HIPAA/SOC2 Data Retention and Reporting

Provides:
- Data retention policy enforcement
- Legal hold management
- HIPAA access reports
- SOC2 compliance evidence
- Authentication failure reports
"""

from assemblyline_common.compliance.retention import (
    RetentionPolicy,
    RetentionConfig,
    DataCategory,
    RetentionService,
    get_retention_service,
)
from assemblyline_common.compliance.legal_hold import (
    LegalHold,
    LegalHoldService,
    get_legal_hold_service,
)
from assemblyline_common.compliance.reports import (
    ComplianceReportService,
    HIPAAAccessReport,
    SOC2AccessControlReport,
    AuthenticationFailureReport,
    DataRetentionReport,
    get_compliance_report_service,
)

__all__ = [
    # Retention
    "RetentionPolicy",
    "RetentionConfig",
    "DataCategory",
    "RetentionService",
    "get_retention_service",
    # Legal Hold
    "LegalHold",
    "LegalHoldService",
    "get_legal_hold_service",
    # Reports
    "ComplianceReportService",
    "HIPAAAccessReport",
    "SOC2AccessControlReport",
    "AuthenticationFailureReport",
    "DataRetentionReport",
    "get_compliance_report_service",
]
