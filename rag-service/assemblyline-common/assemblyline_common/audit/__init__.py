"""
Audit Module - Immutable Audit Logging with Hash Chain Verification

Provides HIPAA/SOC2 compliant audit logging with:
- Hash chain verification (tamper detection)
- PHI access tracking
- Compliance reporting
"""

from assemblyline_common.audit.immutable_audit import (
    ImmutableAuditConfig,
    ImmutableAuditService,
    AuditEntry,
    ChainVerificationResult,
    get_immutable_audit_service,
)

__all__ = [
    "ImmutableAuditConfig",
    "ImmutableAuditService",
    "AuditEntry",
    "ChainVerificationResult",
    "get_immutable_audit_service",
]
