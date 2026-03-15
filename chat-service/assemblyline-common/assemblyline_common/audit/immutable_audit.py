"""
Immutable Audit Log with Hash Chain Verification

Provides tamper-evident audit logging using cryptographic hash chains.
Each audit entry includes a hash computed from:
- Previous entry's hash (genesis block uses zero hash)
- Timestamp
- Tenant ID
- Sequence number
- Canonical JSON of entry data

This creates a blockchain-like structure where any modification
to historical entries will be detected during verification.

PHI Protection (HIPAA §164.312(e)(2)(ii)):
When contains_phi=True, the details field is encrypted with AES-256-GCM
before storage. The hash chain is computed on the plaintext details so
that verification requires decryption (authorized access only).
"""

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any, Dict, List, Tuple
from uuid import UUID, uuid4
from enum import Enum

from pydantic import BaseModel, Field
from sqlalchemy import select, func, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


# ============================================================================
# Configuration
# ============================================================================

@dataclass
class ImmutableAuditConfig:
    """Configuration for immutable audit logging."""

    # Hash algorithm (SHA-256 recommended for HIPAA)
    hash_algorithm: str = "sha256"

    # Genesis block hash (first entry in chain)
    genesis_hash: str = "0" * 64

    # Batch size for verification
    verification_batch_size: int = 1000

    # Whether to fail on first verification error or continue
    fail_fast_verification: bool = False

    # Maximum entries to verify in single request (prevent DoS)
    max_verification_entries: int = 100000

    # Enable async verification for large datasets
    async_verification_threshold: int = 10000

    # Encrypt PHI details with AES-256-GCM (HIPAA §164.312(e)(2)(ii))
    encrypt_phi_details: bool = True


# ============================================================================
# Models
# ============================================================================

class AuditAction(str, Enum):
    """Standard audit actions."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    LOGIN_FAILED = "login_failed"
    MFA_ENROLL = "mfa_enroll"
    MFA_VERIFY = "mfa_verify"
    PASSWORD_CHANGE = "password_change"
    PERMISSION_CHANGE = "permission_change"
    EXPORT = "export"
    IMPORT = "import"
    API_CALL = "api_call"
    FLOW_EXECUTE = "flow_execute"
    PHI_ACCESS = "phi_access"
    SAML_LOGIN = "saml_login"


class AuditEntry(BaseModel):
    """Audit entry for hash chain computation."""

    id: UUID = Field(default_factory=uuid4)
    tenant_id: UUID
    sequence_number: int

    # Actor information
    user_id: Optional[UUID] = None
    api_key_id: Optional[UUID] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None

    # Action details
    action: str
    resource_type: str
    resource_id: Optional[str] = None

    # PHI tracking
    contains_phi: bool = False
    phi_types: List[str] = Field(default_factory=list)

    # Additional details
    details: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[UUID] = None

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Hash chain fields
    prev_hash: str = ""
    entry_hash: str = ""

    class Config:
        json_encoders = {
            UUID: str,
            datetime: lambda v: v.isoformat()
        }


class ChainVerificationResult(BaseModel):
    """Result of hash chain verification."""

    is_valid: bool
    entries_verified: int
    first_entry_id: Optional[UUID] = None
    last_entry_id: Optional[UUID] = None
    first_sequence: Optional[int] = None
    last_sequence: Optional[int] = None

    # Error details (if invalid)
    error_count: int = 0
    errors: List[Dict[str, Any]] = Field(default_factory=list)

    # Timing
    verification_started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    verification_completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None


# ============================================================================
# Hash Chain Service
# ============================================================================

class ImmutableAuditService:
    """
    Service for creating and verifying immutable audit entries.

    Uses SHA-256 hash chain where each entry's hash is computed from:
    - Previous entry's hash
    - Current entry's timestamp
    - Tenant ID
    - Sequence number
    - Canonical JSON of entry data

    This ensures that:
    1. Entries cannot be modified without detection
    2. Entries cannot be deleted without breaking the chain
    3. Entries cannot be reordered without detection
    """

    def __init__(self, config: Optional[ImmutableAuditConfig] = None):
        self.config = config or ImmutableAuditConfig()
        self._hash_func = getattr(hashlib, self.config.hash_algorithm)
        self._encryption_service = None

    def _get_encryption_service(self):
        """Lazy-load encryption service to avoid circular imports."""
        if self._encryption_service is None:
            from assemblyline_common.crypto.encryption import get_encryption_service
            self._encryption_service = get_encryption_service()
        return self._encryption_service

    def _encrypt_details(self, details: Dict[str, Any], entry_id: UUID, tenant_id: UUID) -> str:
        """
        Encrypt audit details using AES-256-GCM.

        Uses entry_id + tenant_id as associated data to bind the
        ciphertext to this specific audit entry (prevents relocation attacks).
        """
        enc = self._get_encryption_service()
        plaintext = json.dumps(details, sort_keys=True, default=str)
        associated_data = f"{entry_id}:{tenant_id}".encode("utf-8")
        return enc.encrypt(plaintext, associated_data=associated_data)

    def _decrypt_details(self, encrypted: str, entry_id: UUID, tenant_id: UUID) -> Dict[str, Any]:
        """
        Decrypt audit details.

        Raises cryptography.exceptions.InvalidTag if tampered.
        """
        enc = self._get_encryption_service()
        associated_data = f"{entry_id}:{tenant_id}".encode("utf-8")
        plaintext = enc.decrypt(encrypted, associated_data=associated_data)
        return json.loads(plaintext)

    def compute_entry_hash(
        self,
        prev_hash: str,
        timestamp: datetime,
        tenant_id: UUID,
        sequence_number: int,
        data: Dict[str, Any]
    ) -> str:
        """
        Compute hash for an audit entry.

        The hash is computed from a canonical representation of:
        - Previous hash
        - ISO timestamp
        - Tenant ID
        - Sequence number
        - Sorted JSON of entry data
        """
        # Create canonical string representation
        canonical_data = {
            "prev_hash": prev_hash,
            "timestamp": timestamp.isoformat(),
            "tenant_id": str(tenant_id),
            "sequence_number": sequence_number,
            "data": self._canonicalize_dict(data)
        }

        # Convert to canonical JSON (sorted keys, no whitespace)
        canonical_json = json.dumps(canonical_data, sort_keys=True, separators=(',', ':'))

        # Compute hash
        hash_bytes = self._hash_func(canonical_json.encode('utf-8')).hexdigest()

        return hash_bytes

    def _canonicalize_dict(self, d: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a canonical representation of a dictionary.

        - Sorts keys
        - Converts UUIDs to strings
        - Converts datetimes to ISO format
        - Recursively processes nested dicts/lists
        """
        result = {}
        for key in sorted(d.keys()):
            value = d[key]
            if isinstance(value, UUID):
                result[key] = str(value)
            elif isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = self._canonicalize_dict(value)
            elif isinstance(value, list):
                result[key] = [
                    self._canonicalize_dict(v) if isinstance(v, dict)
                    else str(v) if isinstance(v, (UUID, datetime))
                    else v
                    for v in value
                ]
            else:
                result[key] = value
        return result

    async def create_entry(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        action: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        user_id: Optional[UUID] = None,
        api_key_id: Optional[UUID] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        contains_phi: bool = False,
        phi_types: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[UUID] = None,
    ) -> AuditEntry:
        """
        Create a new immutable audit entry.

        This method:
        1. Gets the next sequence number for the tenant
        2. Gets the previous entry's hash (or genesis hash)
        3. Computes the entry hash
        4. Inserts the entry into the database

        Uses row-level locking to ensure sequence integrity.
        """
        from assemblyline_common.models import AuditTrail

        phi_types = phi_types or []
        details = details or {}

        # Get next sequence number and previous hash atomically
        # Use SELECT FOR UPDATE to prevent race conditions
        result = await db.execute(
            select(
                AuditTrail.sequence_number,
                AuditTrail.entry_hash
            )
            .where(AuditTrail.tenant_id == tenant_id)
            .order_by(AuditTrail.sequence_number.desc())
            .limit(1)
            .with_for_update()
        )
        last_entry = result.first()

        if last_entry:
            sequence_number = last_entry.sequence_number + 1
            prev_hash = last_entry.entry_hash
        else:
            sequence_number = 1
            prev_hash = self.config.genesis_hash

        # Prepare entry data for hashing
        entry_id = uuid4()
        created_at = datetime.now(timezone.utc)

        entry_data = {
            "id": str(entry_id),
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "user_id": str(user_id) if user_id else None,
            "api_key_id": str(api_key_id) if api_key_id else None,
            "ip_address": ip_address,
            "user_agent": user_agent,
            "contains_phi": contains_phi,
            "phi_types": phi_types,
            "details": details,
            "correlation_id": str(correlation_id) if correlation_id else None,
        }

        # Compute entry hash
        entry_hash = self.compute_entry_hash(
            prev_hash=prev_hash,
            timestamp=created_at,
            tenant_id=tenant_id,
            sequence_number=sequence_number,
            data=entry_data
        )

        # Encrypt PHI details before storage (HIPAA §164.312(e)(2)(ii))
        encrypted_details_value = None
        stored_details = details
        if contains_phi and details and self.config.encrypt_phi_details:
            try:
                encrypted_details_value = self._encrypt_details(details, entry_id, tenant_id)
                # Store redacted marker in JSONB column; real data is in encrypted_details
                stored_details = {"_encrypted": True, "_phi_types": phi_types}
            except Exception as e:
                logger.error(
                    "Failed to encrypt PHI audit details, storing unencrypted",
                    extra={
                        "event_type": "audit.encryption_failed",
                        "entry_id": str(entry_id),
                        "error": str(e),
                    }
                )

        # Create database entry
        audit_entry = AuditTrail(
            id=entry_id,
            tenant_id=tenant_id,
            user_id=user_id,
            api_key_id=api_key_id,
            ip_address=ip_address,
            user_agent=user_agent,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            contains_phi=contains_phi,
            phi_types=phi_types,
            details=stored_details,
            encrypted_details=encrypted_details_value,
            correlation_id=correlation_id,
            sequence_number=sequence_number,
            prev_hash=prev_hash,
            entry_hash=entry_hash,
            created_at=created_at,
        )

        db.add(audit_entry)
        await db.flush()

        logger.info(
            "Created immutable audit entry",
            extra={
                "event_type": "audit.entry_created",
                "entry_id": str(entry_id),
                "tenant_id": str(tenant_id),
                "sequence_number": sequence_number,
                "action": action,
                "resource_type": resource_type,
                "contains_phi": contains_phi,
            }
        )

        return AuditEntry(
            id=entry_id,
            tenant_id=tenant_id,
            sequence_number=sequence_number,
            user_id=user_id,
            api_key_id=api_key_id,
            ip_address=ip_address,
            user_agent=user_agent,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            contains_phi=contains_phi,
            phi_types=phi_types,
            details=details,
            correlation_id=correlation_id,
            created_at=created_at,
            prev_hash=prev_hash,
            entry_hash=entry_hash,
        )

    async def verify_chain(
        self,
        db: AsyncSession,
        tenant_id: UUID,
        start_sequence: Optional[int] = None,
        end_sequence: Optional[int] = None,
    ) -> ChainVerificationResult:
        """
        Verify the integrity of the audit chain for a tenant.

        Checks that:
        1. Sequence numbers are contiguous
        2. Each entry's prev_hash matches the previous entry's entry_hash
        3. Each entry's entry_hash is correctly computed

        Returns a verification result with details of any errors.
        """
        from assemblyline_common.models import AuditTrail

        verification_started = datetime.now(timezone.utc)
        errors: List[Dict[str, Any]] = []

        # Build query
        query = (
            select(AuditTrail)
            .where(AuditTrail.tenant_id == tenant_id)
            .order_by(AuditTrail.sequence_number)
        )

        if start_sequence is not None:
            query = query.where(AuditTrail.sequence_number >= start_sequence)

        if end_sequence is not None:
            query = query.where(AuditTrail.sequence_number <= end_sequence)

        query = query.limit(self.config.max_verification_entries)

        result = await db.execute(query)
        entries = result.scalars().all()

        if not entries:
            return ChainVerificationResult(
                is_valid=True,
                entries_verified=0,
                verification_started_at=verification_started,
                verification_completed_at=datetime.now(timezone.utc),
                duration_ms=int((datetime.now(timezone.utc) - verification_started).total_seconds() * 1000),
            )

        first_entry = entries[0]
        last_entry = entries[-1]

        # Verify chain
        expected_prev_hash = self.config.genesis_hash if first_entry.sequence_number == 1 else None
        expected_sequence = first_entry.sequence_number

        for entry in entries:
            # Check sequence continuity
            if entry.sequence_number != expected_sequence:
                errors.append({
                    "type": "sequence_gap",
                    "entry_id": str(entry.id),
                    "expected_sequence": expected_sequence,
                    "actual_sequence": entry.sequence_number,
                    "message": f"Sequence gap: expected {expected_sequence}, got {entry.sequence_number}"
                })
                if self.config.fail_fast_verification:
                    break

            # Check prev_hash linkage
            if expected_prev_hash is not None and entry.prev_hash != expected_prev_hash:
                errors.append({
                    "type": "prev_hash_mismatch",
                    "entry_id": str(entry.id),
                    "sequence_number": entry.sequence_number,
                    "expected_prev_hash": expected_prev_hash[:16] + "...",
                    "actual_prev_hash": entry.prev_hash[:16] + "..." if entry.prev_hash else None,
                    "message": f"Previous hash mismatch at sequence {entry.sequence_number}"
                })
                if self.config.fail_fast_verification:
                    break

            # Recompute and verify entry hash
            # If details were encrypted, decrypt to get original plaintext for hash verification
            verification_details = entry.details or {}
            if entry.encrypted_details and self.config.encrypt_phi_details:
                try:
                    verification_details = self._decrypt_details(
                        entry.encrypted_details, entry.id, entry.tenant_id
                    )
                except Exception as e:
                    errors.append({
                        "type": "decryption_failed",
                        "entry_id": str(entry.id),
                        "sequence_number": entry.sequence_number,
                        "message": f"Failed to decrypt PHI details at sequence {entry.sequence_number}: {e}"
                    })
                    if self.config.fail_fast_verification:
                        break
                    expected_prev_hash = entry.entry_hash
                    expected_sequence = entry.sequence_number + 1
                    continue

            entry_data = {
                "id": str(entry.id),
                "action": entry.action,
                "resource_type": entry.resource_type,
                "resource_id": entry.resource_id,
                "user_id": str(entry.user_id) if entry.user_id else None,
                "api_key_id": str(entry.api_key_id) if entry.api_key_id else None,
                "ip_address": str(entry.ip_address) if entry.ip_address else None,
                "user_agent": entry.user_agent,
                "contains_phi": entry.contains_phi,
                "phi_types": entry.phi_types or [],
                "details": verification_details,
                "correlation_id": str(entry.correlation_id) if entry.correlation_id else None,
            }

            computed_hash = self.compute_entry_hash(
                prev_hash=entry.prev_hash,
                timestamp=entry.created_at,
                tenant_id=entry.tenant_id,
                sequence_number=entry.sequence_number,
                data=entry_data
            )

            if computed_hash != entry.entry_hash:
                errors.append({
                    "type": "entry_hash_mismatch",
                    "entry_id": str(entry.id),
                    "sequence_number": entry.sequence_number,
                    "expected_hash": computed_hash[:16] + "...",
                    "actual_hash": entry.entry_hash[:16] + "..." if entry.entry_hash else None,
                    "message": f"Entry hash mismatch at sequence {entry.sequence_number} - possible tampering detected"
                })
                if self.config.fail_fast_verification:
                    break

            # Update expectations for next iteration
            expected_prev_hash = entry.entry_hash
            expected_sequence = entry.sequence_number + 1

        verification_completed = datetime.now(timezone.utc)
        duration_ms = int((verification_completed - verification_started).total_seconds() * 1000)

        logger.info(
            "Audit chain verification completed",
            extra={
                "event_type": "audit.chain_verified",
                "tenant_id": str(tenant_id),
                "entries_verified": len(entries),
                "is_valid": len(errors) == 0,
                "error_count": len(errors),
                "duration_ms": duration_ms,
            }
        )

        return ChainVerificationResult(
            is_valid=len(errors) == 0,
            entries_verified=len(entries),
            first_entry_id=first_entry.id,
            last_entry_id=last_entry.id,
            first_sequence=first_entry.sequence_number,
            last_sequence=last_entry.sequence_number,
            error_count=len(errors),
            errors=errors,
            verification_started_at=verification_started,
            verification_completed_at=verification_completed,
            duration_ms=duration_ms,
        )

    async def get_chain_summary(
        self,
        db: AsyncSession,
        tenant_id: UUID,
    ) -> Dict[str, Any]:
        """
        Get summary statistics for the audit chain.
        """
        from assemblyline_common.models import AuditTrail

        # Get counts
        count_result = await db.execute(
            select(func.count(AuditTrail.id))
            .where(AuditTrail.tenant_id == tenant_id)
        )
        total_entries = count_result.scalar() or 0

        # Get first and last entries
        first_result = await db.execute(
            select(AuditTrail)
            .where(AuditTrail.tenant_id == tenant_id)
            .order_by(AuditTrail.sequence_number)
            .limit(1)
        )
        first_entry = first_result.scalar()

        last_result = await db.execute(
            select(AuditTrail)
            .where(AuditTrail.tenant_id == tenant_id)
            .order_by(AuditTrail.sequence_number.desc())
            .limit(1)
        )
        last_entry = last_result.scalar()

        # Count PHI access
        phi_result = await db.execute(
            select(func.count(AuditTrail.id))
            .where(
                AuditTrail.tenant_id == tenant_id,
                AuditTrail.contains_phi == True
            )
        )
        phi_access_count = phi_result.scalar() or 0

        return {
            "total_entries": total_entries,
            "first_sequence": first_entry.sequence_number if first_entry else None,
            "last_sequence": last_entry.sequence_number if last_entry else None,
            "first_entry_at": first_entry.created_at.isoformat() if first_entry else None,
            "last_entry_at": last_entry.created_at.isoformat() if last_entry else None,
            "genesis_hash": self.config.genesis_hash,
            "current_hash": last_entry.entry_hash if last_entry else None,
            "phi_access_count": phi_access_count,
            "hash_algorithm": self.config.hash_algorithm,
        }

    async def read_entry_details(
        self,
        db: AsyncSession,
        entry_id: UUID,
        tenant_id: UUID,
        accessed_by_user_id: Optional[UUID] = None,
        access_reason: str = "audit_review",
    ) -> Dict[str, Any]:
        """
        Read and decrypt audit entry details with audit-of-audit logging.

        Every access to encrypted PHI details is itself logged as an audit entry,
        creating a chain of custody for PHI data (HIPAA §164.312(b)).

        Args:
            db: Database session
            entry_id: The audit entry to read
            tenant_id: Tenant context
            accessed_by_user_id: User performing the read
            access_reason: Why the data is being accessed

        Returns:
            Decrypted details dict (or plain details if not encrypted)
        """
        from assemblyline_common.models import AuditTrail

        result = await db.execute(
            select(AuditTrail)
            .where(AuditTrail.id == entry_id, AuditTrail.tenant_id == tenant_id)
        )
        entry = result.scalar()

        if entry is None:
            raise ValueError(f"Audit entry {entry_id} not found")

        # If not encrypted, return details directly
        if not entry.encrypted_details:
            return entry.details or {}

        # Decrypt PHI details
        decrypted = self._decrypt_details(entry.encrypted_details, entry.id, entry.tenant_id)

        # Log the access (audit-of-audit)
        logger.info(
            "PHI audit details accessed",
            extra={
                "event_type": "audit.phi_details_accessed",
                "entry_id": str(entry_id),
                "tenant_id": str(tenant_id),
                "accessed_by": str(accessed_by_user_id) if accessed_by_user_id else "system",
                "access_reason": access_reason,
                "original_action": entry.action,
                "original_resource_type": entry.resource_type,
            }
        )

        # Create audit-of-audit entry (non-PHI, so no encryption needed)
        await self.create_entry(
            db=db,
            tenant_id=tenant_id,
            action="phi_access",
            resource_type="audit_entry",
            resource_id=str(entry_id),
            user_id=accessed_by_user_id,
            contains_phi=False,
            details={
                "accessed_entry_id": str(entry_id),
                "access_reason": access_reason,
                "original_action": entry.action,
                "original_resource": entry.resource_type,
            },
        )

        return decrypted


# ============================================================================
# Singleton Factory
# ============================================================================

_immutable_audit_service: Optional[ImmutableAuditService] = None


async def get_immutable_audit_service(
    config: Optional[ImmutableAuditConfig] = None
) -> ImmutableAuditService:
    """
    Get singleton instance of immutable audit service.
    """
    global _immutable_audit_service

    if _immutable_audit_service is None:
        _immutable_audit_service = ImmutableAuditService(config)
        logger.info(
            "Initialized immutable audit service",
            extra={
                "event_type": "audit.service_initialized",
                "hash_algorithm": _immutable_audit_service.config.hash_algorithm,
            }
        )

    return _immutable_audit_service
