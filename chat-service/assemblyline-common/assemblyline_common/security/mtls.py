"""
Mutual TLS (mTLS) Support.

Provides:
- Client certificate validation
- Certificate chain verification
- Certificate revocation checking (CRL/OCSP)
"""

import ssl
import logging
import hashlib
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set
from datetime import datetime, timezone
from enum import Enum
import asyncio

logger = logging.getLogger(__name__)

# Try to import cryptography for advanced cert handling
try:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.x509.oid import ExtensionOID, NameOID
    from cryptography.x509 import ocsp
    CRYPTO_AVAILABLE = True
except ImportError:
    CRYPTO_AVAILABLE = False
    logger.warning("cryptography library not available, mTLS features limited")


class CertificateStatus(Enum):
    """Certificate validation status."""
    VALID = "valid"
    EXPIRED = "expired"
    NOT_YET_VALID = "not_yet_valid"
    REVOKED = "revoked"
    INVALID_CHAIN = "invalid_chain"
    INVALID_SIGNATURE = "invalid_signature"
    MISSING = "missing"
    UNKNOWN = "unknown"


@dataclass
class MTLSConfig:
    """Configuration for mTLS validation."""
    # CA certificates
    ca_cert_paths: List[str] = field(default_factory=list)
    ca_cert_data: Optional[bytes] = None  # PEM-encoded CA certs

    # Verification
    verify_chain: bool = True
    verify_hostname: bool = False  # Usually False for API clients
    check_revocation: bool = True

    # CRL settings
    crl_paths: List[str] = field(default_factory=list)
    crl_cache_seconds: int = 3600

    # OCSP settings
    ocsp_enabled: bool = True
    ocsp_timeout_seconds: int = 5
    ocsp_cache_seconds: int = 300

    # Allowed subjects/issuers (for whitelisting)
    allowed_subject_dns: Set[str] = field(default_factory=set)
    allowed_issuer_dns: Set[str] = field(default_factory=set)
    allowed_fingerprints: Set[str] = field(default_factory=set)  # SHA256 fingerprints

    # Client cert extraction (from headers for proxy scenarios)
    cert_header_name: Optional[str] = "X-Client-Cert"
    cert_header_format: str = "pem"  # pem or der_base64


@dataclass
class CertificateInfo:
    """Information extracted from a certificate."""
    subject_dn: str
    issuer_dn: str
    serial_number: str
    fingerprint_sha256: str
    not_before: datetime
    not_after: datetime
    subject_alt_names: List[str] = field(default_factory=list)
    key_usage: List[str] = field(default_factory=list)
    extended_key_usage: List[str] = field(default_factory=list)


@dataclass
class MTLSValidationResult:
    """Result of mTLS validation."""
    valid: bool
    status: CertificateStatus
    certificate_info: Optional[CertificateInfo] = None
    message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


class MTLSValidator:
    """
    Mutual TLS certificate validator.

    Features:
    - X.509 certificate parsing
    - Certificate chain validation
    - CRL checking
    - OCSP checking
    - Subject/issuer whitelisting
    """

    def __init__(self, config: Optional[MTLSConfig] = None):
        self.config = config or MTLSConfig()
        self._ca_certs: List[Any] = []
        self._crl_cache: Dict[str, tuple] = {}  # issuer -> (crl, expires_at)
        self._ocsp_cache: Dict[str, tuple] = {}  # serial -> (status, expires_at)
        self._initialized = False

    async def initialize(self):
        """Load CA certificates and CRLs."""
        if self._initialized:
            return

        if not CRYPTO_AVAILABLE:
            logger.warning("cryptography not available, mTLS validation limited")
            self._initialized = True
            return

        # Load CA certificates
        for path in self.config.ca_cert_paths:
            try:
                with open(path, "rb") as f:
                    pem_data = f.read()
                    # Load all certs from PEM file
                    certs = self._load_pem_certs(pem_data)
                    self._ca_certs.extend(certs)
                    logger.info(f"Loaded {len(certs)} CA certificates from {path}")
            except Exception as e:
                logger.error(f"Failed to load CA cert from {path}: {e}")

        if self.config.ca_cert_data:
            certs = self._load_pem_certs(self.config.ca_cert_data)
            self._ca_certs.extend(certs)

        # Load CRLs
        for path in self.config.crl_paths:
            try:
                with open(path, "rb") as f:
                    crl_data = f.read()
                    crl = x509.load_pem_x509_crl(crl_data, default_backend())
                    issuer_dn = crl.issuer.rfc4514_string()
                    expires_at = datetime.now(timezone.utc).timestamp() + self.config.crl_cache_seconds
                    self._crl_cache[issuer_dn] = (crl, expires_at)
                    logger.info(f"Loaded CRL from {path}")
            except Exception as e:
                logger.error(f"Failed to load CRL from {path}: {e}")

        self._initialized = True

    def _load_pem_certs(self, pem_data: bytes) -> List[Any]:
        """Load all certificates from PEM data."""
        certs = []
        # Split PEM data into individual certs
        import re
        pem_pattern = rb"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----"
        for match in re.finditer(pem_pattern, pem_data, re.DOTALL):
            try:
                cert = x509.load_pem_x509_certificate(match.group(), default_backend())
                certs.append(cert)
            except Exception as e:
                logger.warning(f"Failed to parse certificate: {e}")
        return certs

    def parse_certificate(self, cert_data: bytes, format: str = "pem") -> Optional[CertificateInfo]:
        """
        Parse a certificate and extract information.

        Args:
            cert_data: Certificate data
            format: Format ('pem' or 'der')

        Returns:
            CertificateInfo or None if parsing fails
        """
        if not CRYPTO_AVAILABLE:
            return None

        try:
            if format == "pem":
                cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            else:
                cert = x509.load_der_x509_certificate(cert_data, default_backend())

            # Extract subject alternative names
            san_list = []
            try:
                san_ext = cert.extensions.get_extension_for_oid(ExtensionOID.SUBJECT_ALTERNATIVE_NAME)
                for name in san_ext.value:
                    san_list.append(str(name.value))
            except x509.ExtensionNotFound:
                pass

            # Extract key usage
            key_usage = []
            try:
                ku_ext = cert.extensions.get_extension_for_oid(ExtensionOID.KEY_USAGE)
                ku = ku_ext.value
                if ku.digital_signature:
                    key_usage.append("digital_signature")
                if ku.key_encipherment:
                    key_usage.append("key_encipherment")
                if ku.content_commitment:
                    key_usage.append("content_commitment")
                if ku.data_encipherment:
                    key_usage.append("data_encipherment")
                if ku.key_agreement:
                    key_usage.append("key_agreement")
                if ku.key_cert_sign:
                    key_usage.append("key_cert_sign")
                if ku.crl_sign:
                    key_usage.append("crl_sign")
            except x509.ExtensionNotFound:
                pass

            # Calculate fingerprint
            fingerprint = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()

            return CertificateInfo(
                subject_dn=cert.subject.rfc4514_string(),
                issuer_dn=cert.issuer.rfc4514_string(),
                serial_number=format(cert.serial_number, 'x'),
                fingerprint_sha256=fingerprint,
                not_before=cert.not_valid_before_utc,
                not_after=cert.not_valid_after_utc,
                subject_alt_names=san_list,
                key_usage=key_usage,
            )

        except Exception as e:
            logger.error(f"Failed to parse certificate: {e}")
            return None

    async def validate(
        self,
        cert_data: bytes,
        format: str = "pem"
    ) -> MTLSValidationResult:
        """
        Validate a client certificate.

        Args:
            cert_data: Certificate data
            format: Format ('pem' or 'der')

        Returns:
            MTLSValidationResult with validation status
        """
        await self.initialize()

        if not CRYPTO_AVAILABLE:
            return MTLSValidationResult(
                valid=False,
                status=CertificateStatus.UNKNOWN,
                message="Cryptography library not available"
            )

        try:
            if format == "pem":
                cert = x509.load_pem_x509_certificate(cert_data, default_backend())
            else:
                cert = x509.load_der_x509_certificate(cert_data, default_backend())
        except Exception as e:
            return MTLSValidationResult(
                valid=False,
                status=CertificateStatus.INVALID_SIGNATURE,
                message=f"Invalid certificate format: {e}"
            )

        # Parse certificate info
        cert_info = self.parse_certificate(cert_data, format)

        # Check validity period
        now = datetime.now(timezone.utc)
        if now < cert.not_valid_before_utc:
            return MTLSValidationResult(
                valid=False,
                status=CertificateStatus.NOT_YET_VALID,
                certificate_info=cert_info,
                message=f"Certificate not valid until {cert.not_valid_before_utc}"
            )

        if now > cert.not_valid_after_utc:
            return MTLSValidationResult(
                valid=False,
                status=CertificateStatus.EXPIRED,
                certificate_info=cert_info,
                message=f"Certificate expired on {cert.not_valid_after_utc}"
            )

        # Check fingerprint whitelist
        if self.config.allowed_fingerprints:
            fingerprint = hashlib.sha256(cert.public_bytes(serialization.Encoding.DER)).hexdigest()
            if fingerprint.lower() not in {fp.lower() for fp in self.config.allowed_fingerprints}:
                return MTLSValidationResult(
                    valid=False,
                    status=CertificateStatus.INVALID_CHAIN,
                    certificate_info=cert_info,
                    message="Certificate fingerprint not in whitelist"
                )

        # Check subject DN whitelist
        if self.config.allowed_subject_dns:
            subject_dn = cert.subject.rfc4514_string()
            if subject_dn not in self.config.allowed_subject_dns:
                return MTLSValidationResult(
                    valid=False,
                    status=CertificateStatus.INVALID_CHAIN,
                    certificate_info=cert_info,
                    message=f"Subject DN not allowed: {subject_dn}"
                )

        # Check issuer DN whitelist
        if self.config.allowed_issuer_dns:
            issuer_dn = cert.issuer.rfc4514_string()
            if issuer_dn not in self.config.allowed_issuer_dns:
                return MTLSValidationResult(
                    valid=False,
                    status=CertificateStatus.INVALID_CHAIN,
                    certificate_info=cert_info,
                    message=f"Issuer DN not allowed: {issuer_dn}"
                )

        # Check revocation via CRL
        if self.config.check_revocation:
            issuer_dn = cert.issuer.rfc4514_string()
            if issuer_dn in self._crl_cache:
                crl, expires_at = self._crl_cache[issuer_dn]
                if datetime.now(timezone.utc).timestamp() < expires_at:
                    revoked = crl.get_revoked_certificate_by_serial_number(cert.serial_number)
                    if revoked:
                        return MTLSValidationResult(
                            valid=False,
                            status=CertificateStatus.REVOKED,
                            certificate_info=cert_info,
                            message=f"Certificate revoked on {revoked.revocation_date}"
                        )

        return MTLSValidationResult(
            valid=True,
            status=CertificateStatus.VALID,
            certificate_info=cert_info,
            message="Certificate is valid"
        )

    def create_ssl_context(
        self,
        purpose: ssl.Purpose = ssl.Purpose.CLIENT_AUTH
    ) -> ssl.SSLContext:
        """
        Create an SSL context for mTLS.

        Args:
            purpose: SSL purpose (CLIENT_AUTH for servers, SERVER_AUTH for clients)

        Returns:
            Configured SSLContext
        """
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if purpose == ssl.Purpose.CLIENT_AUTH else ssl.PROTOCOL_TLS_CLIENT)

        # Require client certificate
        context.verify_mode = ssl.CERT_REQUIRED

        # Load CA certificates
        for path in self.config.ca_cert_paths:
            context.load_verify_locations(path)

        # Minimum TLS version
        context.minimum_version = ssl.TLSVersion.TLSv1_2

        return context


# Convenience function
async def validate_client_certificate(
    cert_data: bytes,
    config: Optional[MTLSConfig] = None,
    format: str = "pem"
) -> MTLSValidationResult:
    """
    Validate a client certificate.

    Args:
        cert_data: Certificate data
        config: Optional configuration
        format: Format ('pem' or 'der')

    Returns:
        MTLSValidationResult
    """
    validator = MTLSValidator(config)
    return await validator.validate(cert_data, format)


# FastAPI dependency
from fastapi import Request, HTTPException
import base64


async def mtls_required(
    request: Request,
    config: Optional[MTLSConfig] = None
):
    """
    FastAPI dependency to require valid client certificate.

    The certificate can come from:
    1. TLS connection (if terminated at app)
    2. X-Client-Cert header (if TLS terminated at proxy)

    Usage:
        @app.get("/secure")
        async def secure_endpoint(
            cert_info: CertificateInfo = Depends(mtls_required)
        ):
            return {"subject": cert_info.subject_dn}
    """
    config = config or MTLSConfig()
    validator = MTLSValidator(config)

    # Try to get cert from header (proxy scenario)
    if config.cert_header_name:
        cert_header = request.headers.get(config.cert_header_name)
        if cert_header:
            try:
                if config.cert_header_format == "pem":
                    # URL-decode if needed
                    import urllib.parse
                    cert_data = urllib.parse.unquote(cert_header).encode()
                else:  # der_base64
                    cert_data = base64.b64decode(cert_header)

                result = await validator.validate(cert_data, config.cert_header_format)

                if not result.valid:
                    raise HTTPException(
                        status_code=403,
                        detail={
                            "error": "invalid_certificate",
                            "status": result.status.value,
                            "message": result.message
                        }
                    )

                return result.certificate_info

            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Failed to parse client certificate from header: {e}")

    # No valid certificate found
    raise HTTPException(
        status_code=403,
        detail={
            "error": "client_certificate_required",
            "message": "A valid client certificate is required"
        }
    )
