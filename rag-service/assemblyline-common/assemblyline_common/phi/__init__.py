"""
PHI Module - HIPAA-Compliant Protected Health Information Handling

Provides detection, masking, and de-identification of PHI according to
HIPAA Safe Harbor guidelines (18 identifier categories).
"""

from assemblyline_common.phi.masking import (
    PHIConfig,
    PHIType,
    PHIDetection,
    PHIMaskingService,
    get_phi_masking_service,
)
from assemblyline_common.phi.hl7_masking import (
    HL7PHIMaskingService,
    HL7SegmentConfig,
    get_hl7_phi_masking_service,
)
from assemblyline_common.phi.log_filter import (
    PHILogFilter,
    install_phi_log_filter,
)
from assemblyline_common.phi.decorators import (
    phi_safe,
    phi_mask_output,
    mask_step_log,
)

__all__ = [
    # Core masking
    "PHIConfig",
    "PHIType",
    "PHIDetection",
    "PHIMaskingService",
    "get_phi_masking_service",
    # HL7-specific
    "HL7PHIMaskingService",
    "HL7SegmentConfig",
    "get_hl7_phi_masking_service",
    # Log filter
    "PHILogFilter",
    "install_phi_log_filter",
    # Decorators
    "phi_safe",
    "phi_mask_output",
    "mask_step_log",
]
