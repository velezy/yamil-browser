"""
PHI Log Filter — Automatic PII/PHI redaction in log output.

Attaches as a logging.Filter to any logger. Scans log messages and
extra fields for patterns matching SSN, MRN, email, phone, and other
sensitive identifiers, replacing them with redacted placeholders.

This ensures that even if application code accidentally logs PHI,
the output is scrubbed before reaching CloudWatch/stdout/files.

Usage:
    from assemblyline_common.phi.log_filter import install_phi_log_filter
    install_phi_log_filter()  # Call once at service startup
"""

import logging
import re
from typing import Dict, Pattern

logger = logging.getLogger(__name__)

# ============================================================================
# Fast regex patterns for common PII/PHI in log text
# ============================================================================
# These are intentionally broad to catch variants. False positives in logs
# are acceptable — it's better to over-redact than to leak PHI.

_PHI_PATTERNS: Dict[str, Pattern] = {
    "ssn": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    "ssn_no_dash": re.compile(r"\b\d{9}\b(?=\D|$)"),
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"),
    "phone_us": re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    "mrn": re.compile(r"\bMRN[:\s#]*\d{4,12}\b", re.IGNORECASE),
    "dob": re.compile(r"\b(?:DOB|Date\s*of\s*Birth)[:\s]*\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b", re.IGNORECASE),
    "ip_v4": re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b"),
}

# Fields in log records that are safe to pass through without redaction
_SAFE_FIELDS = frozenset({
    "event_type", "service_name", "tenant_id", "correlation_id",
    "flow_id", "flow_name", "step_index", "step_type",
    "status_code", "method", "path", "duration_ms",
    "circuit_name", "state", "failure_count", "success_count",
    "sequence_number", "entry_id", "action", "resource_type",
    "level", "levelname", "name", "module", "funcName", "lineno",
    "asctime", "created", "msecs", "relativeCreated", "thread",
    "threadName", "process", "processName", "pathname", "filename",
    "exc_info", "exc_text", "stack_info", "msg", "args", "message",
})


def _redact_text(text: str) -> str:
    """Apply all PHI patterns to redact matches in a string."""
    for label, pattern in _PHI_PATTERNS.items():
        text = pattern.sub(f"[REDACTED_{label.upper()}]", text)
    return text


class PHILogFilter(logging.Filter):
    """
    Logging filter that redacts PHI/PII from log messages and extra fields.

    Scans:
    - The formatted log message (record.msg after % formatting)
    - Any extra fields passed via the `extra={}` parameter
    - Exception text if present
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # Redact the main message
        if isinstance(record.msg, str):
            record.msg = _redact_text(record.msg)

        # Redact args used in % formatting
        if record.args:
            if isinstance(record.args, dict):
                record.args = {
                    k: _redact_text(str(v)) if isinstance(v, str) else v
                    for k, v in record.args.items()
                }
            elif isinstance(record.args, tuple):
                record.args = tuple(
                    _redact_text(str(a)) if isinstance(a, str) else a
                    for a in record.args
                )

        # Redact extra fields (anything not in standard LogRecord or safe list)
        for attr in list(vars(record)):
            if attr not in _SAFE_FIELDS and isinstance(getattr(record, attr, None), str):
                val = getattr(record, attr)
                redacted = _redact_text(val)
                if redacted != val:
                    setattr(record, attr, redacted)

        # Redact exception text
        if record.exc_text and isinstance(record.exc_text, str):
            record.exc_text = _redact_text(record.exc_text)

        return True  # Always allow the record through (we just redact, never drop)


def install_phi_log_filter(logger_name: str = "") -> None:
    """
    Install the PHI log filter on a logger (default: root logger).

    Call once at service startup to ensure all log output is scrubbed.

    Args:
        logger_name: Logger name to attach to. Empty string = root logger.
    """
    target = logging.getLogger(logger_name)
    phi_filter = PHILogFilter()

    # Avoid duplicate filters
    for existing in target.filters:
        if isinstance(existing, PHILogFilter):
            return

    target.addFilter(phi_filter)
    logger.info(
        "PHI log filter installed",
        extra={"event_type": "phi.log_filter_installed", "logger": logger_name or "root"}
    )
