"""
PHI-Safe Function Decorators — Enforce masking on function inputs/outputs.

Provides decorators that automatically scan and redact PHI from function
arguments and return values. Use on any function that handles sensitive
data to ensure PHI never leaks into logs, error messages, or downstream
systems.

Usage:
    from assemblyline_common.phi.decorators import phi_safe, phi_mask_output

    @phi_safe
    async def process_patient(data: dict) -> dict:
        ...  # PHI in args/return values is automatically masked in logs

    @phi_mask_output(fields={"ssn", "dob", "name"})
    async def get_patient(patient_id: str) -> dict:
        ...  # Only specified fields are masked in the return value
"""

import functools
import inspect
import logging
from typing import Any, Callable, Dict, Optional, Set

from assemblyline_common.phi.masking import PHIMaskingService, PHIConfig

logger = logging.getLogger(__name__)

# Lazy-initialized service (avoids import-time work)
_masking_service: Optional[PHIMaskingService] = None


def _get_masking_service() -> PHIMaskingService:
    """Get or create the PHI masking service singleton."""
    global _masking_service
    if _masking_service is None:
        _masking_service = PHIMaskingService(PHIConfig())
    return _masking_service


def _mask_value(value: Any, sensitive_fields: Optional[Set[str]] = None) -> Any:
    """Recursively mask PHI in a value."""
    svc = _get_masking_service()

    if isinstance(value, str):
        result = svc.mask_text(value)
        return result.masked_text
    elif isinstance(value, dict):
        masked, _ = svc.mask_dict(value, sensitive_fields=sensitive_fields)
        return masked
    elif isinstance(value, list):
        return [_mask_value(item, sensitive_fields) for item in value]
    else:
        return value


def phi_safe(func: Callable = None, *, log_args: bool = False) -> Callable:
    """
    Decorator that ensures PHI doesn't leak through exceptions or logs.

    Wraps the function so that:
    - Any exception messages are scrubbed of PHI before re-raising
    - If log_args=True, function arguments are masked before debug logging

    This does NOT modify the actual arguments or return values passed to/from
    the function — it only protects the logging and error reporting paths.

    Args:
        log_args: If True, log masked versions of function args at DEBUG level.
    """
    def decorator(fn: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(fn)

        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            if log_args:
                masked_kwargs = {
                    k: _mask_value(v) if isinstance(v, (str, dict)) else repr(v)
                    for k, v in kwargs.items()
                }
                logger.debug(
                    f"PHI-safe call: {fn.__qualname__}",
                    extra={
                        "event_type": "phi.safe_call",
                        "function": fn.__qualname__,
                        "masked_kwargs": str(masked_kwargs),
                    }
                )
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                # Scrub PHI from exception message before it propagates
                svc = _get_masking_service()
                original_msg = str(exc)
                scrubbed = svc.mask_text(original_msg).masked_text
                if scrubbed != original_msg:
                    logger.warning(
                        "PHI detected in exception — scrubbed before propagation",
                        extra={
                            "event_type": "phi.exception_scrubbed",
                            "function": fn.__qualname__,
                        }
                    )
                    # Re-raise with scrubbed message, same type
                    raise type(exc)(scrubbed) from exc.__cause__
                raise

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            if log_args:
                masked_kwargs = {
                    k: _mask_value(v) if isinstance(v, (str, dict)) else repr(v)
                    for k, v in kwargs.items()
                }
                logger.debug(
                    f"PHI-safe call: {fn.__qualname__}",
                    extra={
                        "event_type": "phi.safe_call",
                        "function": fn.__qualname__,
                        "masked_kwargs": str(masked_kwargs),
                    }
                )
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                svc = _get_masking_service()
                original_msg = str(exc)
                scrubbed = svc.mask_text(original_msg).masked_text
                if scrubbed != original_msg:
                    logger.warning(
                        "PHI detected in exception — scrubbed before propagation",
                        extra={
                            "event_type": "phi.exception_scrubbed",
                            "function": fn.__qualname__,
                        }
                    )
                    raise type(exc)(scrubbed) from exc.__cause__
                raise

        return async_wrapper if is_async else sync_wrapper

    # Handle @phi_safe with and without parentheses
    if func is not None:
        return decorator(func)
    return decorator


def phi_mask_output(
    fields: Optional[Set[str]] = None,
) -> Callable:
    """
    Decorator that masks PHI in function return values.

    Use when a function returns data that may contain sensitive fields
    and you want the caller to receive the masked version.

    Args:
        fields: Specific field names to force-mask. If None, uses default
                sensitive field detection from PHIMaskingService.
    """
    def decorator(fn: Callable) -> Callable:
        is_async = inspect.iscoroutinefunction(fn)

        @functools.wraps(fn)
        async def async_wrapper(*args, **kwargs):
            result = await fn(*args, **kwargs)
            return _mask_value(result, sensitive_fields=fields)

        @functools.wraps(fn)
        def sync_wrapper(*args, **kwargs):
            result = fn(*args, **kwargs)
            return _mask_value(result, sensitive_fields=fields)

        return async_wrapper if is_async else sync_wrapper
    return decorator


def mask_step_log(step_log: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mask PHI in a flow execution step log entry.

    Called before persisting step_logs to ensure sensitive data from
    connector responses doesn't leak into the database unmasked.

    Args:
        step_log: A single step log dict with keys like input, output, error, etc.

    Returns:
        Step log with PHI-containing string values masked.
    """
    svc = _get_masking_service()

    sensitive_keys = {"input", "output", "error", "rawResponse", "responseBody"}
    masked = {}

    for key, value in step_log.items():
        if key in sensitive_keys:
            masked[key] = _mask_value(value)
        else:
            masked[key] = value

    return masked
