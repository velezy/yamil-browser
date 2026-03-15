"""
Environment Validation Module

Validates required environment variables and service dependencies at startup.
Fails fast if critical configuration is missing.

Usage:
    from assemblyline_common.utils.env_validator import (
        validate_environment,
        EnvironmentValidator,
        ServiceDependency
    )

    # Quick validation
    validate_environment(
        required_vars=["DATABASE_URL", "JWT_SECRET"],
        optional_vars=["DEBUG"],
        dependencies=["postgres", "redis"]
    )

    # Detailed validation with custom checks
    validator = EnvironmentValidator()
    validator.require("JWT_SECRET", min_length=32)
    validator.require_url("DATABASE_URL")
    validator.require_dependency("postgres", "localhost:5450")
    result = validator.validate()
"""

import os
import re
import sys
import socket
import asyncio
import logging
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation severity level."""
    ERROR = "error"      # Fails startup
    WARNING = "warning"  # Logs warning but continues
    INFO = "info"        # Informational only


@dataclass
class ValidationResult:
    """Result of a single validation check."""
    name: str
    passed: bool
    level: ValidationLevel
    message: str
    value: Optional[str] = None  # Masked value for logging


@dataclass
class EnvironmentValidationReport:
    """Complete validation report."""
    passed: bool
    results: List[ValidationResult] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_result(self, result: ValidationResult):
        self.results.append(result)
        if not result.passed:
            if result.level == ValidationLevel.ERROR:
                self.errors.append(result.message)
                self.passed = False
            elif result.level == ValidationLevel.WARNING:
                self.warnings.append(result.message)

    def print_report(self):
        """Print formatted validation report."""
        print("\n" + "=" * 60)
        print("ENVIRONMENT VALIDATION REPORT")
        print("=" * 60)

        for result in self.results:
            status = "OK" if result.passed else result.level.value.upper()
            icon = "" if result.passed else ""
            print(f"  [{status}] {result.name}: {result.message}")

        print("-" * 60)
        if self.passed:
            print("Status: ALL CHECKS PASSED")
        else:
            print(f"Status: FAILED ({len(self.errors)} errors, {len(self.warnings)} warnings)")
        print("=" * 60 + "\n")


class EnvironmentValidator:
    """
    Validates environment variables and service dependencies.

    Example:
        validator = EnvironmentValidator()
        validator.require("DATABASE_URL")
        validator.require("JWT_SECRET", min_length=32)
        validator.require_one_of(["OPENAI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY"])
        validator.check_dependency("postgres", "localhost", 5450)

        if not validator.validate().passed:
            sys.exit(1)
    """

    def __init__(self, service_name: str = "service"):
        self.service_name = service_name
        self.checks: List[Callable[[], ValidationResult]] = []

    def require(
        self,
        var_name: str,
        min_length: int = 1,
        pattern: Optional[str] = None,
        level: ValidationLevel = ValidationLevel.ERROR,
        default: Optional[str] = None,
    ) -> "EnvironmentValidator":
        """Require an environment variable."""

        def check() -> ValidationResult:
            value = os.getenv(var_name, default)
            masked = self._mask_value(value) if value else None

            if not value:
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"Missing required environment variable: {var_name}",
                    value=None,
                )

            if len(value) < min_length:
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"{var_name} must be at least {min_length} characters",
                    value=masked,
                )

            if pattern and not re.match(pattern, value):
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"{var_name} does not match required pattern",
                    value=masked,
                )

            return ValidationResult(
                name=var_name,
                passed=True,
                level=level,
                message=f"Set ({masked})",
                value=masked,
            )

        self.checks.append(check)
        return self

    def require_url(
        self,
        var_name: str,
        schemes: Optional[List[str]] = None,
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Require a valid URL environment variable."""
        schemes = schemes or ["http", "https", "postgresql", "postgres", "redis"]

        def check() -> ValidationResult:
            value = os.getenv(var_name)

            if not value:
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"Missing required URL: {var_name}",
                )

            try:
                parsed = urlparse(value)
                if parsed.scheme not in schemes:
                    return ValidationResult(
                        name=var_name,
                        passed=False,
                        level=level,
                        message=f"{var_name} has invalid scheme (expected: {schemes})",
                    )

                if not parsed.netloc:
                    return ValidationResult(
                        name=var_name,
                        passed=False,
                        level=level,
                        message=f"{var_name} is not a valid URL",
                    )

                return ValidationResult(
                    name=var_name,
                    passed=True,
                    level=level,
                    message=f"Valid URL ({parsed.scheme}://{parsed.netloc}/...)",
                )
            except Exception as e:
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"{var_name} is not a valid URL: {e}",
                )

        self.checks.append(check)
        return self

    def require_one_of(
        self,
        var_names: List[str],
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Require at least one of the specified variables."""

        def check() -> ValidationResult:
            found = [v for v in var_names if os.getenv(v)]
            if found:
                return ValidationResult(
                    name=f"OneOf({', '.join(var_names)})",
                    passed=True,
                    level=level,
                    message=f"Found: {', '.join(found)}",
                )
            return ValidationResult(
                name=f"OneOf({', '.join(var_names)})",
                passed=False,
                level=level,
                message=f"At least one required: {', '.join(var_names)}",
            )

        self.checks.append(check)
        return self

    def require_int(
        self,
        var_name: str,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        default: Optional[int] = None,
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Require a valid integer environment variable."""

        def check() -> ValidationResult:
            value = os.getenv(var_name)

            if not value:
                if default is not None:
                    return ValidationResult(
                        name=var_name,
                        passed=True,
                        level=level,
                        message=f"Using default: {default}",
                    )
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"Missing required integer: {var_name}",
                )

            try:
                int_value = int(value)
                if min_value is not None and int_value < min_value:
                    return ValidationResult(
                        name=var_name,
                        passed=False,
                        level=level,
                        message=f"{var_name} must be >= {min_value}",
                    )
                if max_value is not None and int_value > max_value:
                    return ValidationResult(
                        name=var_name,
                        passed=False,
                        level=level,
                        message=f"{var_name} must be <= {max_value}",
                    )
                return ValidationResult(
                    name=var_name,
                    passed=True,
                    level=level,
                    message=f"Set to {int_value}",
                )
            except ValueError:
                return ValidationResult(
                    name=var_name,
                    passed=False,
                    level=level,
                    message=f"{var_name} is not a valid integer",
                )

        self.checks.append(check)
        return self

    def check_dependency(
        self,
        name: str,
        host: str,
        port: int,
        timeout: float = 2.0,
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Check if a service dependency is reachable."""

        def check() -> ValidationResult:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(timeout)
                result = sock.connect_ex((host, port))
                sock.close()

                if result == 0:
                    return ValidationResult(
                        name=f"Dependency:{name}",
                        passed=True,
                        level=level,
                        message=f"Reachable at {host}:{port}",
                    )
                else:
                    return ValidationResult(
                        name=f"Dependency:{name}",
                        passed=False,
                        level=level,
                        message=f"Cannot connect to {host}:{port}",
                    )
            except socket.error as e:
                return ValidationResult(
                    name=f"Dependency:{name}",
                    passed=False,
                    level=level,
                    message=f"Connection failed: {e}",
                )

        self.checks.append(check)
        return self

    def check_file_exists(
        self,
        path: str,
        var_name: Optional[str] = None,
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Check if a required file exists."""
        actual_path = os.getenv(var_name, path) if var_name else path

        def check() -> ValidationResult:
            if os.path.exists(actual_path):
                return ValidationResult(
                    name=f"File:{var_name or path}",
                    passed=True,
                    level=level,
                    message=f"Exists at {actual_path}",
                )
            return ValidationResult(
                name=f"File:{var_name or path}",
                passed=False,
                level=level,
                message=f"File not found: {actual_path}",
            )

        self.checks.append(check)
        return self

    def custom_check(
        self,
        name: str,
        check_fn: Callable[[], bool],
        error_message: str,
        success_message: str = "OK",
        level: ValidationLevel = ValidationLevel.ERROR,
    ) -> "EnvironmentValidator":
        """Add a custom validation check."""

        def check() -> ValidationResult:
            try:
                passed = check_fn()
                return ValidationResult(
                    name=name,
                    passed=passed,
                    level=level,
                    message=success_message if passed else error_message,
                )
            except Exception as e:
                return ValidationResult(
                    name=name,
                    passed=False,
                    level=level,
                    message=f"{error_message}: {e}",
                )

        self.checks.append(check)
        return self

    def validate(self, exit_on_failure: bool = False) -> EnvironmentValidationReport:
        """Run all validation checks."""
        report = EnvironmentValidationReport(passed=True)

        for check in self.checks:
            result = check()
            report.add_result(result)

        # Log results
        if report.errors:
            logger.error(f"{self.service_name} environment validation failed:")
            for error in report.errors:
                logger.error(f"  - {error}")

        if report.warnings:
            for warning in report.warnings:
                logger.warning(f"  - {warning}")

        if exit_on_failure and not report.passed:
            report.print_report()
            sys.exit(1)

        return report

    @staticmethod
    def _mask_value(value: str, visible_chars: int = 4) -> str:
        """Mask sensitive values for logging."""
        if not value:
            return ""
        if len(value) <= visible_chars * 2:
            return "*" * len(value)
        return value[:visible_chars] + "*" * (len(value) - visible_chars * 2) + value[-visible_chars:]


# =============================================================================
# PRESET VALIDATORS
# =============================================================================

def get_auth_service_validator() -> EnvironmentValidator:
    """Get validator for auth service."""
    return (
        EnvironmentValidator("auth-service")
        .require("JWT_SECRET", min_length=32)
        .require("DB_PASSWORD", min_length=8)
        .require_int("JWT_EXPIRY", min_value=300, default=3600)
        .require_one_of(
            ["DATABASE_URL", "DB_HOST"],
            level=ValidationLevel.ERROR
        )
    )


def get_orchestrator_validator() -> EnvironmentValidator:
    """Get validator for orchestrator service."""
    return (
        EnvironmentValidator("orchestrator")
        .require("JWT_SECRET", min_length=32)
        .require_one_of(
            ["OLLAMA_BASE_URL", "OPENAI_API_KEY", "GOOGLE_API_KEY", "ANTHROPIC_API_KEY"],
            level=ValidationLevel.WARNING
        )
    )


def get_rag_service_validator() -> EnvironmentValidator:
    """Get validator for RAG service."""
    return (
        EnvironmentValidator("rag-service")
        .require_one_of(
            ["DATABASE_URL", "DB_HOST"],
            level=ValidationLevel.ERROR
        )
    )


# =============================================================================
# CONVENIENCE FUNCTION
# =============================================================================

def validate_environment(
    service_name: str = "service",
    required_vars: Optional[List[str]] = None,
    optional_vars: Optional[List[str]] = None,
    dependencies: Optional[Dict[str, tuple]] = None,
    exit_on_failure: bool = True,
) -> EnvironmentValidationReport:
    """
    Quick environment validation.

    Args:
        service_name: Name of the service for logging
        required_vars: List of required environment variable names
        optional_vars: List of optional variables to check (warnings only)
        dependencies: Dict of {name: (host, port)} for dependency checks
        exit_on_failure: Whether to exit if validation fails

    Example:
        validate_environment(
            service_name="my-service",
            required_vars=["DATABASE_URL", "JWT_SECRET"],
            optional_vars=["DEBUG"],
            dependencies={"postgres": ("localhost", 5450)},
        )
    """
    validator = EnvironmentValidator(service_name)

    for var in (required_vars or []):
        validator.require(var)

    for var in (optional_vars or []):
        validator.require(var, level=ValidationLevel.WARNING)

    for name, (host, port) in (dependencies or {}).items():
        validator.check_dependency(name, host, port)

    return validator.validate(exit_on_failure=exit_on_failure)
