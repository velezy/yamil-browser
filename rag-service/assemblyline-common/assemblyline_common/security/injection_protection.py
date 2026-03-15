"""
Injection Prevention Protection.

Protects against:
- SQL Injection
- Command Injection
- NoSQL Injection
"""

import re
import ast
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Set, Any, Dict
from enum import Enum

logger = logging.getLogger(__name__)


class InjectionType(Enum):
    """Types of injection attacks."""
    SQL_INJECTION = "sql_injection"
    COMMAND_INJECTION = "command_injection"
    NOSQL_INJECTION = "nosql_injection"
    LDAP_INJECTION = "ldap_injection"
    XPATH_INJECTION = "xpath_injection"


@dataclass
class InjectionResult:
    """Result of injection check."""
    safe: bool
    injection_type: Optional[InjectionType] = None
    pattern_matched: Optional[str] = None
    details: Optional[str] = None


# ============================================================================
# SQL Injection Protection
# ============================================================================

@dataclass
class SQLInjectionConfig:
    """Configuration for SQL injection protection."""
    # Detection patterns
    detect_union: bool = True
    detect_comments: bool = True
    detect_stacking: bool = True
    detect_boolean: bool = True
    detect_time_based: bool = True

    # Whitelist
    allowed_characters: str = r"a-zA-Z0-9\s\-_\.@"

    # Behavior
    log_attempts: bool = True


class SQLInjectionProtection:
    """
    SQL Injection detection and prevention.

    Note: The best protection is parameterized queries (which SQLAlchemy uses).
    This provides additional defense-in-depth for input validation.
    """

    # SQL injection patterns
    PATTERNS = [
        # UNION-based injection
        (r"(?i)\bUNION\s+(ALL\s+)?SELECT\b", "UNION SELECT injection"),
        # Comment injection
        (r"--\s*$", "SQL comment injection (double dash)"),
        (r"/\*.*\*/", "SQL comment injection (block comment)"),
        (r"#\s*$", "SQL comment injection (hash)"),
        # Statement stacking
        (r";\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE)\b", "Statement stacking"),
        # Boolean-based injection
        (r"(?i)\bOR\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?", "Boolean-based injection (OR)"),
        (r"(?i)\bAND\s+['\"]?\d+['\"]?\s*=\s*['\"]?\d+['\"]?", "Boolean-based injection (AND)"),
        (r"(?i)\b(OR|AND)\s+['\"]?1['\"]?\s*=\s*['\"]?1['\"]?", "Tautology injection"),
        # Time-based injection
        (r"(?i)\bWAITFOR\s+DELAY\b", "Time-based injection (WAITFOR)"),
        (r"(?i)\bSLEEP\s*\(", "Time-based injection (SLEEP)"),
        (r"(?i)\bBENCHMARK\s*\(", "Time-based injection (BENCHMARK)"),
        # Dangerous keywords
        (r"(?i)\bDROP\s+(TABLE|DATABASE|INDEX)\b", "DROP statement"),
        (r"(?i)\bTRUNCATE\s+TABLE\b", "TRUNCATE statement"),
        (r"(?i)\bEXEC(UTE)?\s*\(", "EXECUTE statement"),
        (r"(?i)\bxp_cmdshell\b", "xp_cmdshell (SQL Server)"),
        # Quote manipulation
        (r"'\s*OR\s*'", "Quote manipulation"),
        (r'"\s*OR\s*"', "Quote manipulation (double)"),
        # Encoded attacks
        (r"(?i)%27|%22|%00|%3B", "URL-encoded special characters"),
        (r"(?i)0x[0-9a-f]+", "Hex-encoded value"),
    ]

    def __init__(self, config: Optional[SQLInjectionConfig] = None):
        self.config = config or SQLInjectionConfig()
        self._compiled_patterns = [
            (re.compile(pattern), description)
            for pattern, description in self.PATTERNS
        ]

    def check(self, value: str) -> InjectionResult:
        """
        Check a string value for SQL injection patterns.

        Args:
            value: String to check

        Returns:
            InjectionResult indicating if the value is safe
        """
        if not value or not isinstance(value, str):
            return InjectionResult(safe=True)

        for pattern, description in self._compiled_patterns:
            if pattern.search(value):
                if self.config.log_attempts:
                    logger.warning(f"SQL injection attempt detected: {description}")

                return InjectionResult(
                    safe=False,
                    injection_type=InjectionType.SQL_INJECTION,
                    pattern_matched=pattern.pattern,
                    details=description
                )

        return InjectionResult(safe=True)

    def sanitize(self, value: str) -> str:
        """
        Sanitize a string by removing potentially dangerous characters.

        Note: This is a last resort. Use parameterized queries instead.

        Args:
            value: String to sanitize

        Returns:
            Sanitized string
        """
        if not value:
            return value

        # Remove null bytes
        value = value.replace("\x00", "")

        # Escape single quotes by doubling them
        value = value.replace("'", "''")

        # Remove semicolons that could enable statement stacking
        value = re.sub(r";(?=\s*(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER))", "", value, flags=re.IGNORECASE)

        return value


# ============================================================================
# Command Injection Protection
# ============================================================================

@dataclass
class CommandInjectionConfig:
    """Configuration for command injection protection."""
    # Detection
    detect_pipes: bool = True
    detect_redirects: bool = True
    detect_command_substitution: bool = True
    detect_chaining: bool = True

    # Sandboxing
    restricted_commands: Set[str] = field(default_factory=lambda: {
        "rm", "rmdir", "del", "format", "mkfs",
        "dd", "mv", "cp", "chmod", "chown",
        "wget", "curl", "nc", "netcat",
        "python", "perl", "ruby", "node",
        "sh", "bash", "zsh", "cmd", "powershell",
        "eval", "exec", "sudo", "su",
    })

    # Restricted Python modules (for sandboxed execution)
    restricted_modules: Set[str] = field(default_factory=lambda: {
        "os", "subprocess", "sys", "shutil",
        "socket", "requests", "urllib",
        "pickle", "marshal", "importlib",
        "__builtins__", "builtins",
    })


class CommandInjectionProtection:
    """
    Command injection detection and prevention.

    Provides:
    - Shell command injection detection
    - Python code sandboxing
    - Restricted execution environment
    """

    # Command injection patterns
    PATTERNS = [
        # Pipe operators
        (r"\|", "Pipe operator"),
        # Redirect operators
        (r"[<>]", "Redirect operator"),
        (r">>", "Append redirect"),
        # Command chaining
        (r"&&", "AND command chaining"),
        (r"\|\|", "OR command chaining"),
        (r";(?!\s*$)", "Semicolon command chaining"),
        # Command substitution
        (r"\$\(", "Command substitution $()"),
        (r"`[^`]+`", "Command substitution backticks"),
        # Variable expansion
        (r"\$\{", "Variable expansion"),
        (r"\$[A-Za-z_]", "Variable reference"),
        # Special characters
        (r"\x00", "Null byte"),
        (r"\\n", "Newline escape"),
    ]

    def __init__(self, config: Optional[CommandInjectionConfig] = None):
        self.config = config or CommandInjectionConfig()
        self._compiled_patterns = [
            (re.compile(pattern), description)
            for pattern, description in self.PATTERNS
        ]

    def check(self, value: str) -> InjectionResult:
        """
        Check a string for command injection patterns.

        Args:
            value: String to check

        Returns:
            InjectionResult indicating if the value is safe
        """
        if not value or not isinstance(value, str):
            return InjectionResult(safe=True)

        # Check for restricted commands
        words = value.lower().split()
        for word in words:
            # Remove path prefixes
            command = word.split("/")[-1].split("\\")[-1]
            if command in self.config.restricted_commands:
                return InjectionResult(
                    safe=False,
                    injection_type=InjectionType.COMMAND_INJECTION,
                    pattern_matched=command,
                    details=f"Restricted command: {command}"
                )

        # Check patterns
        for pattern, description in self._compiled_patterns:
            if pattern.search(value):
                return InjectionResult(
                    safe=False,
                    injection_type=InjectionType.COMMAND_INJECTION,
                    pattern_matched=pattern.pattern,
                    details=description
                )

        return InjectionResult(safe=True)

    def check_python_code(self, code: str) -> InjectionResult:
        """
        Check Python code for dangerous imports/calls.

        Args:
            code: Python source code to check

        Returns:
            InjectionResult indicating if the code is safe
        """
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return InjectionResult(
                safe=False,
                injection_type=InjectionType.COMMAND_INJECTION,
                details=f"Invalid Python syntax: {e}"
            )

        # Check for dangerous imports
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".")[0] in self.config.restricted_modules:
                        return InjectionResult(
                            safe=False,
                            injection_type=InjectionType.COMMAND_INJECTION,
                            pattern_matched=alias.name,
                            details=f"Restricted module import: {alias.name}"
                        )
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.split(".")[0] in self.config.restricted_modules:
                    return InjectionResult(
                        safe=False,
                        injection_type=InjectionType.COMMAND_INJECTION,
                        pattern_matched=node.module,
                        details=f"Restricted module import: {node.module}"
                    )
            elif isinstance(node, ast.Call):
                # Check for dangerous function calls
                if isinstance(node.func, ast.Name):
                    if node.func.id in ("eval", "exec", "compile", "open", "__import__"):
                        return InjectionResult(
                            safe=False,
                            injection_type=InjectionType.COMMAND_INJECTION,
                            pattern_matched=node.func.id,
                            details=f"Dangerous function call: {node.func.id}"
                        )

        return InjectionResult(safe=True)


# ============================================================================
# NoSQL Injection Protection
# ============================================================================

@dataclass
class NoSQLInjectionConfig:
    """Configuration for NoSQL injection protection."""
    # MongoDB operators to block in user input
    blocked_operators: Set[str] = field(default_factory=lambda: {
        "$where", "$regex", "$gt", "$gte", "$lt", "$lte",
        "$ne", "$nin", "$in", "$or", "$and", "$not", "$nor",
        "$exists", "$type", "$mod", "$text", "$search",
        "$expr", "$jsonSchema", "$comment",
    })

    # DynamoDB operators
    blocked_dynamodb_operators: Set[str] = field(default_factory=lambda: {
        "begins_with", "contains", "attribute_exists",
        "attribute_not_exists", "attribute_type", "size",
    })


class NoSQLInjectionProtection:
    """
    NoSQL injection detection for MongoDB and DynamoDB.

    Prevents operator injection in query parameters.
    """

    def __init__(self, config: Optional[NoSQLInjectionConfig] = None):
        self.config = config or NoSQLInjectionConfig()

    def check_mongodb(self, value: Any) -> InjectionResult:
        """
        Check a value for MongoDB operator injection.

        Args:
            value: Value to check (can be dict, list, or string)

        Returns:
            InjectionResult indicating if the value is safe
        """
        if isinstance(value, dict):
            for key, val in value.items():
                # Check for operator keys
                if key.startswith("$"):
                    if key in self.config.blocked_operators:
                        return InjectionResult(
                            safe=False,
                            injection_type=InjectionType.NOSQL_INJECTION,
                            pattern_matched=key,
                            details=f"Blocked MongoDB operator: {key}"
                        )
                # Recursively check nested values
                result = self.check_mongodb(val)
                if not result.safe:
                    return result

        elif isinstance(value, list):
            for item in value:
                result = self.check_mongodb(item)
                if not result.safe:
                    return result

        elif isinstance(value, str):
            # Check for operator patterns in strings
            for op in self.config.blocked_operators:
                if op in value:
                    return InjectionResult(
                        safe=False,
                        injection_type=InjectionType.NOSQL_INJECTION,
                        pattern_matched=op,
                        details=f"MongoDB operator in string: {op}"
                    )

        return InjectionResult(safe=True)

    def check_dynamodb(self, value: Any) -> InjectionResult:
        """
        Check a value for DynamoDB injection patterns.

        Args:
            value: Value to check

        Returns:
            InjectionResult indicating if the value is safe
        """
        if isinstance(value, str):
            value_lower = value.lower()
            for op in self.config.blocked_dynamodb_operators:
                if op in value_lower:
                    return InjectionResult(
                        safe=False,
                        injection_type=InjectionType.NOSQL_INJECTION,
                        pattern_matched=op,
                        details=f"DynamoDB operator in value: {op}"
                    )

        elif isinstance(value, dict):
            for key, val in value.items():
                result = self.check_dynamodb(val)
                if not result.safe:
                    return result

        elif isinstance(value, list):
            for item in value:
                result = self.check_dynamodb(item)
                if not result.safe:
                    return result

        return InjectionResult(safe=True)

    def sanitize_for_mongodb(self, value: Dict) -> Dict:
        """
        Remove MongoDB operators from a dictionary.

        Args:
            value: Dictionary to sanitize

        Returns:
            Sanitized dictionary with operators removed
        """
        if not isinstance(value, dict):
            return value

        sanitized = {}
        for key, val in value.items():
            # Skip operator keys
            if key.startswith("$"):
                logger.warning(f"Removing MongoDB operator from input: {key}")
                continue

            # Recursively sanitize nested values
            if isinstance(val, dict):
                sanitized[key] = self.sanitize_for_mongodb(val)
            elif isinstance(val, list):
                sanitized[key] = [
                    self.sanitize_for_mongodb(item) if isinstance(item, dict) else item
                    for item in val
                ]
            else:
                sanitized[key] = val

        return sanitized
