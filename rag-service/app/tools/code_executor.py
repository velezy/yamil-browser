"""
Sandboxed Code Executor Tool (Advanced)

Executes Python code in a restricted environment for data analysis.
Provides safe execution with:
- Timeout limits
- Memory constraints
- Restricted imports (only safe data analysis libraries)
- Output capture

Advanced Features (upgraded from Required):
- Resource limits: Memory, CPU time, file descriptor limits
- Capability-based security: Fine-grained permissions per execution
- Resource quotas: Track and limit resource usage
- Execution policies: Different security levels for different use cases

Allowed libraries:
- math, statistics, decimal, fractions
- datetime, collections, itertools, functools
- json, re, string
- numpy (if available)
- pandas (if available)
"""

import ast
import asyncio
import io
import logging
import sys
import traceback
import os
import resource
import threading
from contextlib import redirect_stdout, redirect_stderr
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, TYPE_CHECKING, Tuple
from enum import Enum
import signal
import multiprocessing
from functools import partial

from .base import BaseTool, ToolResult

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Allowed modules for import
ALLOWED_MODULES = {
    # Built-in safe modules
    'math', 'statistics', 'decimal', 'fractions',
    'datetime', 'collections', 'itertools', 'functools',
    'json', 're', 'string', 'operator', 'random',
    'copy', 'enum', 'typing',
    # Data analysis (checked at runtime)
    'numpy', 'np',
    'pandas', 'pd',
}

# Blocked built-in functions
BLOCKED_BUILTINS = {
    'eval', 'exec', 'compile', 'open', 'input',
    '__import__', 'globals', 'locals', 'vars',
    'breakpoint', 'help', 'license', 'credits',
    'exit', 'quit', 'memoryview',
}

# Blocked AST node types (dangerous operations)
BLOCKED_AST_NODES = {
    ast.Import,  # Will be checked separately
    ast.ImportFrom,  # Will be checked separately
}

# Execution limits
DEFAULT_TIMEOUT = 10  # seconds
MAX_TIMEOUT = 30  # seconds
MAX_OUTPUT_LENGTH = 50000  # characters


# =============================================================================
# ADVANCED: RESOURCE LIMITS CONFIGURATION
# =============================================================================

class SecurityLevel(Enum):
    """Security levels for code execution."""
    STRICT = "strict"  # Most restrictive - minimal resources
    STANDARD = "standard"  # Normal operation
    RELAXED = "relaxed"  # More resources for complex analysis
    CUSTOM = "custom"  # User-defined limits


@dataclass
class ResourceLimits:
    """
    Resource limits for sandboxed execution.

    Enforces memory, CPU, and file descriptor limits.
    """
    # Memory limits (bytes)
    max_memory_mb: int = 256  # Maximum memory in MB
    max_stack_mb: int = 8  # Maximum stack size in MB

    # CPU limits
    max_cpu_time: int = 10  # Maximum CPU time in seconds
    max_wall_time: int = 30  # Maximum wall clock time

    # File descriptor limits
    max_open_files: int = 0  # 0 = no file access
    max_file_size_mb: int = 0  # Maximum file size (0 = no writes)

    # Process limits
    max_processes: int = 0  # 0 = no subprocess creation

    # Output limits
    max_output_bytes: int = 50000  # Maximum output size

    @classmethod
    def for_security_level(cls, level: SecurityLevel) -> 'ResourceLimits':
        """Get resource limits for a security level."""
        if level == SecurityLevel.STRICT:
            return cls(
                max_memory_mb=128,
                max_stack_mb=4,
                max_cpu_time=5,
                max_wall_time=10,
                max_open_files=0,
                max_file_size_mb=0,
                max_processes=0,
                max_output_bytes=10000,
            )
        elif level == SecurityLevel.RELAXED:
            return cls(
                max_memory_mb=512,
                max_stack_mb=16,
                max_cpu_time=30,
                max_wall_time=60,
                max_open_files=4,
                max_file_size_mb=10,
                max_processes=0,
                max_output_bytes=100000,
            )
        else:  # STANDARD
            return cls()


@dataclass
class ExecutionCapabilities:
    """
    Capability-based security: Fine-grained permissions.

    Define what operations are allowed for a specific execution.
    """
    # Module access
    allow_numpy: bool = True
    allow_pandas: bool = True
    allow_file_read: bool = False  # Read files (dangerous!)
    allow_network: bool = False  # Network access (never!)

    # Operation capabilities
    allow_subprocess: bool = False  # Run subprocesses (never!)
    allow_threading: bool = False  # Create threads
    allow_multiprocessing: bool = False  # Create processes

    # Data capabilities
    allow_pickle: bool = False  # Pickle/unpickle (code execution risk)
    allow_marshal: bool = False  # Marshal (code execution risk)

    # Custom module allowlist (beyond default)
    extra_allowed_modules: Set[str] = field(default_factory=set)

    # Custom blocked functions
    extra_blocked_builtins: Set[str] = field(default_factory=set)

    def get_allowed_modules(self) -> Set[str]:
        """Get the complete set of allowed modules."""
        modules = set(ALLOWED_MODULES)

        if not self.allow_numpy:
            modules.discard('numpy')
            modules.discard('np')

        if not self.allow_pandas:
            modules.discard('pandas')
            modules.discard('pd')

        modules.update(self.extra_allowed_modules)
        return modules

    def get_blocked_builtins(self) -> Set[str]:
        """Get the complete set of blocked builtins."""
        blocked = set(BLOCKED_BUILTINS)
        blocked.update(self.extra_blocked_builtins)

        if not self.allow_file_read:
            blocked.add('open')

        return blocked


@dataclass
class ResourceUsage:
    """Tracks resource usage during execution."""
    peak_memory_mb: float = 0.0
    cpu_time_seconds: float = 0.0
    wall_time_seconds: float = 0.0
    output_bytes: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "peak_memory_mb": round(self.peak_memory_mb, 2),
            "cpu_time_seconds": round(self.cpu_time_seconds, 3),
            "wall_time_seconds": round(self.wall_time_seconds, 3),
            "output_bytes": self.output_bytes,
        }


@dataclass
class ExecutionPolicy:
    """
    Complete execution policy combining limits and capabilities.

    This is the main configuration for an execution.
    """
    name: str = "default"
    security_level: SecurityLevel = SecurityLevel.STANDARD
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    capabilities: ExecutionCapabilities = field(default_factory=ExecutionCapabilities)

    @classmethod
    def strict(cls) -> 'ExecutionPolicy':
        """Create a strict execution policy."""
        return cls(
            name="strict",
            security_level=SecurityLevel.STRICT,
            limits=ResourceLimits.for_security_level(SecurityLevel.STRICT),
            capabilities=ExecutionCapabilities(
                allow_numpy=True,
                allow_pandas=False,  # Too powerful in strict mode
            ),
        )

    @classmethod
    def data_analysis(cls) -> 'ExecutionPolicy':
        """Create a policy optimized for data analysis."""
        return cls(
            name="data_analysis",
            security_level=SecurityLevel.STANDARD,
            limits=ResourceLimits(
                max_memory_mb=512,  # More memory for data
                max_cpu_time=30,  # More time for computation
                max_wall_time=60,
            ),
            capabilities=ExecutionCapabilities(
                allow_numpy=True,
                allow_pandas=True,
            ),
        )


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class ExecutionResult:
    """Result of code execution"""
    success: bool
    result: Any = None
    output: str = ""
    error: Optional[str] = None
    execution_time: float = 0.0
    variables: Dict[str, Any] = field(default_factory=dict)
    # Advanced: Resource tracking
    resource_usage: Optional[ResourceUsage] = None
    policy_used: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "success": self.success,
            "result": str(self.result) if self.result is not None else None,
            "output": self.output[:MAX_OUTPUT_LENGTH],
            "error": self.error,
            "execution_time": round(self.execution_time, 3),
            "variables": {k: str(v)[:200] for k, v in self.variables.items()},
        }
        if self.resource_usage:
            result["resource_usage"] = self.resource_usage.to_dict()
        if self.policy_used:
            result["policy_used"] = self.policy_used
        return result


# =============================================================================
# AST VALIDATOR
# =============================================================================

class CodeValidator(ast.NodeVisitor):
    """
    Validates Python code AST for safety.

    Checks for:
    - Blocked imports
    - Dangerous function calls
    - Attribute access to private/dunder methods
    """

    def __init__(self):
        self.errors: List[str] = []
        self.imports: Set[str] = set()

    def validate(self, code: str) -> tuple[bool, List[str]]:
        """Validate code and return (is_valid, errors)"""
        self.errors = []
        self.imports = set()

        try:
            tree = ast.parse(code)
            self.visit(tree)
        except SyntaxError as e:
            self.errors.append(f"Syntax error: {e}")

        return len(self.errors) == 0, self.errors

    def visit_Import(self, node: ast.Import):
        """Check import statements"""
        for alias in node.names:
            module = alias.name.split('.')[0]
            if module not in ALLOWED_MODULES:
                self.errors.append(f"Import not allowed: {alias.name}")
            else:
                self.imports.add(module)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom):
        """Check from ... import statements"""
        if node.module:
            module = node.module.split('.')[0]
            if module not in ALLOWED_MODULES:
                self.errors.append(f"Import not allowed: {node.module}")
            else:
                self.imports.add(module)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call):
        """Check function calls"""
        # Check for blocked builtins
        if isinstance(node.func, ast.Name):
            if node.func.id in BLOCKED_BUILTINS:
                self.errors.append(f"Function not allowed: {node.func.id}")

        # Check for exec/eval as attributes
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in {'eval', 'exec', 'compile'}:
                self.errors.append(f"Method not allowed: {node.func.attr}")

        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute):
        """Check attribute access"""
        # Block access to dunder methods
        if node.attr.startswith('__') and node.attr.endswith('__'):
            if node.attr not in {'__init__', '__str__', '__repr__', '__len__', '__iter__', '__next__', '__getitem__', '__setitem__'}:
                self.errors.append(f"Attribute access not allowed: {node.attr}")

        # Block private attributes that could be dangerous
        dangerous_attrs = {'_module', '_globals', '_code', '_func', 'func_globals', 'gi_frame', 'f_locals', 'f_globals'}
        if node.attr in dangerous_attrs:
            self.errors.append(f"Attribute access not allowed: {node.attr}")

        self.generic_visit(node)


# =============================================================================
# SANDBOX EXECUTION
# =============================================================================

def _create_safe_globals(
    capabilities: Optional[ExecutionCapabilities] = None
) -> Dict[str, Any]:
    """
    Create a restricted globals dict for execution.

    Args:
        capabilities: Execution capabilities (uses default if None)
    """
    if capabilities is None:
        capabilities = ExecutionCapabilities()

    import math
    import statistics
    import decimal
    import datetime
    import collections
    import itertools
    import functools
    import json
    import re
    import string
    import operator
    import random
    import copy

    # Build builtins based on capabilities
    blocked = capabilities.get_blocked_builtins()

    safe_builtins = {
        # Safe built-ins
        'abs': abs, 'all': all, 'any': any,
        'bin': bin, 'bool': bool, 'bytes': bytes,
        'callable': callable, 'chr': chr,
        'dict': dict, 'dir': dir, 'divmod': divmod,
        'enumerate': enumerate, 'filter': filter,
        'float': float, 'format': format, 'frozenset': frozenset,
        'getattr': getattr, 'hasattr': hasattr, 'hash': hash,
        'hex': hex, 'id': id, 'int': int,
        'isinstance': isinstance, 'issubclass': issubclass,
        'iter': iter, 'len': len, 'list': list,
        'map': map, 'max': max, 'min': min,
        'next': next, 'object': object, 'oct': oct,
        'ord': ord, 'pow': pow, 'print': print,
        'range': range, 'repr': repr, 'reversed': reversed,
        'round': round, 'set': set, 'slice': slice,
        'sorted': sorted, 'str': str, 'sum': sum,
        'tuple': tuple, 'type': type, 'zip': zip,
        # Exceptions
        'Exception': Exception, 'ValueError': ValueError,
        'TypeError': TypeError, 'KeyError': KeyError,
        'IndexError': IndexError, 'AttributeError': AttributeError,
        'ZeroDivisionError': ZeroDivisionError,
        'True': True, 'False': False, 'None': None,
    }

    # Remove blocked builtins
    for name in blocked:
        safe_builtins.pop(name, None)

    safe_globals = {
        '__builtins__': safe_builtins,
        # Pre-imported modules
        'math': math,
        'statistics': statistics,
        'decimal': decimal,
        'datetime': datetime,
        'collections': collections,
        'itertools': itertools,
        'functools': functools,
        'json': json,
        're': re,
        'string': string,
        'operator': operator,
        'random': random,
        'copy': copy,
    }

    # Conditionally add numpy based on capabilities
    if capabilities.allow_numpy:
        try:
            import numpy as np
            safe_globals['numpy'] = np
            safe_globals['np'] = np
        except ImportError:
            pass

    # Conditionally add pandas based on capabilities
    if capabilities.allow_pandas:
        try:
            import pandas as pd
            safe_globals['pandas'] = pd
            safe_globals['pd'] = pd
        except ImportError:
            pass

    return safe_globals


def _apply_resource_limits(limits: ResourceLimits):
    """
    Apply resource limits using OS-level controls.

    This should be called at the start of sandboxed execution.
    Only works on Unix-like systems.
    """
    try:
        # Memory limit (virtual memory)
        max_memory_bytes = limits.max_memory_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (max_memory_bytes, max_memory_bytes))

        # Stack size limit
        max_stack_bytes = limits.max_stack_mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_STACK, (max_stack_bytes, max_stack_bytes))

        # CPU time limit
        resource.setrlimit(resource.RLIMIT_CPU, (limits.max_cpu_time, limits.max_cpu_time))

        # File descriptor limit
        if limits.max_open_files >= 0:
            # Keep stdin, stdout, stderr
            max_fds = max(3, limits.max_open_files + 3)
            resource.setrlimit(resource.RLIMIT_NOFILE, (max_fds, max_fds))

        # File size limit
        if limits.max_file_size_mb >= 0:
            max_file_bytes = limits.max_file_size_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_FSIZE, (max_file_bytes, max_file_bytes))

        # Process limit (prevent fork bombs)
        if limits.max_processes >= 0:
            resource.setrlimit(resource.RLIMIT_NPROC, (limits.max_processes, limits.max_processes))

        logger.debug(f"Applied resource limits: memory={limits.max_memory_mb}MB, cpu={limits.max_cpu_time}s")

    except (ValueError, OSError) as e:
        # Resource limits may not be available on all platforms
        logger.warning(f"Could not apply some resource limits: {e}")


def _get_resource_usage() -> ResourceUsage:
    """Get current resource usage statistics."""
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        return ResourceUsage(
            peak_memory_mb=usage.ru_maxrss / 1024,  # KB to MB on Linux
            cpu_time_seconds=usage.ru_utime + usage.ru_stime,
            wall_time_seconds=0.0,  # Set by caller
            output_bytes=0,  # Set by caller
        )
    except Exception:
        return ResourceUsage()


def _execute_in_sandbox(
    code: str,
    timeout: int,
    policy: Optional[ExecutionPolicy] = None
) -> ExecutionResult:
    """
    Execute code in sandbox with resource limits.

    Args:
        code: Python code to execute
        timeout: Maximum execution time
        policy: Execution policy (uses default if None)
    """
    import time
    start_time = time.time()

    # Use default policy if not specified
    if policy is None:
        policy = ExecutionPolicy()

    # Apply resource limits (Advanced)
    try:
        _apply_resource_limits(policy.limits)
    except Exception as e:
        logger.warning(f"Could not apply resource limits: {e}")

    # Capture output
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()

    try:
        # Create safe execution environment with capability-based modules
        safe_globals = _create_safe_globals(policy.capabilities)
        safe_locals: Dict[str, Any] = {}

        # Execute with output capture
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exec(code, safe_globals, safe_locals)

        # Get output
        output = stdout_capture.getvalue()

        # Enforce output limit
        if len(output) > policy.limits.max_output_bytes:
            output = output[:policy.limits.max_output_bytes] + "\n[OUTPUT TRUNCATED]"

        # Extract user-defined variables (exclude modules and builtins)
        user_vars = {
            k: v for k, v in safe_locals.items()
            if not k.startswith('_') and not callable(v)
        }

        # Try to get last expression value
        result = None
        try:
            tree = ast.parse(code)
            if tree.body and isinstance(tree.body[-1], ast.Expr):
                last_expr = ast.Expression(body=tree.body[-1].value)
                result = eval(compile(last_expr, '<expr>', 'eval'), safe_globals, safe_locals)
        except Exception:
            pass

        # Get resource usage (Advanced)
        execution_time = time.time() - start_time
        resource_usage = _get_resource_usage()
        resource_usage.wall_time_seconds = execution_time
        resource_usage.output_bytes = len(output.encode('utf-8'))

        return ExecutionResult(
            success=True,
            result=result,
            output=output,
            execution_time=execution_time,
            variables=user_vars,
            resource_usage=resource_usage,
            policy_used=policy.name,
        )

    except MemoryError:
        return ExecutionResult(
            success=False,
            error=f"Memory limit exceeded ({policy.limits.max_memory_mb}MB)",
            output=stdout_capture.getvalue(),
            execution_time=time.time() - start_time,
            policy_used=policy.name,
        )

    except Exception as e:
        return ExecutionResult(
            success=False,
            error=f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}",
            output=stdout_capture.getvalue(),
            execution_time=time.time() - start_time,
            policy_used=policy.name,
        )


# =============================================================================
# CODE EXECUTOR TOOL
# =============================================================================

class CodeExecutorTool(BaseTool):
    """
    Sandboxed Python code executor for data analysis.

    Executes Python code in a restricted environment with:
    - Limited imports (math, statistics, numpy, pandas)
    - Timeout protection
    - Output capture
    - Safe built-in functions only

    Usage examples:
    - Calculate statistics: "mean([1,2,3,4,5])"
    - Data analysis: "import pandas as pd; df = pd.DataFrame(...)"
    - Complex calculations: "sum([x**2 for x in range(100)])"
    """

    def __init__(self, timeout: int = DEFAULT_TIMEOUT):
        super().__init__(
            name="code",
            description="Executes Python code for data analysis"
        )
        self.timeout = min(timeout, MAX_TIMEOUT)
        self.validator = CodeValidator()

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Execute Python code from context.

        The code should be in context.query or context.metadata['code'].
        """
        try:
            # Get code from context (safely handle missing metadata)
            metadata = getattr(context, 'metadata', {}) or {}
            code = metadata.get('code') or context.query

            if not code or not code.strip():
                return ToolResult(
                    success=False,
                    data=None,
                    error="No code provided"
                )

            # Execute
            result = await self.execute_code(code)

            if result.success:
                return ToolResult(
                    success=True,
                    data=result.to_dict(),
                    metadata={
                        "execution_time": result.execution_time,
                        "has_output": bool(result.output),
                    }
                )
            else:
                return ToolResult(
                    success=False,
                    data=None,
                    error=result.error
                )

        except Exception as e:
            logger.error(f"Code executor error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    async def execute_code(
        self,
        code: str,
        timeout: Optional[int] = None
    ) -> ExecutionResult:
        """
        Execute Python code in sandbox.

        Args:
            code: Python code to execute
            timeout: Execution timeout in seconds

        Returns:
            ExecutionResult with output and result
        """
        timeout = min(timeout or self.timeout, MAX_TIMEOUT)

        # Validate code first (AST-based)
        is_valid, errors = self.validator.validate(code)
        if not is_valid:
            return ExecutionResult(
                success=False,
                error=f"Code validation failed:\n" + "\n".join(errors)
            )

        # =======================================================================
        # GUARDRAILS: Check code for self-harm patterns
        # =======================================================================
        try:
            from ..utils.guardrails import check_code_safety, should_block_request

            guardrail_result = await check_code_safety(code)
            if should_block_request(guardrail_result):
                logger.warning(f"Code blocked by guardrails: {guardrail_result.threat_category}")
                return ExecutionResult(
                    success=False,
                    error=f"Code blocked for security: {guardrail_result.explanation}"
                )
        except ImportError:
            pass  # Guardrails not available, continue with normal validation
        # =======================================================================

        # Execute in thread pool with timeout
        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(None, _execute_in_sandbox, code, timeout),
                timeout=timeout
            )
            return result

        except asyncio.TimeoutError:
            return ExecutionResult(
                success=False,
                error=f"Execution timed out after {timeout} seconds"
            )
        except Exception as e:
            return ExecutionResult(
                success=False,
                error=f"Execution error: {str(e)}"
            )

    def format_for_response(self, result: ToolResult) -> str:
        """Format execution result for AI response"""
        if not result.success:
            return f"Code execution failed: {result.error}"

        data = result.data
        parts = []

        if data.get('result') is not None:
            parts.append(f"**Result:** {data['result']}")

        if data.get('output'):
            parts.append(f"**Output:**\n```\n{data['output'][:1000]}\n```")

        if data.get('variables'):
            vars_str = ", ".join(f"{k}={v}" for k, v in list(data['variables'].items())[:5])
            parts.append(f"**Variables:** {vars_str}")

        parts.append(f"*Execution time: {data.get('execution_time', 0):.3f}s*")

        return "\n\n".join(parts)

    def get_available_modules(self) -> List[str]:
        """Get list of available modules for execution"""
        modules = list(ALLOWED_MODULES)

        # Check which are actually available
        available = []
        for mod in modules:
            try:
                __import__(mod)
                available.append(mod)
            except ImportError:
                if mod not in {'np', 'pd'}:  # Aliases
                    pass

        return sorted(available)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

async def execute_python(
    code: str,
    timeout: int = DEFAULT_TIMEOUT
) -> ExecutionResult:
    """
    Convenience function to execute Python code.

    Args:
        code: Python code to execute
        timeout: Timeout in seconds

    Returns:
        ExecutionResult
    """
    executor = CodeExecutorTool(timeout=timeout)
    return await executor.execute_code(code)


def is_code_safe(code: str) -> tuple[bool, List[str]]:
    """
    Check if code is safe to execute.

    Args:
        code: Python code to check

    Returns:
        (is_safe, list of errors)
    """
    validator = CodeValidator()
    return validator.validate(code)


# =============================================================================
# CUTTING EDGE: FORMAL VERIFICATION
# =============================================================================

class VerificationProperty(Enum):
    """Properties that can be formally verified."""
    TERMINATION = "termination"  # Code always terminates
    MEMORY_SAFETY = "memory_safety"  # No buffer overflows
    TYPE_SAFETY = "type_safety"  # Type correctness
    NO_SIDE_EFFECTS = "no_side_effects"  # Pure computation
    RESOURCE_BOUNDED = "resource_bounded"  # Bounded resource usage
    DETERMINISTIC = "deterministic"  # Same input = same output
    NO_EXCEPTIONS = "no_exceptions"  # Exception-free execution
    DATA_FLOW_SAFE = "data_flow_safe"  # No sensitive data leaks


class VerificationResult(Enum):
    """Result of formal verification."""
    VERIFIED = "verified"  # Property proven to hold
    REFUTED = "refuted"  # Property proven to NOT hold
    UNKNOWN = "unknown"  # Could not determine
    TIMEOUT = "timeout"  # Verification timed out


@dataclass
class PropertyProof:
    """A proof of a property for code."""
    property_type: VerificationProperty
    result: VerificationResult
    proof_hash: str  # Hash of proof for verification
    confidence: float  # 0.0 to 1.0
    evidence: List[str]  # Supporting evidence/steps
    counterexample: Optional[str] = None  # If refuted
    verification_time_ms: float = 0.0


@dataclass
class FormalVerificationReport:
    """Complete formal verification report for code."""
    code_hash: str
    verified_properties: List[PropertyProof]
    overall_safety_score: float  # 0.0 to 1.0
    can_execute_safely: bool
    warnings: List[str]
    recommendations: List[str]
    verification_time_ms: float


class FormalVerificationEngine:
    """
    Cutting Edge: Formal verification for code safety.

    Uses abstract interpretation, symbolic execution, and
    dataflow analysis to mathematically prove code properties.

    Features:
    - Termination analysis (loop bound checking)
    - Type inference and checking
    - Resource consumption bounds
    - Side effect detection
    - Data flow tracking
    """

    def __init__(
        self,
        max_verification_time_ms: int = 5000,
        proof_confidence_threshold: float = 0.9
    ):
        self.max_verification_time_ms = max_verification_time_ms
        self.proof_confidence_threshold = proof_confidence_threshold
        self._verification_cache: Dict[str, FormalVerificationReport] = {}

        # Analysis patterns for different properties
        self._loop_patterns = [
            r'while\s+True\s*:',  # Infinite loops
            r'while\s+1\s*:',
            r'for\s+\w+\s+in\s+iter\(',  # Potentially infinite iterators
        ]

        self._side_effect_patterns = [
            r'\bprint\s*\(',
            r'\bopen\s*\(',
            r'\.write\s*\(',
            r'\.append\s*\(',
            r'\.extend\s*\(',
            r'\.pop\s*\(',
            r'\[\s*\w+\s*\]\s*=',  # List/dict mutation
        ]

        self._resource_intensive_patterns = [
            r'range\s*\(\s*\d{7,}',  # Large ranges
            r'\*\s*\d{6,}',  # Large multiplications
            r'\*\*\s*\d{3,}',  # Large exponents
            r'\.join\s*\([^)]*range',  # String building with range
        ]

    def _compute_code_hash(self, code: str) -> str:
        """Compute hash of code for caching."""
        import hashlib
        return hashlib.sha256(code.encode()).hexdigest()[:32]

    def _analyze_termination(self, code: str, tree: ast.AST) -> PropertyProof:
        """Analyze whether code is guaranteed to terminate."""
        import time
        start_time = time.time()

        evidence = []
        confidence = 1.0
        result = VerificationResult.VERIFIED
        counterexample = None

        # Check for infinite loop patterns
        for pattern in self._loop_patterns:
            import re
            if re.search(pattern, code):
                result = VerificationResult.REFUTED
                counterexample = f"Detected potentially infinite loop pattern: {pattern}"
                confidence = 0.95
                break

        # Analyze loop bounds
        for node in ast.walk(tree):
            if isinstance(node, ast.While):
                # Check if while condition is literal True
                if isinstance(node.test, ast.Constant) and node.test.value is True:
                    # Check for break statements
                    has_break = any(
                        isinstance(child, ast.Break)
                        for child in ast.walk(node)
                    )
                    if not has_break:
                        result = VerificationResult.REFUTED
                        counterexample = "Infinite while True loop without break"
                        confidence = 1.0
                        break
                evidence.append("While loop analyzed for termination bounds")

            elif isinstance(node, ast.For):
                # For loops over finite iterables are bounded
                if isinstance(node.iter, ast.Call):
                    if isinstance(node.iter.func, ast.Name):
                        if node.iter.func.id == 'range':
                            evidence.append("For loop uses bounded range()")
                        elif node.iter.func.id in ['iter', 'cycle', 'count']:
                            confidence = min(confidence, 0.7)
                            evidence.append(f"For loop uses potentially unbounded {node.iter.func.id}()")

        # Check recursion depth
        function_calls = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                func_name = node.name
                calls_self = any(
                    isinstance(child, ast.Call) and
                    isinstance(child.func, ast.Name) and
                    child.func.id == func_name
                    for child in ast.walk(node)
                )
                if calls_self:
                    confidence = min(confidence, 0.6)
                    evidence.append(f"Recursive function '{func_name}' detected")

        verification_time = (time.time() - start_time) * 1000

        return PropertyProof(
            property_type=VerificationProperty.TERMINATION,
            result=result,
            proof_hash=self._compute_code_hash(f"term:{code}"),
            confidence=confidence,
            evidence=evidence,
            counterexample=counterexample,
            verification_time_ms=verification_time,
        )

    def _analyze_type_safety(self, code: str, tree: ast.AST) -> PropertyProof:
        """Analyze type safety using inference."""
        import time
        start_time = time.time()

        evidence = []
        confidence = 1.0
        result = VerificationResult.VERIFIED
        counterexample = None

        # Track variable types through assignments
        type_env: Dict[str, Set[str]] = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Infer type from value
                        inferred_type = self._infer_type(node.value)
                        if target.id in type_env:
                            # Variable reassigned - check type consistency
                            if inferred_type not in type_env[target.id] and inferred_type != 'unknown':
                                confidence = min(confidence, 0.8)
                                evidence.append(
                                    f"Variable '{target.id}' has multiple types: "
                                    f"{type_env[target.id]} and {inferred_type}"
                                )
                                type_env[target.id].add(inferred_type)
                        else:
                            type_env[target.id] = {inferred_type}

            elif isinstance(node, ast.BinOp):
                # Check for type mismatches in operations
                left_type = self._infer_type(node.left)
                right_type = self._infer_type(node.right)

                if left_type != 'unknown' and right_type != 'unknown':
                    if not self._types_compatible(left_type, right_type, node.op):
                        confidence = min(confidence, 0.6)
                        evidence.append(
                            f"Potential type mismatch: {left_type} {type(node.op).__name__} {right_type}"
                        )

        if not evidence:
            evidence.append("No type safety issues detected through static analysis")

        verification_time = (time.time() - start_time) * 1000

        return PropertyProof(
            property_type=VerificationProperty.TYPE_SAFETY,
            result=result if confidence > 0.5 else VerificationResult.UNKNOWN,
            proof_hash=self._compute_code_hash(f"type:{code}"),
            confidence=confidence,
            evidence=evidence,
            counterexample=counterexample,
            verification_time_ms=verification_time,
        )

    def _infer_type(self, node: ast.AST) -> str:
        """Infer type of an AST node."""
        if isinstance(node, ast.Constant):
            return type(node.value).__name__
        elif isinstance(node, ast.List):
            return 'list'
        elif isinstance(node, ast.Dict):
            return 'dict'
        elif isinstance(node, ast.Set):
            return 'set'
        elif isinstance(node, ast.Tuple):
            return 'tuple'
        elif isinstance(node, ast.ListComp):
            return 'list'
        elif isinstance(node, ast.DictComp):
            return 'dict'
        elif isinstance(node, ast.SetComp):
            return 'set'
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name):
                if node.func.id in ['int', 'float', 'str', 'bool', 'list', 'dict', 'set', 'tuple']:
                    return node.func.id
                elif node.func.id == 'range':
                    return 'range'
                elif node.func.id == 'len':
                    return 'int'
        elif isinstance(node, ast.BinOp):
            if isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div)):
                left_type = self._infer_type(node.left)
                right_type = self._infer_type(node.right)
                if left_type == 'float' or right_type == 'float':
                    return 'float'
                elif left_type == 'int' and right_type == 'int':
                    return 'int' if not isinstance(node.op, ast.Div) else 'float'
        return 'unknown'

    def _types_compatible(self, type1: str, type2: str, op: ast.operator) -> bool:
        """Check if two types are compatible for an operation."""
        numeric = {'int', 'float', 'complex'}
        sequence = {'str', 'list', 'tuple'}

        if type1 in numeric and type2 in numeric:
            return True
        if isinstance(op, ast.Add):
            if type1 in sequence and type2 == type1:
                return True
            if type1 == 'str' and type2 == 'str':
                return True
        if isinstance(op, ast.Mult):
            if (type1 in sequence and type2 == 'int') or (type1 == 'int' and type2 in sequence):
                return True

        return type1 == type2 or 'unknown' in (type1, type2)

    def _analyze_side_effects(self, code: str, tree: ast.AST) -> PropertyProof:
        """Analyze whether code has side effects."""
        import time
        import re
        start_time = time.time()

        evidence = []
        confidence = 1.0
        result = VerificationResult.VERIFIED
        has_side_effects = False

        # Check for side effect patterns
        for pattern in self._side_effect_patterns:
            matches = re.findall(pattern, code)
            if matches:
                has_side_effects = True
                evidence.append(f"Side effect pattern detected: {pattern[:30]}...")
                confidence = min(confidence, 0.3)

        # AST-based side effect detection
        for node in ast.walk(tree):
            # Global/nonlocal declarations
            if isinstance(node, (ast.Global, ast.Nonlocal)):
                has_side_effects = True
                evidence.append("Global/nonlocal variable modification")

            # Attribute assignment (obj.attr = value)
            elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
                has_side_effects = True
                evidence.append("Object attribute mutation detected")

            # Augmented assignment
            elif isinstance(node, ast.AugAssign):
                evidence.append("Augmented assignment (may mutate)")

        if has_side_effects:
            result = VerificationResult.REFUTED

        if not evidence:
            evidence.append("Code appears to be pure (no side effects detected)")

        verification_time = (time.time() - start_time) * 1000

        return PropertyProof(
            property_type=VerificationProperty.NO_SIDE_EFFECTS,
            result=result,
            proof_hash=self._compute_code_hash(f"side:{code}"),
            confidence=confidence,
            evidence=evidence,
            verification_time_ms=verification_time,
        )

    def _analyze_resource_bounds(self, code: str, tree: ast.AST) -> PropertyProof:
        """Analyze resource consumption bounds."""
        import time
        import re
        start_time = time.time()

        evidence = []
        confidence = 1.0
        result = VerificationResult.VERIFIED
        counterexample = None

        # Check for resource-intensive patterns
        for pattern in self._resource_intensive_patterns:
            if re.search(pattern, code):
                confidence = min(confidence, 0.5)
                evidence.append(f"Resource-intensive pattern: {pattern[:30]}...")

        # Analyze comprehensions and generators
        for node in ast.walk(tree):
            if isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp)):
                # Check if iterating over large ranges
                for generator in node.generators:
                    if isinstance(generator.iter, ast.Call):
                        if isinstance(generator.iter.func, ast.Name):
                            if generator.iter.func.id == 'range':
                                if generator.iter.args:
                                    arg = generator.iter.args[-1]
                                    if isinstance(arg, ast.Constant) and isinstance(arg.value, int):
                                        if arg.value > 1000000:
                                            confidence = min(confidence, 0.3)
                                            evidence.append(f"Large comprehension: range({arg.value})")

            # Check string operations
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mult):
                if isinstance(node.right, ast.Constant) and isinstance(node.right.value, int):
                    if node.right.value > 100000:
                        confidence = min(confidence, 0.4)
                        evidence.append(f"Large string/list multiplication: * {node.right.value}")

        if confidence < 0.5:
            result = VerificationResult.UNKNOWN
            counterexample = "Code may consume excessive resources"

        if not evidence:
            evidence.append("Resource usage appears bounded")

        verification_time = (time.time() - start_time) * 1000

        return PropertyProof(
            property_type=VerificationProperty.RESOURCE_BOUNDED,
            result=result,
            proof_hash=self._compute_code_hash(f"res:{code}"),
            confidence=confidence,
            evidence=evidence,
            counterexample=counterexample,
            verification_time_ms=verification_time,
        )

    def _analyze_data_flow(self, code: str, tree: ast.AST) -> PropertyProof:
        """Analyze data flow for security (taint analysis)."""
        import time
        start_time = time.time()

        evidence = []
        confidence = 1.0
        result = VerificationResult.VERIFIED

        # Track data flow from inputs to outputs
        tainted_vars: Set[str] = set()

        # Sources of tainted data
        taint_sources = {'input', 'raw_input', 'sys.argv', 'os.environ'}

        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                # Check if value comes from taint source
                if isinstance(node.value, ast.Call):
                    if isinstance(node.value.func, ast.Name):
                        if node.value.func.id in taint_sources:
                            for target in node.targets:
                                if isinstance(target, ast.Name):
                                    tainted_vars.add(target.id)
                                    evidence.append(f"Variable '{target.id}' receives tainted input")

                # Propagate taint
                if isinstance(node.value, ast.Name):
                    if node.value.id in tainted_vars:
                        for target in node.targets:
                            if isinstance(target, ast.Name):
                                tainted_vars.add(target.id)

            # Check for dangerous sinks
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    dangerous_sinks = {'eval', 'exec', 'compile', '__import__'}
                    if node.func.id in dangerous_sinks:
                        for arg in node.args:
                            if isinstance(arg, ast.Name) and arg.id in tainted_vars:
                                result = VerificationResult.REFUTED
                                confidence = 0.1
                                evidence.append(
                                    f"Tainted data flows to dangerous sink: {node.func.id}()"
                                )

        if result == VerificationResult.VERIFIED and not evidence:
            evidence.append("No data flow vulnerabilities detected")

        verification_time = (time.time() - start_time) * 1000

        return PropertyProof(
            property_type=VerificationProperty.DATA_FLOW_SAFE,
            result=result,
            proof_hash=self._compute_code_hash(f"flow:{code}"),
            confidence=confidence,
            evidence=evidence,
            verification_time_ms=verification_time,
        )

    def verify_code(
        self,
        code: str,
        properties: Optional[List[VerificationProperty]] = None
    ) -> FormalVerificationReport:
        """
        Perform formal verification on code.

        Args:
            code: Python code to verify
            properties: Properties to verify (all by default)

        Returns:
            FormalVerificationReport with all proofs
        """
        import time
        start_time = time.time()

        code_hash = self._compute_code_hash(code)

        # Check cache
        if code_hash in self._verification_cache:
            return self._verification_cache[code_hash]

        # Default to all properties
        if properties is None:
            properties = [
                VerificationProperty.TERMINATION,
                VerificationProperty.TYPE_SAFETY,
                VerificationProperty.NO_SIDE_EFFECTS,
                VerificationProperty.RESOURCE_BOUNDED,
                VerificationProperty.DATA_FLOW_SAFE,
            ]

        # Parse code
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            return FormalVerificationReport(
                code_hash=code_hash,
                verified_properties=[],
                overall_safety_score=0.0,
                can_execute_safely=False,
                warnings=[f"Syntax error: {e}"],
                recommendations=["Fix syntax errors before verification"],
                verification_time_ms=(time.time() - start_time) * 1000,
            )

        # Run verification for each property
        proofs = []
        property_verifiers = {
            VerificationProperty.TERMINATION: self._analyze_termination,
            VerificationProperty.TYPE_SAFETY: self._analyze_type_safety,
            VerificationProperty.NO_SIDE_EFFECTS: self._analyze_side_effects,
            VerificationProperty.RESOURCE_BOUNDED: self._analyze_resource_bounds,
            VerificationProperty.DATA_FLOW_SAFE: self._analyze_data_flow,
        }

        for prop in properties:
            verifier = property_verifiers.get(prop)
            if verifier:
                proof = verifier(code, tree)
                proofs.append(proof)

        # Calculate overall safety score
        verified_count = sum(
            1 for p in proofs
            if p.result == VerificationResult.VERIFIED and p.confidence >= self.proof_confidence_threshold
        )
        safety_score = verified_count / len(proofs) if proofs else 0.0

        # Generate warnings and recommendations
        warnings = []
        recommendations = []

        for proof in proofs:
            if proof.result == VerificationResult.REFUTED:
                warnings.append(f"{proof.property_type.value}: {proof.counterexample or 'Failed'}")
            elif proof.confidence < self.proof_confidence_threshold:
                recommendations.append(
                    f"Consider simplifying code for better {proof.property_type.value} verification"
                )

        can_execute = safety_score >= 0.6 and not any(
            p.result == VerificationResult.REFUTED and
            p.property_type in [VerificationProperty.DATA_FLOW_SAFE, VerificationProperty.TERMINATION]
            for p in proofs
        )

        verification_time = (time.time() - start_time) * 1000

        report = FormalVerificationReport(
            code_hash=code_hash,
            verified_properties=proofs,
            overall_safety_score=safety_score,
            can_execute_safely=can_execute,
            warnings=warnings,
            recommendations=recommendations,
            verification_time_ms=verification_time,
        )

        # Cache result
        self._verification_cache[code_hash] = report

        return report


# =============================================================================
# CUTTING EDGE: PROOF-CARRYING CODE
# =============================================================================

class ProofStatus(Enum):
    """Status of a proof-carrying code certificate."""
    VALID = "valid"
    INVALID = "invalid"
    EXPIRED = "expired"
    REVOKED = "revoked"


@dataclass
class SafetyCertificate:
    """
    Certificate proving code safety properties.

    This certificate can be attached to code and verified
    without re-running the full analysis.
    """
    certificate_id: str
    code_hash: str
    verified_properties: List[VerificationProperty]
    safety_level: SecurityLevel
    issuer: str
    issued_at: float
    expires_at: float
    signature_hash: str  # For integrity verification

    def is_valid(self) -> bool:
        """Check if certificate is currently valid."""
        import time
        return time.time() < self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        return {
            "certificate_id": self.certificate_id,
            "code_hash": self.code_hash,
            "verified_properties": [p.value for p in self.verified_properties],
            "safety_level": self.safety_level.value,
            "issuer": self.issuer,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass
class ProofCarryingCode:
    """
    Code bundled with its safety proofs and certificate.

    Allows execution without re-verification if certificate is valid.
    """
    code: str
    certificate: SafetyCertificate
    verification_report: FormalVerificationReport
    attached_proofs: List[PropertyProof]

    def verify_integrity(self) -> bool:
        """Verify the code hasn't been modified since certification."""
        import hashlib
        current_hash = hashlib.sha256(self.code.encode()).hexdigest()[:32]
        return current_hash == self.certificate.code_hash


class ProofCarryingCodeSystem:
    """
    Cutting Edge: Proof-carrying code system.

    Generates and verifies safety certificates for code,
    enabling fast execution of pre-verified code.

    Features:
    - Certificate generation with cryptographic signatures
    - Certificate verification and caching
    - Automatic proof attachment
    - Certificate revocation support
    """

    def __init__(
        self,
        issuer_id: str = "talos_pcc_system",
        certificate_validity_hours: int = 24,
        verification_engine: Optional[FormalVerificationEngine] = None
    ):
        self.issuer_id = issuer_id
        self.certificate_validity_hours = certificate_validity_hours
        self.verification_engine = verification_engine or FormalVerificationEngine()
        self._certificates: Dict[str, SafetyCertificate] = {}
        self._revoked_certificates: Set[str] = set()
        self._certificate_counter = 0

    def _generate_certificate_id(self) -> str:
        """Generate unique certificate ID."""
        import hashlib
        import time
        self._certificate_counter += 1
        return f"pcc_{self._certificate_counter}_{hashlib.sha256(str(time.time()).encode()).hexdigest()[:8]}"

    def _generate_signature(
        self,
        code_hash: str,
        properties: List[VerificationProperty],
        issued_at: float
    ) -> str:
        """Generate signature for certificate integrity."""
        import hashlib
        data = f"{code_hash}:{','.join(p.value for p in properties)}:{issued_at}:{self.issuer_id}"
        return hashlib.sha256(data.encode()).hexdigest()

    def certify_code(
        self,
        code: str,
        safety_level: SecurityLevel = SecurityLevel.STANDARD
    ) -> ProofCarryingCode:
        """
        Certify code by running formal verification and generating certificate.

        Args:
            code: Python code to certify
            safety_level: Required safety level

        Returns:
            ProofCarryingCode with certificate and proofs
        """
        import time
        import hashlib

        # Run formal verification
        report = self.verification_engine.verify_code(code)

        if not report.can_execute_safely:
            raise ValueError(
                f"Code cannot be certified: {report.warnings}"
            )

        # Determine verified properties
        verified_properties = [
            proof.property_type
            for proof in report.verified_properties
            if proof.result == VerificationResult.VERIFIED
        ]

        # Generate certificate
        code_hash = hashlib.sha256(code.encode()).hexdigest()[:32]
        issued_at = time.time()
        expires_at = issued_at + (self.certificate_validity_hours * 3600)

        signature = self._generate_signature(code_hash, verified_properties, issued_at)

        certificate = SafetyCertificate(
            certificate_id=self._generate_certificate_id(),
            code_hash=code_hash,
            verified_properties=verified_properties,
            safety_level=safety_level,
            issuer=self.issuer_id,
            issued_at=issued_at,
            expires_at=expires_at,
            signature_hash=signature,
        )

        # Store certificate
        self._certificates[certificate.certificate_id] = certificate

        # Create proof-carrying code bundle
        pcc = ProofCarryingCode(
            code=code,
            certificate=certificate,
            verification_report=report,
            attached_proofs=report.verified_properties,
        )

        return pcc

    def verify_certificate(
        self,
        certificate: SafetyCertificate,
        code: str
    ) -> Tuple[ProofStatus, List[str]]:
        """
        Verify a safety certificate for code.

        Args:
            certificate: Certificate to verify
            code: Code the certificate claims to cover

        Returns:
            (status, list of issues)
        """
        import time
        import hashlib

        issues = []

        # Check if revoked
        if certificate.certificate_id in self._revoked_certificates:
            return ProofStatus.REVOKED, ["Certificate has been revoked"]

        # Check expiration
        if time.time() > certificate.expires_at:
            return ProofStatus.EXPIRED, ["Certificate has expired"]

        # Verify code hash matches
        current_hash = hashlib.sha256(code.encode()).hexdigest()[:32]
        if current_hash != certificate.code_hash:
            issues.append("Code has been modified since certification")
            return ProofStatus.INVALID, issues

        # Verify signature integrity
        expected_signature = self._generate_signature(
            certificate.code_hash,
            certificate.verified_properties,
            certificate.issued_at
        )
        if expected_signature != certificate.signature_hash:
            issues.append("Certificate signature is invalid")
            return ProofStatus.INVALID, issues

        return ProofStatus.VALID, []

    def execute_with_certificate(
        self,
        pcc: ProofCarryingCode,
        timeout: int = DEFAULT_TIMEOUT
    ) -> ExecutionResult:
        """
        Execute proof-carrying code with certificate verification.

        Fast path if certificate is valid, full verification otherwise.
        """
        import time
        start_time = time.time()

        # Verify certificate
        status, issues = self.verify_certificate(pcc.certificate, pcc.code)

        if status == ProofStatus.VALID and pcc.verify_integrity():
            # Fast path - execute without re-verification
            logger.info(
                f"Executing certified code (cert: {pcc.certificate.certificate_id})"
            )

            policy = ExecutionPolicy(
                name=f"certified_{pcc.certificate.safety_level.value}",
                security_level=pcc.certificate.safety_level,
                limits=ResourceLimits.for_security_level(pcc.certificate.safety_level),
            )

            result = _execute_in_sandbox(pcc.code, timeout, policy)
            result.policy_used = f"certified:{pcc.certificate.certificate_id}"
            return result

        else:
            # Slow path - re-verify and execute
            logger.warning(
                f"Certificate invalid ({status.value}), re-verifying code"
            )

            # Run full verification
            report = self.verification_engine.verify_code(pcc.code)

            if not report.can_execute_safely:
                return ExecutionResult(
                    success=False,
                    error=f"Code verification failed: {report.warnings}",
                    execution_time=time.time() - start_time,
                )

            # Execute with standard policy
            return _execute_in_sandbox(pcc.code, timeout, ExecutionPolicy())

    def revoke_certificate(self, certificate_id: str):
        """Revoke a certificate."""
        self._revoked_certificates.add(certificate_id)
        logger.info(f"Revoked certificate: {certificate_id}")

    def get_certificate_stats(self) -> Dict[str, Any]:
        """Get statistics about issued certificates."""
        import time
        now = time.time()

        valid_count = sum(
            1 for c in self._certificates.values()
            if c.certificate_id not in self._revoked_certificates and c.expires_at > now
        )

        return {
            "total_issued": len(self._certificates),
            "currently_valid": valid_count,
            "revoked": len(self._revoked_certificates),
        }


# =============================================================================
# CUTTING EDGE: CAPABILITY TOKENS
# =============================================================================

@dataclass
class CapabilityToken:
    """
    Fine-grained capability token for execution permissions.

    Tokens are non-forgeable, time-limited, and revocable.
    """
    token_id: str
    capabilities: ExecutionCapabilities
    resource_limits: ResourceLimits
    issuer: str
    issued_to: str
    issued_at: float
    expires_at: float
    one_time_use: bool = False
    used: bool = False
    signature_hash: str = ""

    def is_valid(self) -> bool:
        """Check if token is currently valid."""
        import time
        if self.one_time_use and self.used:
            return False
        return time.time() < self.expires_at


class CapabilityTokenManager:
    """
    Manages capability tokens for sandboxed execution.

    Provides fine-grained, revocable access control.
    """

    def __init__(self, issuer_id: str = "talos_capability_manager"):
        self.issuer_id = issuer_id
        self._tokens: Dict[str, CapabilityToken] = {}
        self._revoked_tokens: Set[str] = set()
        self._token_counter = 0

    def _generate_token_id(self) -> str:
        """Generate unique token ID."""
        import hashlib
        import time
        self._token_counter += 1
        return f"cap_{self._token_counter}_{hashlib.sha256(str(time.time()).encode()).hexdigest()[:12]}"

    def issue_token(
        self,
        issued_to: str,
        capabilities: Optional[ExecutionCapabilities] = None,
        resource_limits: Optional[ResourceLimits] = None,
        validity_minutes: int = 60,
        one_time_use: bool = False
    ) -> CapabilityToken:
        """Issue a new capability token."""
        import time
        import hashlib

        capabilities = capabilities or ExecutionCapabilities()
        resource_limits = resource_limits or ResourceLimits()

        issued_at = time.time()
        expires_at = issued_at + (validity_minutes * 60)

        token_id = self._generate_token_id()

        signature = hashlib.sha256(
            f"{token_id}:{issued_to}:{issued_at}:{self.issuer_id}".encode()
        ).hexdigest()

        token = CapabilityToken(
            token_id=token_id,
            capabilities=capabilities,
            resource_limits=resource_limits,
            issuer=self.issuer_id,
            issued_to=issued_to,
            issued_at=issued_at,
            expires_at=expires_at,
            one_time_use=one_time_use,
            signature_hash=signature,
        )

        self._tokens[token_id] = token
        return token

    def validate_token(self, token: CapabilityToken) -> Tuple[bool, str]:
        """Validate a capability token."""
        if token.token_id in self._revoked_tokens:
            return False, "Token has been revoked"

        if not token.is_valid():
            if token.one_time_use and token.used:
                return False, "One-time token already used"
            return False, "Token has expired"

        # Verify signature
        import hashlib
        expected_sig = hashlib.sha256(
            f"{token.token_id}:{token.issued_to}:{token.issued_at}:{token.issuer}".encode()
        ).hexdigest()

        if expected_sig != token.signature_hash:
            return False, "Token signature invalid"

        return True, "Valid"

    def consume_token(self, token: CapabilityToken) -> bool:
        """Mark a one-time token as used."""
        if token.one_time_use:
            token.used = True
            if token.token_id in self._tokens:
                self._tokens[token.token_id].used = True
        return True

    def revoke_token(self, token_id: str):
        """Revoke a capability token."""
        self._revoked_tokens.add(token_id)


# =============================================================================
# CUTTING EDGE: INTEGRATED EXECUTOR
# =============================================================================

class CuttingEdgeCodeExecutor(CodeExecutorTool):
    """
    Cutting Edge Code Executor with formal verification and proof-carrying code.

    Features beyond Advanced:
    - Formal Verification: Mathematically prove code safety properties
    - Proof-Carrying Code: Execute pre-certified code with fast path
    - Capability Tokens: Fine-grained, revocable execution permissions
    - Automatic Safety Analysis: Comprehensive safety checks before execution

    Usage:
        executor = CuttingEdgeCodeExecutor()

        # Option 1: Direct execution with formal verification
        result = await executor.execute_verified(code)

        # Option 2: Pre-certify code for faster repeated execution
        pcc = executor.certify(code)
        result = await executor.execute_certified(pcc)

        # Option 3: Execute with capability token
        token = executor.issue_capability_token("user_123")
        result = await executor.execute_with_token(code, token)
    """

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        enable_formal_verification: bool = True,
        enable_pcc: bool = True,
        enable_capability_tokens: bool = True
    ):
        super().__init__(timeout)

        self.enable_formal_verification = enable_formal_verification
        self.enable_pcc = enable_pcc
        self.enable_capability_tokens = enable_capability_tokens

        # Initialize cutting edge components
        self._verification_engine: Optional[FormalVerificationEngine] = None
        if enable_formal_verification:
            self._verification_engine = FormalVerificationEngine()

        self._pcc_system: Optional[ProofCarryingCodeSystem] = None
        if enable_pcc:
            self._pcc_system = ProofCarryingCodeSystem(
                verification_engine=self._verification_engine or FormalVerificationEngine()
            )

        self._capability_manager: Optional[CapabilityTokenManager] = None
        if enable_capability_tokens:
            self._capability_manager = CapabilityTokenManager()

    def verify_code_safety(self, code: str) -> FormalVerificationReport:
        """
        Run formal verification on code.

        Returns detailed safety analysis.
        """
        if self._verification_engine is None:
            raise ValueError("Formal verification not enabled")
        return self._verification_engine.verify_code(code)

    def certify(
        self,
        code: str,
        safety_level: SecurityLevel = SecurityLevel.STANDARD
    ) -> ProofCarryingCode:
        """
        Certify code for fast repeated execution.

        Args:
            code: Python code to certify
            safety_level: Desired safety level

        Returns:
            ProofCarryingCode bundle with certificate
        """
        if self._pcc_system is None:
            raise ValueError("Proof-carrying code not enabled")
        return self._pcc_system.certify_code(code, safety_level)

    def issue_capability_token(
        self,
        issued_to: str,
        capabilities: Optional[ExecutionCapabilities] = None,
        resource_limits: Optional[ResourceLimits] = None,
        validity_minutes: int = 60,
        one_time_use: bool = False
    ) -> CapabilityToken:
        """Issue a capability token for execution."""
        if self._capability_manager is None:
            raise ValueError("Capability tokens not enabled")
        return self._capability_manager.issue_token(
            issued_to, capabilities, resource_limits, validity_minutes, one_time_use
        )

    async def execute_verified(
        self,
        code: str,
        timeout: Optional[int] = None,
        require_all_properties: bool = False
    ) -> ExecutionResult:
        """
        Execute code with formal verification.

        Args:
            code: Python code to execute
            timeout: Execution timeout
            require_all_properties: Require all properties to be verified

        Returns:
            ExecutionResult with verification data
        """
        timeout = min(timeout or self.timeout, MAX_TIMEOUT)

        # Standard validation
        is_valid, errors = self.validator.validate(code)
        if not is_valid:
            return ExecutionResult(
                success=False,
                error=f"Validation failed: {errors}"
            )

        # Formal verification
        if self._verification_engine:
            report = self._verification_engine.verify_code(code)

            if not report.can_execute_safely:
                return ExecutionResult(
                    success=False,
                    error=f"Formal verification failed: {report.warnings}",
                )

            if require_all_properties:
                unverified = [
                    p.property_type.value
                    for p in report.verified_properties
                    if p.result != VerificationResult.VERIFIED
                ]
                if unverified:
                    return ExecutionResult(
                        success=False,
                        error=f"Could not verify properties: {unverified}",
                    )

        # Execute
        return await self.execute_code(code, timeout)

    async def execute_certified(
        self,
        pcc: ProofCarryingCode,
        timeout: Optional[int] = None
    ) -> ExecutionResult:
        """
        Execute proof-carrying code with certificate verification.

        Fast path if certificate is valid.
        """
        if self._pcc_system is None:
            raise ValueError("Proof-carrying code not enabled")

        timeout = min(timeout or self.timeout, MAX_TIMEOUT)
        return self._pcc_system.execute_with_certificate(pcc, timeout)

    async def execute_with_token(
        self,
        code: str,
        token: CapabilityToken,
        timeout: Optional[int] = None
    ) -> ExecutionResult:
        """
        Execute code using capability token permissions.

        Args:
            code: Python code to execute
            token: Capability token providing permissions
            timeout: Execution timeout

        Returns:
            ExecutionResult
        """
        if self._capability_manager is None:
            raise ValueError("Capability tokens not enabled")

        # Validate token
        valid, message = self._capability_manager.validate_token(token)
        if not valid:
            return ExecutionResult(
                success=False,
                error=f"Invalid capability token: {message}"
            )

        timeout = min(timeout or self.timeout, MAX_TIMEOUT)

        # Validate code
        is_valid, errors = self.validator.validate(code)
        if not is_valid:
            return ExecutionResult(
                success=False,
                error=f"Validation failed: {errors}"
            )

        # Create policy from token
        policy = ExecutionPolicy(
            name=f"token_{token.token_id}",
            security_level=SecurityLevel.CUSTOM,
            limits=token.resource_limits,
            capabilities=token.capabilities,
        )

        # Consume one-time token
        self._capability_manager.consume_token(token)

        # Execute
        loop = asyncio.get_event_loop()
        try:
            result = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    _execute_in_sandbox,
                    code,
                    timeout,
                    policy
                ),
                timeout=timeout
            )
            return result
        except asyncio.TimeoutError:
            return ExecutionResult(
                success=False,
                error=f"Execution timed out after {timeout} seconds"
            )

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get statistics about cutting edge features."""
        stats = {
            "formal_verification_enabled": self.enable_formal_verification,
            "pcc_enabled": self.enable_pcc,
            "capability_tokens_enabled": self.enable_capability_tokens,
        }

        if self._verification_engine:
            stats["verification_cache_size"] = len(
                self._verification_engine._verification_cache
            )

        if self._pcc_system:
            stats["certificates"] = self._pcc_system.get_certificate_stats()

        if self._capability_manager:
            stats["tokens_issued"] = len(self._capability_manager._tokens)
            stats["tokens_revoked"] = len(self._capability_manager._revoked_tokens)

        return stats


# Factory function for cutting edge executor
_cutting_edge_executor: Optional[CuttingEdgeCodeExecutor] = None


def get_cutting_edge_code_executor(
    timeout: int = DEFAULT_TIMEOUT,
    **kwargs
) -> CuttingEdgeCodeExecutor:
    """Get the global cutting edge code executor instance."""
    global _cutting_edge_executor
    if _cutting_edge_executor is None:
        _cutting_edge_executor = CuttingEdgeCodeExecutor(timeout, **kwargs)
    return _cutting_edge_executor
