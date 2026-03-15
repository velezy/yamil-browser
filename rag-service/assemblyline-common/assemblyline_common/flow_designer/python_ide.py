"""
Python Transform IDE for Logic Weaver Flow Designer.

Provides an in-browser Python editing experience with:
- Intelligent code completion
- Real-time diagnostics
- Secure sandboxed execution
- Context-aware suggestions

This is a LIGHTWEIGHT module - heavy computation is delegated
to external language server and sandbox execution services.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import ast
import re
import hashlib
from datetime import datetime


# =============================================================================
# Code Completion System
# =============================================================================


class CompletionKind(Enum):
    """Types of completion items."""
    TEXT = "text"
    METHOD = "method"
    FUNCTION = "function"
    CONSTRUCTOR = "constructor"
    FIELD = "field"
    VARIABLE = "variable"
    CLASS = "class"
    INTERFACE = "interface"
    MODULE = "module"
    PROPERTY = "property"
    UNIT = "unit"
    VALUE = "value"
    ENUM = "enum"
    KEYWORD = "keyword"
    SNIPPET = "snippet"
    COLOR = "color"
    FILE = "file"
    REFERENCE = "reference"
    FOLDER = "folder"
    ENUM_MEMBER = "enum_member"
    CONSTANT = "constant"
    STRUCT = "struct"
    EVENT = "event"
    OPERATOR = "operator"
    TYPE_PARAMETER = "type_parameter"


@dataclass
class CompletionItem:
    """A code completion suggestion."""
    label: str
    kind: CompletionKind
    detail: Optional[str] = None
    documentation: Optional[str] = None
    insert_text: Optional[str] = None
    sort_text: Optional[str] = None
    filter_text: Optional[str] = None
    preselect: bool = False
    deprecated: bool = False


@dataclass
class CompletionContext:
    """Context for completion request."""
    line: int
    character: int
    trigger_kind: str = "invoked"  # invoked, trigger_character
    trigger_character: Optional[str] = None


class CodeCompletion:
    """
    Provides intelligent code completion for Python transforms.

    Delegating heavy analysis to external language servers,
    this class handles local context and built-in completions.
    """

    # Built-in transform context variables
    CONTEXT_VARIABLES = {
        "message": ("Dict[str, Any]", "The incoming message payload"),
        "headers": ("Dict[str, str]", "Message headers/metadata"),
        "tenant_id": ("str", "Current tenant identifier"),
        "flow_id": ("str", "Current flow identifier"),
        "node_id": ("str", "Current node identifier"),
        "execution_id": ("str", "Current execution run ID"),
        "timestamp": ("datetime", "Message timestamp"),
        "logger": ("Logger", "Structured logger instance"),
    }

    # Built-in helper functions
    HELPER_FUNCTIONS = {
        "get_field": ("(path: str, default: Any = None) -> Any", "Get nested field by dot path"),
        "set_field": ("(path: str, value: Any) -> None", "Set nested field by dot path"),
        "delete_field": ("(path: str) -> bool", "Delete field, returns True if existed"),
        "has_field": ("(path: str) -> bool", "Check if field exists"),
        "map_fields": ("(mapping: Dict[str, str]) -> Dict", "Map source to target fields"),
        "filter_fields": ("(include: List[str] = None, exclude: List[str] = None) -> Dict", "Filter message fields"),
        "merge": ("(source: Dict, target: Dict) -> Dict", "Deep merge dictionaries"),
        "validate": ("(schema: Dict) -> List[str]", "Validate message against JSON schema"),
        "lookup": ("(table: str, key: str) -> Any", "Lookup value from reference table"),
        "emit": ("(message: Dict, target: str = None) -> None", "Emit message to next node or target"),
        "emit_error": ("(error: str, code: str = None) -> None", "Emit error to error handler"),
    }

    # Python keywords for completion
    PYTHON_KEYWORDS = [
        "False", "None", "True", "and", "as", "assert", "async", "await",
        "break", "class", "continue", "def", "del", "elif", "else", "except",
        "finally", "for", "from", "global", "if", "import", "in", "is",
        "lambda", "nonlocal", "not", "or", "pass", "raise", "return", "try",
        "while", "with", "yield"
    ]

    def __init__(
        self,
        custom_functions: Optional[Dict[str, Tuple[str, str]]] = None,
        custom_variables: Optional[Dict[str, Tuple[str, str]]] = None
    ):
        """
        Initialize code completion.

        Args:
            custom_functions: Additional functions (name -> (signature, doc))
            custom_variables: Additional variables (name -> (type, doc))
        """
        self.custom_functions = custom_functions or {}
        self.custom_variables = custom_variables or {}

    def get_completions(
        self,
        code: str,
        context: CompletionContext
    ) -> List[CompletionItem]:
        """
        Get completion suggestions for given code and position.

        Args:
            code: The Python code
            context: Completion context with position

        Returns:
            List of completion items
        """
        lines = code.split('\n')
        if context.line >= len(lines):
            return []

        line = lines[context.line]
        prefix = line[:context.character]

        completions = []

        # Determine completion context
        if '.' in prefix:
            # Object member completion
            completions.extend(self._get_member_completions(prefix, code))
        elif context.trigger_character == '(':
            # Function signature help
            completions.extend(self._get_signature_completions(prefix))
        else:
            # General completions
            completions.extend(self._get_context_completions(prefix))
            completions.extend(self._get_keyword_completions(prefix))
            completions.extend(self._get_local_completions(code, context.line))

        # Filter by prefix
        word_match = re.search(r'(\w+)$', prefix)
        if word_match:
            word = word_match.group(1).lower()
            completions = [
                c for c in completions
                if c.filter_text and c.filter_text.lower().startswith(word)
                or c.label.lower().startswith(word)
            ]

        return completions

    def _get_context_completions(self, prefix: str) -> List[CompletionItem]:
        """Get completions for context variables and helper functions."""
        items = []

        # Context variables
        for name, (type_hint, doc) in self.CONTEXT_VARIABLES.items():
            items.append(CompletionItem(
                label=name,
                kind=CompletionKind.VARIABLE,
                detail=type_hint,
                documentation=doc,
                filter_text=name
            ))

        # Custom variables
        for name, (type_hint, doc) in self.custom_variables.items():
            items.append(CompletionItem(
                label=name,
                kind=CompletionKind.VARIABLE,
                detail=type_hint,
                documentation=doc,
                filter_text=name
            ))

        # Helper functions
        for name, (signature, doc) in self.HELPER_FUNCTIONS.items():
            items.append(CompletionItem(
                label=name,
                kind=CompletionKind.FUNCTION,
                detail=signature,
                documentation=doc,
                insert_text=f"{name}($1)",
                filter_text=name
            ))

        # Custom functions
        for name, (signature, doc) in self.custom_functions.items():
            items.append(CompletionItem(
                label=name,
                kind=CompletionKind.FUNCTION,
                detail=signature,
                documentation=doc,
                insert_text=f"{name}($1)",
                filter_text=name
            ))

        return items

    def _get_keyword_completions(self, prefix: str) -> List[CompletionItem]:
        """Get Python keyword completions."""
        return [
            CompletionItem(
                label=kw,
                kind=CompletionKind.KEYWORD,
                filter_text=kw
            )
            for kw in self.PYTHON_KEYWORDS
        ]

    def _get_member_completions(self, prefix: str, code: str) -> List[CompletionItem]:
        """Get member completions for object access."""
        items = []

        # Extract object name
        match = re.search(r'(\w+)\.$', prefix)
        if not match:
            return items

        obj_name = match.group(1)

        # Check if it's a known context variable
        if obj_name == "message":
            items.extend([
                CompletionItem(
                    label="get",
                    kind=CompletionKind.METHOD,
                    detail="(key, default=None) -> Any",
                    documentation="Get value with optional default",
                    insert_text="get($1)"
                ),
                CompletionItem(
                    label="keys",
                    kind=CompletionKind.METHOD,
                    detail="() -> KeysView",
                    insert_text="keys()"
                ),
                CompletionItem(
                    label="values",
                    kind=CompletionKind.METHOD,
                    detail="() -> ValuesView",
                    insert_text="values()"
                ),
                CompletionItem(
                    label="items",
                    kind=CompletionKind.METHOD,
                    detail="() -> ItemsView",
                    insert_text="items()"
                ),
            ])
        elif obj_name == "logger":
            for level in ["debug", "info", "warning", "error", "critical"]:
                items.append(CompletionItem(
                    label=level,
                    kind=CompletionKind.METHOD,
                    detail="(msg: str, **kwargs) -> None",
                    documentation=f"Log {level} message",
                    insert_text=f"{level}($1)"
                ))

        return items

    def _get_signature_completions(self, prefix: str) -> List[CompletionItem]:
        """Get parameter hints for function calls."""
        # Extract function name
        match = re.search(r'(\w+)\($', prefix)
        if not match:
            return []

        func_name = match.group(1)

        # Check helper functions
        if func_name in self.HELPER_FUNCTIONS:
            sig, doc = self.HELPER_FUNCTIONS[func_name]
            return [CompletionItem(
                label=f"{func_name}{sig}",
                kind=CompletionKind.FUNCTION,
                documentation=doc
            )]

        if func_name in self.custom_functions:
            sig, doc = self.custom_functions[func_name]
            return [CompletionItem(
                label=f"{func_name}{sig}",
                kind=CompletionKind.FUNCTION,
                documentation=doc
            )]

        return []

    def _get_local_completions(
        self,
        code: str,
        current_line: int
    ) -> List[CompletionItem]:
        """Get completions for locally defined names."""
        items = []

        try:
            tree = ast.parse(code)
        except SyntaxError:
            # Try to parse partial code
            lines = code.split('\n')[:current_line]
            try:
                tree = ast.parse('\n'.join(lines))
            except SyntaxError:
                return items

        # Collect all defined names
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                items.append(CompletionItem(
                    label=node.name,
                    kind=CompletionKind.FUNCTION,
                    detail=f"def {node.name}(...)",
                    filter_text=node.name
                ))
            elif isinstance(node, ast.ClassDef):
                items.append(CompletionItem(
                    label=node.name,
                    kind=CompletionKind.CLASS,
                    detail=f"class {node.name}",
                    filter_text=node.name
                ))
            elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                items.append(CompletionItem(
                    label=node.id,
                    kind=CompletionKind.VARIABLE,
                    filter_text=node.id
                ))

        return items


# =============================================================================
# Diagnostics System
# =============================================================================


class DiagnosticSeverity(Enum):
    """Severity levels for diagnostics."""
    ERROR = "error"
    WARNING = "warning"
    INFORMATION = "information"
    HINT = "hint"


@dataclass
class DiagnosticMessage:
    """A diagnostic message (error, warning, etc.)."""
    message: str
    severity: DiagnosticSeverity
    line: int
    column: int
    end_line: Optional[int] = None
    end_column: Optional[int] = None
    code: Optional[str] = None
    source: str = "python-ide"


class DiagnosticsProvider:
    """
    Provides real-time diagnostics for Python code.

    Performs lightweight static analysis locally and
    delegates heavy analysis to external services.
    """

    # Forbidden modules in transform context
    FORBIDDEN_MODULES = {
        "os", "sys", "subprocess", "shutil", "pathlib",
        "socket", "http", "urllib", "requests",
        "pickle", "marshal", "shelve",
        "importlib", "__import__", "exec", "eval", "compile",
        "open", "file", "input",
        "multiprocessing", "threading", "concurrent",
    }

    # Forbidden built-in functions
    FORBIDDEN_BUILTINS = {
        "open", "exec", "eval", "compile", "__import__",
        "input", "breakpoint", "help", "quit", "exit"
    }

    def __init__(
        self,
        allowed_modules: Optional[Set[str]] = None,
        max_code_size: int = 50000
    ):
        """
        Initialize diagnostics provider.

        Args:
            allowed_modules: Additional allowed modules
            max_code_size: Maximum code size in characters
        """
        self.allowed_modules = allowed_modules or set()
        self.max_code_size = max_code_size

    def get_diagnostics(self, code: str) -> List[DiagnosticMessage]:
        """
        Analyze code and return diagnostics.

        Args:
            code: Python code to analyze

        Returns:
            List of diagnostic messages
        """
        diagnostics = []

        # Check code size
        if len(code) > self.max_code_size:
            diagnostics.append(DiagnosticMessage(
                message=f"Code exceeds maximum size ({self.max_code_size} chars)",
                severity=DiagnosticSeverity.ERROR,
                line=0,
                column=0,
                code="E001"
            ))
            return diagnostics

        # Syntax check
        syntax_diags = self._check_syntax(code)
        diagnostics.extend(syntax_diags)

        if syntax_diags:
            # Don't continue if syntax errors
            return diagnostics

        # Security check
        diagnostics.extend(self._check_security(code))

        # Style checks (lighter weight)
        diagnostics.extend(self._check_style(code))

        return diagnostics

    def _check_syntax(self, code: str) -> List[DiagnosticMessage]:
        """Check for syntax errors."""
        try:
            ast.parse(code)
            return []
        except SyntaxError as e:
            return [DiagnosticMessage(
                message=str(e.msg),
                severity=DiagnosticSeverity.ERROR,
                line=e.lineno - 1 if e.lineno else 0,
                column=e.offset - 1 if e.offset else 0,
                code="E100"
            )]

    def _check_security(self, code: str) -> List[DiagnosticMessage]:
        """Check for security issues."""
        diagnostics = []

        try:
            tree = ast.parse(code)
        except SyntaxError:
            return diagnostics

        for node in ast.walk(tree):
            # Check imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module = alias.name.split('.')[0]
                    if module in self.FORBIDDEN_MODULES and module not in self.allowed_modules:
                        diagnostics.append(DiagnosticMessage(
                            message=f"Import of '{alias.name}' is not allowed in transforms",
                            severity=DiagnosticSeverity.ERROR,
                            line=node.lineno - 1,
                            column=node.col_offset,
                            code="S001"
                        ))

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    module = node.module.split('.')[0]
                    if module in self.FORBIDDEN_MODULES and module not in self.allowed_modules:
                        diagnostics.append(DiagnosticMessage(
                            message=f"Import from '{node.module}' is not allowed in transforms",
                            severity=DiagnosticSeverity.ERROR,
                            line=node.lineno - 1,
                            column=node.col_offset,
                            code="S001"
                        ))

            # Check forbidden function calls
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.FORBIDDEN_BUILTINS:
                        diagnostics.append(DiagnosticMessage(
                            message=f"Use of '{node.func.id}' is not allowed in transforms",
                            severity=DiagnosticSeverity.ERROR,
                            line=node.lineno - 1,
                            column=node.col_offset,
                            code="S002"
                        ))

            # Check for attribute access to __builtins__ etc
            elif isinstance(node, ast.Attribute):
                if node.attr.startswith('__') and node.attr.endswith('__'):
                    if node.attr not in ('__init__', '__str__', '__repr__', '__len__', '__iter__'):
                        diagnostics.append(DiagnosticMessage(
                            message=f"Access to dunder attribute '{node.attr}' may be restricted",
                            severity=DiagnosticSeverity.WARNING,
                            line=node.lineno - 1,
                            column=node.col_offset,
                            code="S003"
                        ))

        return diagnostics

    def _check_style(self, code: str) -> List[DiagnosticMessage]:
        """Check for style issues."""
        diagnostics = []
        lines = code.split('\n')

        for i, line in enumerate(lines):
            # Line too long
            if len(line) > 120:
                diagnostics.append(DiagnosticMessage(
                    message=f"Line too long ({len(line)} > 120 characters)",
                    severity=DiagnosticSeverity.HINT,
                    line=i,
                    column=120,
                    code="W001"
                ))

            # Trailing whitespace
            if line.endswith(' ') or line.endswith('\t'):
                diagnostics.append(DiagnosticMessage(
                    message="Trailing whitespace",
                    severity=DiagnosticSeverity.HINT,
                    line=i,
                    column=len(line.rstrip()),
                    code="W002"
                ))

        return diagnostics


# =============================================================================
# Runtime Execution
# =============================================================================


@dataclass
class RuntimeContext:
    """Context for transform execution."""
    message: Dict[str, Any]
    headers: Dict[str, str] = field(default_factory=dict)
    tenant_id: str = ""
    flow_id: str = ""
    node_id: str = ""
    execution_id: str = ""
    timestamp: Optional[datetime] = None
    variables: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RuntimeResult:
    """Result of transform execution."""
    success: bool
    output: Optional[Dict[str, Any]] = None
    errors: List[str] = field(default_factory=list)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    emitted_messages: List[Dict[str, Any]] = field(default_factory=list)
    execution_time_ms: float = 0.0
    memory_used_bytes: int = 0


@dataclass
class SandboxConfig:
    """Configuration for sandbox execution."""
    max_execution_time_ms: int = 5000
    max_memory_bytes: int = 50 * 1024 * 1024  # 50MB
    max_output_size: int = 1024 * 1024  # 1MB
    allowed_modules: Set[str] = field(default_factory=set)
    enable_numpy: bool = False
    enable_pandas: bool = False
    enable_dateutil: bool = True


class PythonSandbox:
    """
    Secure sandbox for executing Python transforms.

    This is a LIGHTWEIGHT orchestrator - actual sandboxed
    execution happens in isolated external services.
    """

    def __init__(
        self,
        config: Optional[SandboxConfig] = None,
        sandbox_service_url: Optional[str] = None
    ):
        """
        Initialize sandbox.

        Args:
            config: Sandbox configuration
            sandbox_service_url: URL of external sandbox service
        """
        self.config = config or SandboxConfig()
        self.sandbox_service_url = sandbox_service_url

    async def execute(
        self,
        code: str,
        context: RuntimeContext
    ) -> RuntimeResult:
        """
        Execute transform code in sandbox.

        Args:
            code: Python code to execute
            context: Execution context

        Returns:
            Execution result
        """
        import time
        start_time = time.time()

        # Validate code first
        diagnostics = DiagnosticsProvider(
            allowed_modules=self.config.allowed_modules
        ).get_diagnostics(code)

        errors = [d.message for d in diagnostics if d.severity == DiagnosticSeverity.ERROR]
        if errors:
            return RuntimeResult(
                success=False,
                errors=errors,
                execution_time_ms=(time.time() - start_time) * 1000
            )

        # Prepare execution environment
        exec_globals = self._build_globals(context)
        exec_locals: Dict[str, Any] = {}

        # Track emitted messages and logs
        emitted: List[Dict[str, Any]] = []
        logs: List[Dict[str, Any]] = []

        def emit(message: Dict[str, Any], target: str = None):
            emitted.append({"message": message, "target": target})

        def emit_error(error: str, code: str = None):
            emitted.append({"error": error, "code": code, "is_error": True})

        class MockLogger:
            def _log(self, level: str, msg: str, **kwargs):
                logs.append({"level": level, "message": msg, "extra": kwargs})

            def debug(self, msg: str, **kwargs): self._log("debug", msg, **kwargs)
            def info(self, msg: str, **kwargs): self._log("info", msg, **kwargs)
            def warning(self, msg: str, **kwargs): self._log("warning", msg, **kwargs)
            def error(self, msg: str, **kwargs): self._log("error", msg, **kwargs)
            def critical(self, msg: str, **kwargs): self._log("critical", msg, **kwargs)

        exec_globals["emit"] = emit
        exec_globals["emit_error"] = emit_error
        exec_globals["logger"] = MockLogger()

        # Execute with timeout (simplified - real impl uses external service)
        try:
            # In production, this would call external sandbox service
            exec(code, exec_globals, exec_locals)

            # Get result
            output = exec_locals.get("result", exec_globals.get("message"))

            return RuntimeResult(
                success=True,
                output=output,
                logs=logs,
                emitted_messages=emitted,
                execution_time_ms=(time.time() - start_time) * 1000
            )
        except Exception as e:
            return RuntimeResult(
                success=False,
                errors=[str(e)],
                logs=logs,
                execution_time_ms=(time.time() - start_time) * 1000
            )

    def _build_globals(self, context: RuntimeContext) -> Dict[str, Any]:
        """Build global namespace for execution."""
        # Safe built-ins
        safe_builtins = {
            'abs': abs, 'all': all, 'any': any, 'bool': bool,
            'dict': dict, 'enumerate': enumerate, 'filter': filter,
            'float': float, 'frozenset': frozenset, 'getattr': getattr,
            'hasattr': hasattr, 'hash': hash, 'int': int, 'isinstance': isinstance,
            'issubclass': issubclass, 'iter': iter, 'len': len, 'list': list,
            'map': map, 'max': max, 'min': min, 'next': next,
            'print': print,  # Captured
            'range': range, 'repr': repr, 'reversed': reversed, 'round': round,
            'set': set, 'slice': slice, 'sorted': sorted, 'str': str,
            'sum': sum, 'tuple': tuple, 'type': type, 'zip': zip,
            'True': True, 'False': False, 'None': None,
        }

        # Helper functions
        def get_field(path: str, default: Any = None) -> Any:
            """Get nested field by dot path."""
            obj = context.message
            for key in path.split('.'):
                if isinstance(obj, dict):
                    obj = obj.get(key)
                else:
                    return default
                if obj is None:
                    return default
            return obj

        def set_field(path: str, value: Any) -> None:
            """Set nested field by dot path."""
            obj = context.message
            keys = path.split('.')
            for key in keys[:-1]:
                if key not in obj:
                    obj[key] = {}
                obj = obj[key]
            obj[keys[-1]] = value

        def has_field(path: str) -> bool:
            """Check if field exists."""
            obj = context.message
            for key in path.split('.'):
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return False
            return True

        def delete_field(path: str) -> bool:
            """Delete field, returns True if existed."""
            obj = context.message
            keys = path.split('.')
            for key in keys[:-1]:
                if isinstance(obj, dict) and key in obj:
                    obj = obj[key]
                else:
                    return False
            if keys[-1] in obj:
                del obj[keys[-1]]
                return True
            return False

        return {
            '__builtins__': safe_builtins,
            # Context
            'message': context.message,
            'headers': context.headers,
            'tenant_id': context.tenant_id,
            'flow_id': context.flow_id,
            'node_id': context.node_id,
            'execution_id': context.execution_id,
            'timestamp': context.timestamp or datetime.utcnow(),
            # Helpers
            'get_field': get_field,
            'set_field': set_field,
            'has_field': has_field,
            'delete_field': delete_field,
            # Standard library subsets
            're': re,
            'datetime': datetime,
            'json': __import__('json'),
            'hashlib': hashlib,
        }


class TransformRuntime:
    """
    High-level runtime for Python transforms.

    Coordinates code completion, diagnostics, and execution.
    """

    def __init__(
        self,
        sandbox_config: Optional[SandboxConfig] = None,
        completion_config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize transform runtime.

        Args:
            sandbox_config: Configuration for sandbox execution
            completion_config: Configuration for code completion
        """
        self.sandbox = PythonSandbox(config=sandbox_config)
        self.completion = CodeCompletion()
        self.diagnostics = DiagnosticsProvider()

    def get_completions(
        self,
        code: str,
        line: int,
        character: int,
        trigger_character: Optional[str] = None
    ) -> List[CompletionItem]:
        """Get code completions at position."""
        context = CompletionContext(
            line=line,
            character=character,
            trigger_kind="trigger_character" if trigger_character else "invoked",
            trigger_character=trigger_character
        )
        return self.completion.get_completions(code, context)

    def get_diagnostics(self, code: str) -> List[DiagnosticMessage]:
        """Get diagnostics for code."""
        return self.diagnostics.get_diagnostics(code)

    async def execute(
        self,
        code: str,
        context: RuntimeContext
    ) -> RuntimeResult:
        """Execute transform code."""
        return await self.sandbox.execute(code, context)

    def validate(self, code: str) -> Tuple[bool, List[str]]:
        """
        Validate code without executing.

        Returns:
            Tuple of (is_valid, error_messages)
        """
        diagnostics = self.get_diagnostics(code)
        errors = [d.message for d in diagnostics if d.severity == DiagnosticSeverity.ERROR]
        return len(errors) == 0, errors


# =============================================================================
# IDE Configuration
# =============================================================================


@dataclass
class IDEConfig:
    """Configuration for the Python Transform IDE."""

    # Editor settings
    theme: str = "vs-dark"
    font_size: int = 14
    tab_size: int = 4
    insert_spaces: bool = True
    word_wrap: bool = True
    show_line_numbers: bool = True
    show_minimap: bool = False

    # Completion settings
    auto_complete: bool = True
    completion_delay_ms: int = 100
    show_documentation: bool = True

    # Diagnostics settings
    real_time_diagnostics: bool = True
    diagnostics_delay_ms: int = 500

    # Execution settings
    auto_save: bool = True
    confirm_execution: bool = True
    show_execution_time: bool = True


class PythonTransformIDE:
    """
    Main IDE controller for Python transforms.

    Coordinates all IDE features and provides unified API
    for frontend integration.
    """

    def __init__(
        self,
        config: Optional[IDEConfig] = None,
        sandbox_config: Optional[SandboxConfig] = None
    ):
        """
        Initialize the IDE.

        Args:
            config: IDE configuration
            sandbox_config: Sandbox execution configuration
        """
        self.config = config or IDEConfig()
        self.runtime = TransformRuntime(sandbox_config=sandbox_config)

        # Code cache for versioning
        self._code_versions: Dict[str, List[str]] = {}

    def get_completions(
        self,
        code: str,
        line: int,
        character: int,
        trigger_character: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get code completions for frontend.

        Returns completions as dictionaries for JSON serialization.
        """
        items = self.runtime.get_completions(code, line, character, trigger_character)
        return [
            {
                "label": item.label,
                "kind": item.kind.value,
                "detail": item.detail,
                "documentation": item.documentation,
                "insertText": item.insert_text,
                "sortText": item.sort_text,
                "filterText": item.filter_text,
                "preselect": item.preselect,
                "deprecated": item.deprecated
            }
            for item in items
        ]

    def get_diagnostics(self, code: str) -> List[Dict[str, Any]]:
        """
        Get diagnostics for frontend.

        Returns diagnostics as dictionaries for JSON serialization.
        """
        messages = self.runtime.get_diagnostics(code)
        return [
            {
                "message": msg.message,
                "severity": msg.severity.value,
                "startLineNumber": msg.line + 1,
                "startColumn": msg.column + 1,
                "endLineNumber": (msg.end_line or msg.line) + 1,
                "endColumn": (msg.end_column or msg.column) + 1,
                "code": msg.code,
                "source": msg.source
            }
            for msg in messages
        ]

    async def execute(
        self,
        code: str,
        message: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        tenant_id: str = "",
        flow_id: str = "",
        node_id: str = ""
    ) -> Dict[str, Any]:
        """
        Execute transform and return result.

        Returns result as dictionary for JSON serialization.
        """
        import uuid

        context = RuntimeContext(
            message=message,
            headers=headers or {},
            tenant_id=tenant_id,
            flow_id=flow_id,
            node_id=node_id,
            execution_id=str(uuid.uuid4())
        )

        result = await self.runtime.execute(code, context)

        return {
            "success": result.success,
            "output": result.output,
            "errors": result.errors,
            "logs": result.logs,
            "emittedMessages": result.emitted_messages,
            "executionTimeMs": result.execution_time_ms,
            "memoryUsedBytes": result.memory_used_bytes
        }

    def validate(self, code: str) -> Dict[str, Any]:
        """
        Validate code without executing.

        Returns validation result for frontend.
        """
        is_valid, errors = self.runtime.validate(code)
        return {
            "valid": is_valid,
            "errors": errors
        }

    def format_code(self, code: str) -> str:
        """
        Format Python code.

        In production, delegates to external formatter service.
        """
        # Simple formatting - production would use black/autopep8
        lines = code.split('\n')
        formatted = []
        indent_level = 0

        for line in lines:
            stripped = line.strip()

            # Decrease indent for closing
            if stripped.startswith(('elif', 'else', 'except', 'finally')):
                indent_level = max(0, indent_level - 1)
            elif stripped in (')', ']', '}'):
                indent_level = max(0, indent_level - 1)

            # Add line with proper indent
            if stripped:
                formatted.append('    ' * indent_level + stripped)
            else:
                formatted.append('')

            # Increase indent after colons
            if stripped.endswith(':'):
                indent_level += 1

        return '\n'.join(formatted)

    def get_code_template(self, template_name: str) -> str:
        """Get a code template for common operations."""
        templates = {
            "basic": '''# Basic transform
# Access the message with: message
# Use helpers: get_field, set_field, has_field, delete_field

# Your transform logic here
result = message
''',
            "filter": '''# Filter transform
# Returns the message if condition is True, otherwise None

condition = get_field("status") == "active"

if condition:
    result = message
else:
    result = None
''',
            "map": '''# Map fields transform
# Rename and restructure fields

result = {
    "id": get_field("source_id"),
    "name": get_field("full_name"),
    "email": get_field("contact.email"),
    "created": timestamp.isoformat()
}
''',
            "enrich": '''# Enrich transform
# Add computed fields to the message

message["processed_at"] = timestamp.isoformat()
message["tenant"] = tenant_id
message["word_count"] = len(get_field("text", "").split())

result = message
''',
            "validate": '''# Validate transform
# Emit errors for invalid messages

errors = []

if not get_field("id"):
    errors.append("Missing required field: id")

if not get_field("email"):
    errors.append("Missing required field: email")
elif "@" not in get_field("email"):
    errors.append("Invalid email format")

if errors:
    emit_error("; ".join(errors), "VALIDATION_ERROR")
    result = None
else:
    result = message
''',
            "split": '''# Split transform
# Emit multiple messages from one

items = get_field("items", [])

for item in items:
    emit({
        "parent_id": get_field("id"),
        "item": item,
        "timestamp": timestamp.isoformat()
    })

result = None  # Don't emit original
''',
        }

        return templates.get(template_name, templates["basic"])


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # IDE
    "PythonTransformIDE",
    "IDEConfig",
    # Completion
    "CodeCompletion",
    "CompletionItem",
    "CompletionKind",
    "CompletionContext",
    # Diagnostics
    "DiagnosticsProvider",
    "DiagnosticMessage",
    "DiagnosticSeverity",
    # Runtime
    "TransformRuntime",
    "RuntimeContext",
    "RuntimeResult",
    # Sandbox
    "PythonSandbox",
    "SandboxConfig",
]
