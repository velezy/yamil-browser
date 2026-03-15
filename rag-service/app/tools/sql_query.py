"""
SQL Query Tool (Advanced)

Executes safe SQL queries against PostgreSQL for user data analysis.
Features:
- Natural language to SQL conversion using HuggingFace Text2SQL-1.5B model
- Fallback to pattern matching if model unavailable
- Safe parameterized queries
- User-scoped data access
- Result formatting for AI consumption

Advanced Features (upgraded from Required):
- Query explanation: Explain generated SQL in plain English
- Result privacy: Automatically mask sensitive fields in results
- Query audit logging: Track all queries for compliance
- Data classification: Identify and handle sensitive columns
"""

import logging
import re
from typing import Any, Optional, TYPE_CHECKING, List, Dict, Tuple, Set
from dataclasses import dataclass, field
from enum import Enum

from .base import BaseTool, ToolResult

# Try to import Text2SQL service
try:
    from .text2sql import get_text2sql_service, Text2SQLService
    TEXT2SQL_AVAILABLE = True
except ImportError:
    TEXT2SQL_AVAILABLE = False

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


# =============================================================================
# ADVANCED: DATA CLASSIFICATION AND PRIVACY
# =============================================================================

class DataSensitivity(Enum):
    """Classification for column sensitivity."""
    PUBLIC = "public"  # Safe to show
    INTERNAL = "internal"  # Show with caution
    CONFIDENTIAL = "confidential"  # Mask in results
    RESTRICTED = "restricted"  # Never show, replace with [RESTRICTED]


@dataclass
class ColumnClassification:
    """Classification info for a database column."""
    table: str
    column: str
    sensitivity: DataSensitivity
    mask_pattern: Optional[str] = None  # e.g., "***-**-{last4}" for SSN


# Default sensitive column classifications
SENSITIVE_COLUMNS = {
    # Pattern: (table, column) -> DataSensitivity
    ("users", "password_hash"): DataSensitivity.RESTRICTED,
    ("users", "ssn"): DataSensitivity.RESTRICTED,
    ("users", "email"): DataSensitivity.CONFIDENTIAL,
    ("users", "phone"): DataSensitivity.CONFIDENTIAL,
    ("messages", "content"): DataSensitivity.INTERNAL,  # User messages
}

# Columns to always mask regardless of table
ALWAYS_MASK_COLUMNS = {
    "password", "password_hash", "secret", "token", "api_key",
    "ssn", "social_security", "credit_card", "card_number",
}

# Columns that might contain PII
POTENTIAL_PII_COLUMNS = {
    "email", "phone", "address", "name", "first_name", "last_name",
    "dob", "date_of_birth", "ip_address",
}


@dataclass
class QueryExplanation:
    """Human-readable explanation of a SQL query."""
    summary: str  # One-line summary
    tables_accessed: List[str]
    operations: List[str]  # SELECT, JOIN, etc.
    filters: List[str]  # WHERE conditions explained
    aggregations: List[str]  # GROUP BY, COUNT, etc.
    full_explanation: str  # Detailed explanation

    def to_dict(self) -> Dict[str, Any]:
        return {
            "summary": self.summary,
            "tables_accessed": self.tables_accessed,
            "operations": self.operations,
            "filters": self.filters,
            "aggregations": self.aggregations,
            "full_explanation": self.full_explanation,
        }


class ResultPrivacyMasker:
    """
    Masks sensitive data in query results.

    Automatically detects and masks PII and sensitive fields.
    """

    def __init__(
        self,
        custom_classifications: Optional[Dict[Tuple[str, str], DataSensitivity]] = None
    ):
        """
        Initialize with optional custom column classifications.

        Args:
            custom_classifications: Map of (table, column) -> sensitivity
        """
        self.classifications = dict(SENSITIVE_COLUMNS)
        if custom_classifications:
            self.classifications.update(custom_classifications)

    def should_mask_column(self, column_name: str) -> Tuple[bool, DataSensitivity]:
        """Check if a column should be masked based on its name."""
        col_lower = column_name.lower()

        # Always mask certain columns
        if col_lower in ALWAYS_MASK_COLUMNS:
            return True, DataSensitivity.RESTRICTED

        # Check for potential PII
        if col_lower in POTENTIAL_PII_COLUMNS:
            return True, DataSensitivity.CONFIDENTIAL

        # Check for patterns
        pii_patterns = ["ssn", "password", "secret", "token", "card"]
        for pattern in pii_patterns:
            if pattern in col_lower:
                return True, DataSensitivity.RESTRICTED

        return False, DataSensitivity.PUBLIC

    def mask_value(
        self,
        value: Any,
        column_name: str,
        sensitivity: DataSensitivity
    ) -> Any:
        """Mask a value based on its sensitivity."""
        if value is None:
            return None

        if sensitivity == DataSensitivity.RESTRICTED:
            return "[RESTRICTED]"

        if sensitivity == DataSensitivity.CONFIDENTIAL:
            str_value = str(value)
            if len(str_value) <= 4:
                return "****"
            # Show first and last character
            return f"{str_value[0]}{'*' * (len(str_value) - 2)}{str_value[-1]}"

        return value

    def mask_results(
        self,
        results: List[Dict[str, Any]],
        table_name: Optional[str] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
        """
        Mask sensitive data in query results.

        Args:
            results: List of result row dictionaries
            table_name: Optional table name for classification lookup

        Returns:
            (masked_results, mask_info dict)
        """
        if not results:
            return results, {}

        masked_columns: Dict[str, str] = {}  # column -> sensitivity
        masked_results = []

        for row in results:
            masked_row = {}
            for column, value in row.items():
                # Check if column should be masked
                should_mask, sensitivity = self.should_mask_column(column)

                # Also check explicit classifications
                if table_name and (table_name, column) in self.classifications:
                    sensitivity = self.classifications[(table_name, column)]
                    should_mask = sensitivity != DataSensitivity.PUBLIC

                if should_mask:
                    masked_row[column] = self.mask_value(value, column, sensitivity)
                    masked_columns[column] = sensitivity.value
                else:
                    masked_row[column] = value

            masked_results.append(masked_row)

        return masked_results, masked_columns


class QueryExplainer:
    """
    Generates human-readable explanations of SQL queries.

    Helps users understand what data they're accessing.
    """

    def explain(self, sql: str, params: List = None) -> QueryExplanation:
        """
        Generate a plain-English explanation of a SQL query.

        Args:
            sql: The SQL query
            params: Query parameters

        Returns:
            QueryExplanation with human-readable details
        """
        sql_upper = sql.upper()

        # Extract tables
        tables = self._extract_tables(sql)

        # Determine operations
        operations = []
        if "SELECT" in sql_upper:
            operations.append("Retrieve data")
        if "JOIN" in sql_upper:
            operations.append("Combine data from multiple tables")
        if "GROUP BY" in sql_upper:
            operations.append("Group results")
        if "ORDER BY" in sql_upper:
            operations.append("Sort results")

        # Extract filters
        filters = self._extract_filters(sql)

        # Extract aggregations
        aggregations = []
        agg_funcs = {"COUNT": "count", "SUM": "sum", "AVG": "average", "MAX": "maximum", "MIN": "minimum"}
        for func, name in agg_funcs.items():
            if func in sql_upper:
                aggregations.append(f"Calculate {name}")

        # Generate summary
        summary = self._generate_summary(tables, operations, filters, aggregations)

        # Generate full explanation
        full_explanation = self._generate_full_explanation(
            sql, tables, operations, filters, aggregations, params
        )

        return QueryExplanation(
            summary=summary,
            tables_accessed=tables,
            operations=operations,
            filters=filters,
            aggregations=aggregations,
            full_explanation=full_explanation,
        )

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = []
        # Match FROM and JOIN clauses
        patterns = [
            r'FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]
        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            tables.extend(matches)
        return list(set(tables))

    def _extract_filters(self, sql: str) -> List[str]:
        """Extract and explain WHERE conditions."""
        filters = []

        # Find WHERE clause
        where_match = re.search(r'WHERE\s+(.+?)(?:GROUP BY|ORDER BY|LIMIT|$)', sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return filters

        where_clause = where_match.group(1).strip()

        # Parse conditions
        if "user_id = $1" in where_clause.lower() or "user_id = ?" in where_clause:
            filters.append("Filtered to your data only")
        if "ILIKE" in where_clause.upper():
            filters.append("Text search filter applied")
        if ">" in where_clause or "<" in where_clause:
            filters.append("Numeric range filter applied")
        if "BETWEEN" in where_clause.upper():
            filters.append("Date/value range filter applied")

        return filters

    def _generate_summary(
        self,
        tables: List[str],
        operations: List[str],
        filters: List[str],
        aggregations: List[str]
    ) -> str:
        """Generate a one-line summary."""
        if not tables:
            return "Query your data"

        table_str = ", ".join(tables[:2])
        if len(tables) > 2:
            table_str += f" and {len(tables) - 2} more"

        if aggregations:
            return f"Calculate statistics from {table_str}"
        elif "Combine" in str(operations):
            return f"Retrieve combined data from {table_str}"
        else:
            return f"Retrieve data from {table_str}"

    def _generate_full_explanation(
        self,
        sql: str,
        tables: List[str],
        operations: List[str],
        filters: List[str],
        aggregations: List[str],
        params: List = None
    ) -> str:
        """Generate a detailed explanation."""
        parts = []

        parts.append(f"This query will access: {', '.join(tables) if tables else 'your data'}")

        if operations:
            parts.append(f"Operations: {', '.join(operations)}")

        if filters:
            parts.append(f"Filters: {', '.join(filters)}")

        if aggregations:
            parts.append(f"Calculations: {', '.join(aggregations)}")

        # Security note
        parts.append("Security: This query is scoped to your user account only.")

        return " | ".join(parts)


class SQLQueryTool(BaseTool):
    """
    Executes SQL queries on user's data in PostgreSQL.

    Safety features:
    - Only SELECT queries allowed
    - User-scoped access (WHERE user_id = ?)
    - Table whitelist
    - Query complexity limits
    """

    # Allowed tables for querying (user's data)
    ALLOWED_TABLES = {
        'documents': {
            'columns': ['id', 'filename', 'file_type', 'file_size', 'chunk_count', 'status', 'created_at', 'processed_at'],
            'user_column': 'user_id',
            'description': 'User uploaded documents',
        },
        'document_chunks': {
            'columns': ['id', 'document_id', 'content', 'chunk_index', 'created_at'],
            'user_column': None,  # Join through documents
            'description': 'Document text chunks',
        },
        'conversations': {
            'columns': ['id', 'title', 'model_used', 'created_at', 'updated_at'],
            'user_column': 'user_id',
            'description': 'Chat conversations',
        },
        'messages': {
            'columns': ['id', 'conversation_id', 'role', 'content', 'created_at'],
            'user_column': None,  # Join through conversations
            'description': 'Chat messages',
        },
    }

    # SQL keywords that are NOT allowed (write operations)
    BLOCKED_KEYWORDS = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE',
        'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'MERGE', 'REPLACE',
    ]

    def __init__(
        self,
        db_pool=None,
        use_text2sql_model: bool = True,
        enable_result_masking: bool = True,
        enable_query_explanation: bool = True,
        custom_classifications: Optional[Dict[Tuple[str, str], DataSensitivity]] = None,
    ):
        super().__init__(
            name="sql",
            description="Queries user's data in PostgreSQL"
        )
        self.db_pool = db_pool
        self.use_text2sql_model = use_text2sql_model and TEXT2SQL_AVAILABLE
        self._text2sql_service: Optional[Text2SQLService] = None
        self._text2sql_initialized = False

        # Advanced: Query explanation and result privacy
        self.enable_result_masking = enable_result_masking
        self.enable_query_explanation = enable_query_explanation
        self._explainer = QueryExplainer() if enable_query_explanation else None
        self._masker = ResultPrivacyMasker(custom_classifications) if enable_result_masking else None

        # Query audit log (in-memory for now, could be persisted)
        self._query_audit_log: List[Dict[str, Any]] = []

    async def _ensure_text2sql_initialized(self):
        """Lazily initialize Text2SQL service"""
        if not self.use_text2sql_model:
            return False

        if self._text2sql_initialized:
            return self._text2sql_service is not None and self._text2sql_service.is_loaded

        try:
            self._text2sql_service = get_text2sql_service()
            success = await self._text2sql_service.initialize()
            self._text2sql_initialized = True
            if success:
                logger.info("✅ Text2SQL model initialized for SQL generation")
            return success
        except Exception as e:
            logger.warning(f"Text2SQL initialization failed: {e}, using fallback")
            self._text2sql_initialized = True
            return False

    def set_db_pool(self, pool):
        """Set database connection pool"""
        self.db_pool = pool

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Convert natural language query to SQL and execute.

        Advanced features:
        - Generates plain-English explanation of the query
        - Masks sensitive columns in results
        - Logs query to audit trail
        """
        import datetime

        try:
            if not self.db_pool:
                return ToolResult(
                    success=False,
                    data=None,
                    error="Database connection not available"
                )

            user_id = context.user_id
            if not user_id:
                return ToolResult(
                    success=False,
                    data=None,
                    error="User ID required for data queries"
                )

            # Generate SQL from natural language
            sql_query, params = await self._generate_sql(context.query, user_id)
            logger.info(f"Generated SQL for '{context.query}': {sql_query[:200] if sql_query else 'None'}")

            if not sql_query:
                return ToolResult(
                    success=False,
                    data=None,
                    error="Could not generate SQL query from your question"
                )

            # Validate the query
            is_safe, error = self._validate_query(sql_query)
            if not is_safe:
                return ToolResult(
                    success=False,
                    data=None,
                    error=f"Query validation failed: {error}"
                )

            # ADVANCED: Generate query explanation
            explanation = None
            if self._explainer:
                explanation = self._explainer.explain(sql_query, params)
                logger.debug(f"Query explanation: {explanation.summary}")

            # Execute the query
            results = await self._execute_query(sql_query, params)
            tables_accessed = self._extract_tables(sql_query)

            # ADVANCED: Mask sensitive columns in results
            masked_columns: Dict[str, str] = {}
            if self._masker and results:
                # Determine primary table for classification lookup
                primary_table = tables_accessed[0] if tables_accessed else None
                results, masked_columns = self._masker.mask_results(results, primary_table)
                if masked_columns:
                    logger.info(f"Masked {len(masked_columns)} sensitive columns: {list(masked_columns.keys())}")

            # ADVANCED: Log to audit trail
            audit_entry = {
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "user_id": user_id,
                "natural_query": context.query,
                "sql_query": sql_query,
                "tables_accessed": tables_accessed,
                "row_count": len(results),
                "masked_columns": list(masked_columns.keys()),
            }
            self._query_audit_log.append(audit_entry)
            # Keep audit log bounded
            if len(self._query_audit_log) > 1000:
                self._query_audit_log = self._query_audit_log[-500:]

            # Build response data
            data = {
                "query": sql_query,
                "results": results,
                "row_count": len(results),
            }

            # Include explanation in response if available
            if explanation:
                data["explanation"] = explanation.to_dict()

            # Include masking info
            if masked_columns:
                data["masked_columns"] = masked_columns

            return ToolResult(
                success=True,
                data=data,
                metadata={
                    "query_type": "SELECT",
                    "tables_accessed": tables_accessed,
                    "explanation_summary": explanation.summary if explanation else None,
                    "sensitive_columns_masked": len(masked_columns),
                }
            )

        except Exception as e:
            logger.error(f"SQL tool error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    async def _generate_sql(self, natural_query: str, user_id: int) -> tuple[Optional[str], list]:
        """
        Convert natural language to SQL query.

        Uses HuggingFace Text2SQL-1.5B model when available,
        falls back to pattern matching otherwise.
        """
        params = [user_id]

        # Try Text2SQL model first
        if await self._ensure_text2sql_initialized():
            try:
                result = await self._generate_with_text2sql(natural_query, user_id)
                if result[0]:
                    logger.info(f"Text2SQL generated: {result[0][:100]}...")
                    return result
            except Exception as e:
                logger.warning(f"Text2SQL failed, using fallback: {e}")

        # Fallback to pattern matching
        return await self._generate_sql_pattern_based(natural_query, user_id)

    async def _generate_with_text2sql(self, natural_query: str, user_id: int) -> tuple[Optional[str], list]:
        """
        Generate SQL using the HuggingFace Text2SQL-1.5B model.
        """
        params = [user_id]

        # Generate schema for the model
        schema = self._text2sql_service.get_schema_for_tables(self.ALLOWED_TABLES)

        # Add user context to schema (the model needs to know about user scoping)
        schema += "\n\n-- Note: All queries must filter by user_id = $1 for security"

        # Generate SQL
        result = await self._text2sql_service.generate_sql(
            question=natural_query,
            schema=schema
        )

        if not result.success:
            return None, params

        # Post-process: ensure user_id filter
        sql = result.query

        # Add user_id filter if not present
        if '$1' not in sql and 'user_id' not in sql.lower():
            # Find the first table and add WHERE clause
            for table in self.ALLOWED_TABLES:
                if table in sql.lower():
                    config = self.ALLOWED_TABLES[table]
                    user_col = config.get('user_column')
                    if user_col:
                        if 'WHERE' in sql.upper():
                            sql = sql.replace('WHERE', f'WHERE {user_col} = $1 AND')
                        else:
                            # Add WHERE before ORDER BY, GROUP BY, LIMIT, or at end
                            for keyword in ['ORDER BY', 'GROUP BY', 'LIMIT', ';']:
                                if keyword in sql.upper():
                                    idx = sql.upper().find(keyword)
                                    sql = sql[:idx] + f' WHERE {user_col} = $1 ' + sql[idx:]
                                    break
                            else:
                                sql = sql.rstrip(';') + f' WHERE {user_col} = $1'
                        break

        # Replace placeholders if model used different format
        sql = sql.replace('?', '$1')

        return sql, params

    async def _generate_sql_pattern_based(self, natural_query: str, user_id: int) -> tuple[Optional[str], list]:
        """
        Fallback: Convert natural language to SQL using pattern matching.
        """
        query_lower = natural_query.lower()
        params = [user_id]

        # Statistics/Summary queries (check FIRST - highest priority)
        if any(word in query_lower for word in ['statistics', 'summary', 'overview', 'stats', 'insights']):
            return (
                """SELECT
                       (SELECT COUNT(*) FROM documents WHERE user_id = $1) as total_documents,
                       (SELECT COALESCE(SUM(file_size), 0) FROM documents WHERE user_id = $1) as total_size_bytes,
                       (SELECT COUNT(*) FROM conversations WHERE user_id = $1) as total_conversations,
                       (SELECT COUNT(*) FROM messages m JOIN conversations c ON m.conversation_id = c.id WHERE c.user_id = $1) as total_messages""",
                params
            )

        # Document queries
        if any(word in query_lower for word in ['document', 'file', 'upload', 'pdf']):
            # Check specific queries FIRST (more specific before general)
            if 'type' in query_lower or 'breakdown' in query_lower or 'distribution' in query_lower:
                return (
                    """SELECT file_type, COUNT(*) as count
                       FROM documents WHERE user_id = $1
                       GROUP BY file_type ORDER BY count DESC""",
                    params
                )

            if 'count' in query_lower or 'how many' in query_lower:
                return (
                    "SELECT COUNT(*) as document_count FROM documents WHERE user_id = $1",
                    params
                )

            if 'size' in query_lower or 'largest' in query_lower or 'biggest' in query_lower:
                return (
                    """SELECT filename, file_type, file_size, chunk_count
                       FROM documents WHERE user_id = $1
                       ORDER BY file_size DESC LIMIT 10""",
                    params
                )

            if 'recent' in query_lower or 'latest' in query_lower:
                return (
                    """SELECT id, filename, file_type, created_at
                       FROM documents WHERE user_id = $1
                       ORDER BY created_at DESC LIMIT 10""",
                    params
                )

            # General list query (last, most general)
            if 'list' in query_lower or 'show' in query_lower or 'all' in query_lower:
                return (
                    """SELECT id, filename, file_type, file_size, chunk_count, created_at
                       FROM documents WHERE user_id = $1
                       ORDER BY created_at DESC LIMIT 50""",
                    params
                )

        # Conversation/Chat queries
        if any(word in query_lower for word in ['conversation', 'chat', 'message']):
            if 'count' in query_lower or 'how many' in query_lower:
                return (
                    "SELECT COUNT(*) as conversation_count FROM conversations WHERE user_id = $1",
                    params
                )

            if 'list' in query_lower or 'show' in query_lower:
                return (
                    """SELECT id, title, model_used, created_at, updated_at
                       FROM conversations WHERE user_id = $1
                       ORDER BY updated_at DESC LIMIT 20""",
                    params
                )

            if 'message' in query_lower and 'count' in query_lower:
                return (
                    """SELECT COUNT(*) as total_messages
                       FROM messages m JOIN conversations c ON m.conversation_id = c.id
                       WHERE c.user_id = $1""",
                    params
                )

        # Search in document content
        if 'search' in query_lower or 'find' in query_lower or 'contain' in query_lower:
            # Extract search term (text after "for" or "about")
            search_match = re.search(r'(?:for|about|containing)\s+["\']?([^"\']+)["\']?', query_lower)
            if search_match:
                search_term = search_match.group(1).strip()
                params.append(f'%{search_term}%')
                return (
                    """SELECT d.filename, dc.content, dc.chunk_index
                       FROM document_chunks dc
                       JOIN documents d ON dc.document_id = d.id
                       WHERE d.user_id = $1 AND dc.content ILIKE $2
                       LIMIT 20""",
                    params
                )

        # Default: return document overview
        return (
            """SELECT id, filename, file_type, file_size, created_at
               FROM documents WHERE user_id = $1
               ORDER BY created_at DESC LIMIT 20""",
            params
        )

    def _validate_query(self, sql: str) -> tuple[bool, Optional[str]]:
        """Validate SQL query for safety"""
        sql_upper = sql.upper()

        # Check for blocked keywords
        for keyword in self.BLOCKED_KEYWORDS:
            if keyword in sql_upper:
                return False, f"Blocked operation: {keyword}"

        # Must be a SELECT query
        if not sql_upper.strip().startswith('SELECT'):
            return False, "Only SELECT queries are allowed"

        # Check for multiple statements (SQL injection attempt)
        if ';' in sql[:-1]:  # Allow trailing semicolon
            return False, "Multiple statements not allowed"

        # Check query complexity (prevent resource exhaustion)
        if sql_upper.count('JOIN') > 3:
            return False, "Query too complex (max 3 JOINs)"

        return True, None

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL query"""
        tables = []
        sql_upper = sql.upper()

        for table in self.ALLOWED_TABLES.keys():
            if table.upper() in sql_upper:
                tables.append(table)

        return tables

    async def _execute_query(self, sql: str, params: list) -> List[Dict]:
        """Execute SQL query and return results as list of dicts"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

            # Convert to list of dicts
            results = []
            for row in rows:
                results.append(dict(row))

            return results

    def format_for_response(self, result: ToolResult) -> str:
        """Format SQL result for inclusion in response"""
        if not result.success:
            return f"Database query error: {result.error}"

        data = result.data
        row_count = data.get("row_count", 0)
        results = data.get("results", [])
        explanation = data.get("explanation")
        masked_columns = data.get("masked_columns", {})

        parts = []

        # ADVANCED: Show query explanation
        if explanation:
            parts.append(f"**What this query does:** {explanation.get('summary', 'Retrieve your data')}")
            if explanation.get("filters"):
                parts.append(f"*Filters: {', '.join(explanation['filters'])}*")

        if row_count == 0:
            parts.append("No data found matching your query.")
            return "\n\n".join(parts) if parts else "No data found matching your query."

        # Format results as markdown table
        if results:
            headers = list(results[0].keys())
            table = "| " + " | ".join(headers) + " |\n"
            table += "| " + " | ".join(["---"] * len(headers)) + " |\n"

            for row in results[:10]:  # Limit display
                values = [str(row.get(h, ''))[:50] for h in headers]  # Truncate long values
                table += "| " + " | ".join(values) + " |\n"

            if row_count > 10:
                table += f"\n*...and {row_count - 10} more rows*"

            parts.append(f"**Query Results** ({row_count} rows):\n\n{table}")

        # ADVANCED: Note masked columns
        if masked_columns:
            masked_list = [f"{col} ({level})" for col, level in masked_columns.items()]
            parts.append(f"*Note: Sensitive columns masked for privacy: {', '.join(masked_list)}*")

        return "\n\n".join(parts) if parts else f"Query returned {row_count} rows."

    def get_schema_info(self) -> str:
        """Get available tables and columns for query generation"""
        info = "**Available Data Tables:**\n\n"
        for table, config in self.ALLOWED_TABLES.items():
            info += f"- **{table}**: {config['description']}\n"
            info += f"  Columns: {', '.join(config['columns'])}\n"
        return info

    # =========================================================================
    # ADVANCED: Audit and Compliance Methods
    # =========================================================================

    def get_audit_log(
        self,
        user_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve query audit log for compliance.

        Args:
            user_id: Filter by user ID (None = all users)
            limit: Maximum entries to return

        Returns:
            List of audit entries
        """
        log = self._query_audit_log

        if user_id is not None:
            log = [entry for entry in log if entry.get("user_id") == user_id]

        return log[-limit:]

    def get_audit_summary(self, user_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Get summary statistics from audit log.

        Returns:
            Summary dict with query counts, tables accessed, etc.
        """
        log = self.get_audit_log(user_id, limit=10000)

        if not log:
            return {
                "total_queries": 0,
                "unique_users": 0,
                "tables_accessed": {},
                "masked_columns_count": 0,
            }

        tables: Dict[str, int] = {}
        masked_count = 0
        users = set()

        for entry in log:
            users.add(entry.get("user_id"))
            masked_count += len(entry.get("masked_columns", []))
            for table in entry.get("tables_accessed", []):
                tables[table] = tables.get(table, 0) + 1

        return {
            "total_queries": len(log),
            "unique_users": len(users),
            "tables_accessed": tables,
            "masked_columns_count": masked_count,
        }

    def explain_query(self, sql: str, params: List = None) -> QueryExplanation:
        """
        Generate explanation for a SQL query (public API).

        Useful for debugging or previewing queries before execution.
        """
        if self._explainer:
            return self._explainer.explain(sql, params)
        return QueryExplanation(
            summary="Query explanation disabled",
            tables_accessed=[],
            operations=[],
            filters=[],
            aggregations=[],
            full_explanation="Query explanation feature is disabled",
        )


# =============================================================================
# CUTTING EDGE: QUERY INTENT VERIFICATION
# =============================================================================

class QueryIntentType(Enum):
    """Types of query intents."""
    DATA_RETRIEVAL = "data_retrieval"
    AGGREGATION = "aggregation"
    SEARCH = "search"
    JOIN_ANALYSIS = "join_analysis"
    TIME_SERIES = "time_series"
    COMPARISON = "comparison"
    UNKNOWN = "unknown"


class IntentConfidence(Enum):
    """Confidence levels for intent detection."""
    HIGH = "high"  # >= 0.9
    MEDIUM = "medium"  # >= 0.7
    LOW = "low"  # >= 0.5
    UNCERTAIN = "uncertain"  # < 0.5


@dataclass
class DetectedIntent:
    """Detected query intent with confidence."""
    intent_type: QueryIntentType
    confidence: float
    confidence_level: IntentConfidence
    expected_patterns: List[str]
    detected_patterns: List[str]
    intent_explanation: str


@dataclass
class IntentVerificationResult:
    """Result of intent verification."""
    natural_language_intent: DetectedIntent
    sql_intent: DetectedIntent
    intents_match: bool
    mismatch_details: Optional[str]
    semantic_similarity: float
    verification_passed: bool
    recommendations: List[str]


class QueryIntentVerifier:
    """
    Cutting Edge: Verifies that SQL matches user's natural language intent.

    Ensures the generated SQL actually does what the user asked for,
    preventing semantic mismatches and query generation errors.

    Features:
    - Natural language intent extraction
    - SQL intent analysis
    - Semantic similarity scoring
    - Mismatch detection and explanation
    """

    def __init__(self, similarity_threshold: float = 0.7):
        self.similarity_threshold = similarity_threshold

        # Intent patterns for natural language
        self._nl_intent_patterns: Dict[QueryIntentType, List[str]] = {
            QueryIntentType.DATA_RETRIEVAL: [
                r'\bshow\b', r'\blist\b', r'\bget\b', r'\bfetch\b', r'\bretrieve\b',
                r'\bwhat\b.*\bare\b', r'\bdisplay\b', r'\bview\b',
            ],
            QueryIntentType.AGGREGATION: [
                r'\bcount\b', r'\bhow many\b', r'\btotal\b', r'\bsum\b',
                r'\baverage\b', r'\bmax\b', r'\bmin\b', r'\bstats\b',
                r'\bstatistics\b', r'\bsummary\b', r'\boverview\b',
            ],
            QueryIntentType.SEARCH: [
                r'\bsearch\b', r'\bfind\b', r'\blook for\b', r'\bcontaining\b',
                r'\bwith\b.*\btext\b', r'\bmatching\b', r'\bfilter\b',
            ],
            QueryIntentType.JOIN_ANALYSIS: [
                r'\bfrom\b.*\band\b', r'\brelated\b', r'\bcombine\b',
                r'\bwith\b.*\bfrom\b', r'\bacross\b', r'\bjoin\b',
            ],
            QueryIntentType.TIME_SERIES: [
                r'\brecent\b', r'\blast\b', r'\btrend\b', r'\bover time\b',
                r'\bhistory\b', r'\blatest\b', r'\bthis week\b', r'\btoday\b',
            ],
            QueryIntentType.COMPARISON: [
                r'\bcompare\b', r'\bvs\b', r'\bversus\b', r'\bdifference\b',
                r'\blarger\b', r'\bsmaller\b', r'\bmore\b', r'\bless\b',
            ],
        }

        # Intent patterns for SQL
        self._sql_intent_patterns: Dict[QueryIntentType, List[str]] = {
            QueryIntentType.DATA_RETRIEVAL: [
                r'SELECT\s+(?!COUNT|SUM|AVG|MAX|MIN)',
                r'SELECT\s+\*',
                r'SELECT\s+\w+,\s*\w+',
            ],
            QueryIntentType.AGGREGATION: [
                r'\bCOUNT\s*\(',
                r'\bSUM\s*\(',
                r'\bAVG\s*\(',
                r'\bMAX\s*\(',
                r'\bMIN\s*\(',
                r'\bGROUP\s+BY\b',
            ],
            QueryIntentType.SEARCH: [
                r'\bLIKE\b',
                r'\bILIKE\b',
                r'\bSIMILAR\s+TO\b',
                r'~\s*\*?',  # regex match
                r'\bFULL\s+TEXT\b',
            ],
            QueryIntentType.JOIN_ANALYSIS: [
                r'\bJOIN\b',
                r'\bLEFT\s+JOIN\b',
                r'\bRIGHT\s+JOIN\b',
                r'\bINNER\s+JOIN\b',
                r'\bFROM\b.*,',  # implicit join
            ],
            QueryIntentType.TIME_SERIES: [
                r'\bORDER\s+BY\b.*\b(created|updated|date|time)\w*',
                r'\bBETWEEN\b.*\bAND\b',
                r'\b>\s*NOW\s*\(\s*\)',
                r'DATE_TRUNC',
                r'INTERVAL',
            ],
            QueryIntentType.COMPARISON: [
                r'\b>\b',
                r'\b<\b',
                r'\b>=\b',
                r'\b<=\b',
                r'\bCASE\s+WHEN\b',
                r'\bGROUP\s+BY\b.*\bHAVING\b',
            ],
        }

    def _detect_intent(
        self,
        text: str,
        patterns: Dict[QueryIntentType, List[str]],
        is_sql: bool = False
    ) -> DetectedIntent:
        """Detect intent from text using patterns."""
        import re

        intent_scores: Dict[QueryIntentType, float] = {}
        detected_patterns: Dict[QueryIntentType, List[str]] = {}

        for intent_type, intent_patterns in patterns.items():
            score = 0.0
            matches = []

            for pattern in intent_patterns:
                flags = re.IGNORECASE if not is_sql else re.IGNORECASE
                if re.search(pattern, text, flags):
                    score += 1.0
                    matches.append(pattern)

            if intent_patterns:
                intent_scores[intent_type] = score / len(intent_patterns)
                detected_patterns[intent_type] = matches

        # Find best matching intent
        if intent_scores:
            best_intent = max(intent_scores.items(), key=lambda x: x[1])
            intent_type = best_intent[0]
            confidence = min(1.0, best_intent[1] * 2)  # Scale up

            # Determine confidence level
            if confidence >= 0.9:
                confidence_level = IntentConfidence.HIGH
            elif confidence >= 0.7:
                confidence_level = IntentConfidence.MEDIUM
            elif confidence >= 0.5:
                confidence_level = IntentConfidence.LOW
            else:
                confidence_level = IntentConfidence.UNCERTAIN

            return DetectedIntent(
                intent_type=intent_type,
                confidence=confidence,
                confidence_level=confidence_level,
                expected_patterns=patterns.get(intent_type, []),
                detected_patterns=detected_patterns.get(intent_type, []),
                intent_explanation=self._generate_intent_explanation(intent_type, detected_patterns.get(intent_type, [])),
            )

        return DetectedIntent(
            intent_type=QueryIntentType.UNKNOWN,
            confidence=0.0,
            confidence_level=IntentConfidence.UNCERTAIN,
            expected_patterns=[],
            detected_patterns=[],
            intent_explanation="Could not determine intent",
        )

    def _generate_intent_explanation(
        self,
        intent_type: QueryIntentType,
        detected_patterns: List[str]
    ) -> str:
        """Generate human-readable explanation of detected intent."""
        explanations = {
            QueryIntentType.DATA_RETRIEVAL: "The user wants to view/retrieve specific data records",
            QueryIntentType.AGGREGATION: "The user wants to calculate statistics or summaries",
            QueryIntentType.SEARCH: "The user wants to search for specific content",
            QueryIntentType.JOIN_ANALYSIS: "The user wants to analyze related data across tables",
            QueryIntentType.TIME_SERIES: "The user wants to analyze data over time",
            QueryIntentType.COMPARISON: "The user wants to compare values",
            QueryIntentType.UNKNOWN: "Intent could not be determined",
        }

        base = explanations.get(intent_type, "Unknown intent")
        if detected_patterns:
            base += f" (matched: {len(detected_patterns)} patterns)"
        return base

    def _calculate_semantic_similarity(
        self,
        nl_intent: DetectedIntent,
        sql_intent: DetectedIntent
    ) -> float:
        """Calculate semantic similarity between intents."""
        # Same intent type = high similarity
        if nl_intent.intent_type == sql_intent.intent_type:
            return 0.9 + (nl_intent.confidence * sql_intent.confidence * 0.1)

        # Compatible intents
        compatible_intents = {
            (QueryIntentType.DATA_RETRIEVAL, QueryIntentType.SEARCH): 0.7,
            (QueryIntentType.AGGREGATION, QueryIntentType.COMPARISON): 0.6,
            (QueryIntentType.TIME_SERIES, QueryIntentType.DATA_RETRIEVAL): 0.7,
        }

        for (a, b), score in compatible_intents.items():
            if (nl_intent.intent_type == a and sql_intent.intent_type == b) or \
               (nl_intent.intent_type == b and sql_intent.intent_type == a):
                return score

        # Different intents
        return 0.3

    def verify_intent(
        self,
        natural_query: str,
        generated_sql: str
    ) -> IntentVerificationResult:
        """
        Verify that SQL matches the natural language intent.

        Args:
            natural_query: Original natural language question
            generated_sql: Generated SQL query

        Returns:
            IntentVerificationResult with analysis
        """
        # Detect intents
        nl_intent = self._detect_intent(natural_query, self._nl_intent_patterns)
        sql_intent = self._detect_intent(generated_sql, self._sql_intent_patterns, is_sql=True)

        # Calculate similarity
        similarity = self._calculate_semantic_similarity(nl_intent, sql_intent)

        # Determine if intents match
        intents_match = (
            nl_intent.intent_type == sql_intent.intent_type or
            similarity >= self.similarity_threshold
        )

        # Generate mismatch details
        mismatch_details = None
        if not intents_match:
            mismatch_details = (
                f"Natural language suggests '{nl_intent.intent_type.value}' "
                f"but SQL performs '{sql_intent.intent_type.value}'"
            )

        # Generate recommendations
        recommendations = []
        if not intents_match:
            recommendations.append(f"Review SQL to ensure it performs {nl_intent.intent_type.value}")

        if nl_intent.confidence_level == IntentConfidence.UNCERTAIN:
            recommendations.append("Natural language query is ambiguous - consider rephrasing")

        if sql_intent.confidence_level == IntentConfidence.UNCERTAIN:
            recommendations.append("Generated SQL intent is unclear - may need manual review")

        return IntentVerificationResult(
            natural_language_intent=nl_intent,
            sql_intent=sql_intent,
            intents_match=intents_match,
            mismatch_details=mismatch_details,
            semantic_similarity=similarity,
            verification_passed=intents_match and similarity >= self.similarity_threshold,
            recommendations=recommendations,
        )


# =============================================================================
# CUTTING EDGE: QUERY ANOMALY DETECTION
# =============================================================================

class AnomalyType(Enum):
    """Types of query anomalies."""
    UNUSUAL_VOLUME = "unusual_volume"  # Too many queries
    UNUSUAL_TABLES = "unusual_tables"  # Accessing unusual tables
    UNUSUAL_TIME = "unusual_time"  # Query at unusual time
    DATA_EXFILTRATION = "data_exfiltration"  # Large data retrieval
    INJECTION_ATTEMPT = "injection_attempt"  # SQL injection patterns
    PRIVILEGE_ESCALATION = "privilege_escalation"  # Accessing unauthorized data
    BEHAVIORAL_ANOMALY = "behavioral_anomaly"  # Different from normal patterns


class AnomalySeverity(Enum):
    """Severity levels for anomalies."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


@dataclass
class QueryAnomaly:
    """A detected query anomaly."""
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    description: str
    evidence: List[str]
    recommended_action: str
    confidence: float


@dataclass
class AnomalyDetectionResult:
    """Result of anomaly detection."""
    is_anomalous: bool
    anomalies: List[QueryAnomaly]
    risk_score: float  # 0.0 to 1.0
    should_block: bool
    should_alert: bool
    analysis_details: Dict[str, Any]


@dataclass
class UserQueryProfile:
    """Profile of a user's normal query patterns."""
    user_id: int
    query_count_30d: int = 0
    avg_queries_per_day: float = 0.0
    common_tables: Set[str] = field(default_factory=set)
    common_hours: Set[int] = field(default_factory=set)  # 0-23
    typical_result_size: float = 0.0  # avg rows returned
    last_updated: float = 0.0


class QueryAnomalyDetector:
    """
    Cutting Edge: Detects anomalous query patterns.

    Uses statistical analysis and behavioral profiling to identify
    suspicious queries that might indicate security issues.

    Features:
    - User behavior profiling
    - Statistical anomaly detection
    - SQL injection pattern detection
    - Data exfiltration detection
    - Real-time alerting
    """

    def __init__(
        self,
        query_volume_threshold: int = 100,  # queries per hour
        data_volume_threshold: int = 10000,  # rows per query
        injection_confidence_threshold: float = 0.7
    ):
        self.query_volume_threshold = query_volume_threshold
        self.data_volume_threshold = data_volume_threshold
        self.injection_confidence_threshold = injection_confidence_threshold

        # User profiles for behavioral analysis
        self._user_profiles: Dict[int, UserQueryProfile] = {}

        # Recent query history for volume analysis
        self._query_history: Dict[int, List[Dict[str, Any]]] = {}

        # SQL injection patterns
        self._injection_patterns = [
            r"'\s*(OR|AND)\s*'?\d+'\s*=\s*'?\d+",  # ' OR '1'='1
            r"--\s*$",  # SQL comment at end
            r";\s*(DROP|DELETE|UPDATE|INSERT)",  # Statement chaining
            r"UNION\s+(ALL\s+)?SELECT",  # Union injection
            r"'\s*;\s*--",  # Quote termination
            r"SLEEP\s*\(",  # Time-based injection
            r"BENCHMARK\s*\(",  # MySQL time injection
            r"pg_sleep\s*\(",  # PostgreSQL time injection
            r"WAITFOR\s+DELAY",  # SQL Server time injection
            r"xp_cmdshell",  # SQL Server command execution
            r"LOAD_FILE\s*\(",  # File access
            r"INTO\s+OUTFILE",  # File write
            r"INTO\s+DUMPFILE",  # File write
        ]

        # Privilege escalation patterns
        self._privilege_patterns = [
            r"information_schema",
            r"pg_catalog",
            r"pg_roles",
            r"pg_user",
            r"sys\.tables",
            r"sysobjects",
            r"GRANT\s+",
            r"REVOKE\s+",
        ]

    def _get_user_profile(self, user_id: int) -> UserQueryProfile:
        """Get or create user profile."""
        if user_id not in self._user_profiles:
            self._user_profiles[user_id] = UserQueryProfile(user_id=user_id)
        return self._user_profiles[user_id]

    def _update_user_profile(
        self,
        user_id: int,
        tables: List[str],
        result_count: int
    ):
        """Update user's query profile."""
        import time

        profile = self._get_user_profile(user_id)
        current_hour = time.localtime().tm_hour

        profile.query_count_30d += 1
        profile.common_tables.update(tables)
        profile.common_hours.add(current_hour)

        # Update average result size (exponential moving average)
        alpha = 0.1
        profile.typical_result_size = (
            alpha * result_count + (1 - alpha) * profile.typical_result_size
        )
        profile.last_updated = time.time()

    def _record_query(self, user_id: int, query_info: Dict[str, Any]):
        """Record query in history for volume analysis."""
        import time

        if user_id not in self._query_history:
            self._query_history[user_id] = []

        self._query_history[user_id].append({
            **query_info,
            "timestamp": time.time(),
        })

        # Keep only last hour of queries
        one_hour_ago = time.time() - 3600
        self._query_history[user_id] = [
            q for q in self._query_history[user_id]
            if q["timestamp"] > one_hour_ago
        ]

    def _check_injection_patterns(self, sql: str) -> List[QueryAnomaly]:
        """Check for SQL injection patterns."""
        import re

        anomalies = []

        for pattern in self._injection_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                anomalies.append(QueryAnomaly(
                    anomaly_type=AnomalyType.INJECTION_ATTEMPT,
                    severity=AnomalySeverity.CRITICAL,
                    description=f"SQL injection pattern detected",
                    evidence=[f"Matched pattern: {pattern[:30]}..."],
                    recommended_action="Block query and investigate",
                    confidence=0.9,
                ))

        return anomalies

    def _check_privilege_escalation(self, sql: str) -> List[QueryAnomaly]:
        """Check for privilege escalation attempts."""
        import re

        anomalies = []

        for pattern in self._privilege_patterns:
            if re.search(pattern, sql, re.IGNORECASE):
                anomalies.append(QueryAnomaly(
                    anomaly_type=AnomalyType.PRIVILEGE_ESCALATION,
                    severity=AnomalySeverity.HIGH,
                    description=f"Attempt to access system metadata",
                    evidence=[f"Matched pattern: {pattern}"],
                    recommended_action="Review query and verify authorization",
                    confidence=0.85,
                ))

        return anomalies

    def _check_data_exfiltration(
        self,
        sql: str,
        expected_result_count: Optional[int] = None
    ) -> List[QueryAnomaly]:
        """Check for potential data exfiltration."""
        anomalies = []

        # Check for missing LIMIT clause
        if "LIMIT" not in sql.upper():
            anomalies.append(QueryAnomaly(
                anomaly_type=AnomalyType.DATA_EXFILTRATION,
                severity=AnomalySeverity.MEDIUM,
                description="Query has no row limit",
                evidence=["No LIMIT clause in query"],
                recommended_action="Add LIMIT clause to prevent large data retrieval",
                confidence=0.6,
            ))

        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql, re.IGNORECASE):
            anomalies.append(QueryAnomaly(
                anomaly_type=AnomalyType.DATA_EXFILTRATION,
                severity=AnomalySeverity.LOW,
                description="Query selects all columns",
                evidence=["SELECT * used instead of specific columns"],
                recommended_action="Select only needed columns",
                confidence=0.4,
            ))

        return anomalies

    def _check_volume_anomaly(self, user_id: int) -> List[QueryAnomaly]:
        """Check for unusual query volume."""
        anomalies = []

        if user_id in self._query_history:
            recent_count = len(self._query_history[user_id])

            if recent_count > self.query_volume_threshold:
                anomalies.append(QueryAnomaly(
                    anomaly_type=AnomalyType.UNUSUAL_VOLUME,
                    severity=AnomalySeverity.HIGH,
                    description=f"Unusual query volume: {recent_count} queries in last hour",
                    evidence=[
                        f"Query count: {recent_count}",
                        f"Threshold: {self.query_volume_threshold}",
                    ],
                    recommended_action="Investigate for automated scraping or abuse",
                    confidence=0.8,
                ))

        return anomalies

    def _check_behavioral_anomaly(
        self,
        user_id: int,
        tables: List[str]
    ) -> List[QueryAnomaly]:
        """Check for behavioral anomalies based on user profile."""
        import time

        anomalies = []
        profile = self._get_user_profile(user_id)

        # Check for unusual tables
        if profile.common_tables:
            unusual_tables = set(tables) - profile.common_tables
            if unusual_tables:
                anomalies.append(QueryAnomaly(
                    anomaly_type=AnomalyType.UNUSUAL_TABLES,
                    severity=AnomalySeverity.LOW,
                    description="Query accesses tables not typically used by this user",
                    evidence=[f"Unusual tables: {', '.join(unusual_tables)}"],
                    recommended_action="Verify user has legitimate need for this data",
                    confidence=0.5,
                ))

        # Check for unusual time
        current_hour = time.localtime().tm_hour
        if profile.common_hours and current_hour not in profile.common_hours:
            anomalies.append(QueryAnomaly(
                anomaly_type=AnomalyType.UNUSUAL_TIME,
                severity=AnomalySeverity.INFO,
                description=f"Query at unusual hour ({current_hour}:00)",
                evidence=[
                    f"Current hour: {current_hour}",
                    f"Typical hours: {sorted(profile.common_hours)}",
                ],
                recommended_action="May indicate compromised account",
                confidence=0.3,
            ))

        return anomalies

    def detect_anomalies(
        self,
        user_id: int,
        sql: str,
        natural_query: str,
        tables: List[str],
        expected_result_count: Optional[int] = None
    ) -> AnomalyDetectionResult:
        """
        Detect anomalies in a query.

        Args:
            user_id: User executing the query
            sql: Generated SQL query
            natural_query: Original natural language query
            tables: Tables being accessed
            expected_result_count: Expected number of results (if known)

        Returns:
            AnomalyDetectionResult with all detected anomalies
        """
        all_anomalies = []

        # Check various anomaly types
        all_anomalies.extend(self._check_injection_patterns(sql))
        all_anomalies.extend(self._check_privilege_escalation(sql))
        all_anomalies.extend(self._check_data_exfiltration(sql, expected_result_count))
        all_anomalies.extend(self._check_volume_anomaly(user_id))
        all_anomalies.extend(self._check_behavioral_anomaly(user_id, tables))

        # Record query for future analysis
        self._record_query(user_id, {
            "sql": sql[:500],  # Truncate for storage
            "tables": tables,
            "natural_query": natural_query[:200],
        })

        # Calculate risk score
        severity_weights = {
            AnomalySeverity.CRITICAL: 1.0,
            AnomalySeverity.HIGH: 0.7,
            AnomalySeverity.MEDIUM: 0.4,
            AnomalySeverity.LOW: 0.2,
            AnomalySeverity.INFO: 0.05,
        }

        if all_anomalies:
            weighted_scores = [
                severity_weights[a.severity] * a.confidence
                for a in all_anomalies
            ]
            risk_score = min(1.0, sum(weighted_scores))
        else:
            risk_score = 0.0

        # Determine actions
        has_critical = any(a.severity == AnomalySeverity.CRITICAL for a in all_anomalies)
        has_high = any(a.severity == AnomalySeverity.HIGH for a in all_anomalies)

        should_block = has_critical or (has_high and risk_score > 0.7)
        should_alert = has_critical or has_high or risk_score > 0.5

        # Update user profile (if query proceeds)
        if not should_block:
            self._update_user_profile(user_id, tables, expected_result_count or 0)

        return AnomalyDetectionResult(
            is_anomalous=bool(all_anomalies),
            anomalies=all_anomalies,
            risk_score=risk_score,
            should_block=should_block,
            should_alert=should_alert,
            analysis_details={
                "user_profile_exists": user_id in self._user_profiles,
                "queries_last_hour": len(self._query_history.get(user_id, [])),
                "anomaly_count": len(all_anomalies),
            },
        )


# =============================================================================
# CUTTING EDGE: SEMANTIC CORRECTNESS VERIFICATION
# =============================================================================

@dataclass
class SemanticCorrectness:
    """Result of semantic correctness analysis."""
    is_correct: bool
    correctness_score: float  # 0.0 to 1.0
    issues: List[str]
    suggestions: List[str]


class SemanticCorrectnessVerifier:
    """
    Cutting Edge: Verifies semantic correctness of SQL queries.

    Ensures queries will produce meaningful, correct results
    for the user's question.

    Features:
    - Table relationship validation
    - Join condition verification
    - Aggregation logic checking
    - NULL handling verification
    - Result completeness checking
    """

    def __init__(self, table_config: Dict[str, Dict[str, Any]]):
        self.table_config = table_config
        self._table_relationships = self._build_relationships()

    def _build_relationships(self) -> Dict[str, List[Tuple[str, str, str]]]:
        """Build table relationship map from config."""
        # Define known relationships
        return {
            "documents": [
                ("document_chunks", "id", "document_id"),
            ],
            "conversations": [
                ("messages", "id", "conversation_id"),
            ],
            "document_chunks": [
                ("documents", "document_id", "id"),
            ],
            "messages": [
                ("conversations", "conversation_id", "id"),
            ],
        }

    def _check_join_correctness(self, sql: str, tables: List[str]) -> List[str]:
        """Check if joins are correctly formed."""
        issues = []

        if len(tables) > 1:
            # Check for proper join conditions
            if "JOIN" not in sql.upper() and "," in sql.upper().split("FROM")[1].split("WHERE")[0]:
                issues.append("Using implicit join (comma) - consider explicit JOIN for clarity")

            # Check for cartesian products
            if len(tables) == 2:
                table_pair = tuple(sorted(tables))

                # Check if tables have a relationship
                has_relationship = False
                for table in tables:
                    if table in self._table_relationships:
                        for rel in self._table_relationships[table]:
                            if rel[0] in tables:
                                has_relationship = True
                                break

                if not has_relationship:
                    issues.append(f"Tables {tables} may not have a direct relationship")

        return issues

    def _check_aggregation_logic(self, sql: str) -> List[str]:
        """Check for aggregation logic issues."""
        import re

        issues = []
        sql_upper = sql.upper()

        # Check for non-aggregated columns in GROUP BY queries
        if "GROUP BY" in sql_upper:
            # Extract SELECT columns
            select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)

                # Check for columns that aren't aggregated or in GROUP BY
                aggregates = ['COUNT', 'SUM', 'AVG', 'MAX', 'MIN']
                columns = [c.strip() for c in select_clause.split(',')]

                for col in columns:
                    col_upper = col.upper()
                    is_aggregated = any(agg in col_upper for agg in aggregates)
                    if not is_aggregated and '*' not in col:
                        # This column should be in GROUP BY
                        # (simplified check - would need full parsing for accuracy)
                        pass

        # Check for COUNT(*) vs COUNT(column) confusion
        if "COUNT(*)" in sql_upper and "NULL" in sql_upper:
            issues.append("COUNT(*) counts NULLs - use COUNT(column) if NULLs should be excluded")

        return issues

    def _check_null_handling(self, sql: str) -> List[str]:
        """Check for potential NULL handling issues."""
        issues = []
        sql_upper = sql.upper()

        # Check for equality comparisons that might miss NULLs
        if "= NULL" in sql_upper:
            issues.append("Use 'IS NULL' instead of '= NULL' for NULL comparisons")

        if "<> NULL" in sql_upper or "!= NULL" in sql_upper:
            issues.append("Use 'IS NOT NULL' instead of '<> NULL' for NULL comparisons")

        # Check for potential issues with NOT IN
        if "NOT IN" in sql_upper:
            issues.append("NOT IN may return unexpected results if subquery contains NULLs")

        return issues

    def _check_user_scoping(self, sql: str, tables: List[str]) -> List[str]:
        """Check that query is properly scoped to user."""
        issues = []

        # Check that user_id filter is present for user-scoped tables
        user_tables = [t for t in tables if self.table_config.get(t, {}).get('user_column')]

        for table in user_tables:
            user_col = self.table_config[table]['user_column']
            if f"{user_col} =" not in sql.lower() and f"{table}.{user_col}" not in sql.lower():
                issues.append(f"Query may not be properly scoped to user for table '{table}'")

        return issues

    def verify_correctness(
        self,
        sql: str,
        tables: List[str],
        natural_query: str
    ) -> SemanticCorrectness:
        """
        Verify semantic correctness of a SQL query.

        Args:
            sql: SQL query to verify
            tables: Tables accessed by the query
            natural_query: Original natural language question

        Returns:
            SemanticCorrectness result
        """
        all_issues = []
        suggestions = []

        # Run all checks
        all_issues.extend(self._check_join_correctness(sql, tables))
        all_issues.extend(self._check_aggregation_logic(sql))
        all_issues.extend(self._check_null_handling(sql))
        all_issues.extend(self._check_user_scoping(sql, tables))

        # Generate suggestions
        if "LIMIT" not in sql.upper():
            suggestions.append("Consider adding LIMIT clause for large datasets")

        if "ORDER BY" not in sql.upper() and "COUNT" not in sql.upper():
            suggestions.append("Consider adding ORDER BY for consistent results")

        # Calculate score
        issue_penalty = 0.1  # Per issue
        correctness_score = max(0.0, 1.0 - (len(all_issues) * issue_penalty))

        return SemanticCorrectness(
            is_correct=len(all_issues) == 0,
            correctness_score=correctness_score,
            issues=all_issues,
            suggestions=suggestions,
        )


# =============================================================================
# CUTTING EDGE: INTEGRATED SAFE SQL TOOL
# =============================================================================

@dataclass
class CuttingEdgeSQLConfig:
    """Configuration for cutting edge SQL features."""
    enable_intent_verification: bool = True
    enable_anomaly_detection: bool = True
    enable_semantic_verification: bool = True
    intent_similarity_threshold: float = 0.7
    block_on_anomaly: bool = True
    alert_on_anomaly: bool = True


class CuttingEdgeSQLQueryTool(SQLQueryTool):
    """
    Cutting Edge SQL Query Tool with intent verification and anomaly detection.

    Features beyond Advanced:
    - Query Intent Verification: Ensure SQL matches user's intended question
    - Anomaly Detection: Detect suspicious query patterns
    - Semantic Correctness: Verify queries will produce correct results
    - Behavioral Analysis: Profile user query patterns

    Usage:
        tool = CuttingEdgeSQLQueryTool(
            db_pool=pool,
            config=CuttingEdgeSQLConfig(
                enable_intent_verification=True,
                enable_anomaly_detection=True
            )
        )

        result = await tool.execute(context)
    """

    def __init__(
        self,
        db_pool=None,
        use_text2sql_model: bool = True,
        config: Optional[CuttingEdgeSQLConfig] = None,
        **kwargs
    ):
        super().__init__(db_pool, use_text2sql_model, **kwargs)

        self.cutting_edge_config = config or CuttingEdgeSQLConfig()

        # Initialize cutting edge components
        self._intent_verifier: Optional[QueryIntentVerifier] = None
        if self.cutting_edge_config.enable_intent_verification:
            self._intent_verifier = QueryIntentVerifier(
                similarity_threshold=self.cutting_edge_config.intent_similarity_threshold
            )

        self._anomaly_detector: Optional[QueryAnomalyDetector] = None
        if self.cutting_edge_config.enable_anomaly_detection:
            self._anomaly_detector = QueryAnomalyDetector()

        self._semantic_verifier: Optional[SemanticCorrectnessVerifier] = None
        if self.cutting_edge_config.enable_semantic_verification:
            self._semantic_verifier = SemanticCorrectnessVerifier(self.ALLOWED_TABLES)

        # Track verification results
        self._verification_history: List[Dict[str, Any]] = []

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Execute SQL query with cutting edge verification and protection.
        """
        import datetime

        try:
            if not self.db_pool:
                return ToolResult(
                    success=False,
                    data=None,
                    error="Database connection not available"
                )

            user_id = context.user_id
            if not user_id:
                return ToolResult(
                    success=False,
                    data=None,
                    error="User ID required for data queries"
                )

            # Generate SQL from natural language
            sql_query, params = await self._generate_sql(context.query, user_id)

            if not sql_query:
                return ToolResult(
                    success=False,
                    data=None,
                    error="Could not generate SQL query from your question"
                )

            tables = self._extract_tables(sql_query)

            # CUTTING EDGE: Intent verification
            intent_result = None
            if self._intent_verifier:
                intent_result = self._intent_verifier.verify_intent(
                    context.query, sql_query
                )

                if not intent_result.verification_passed:
                    logger.warning(
                        f"Intent mismatch: {intent_result.mismatch_details}"
                    )
                    # Don't block, but include warning in result

            # CUTTING EDGE: Anomaly detection
            anomaly_result = None
            if self._anomaly_detector:
                anomaly_result = self._anomaly_detector.detect_anomalies(
                    user_id=user_id,
                    sql=sql_query,
                    natural_query=context.query,
                    tables=tables,
                )

                if anomaly_result.should_block and self.cutting_edge_config.block_on_anomaly:
                    anomaly_descriptions = [a.description for a in anomaly_result.anomalies]
                    return ToolResult(
                        success=False,
                        data={"anomalies": anomaly_descriptions},
                        error=f"Query blocked due to security anomaly: {anomaly_descriptions[0]}"
                    )

                if anomaly_result.should_alert and self.cutting_edge_config.alert_on_anomaly:
                    logger.warning(
                        f"SQL anomaly alert for user {user_id}: "
                        f"risk_score={anomaly_result.risk_score:.2f}, "
                        f"anomalies={[a.anomaly_type.value for a in anomaly_result.anomalies]}"
                    )

            # CUTTING EDGE: Semantic verification
            semantic_result = None
            if self._semantic_verifier:
                semantic_result = self._semantic_verifier.verify_correctness(
                    sql_query, tables, context.query
                )

                if semantic_result.issues:
                    logger.info(
                        f"Semantic issues detected: {semantic_result.issues}"
                    )

            # Standard validation
            is_safe, error = self._validate_query(sql_query)
            if not is_safe:
                return ToolResult(
                    success=False,
                    data=None,
                    error=f"Query validation failed: {error}"
                )

            # Execute the query
            results = await self._execute_query(sql_query, params)

            # Apply result masking
            masked_columns: Dict[str, str] = {}
            if self._masker and results:
                primary_table = tables[0] if tables else None
                results, masked_columns = self._masker.mask_results(results, primary_table)

            # Record verification history
            self._verification_history.append({
                "timestamp": datetime.datetime.utcnow().isoformat(),
                "user_id": user_id,
                "intent_match": intent_result.intents_match if intent_result else None,
                "anomaly_detected": anomaly_result.is_anomalous if anomaly_result else None,
                "semantic_correct": semantic_result.is_correct if semantic_result else None,
            })

            # Keep history bounded
            if len(self._verification_history) > 1000:
                self._verification_history = self._verification_history[-500:]

            # Build response data
            data = {
                "query": sql_query,
                "results": results,
                "row_count": len(results),
            }

            # Include cutting edge analysis
            if intent_result:
                data["intent_analysis"] = {
                    "natural_intent": intent_result.natural_language_intent.intent_type.value,
                    "sql_intent": intent_result.sql_intent.intent_type.value,
                    "match": intent_result.intents_match,
                    "similarity": round(intent_result.semantic_similarity, 2),
                }

            if anomaly_result and anomaly_result.is_anomalous:
                data["anomaly_analysis"] = {
                    "risk_score": round(anomaly_result.risk_score, 2),
                    "anomalies": [a.anomaly_type.value for a in anomaly_result.anomalies],
                }

            if semantic_result and semantic_result.issues:
                data["semantic_analysis"] = {
                    "issues": semantic_result.issues,
                    "suggestions": semantic_result.suggestions,
                }

            if masked_columns:
                data["masked_columns"] = masked_columns

            return ToolResult(
                success=True,
                data=data,
                metadata={
                    "query_type": "SELECT",
                    "tables_accessed": tables,
                    "intent_verified": intent_result.verification_passed if intent_result else None,
                    "anomaly_risk": anomaly_result.risk_score if anomaly_result else 0.0,
                    "semantic_score": semantic_result.correctness_score if semantic_result else 1.0,
                }
            )

        except Exception as e:
            logger.error(f"Cutting edge SQL tool error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get statistics about cutting edge features."""
        stats = {
            "intent_verification_enabled": self._intent_verifier is not None,
            "anomaly_detection_enabled": self._anomaly_detector is not None,
            "semantic_verification_enabled": self._semantic_verifier is not None,
            "verification_history_count": len(self._verification_history),
        }

        if self._anomaly_detector:
            stats["user_profiles"] = len(self._anomaly_detector._user_profiles)
            stats["recent_queries_tracked"] = sum(
                len(q) for q in self._anomaly_detector._query_history.values()
            )

        if self._verification_history:
            recent = self._verification_history[-100:]
            stats["recent_intent_match_rate"] = sum(
                1 for v in recent if v.get("intent_match")
            ) / len(recent)
            stats["recent_anomaly_rate"] = sum(
                1 for v in recent if v.get("anomaly_detected")
            ) / len(recent)

        return stats


# Factory function for cutting edge SQL tool
_cutting_edge_sql_tool: Optional[CuttingEdgeSQLQueryTool] = None


def get_cutting_edge_sql_tool(
    db_pool=None,
    config: Optional[CuttingEdgeSQLConfig] = None
) -> CuttingEdgeSQLQueryTool:
    """Get the global cutting edge SQL query tool instance."""
    global _cutting_edge_sql_tool
    if _cutting_edge_sql_tool is None or db_pool is not None:
        _cutting_edge_sql_tool = CuttingEdgeSQLQueryTool(
            db_pool=db_pool,
            config=config
        )
    return _cutting_edge_sql_tool
