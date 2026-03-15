"""
AI Tools for Data Analysis and Visualization

Provides tools for:
- Calculator: Mathematical operations on data
- Chart: Interactive visualizations with Plotly
- SQL: Query user's data in PostgreSQL (with HuggingFace Text2SQL-1.5B model)
- Insights: Data-driven recommendations and decision support
- Text2SQL: HuggingFace model for natural language to SQL conversion
- Code: Sandboxed Python execution for complex analysis
"""

from .base import BaseTool, ToolResult
from .calculator import CalculatorTool
from .chart import ChartTool
from .sql_query import SQLQueryTool
from .insights import DataInsightsTool, Insight
from .code_executor import CodeExecutorTool, execute_python, is_code_safe

# Try to import Text2SQL service
try:
    from .text2sql import (
        Text2SQLService,
        Text2SQLResult,
        get_text2sql_service,
        generate_sql_from_question
    )
    TEXT2SQL_AVAILABLE = True
except ImportError:
    TEXT2SQL_AVAILABLE = False
    Text2SQLService = None
    Text2SQLResult = None
    get_text2sql_service = None
    generate_sql_from_question = None

__all__ = [
    'BaseTool',
    'ToolResult',
    'CalculatorTool',
    'ChartTool',
    'SQLQueryTool',
    'DataInsightsTool',
    'Insight',
    # Code Executor
    'CodeExecutorTool',
    'execute_python',
    'is_code_safe',
    # Text2SQL
    'Text2SQLService',
    'Text2SQLResult',
    'get_text2sql_service',
    'generate_sql_from_question',
    'TEXT2SQL_AVAILABLE',
]


def create_all_tools(db_pool=None, use_text2sql_model: bool = True):
    """
    Factory function to create and wire up all tools.

    Args:
        db_pool: Database connection pool for SQL queries
        use_text2sql_model: Whether to use HuggingFace Text2SQL-1.5B model
                           for natural language to SQL conversion

    Returns:
        dict of tool name -> tool instance
    """
    calculator = CalculatorTool()
    chart = ChartTool()
    sql = SQLQueryTool(db_pool=db_pool, use_text2sql_model=use_text2sql_model)
    code = CodeExecutorTool()
    insights = DataInsightsTool(
        calculator=calculator,
        chart=chart,
        sql=sql,
    )

    return {
        'calculator': calculator,
        'chart': chart,
        'sql': sql,
        'code': code,
        'insights': insights,
    }
