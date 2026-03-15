"""
Table-to-Knowledge Conversion

Converts structured table data into knowledge statements and insights.
Enables natural language querying of tabular data.

Features:
- Automatic column semantic detection
- Time series analysis
- Trend detection
- Statistical insights generation
- Relationship discovery
- Natural language knowledge statements
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
import statistics

logger = logging.getLogger(__name__)

# =============================================================================
# LIBRARY AVAILABILITY
# =============================================================================

PANDAS_AVAILABLE = False
NUMPY_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    logger.warning("pandas not installed. Some features disabled.")

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    logger.warning("numpy not installed. Some features disabled.")


# =============================================================================
# DATA MODELS
# =============================================================================

class ColumnSemantic(str, Enum):
    """Semantic meaning of a column"""
    IDENTIFIER = "identifier"      # IDs, keys
    DIMENSION = "dimension"        # Categories, labels
    MEASURE = "measure"           # Numeric values
    TEMPORAL = "temporal"         # Date/time
    CALCULATED = "calculated"     # Derived values
    DESCRIPTIVE = "descriptive"   # Text descriptions


class InsightType(str, Enum):
    """Types of insights that can be generated"""
    SUMMARY = "summary"
    TREND = "trend"
    COMPARISON = "comparison"
    ANOMALY = "anomaly"
    CORRELATION = "correlation"
    DISTRIBUTION = "distribution"
    TOP_N = "top_n"
    PERCENTAGE = "percentage"


@dataclass
class ColumnProfile:
    """Profile of a single column"""
    name: str
    semantic: ColumnSemantic
    data_type: str
    sample_values: List[Any]
    unique_count: int
    null_count: int
    total_count: int
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean_value: Optional[float] = None
    sum_value: Optional[float] = None
    is_monotonic: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "semantic": self.semantic.value,
            "data_type": self.data_type,
            "unique_count": self.unique_count,
            "null_count": self.null_count,
            "null_percentage": round(self.null_count / self.total_count * 100, 1) if self.total_count else 0,
            "min": self.min_value,
            "max": self.max_value,
            "mean": round(self.mean_value, 2) if self.mean_value else None,
            "sum": round(self.sum_value, 2) if self.sum_value else None,
        }


@dataclass
class KnowledgeStatement:
    """A natural language knowledge statement derived from data"""
    statement: str
    insight_type: InsightType
    confidence: float
    supporting_data: Dict[str, Any] = field(default_factory=dict)
    columns_involved: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "statement": self.statement,
            "type": self.insight_type.value,
            "confidence": self.confidence,
            "supporting_data": self.supporting_data,
            "columns": self.columns_involved,
        }


@dataclass
class TableKnowledge:
    """Complete knowledge extracted from a table"""
    table_name: str
    row_count: int
    column_count: int
    columns: List[ColumnProfile]
    statements: List[KnowledgeStatement]
    relationships: List[Dict[str, Any]]
    answerable_questions: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.to_dict() for c in self.columns],
            "statements": [s.to_dict() for s in self.statements],
            "relationships": self.relationships,
            "answerable_questions": self.answerable_questions,
        }

    def to_markdown(self) -> str:
        """Generate markdown summary"""
        lines = [f"# Table Knowledge: {self.table_name}"]
        lines.append(f"\n**Rows:** {self.row_count} | **Columns:** {self.column_count}")

        lines.append("\n## Column Summary")
        lines.append("| Column | Type | Semantic | Stats |")
        lines.append("|--------|------|----------|-------|")
        for col in self.columns:
            stats = []
            if col.mean_value is not None:
                stats.append(f"avg: {col.mean_value:.2f}")
            if col.sum_value is not None:
                stats.append(f"sum: {col.sum_value:.2f}")
            if col.min_value is not None and col.max_value is not None:
                stats.append(f"range: {col.min_value}-{col.max_value}")

            lines.append(
                f"| {col.name} | {col.data_type} | {col.semantic.value} | "
                f"{', '.join(stats) if stats else 'N/A'} |"
            )

        if self.statements:
            lines.append("\n## Key Insights")
            for stmt in self.statements[:10]:
                lines.append(f"- {stmt.statement}")

        if self.relationships:
            lines.append("\n## Detected Relationships")
            for rel in self.relationships[:5]:
                lines.append(f"- {rel.get('description', str(rel))}")

        if self.answerable_questions:
            lines.append("\n## Questions This Data Can Answer")
            for q in self.answerable_questions[:10]:
                lines.append(f"- {q}")

        return "\n".join(lines)


# =============================================================================
# TABLE KNOWLEDGE CONVERTER
# =============================================================================

class TableKnowledgeConverter:
    """
    Converts tabular data into knowledge statements and insights.

    Capabilities:
    - Column semantic detection
    - Statistical analysis
    - Trend detection
    - Natural language statement generation
    - Question generation
    """

    # Keywords for semantic detection
    ID_KEYWORDS = ['id', 'key', 'code', 'number', 'index', 'ref', 'uuid']
    TEMPORAL_KEYWORDS = ['date', 'time', 'year', 'month', 'day', 'quarter', 'week', 'period']
    MEASURE_KEYWORDS = ['amount', 'total', 'sum', 'count', 'qty', 'quantity', 'price',
                       'cost', 'revenue', 'sales', 'profit', 'value', 'rate', 'percent']
    DIMENSION_KEYWORDS = ['name', 'category', 'type', 'status', 'region', 'country',
                         'product', 'customer', 'vendor', 'department']

    def __init__(self):
        pass

    def convert(
        self,
        data: Union[List[Dict], 'pd.DataFrame'],
        table_name: str = "data"
    ) -> TableKnowledge:
        """
        Convert table data into knowledge.

        Args:
            data: List of dicts or pandas DataFrame
            table_name: Name of the table

        Returns:
            TableKnowledge with extracted insights
        """
        # Convert to list of dicts if DataFrame
        if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
            records = data.to_dict('records')
            columns = list(data.columns)
        else:
            records = data
            columns = list(data[0].keys()) if data else []

        if not records:
            return TableKnowledge(
                table_name=table_name,
                row_count=0,
                column_count=0,
                columns=[],
                statements=[],
                relationships=[],
                answerable_questions=[],
            )

        # Profile columns
        column_profiles = [
            self._profile_column(col, records) for col in columns
        ]

        # Generate statements
        statements = []
        statements.extend(self._generate_summary_statements(column_profiles, records))
        statements.extend(self._generate_trend_statements(column_profiles, records))
        statements.extend(self._generate_comparison_statements(column_profiles, records))
        statements.extend(self._generate_top_n_statements(column_profiles, records))

        # Detect relationships
        relationships = self._detect_relationships(column_profiles, records)

        # Generate answerable questions
        questions = self._generate_questions(column_profiles, records)

        return TableKnowledge(
            table_name=table_name,
            row_count=len(records),
            column_count=len(columns),
            columns=column_profiles,
            statements=statements,
            relationships=relationships,
            answerable_questions=questions,
        )

    def _profile_column(self, column: str, records: List[Dict]) -> ColumnProfile:
        """Profile a single column."""
        values = [r.get(column) for r in records]
        non_null = [v for v in values if v is not None and v != '']

        # Detect data type
        data_type = self._detect_data_type(non_null)

        # Detect semantic
        semantic = self._detect_semantic(column, non_null, data_type)

        # Calculate statistics
        unique_count = len(set(str(v) for v in non_null))
        null_count = len(values) - len(non_null)

        min_val = None
        max_val = None
        mean_val = None
        sum_val = None
        is_monotonic = False

        if data_type in ['int', 'float'] and non_null:
            numeric = [float(v) for v in non_null if self._is_numeric(v)]
            if numeric:
                min_val = min(numeric)
                max_val = max(numeric)
                mean_val = statistics.mean(numeric)
                sum_val = sum(numeric)

                # Check monotonicity
                if len(numeric) > 1:
                    diffs = [numeric[i+1] - numeric[i] for i in range(len(numeric)-1)]
                    is_monotonic = all(d >= 0 for d in diffs) or all(d <= 0 for d in diffs)

        return ColumnProfile(
            name=column,
            semantic=semantic,
            data_type=data_type,
            sample_values=non_null[:5],
            unique_count=unique_count,
            null_count=null_count,
            total_count=len(values),
            min_value=min_val,
            max_value=max_val,
            mean_value=mean_val,
            sum_value=sum_val,
            is_monotonic=is_monotonic,
        )

    def _detect_data_type(self, values: List[Any]) -> str:
        """Detect data type from values."""
        if not values:
            return 'unknown'

        # Sample values for detection
        sample = values[:100]

        # Check types
        int_count = sum(1 for v in sample if self._is_int(v))
        float_count = sum(1 for v in sample if self._is_float(v))
        date_count = sum(1 for v in sample if self._is_date(v))

        total = len(sample)

        if date_count > total * 0.8:
            return 'date'
        if int_count > total * 0.8:
            return 'int'
        if float_count > total * 0.8:
            return 'float'

        return 'string'

    def _detect_semantic(
        self,
        column: str,
        values: List[Any],
        data_type: str
    ) -> ColumnSemantic:
        """Detect semantic meaning of column."""
        col_lower = column.lower()

        # Check keywords
        if any(kw in col_lower for kw in self.ID_KEYWORDS):
            return ColumnSemantic.IDENTIFIER

        if any(kw in col_lower for kw in self.TEMPORAL_KEYWORDS):
            return ColumnSemantic.TEMPORAL

        if data_type == 'date':
            return ColumnSemantic.TEMPORAL

        if any(kw in col_lower for kw in self.MEASURE_KEYWORDS):
            return ColumnSemantic.MEASURE

        if data_type in ['int', 'float']:
            return ColumnSemantic.MEASURE

        if any(kw in col_lower for kw in self.DIMENSION_KEYWORDS):
            return ColumnSemantic.DIMENSION

        # Check cardinality
        if values:
            unique_ratio = len(set(str(v) for v in values)) / len(values)
            if unique_ratio < 0.3:
                return ColumnSemantic.DIMENSION
            if unique_ratio > 0.9:
                return ColumnSemantic.DESCRIPTIVE

        return ColumnSemantic.DIMENSION

    def _generate_summary_statements(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[KnowledgeStatement]:
        """Generate summary statistics statements."""
        statements = []

        # Overall summary
        statements.append(KnowledgeStatement(
            statement=f"The dataset contains {len(records)} records with {len(columns)} columns.",
            insight_type=InsightType.SUMMARY,
            confidence=1.0,
            supporting_data={"rows": len(records), "columns": len(columns)},
        ))

        # Measure column summaries
        for col in columns:
            if col.semantic == ColumnSemantic.MEASURE and col.sum_value is not None:
                statements.append(KnowledgeStatement(
                    statement=f"Total {col.name} is {col.sum_value:,.2f} with an average of {col.mean_value:,.2f}.",
                    insight_type=InsightType.SUMMARY,
                    confidence=1.0,
                    supporting_data={"sum": col.sum_value, "mean": col.mean_value},
                    columns_involved=[col.name],
                ))

                if col.min_value is not None and col.max_value is not None:
                    statements.append(KnowledgeStatement(
                        statement=f"{col.name} ranges from {col.min_value:,.2f} to {col.max_value:,.2f}.",
                        insight_type=InsightType.SUMMARY,
                        confidence=1.0,
                        supporting_data={"min": col.min_value, "max": col.max_value},
                        columns_involved=[col.name],
                    ))

        # Dimension column summaries
        for col in columns:
            if col.semantic == ColumnSemantic.DIMENSION:
                statements.append(KnowledgeStatement(
                    statement=f"There are {col.unique_count} unique {col.name} values.",
                    insight_type=InsightType.SUMMARY,
                    confidence=1.0,
                    supporting_data={"unique_count": col.unique_count},
                    columns_involved=[col.name],
                ))

        return statements

    def _generate_trend_statements(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[KnowledgeStatement]:
        """Generate trend-related statements."""
        statements = []

        # Find temporal and measure columns
        temporal_cols = [c for c in columns if c.semantic == ColumnSemantic.TEMPORAL]
        measure_cols = [c for c in columns if c.semantic == ColumnSemantic.MEASURE]

        if not temporal_cols or not measure_cols:
            return statements

        temporal_col = temporal_cols[0]

        for measure_col in measure_cols[:3]:  # Limit to first 3 measures
            if measure_col.is_monotonic:
                # Get first and last values
                values = [r.get(measure_col.name) for r in records
                         if self._is_numeric(r.get(measure_col.name))]
                if len(values) >= 2:
                    first_val = float(values[0])
                    last_val = float(values[-1])

                    if last_val > first_val:
                        pct_change = ((last_val - first_val) / first_val) * 100 if first_val != 0 else 0
                        statements.append(KnowledgeStatement(
                            statement=f"{measure_col.name} shows an increasing trend over {temporal_col.name}, growing by {pct_change:.1f}%.",
                            insight_type=InsightType.TREND,
                            confidence=0.85,
                            supporting_data={
                                "start": first_val,
                                "end": last_val,
                                "change_pct": pct_change
                            },
                            columns_involved=[temporal_col.name, measure_col.name],
                        ))
                    elif last_val < first_val:
                        pct_change = ((first_val - last_val) / first_val) * 100 if first_val != 0 else 0
                        statements.append(KnowledgeStatement(
                            statement=f"{measure_col.name} shows a decreasing trend over {temporal_col.name}, declining by {pct_change:.1f}%.",
                            insight_type=InsightType.TREND,
                            confidence=0.85,
                            supporting_data={
                                "start": first_val,
                                "end": last_val,
                                "change_pct": -pct_change
                            },
                            columns_involved=[temporal_col.name, measure_col.name],
                        ))

        return statements

    def _generate_comparison_statements(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[KnowledgeStatement]:
        """Generate comparison statements."""
        statements = []

        # Find dimension and measure columns
        dimension_cols = [c for c in columns if c.semantic == ColumnSemantic.DIMENSION]
        measure_cols = [c for c in columns if c.semantic == ColumnSemantic.MEASURE]

        if not dimension_cols or not measure_cols:
            return statements

        for dim_col in dimension_cols[:2]:
            for measure_col in measure_cols[:2]:
                # Group by dimension
                groups = {}
                for r in records:
                    key = r.get(dim_col.name)
                    value = r.get(measure_col.name)
                    if key is not None and self._is_numeric(value):
                        if key not in groups:
                            groups[key] = []
                        groups[key].append(float(value))

                if len(groups) >= 2:
                    # Calculate group totals
                    group_totals = {k: sum(v) for k, v in groups.items()}
                    sorted_groups = sorted(group_totals.items(), key=lambda x: x[1], reverse=True)

                    if sorted_groups:
                        top_name, top_val = sorted_groups[0]
                        total = sum(group_totals.values())
                        pct = (top_val / total) * 100 if total > 0 else 0

                        statements.append(KnowledgeStatement(
                            statement=f"{top_name} has the highest {measure_col.name} ({top_val:,.2f}), accounting for {pct:.1f}% of the total.",
                            insight_type=InsightType.COMPARISON,
                            confidence=0.9,
                            supporting_data={
                                "top_category": top_name,
                                "top_value": top_val,
                                "percentage": pct,
                            },
                            columns_involved=[dim_col.name, measure_col.name],
                        ))

                        if len(sorted_groups) >= 2:
                            bottom_name, bottom_val = sorted_groups[-1]
                            statements.append(KnowledgeStatement(
                                statement=f"{bottom_name} has the lowest {measure_col.name} ({bottom_val:,.2f}).",
                                insight_type=InsightType.COMPARISON,
                                confidence=0.9,
                                supporting_data={
                                    "bottom_category": bottom_name,
                                    "bottom_value": bottom_val,
                                },
                                columns_involved=[dim_col.name, measure_col.name],
                            ))

        return statements

    def _generate_top_n_statements(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[KnowledgeStatement]:
        """Generate top N statements."""
        statements = []

        # Find identifier and measure columns
        id_cols = [c for c in columns if c.semantic in [ColumnSemantic.IDENTIFIER, ColumnSemantic.DIMENSION]]
        measure_cols = [c for c in columns if c.semantic == ColumnSemantic.MEASURE]

        if not id_cols or not measure_cols:
            return statements

        id_col = id_cols[0]

        for measure_col in measure_cols[:2]:
            # Sort by measure
            sorted_records = sorted(
                [r for r in records if self._is_numeric(r.get(measure_col.name))],
                key=lambda r: float(r.get(measure_col.name, 0)),
                reverse=True
            )

            if len(sorted_records) >= 3:
                top_3 = sorted_records[:3]
                top_names = [r.get(id_col.name) for r in top_3]
                top_values = [float(r.get(measure_col.name)) for r in top_3]

                statements.append(KnowledgeStatement(
                    statement=f"Top 3 by {measure_col.name}: {top_names[0]} ({top_values[0]:,.2f}), {top_names[1]} ({top_values[1]:,.2f}), {top_names[2]} ({top_values[2]:,.2f}).",
                    insight_type=InsightType.TOP_N,
                    confidence=1.0,
                    supporting_data={
                        "top_3_names": top_names,
                        "top_3_values": top_values,
                    },
                    columns_involved=[id_col.name, measure_col.name],
                ))

        return statements

    def _detect_relationships(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[Dict[str, Any]]:
        """Detect relationships between columns."""
        relationships = []

        measure_cols = [c for c in columns if c.semantic == ColumnSemantic.MEASURE]

        # Check for formula relationships (e.g., Profit = Revenue - Expenses)
        for i, col1 in enumerate(measure_cols):
            for j, col2 in enumerate(measure_cols):
                if i >= j:
                    continue

                # Check if col1 + col2 = col3 for some col3
                for col3 in measure_cols:
                    if col3 == col1 or col3 == col2:
                        continue

                    # Sample check
                    matches = 0
                    total = 0
                    for r in records[:20]:
                        v1 = r.get(col1.name)
                        v2 = r.get(col2.name)
                        v3 = r.get(col3.name)

                        if all(self._is_numeric(v) for v in [v1, v2, v3]):
                            total += 1
                            if abs(float(v1) + float(v2) - float(v3)) < 0.01:
                                matches += 1
                            elif abs(float(v1) - float(v2) - float(v3)) < 0.01:
                                matches += 1

                    if total > 0 and matches / total > 0.9:
                        relationships.append({
                            "type": "formula",
                            "columns": [col1.name, col2.name, col3.name],
                            "description": f"{col3.name} appears to be calculated from {col1.name} and {col2.name}",
                        })

        return relationships

    def _generate_questions(
        self,
        columns: List[ColumnProfile],
        records: List[Dict]
    ) -> List[str]:
        """Generate questions this data can answer."""
        questions = []

        measure_cols = [c for c in columns if c.semantic == ColumnSemantic.MEASURE]
        dimension_cols = [c for c in columns if c.semantic == ColumnSemantic.DIMENSION]
        temporal_cols = [c for c in columns if c.semantic == ColumnSemantic.TEMPORAL]

        for measure in measure_cols:
            questions.append(f"What is the total {measure.name}?")
            questions.append(f"What is the average {measure.name}?")

            for dim in dimension_cols:
                questions.append(f"What is the {measure.name} by {dim.name}?")
                questions.append(f"Which {dim.name} has the highest {measure.name}?")

            for temporal in temporal_cols:
                questions.append(f"How has {measure.name} changed over {temporal.name}?")
                questions.append(f"What was the {measure.name} trend in the last period?")

        for dim in dimension_cols:
            questions.append(f"How many unique {dim.name} values are there?")

        return questions[:15]  # Limit to 15 questions

    # Helper methods
    def _is_numeric(self, value: Any) -> bool:
        if value is None:
            return False
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False

    def _is_int(self, value: Any) -> bool:
        if not self._is_numeric(value):
            return False
        try:
            return float(value) == int(float(value))
        except Exception:
            return False

    def _is_float(self, value: Any) -> bool:
        return self._is_numeric(value) and not self._is_int(value)

    def _is_date(self, value: Any) -> bool:
        if isinstance(value, (datetime,)):
            return True
        if isinstance(value, str):
            date_patterns = [
                r'^\d{4}-\d{2}-\d{2}$',
                r'^\d{2}/\d{2}/\d{4}$',
                r'^\d{2}-\d{2}-\d{4}$',
            ]
            return any(re.match(p, value) for p in date_patterns)
        return False


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def convert_table_to_knowledge(
    data: Union[List[Dict], Any],
    table_name: str = "data"
) -> TableKnowledge:
    """
    Convert tabular data to knowledge statements.

    Args:
        data: List of dicts or pandas DataFrame
        table_name: Name of the table

    Returns:
        TableKnowledge with extracted insights
    """
    converter = TableKnowledgeConverter()
    return converter.convert(data, table_name)


def generate_data_summary(data: Union[List[Dict], Any]) -> str:
    """
    Generate a natural language summary of tabular data.

    Args:
        data: List of dicts or pandas DataFrame

    Returns:
        Markdown summary string
    """
    knowledge = convert_table_to_knowledge(data)
    return knowledge.to_markdown()


__all__ = [
    'TableKnowledgeConverter',
    'TableKnowledge',
    'KnowledgeStatement',
    'ColumnProfile',
    'ColumnSemantic',
    'InsightType',
    'convert_table_to_knowledge',
    'generate_data_summary',
]
