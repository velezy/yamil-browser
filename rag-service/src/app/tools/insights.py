"""
Data Insights Generator

Combines multiple tools to generate actionable insights from user data.
Provides:
- Automated data analysis
- Trend detection
- Anomaly identification
- Decision support recommendations
"""

import logging
from typing import Any, Optional, TYPE_CHECKING, List, Dict
from dataclasses import dataclass

from .base import BaseTool, ToolResult

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext
    from .calculator import CalculatorTool
    from .chart import ChartTool
    from .sql_query import SQLQueryTool

logger = logging.getLogger(__name__)


@dataclass
class Insight:
    """A single insight from data analysis"""
    category: str  # trend, anomaly, recommendation, summary
    title: str
    description: str
    confidence: float  # 0.0 - 1.0
    data: Optional[dict] = None
    action: Optional[str] = None  # Recommended action


class DataInsightsTool(BaseTool):
    """
    Generates actionable insights by combining multiple analysis tools.

    Flow:
    1. Query user data (SQL)
    2. Analyze numbers (Calculator)
    3. Identify patterns and trends
    4. Generate visualizations (Chart)
    5. Provide recommendations
    """

    def __init__(
        self,
        calculator: 'CalculatorTool' = None,
        chart: 'ChartTool' = None,
        sql: 'SQLQueryTool' = None,
    ):
        super().__init__(
            name="insights",
            description="Generates actionable insights from user data"
        )
        self.calculator = calculator
        self.chart = chart
        self.sql = sql

    def set_tools(
        self,
        calculator: 'CalculatorTool' = None,
        chart: 'ChartTool' = None,
        sql: 'SQLQueryTool' = None,
    ):
        """Set dependent tools"""
        if calculator:
            self.calculator = calculator
        if chart:
            self.chart = chart
        if sql:
            self.sql = sql

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Generate comprehensive insights from user's data.
        """
        try:
            insights: List[Insight] = []
            charts: List[dict] = []

            # Step 1: Get user's data overview
            if self.sql:
                data_overview = await self._get_data_overview(context)
                if data_overview:
                    insights.extend(self._analyze_overview(data_overview))

            # Step 2: Analyze document patterns
            if self.sql and context.user_id:
                doc_insights = await self._analyze_documents(context)
                insights.extend(doc_insights)

            # Step 3: Analyze usage patterns
            usage_insights = await self._analyze_usage(context)
            insights.extend(usage_insights)

            # Step 4: Generate trend analysis
            if self.calculator and context.retrieved_chunks:
                calc_result = await self.calculator.execute(context)
                if calc_result.success:
                    insights.append(Insight(
                        category="summary",
                        title="Numerical Analysis",
                        description=calc_result.data.get("explanation", ""),
                        confidence=0.85,
                        data=calc_result.data,
                    ))

            # Step 5: Create visualization if chart tool available
            if self.chart and insights:
                chart_result = await self.chart.execute(context)
                if chart_result.success:
                    charts.append({
                        "type": chart_result.data.get("chart_type"),
                        "html": chart_result.html,
                        "image": chart_result.image_base64,
                    })

            # Step 6: Generate recommendations
            recommendations = self._generate_recommendations(insights)
            insights.extend(recommendations)

            return ToolResult(
                success=True,
                data={
                    "insights": [self._insight_to_dict(i) for i in insights],
                    "charts": charts,
                    "insight_count": len(insights),
                    "recommendation_count": len([i for i in insights if i.category == "recommendation"]),
                },
                html=charts[0]["html"] if charts else None,
                metadata={
                    "has_visualizations": len(charts) > 0,
                    "categories": list(set(i.category for i in insights)),
                }
            )

        except Exception as e:
            logger.error(f"Insights tool error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    async def _get_data_overview(self, context: 'AgentContext') -> Optional[dict]:
        """Get overview of user's data"""
        if not self.sql or not context.user_id:
            return None

        # Create a temporary context for the overview query
        from ..agents.base_agent import AgentContext
        overview_context = AgentContext(
            query="statistics summary overview",
            user_id=context.user_id,
        )

        result = await self.sql.execute(overview_context)
        if result.success and result.data.get("results"):
            return result.data["results"][0]
        return None

    def _analyze_overview(self, overview: dict) -> List[Insight]:
        """Analyze data overview and generate insights"""
        insights = []

        total_docs = overview.get("total_documents", 0)
        total_size = overview.get("total_size_bytes", 0)
        total_convos = overview.get("total_conversations", 0)
        total_messages = overview.get("total_messages", 0)

        # Document insights
        if total_docs > 0:
            avg_size = total_size / total_docs if total_docs > 0 else 0
            insights.append(Insight(
                category="summary",
                title="Knowledge Base Summary",
                description=f"You have {total_docs} documents totaling {self._format_bytes(total_size)}. Average document size: {self._format_bytes(avg_size)}",
                confidence=1.0,
                data={"documents": total_docs, "size_bytes": total_size},
            ))

        # Conversation insights
        if total_convos > 0:
            avg_messages = total_messages / total_convos if total_convos > 0 else 0
            insights.append(Insight(
                category="summary",
                title="Conversation Activity",
                description=f"You have {total_convos} conversations with {total_messages} total messages. Average {avg_messages:.1f} messages per conversation.",
                confidence=1.0,
                data={"conversations": total_convos, "messages": total_messages},
            ))

        # Empty state
        if total_docs == 0:
            insights.append(Insight(
                category="recommendation",
                title="Get Started",
                description="Upload your first document to start building your knowledge base.",
                confidence=1.0,
                action="Upload a document",
            ))

        return insights

    async def _analyze_documents(self, context: 'AgentContext') -> List[Insight]:
        """Analyze document patterns"""
        insights = []

        if not self.sql or not context.user_id:
            return insights

        # Get document types breakdown
        from ..agents.base_agent import AgentContext
        type_context = AgentContext(
            query="document type breakdown",
            user_id=context.user_id,
        )

        result = await self.sql.execute(type_context)
        if result.success and result.data.get("results"):
            types = result.data["results"]
            if len(types) > 1:
                most_common = types[0]
                insights.append(Insight(
                    category="trend",
                    title="Document Types",
                    description=f"Your most common document type is {most_common.get('file_type', 'unknown')} ({most_common.get('count', 0)} files)",
                    confidence=0.9,
                    data={"types": types},
                ))

        return insights

    async def _analyze_usage(self, context: 'AgentContext') -> List[Insight]:
        """Analyze usage patterns from context"""
        insights = []

        # Analyze retrieved chunks if available
        if context.retrieved_chunks:
            chunk_count = len(context.retrieved_chunks)
            avg_score = sum(context.chunk_scores.values()) / len(context.chunk_scores) if context.chunk_scores else 0

            if avg_score < 0.5:
                insights.append(Insight(
                    category="recommendation",
                    title="Improve Search Results",
                    description="The relevance scores for your query are low. Consider uploading more specific documents or rephrasing your question.",
                    confidence=0.75,
                    action="Upload more relevant documents",
                ))

        return insights

    def _generate_recommendations(self, insights: List[Insight]) -> List[Insight]:
        """Generate actionable recommendations based on insights"""
        recommendations = []

        # Check for patterns that suggest recommendations
        categories = [i.category for i in insights]
        has_data = any(i.data for i in insights)

        if 'summary' in categories and has_data:
            recommendations.append(Insight(
                category="recommendation",
                title="Data-Driven Decision",
                description="Based on your data analysis, consider reviewing the trends identified above to inform your next steps.",
                confidence=0.7,
                action="Review insights and take action",
            ))

        return recommendations

    def _insight_to_dict(self, insight: Insight) -> dict:
        """Convert Insight to dictionary"""
        return {
            "category": insight.category,
            "title": insight.title,
            "description": insight.description,
            "confidence": insight.confidence,
            "data": insight.data,
            "action": insight.action,
        }

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable string"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_value < 1024:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024
        return f"{bytes_value:.1f} TB"

    def format_for_response(self, result: ToolResult) -> str:
        """Format insights for inclusion in response"""
        if not result.success:
            return f"Insight generation error: {result.error}"

        data = result.data
        insights = data.get("insights", [])

        if not insights:
            return "No specific insights could be generated from the available data."

        output = "**Data Insights:**\n\n"

        # Group by category
        categories = {}
        for insight in insights:
            cat = insight["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(insight)

        # Format each category
        for category, items in categories.items():
            output += f"### {category.title()}\n"
            for item in items:
                output += f"- **{item['title']}**: {item['description']}\n"
                if item.get("action"):
                    output += f"  - *Recommended action: {item['action']}*\n"
            output += "\n"

        return output
