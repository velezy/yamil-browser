"""
Chart Tool

Creates interactive visualizations using Plotly.
Supports:
- Bar charts
- Line charts
- Pie charts
- Scatter plots
- Area charts
- Histograms
"""

import logging
import re
import json
import base64
from typing import Any, Optional, TYPE_CHECKING

import plotly.graph_objects as go
import plotly.express as px
from plotly.io import to_html, to_image

from .base import BaseTool, ToolResult

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


class ChartTool(BaseTool):
    """
    Creates interactive charts from data.

    Analyzes query to determine chart type, extracts data from context,
    and generates Plotly visualizations.
    """

    # Chart type detection keywords
    CHART_KEYWORDS = {
        'bar': ['bar', 'bar chart', 'comparison', 'compare', 'categories'],
        'line': ['line', 'trend', 'over time', 'timeline', 'progress', 'growth'],
        'pie': ['pie', 'distribution', 'breakdown', 'percentage', 'share', 'composition'],
        'scatter': ['scatter', 'correlation', 'relationship', 'vs', 'versus'],
        'area': ['area', 'cumulative', 'stacked'],
        'histogram': ['histogram', 'frequency', 'distribution of'],
    }

    def __init__(self):
        super().__init__(
            name="chart",
            description="Creates interactive visualizations from data"
        )

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Create a chart based on query and context data.
        """
        try:
            query = context.query.lower()

            # Determine chart type
            chart_type = self._detect_chart_type(query)

            # Extract data from context
            data = self._extract_chart_data(context)

            if not data or (not data.get('labels') and not data.get('values')):
                return ToolResult(
                    success=False,
                    data=None,
                    error="No suitable data found for visualization"
                )

            # Create the chart
            fig = self._create_chart(chart_type, data, context.query)

            # Generate outputs
            html_content = to_html(fig, full_html=False, include_plotlyjs='cdn')

            # Try to generate image (requires kaleido)
            try:
                img_bytes = to_image(fig, format='png', width=800, height=500)
                img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            except Exception as e:
                logger.warning(f"Could not generate image: {e}")
                img_base64 = None

            # Get chart config as JSON for frontend
            chart_json = fig.to_json()

            return ToolResult(
                success=True,
                data={
                    "chart_type": chart_type,
                    "chart_json": chart_json,
                    "data_points": len(data.get('values', [])),
                },
                html=html_content,
                image_base64=img_base64,
                metadata={
                    "chart_type": chart_type,
                    "has_interactive": True,
                }
            )

        except Exception as e:
            logger.error(f"Chart tool error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    def _detect_chart_type(self, query: str) -> str:
        """Determine the best chart type based on query"""
        query_lower = query.lower()

        for chart_type, keywords in self.CHART_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return chart_type

        # Default to bar chart
        return 'bar'

    def _extract_chart_data(self, context: 'AgentContext') -> dict:
        """Extract labels and values from context for charting"""
        labels = []
        values = []

        for chunk in context.retrieved_chunks:
            content = chunk.get('content', '')

            # Try to find label:value patterns
            # Pattern: "Label: 123" or "Label - 123" or "Label (123)"
            patterns = [
                r'([A-Za-z][A-Za-z\s]+):\s*(\d+\.?\d*)',
                r'([A-Za-z][A-Za-z\s]+)\s*-\s*(\d+\.?\d*)',
                r'([A-Za-z][A-Za-z\s]+)\s*\((\d+\.?\d*)\)',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, content)
                for label, value in matches:
                    label = label.strip()
                    if label and len(label) < 50:  # Reasonable label length
                        try:
                            labels.append(label)
                            values.append(float(value))
                        except ValueError:
                            continue

            # Also extract standalone numbers with context
            if not values:
                numbers = re.findall(r'(\d+\.?\d*)', content)
                for i, num in enumerate(numbers[:10]):  # Limit to 10
                    try:
                        values.append(float(num))
                        labels.append(f"Value {i+1}")
                    except ValueError:
                        continue

        # Remove duplicates while preserving order
        seen = set()
        unique_labels = []
        unique_values = []
        for label, value in zip(labels, values):
            if label not in seen:
                seen.add(label)
                unique_labels.append(label)
                unique_values.append(value)

        return {
            'labels': unique_labels[:15],  # Limit for readability
            'values': unique_values[:15],
        }

    def _create_chart(self, chart_type: str, data: dict, title: str) -> go.Figure:
        """Create a Plotly figure based on chart type"""
        labels = data.get('labels', [])
        values = data.get('values', [])

        # Clean up title
        title = title[:80] + "..." if len(title) > 80 else title

        # Color scheme
        colors = px.colors.qualitative.Set2

        if chart_type == 'bar':
            fig = go.Figure(data=[
                go.Bar(
                    x=labels,
                    y=values,
                    marker_color=colors[:len(labels)],
                    text=values,
                    textposition='auto',
                )
            ])
            fig.update_layout(
                title=title,
                xaxis_title="Category",
                yaxis_title="Value",
            )

        elif chart_type == 'line':
            fig = go.Figure(data=[
                go.Scatter(
                    x=labels,
                    y=values,
                    mode='lines+markers',
                    line=dict(color=colors[0], width=2),
                    marker=dict(size=8),
                )
            ])
            fig.update_layout(
                title=title,
                xaxis_title="",
                yaxis_title="Value",
            )

        elif chart_type == 'pie':
            fig = go.Figure(data=[
                go.Pie(
                    labels=labels,
                    values=values,
                    hole=0.3,  # Donut style
                    marker=dict(colors=colors),
                )
            ])
            fig.update_layout(title=title)

        elif chart_type == 'scatter':
            # For scatter, use indices as x if we don't have paired data
            x_vals = list(range(len(values)))
            fig = go.Figure(data=[
                go.Scatter(
                    x=x_vals,
                    y=values,
                    mode='markers',
                    marker=dict(
                        size=12,
                        color=values,
                        colorscale='Viridis',
                        showscale=True,
                    ),
                    text=labels,
                )
            ])
            fig.update_layout(
                title=title,
                xaxis_title="Index",
                yaxis_title="Value",
            )

        elif chart_type == 'area':
            fig = go.Figure(data=[
                go.Scatter(
                    x=labels,
                    y=values,
                    fill='tozeroy',
                    fillcolor='rgba(0, 176, 246, 0.2)',
                    line=dict(color='rgb(0, 176, 246)'),
                )
            ])
            fig.update_layout(
                title=title,
                xaxis_title="",
                yaxis_title="Value",
            )

        elif chart_type == 'histogram':
            fig = go.Figure(data=[
                go.Histogram(
                    x=values,
                    nbinsx=min(20, len(values)),
                    marker_color=colors[0],
                )
            ])
            fig.update_layout(
                title=title,
                xaxis_title="Value",
                yaxis_title="Frequency",
            )

        else:
            # Default to bar
            fig = go.Figure(data=[go.Bar(x=labels, y=values)])
            fig.update_layout(title=title)

        # Common styling
        fig.update_layout(
            template='plotly_white',
            font=dict(family="Inter, sans-serif", size=12),
            margin=dict(l=50, r=50, t=80, b=50),
            height=500,
        )

        return fig

    def format_for_response(self, result: ToolResult) -> str:
        """Format chart result for inclusion in response"""
        if not result.success:
            return f"Chart error: {result.error}"

        data = result.data
        chart_type = data.get("chart_type", "chart")
        points = data.get("data_points", 0)

        return f"**Interactive {chart_type.title()} Chart** created with {points} data points. View the visualization above."
