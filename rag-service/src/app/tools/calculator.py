"""
Calculator Tool

Performs mathematical operations on data extracted from documents.
Supports:
- Basic arithmetic
- Statistical calculations (mean, median, std, etc.)
- Percentage calculations
- Growth/change calculations
- Comparisons
"""

import logging
import re
import statistics
from typing import Any, Optional, TYPE_CHECKING

from .base import BaseTool, ToolResult

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


class CalculatorTool(BaseTool):
    """
    Calculator for data analysis operations.

    Can extract numbers from context and perform calculations.
    """

    def __init__(self):
        super().__init__(
            name="calculator",
            description="Performs mathematical calculations on data"
        )

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Execute calculations based on query and context.

        Analyzes the query to determine what calculation is needed,
        extracts numbers from retrieved chunks, and performs the operation.
        """
        try:
            query = context.query.lower()

            # Extract all numbers from context
            numbers = self._extract_numbers_from_context(context)

            if not numbers:
                return ToolResult(
                    success=False,
                    data=None,
                    error="No numerical data found in context"
                )

            # Determine operation from query
            result = await self._perform_operation(query, numbers, context)

            return ToolResult(
                success=True,
                data=result,
                metadata={
                    "numbers_found": len(numbers),
                    "operation": result.get("operation", "unknown"),
                }
            )

        except Exception as e:
            logger.error(f"Calculator error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    def _extract_numbers_from_context(self, context: 'AgentContext') -> list[float]:
        """Extract numerical values from retrieved chunks"""
        numbers = []

        for chunk in context.retrieved_chunks:
            content = chunk.get('content', '')
            # Find all numbers (including decimals and negative)
            found = re.findall(r'-?\d+\.?\d*', content)
            for num_str in found:
                try:
                    num = float(num_str)
                    # Filter out likely non-data numbers (page numbers, IDs, etc.)
                    if abs(num) < 1e12:  # Reasonable range
                        numbers.append(num)
                except ValueError:
                    continue

        return numbers

    async def _perform_operation(
        self,
        query: str,
        numbers: list[float],
        context: 'AgentContext'
    ) -> dict:
        """Determine and perform the appropriate calculation"""

        # Sum/Total
        if any(word in query for word in ['sum', 'total', 'add', 'combined']):
            return {
                "operation": "sum",
                "result": sum(numbers),
                "formula": f"sum({len(numbers)} numbers)",
                "explanation": f"The sum of {len(numbers)} values is {sum(numbers):.2f}"
            }

        # Average/Mean
        if any(word in query for word in ['average', 'mean', 'avg']):
            avg = statistics.mean(numbers)
            return {
                "operation": "average",
                "result": avg,
                "formula": f"sum / count = {sum(numbers):.2f} / {len(numbers)}",
                "explanation": f"The average of {len(numbers)} values is {avg:.2f}"
            }

        # Median
        if 'median' in query:
            med = statistics.median(numbers)
            return {
                "operation": "median",
                "result": med,
                "explanation": f"The median value is {med:.2f}"
            }

        # Standard Deviation
        if any(word in query for word in ['deviation', 'std', 'variance']):
            if len(numbers) >= 2:
                std = statistics.stdev(numbers)
                return {
                    "operation": "standard_deviation",
                    "result": std,
                    "explanation": f"The standard deviation is {std:.2f}"
                }

        # Min/Max
        if 'minimum' in query or 'min' in query or 'lowest' in query:
            return {
                "operation": "minimum",
                "result": min(numbers),
                "explanation": f"The minimum value is {min(numbers):.2f}"
            }

        if 'maximum' in query or 'max' in query or 'highest' in query:
            return {
                "operation": "maximum",
                "result": max(numbers),
                "explanation": f"The maximum value is {max(numbers):.2f}"
            }

        # Percentage/Growth (needs at least 2 numbers)
        if len(numbers) >= 2:
            if any(word in query for word in ['percentage', 'percent', '%', 'growth', 'change', 'increase', 'decrease']):
                old_val, new_val = numbers[0], numbers[-1]
                if old_val != 0:
                    pct_change = ((new_val - old_val) / abs(old_val)) * 100
                    direction = "increase" if pct_change > 0 else "decrease"
                    return {
                        "operation": "percentage_change",
                        "result": pct_change,
                        "formula": f"((new - old) / old) * 100 = (({new_val:.2f} - {old_val:.2f}) / {old_val:.2f}) * 100",
                        "explanation": f"There was a {abs(pct_change):.1f}% {direction} from {old_val:.2f} to {new_val:.2f}"
                    }

            # Difference
            if any(word in query for word in ['difference', 'diff', 'compare', 'comparison']):
                diff = numbers[-1] - numbers[0]
                return {
                    "operation": "difference",
                    "result": diff,
                    "formula": f"{numbers[-1]:.2f} - {numbers[0]:.2f}",
                    "explanation": f"The difference is {diff:.2f}"
                }

            # Ratio
            if 'ratio' in query:
                if numbers[1] != 0:
                    ratio = numbers[0] / numbers[1]
                    return {
                        "operation": "ratio",
                        "result": ratio,
                        "formula": f"{numbers[0]:.2f} / {numbers[1]:.2f}",
                        "explanation": f"The ratio is {ratio:.2f}:1"
                    }

        # Default: provide summary statistics
        return {
            "operation": "summary",
            "result": {
                "count": len(numbers),
                "sum": sum(numbers),
                "mean": statistics.mean(numbers),
                "min": min(numbers),
                "max": max(numbers),
                "range": max(numbers) - min(numbers),
            },
            "explanation": f"Summary of {len(numbers)} numerical values found in the data"
        }

    def format_for_response(self, result: ToolResult) -> str:
        """Format calculator result for inclusion in response"""
        if not result.success:
            return f"Calculation error: {result.error}"

        data = result.data
        operation = data.get("operation", "calculation")
        explanation = data.get("explanation", "")

        if operation == "summary":
            stats = data["result"]
            return f"""
**Data Analysis Summary:**
- Count: {stats['count']} values
- Sum: {stats['sum']:.2f}
- Average: {stats['mean']:.2f}
- Range: {stats['min']:.2f} to {stats['max']:.2f}
"""
        else:
            return f"**{operation.replace('_', ' ').title()}:** {explanation}"
