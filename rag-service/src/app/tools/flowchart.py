"""
Flowchart Generator Tool

Creates flowcharts and diagrams from natural language descriptions.
Supports Mermaid, Graphviz DOT, and ASCII output formats.

Features:
- Natural language to diagram conversion
- Process flow visualization
- Decision tree generation
- Sequence diagrams
- Entity relationship diagrams
- Multiple output formats
"""

import logging
import re
from typing import Any, List, Optional, TYPE_CHECKING
from enum import Enum

from .base import BaseTool, ToolResult

if TYPE_CHECKING:
    from ..agents.base_agent import AgentContext

logger = logging.getLogger(__name__)


class DiagramType(str, Enum):
    """Types of diagrams that can be generated"""
    FLOWCHART = "flowchart"
    SEQUENCE = "sequence"
    STATE = "state"
    ER = "er"
    MINDMAP = "mindmap"
    TIMELINE = "timeline"
    GANTT = "gantt"
    CLASS = "class"


class OutputFormat(str, Enum):
    """Output format options"""
    MERMAID = "mermaid"
    DOT = "dot"
    ASCII = "ascii"


class FlowchartTool(BaseTool):
    """
    Creates flowcharts and diagrams from descriptions or data.

    Analyzes query and context to determine diagram type,
    then generates appropriate visualization code.
    """

    # Keywords for diagram type detection
    DIAGRAM_KEYWORDS = {
        DiagramType.FLOWCHART: ['flowchart', 'process', 'flow', 'workflow', 'steps', 'procedure'],
        DiagramType.SEQUENCE: ['sequence', 'interaction', 'message', 'api call', 'request'],
        DiagramType.STATE: ['state', 'status', 'lifecycle', 'transition', 'fsm'],
        DiagramType.ER: ['entity', 'relationship', 'database', 'schema', 'table'],
        DiagramType.MINDMAP: ['mindmap', 'brainstorm', 'ideas', 'concepts', 'hierarchy'],
        DiagramType.TIMELINE: ['timeline', 'history', 'chronology', 'events'],
        DiagramType.GANTT: ['gantt', 'schedule', 'project', 'timeline', 'tasks'],
        DiagramType.CLASS: ['class', 'uml', 'inheritance', 'object', 'oop'],
    }

    def __init__(self):
        super().__init__(
            name="flowchart",
            description="Creates flowcharts and diagrams from descriptions"
        )

    async def execute(self, context: 'AgentContext') -> ToolResult:
        """
        Generate a diagram based on query and context.
        """
        try:
            query = context.query.lower()

            # Determine diagram type
            diagram_type = self._detect_diagram_type(query)

            # Extract elements from context
            elements = self._extract_elements(context)

            if not elements:
                return ToolResult(
                    success=False,
                    data=None,
                    error="No diagram elements could be extracted from the context"
                )

            # Generate diagram code
            mermaid_code = self._generate_diagram(diagram_type, elements)
            dot_code = self._generate_dot(diagram_type, elements)
            ascii_art = self._generate_ascii(diagram_type, elements)

            return ToolResult(
                success=True,
                data={
                    "diagram_type": diagram_type.value,
                    "element_count": len(elements.get('nodes', [])),
                    "mermaid": mermaid_code,
                    "dot": dot_code,
                    "ascii": ascii_art,
                },
                metadata={
                    "diagram_type": diagram_type.value,
                    "format": "mermaid",
                }
            )

        except Exception as e:
            logger.error(f"Flowchart tool error: {e}")
            return ToolResult(
                success=False,
                data=None,
                error=str(e)
            )

    def _detect_diagram_type(self, query: str) -> DiagramType:
        """Determine the best diagram type based on query."""
        query_lower = query.lower()

        for diagram_type, keywords in self.DIAGRAM_KEYWORDS.items():
            for keyword in keywords:
                if keyword in query_lower:
                    return diagram_type

        # Default to flowchart
        return DiagramType.FLOWCHART

    def _extract_elements(self, context: 'AgentContext') -> dict:
        """Extract diagram elements from context."""
        nodes = []
        edges = []

        # Extract from retrieved chunks
        for chunk in context.retrieved_chunks:
            content = chunk.get('content', '')

            # Find step patterns: "Step 1: ...", "1. ...", "First, ..."
            step_patterns = [
                r'(?:Step\s*\d+[:\.]?\s*)(.+?)(?:\n|$)',
                r'(?:^\d+[\.)\s]+)(.+?)(?:\n|$)',
                r'(?:First|Then|Next|After|Finally)[,\s]+(.+?)(?:\n|$)',
            ]

            for pattern in step_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE | re.MULTILINE)
                for match in matches:
                    clean = match.strip()[:50]  # Limit length
                    if clean and len(clean) > 3:
                        nodes.append(clean)

            # Find decision patterns: "If ... then ..."
            decision_pattern = r'[Ii]f\s+(.+?)\s*(?:then|,)\s*(.+?)(?:\.|;|\n|$)'
            decisions = re.findall(decision_pattern, content)
            for condition, action in decisions:
                nodes.append(f"Is {condition.strip()[:30]}?")
                nodes.append(action.strip()[:30])
                edges.append({
                    'from': f"Is {condition.strip()[:30]}?",
                    'to': action.strip()[:30],
                    'label': 'Yes'
                })

            # Find arrow patterns: "A -> B", "A leads to B"
            arrow_patterns = [
                r'(.+?)\s*(?:->|→|leads to|results in)\s*(.+?)(?:\n|$)',
            ]

            for pattern in arrow_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                for source, target in matches:
                    source = source.strip()[:30]
                    target = target.strip()[:30]
                    if source and target:
                        if source not in nodes:
                            nodes.append(source)
                        if target not in nodes:
                            nodes.append(target)
                        edges.append({'from': source, 'to': target})

        # Remove duplicates while preserving order
        seen = set()
        unique_nodes = []
        for node in nodes:
            if node.lower() not in seen:
                seen.add(node.lower())
                unique_nodes.append(node)

        # If no edges, create sequential flow
        if not edges and len(unique_nodes) > 1:
            for i in range(len(unique_nodes) - 1):
                edges.append({
                    'from': unique_nodes[i],
                    'to': unique_nodes[i + 1]
                })

        return {
            'nodes': unique_nodes[:15],  # Limit nodes
            'edges': edges[:20],  # Limit edges
        }

    def _generate_diagram(self, diagram_type: DiagramType, elements: dict) -> str:
        """Generate Mermaid diagram code."""
        nodes = elements.get('nodes', [])
        edges = elements.get('edges', [])

        if diagram_type == DiagramType.FLOWCHART:
            return self._generate_flowchart_mermaid(nodes, edges)
        elif diagram_type == DiagramType.SEQUENCE:
            return self._generate_sequence_mermaid(nodes, edges)
        elif diagram_type == DiagramType.STATE:
            return self._generate_state_mermaid(nodes, edges)
        elif diagram_type == DiagramType.MINDMAP:
            return self._generate_mindmap_mermaid(nodes)
        else:
            return self._generate_flowchart_mermaid(nodes, edges)

    def _generate_flowchart_mermaid(self, nodes: List[str], edges: List[dict]) -> str:
        """Generate Mermaid flowchart."""
        lines = ["flowchart TD"]

        # Create node ID mapping
        node_ids = {}
        for i, node in enumerate(nodes):
            node_id = f"N{i}"
            node_ids[node] = node_id

            # Determine shape based on content
            if '?' in node:
                # Decision diamond
                lines.append(f"    {node_id}{{{node}}}")
            elif any(word in node.lower() for word in ['start', 'begin', 'end', 'finish']):
                # Terminal rounded
                lines.append(f"    {node_id}([{node}])")
            else:
                # Regular rectangle
                lines.append(f"    {node_id}[{node}]")

        # Add edges
        for edge in edges:
            from_id = node_ids.get(edge['from'])
            to_id = node_ids.get(edge['to'])
            if from_id and to_id:
                label = edge.get('label', '')
                if label:
                    lines.append(f"    {from_id} -->|{label}| {to_id}")
                else:
                    lines.append(f"    {from_id} --> {to_id}")

        return "\n".join(lines)

    def _generate_sequence_mermaid(self, nodes: List[str], edges: List[dict]) -> str:
        """Generate Mermaid sequence diagram."""
        lines = ["sequenceDiagram"]

        # Use nodes as participants
        for node in nodes[:6]:  # Limit participants
            clean_name = re.sub(r'[^a-zA-Z0-9]', '', node)
            lines.append(f"    participant {clean_name} as {node}")

        # Add interactions
        for edge in edges:
            from_name = re.sub(r'[^a-zA-Z0-9]', '', edge['from'])
            to_name = re.sub(r'[^a-zA-Z0-9]', '', edge['to'])
            label = edge.get('label', 'request')
            lines.append(f"    {from_name}->>+{to_name}: {label}")
            lines.append(f"    {to_name}-->>-{from_name}: response")

        return "\n".join(lines)

    def _generate_state_mermaid(self, nodes: List[str], edges: List[dict]) -> str:
        """Generate Mermaid state diagram."""
        lines = ["stateDiagram-v2"]

        if nodes:
            lines.append(f"    [*] --> {self._clean_state(nodes[0])}")

        for edge in edges:
            from_state = self._clean_state(edge['from'])
            to_state = self._clean_state(edge['to'])
            lines.append(f"    {from_state} --> {to_state}")

        if nodes:
            lines.append(f"    {self._clean_state(nodes[-1])} --> [*]")

        return "\n".join(lines)

    def _generate_mindmap_mermaid(self, nodes: List[str]) -> str:
        """Generate Mermaid mindmap."""
        if not nodes:
            return "mindmap\n    root((Topic))"

        lines = ["mindmap"]
        lines.append(f"    root(({nodes[0]}))")

        for node in nodes[1:]:
            lines.append(f"        {node}")

        return "\n".join(lines)

    def _generate_dot(self, diagram_type: DiagramType, elements: dict) -> str:
        """Generate Graphviz DOT code."""
        nodes = elements.get('nodes', [])
        edges = elements.get('edges', [])

        lines = [
            "digraph G {",
            "    rankdir=TB;",
            "    node [shape=box, style=rounded];",
        ]

        # Create node ID mapping
        node_ids = {}
        for i, node in enumerate(nodes):
            node_id = f"n{i}"
            node_ids[node] = node_id

            shape = "diamond" if '?' in node else "box"
            lines.append(f'    {node_id} [label="{node}", shape={shape}];')

        # Add edges
        for edge in edges:
            from_id = node_ids.get(edge['from'])
            to_id = node_ids.get(edge['to'])
            if from_id and to_id:
                label = edge.get('label', '')
                if label:
                    lines.append(f'    {from_id} -> {to_id} [label="{label}"];')
                else:
                    lines.append(f"    {from_id} -> {to_id};")

        lines.append("}")
        return "\n".join(lines)

    def _generate_ascii(self, diagram_type: DiagramType, elements: dict) -> str:
        """Generate ASCII art diagram."""
        nodes = elements.get('nodes', [])
        edges = elements.get('edges', [])

        if not nodes:
            return "No diagram elements found"

        lines = []

        for i, node in enumerate(nodes):
            # Calculate box width
            width = max(len(node) + 4, 20)

            # Draw box
            lines.append("+" + "-" * (width - 2) + "+")
            padding = (width - 2 - len(node)) // 2
            lines.append("|" + " " * padding + node + " " * (width - 2 - padding - len(node)) + "|")
            lines.append("+" + "-" * (width - 2) + "+")

            # Draw arrow if not last node
            if i < len(nodes) - 1:
                lines.append(" " * (width // 2 - 1) + "|")
                lines.append(" " * (width // 2 - 1) + "v")

        return "\n".join(lines)

    def _clean_state(self, text: str) -> str:
        """Clean text for use as state name."""
        return re.sub(r'[^a-zA-Z0-9_]', '_', text)[:20]

    def format_for_response(self, result: ToolResult) -> str:
        """Format diagram result for inclusion in response."""
        if not result.success:
            return f"Diagram error: {result.error}"

        data = result.data
        diagram_type = data.get("diagram_type", "flowchart")
        element_count = data.get("element_count", 0)

        mermaid = data.get("mermaid", "")

        return f"""**{diagram_type.title()} Diagram** ({element_count} elements)

```mermaid
{mermaid}
```

You can paste this code into any Mermaid-compatible viewer to see the diagram."""


# =============================================================================
# DIAGRAM BUILDER (Standalone use)
# =============================================================================

class DiagramBuilder:
    """
    Standalone diagram builder for programmatic use.

    Example:
        builder = DiagramBuilder()
        builder.add_node("Start", shape="circle")
        builder.add_node("Process", shape="box")
        builder.add_edge("Start", "Process")
        print(builder.to_mermaid())
    """

    def __init__(self, title: str = "Diagram"):
        self.title = title
        self.nodes = []
        self.edges = []
        self.node_shapes = {}

    def add_node(
        self,
        name: str,
        label: Optional[str] = None,
        shape: str = "box"
    ) -> 'DiagramBuilder':
        """Add a node to the diagram."""
        self.nodes.append(name)
        self.node_shapes[name] = {
            'label': label or name,
            'shape': shape
        }
        return self

    def add_edge(
        self,
        source: str,
        target: str,
        label: Optional[str] = None
    ) -> 'DiagramBuilder':
        """Add an edge between nodes."""
        self.edges.append({
            'from': source,
            'to': target,
            'label': label
        })
        # Auto-add nodes if not exists
        if source not in self.nodes:
            self.nodes.append(source)
        if target not in self.nodes:
            self.nodes.append(target)
        return self

    def to_mermaid(self, direction: str = "TD") -> str:
        """Generate Mermaid code."""
        lines = [f"flowchart {direction}"]

        # Add nodes
        for i, node in enumerate(self.nodes):
            node_id = f"N{i}"
            info = self.node_shapes.get(node, {'label': node, 'shape': 'box'})
            label = info['label']
            shape = info['shape']

            if shape == 'circle':
                lines.append(f"    {node_id}(({label}))")
            elif shape == 'diamond':
                lines.append(f"    {node_id}{{{label}}}")
            elif shape == 'rounded':
                lines.append(f"    {node_id}([{label}])")
            else:
                lines.append(f"    {node_id}[{label}]")

        # Create mapping for edges
        node_ids = {node: f"N{i}" for i, node in enumerate(self.nodes)}

        # Add edges
        for edge in self.edges:
            from_id = node_ids.get(edge['from'])
            to_id = node_ids.get(edge['to'])
            if from_id and to_id:
                if edge.get('label'):
                    lines.append(f"    {from_id} -->|{edge['label']}| {to_id}")
                else:
                    lines.append(f"    {from_id} --> {to_id}")

        return "\n".join(lines)

    def to_dot(self) -> str:
        """Generate Graphviz DOT code."""
        lines = [
            "digraph G {",
            f'    label="{self.title}";',
            "    rankdir=TB;",
        ]

        # Add nodes
        node_ids = {}
        for i, node in enumerate(self.nodes):
            node_id = f"n{i}"
            node_ids[node] = node_id
            info = self.node_shapes.get(node, {'label': node, 'shape': 'box'})
            lines.append(f'    {node_id} [label="{info["label"]}", shape={info["shape"]}];')

        # Add edges
        for edge in self.edges:
            from_id = node_ids.get(edge['from'])
            to_id = node_ids.get(edge['to'])
            if from_id and to_id:
                if edge.get('label'):
                    lines.append(f'    {from_id} -> {to_id} [label="{edge["label"]}"];')
                else:
                    lines.append(f"    {from_id} -> {to_id};")

        lines.append("}")
        return "\n".join(lines)


__all__ = [
    'FlowchartTool',
    'DiagramBuilder',
    'DiagramType',
    'OutputFormat',
]
