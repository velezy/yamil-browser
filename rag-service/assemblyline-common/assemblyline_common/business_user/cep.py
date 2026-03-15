"""
Complex Event Processing (CEP) for Logic Weaver.

Provides real-time event processing:
- Event patterns and sequences
- Time and count windows
- Aggregations
- Real-time dashboard metrics

This enables business users to define complex event patterns
and monitor real-time data streams.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from collections import deque
from datetime import datetime, timedelta
import json
import hashlib
import time


# =============================================================================
# Events
# =============================================================================


@dataclass
class Event:
    """An event in the event stream."""

    event_type: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_id: Optional[str] = None
    source: Optional[str] = None
    tenant_id: Optional[str] = None
    correlation_id: Optional[str] = None

    def __post_init__(self):
        if not self.event_id:
            self.event_id = hashlib.md5(
                f"{self.event_type}:{self.timestamp.isoformat()}:{json.dumps(self.data, sort_keys=True, default=str)}".encode()
            ).hexdigest()[:16]

    def matches_filter(self, filters: Dict[str, Any]) -> bool:
        """Check if event matches filters."""
        for key, value in filters.items():
            if key == "event_type":
                if self.event_type != value:
                    return False
            elif key.startswith("data."):
                field_path = key[5:]
                field_value = self._get_field(field_path)
                if field_value != value:
                    return False
            elif key in self.data:
                if self.data[key] != value:
                    return False
        return True

    def _get_field(self, path: str) -> Any:
        """Get nested field from data."""
        parts = path.split(".")
        current = self.data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current


# =============================================================================
# Windows
# =============================================================================


class Window(ABC):
    """Base class for event windows."""

    @abstractmethod
    def add(self, event: Event) -> None:
        """Add event to window."""
        pass

    @abstractmethod
    def get_events(self) -> List[Event]:
        """Get all events in window."""
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear window."""
        pass


@dataclass
class TimeWindow(Window):
    """
    Time-based sliding window.

    Keeps events within a time duration.
    """

    duration: timedelta
    events: deque = field(default_factory=deque)

    def __post_init__(self):
        if isinstance(self.duration, (int, float)):
            self.duration = timedelta(seconds=self.duration)

    def add(self, event: Event) -> None:
        """Add event and expire old ones."""
        self._expire()
        self.events.append(event)

    def get_events(self) -> List[Event]:
        """Get current events in window."""
        self._expire()
        return list(self.events)

    def clear(self) -> None:
        """Clear all events."""
        self.events.clear()

    def _expire(self) -> None:
        """Remove expired events."""
        cutoff = datetime.utcnow() - self.duration
        while self.events and self.events[0].timestamp < cutoff:
            self.events.popleft()


@dataclass
class CountWindow(Window):
    """
    Count-based sliding window.

    Keeps the last N events.
    """

    max_count: int
    events: deque = field(default_factory=deque)

    def add(self, event: Event) -> None:
        """Add event and remove oldest if over limit."""
        self.events.append(event)
        while len(self.events) > self.max_count:
            self.events.popleft()

    def get_events(self) -> List[Event]:
        """Get current events in window."""
        return list(self.events)

    def clear(self) -> None:
        """Clear all events."""
        self.events.clear()


@dataclass
class SessionWindow(Window):
    """
    Session-based window.

    Groups events by session with timeout gap.
    """

    gap: timedelta
    session_key: str  # Field to use for session grouping
    sessions: Dict[str, List[Event]] = field(default_factory=dict)
    last_event_time: Dict[str, datetime] = field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.gap, (int, float)):
            self.gap = timedelta(seconds=self.gap)

    def add(self, event: Event) -> None:
        """Add event to appropriate session."""
        key = event.data.get(self.session_key, "default")
        now = datetime.utcnow()

        # Check if session expired
        if key in self.last_event_time:
            if now - self.last_event_time[key] > self.gap:
                # Session expired, start new one
                del self.sessions[key]

        if key not in self.sessions:
            self.sessions[key] = []

        self.sessions[key].append(event)
        self.last_event_time[key] = now

    def get_events(self, session_key: Optional[str] = None) -> List[Event]:
        """Get events, optionally for a specific session."""
        if session_key:
            return self.sessions.get(session_key, [])

        all_events = []
        for events in self.sessions.values():
            all_events.extend(events)
        return all_events

    def get_sessions(self) -> Dict[str, List[Event]]:
        """Get all sessions."""
        return dict(self.sessions)

    def clear(self) -> None:
        """Clear all sessions."""
        self.sessions.clear()
        self.last_event_time.clear()


# =============================================================================
# Aggregations
# =============================================================================


class AggregationType(Enum):
    """Types of aggregations."""
    COUNT = "count"
    SUM = "sum"
    AVERAGE = "average"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT_COUNT = "distinct_count"
    PERCENTILE = "percentile"


@dataclass
class Aggregation:
    """An aggregation over events."""

    type: AggregationType
    field: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)

    def compute(self, events: List[Event]) -> Any:
        """Compute aggregation over events."""
        if not events:
            return None

        values = []
        if self.field:
            for event in events:
                value = event.data.get(self.field)
                if value is not None:
                    values.append(value)
        else:
            values = events

        if not values:
            return None

        t = self.type

        if t == AggregationType.COUNT:
            return len(values)

        elif t == AggregationType.SUM:
            return sum(float(v) for v in values if isinstance(v, (int, float)))

        elif t == AggregationType.AVERAGE:
            numeric = [float(v) for v in values if isinstance(v, (int, float))]
            return sum(numeric) / len(numeric) if numeric else None

        elif t == AggregationType.MIN:
            return min(values)

        elif t == AggregationType.MAX:
            return max(values)

        elif t == AggregationType.FIRST:
            return values[0] if values else None

        elif t == AggregationType.LAST:
            return values[-1] if values else None

        elif t == AggregationType.DISTINCT_COUNT:
            try:
                return len(set(json.dumps(v, sort_keys=True, default=str) for v in values))
            except (TypeError, ValueError):
                return len(values)

        elif t == AggregationType.PERCENTILE:
            percentile = self.parameters.get("percentile", 50)
            numeric = sorted([float(v) for v in values if isinstance(v, (int, float))])
            if not numeric:
                return None
            idx = int(len(numeric) * percentile / 100)
            return numeric[min(idx, len(numeric) - 1)]

        return None


# =============================================================================
# Event Patterns
# =============================================================================


@dataclass
class EventPattern:
    """A pattern to match against events."""

    event_type: str
    filters: Dict[str, Any] = field(default_factory=dict)
    alias: Optional[str] = None
    optional: bool = False

    def matches(self, event: Event) -> bool:
        """Check if event matches pattern."""
        if event.event_type != self.event_type:
            return False
        return event.matches_filter(self.filters)


@dataclass
class SequencePattern:
    """A sequence of event patterns."""

    patterns: List[EventPattern]
    within: Optional[timedelta] = None
    strict: bool = False  # If True, no other events allowed between

    def __post_init__(self):
        if isinstance(self.within, (int, float)):
            self.within = timedelta(seconds=self.within)

    def find_matches(self, events: List[Event]) -> List[List[Event]]:
        """
        Find all matches of the sequence in events.

        Returns list of matched event sequences.
        """
        if not self.patterns or not events:
            return []

        matches = []
        sorted_events = sorted(events, key=lambda e: e.timestamp)

        # Find all matches for first pattern
        for i, event in enumerate(sorted_events):
            if self.patterns[0].matches(event):
                # Try to extend from this starting point
                matched = self._extend_match([event], sorted_events[i + 1:], 1)
                matches.extend(matched)

        return matches

    def _extend_match(
        self,
        current_match: List[Event],
        remaining_events: List[Event],
        pattern_idx: int
    ) -> List[List[Event]]:
        """Recursively extend a match."""
        if pattern_idx >= len(self.patterns):
            return [current_match]

        pattern = self.patterns[pattern_idx]
        start_time = current_match[0].timestamp
        matches = []

        for i, event in enumerate(remaining_events):
            # Check time window
            if self.within:
                if event.timestamp - start_time > self.within:
                    break

            # Check strict ordering (no other events between)
            if self.strict and i > 0:
                break

            if pattern.matches(event):
                new_match = current_match + [event]
                extended = self._extend_match(
                    new_match,
                    remaining_events[i + 1:],
                    pattern_idx + 1
                )
                matches.extend(extended)

                if pattern_idx == len(self.patterns) - 1:
                    # Found complete match, continue looking for more
                    continue

            elif pattern.optional:
                # Try skipping optional pattern
                extended = self._extend_match(
                    current_match,
                    remaining_events[i:],
                    pattern_idx + 1
                )
                matches.extend(extended)

        # Handle optional final patterns
        if pattern.optional and pattern_idx == len(self.patterns) - 1:
            matches.append(current_match)

        return matches


# =============================================================================
# Event Stream
# =============================================================================


class EventStream:
    """
    A stream of events with windowing and processing.
    """

    def __init__(
        self,
        stream_id: str,
        window: Optional[Window] = None
    ):
        """Initialize event stream."""
        self.stream_id = stream_id
        self.window = window or TimeWindow(timedelta(hours=1))
        self._handlers: List[Callable[[Event], None]] = []
        self._event_count = 0

    def emit(self, event: Event) -> None:
        """Emit an event to the stream."""
        self.window.add(event)
        self._event_count += 1

        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                pass  # Log in production

    def on_event(self, handler: Callable[[Event], None]) -> None:
        """Register event handler."""
        self._handlers.append(handler)

    def query(
        self,
        event_type: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Event]:
        """Query events in window."""
        events = self.window.get_events()

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if filters:
            events = [e for e in events if e.matches_filter(filters)]

        return events

    def aggregate(
        self,
        aggregation: Aggregation,
        event_type: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Compute aggregation over events."""
        events = self.query(event_type, filters)
        return aggregation.compute(events)

    def find_pattern(
        self,
        pattern: Union[EventPattern, SequencePattern]
    ) -> List[Union[Event, List[Event]]]:
        """Find events matching pattern."""
        events = self.window.get_events()

        if isinstance(pattern, EventPattern):
            return [e for e in events if pattern.matches(e)]
        else:
            return pattern.find_matches(events)

    @property
    def event_count(self) -> int:
        """Total events emitted."""
        return self._event_count


# =============================================================================
# CEP Rules
# =============================================================================


@dataclass
class CEPRule:
    """A CEP rule for pattern detection."""

    id: str
    name: str
    pattern: Union[EventPattern, SequencePattern]
    condition: Optional[str] = None  # Additional condition expression
    action: str = "emit"  # emit, alert, aggregate
    action_params: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    # Aggregation for action
    aggregation: Optional[Aggregation] = None

    # Alert configuration
    alert_threshold: Optional[int] = None
    alert_window: Optional[timedelta] = None

    # State tracking
    match_count: int = 0
    last_match: Optional[datetime] = None

    def __post_init__(self):
        if isinstance(self.alert_window, (int, float)):
            self.alert_window = timedelta(seconds=self.alert_window)


@dataclass
class CEPResult:
    """Result of CEP evaluation."""

    rule_id: str
    rule_name: str
    matched: bool
    match_count: int = 0
    matched_events: List[Event] = field(default_factory=list)
    aggregation_result: Any = None
    alert_triggered: bool = False
    timestamp: datetime = field(default_factory=datetime.utcnow)


# =============================================================================
# CEP Engine
# =============================================================================


class CEPEngine:
    """
    Complex Event Processing Engine.

    Processes event streams and evaluates CEP rules.
    """

    def __init__(self):
        """Initialize engine."""
        self._streams: Dict[str, EventStream] = {}
        self._rules: Dict[str, CEPRule] = {}
        self._alert_counts: Dict[str, List[datetime]] = {}

    def create_stream(
        self,
        stream_id: str,
        window: Optional[Window] = None
    ) -> EventStream:
        """Create an event stream."""
        stream = EventStream(stream_id, window)
        self._streams[stream_id] = stream
        return stream

    def get_stream(self, stream_id: str) -> Optional[EventStream]:
        """Get a stream by ID."""
        return self._streams.get(stream_id)

    def register_rule(self, rule: CEPRule) -> None:
        """Register a CEP rule."""
        self._rules[rule.id] = rule

    def get_rule(self, rule_id: str) -> Optional[CEPRule]:
        """Get a rule by ID."""
        return self._rules.get(rule_id)

    def emit(
        self,
        stream_id: str,
        event: Event
    ) -> List[CEPResult]:
        """
        Emit event and evaluate rules.

        Returns results for any matched rules.
        """
        stream = self.get_stream(stream_id)
        if not stream:
            return []

        stream.emit(event)
        return self.evaluate_rules(stream)

    def evaluate_rules(self, stream: EventStream) -> List[CEPResult]:
        """Evaluate all rules against stream."""
        results = []

        for rule in self._rules.values():
            if not rule.enabled:
                continue

            result = self._evaluate_rule(rule, stream)
            if result.matched:
                results.append(result)

        return results

    def _evaluate_rule(
        self,
        rule: CEPRule,
        stream: EventStream
    ) -> CEPResult:
        """Evaluate a single rule."""
        result = CEPResult(
            rule_id=rule.id,
            rule_name=rule.name,
            matched=False
        )

        # Find matches
        matches = stream.find_pattern(rule.pattern)

        if not matches:
            return result

        result.matched = True
        result.match_count = len(matches)

        # Flatten matched events
        for match in matches:
            if isinstance(match, list):
                result.matched_events.extend(match)
            else:
                result.matched_events.append(match)

        # Update rule state
        rule.match_count += len(matches)
        rule.last_match = datetime.utcnow()

        # Compute aggregation if configured
        if rule.aggregation:
            result.aggregation_result = rule.aggregation.compute(
                result.matched_events
            )

        # Check alert threshold
        if rule.alert_threshold and rule.alert_window:
            result.alert_triggered = self._check_alert(rule)

        return result

    def _check_alert(self, rule: CEPRule) -> bool:
        """Check if alert threshold is exceeded."""
        now = datetime.utcnow()

        # Get or create alert count list
        if rule.id not in self._alert_counts:
            self._alert_counts[rule.id] = []

        counts = self._alert_counts[rule.id]

        # Add current match
        counts.append(now)

        # Remove old counts
        cutoff = now - rule.alert_window
        counts[:] = [t for t in counts if t >= cutoff]

        return len(counts) >= rule.alert_threshold


# =============================================================================
# Dashboard Metrics
# =============================================================================


class MetricType(Enum):
    """Types of dashboard metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    RATE = "rate"
    HISTOGRAM = "histogram"
    TREND = "trend"


@dataclass
class DashboardMetric:
    """A metric for real-time dashboards."""

    id: str
    name: str
    metric_type: MetricType
    stream_id: str
    aggregation: Aggregation
    event_type: Optional[str] = None
    filters: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None

    # Display configuration
    unit: Optional[str] = None
    precision: int = 2
    color: Optional[str] = None

    # Thresholds for alerting
    warning_threshold: Optional[float] = None
    critical_threshold: Optional[float] = None

    # Refresh configuration
    refresh_interval_seconds: int = 5

    def compute(self, stream: EventStream) -> Dict[str, Any]:
        """Compute metric value."""
        events = stream.query(self.event_type, self.filters)
        value = self.aggregation.compute(events)

        status = "normal"
        if self.critical_threshold and value is not None:
            if value >= self.critical_threshold:
                status = "critical"
            elif self.warning_threshold and value >= self.warning_threshold:
                status = "warning"

        return {
            "id": self.id,
            "name": self.name,
            "value": round(value, self.precision) if isinstance(value, float) else value,
            "unit": self.unit,
            "status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "eventCount": len(events)
        }


class WidgetType(Enum):
    """Types of dashboard widgets."""
    NUMBER = "number"
    GAUGE = "gauge"
    CHART = "chart"
    TABLE = "table"
    MAP = "map"
    ALERT = "alert"


@dataclass
class DashboardWidget:
    """A widget for real-time dashboards."""

    id: str
    name: str
    widget_type: WidgetType
    metrics: List[DashboardMetric] = field(default_factory=list)
    description: Optional[str] = None

    # Layout
    width: int = 1  # Grid units
    height: int = 1
    position_x: int = 0
    position_y: int = 0

    # Chart configuration (for chart type)
    chart_type: str = "line"  # line, bar, area, pie
    time_range_minutes: int = 60

    # Display options
    show_legend: bool = True
    show_grid: bool = True
    animation: bool = True

    def render(self, streams: Dict[str, EventStream]) -> Dict[str, Any]:
        """Render widget data."""
        metric_data = []
        for metric in self.metrics:
            stream = streams.get(metric.stream_id)
            if stream:
                metric_data.append(metric.compute(stream))

        return {
            "id": self.id,
            "name": self.name,
            "type": self.widget_type.value,
            "metrics": metric_data,
            "layout": {
                "width": self.width,
                "height": self.height,
                "x": self.position_x,
                "y": self.position_y
            },
            "config": {
                "chartType": self.chart_type,
                "timeRange": self.time_range_minutes,
                "showLegend": self.show_legend,
                "showGrid": self.show_grid
            }
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Events
    "Event",
    "EventPattern",
    "SequencePattern",
    "EventStream",
    # Windows
    "Window",
    "TimeWindow",
    "CountWindow",
    "SessionWindow",
    # Aggregations
    "Aggregation",
    "AggregationType",
    # Engine
    "CEPEngine",
    "CEPRule",
    "CEPResult",
    # Dashboard
    "DashboardMetric",
    "DashboardWidget",
    "MetricType",
    "WidgetType",
]
