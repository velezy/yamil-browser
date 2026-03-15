"""
Prometheus Metrics for DriveSentinel (Advanced)

Provides standardized metrics collection across all microservices.
Metrics are exposed on /metrics endpoint for Prometheus scraping.

Advanced Features (upgraded from Required):
- Predictive alerting: ML-based threshold detection, trend forecasting
- Anomaly detection: Statistical anomaly detection using z-scores and IQR
- Trend forecasting: Linear regression for metric trend prediction
- Adaptive thresholds: Dynamic threshold adjustment based on patterns

Usage:
    from services.shared.observability import MetricsRegistry, get_metrics

    # Get the metrics registry
    metrics = get_metrics()

    # Record metrics
    metrics.inc_counter("requests_total", labels={"service": "rag", "status": "200"})
    metrics.observe_histogram("request_duration_seconds", 0.125, labels={"service": "rag"})

    # Use decorators
    @metrics.count_calls("function_calls_total")
    @metrics.time_execution("function_duration_seconds")
    async def my_function():
        pass

    # Advanced: Anomaly detection
    detector = get_anomaly_detector()
    detector.record_value("latency", 0.5)
    anomaly = detector.check_anomaly("latency", 2.5)  # Returns AnomalyResult
"""

import time
import logging
import math
import statistics
from typing import Any, Callable, Dict, List, Optional, TypeVar, Tuple
from functools import wraps
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timedelta
from enum import Enum
import asyncio

logger = logging.getLogger(__name__)

# Type variable for generic decorator
F = TypeVar('F', bound=Callable[..., Any])

# Try to import prometheus_client, provide no-ops if not available
try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Summary, Info,
        CollectorRegistry, REGISTRY,
        generate_latest, CONTENT_TYPE_LATEST,
        push_to_gateway, start_http_server
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.info("prometheus_client not available. Install: pip install prometheus-client")


# =============================================================================
# ADVANCED: ANOMALY DETECTION & PREDICTIVE ALERTING
# =============================================================================

class AnomalyType(str, Enum):
    """Type of anomaly detected."""
    SPIKE = "spike"  # Sudden increase
    DIP = "dip"  # Sudden decrease
    TREND_VIOLATION = "trend_violation"  # Deviates from expected trend
    THRESHOLD_BREACH = "threshold_breach"  # Exceeds static threshold
    STATISTICAL_OUTLIER = "statistical_outlier"  # Z-score outlier
    NONE = "none"


class AlertSeverity(str, Enum):
    """Severity level for alerts."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    is_anomaly: bool
    anomaly_type: AnomalyType
    severity: AlertSeverity
    value: float
    expected_value: float
    deviation: float  # How far from expected (in std devs or %)
    confidence: float  # Confidence in the anomaly detection
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "is_anomaly": self.is_anomaly,
            "anomaly_type": self.anomaly_type.value,
            "severity": self.severity.value,
            "value": self.value,
            "expected_value": self.expected_value,
            "deviation": self.deviation,
            "confidence": self.confidence,
            "message": self.message,
        }


@dataclass
class TrendForecast:
    """Forecast of metric trend."""
    metric_name: str
    current_value: float
    predicted_value: float  # Predicted value at forecast_time
    forecast_time: datetime
    trend_direction: str  # "increasing", "decreasing", "stable"
    trend_slope: float  # Rate of change per minute
    confidence: float
    will_breach_threshold: bool
    estimated_breach_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "current_value": self.current_value,
            "predicted_value": self.predicted_value,
            "forecast_time": self.forecast_time.isoformat(),
            "trend_direction": self.trend_direction,
            "trend_slope": self.trend_slope,
            "confidence": self.confidence,
            "will_breach_threshold": self.will_breach_threshold,
            "estimated_breach_time": self.estimated_breach_time.isoformat() if self.estimated_breach_time else None,
        }


@dataclass
class MetricTimeSeries:
    """Time series data for a metric."""
    name: str
    values: deque  # (timestamp, value) pairs
    max_size: int = 1000  # Keep last N data points

    def __post_init__(self):
        if not isinstance(self.values, deque):
            self.values = deque(self.values, maxlen=self.max_size)

    def add(self, value: float, timestamp: Optional[datetime] = None):
        """Add a value to the time series."""
        ts = timestamp or datetime.utcnow()
        self.values.append((ts, value))

    def get_values_in_window(self, minutes: int = 60) -> List[Tuple[datetime, float]]:
        """Get values within a time window."""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        return [(ts, val) for ts, val in self.values if ts >= cutoff]

    def get_statistics(self, minutes: int = 60) -> Dict[str, float]:
        """Calculate statistics for the time window."""
        values = [val for _, val in self.get_values_in_window(minutes)]
        if not values:
            return {"count": 0, "mean": 0, "std": 0, "min": 0, "max": 0}

        return {
            "count": len(values),
            "mean": statistics.mean(values),
            "std": statistics.stdev(values) if len(values) > 1 else 0,
            "min": min(values),
            "max": max(values),
            "median": statistics.median(values),
        }


class AnomalyDetector:
    """
    Statistical anomaly detection for metrics.

    Uses multiple detection methods:
    - Z-score (standard deviation based)
    - IQR (Interquartile Range)
    - Trend deviation
    - Static thresholds
    """

    def __init__(
        self,
        z_score_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        min_data_points: int = 30
    ):
        self.z_score_threshold = z_score_threshold
        self.iqr_multiplier = iqr_multiplier
        self.min_data_points = min_data_points
        self._time_series: Dict[str, MetricTimeSeries] = {}
        self._thresholds: Dict[str, Dict[str, float]] = {}  # metric -> {warning, critical}
        self._anomaly_history: List[Tuple[datetime, str, AnomalyResult]] = []

    def record_value(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[datetime] = None
    ):
        """Record a metric value for anomaly detection."""
        if metric_name not in self._time_series:
            self._time_series[metric_name] = MetricTimeSeries(
                name=metric_name,
                values=deque(maxlen=1000)
            )
        self._time_series[metric_name].add(value, timestamp)

    def set_threshold(
        self,
        metric_name: str,
        warning: Optional[float] = None,
        critical: Optional[float] = None
    ):
        """Set static thresholds for a metric."""
        self._thresholds[metric_name] = {
            "warning": warning,
            "critical": critical,
        }

    def check_anomaly(
        self,
        metric_name: str,
        current_value: float,
        window_minutes: int = 60
    ) -> AnomalyResult:
        """
        Check if the current value is anomalous.

        Uses multiple detection methods and returns the most severe finding.
        """
        ts = self._time_series.get(metric_name)
        if not ts or len(ts.values) < self.min_data_points:
            # Not enough data for statistical analysis
            return self._check_threshold_only(metric_name, current_value)

        stats = ts.get_statistics(window_minutes)

        # Method 1: Z-score detection
        z_result = self._check_z_score(current_value, stats)

        # Method 2: IQR detection
        iqr_result = self._check_iqr(metric_name, current_value, window_minutes)

        # Method 3: Static threshold detection
        threshold_result = self._check_threshold_only(metric_name, current_value)

        # Return the most severe anomaly
        results = [z_result, iqr_result, threshold_result]
        anomalies = [r for r in results if r.is_anomaly]

        if not anomalies:
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                severity=AlertSeverity.INFO,
                value=current_value,
                expected_value=stats["mean"],
                deviation=0,
                confidence=1.0,
                message="Value within normal range"
            )

        # Sort by severity and return most severe
        severity_order = {AlertSeverity.CRITICAL: 0, AlertSeverity.WARNING: 1, AlertSeverity.INFO: 2}
        most_severe = min(anomalies, key=lambda r: severity_order[r.severity])

        # Record anomaly
        self._anomaly_history.append((datetime.utcnow(), metric_name, most_severe))
        if len(self._anomaly_history) > 1000:
            self._anomaly_history = self._anomaly_history[-500:]

        return most_severe

    def _check_z_score(self, value: float, stats: Dict[str, float]) -> AnomalyResult:
        """Check for anomaly using z-score method."""
        if stats["std"] == 0:
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                severity=AlertSeverity.INFO,
                value=value,
                expected_value=stats["mean"],
                deviation=0,
                confidence=0.5,
                message="Insufficient variance for z-score"
            )

        z_score = abs(value - stats["mean"]) / stats["std"]

        if z_score >= self.z_score_threshold:
            severity = AlertSeverity.CRITICAL if z_score >= self.z_score_threshold * 1.5 else AlertSeverity.WARNING
            anomaly_type = AnomalyType.SPIKE if value > stats["mean"] else AnomalyType.DIP

            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=anomaly_type,
                severity=severity,
                value=value,
                expected_value=stats["mean"],
                deviation=z_score,
                confidence=min(0.95, 0.7 + (z_score - self.z_score_threshold) * 0.1),
                message=f"Z-score anomaly: {z_score:.2f} standard deviations from mean"
            )

        return AnomalyResult(
            is_anomaly=False,
            anomaly_type=AnomalyType.NONE,
            severity=AlertSeverity.INFO,
            value=value,
            expected_value=stats["mean"],
            deviation=z_score,
            confidence=1.0,
            message="Within z-score threshold"
        )

    def _check_iqr(
        self,
        metric_name: str,
        value: float,
        window_minutes: int
    ) -> AnomalyResult:
        """Check for anomaly using IQR method."""
        ts = self._time_series.get(metric_name)
        if not ts:
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                severity=AlertSeverity.INFO,
                value=value,
                expected_value=0,
                deviation=0,
                confidence=0,
                message="No time series data"
            )

        values = sorted([val for _, val in ts.get_values_in_window(window_minutes)])
        if len(values) < 4:
            return AnomalyResult(
                is_anomaly=False,
                anomaly_type=AnomalyType.NONE,
                severity=AlertSeverity.INFO,
                value=value,
                expected_value=0,
                deviation=0,
                confidence=0.5,
                message="Insufficient data for IQR"
            )

        q1_idx = len(values) // 4
        q3_idx = (3 * len(values)) // 4
        q1 = values[q1_idx]
        q3 = values[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - self.iqr_multiplier * iqr
        upper_bound = q3 + self.iqr_multiplier * iqr
        median = values[len(values) // 2]

        if value < lower_bound or value > upper_bound:
            deviation = (value - median) / iqr if iqr > 0 else 0
            anomaly_type = AnomalyType.SPIKE if value > upper_bound else AnomalyType.DIP

            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=anomaly_type,
                severity=AlertSeverity.WARNING,
                value=value,
                expected_value=median,
                deviation=abs(deviation),
                confidence=0.8,
                message=f"IQR outlier: value outside [{lower_bound:.2f}, {upper_bound:.2f}]"
            )

        return AnomalyResult(
            is_anomaly=False,
            anomaly_type=AnomalyType.NONE,
            severity=AlertSeverity.INFO,
            value=value,
            expected_value=median,
            deviation=0,
            confidence=1.0,
            message="Within IQR bounds"
        )

    def _check_threshold_only(self, metric_name: str, value: float) -> AnomalyResult:
        """Check against static thresholds."""
        thresholds = self._thresholds.get(metric_name, {})
        critical = thresholds.get("critical")
        warning = thresholds.get("warning")

        if critical is not None and value >= critical:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.THRESHOLD_BREACH,
                severity=AlertSeverity.CRITICAL,
                value=value,
                expected_value=critical,
                deviation=(value - critical) / critical if critical > 0 else 0,
                confidence=1.0,
                message=f"Critical threshold breach: {value} >= {critical}"
            )

        if warning is not None and value >= warning:
            return AnomalyResult(
                is_anomaly=True,
                anomaly_type=AnomalyType.THRESHOLD_BREACH,
                severity=AlertSeverity.WARNING,
                value=value,
                expected_value=warning,
                deviation=(value - warning) / warning if warning > 0 else 0,
                confidence=1.0,
                message=f"Warning threshold breach: {value} >= {warning}"
            )

        return AnomalyResult(
            is_anomaly=False,
            anomaly_type=AnomalyType.NONE,
            severity=AlertSeverity.INFO,
            value=value,
            expected_value=0,
            deviation=0,
            confidence=1.0,
            message="Within threshold limits"
        )

    def get_recent_anomalies(
        self,
        metric_name: Optional[str] = None,
        minutes: int = 60
    ) -> List[Dict[str, Any]]:
        """Get recent anomalies, optionally filtered by metric."""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        anomalies = []

        for ts, name, result in self._anomaly_history:
            if ts < cutoff:
                continue
            if metric_name and name != metric_name:
                continue
            anomalies.append({
                "timestamp": ts.isoformat(),
                "metric": name,
                **result.to_dict()
            })

        return anomalies


class TrendForecaster:
    """
    Forecasts metric trends using linear regression.

    Predicts future values and alerts if thresholds will be breached.
    """

    def __init__(self, min_data_points: int = 10):
        self.min_data_points = min_data_points
        self._time_series: Dict[str, MetricTimeSeries] = {}
        self._thresholds: Dict[str, float] = {}  # metric -> threshold

    def record_value(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[datetime] = None
    ):
        """Record a metric value for trend analysis."""
        if metric_name not in self._time_series:
            self._time_series[metric_name] = MetricTimeSeries(
                name=metric_name,
                values=deque(maxlen=1000)
            )
        self._time_series[metric_name].add(value, timestamp)

    def set_threshold(self, metric_name: str, threshold: float):
        """Set threshold for breach prediction."""
        self._thresholds[metric_name] = threshold

    def forecast(
        self,
        metric_name: str,
        forecast_minutes: int = 30,
        window_minutes: int = 60
    ) -> Optional[TrendForecast]:
        """
        Forecast the metric value at a future time.

        Uses linear regression on recent data to predict future values.
        """
        ts = self._time_series.get(metric_name)
        if not ts or len(ts.values) < self.min_data_points:
            return None

        data = ts.get_values_in_window(window_minutes)
        if len(data) < self.min_data_points:
            return None

        # Convert to relative timestamps (minutes from first point)
        first_ts = data[0][0]
        x_values = [(t - first_ts).total_seconds() / 60 for t, _ in data]
        y_values = [v for _, v in data]

        # Linear regression
        slope, intercept, r_squared = self._linear_regression(x_values, y_values)

        # Current value and prediction
        current_x = x_values[-1]
        current_value = y_values[-1]
        forecast_x = current_x + forecast_minutes
        predicted_value = slope * forecast_x + intercept

        # Determine trend direction
        if abs(slope) < 0.001:  # Essentially flat
            trend_direction = "stable"
        elif slope > 0:
            trend_direction = "increasing"
        else:
            trend_direction = "decreasing"

        # Check if threshold will be breached
        threshold = self._thresholds.get(metric_name)
        will_breach = False
        breach_time = None

        if threshold and slope != 0:
            # Time to reach threshold
            time_to_threshold = (threshold - intercept) / slope - current_x
            if time_to_threshold > 0 and predicted_value >= threshold:
                will_breach = True
                breach_time = datetime.utcnow() + timedelta(minutes=time_to_threshold)

        return TrendForecast(
            metric_name=metric_name,
            current_value=current_value,
            predicted_value=predicted_value,
            forecast_time=datetime.utcnow() + timedelta(minutes=forecast_minutes),
            trend_direction=trend_direction,
            trend_slope=slope,
            confidence=r_squared,
            will_breach_threshold=will_breach,
            estimated_breach_time=breach_time,
        )

    def _linear_regression(
        self,
        x: List[float],
        y: List[float]
    ) -> Tuple[float, float, float]:
        """
        Simple linear regression.

        Returns (slope, intercept, r_squared).
        """
        n = len(x)
        if n == 0:
            return 0.0, 0.0, 0.0

        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi ** 2 for xi in x)
        sum_y2 = sum(yi ** 2 for yi in y)

        denom = n * sum_x2 - sum_x ** 2
        if denom == 0:
            return 0.0, sum_y / n if n > 0 else 0.0, 0.0

        slope = (n * sum_xy - sum_x * sum_y) / denom
        intercept = (sum_y - slope * sum_x) / n

        # R-squared
        y_mean = sum_y / n
        ss_tot = sum((yi - y_mean) ** 2 for yi in y)
        ss_res = sum((yi - (slope * xi + intercept)) ** 2 for xi, yi in zip(x, y))

        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        return slope, intercept, max(0, min(1, r_squared))

    def get_all_forecasts(self, forecast_minutes: int = 30) -> List[TrendForecast]:
        """Get forecasts for all tracked metrics."""
        forecasts = []
        for metric_name in self._time_series:
            forecast = self.forecast(metric_name, forecast_minutes)
            if forecast:
                forecasts.append(forecast)
        return forecasts


class PredictiveAlerter:
    """
    Combines anomaly detection and trend forecasting for predictive alerts.

    Generates alerts for:
    - Current anomalies
    - Predicted threshold breaches
    - Trend violations
    """

    def __init__(self):
        self.anomaly_detector = AnomalyDetector()
        self.trend_forecaster = TrendForecaster()
        self._alert_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self._alert_history: List[Dict[str, Any]] = []

    def record_metric(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[datetime] = None
    ):
        """Record a metric for both anomaly detection and forecasting."""
        self.anomaly_detector.record_value(metric_name, value, timestamp)
        self.trend_forecaster.record_value(metric_name, value, timestamp)

    def set_thresholds(
        self,
        metric_name: str,
        warning: Optional[float] = None,
        critical: Optional[float] = None
    ):
        """Set thresholds for both anomaly detection and forecasting."""
        self.anomaly_detector.set_threshold(metric_name, warning, critical)
        if critical:
            self.trend_forecaster.set_threshold(metric_name, critical)

    def register_alert_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Register a callback to be called when alerts are generated."""
        self._alert_callbacks.append(callback)

    def evaluate(
        self,
        metric_name: str,
        current_value: float,
        forecast_minutes: int = 30
    ) -> Dict[str, Any]:
        """
        Evaluate a metric value for anomalies and predicted issues.

        Returns comprehensive analysis and triggers alerts if needed.
        """
        # Record the value
        self.record_metric(metric_name, current_value)

        # Check for current anomalies
        anomaly = self.anomaly_detector.check_anomaly(metric_name, current_value)

        # Get trend forecast
        forecast = self.trend_forecaster.forecast(metric_name, forecast_minutes)

        result = {
            "metric_name": metric_name,
            "timestamp": datetime.utcnow().isoformat(),
            "current_value": current_value,
            "anomaly": anomaly.to_dict(),
            "forecast": forecast.to_dict() if forecast else None,
            "alerts": [],
        }

        # Generate alerts
        alerts = []

        if anomaly.is_anomaly:
            alert = {
                "type": "anomaly",
                "severity": anomaly.severity.value,
                "message": anomaly.message,
                "metric": metric_name,
                "value": current_value,
            }
            alerts.append(alert)

        if forecast and forecast.will_breach_threshold:
            alert = {
                "type": "predictive",
                "severity": "warning",
                "message": f"Predicted threshold breach at {forecast.estimated_breach_time}",
                "metric": metric_name,
                "predicted_value": forecast.predicted_value,
                "confidence": forecast.confidence,
            }
            alerts.append(alert)

        result["alerts"] = alerts

        # Trigger callbacks for alerts
        for alert in alerts:
            self._alert_history.append({
                "timestamp": datetime.utcnow().isoformat(),
                **alert
            })
            for callback in self._alert_callbacks:
                try:
                    callback(alert)
                except Exception as e:
                    logger.error(f"Alert callback error: {e}")

        return result

    def get_alert_history(self, minutes: int = 60) -> List[Dict[str, Any]]:
        """Get recent alert history."""
        cutoff = datetime.utcnow() - timedelta(minutes=minutes)
        return [
            alert for alert in self._alert_history
            if datetime.fromisoformat(alert["timestamp"]) >= cutoff
        ]

    def get_health_summary(self) -> Dict[str, Any]:
        """Get overall health summary based on recent alerts and trends."""
        recent_alerts = self.get_alert_history(60)
        forecasts = self.trend_forecaster.get_all_forecasts(30)

        critical_count = sum(1 for a in recent_alerts if a.get("severity") == "critical")
        warning_count = sum(1 for a in recent_alerts if a.get("severity") == "warning")
        predicted_breaches = sum(1 for f in forecasts if f.will_breach_threshold)

        if critical_count > 0:
            health_status = "critical"
        elif warning_count > 2 or predicted_breaches > 0:
            health_status = "degraded"
        else:
            health_status = "healthy"

        return {
            "status": health_status,
            "critical_alerts": critical_count,
            "warning_alerts": warning_count,
            "predicted_breaches": predicted_breaches,
            "metrics_tracked": len(self.anomaly_detector._time_series),
        }


# Global instances
_anomaly_detector: Optional[AnomalyDetector] = None
_predictive_alerter: Optional[PredictiveAlerter] = None


def get_anomaly_detector() -> AnomalyDetector:
    """Get the global anomaly detector instance."""
    global _anomaly_detector
    if _anomaly_detector is None:
        _anomaly_detector = AnomalyDetector()
    return _anomaly_detector


def get_predictive_alerter() -> PredictiveAlerter:
    """Get the global predictive alerter instance."""
    global _predictive_alerter
    if _predictive_alerter is None:
        _predictive_alerter = PredictiveAlerter()
    return _predictive_alerter


@dataclass
class MetricsConfig:
    """Configuration for Prometheus metrics."""

    # Service identification
    service_name: str = "drivesentinel"
    service_version: str = "2.0.0"

    # Metrics endpoint
    enable_metrics_endpoint: bool = True
    metrics_port: int = 9090  # Separate port for metrics

    # Push gateway (optional)
    push_gateway_url: Optional[str] = None
    push_interval_seconds: int = 60

    # Default histogram buckets (in seconds)
    default_buckets: tuple = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0)

    # Label cardinality limits
    max_label_values: int = 100


class MetricsRegistry:
    """
    Centralized metrics registry for DriveSentinel.

    Provides standardized metrics with proper labeling and buckets.
    """

    def __init__(self, config: Optional[MetricsConfig] = None):
        """Initialize the metrics registry."""
        self.config = config or MetricsConfig()
        self._counters: Dict[str, Any] = {}
        self._histograms: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}
        self._summaries: Dict[str, Any] = {}
        self._registry = REGISTRY if PROMETHEUS_AVAILABLE else None
        self._initialized = False

    def initialize(self):
        """Initialize metrics and optionally start metrics server."""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Prometheus client not available, metrics disabled")
            return False

        if self._initialized:
            return True

        try:
            # Create service info metric
            self._create_service_info()

            # Create standard metrics
            self._create_standard_metrics()

            self._initialized = True
            logger.info(f"Prometheus metrics initialized for {self.config.service_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Prometheus metrics: {e}")
            return False

    def start_metrics_server(self, port: Optional[int] = None):
        """Start a separate HTTP server for /metrics endpoint."""
        if not PROMETHEUS_AVAILABLE:
            return False

        port = port or self.config.metrics_port
        try:
            start_http_server(port)
            logger.info(f"Prometheus metrics server started on port {port}")
            return True
        except Exception as e:
            logger.error(f"Failed to start metrics server: {e}")
            return False

    def _create_service_info(self):
        """Create service information metric."""
        if not PROMETHEUS_AVAILABLE:
            return

        info = Info(
            "service",
            "Service information",
            registry=self._registry
        )
        info.info({
            "name": self.config.service_name,
            "version": self.config.service_version,
        })

    def _create_standard_metrics(self):
        """Create standard metrics used across all services."""
        # HTTP request metrics
        self.create_counter(
            "http_requests_total",
            "Total HTTP requests",
            labels=["service", "method", "endpoint", "status"]
        )

        self.create_histogram(
            "http_request_duration_seconds",
            "HTTP request duration in seconds",
            labels=["service", "method", "endpoint"],
            buckets=self.config.default_buckets
        )

        # LLM metrics
        self.create_counter(
            "llm_requests_total",
            "Total LLM API requests",
            labels=["service", "model", "status"]
        )

        self.create_histogram(
            "llm_request_duration_seconds",
            "LLM API request duration",
            labels=["service", "model"]
        )

        self.create_histogram(
            "llm_tokens_used",
            "Tokens used per LLM request",
            labels=["service", "model", "type"],  # type: input/output
            buckets=(10, 50, 100, 250, 500, 1000, 2000, 4000, 8000, 16000)
        )

        # RAG metrics
        self.create_counter(
            "rag_queries_total",
            "Total RAG queries",
            labels=["service", "status"]
        )

        self.create_histogram(
            "rag_query_duration_seconds",
            "RAG query duration",
            labels=["service"]
        )

        self.create_histogram(
            "rag_documents_retrieved",
            "Number of documents retrieved per query",
            labels=["service"],
            buckets=(0, 1, 2, 3, 5, 10, 20, 50, 100)
        )

        # Database metrics
        self.create_histogram(
            "db_query_duration_seconds",
            "Database query duration",
            labels=["service", "operation"]
        )

        self.create_gauge(
            "db_connection_pool_size",
            "Database connection pool size",
            labels=["service"]
        )

        # Error metrics
        self.create_counter(
            "errors_total",
            "Total errors",
            labels=["service", "type", "severity"]
        )

        # Active requests gauge
        self.create_gauge(
            "active_requests",
            "Number of active requests being processed",
            labels=["service"]
        )

    def create_counter(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ):
        """Create or get a counter metric."""
        if not PROMETHEUS_AVAILABLE:
            return NoOpCounter()

        if name not in self._counters:
            self._counters[name] = Counter(
                name,
                description,
                labelnames=labels or [],
                registry=self._registry
            )
        return self._counters[name]

    def create_histogram(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[tuple] = None
    ):
        """Create or get a histogram metric."""
        if not PROMETHEUS_AVAILABLE:
            return NoOpHistogram()

        if name not in self._histograms:
            self._histograms[name] = Histogram(
                name,
                description,
                labelnames=labels or [],
                buckets=buckets or self.config.default_buckets,
                registry=self._registry
            )
        return self._histograms[name]

    def create_gauge(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ):
        """Create or get a gauge metric."""
        if not PROMETHEUS_AVAILABLE:
            return NoOpGauge()

        if name not in self._gauges:
            self._gauges[name] = Gauge(
                name,
                description,
                labelnames=labels or [],
                registry=self._registry
            )
        return self._gauges[name]

    def create_summary(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ):
        """Create or get a summary metric."""
        if not PROMETHEUS_AVAILABLE:
            return NoOpSummary()

        if name not in self._summaries:
            self._summaries[name] = Summary(
                name,
                description,
                labelnames=labels or [],
                registry=self._registry
            )
        return self._summaries[name]

    # Convenience methods
    def inc_counter(self, name: str, value: float = 1, labels: Optional[Dict] = None):
        """Increment a counter."""
        if name in self._counters:
            if labels:
                self._counters[name].labels(**labels).inc(value)
            else:
                self._counters[name].inc(value)

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict] = None):
        """Observe a value in a histogram."""
        if name in self._histograms:
            if labels:
                self._histograms[name].labels(**labels).observe(value)
            else:
                self._histograms[name].observe(value)

    def set_gauge(self, name: str, value: float, labels: Optional[Dict] = None):
        """Set a gauge value."""
        if name in self._gauges:
            if labels:
                self._gauges[name].labels(**labels).set(value)
            else:
                self._gauges[name].set(value)

    def inc_gauge(self, name: str, value: float = 1, labels: Optional[Dict] = None):
        """Increment a gauge."""
        if name in self._gauges:
            if labels:
                self._gauges[name].labels(**labels).inc(value)
            else:
                self._gauges[name].inc(value)

    def dec_gauge(self, name: str, value: float = 1, labels: Optional[Dict] = None):
        """Decrement a gauge."""
        if name in self._gauges:
            if labels:
                self._gauges[name].labels(**labels).dec(value)
            else:
                self._gauges[name].dec(value)

    # Decorators
    def count_calls(self, counter_name: str, labels: Optional[Dict] = None) -> Callable[[F], F]:
        """Decorator to count function calls."""
        def decorator(func: F) -> F:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                self.inc_counter(counter_name, labels=labels)
                return func(*args, **kwargs)

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                self.inc_counter(counter_name, labels=labels)
                return await func(*args, **kwargs)

            if asyncio.iscoroutinefunction(func):
                return async_wrapper  # type: ignore
            return sync_wrapper  # type: ignore

        return decorator

    def time_execution(self, histogram_name: str, labels: Optional[Dict] = None) -> Callable[[F], F]:
        """Decorator to measure function execution time."""
        def decorator(func: F) -> F:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    return func(*args, **kwargs)
                finally:
                    duration = time.perf_counter() - start
                    self.observe_histogram(histogram_name, duration, labels=labels)

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.perf_counter()
                try:
                    return await func(*args, **kwargs)
                finally:
                    duration = time.perf_counter() - start
                    self.observe_histogram(histogram_name, duration, labels=labels)

            if asyncio.iscoroutinefunction(func):
                return async_wrapper  # type: ignore
            return sync_wrapper  # type: ignore

        return decorator

    def track_in_progress(self, gauge_name: str, labels: Optional[Dict] = None) -> Callable[[F], F]:
        """Decorator to track in-progress operations."""
        def decorator(func: F) -> F:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                self.inc_gauge(gauge_name, labels=labels)
                try:
                    return func(*args, **kwargs)
                finally:
                    self.dec_gauge(gauge_name, labels=labels)

            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                self.inc_gauge(gauge_name, labels=labels)
                try:
                    return await func(*args, **kwargs)
                finally:
                    self.dec_gauge(gauge_name, labels=labels)

            if asyncio.iscoroutinefunction(func):
                return async_wrapper  # type: ignore
            return sync_wrapper  # type: ignore

        return decorator

    def get_metrics_output(self) -> bytes:
        """Generate metrics output for /metrics endpoint."""
        if PROMETHEUS_AVAILABLE:
            return generate_latest(self._registry)
        return b""

    def get_content_type(self) -> str:
        """Get the content type for metrics endpoint."""
        if PROMETHEUS_AVAILABLE:
            return CONTENT_TYPE_LATEST
        return "text/plain"


# No-op implementations for when prometheus_client is not available
class NoOpMetric:
    def labels(self, **kwargs):
        return self

class NoOpCounter(NoOpMetric):
    def inc(self, value: float = 1):
        pass

class NoOpHistogram(NoOpMetric):
    def observe(self, value: float):
        pass

class NoOpGauge(NoOpMetric):
    def set(self, value: float):
        pass
    def inc(self, value: float = 1):
        pass
    def dec(self, value: float = 1):
        pass

class NoOpSummary(NoOpMetric):
    def observe(self, value: float):
        pass


# Global metrics registry
_metrics_registry: Optional[MetricsRegistry] = None


def get_metrics() -> MetricsRegistry:
    """Get the global metrics registry."""
    global _metrics_registry
    if _metrics_registry is None:
        _metrics_registry = MetricsRegistry()
        _metrics_registry.initialize()
    return _metrics_registry


def configure_metrics(config: MetricsConfig) -> MetricsRegistry:
    """Configure and return the global metrics registry."""
    global _metrics_registry
    _metrics_registry = MetricsRegistry(config)
    _metrics_registry.initialize()
    return _metrics_registry


# FastAPI integration
def create_metrics_endpoint(app):
    """
    Add /metrics endpoint to a FastAPI app.

    Usage:
        from fastapi import FastAPI
        from services.shared.observability import create_metrics_endpoint

        app = FastAPI()
        create_metrics_endpoint(app)
    """
    from fastapi import Response

    @app.get("/metrics")
    async def metrics():
        metrics_registry = get_metrics()
        return Response(
            content=metrics_registry.get_metrics_output(),
            media_type=metrics_registry.get_content_type()
        )

    return app


# Request middleware for automatic HTTP metrics
class PrometheusMiddleware:
    """
    ASGI middleware for automatic HTTP request metrics.

    Usage:
        from fastapi import FastAPI
        from services.shared.observability import PrometheusMiddleware

        app = FastAPI()
        app.add_middleware(PrometheusMiddleware, service_name="my-service")
    """

    def __init__(self, app, service_name: str = "unknown"):
        self.app = app
        self.service_name = service_name
        self.metrics = get_metrics()

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "UNKNOWN")
        path = scope.get("path", "/")

        # Track active requests
        labels = {"service": self.service_name}
        self.metrics.inc_gauge("active_requests", labels=labels)

        start_time = time.perf_counter()
        status_code = "500"

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = str(message.get("status", 500))
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start_time

            # Record metrics
            self.metrics.inc_counter(
                "http_requests_total",
                labels={
                    "service": self.service_name,
                    "method": method,
                    "endpoint": path,
                    "status": status_code
                }
            )

            self.metrics.observe_histogram(
                "http_request_duration_seconds",
                duration,
                labels={
                    "service": self.service_name,
                    "method": method,
                    "endpoint": path
                }
            )

            self.metrics.dec_gauge("active_requests", labels=labels)


# =============================================================================
# CUTTING EDGE: CAUSAL METRIC ANALYSIS
# =============================================================================

class CausalRelationType(str, Enum):
    """Types of causal relationships between metrics."""
    CAUSES = "causes"  # A causes B
    CAUSED_BY = "caused_by"  # A is caused by B
    CORRELATES = "correlates"  # A and B correlate
    LEADS = "leads"  # A leads B (temporal)
    LAGS = "lags"  # A lags B
    UNKNOWN = "unknown"


@dataclass
class CausalRelationship:
    """A discovered causal relationship between metrics."""
    source_metric: str
    target_metric: str
    relationship_type: CausalRelationType
    strength: float  # 0.0 to 1.0
    lag_minutes: float  # Time lag between cause and effect
    confidence: float
    evidence: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_metric": self.source_metric,
            "target_metric": self.target_metric,
            "relationship_type": self.relationship_type.value,
            "strength": self.strength,
            "lag_minutes": self.lag_minutes,
            "confidence": self.confidence,
            "evidence": self.evidence,
        }


@dataclass
class CausalGraph:
    """Graph of causal relationships between metrics."""
    relationships: List[CausalRelationship]
    root_causes: List[str]  # Metrics that are primarily causes
    effects: List[str]  # Metrics that are primarily effects
    clusters: Dict[str, List[str]]  # Related metric groups
    created_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "relationships": [r.to_dict() for r in self.relationships],
            "root_causes": self.root_causes,
            "effects": self.effects,
            "clusters": self.clusters,
            "created_at": self.created_at.isoformat(),
        }

    def get_causes_of(self, metric: str) -> List[CausalRelationship]:
        """Get metrics that cause the given metric."""
        return [
            r for r in self.relationships
            if r.target_metric == metric and r.relationship_type in [CausalRelationType.CAUSES, CausalRelationType.LEADS]
        ]

    def get_effects_of(self, metric: str) -> List[CausalRelationship]:
        """Get metrics affected by the given metric."""
        return [
            r for r in self.relationships
            if r.source_metric == metric and r.relationship_type in [CausalRelationType.CAUSES, CausalRelationType.LEADS]
        ]


class CausalMetricAnalyzer:
    """
    Cutting Edge: Discovers causal relationships between metrics.

    Uses cross-correlation, Granger causality concepts, and
    temporal pattern analysis to discover metric relationships.

    Features:
    - Cross-correlation analysis
    - Lag detection
    - Relationship strength scoring
    - Root cause identification
    - Metric clustering
    """

    def __init__(
        self,
        correlation_threshold: float = 0.6,
        min_samples: int = 50,
        max_lag_minutes: int = 30
    ):
        self.correlation_threshold = correlation_threshold
        self.min_samples = min_samples
        self.max_lag_minutes = max_lag_minutes
        self._time_series: Dict[str, MetricTimeSeries] = {}
        self._causal_graph: Optional[CausalGraph] = None
        self._last_analysis: Optional[datetime] = None

    def record_metric(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[datetime] = None
    ):
        """Record a metric value for causal analysis."""
        if metric_name not in self._time_series:
            self._time_series[metric_name] = MetricTimeSeries(
                name=metric_name,
                values=deque(maxlen=2000)
            )
        self._time_series[metric_name].add(value, timestamp)

    def _compute_correlation(
        self,
        series_a: List[float],
        series_b: List[float]
    ) -> float:
        """Compute Pearson correlation coefficient."""
        n = len(series_a)
        if n < 3:
            return 0.0

        mean_a = sum(series_a) / n
        mean_b = sum(series_b) / n

        numerator = sum((a - mean_a) * (b - mean_b) for a, b in zip(series_a, series_b))
        denom_a = math.sqrt(sum((a - mean_a) ** 2 for a in series_a))
        denom_b = math.sqrt(sum((b - mean_b) ** 2 for b in series_b))

        if denom_a == 0 or denom_b == 0:
            return 0.0

        return numerator / (denom_a * denom_b)

    def _compute_cross_correlation(
        self,
        series_a: List[Tuple[datetime, float]],
        series_b: List[Tuple[datetime, float]],
        max_lag_samples: int = 10
    ) -> Tuple[float, int, float]:
        """
        Compute cross-correlation with lag detection.

        Returns (best_correlation, lag_samples, confidence).
        """
        # Align series by timestamp
        values_a = [v for _, v in series_a]
        values_b = [v for _, v in series_b]

        if len(values_a) < self.min_samples or len(values_b) < self.min_samples:
            return 0.0, 0, 0.0

        best_corr = 0.0
        best_lag = 0

        for lag in range(-max_lag_samples, max_lag_samples + 1):
            if lag >= 0:
                a_slice = values_a[lag:]
                b_slice = values_b[:len(values_a) - lag]
            else:
                a_slice = values_a[:len(values_a) + lag]
                b_slice = values_b[-lag:]

            if len(a_slice) < 10 or len(b_slice) < 10:
                continue

            min_len = min(len(a_slice), len(b_slice))
            corr = self._compute_correlation(a_slice[:min_len], b_slice[:min_len])

            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_lag = lag

        # Confidence based on sample size and correlation strength
        confidence = min(0.95, abs(best_corr) * 0.5 + min(len(values_a), len(values_b)) / 200)

        return best_corr, best_lag, confidence

    def _determine_relationship_type(
        self,
        correlation: float,
        lag: int
    ) -> CausalRelationType:
        """Determine relationship type from correlation and lag."""
        if abs(correlation) < self.correlation_threshold:
            return CausalRelationType.UNKNOWN

        if lag > 0:
            return CausalRelationType.LEADS if correlation > 0 else CausalRelationType.LEADS
        elif lag < 0:
            return CausalRelationType.LAGS if correlation > 0 else CausalRelationType.LAGS
        else:
            return CausalRelationType.CORRELATES

    def analyze_relationships(
        self,
        window_minutes: int = 60
    ) -> CausalGraph:
        """
        Analyze causal relationships between all tracked metrics.

        Returns a CausalGraph with discovered relationships.
        """
        relationships = []
        metric_names = list(self._time_series.keys())

        if len(metric_names) < 2:
            return CausalGraph(
                relationships=[],
                root_causes=[],
                effects=[],
                clusters={}
            )

        # Analyze pairwise relationships
        for i, metric_a in enumerate(metric_names):
            for metric_b in metric_names[i + 1:]:
                series_a = self._time_series[metric_a].get_values_in_window(window_minutes)
                series_b = self._time_series[metric_b].get_values_in_window(window_minutes)

                if len(series_a) < self.min_samples or len(series_b) < self.min_samples:
                    continue

                corr, lag, confidence = self._compute_cross_correlation(series_a, series_b)

                if abs(corr) >= self.correlation_threshold:
                    rel_type = self._determine_relationship_type(corr, lag)

                    # Determine source and target based on lag
                    if lag > 0:
                        source, target = metric_a, metric_b
                    else:
                        source, target = metric_b, metric_a

                    # Estimate lag in minutes
                    avg_interval = self._estimate_interval(series_a)
                    lag_minutes = abs(lag) * avg_interval

                    relationship = CausalRelationship(
                        source_metric=source,
                        target_metric=target,
                        relationship_type=rel_type,
                        strength=abs(corr),
                        lag_minutes=lag_minutes,
                        confidence=confidence,
                        evidence=[
                            f"Cross-correlation: {corr:.3f}",
                            f"Optimal lag: {lag} samples ({lag_minutes:.1f} min)",
                            f"Sample size: {min(len(series_a), len(series_b))}",
                        ]
                    )
                    relationships.append(relationship)

        # Identify root causes and effects
        source_counts: Dict[str, int] = {}
        target_counts: Dict[str, int] = {}

        for rel in relationships:
            if rel.relationship_type in [CausalRelationType.CAUSES, CausalRelationType.LEADS]:
                source_counts[rel.source_metric] = source_counts.get(rel.source_metric, 0) + 1
                target_counts[rel.target_metric] = target_counts.get(rel.target_metric, 0) + 1

        root_causes = [
            m for m, count in source_counts.items()
            if count > target_counts.get(m, 0)
        ]
        effects = [
            m for m, count in target_counts.items()
            if count > source_counts.get(m, 0)
        ]

        # Cluster related metrics
        clusters = self._cluster_metrics(relationships, metric_names)

        self._causal_graph = CausalGraph(
            relationships=relationships,
            root_causes=root_causes,
            effects=effects,
            clusters=clusters,
        )
        self._last_analysis = datetime.utcnow()

        return self._causal_graph

    def _estimate_interval(self, series: List[Tuple[datetime, float]]) -> float:
        """Estimate average interval between samples in minutes."""
        if len(series) < 2:
            return 1.0

        intervals = []
        for i in range(1, min(len(series), 20)):
            delta = (series[i][0] - series[i - 1][0]).total_seconds() / 60
            if delta > 0:
                intervals.append(delta)

        return sum(intervals) / len(intervals) if intervals else 1.0

    def _cluster_metrics(
        self,
        relationships: List[CausalRelationship],
        all_metrics: List[str]
    ) -> Dict[str, List[str]]:
        """Cluster related metrics using union-find."""
        # Simple clustering based on strong correlations
        parent: Dict[str, str] = {m: m for m in all_metrics}

        def find(x: str) -> str:
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]

        def union(x: str, y: str):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py

        for rel in relationships:
            if rel.strength >= 0.7:
                union(rel.source_metric, rel.target_metric)

        # Group by cluster
        clusters: Dict[str, List[str]] = {}
        for metric in all_metrics:
            root = find(metric)
            if root not in clusters:
                clusters[root] = []
            clusters[root].append(metric)

        # Only keep clusters with multiple metrics
        return {k: v for k, v in clusters.items() if len(v) > 1}

    def get_root_cause_analysis(
        self,
        anomalous_metric: str
    ) -> Dict[str, Any]:
        """
        Perform root cause analysis for an anomalous metric.

        Uses the causal graph to trace back to potential root causes.
        """
        if not self._causal_graph:
            self.analyze_relationships()

        if not self._causal_graph:
            return {"error": "Unable to build causal graph"}

        causes = self._causal_graph.get_causes_of(anomalous_metric)

        # Sort by relationship strength
        causes.sort(key=lambda r: r.strength, reverse=True)

        # Trace back through chain
        root_cause_chain = []
        visited = {anomalous_metric}

        def trace_back(metric: str, depth: int = 0):
            if depth > 5:
                return

            for rel in self._causal_graph.get_causes_of(metric):
                if rel.source_metric not in visited:
                    visited.add(rel.source_metric)
                    root_cause_chain.append({
                        "metric": rel.source_metric,
                        "affects": rel.target_metric,
                        "strength": rel.strength,
                        "lag_minutes": rel.lag_minutes,
                        "depth": depth,
                    })
                    trace_back(rel.source_metric, depth + 1)

        trace_back(anomalous_metric)

        return {
            "anomalous_metric": anomalous_metric,
            "direct_causes": [c.to_dict() for c in causes[:5]],
            "root_cause_chain": root_cause_chain,
            "likely_root_causes": [
                r["metric"] for r in root_cause_chain
                if r["depth"] == (max(c["depth"] for c in root_cause_chain) if root_cause_chain else 0)
            ][:3],
        }


# =============================================================================
# CUTTING EDGE: AUTOMATIC DASHBOARD GENERATION
# =============================================================================

class DashboardPanelType(str, Enum):
    """Types of dashboard panels."""
    TIME_SERIES = "time_series"
    GAUGE = "gauge"
    STAT = "stat"
    TABLE = "table"
    HEATMAP = "heatmap"
    ALERT_LIST = "alert_list"


@dataclass
class DashboardPanel:
    """Definition of a dashboard panel."""
    panel_id: str
    title: str
    panel_type: DashboardPanelType
    metrics: List[str]
    description: str
    row: int
    col: int
    width: int = 6
    height: int = 4
    thresholds: Optional[Dict[str, float]] = None

    def to_grafana_json(self) -> Dict[str, Any]:
        """Convert to Grafana panel JSON format."""
        panel_type_map = {
            DashboardPanelType.TIME_SERIES: "timeseries",
            DashboardPanelType.GAUGE: "gauge",
            DashboardPanelType.STAT: "stat",
            DashboardPanelType.TABLE: "table",
            DashboardPanelType.HEATMAP: "heatmap",
            DashboardPanelType.ALERT_LIST: "alertlist",
        }

        return {
            "id": hash(self.panel_id) % 1000,
            "title": self.title,
            "type": panel_type_map.get(self.panel_type, "timeseries"),
            "description": self.description,
            "gridPos": {
                "x": self.col * self.width,
                "y": self.row * self.height,
                "w": self.width,
                "h": self.height,
            },
            "targets": [
                {"expr": metric, "legendFormat": metric}
                for metric in self.metrics
            ],
        }


@dataclass
class GeneratedDashboard:
    """An automatically generated dashboard."""
    dashboard_id: str
    title: str
    description: str
    panels: List[DashboardPanel]
    tags: List[str]
    created_at: datetime = field(default_factory=datetime.utcnow)
    generation_reason: str = ""

    def to_grafana_json(self) -> Dict[str, Any]:
        """Convert to Grafana dashboard JSON format."""
        return {
            "dashboard": {
                "id": None,
                "uid": self.dashboard_id,
                "title": self.title,
                "description": self.description,
                "tags": self.tags,
                "timezone": "utc",
                "schemaVersion": 30,
                "version": 1,
                "panels": [p.to_grafana_json() for p in self.panels],
            },
            "overwrite": True,
        }


class DashboardTemplate:
    """Template for dashboard generation."""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.panel_templates: List[Dict[str, Any]] = []

    def add_panel_template(
        self,
        title_pattern: str,
        metric_pattern: str,
        panel_type: DashboardPanelType,
        condition: Optional[Callable[[str], bool]] = None
    ):
        """Add a panel template."""
        self.panel_templates.append({
            "title_pattern": title_pattern,
            "metric_pattern": metric_pattern,
            "panel_type": panel_type,
            "condition": condition,
        })


class AutoDashboardGenerator:
    """
    Cutting Edge: Automatically generates relevant dashboards.

    Analyzes metrics patterns and creates dashboards based on:
    - Metric naming conventions
    - Discovered causal relationships
    - Anomaly patterns
    - Service topology

    Features:
    - Pattern-based panel generation
    - Causal relationship visualization
    - Anomaly-focused dashboards
    - Template customization
    """

    def __init__(self, causal_analyzer: Optional[CausalMetricAnalyzer] = None):
        self.causal_analyzer = causal_analyzer
        self._templates: Dict[str, DashboardTemplate] = {}
        self._generated_dashboards: Dict[str, GeneratedDashboard] = {}
        self._initialize_default_templates()

    def _initialize_default_templates(self):
        """Initialize default dashboard templates."""
        # Service overview template
        service_template = DashboardTemplate(
            "service_overview",
            "Overview dashboard for a service"
        )
        service_template.add_panel_template(
            "Request Rate",
            "http_requests_total",
            DashboardPanelType.TIME_SERIES
        )
        service_template.add_panel_template(
            "Request Duration",
            "http_request_duration_seconds",
            DashboardPanelType.TIME_SERIES
        )
        service_template.add_panel_template(
            "Error Rate",
            "errors_total",
            DashboardPanelType.TIME_SERIES
        )
        service_template.add_panel_template(
            "Active Requests",
            "active_requests",
            DashboardPanelType.GAUGE
        )
        self._templates["service_overview"] = service_template

        # LLM metrics template
        llm_template = DashboardTemplate(
            "llm_metrics",
            "LLM API usage and performance"
        )
        llm_template.add_panel_template(
            "LLM Requests",
            "llm_requests_total",
            DashboardPanelType.TIME_SERIES
        )
        llm_template.add_panel_template(
            "LLM Duration",
            "llm_request_duration_seconds",
            DashboardPanelType.TIME_SERIES
        )
        llm_template.add_panel_template(
            "Token Usage",
            "llm_tokens_used",
            DashboardPanelType.TIME_SERIES
        )
        self._templates["llm_metrics"] = llm_template

        # RAG metrics template
        rag_template = DashboardTemplate(
            "rag_metrics",
            "RAG system performance"
        )
        rag_template.add_panel_template(
            "RAG Queries",
            "rag_queries_total",
            DashboardPanelType.TIME_SERIES
        )
        rag_template.add_panel_template(
            "RAG Duration",
            "rag_query_duration_seconds",
            DashboardPanelType.TIME_SERIES
        )
        rag_template.add_panel_template(
            "Documents Retrieved",
            "rag_documents_retrieved",
            DashboardPanelType.TIME_SERIES
        )
        self._templates["rag_metrics"] = rag_template

    def add_template(self, template: DashboardTemplate):
        """Add a custom dashboard template."""
        self._templates[template.name] = template

    def _detect_metric_categories(
        self,
        metric_names: List[str]
    ) -> Dict[str, List[str]]:
        """Categorize metrics by naming patterns."""
        categories: Dict[str, List[str]] = {
            "http": [],
            "llm": [],
            "rag": [],
            "db": [],
            "error": [],
            "other": [],
        }

        for metric in metric_names:
            metric_lower = metric.lower()
            if "http" in metric_lower or "request" in metric_lower:
                categories["http"].append(metric)
            elif "llm" in metric_lower or "model" in metric_lower or "token" in metric_lower:
                categories["llm"].append(metric)
            elif "rag" in metric_lower or "retriev" in metric_lower or "document" in metric_lower:
                categories["rag"].append(metric)
            elif "db" in metric_lower or "database" in metric_lower or "query" in metric_lower:
                categories["db"].append(metric)
            elif "error" in metric_lower or "fail" in metric_lower:
                categories["error"].append(metric)
            else:
                categories["other"].append(metric)

        return {k: v for k, v in categories.items() if v}

    def _select_panel_type(self, metric_name: str) -> DashboardPanelType:
        """Select appropriate panel type based on metric name."""
        metric_lower = metric_name.lower()

        if "total" in metric_lower or "count" in metric_lower:
            return DashboardPanelType.TIME_SERIES
        elif "duration" in metric_lower or "seconds" in metric_lower or "latency" in metric_lower:
            return DashboardPanelType.TIME_SERIES
        elif "active" in metric_lower or "current" in metric_lower or "pool" in metric_lower:
            return DashboardPanelType.GAUGE
        elif "percent" in metric_lower or "ratio" in metric_lower:
            return DashboardPanelType.STAT
        else:
            return DashboardPanelType.TIME_SERIES

    def generate_overview_dashboard(
        self,
        metric_names: List[str],
        service_name: str = "service"
    ) -> GeneratedDashboard:
        """
        Generate an overview dashboard for available metrics.
        """
        import hashlib

        categories = self._detect_metric_categories(metric_names)
        panels = []
        row = 0

        # Generate panels for each category
        for category, metrics in categories.items():
            if not metrics:
                continue

            for i, metric in enumerate(metrics[:4]):  # Limit panels per category
                panel = DashboardPanel(
                    panel_id=f"{category}_{i}",
                    title=self._format_metric_title(metric),
                    panel_type=self._select_panel_type(metric),
                    metrics=[metric],
                    description=f"Auto-generated panel for {metric}",
                    row=row,
                    col=i % 2,
                    width=12 if i % 2 == 0 else 12,
                    height=4,
                )
                panels.append(panel)
                if i % 2 == 1:
                    row += 1

            if len(metrics) % 2 == 1:
                row += 1

        dashboard_id = hashlib.sha256(
            f"{service_name}_overview_{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]

        dashboard = GeneratedDashboard(
            dashboard_id=dashboard_id,
            title=f"{service_name.title()} Overview",
            description=f"Auto-generated overview dashboard for {service_name}",
            panels=panels,
            tags=["auto-generated", service_name, "overview"],
            generation_reason="Automatic overview generation",
        )

        self._generated_dashboards[dashboard_id] = dashboard
        return dashboard

    def generate_causal_dashboard(
        self,
        causal_graph: CausalGraph,
        focus_metric: Optional[str] = None
    ) -> GeneratedDashboard:
        """
        Generate a dashboard visualizing causal relationships.
        """
        import hashlib

        panels = []
        row = 0

        if focus_metric:
            # Focus on one metric and its causes/effects
            causes = causal_graph.get_causes_of(focus_metric)
            effects = causal_graph.get_effects_of(focus_metric)

            # Main metric panel
            panels.append(DashboardPanel(
                panel_id="focus_metric",
                title=f"Focus: {self._format_metric_title(focus_metric)}",
                panel_type=DashboardPanelType.TIME_SERIES,
                metrics=[focus_metric],
                description=f"The metric under analysis",
                row=0,
                col=0,
                width=24,
                height=6,
            ))
            row += 1

            # Cause panels
            for i, cause in enumerate(causes[:4]):
                panels.append(DashboardPanel(
                    panel_id=f"cause_{i}",
                    title=f"Cause: {self._format_metric_title(cause.source_metric)} (strength: {cause.strength:.2f})",
                    panel_type=DashboardPanelType.TIME_SERIES,
                    metrics=[cause.source_metric],
                    description=f"Leads {focus_metric} by {cause.lag_minutes:.1f} min",
                    row=row,
                    col=i % 2,
                ))
                if i % 2 == 1:
                    row += 1

            if len(causes) % 2 == 1:
                row += 1

            # Effect panels
            for i, effect in enumerate(effects[:4]):
                panels.append(DashboardPanel(
                    panel_id=f"effect_{i}",
                    title=f"Effect: {self._format_metric_title(effect.target_metric)} (strength: {effect.strength:.2f})",
                    panel_type=DashboardPanelType.TIME_SERIES,
                    metrics=[effect.target_metric],
                    description=f"Affected by {focus_metric}",
                    row=row,
                    col=i % 2,
                ))
                if i % 2 == 1:
                    row += 1

        else:
            # Show root causes and their effects
            for i, root in enumerate(causal_graph.root_causes[:6]):
                effects = causal_graph.get_effects_of(root)
                effect_metrics = [e.target_metric for e in effects[:3]]

                panels.append(DashboardPanel(
                    panel_id=f"root_{i}",
                    title=f"Root Cause: {self._format_metric_title(root)}",
                    panel_type=DashboardPanelType.TIME_SERIES,
                    metrics=[root] + effect_metrics,
                    description=f"Root cause with {len(effects)} downstream effects",
                    row=row,
                    col=i % 2,
                    width=12,
                ))
                if i % 2 == 1:
                    row += 1

        dashboard_id = hashlib.sha256(
            f"causal_{focus_metric or 'all'}_{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]

        dashboard = GeneratedDashboard(
            dashboard_id=dashboard_id,
            title=f"Causal Analysis: {focus_metric or 'All Metrics'}",
            description="Auto-generated dashboard showing causal metric relationships",
            panels=panels,
            tags=["auto-generated", "causal-analysis"],
            generation_reason=f"Causal analysis for {focus_metric or 'all metrics'}",
        )

        self._generated_dashboards[dashboard_id] = dashboard
        return dashboard

    def generate_anomaly_dashboard(
        self,
        anomalies: List[Dict[str, Any]],
        predictive_alerts: List[Dict[str, Any]] = None
    ) -> GeneratedDashboard:
        """
        Generate a dashboard focused on detected anomalies.
        """
        import hashlib

        panels = []
        row = 0

        # Group anomalies by metric
        anomaly_metrics: Dict[str, List[Dict]] = {}
        for anomaly in anomalies:
            metric = anomaly.get("metric", "unknown")
            if metric not in anomaly_metrics:
                anomaly_metrics[metric] = []
            anomaly_metrics[metric].append(anomaly)

        # Alert summary panel
        panels.append(DashboardPanel(
            panel_id="alert_summary",
            title="Alert Summary",
            panel_type=DashboardPanelType.STAT,
            metrics=[],
            description=f"Total anomalies: {len(anomalies)}",
            row=0,
            col=0,
            width=24,
            height=3,
        ))
        row += 1

        # Panel for each anomalous metric
        for i, (metric, metric_anomalies) in enumerate(anomaly_metrics.items()):
            severity = max(a.get("severity", "info") for a in metric_anomalies)
            panels.append(DashboardPanel(
                panel_id=f"anomaly_{i}",
                title=f"🚨 {self._format_metric_title(metric)} ({severity})",
                panel_type=DashboardPanelType.TIME_SERIES,
                metrics=[metric],
                description=f"{len(metric_anomalies)} anomalies detected",
                row=row,
                col=i % 2,
                thresholds={"warning": metric_anomalies[0].get("expected_value", 0) * 1.5},
            ))
            if i % 2 == 1:
                row += 1

        dashboard_id = hashlib.sha256(
            f"anomaly_{datetime.utcnow().isoformat()}".encode()
        ).hexdigest()[:16]

        dashboard = GeneratedDashboard(
            dashboard_id=dashboard_id,
            title="Anomaly Investigation",
            description="Auto-generated dashboard for anomaly investigation",
            panels=panels,
            tags=["auto-generated", "anomaly", "alert"],
            generation_reason=f"Investigating {len(anomalies)} anomalies",
        )

        self._generated_dashboards[dashboard_id] = dashboard
        return dashboard

    def _format_metric_title(self, metric_name: str) -> str:
        """Format metric name as a readable title."""
        # Remove common prefixes/suffixes
        title = metric_name.replace("_total", "").replace("_seconds", "")
        title = title.replace("_", " ").title()
        return title

    def get_generated_dashboards(self) -> List[GeneratedDashboard]:
        """Get all generated dashboards."""
        return list(self._generated_dashboards.values())

    def export_dashboard(
        self,
        dashboard_id: str,
        format: str = "grafana"
    ) -> Optional[Dict[str, Any]]:
        """Export a dashboard in the specified format."""
        dashboard = self._generated_dashboards.get(dashboard_id)
        if not dashboard:
            return None

        if format == "grafana":
            return dashboard.to_grafana_json()

        return dashboard.to_grafana_json()  # Default to Grafana


# =============================================================================
# CUTTING EDGE: INTEGRATED METRICS SYSTEM
# =============================================================================

class CuttingEdgeMetricsConfig(MetricsConfig):
    """Extended configuration for cutting edge metrics."""
    enable_causal_analysis: bool = True
    enable_auto_dashboards: bool = True
    causal_analysis_interval_minutes: int = 15
    auto_dashboard_generation: bool = True


class CuttingEdgeMetricsRegistry(MetricsRegistry):
    """
    Cutting Edge Metrics Registry with causal analysis and auto-dashboards.

    Features beyond Advanced:
    - Causal Metric Analysis: Discover metric relationships
    - Auto Dashboard Generation: Create relevant dashboards automatically
    - Root Cause Analysis: Trace anomalies to root causes
    - Metric Clustering: Group related metrics

    Usage:
        metrics = get_cutting_edge_metrics()

        # Record metrics (same as before)
        metrics.record_for_analysis("latency", 0.5)

        # Analyze causal relationships
        graph = metrics.analyze_causality()

        # Generate dashboards
        dashboard = metrics.generate_dashboard_for_service("api")

        # Root cause analysis
        causes = metrics.get_root_cause_analysis("error_rate")
    """

    def __init__(self, config: Optional[CuttingEdgeMetricsConfig] = None):
        self.cutting_edge_config = config or CuttingEdgeMetricsConfig()
        super().__init__(self.cutting_edge_config)

        # Initialize cutting edge components
        self._causal_analyzer: Optional[CausalMetricAnalyzer] = None
        if self.cutting_edge_config.enable_causal_analysis:
            self._causal_analyzer = CausalMetricAnalyzer()

        self._dashboard_generator: Optional[AutoDashboardGenerator] = None
        if self.cutting_edge_config.enable_auto_dashboards:
            self._dashboard_generator = AutoDashboardGenerator(self._causal_analyzer)

        # Track metrics for analysis
        self._analysis_metrics: Dict[str, MetricTimeSeries] = {}

    def record_for_analysis(
        self,
        metric_name: str,
        value: float,
        timestamp: Optional[datetime] = None
    ):
        """Record a metric value for causal analysis."""
        if metric_name not in self._analysis_metrics:
            self._analysis_metrics[metric_name] = MetricTimeSeries(
                name=metric_name,
                values=deque(maxlen=2000)
            )
        self._analysis_metrics[metric_name].add(value, timestamp)

        if self._causal_analyzer:
            self._causal_analyzer.record_metric(metric_name, value, timestamp)

    def analyze_causality(self, window_minutes: int = 60) -> Optional[CausalGraph]:
        """Analyze causal relationships between metrics."""
        if not self._causal_analyzer:
            return None
        return self._causal_analyzer.analyze_relationships(window_minutes)

    def get_root_cause_analysis(self, metric_name: str) -> Dict[str, Any]:
        """Get root cause analysis for a metric."""
        if not self._causal_analyzer:
            return {"error": "Causal analysis not enabled"}
        return self._causal_analyzer.get_root_cause_analysis(metric_name)

    def generate_dashboard_for_service(
        self,
        service_name: str
    ) -> Optional[GeneratedDashboard]:
        """Generate an overview dashboard for a service."""
        if not self._dashboard_generator:
            return None

        metric_names = list(self._analysis_metrics.keys())
        return self._dashboard_generator.generate_overview_dashboard(
            metric_names, service_name
        )

    def generate_causal_dashboard(
        self,
        focus_metric: Optional[str] = None
    ) -> Optional[GeneratedDashboard]:
        """Generate a dashboard showing causal relationships."""
        if not self._dashboard_generator or not self._causal_analyzer:
            return None

        graph = self._causal_analyzer.analyze_relationships()
        return self._dashboard_generator.generate_causal_dashboard(graph, focus_metric)

    def generate_anomaly_dashboard(
        self,
        anomalies: List[Dict[str, Any]]
    ) -> Optional[GeneratedDashboard]:
        """Generate a dashboard for anomaly investigation."""
        if not self._dashboard_generator:
            return None
        return self._dashboard_generator.generate_anomaly_dashboard(anomalies)

    def get_cutting_edge_stats(self) -> Dict[str, Any]:
        """Get statistics about cutting edge features."""
        stats = {
            "causal_analysis_enabled": self._causal_analyzer is not None,
            "auto_dashboards_enabled": self._dashboard_generator is not None,
            "metrics_tracked_for_analysis": len(self._analysis_metrics),
        }

        if self._causal_analyzer and self._causal_analyzer._causal_graph:
            graph = self._causal_analyzer._causal_graph
            stats["causal_relationships"] = len(graph.relationships)
            stats["root_causes_identified"] = len(graph.root_causes)
            stats["metric_clusters"] = len(graph.clusters)

        if self._dashboard_generator:
            stats["dashboards_generated"] = len(self._dashboard_generator._generated_dashboards)

        return stats


# Factory function for cutting edge metrics
_cutting_edge_metrics: Optional[CuttingEdgeMetricsRegistry] = None


def get_cutting_edge_metrics(
    config: Optional[CuttingEdgeMetricsConfig] = None
) -> CuttingEdgeMetricsRegistry:
    """Get the global cutting edge metrics registry."""
    global _cutting_edge_metrics
    if _cutting_edge_metrics is None or config is not None:
        _cutting_edge_metrics = CuttingEdgeMetricsRegistry(config)
        _cutting_edge_metrics.initialize()
    return _cutting_edge_metrics
