"""Cross-node drift correlation - The novel differentiator

Arthedain handles per-node drift. Vegard correlates drift spatially across nodes.
If drones over the same region all show drift simultaneously, that's a
contamination event or model recalibration signal, not individual noise.
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

import numpy as np
import structlog
from pydantic import BaseModel, Field

from syndar.fabric.mesh import EntityState

logger = structlog.get_logger()


class NodeDriftSignal(BaseModel):
    """Per-node drift signal from Arthedain SNN"""

    entity_id: str
    e_fast: float  # Fast timescale trace (~100ms)
    e_slow: float  # Slow timescale trace (~700ms)
    combined_e: float  # E(t) = α·e_fast + β·e_slow
    threshold: float
    exceeded: bool
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    lat: float
    lng: float
    field_id: str
    task_id: str


class DriftCorrelation(BaseModel):
    """Spatial drift correlation analysis"""

    field_id: str
    analysis_timestamp_ms: int
    entity_ids: list[str]
    node_count: int
    spatial_correlation_score: float  # 0.0-1.0, high = field-wide drift
    temporal_correlation_score: float  # 0.0-1.0, simultaneous drift
    mean_drift_e: float
    max_drift_e: float
    std_drift_e: float
    drift_type: str
    confidence: float
    interpretation: str


class DriftAlert(BaseModel):
    """Drift alert for immediate action"""

    alert_id: str
    severity: str  # info, warning, critical, emergency
    correlation: DriftCorrelation
    node_signals: list[NodeDriftSignal]
    created_at_ms: int
    recommended_actions: list[str]
    triggers_recalibration: bool
    triggers_inspection: bool


@dataclass
class DriftThresholds:
    """Configurable thresholds for drift detection"""

    per_node_e_threshold: float = 0.5
    spatial_correlation_threshold: float = 0.7
    temporal_correlation_threshold: float = 0.8
    min_nodes_for_correlation: int = 3
    max_signal_age_ms: int = 60000


class DriftMonitor:
    """Correlates drift signals across nodes spatially"""

    def __init__(self, thresholds: Optional[DriftThresholds] = None):
        self.thresholds = thresholds or DriftThresholds()
        self._signals: dict[str, list[NodeDriftSignal]] = defaultdict(list)
        self._alerts: list[DriftAlert] = []
        self._subscribers: list[callable] = []
        self._lock = asyncio.Lock()
        self._analysis_task: Optional[asyncio.Task] = None
        self._running = False

        # Weights for combined E(t)
        self.alpha = 0.3  # e_fast weight
        self.beta = 0.7  # e_slow weight
        
        # Historical baselines for adaptive thresholds
        self._baselines: dict[str, dict[str, float]] = defaultdict(dict)
        self._trend_history: dict[str, list[float]] = defaultdict(list)
        self._max_trend_history = 100  # Keep last 100 trend points

    async def start(self) -> None:
        """Start periodic analysis"""
        self._running = True
        self._analysis_task = asyncio.create_task(self._analysis_loop())
        logger.info("Drift monitor started")

    async def stop(self) -> None:
        """Stop analysis"""
        self._running = False
        if self._analysis_task:
            self._analysis_task.cancel()
        logger.info("Drift monitor stopped")

    async def report_signal(self, signal: NodeDriftSignal) -> None:
        """Report drift signal from a node"""
        # Calculate combined E(t)
        signal.combined_e = self.alpha * signal.e_fast + self.beta * signal.e_slow
        signal.exceeded = signal.combined_e > signal.threshold

        async with self._lock:
            self._signals[signal.field_id].append(signal)
            # Trim old signals
            cutoff = time.time() * 1000 - self.thresholds.max_signal_age_ms
            self._signals[signal.field_id] = [
                s for s in self._signals[signal.field_id] if s.timestamp_ms > cutoff
            ]

        if signal.exceeded:
            logger.warning(
                "Drift threshold exceeded",
                entity_id=signal.entity_id,
                field_id=signal.field_id,
                e_value=signal.combined_e,
            )

    async def _analysis_loop(self) -> None:
        """Periodic correlation analysis"""
        while self._running:
            try:
                await asyncio.sleep(5.0)  # Analyze every 5 seconds
                await self._analyze_all_fields()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Drift analysis error")

    async def _analyze_all_fields(self) -> None:
        """Run correlation analysis on all fields with recent signals"""
        async with self._lock:
            field_ids = list(self._signals.keys())

        for field_id in field_ids:
            correlation = await self._analyze_field(field_id)
            if correlation and correlation.confidence > 0.6:
                await self._process_correlation(correlation)

    async def _analyze_field(self, field_id: str) -> Optional[DriftCorrelation]:
        """Analyze drift correlation for a specific field"""
        async with self._lock:
            signals = self._signals.get(field_id, [])

        if len(signals) < self.thresholds.min_nodes_for_correlation:
            return None

        # Get recent signals (last 30 seconds)
        cutoff = time.time() * 1000 - 30000
        recent = [s for s in signals if s.timestamp_ms > cutoff]

        if len(recent) < self.thresholds.min_nodes_for_correlation:
            return None

        # Update baselines and compute adaptive threshold
        self._update_baselines(field_id, recent)
        adaptive_threshold = self._get_adaptive_threshold(field_id)

        # Trend analysis
        trend_score = self._analyze_trend(field_id, recent)

        # Spatial correlation: are nodes physically clustered?
        positions = np.array([(s.lat, s.lng) for s in recent])
        if len(positions) > 1:
            spatial_spread = np.std(positions, axis=0).mean()
            # Lower spread = higher spatial correlation
            spatial_score = max(0.0, 1.0 - spatial_spread * 1000)
        else:
            spatial_score = 1.0

        # Temporal correlation: are drift signals simultaneous?
        timestamps = [s.timestamp_ms for s in recent]
        time_spread = max(timestamps) - min(timestamps) if len(timestamps) > 1 else 0
        # Within 10 seconds = high temporal correlation
        temporal_score = max(0.0, 1.0 - time_spread / 10000)

        # Drift magnitude analysis
        e_values = [s.combined_e for s in recent if s.combined_e > adaptive_threshold]
        if not e_values:
            return None

        mean_e = np.mean(e_values)
        max_e = np.max(e_values)
        std_e = np.std(e_values)

        # Determine drift type
        if spatial_score > self.thresholds.spatial_correlation_threshold:
            if temporal_score > self.thresholds.temporal_correlation_threshold:
                if trend_score > 0.7:
                    drift_type = "escalating"
                    interpretation = (
                        "Escalating field-wide drift - urgent intervention required"
                    )
                else:
                    drift_type = "compound"
                    interpretation = (
                        "Simultaneous drift across field - likely contamination event "
                        "or rapid soil chemistry change"
                    )
            else:
                drift_type = "spatial"
                interpretation = (
                    "Field-wide drift pattern - possible model recalibration needed"
                )
        else:
            if temporal_score > self.thresholds.temporal_correlation_threshold:
                drift_type = "temporal"
                interpretation = "Simultaneous but localized drift - check for coordinated interference"
            else:
                drift_type = "independent"
                interpretation = "Random per-node noise - no action needed"

        # Confidence based on number of nodes and correlation scores
        confidence = min(1.0, len(recent) / 10) * (spatial_score + temporal_score + trend_score) / 3

        return DriftCorrelation(
            field_id=field_id,
            analysis_timestamp_ms=int(time.time() * 1000),
            entity_ids=[s.entity_id for s in recent],
            node_count=len(recent),
            spatial_correlation_score=spatial_score,
            temporal_correlation_score=temporal_score,
            mean_drift_e=mean_e,
            max_drift_e=max_e,
            std_drift_e=std_e,
            drift_type=drift_type,
            confidence=confidence,
            interpretation=interpretation,
        )

    async def _process_correlation(self, correlation: DriftCorrelation) -> None:
        """Process significant correlation - may trigger alerts"""
        # Determine severity
        if correlation.drift_type == "compound" and correlation.confidence > 0.8:
            severity = "emergency"
        elif correlation.drift_type in ("compound", "spatial") and correlation.confidence > 0.7:
            severity = "critical"
        elif correlation.confidence > 0.6:
            severity = "warning"
        else:
            severity = "info"

        # Generate alert if significant
        if severity in ("critical", "emergency"):
            await self._generate_alert(correlation, severity)

        logger.info(
            "Drift correlation analyzed",
            field_id=correlation.field_id,
            drift_type=correlation.drift_type,
            confidence=correlation.confidence,
            severity=severity,
        )

    async def _generate_alert(self, correlation: DriftCorrelation, severity: str) -> None:
        """Generate drift alert"""
        # Get node signals for alert
        async with self._lock:
            all_signals = self._signals.get(correlation.field_id, [])
            alert_signals = [
                s for s in all_signals if s.entity_id in correlation.entity_ids
            ]

        # Recommended actions
        actions = []
        triggers_recalibration = False
        triggers_inspection = False

        if correlation.drift_type == "compound":
            actions.append("Immediate field inspection recommended")
            actions.append("Halt new contracts for this field")
            triggers_inspection = True

        if correlation.drift_type == "spatial":
            actions.append("Model recalibration recommended")
            actions.append("Validate against ground truth samples")
            triggers_recalibration = True

        actions.append(f"Review {correlation.node_count} affected drone nodes")

        alert = DriftAlert(
            alert_id=f"drift-{correlation.field_id}-{int(time.time() * 1000)}",
            severity=severity,
            correlation=correlation,
            node_signals=alert_signals,
            created_at_ms=int(time.time() * 1000),
            recommended_actions=actions,
            triggers_recalibration=triggers_recalibration,
            triggers_inspection=triggers_inspection,
        )

        async with self._lock:
            self._alerts.append(alert)

        # Notify subscribers
        for cb in self._subscribers:
            try:
                cb(alert)
            except Exception:
                logger.exception("Alert subscriber failed")

        logger.warning(
            "Drift alert generated",
            alert_id=alert.alert_id,
            field_id=correlation.field_id,
            severity=severity,
            actions=actions,
        )

    def subscribe(self, callback: callable) -> None:
        """Subscribe to drift alerts"""
        self._subscribers.append(callback)

    async def get_alerts(
        self, field_id: Optional[str] = None, min_severity: str = "info"
    ) -> list[DriftAlert]:
        """Get drift alerts"""
        severity_order = {"info": 0, "warning": 1, "critical": 2, "emergency": 3}
        min_level = severity_order.get(min_severity, 0)

        async with self._lock:
            alerts = self._alerts
            if field_id:
                alerts = [a for a in alerts if a.correlation.field_id == field_id]
            return [
                a for a in alerts if severity_order.get(a.severity, 0) >= min_level
            ]

    async def get_correlations(self, field_id: str) -> list[DriftCorrelation]:
        """Get historical correlations for a field"""
        async with self._lock:
            return [a.correlation for a in self._alerts if a.correlation.field_id == field_id]

    async def get_stats(self) -> dict:
        """Get monitor statistics"""
        async with self._lock:
            return {
                "fields_monitored": len(self._signals),
                "total_signals": sum(len(s) for s in self._signals.values()),
                "total_alerts": len(self._alerts),
                "critical_alerts": len([a for a in self._alerts if a.severity == "critical"]),
                "emergency_alerts": len([a for a in self._alerts if a.severity == "emergency"]),
                "fields_with_baselines": len(self._baselines),
            }

    def _update_baselines(self, field_id: str, signals: list[NodeDriftSignal]) -> None:
        """Update historical baselines for adaptive thresholds"""
        if not signals:
            return

        e_values = [s.combined_e for s in signals]
        baseline = self._baselines[field_id]
        
        # Exponential moving average for baseline
        alpha = 0.1  # Learning rate
        if "mean_e" not in baseline:
            baseline["mean_e"] = np.mean(e_values)
            baseline["std_e"] = np.std(e_values)
        else:
            baseline["mean_e"] = alpha * np.mean(e_values) + (1 - alpha) * baseline["mean_e"]
            baseline["std_e"] = alpha * np.std(e_values) + (1 - alpha) * baseline["std_e"]
        
        baseline["count"] = baseline.get("count", 0) + len(signals)

    def _get_adaptive_threshold(self, field_id: str) -> float:
        """Get adaptive threshold based on historical baseline"""
        baseline = self._baselines.get(field_id, {})
        if "mean_e" in baseline and "std_e" in baseline:
            # Adaptive threshold: baseline mean + 2 * baseline std
            return baseline["mean_e"] + 2 * baseline["std_e"]
        return self.thresholds.per_node_e_threshold

    def _analyze_trend(self, field_id: str, signals: list[NodeDriftSignal]) -> float:
        """Analyze drift trend over time (0-1 score, higher = increasing trend)"""
        if len(signals) < 2:
            return 0.0

        # Sort by timestamp
        sorted_signals = sorted(signals, key=lambda s: s.timestamp_ms)
        e_values = [s.combined_e for s in sorted_signals]
        
        # Calculate trend using linear regression
        x = np.arange(len(e_values))
        y = np.array(e_values)
        
        # Simple linear regression
        if len(x) > 1:
            slope = np.polyfit(x, y, 1)[0]
            # Normalize slope to 0-1 range
            # Positive slope indicates increasing trend
            trend_score = min(1.0, max(0.0, slope * 10))
        else:
            trend_score = 0.0

        # Update trend history
        self._trend_history[field_id].append(trend_score)
        if len(self._trend_history[field_id]) > self._max_trend_history:
            self._trend_history[field_id].pop(0)

        # Return smoothed trend score
        if len(self._trend_history[field_id]) > 5:
            return np.mean(self._trend_history[field_id][-5:])
        return trend_score
