"""SimpleDriftMonitor - Basic spatial drift detection

MVP: If 3+ drones in same area show high drift, flag it.
"""

import time
from typing import Optional
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class DriftReading:
    """Single drift reading from a drone"""
    drone_id: str
    field_id: str
    drift_e: float
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class SimpleDriftMonitor:
    """Minimal drift aggregation - spatial correlation"""
    
    def __init__(self, threshold: float = 0.5, min_nodes: int = 3, window_s: int = 60):
        """
        Args:
            threshold: Drift E(t) value to consider "high"
            min_nodes: Minimum drones with high drift to trigger alert
            window_s: Time window to consider readings (seconds)
        """
        self.threshold = threshold
        self.min_nodes = min_nodes
        self.window_ms = window_s * 1000
        self.readings: list[DriftReading] = []
        self.alerts_triggered = 0
    
    def report(self, drone_id: str, field_id: str, drift_e: float) -> Optional[dict]:
        """Report drift reading, check for anomalies
        
        Args:
            drone_id: Reporting drone
            field_id: Field being scanned
            drift_e: E(t) value from Arthedain
            
        Returns:
            Alert dict if anomaly detected, None otherwise
        """
        # Store reading
        reading = DriftReading(
            drone_id=drone_id,
            field_id=field_id,
            drift_e=drift_e
        )
        self.readings.append(reading)
        
        # Clean old readings
        cutoff = int(time.time() * 1000) - self.window_ms
        self.readings = [r for r in self.readings if r.timestamp_ms > cutoff]
        
        # Check for cluster in this field
        field_readings = [r for r in self.readings if r.field_id == field_id]
        high_drifts = [r for r in field_readings if r.drift_e > self.threshold]
        
        if len(high_drifts) >= self.min_nodes:
            self.alerts_triggered += 1
            
            # Calculate average
            avg_drift = sum(r.drift_e for r in high_drifts) / len(high_drifts)
            
            return {
                "alert": "possible_anomaly",
                "field_id": field_id,
                "severity": "warning",
                "affected_drones": [r.drone_id for r in high_drifts],
                "drone_count": len(high_drifts),
                "avg_drift_e": round(avg_drift, 4),
                "threshold": self.threshold,
                "timestamp_ms": int(time.time() * 1000),
                "message": f"{len(high_drifts)} drones showing elevated drift in {field_id}"
            }
        
        return None
    
    def get_field_summary(self, field_id: str) -> dict:
        """Get drift summary for a field"""
        field_readings = [r for r in self.readings if r.field_id == field_id]
        
        if not field_readings:
            return {"field_id": field_id, "status": "no_data"}
        
        high_count = len([r for r in field_readings if r.drift_e > self.threshold])
        
        return {
            "field_id": field_id,
            "total_readings": len(field_readings),
            "high_drift_count": high_count,
            "avg_drift_e": sum(r.drift_e for r in field_readings) / len(field_readings),
            "max_drift_e": max(r.drift_e for r in field_readings),
            "status": "anomaly" if high_count >= self.min_nodes else "normal"
        }
    
    def get_stats(self) -> dict:
        """Get monitor statistics"""
        return {
            "total_readings": len(self.readings),
            "alerts_triggered": self.alerts_triggered,
            "fields_monitored": len(set(r.field_id for r in self.readings)),
            "threshold": self.threshold,
            "min_nodes": self.min_nodes
        }
