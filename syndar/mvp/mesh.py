"""SimpleMesh - In-memory drone registry + pub/sub

MVP: Dict of drones + callbacks. No gossip, no distributed consensus.
"""

import time
from typing import Callable, Optional
from dataclasses import dataclass, field
from pydantic import BaseModel


class Position(BaseModel):
    lat: float
    lng: float
    alt: float = 0.0


class SoilPrediction(BaseModel):
    field_id: str
    nutrients: dict = field(default_factory=dict)
    land_value_score: float = 0.0
    contamination_detected: bool = False
    spectral_hash: str = ""


@dataclass
class DroneState:
    """Simple drone state - flat, serializable"""
    drone_id: str
    position: Position
    status: str = "idle"  # idle, transiting, scanning, returning
    current_task_id: Optional[str] = None
    battery_pct: float = 100.0
    last_soil: Optional[SoilPrediction] = None
    drift_e: float = 0.0
    drift_flag: bool = False
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    
    def to_dict(self) -> dict:
        return {
            "drone_id": self.drone_id,
            "position": self.position.model_dump(),
            "status": self.status,
            "current_task_id": self.current_task_id,
            "battery_pct": self.battery_pct,
            "last_soil": self.last_soil.model_dump() if self.last_soil else None,
            "drift_e": self.drift_e,
            "drift_flag": self.drift_flag,
            "timestamp_ms": self.timestamp_ms,
        }


class SimpleMesh:
    """In-memory drone registry + pub/sub"""
    
    def __init__(self):
        self.drones: dict[str, DroneState] = {}
        self.state_callbacks: list[Callable[[str, DroneState], None]] = []
        self.result_callbacks: list[Callable[[dict], None]] = []
    
    def update(self, drone_id: str, state: DroneState) -> None:
        """Update drone state and notify subscribers"""
        state.timestamp_ms = int(time.time() * 1000)
        self.drones[drone_id] = state
        
        # Notify state subscribers
        for cb in self.state_callbacks:
            try:
                cb(drone_id, state)
            except Exception:
                pass  # MVP: fail silently
    
    def update_from_dict(self, drone_id: str, state_dict: dict) -> None:
        """Update from dict (convenience method)"""
        state = DroneState(
            drone_id=drone_id,
            position=Position(**state_dict.get("position", {"lat": 0, "lng": 0})),
            status=state_dict.get("status", "idle"),
            current_task_id=state_dict.get("current_task_id"),
            battery_pct=state_dict.get("battery_pct", 100.0),
            drift_e=state_dict.get("drift_e", 0.0),
            drift_flag=state_dict.get("drift_flag", False),
        )
        self.update(drone_id, state)
    
    def get(self, drone_id: str) -> Optional[DroneState]:
        """Get drone state"""
        return self.drones.get(drone_id)
    
    def get_all(self) -> list[DroneState]:
        """Get all drone states"""
        return list(self.drones.values())
    
    def get_available(self) -> list[DroneState]:
        """Get idle drones"""
        return [d for d in self.drones.values() if d.status == "idle"]
    
    def subscribe_to_state(self, callback: Callable[[str, DroneState], None]) -> None:
        """Subscribe to state updates"""
        self.state_callbacks.append(callback)
    
    def unsubscribe_from_state(self, callback: Callable[[str, DroneState], None]) -> None:
        """Unsubscribe from state updates"""
        if callback in self.state_callbacks:
            self.state_callbacks.remove(callback)
    
    def emit_result(self, result: dict) -> None:
        """Emit scan result to subscribers"""
        for cb in self.result_callbacks:
            try:
                cb(result)
            except Exception:
                pass
    
    def subscribe_to_results(self, callback: Callable[[dict], None]) -> None:
        """Subscribe to scan results"""
        self.result_callbacks.append(callback)
    
    def get_fleet_status(self) -> dict:
        """Get summary of fleet status"""
        all_drones = self.get_all()
        return {
            "total": len(all_drones),
            "idle": len([d for d in all_drones if d.status == "idle"]),
            "scanning": len([d for d in all_drones if d.status == "scanning"]),
            "transiting": len([d for d in all_drones if d.status == "transiting"]),
            "low_battery": len([d for d in all_drones if d.battery_pct < 20]),
            "drift_alerts": len([d for d in all_drones if d.drift_flag]),
        }
