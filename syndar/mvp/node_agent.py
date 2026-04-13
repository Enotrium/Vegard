"""SimpleNodeAgent - Execute task loop: capture -> HSI -> emit

MVP: Orchestrates the scan loop without computing anything.
"""

import time
import threading
from typing import Optional, Callable
from syndar.mvp.mesh import SimpleMesh, DroneState, Position, SoilPrediction


class SimpleNodeAgent:
    """Minimal drone agent that executes tasks"""
    
    def __init__(
        self,
        drone_id: str,
        mesh: SimpleMesh,
        hsi_client,  # Has .predict(cube) method
        arthedain_client=None,  # Optional, has .get_drift_e() method
        start_position: Optional[tuple] = None
    ):
        self.drone_id = drone_id
        self.mesh = mesh
        self.hsi = hsi_client
        self.arthedain = arthedain_client
        
        # State
        self.position = Position(
            lat=start_position[0] if start_position else 39.0,
            lng=start_position[1] if start_position else -77.0,
            alt=start_position[2] if start_position and len(start_position) > 2 else 50.0
        )
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._on_result: Optional[Callable[[dict], None]] = None
        
        # Register with mesh
        self._update_mesh(status="idle", task_id=None)
    
    def _update_mesh(self, status: str, task_id: Optional[str], **kwargs) -> None:
        """Update mesh with current state"""
        state = DroneState(
            drone_id=self.drone_id,
            position=self.position,
            status=status,
            current_task_id=task_id,
            **kwargs
        )
        self.mesh.update(self.drone_id, state)
    
    def start(self) -> None:
        """Start the agent loop"""
        self.running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the agent loop"""
        self.running = False
        if self._thread:
            self._thread.join(timeout=2.0)
    
    def _loop(self) -> None:
        """Main loop: check for tasks and execute"""
        while self.running:
            # Check for assigned tasks
            state = self.mesh.get(self.drone_id)
            
            if state and state.current_task_id and state.status == "assigned":
                # Get task details from mesh (stored in current_task field)
                # For MVP, we execute immediately
                self._execute_current_task(state)
            
            time.sleep(1.0)
    
    def _execute_current_task(self, state: DroneState) -> None:
        """Execute the current task"""
        task_id = state.current_task_id
        
        # 1. Update status to scanning
        self._update_mesh(status="transiting", task_id=task_id)
        
        # 2. Simulate navigation to field (2 seconds)
        time.sleep(2.0)
        self._update_mesh(status="scanning", task_id=task_id)
        
        # 3. Simulate spectral capture (3 seconds)
        time.sleep(3.0)
        
        # Create mock spectral cube
        cube = {
            "field_id": f"field-{task_id}",
            "width": 128,
            "height": 128,
            "bands": 200,
            "data": "mock_spectral_data"
        }
        
        # 4. Call HSI API
        try:
            prediction = self.hsi.predict(cube)
        except Exception as e:
            prediction = {
                "field_id": cube["field_id"],
                "error": str(e),
                "nutrients": {},
                "land_value_score": 0.0,
                "contamination_detected": False,
            }
        
        # 5. Get drift from Arthedain (if available)
        drift_e = 0.0
        if self.arthedain:
            try:
                drift_e = self.arthedain.get_drift_e()
            except Exception:
                pass
        
        # 6. Create result
        result = {
            "drone_id": self.drone_id,
            "task_id": task_id,
            "field_id": prediction.get("field_id", "unknown"),
            "prediction": prediction,
            "position": {
                "lat": self.position.lat,
                "lng": self.position.lng,
                "alt": self.position.alt
            },
            "drift_e": drift_e,
            "drift_flag": drift_e > 0.5,
            "timestamp_ms": int(time.time() * 1000)
        }
        
        # 7. Emit result
        self.mesh.emit_result(result)
        
        # 8. Update state with last soil prediction
        soil = SoilPrediction(
            field_id=prediction.get("field_id", "unknown"),
            nutrients=prediction.get("nutrients", {}),
            land_value_score=prediction.get("land_value_score", 0.0),
            contamination_detected=prediction.get("contamination_detected", False),
            spectral_hash=prediction.get("spectral_hash", ""),
        )
        
        # 9. Return to idle
        self._update_mesh(
            status="idle",
            task_id=None,
            last_soil=soil,
            drift_e=drift_e,
            drift_flag=drift_e > 0.5
        )
    
    def execute_task_direct(self, task: dict) -> dict:
        """Execute a task directly (synchronous, for testing)"""
        # Update mesh
        self._update_mesh(status="assigned", task_id=task["id"])
        
        # Execute
        state = self.mesh.get(self.drone_id)
        self._execute_current_task(state)
        
        # Return result
        return {
            "drone_id": self.drone_id,
            "task_id": task["id"],
            "status": "completed"
        }
