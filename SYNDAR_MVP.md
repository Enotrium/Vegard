# Vegard MVP — Minimum Viable System

**Goal**: End-to-end working system in 1-2 days. Strip everything non-essential.

---

## The 5 Problems to Solve

### 1. Tasking — Who scans what, when

**Required:**
- Accept scan requests (from AIP or manual)
- Assign them to drones  
- Ensure they get done

**Minimal implementation:**
```python
def assign_task(task, drones):
    # No fancy auctions — just nearest available
    available = [d for d in drones if d.status == "idle"]
    if not available:
        return None
    return min(available, key=lambda d: distance(d.position, task.location))
```

**If you don't build this:** Nothing happens. Drones sit idle.

---

### 2. State Sharing — Where everything is + what it produced

**Required:**
Live registry of all drones with:
```python
{
  drone_id: str,
  position: {lat, lng, alt},
  status: str,  # idle, transiting, scanning, returning
  current_task_id: str | None,
  last_soil_prediction: SoilPrediction | None,
  drift_e: float,
  drift_flag: bool,
  battery_pct: float,
  timestamp_ms: int
}
```

**Minimal implementation:**
```python
class SimpleMesh:
    def __init__(self):
        self.drones = {}  # drone_id -> DroneState
        self.callbacks = []  # list of functions to call on update
    
    def update(self, drone_id, state):
        self.drones[drone_id] = state
        for cb in self.callbacks:
            cb(state)
    
    def subscribe(self, callback):
        self.callbacks.append(callback)
```

**If you skip this:** No coordination, no visibility, no FOP possible.

---

### 3. Execution Loop (NodeAgent glue)

**Most critical integration point.**

Each drone must:
1. Receive task
2. Capture spectral data
3. Call HSI API  
4. Get prediction
5. Emit result
6. Report drift (from Arthedain)

**Vegard responsibility:** Orchestrate the loop, not compute.

**Minimal implementation:**
```python
class SimpleNodeAgent:
    def __init__(self, drone_id, mesh, hsi_client, arthedain_client):
        self.drone_id = drone_id
        self.mesh = mesh
        self.hsi = hsi_client
        self.arthedain = arthedain_client
        self.position = (39.0, -77.0, 50)  # lat, lng, alt
        self.status = "idle"
        self.current_task = None
    
    def execute_task(self, task):
        self.status = "scanning"
        self.current_task = task
        self.mesh.update(self.drone_id, self._to_state())
        
        # 1. Navigate to field (simulated)
        self._navigate_to(task.field_center)
        
        # 2. Capture spectral data
        cube = self._capture_spectral(task.field_polygon)
        
        # 3. Call HSI API
        prediction = self.hsi.predict(cube)
        
        # 4. Get drift from Arthedain
        drift_e = self.arthedain.get_drift_e()
        
        # 5. Emit result to mesh
        result = {
            "drone_id": self.drone_id,
            "task_id": task.id,
            "prediction": prediction,
            "position": self.position,
            "drift_e": drift_e,
            "drift_flag": drift_e > 0.5,
            "timestamp_ms": now()
        }
        self.mesh.emit_result(result)
        
        self.status = "idle"
        self.current_task = None
        self.mesh.update(self.drone_id, self._to_state())
        
        return result
```

---

### 4. AIP Bridge — Deliver value downstream

**Where money happens.**

**Required:**
- POST results to AIP
- Include: soil prediction, GPS, timestamp, drone ID, signature

**Minimal implementation:**
```python
class SimpleAIPBridge:
    def __init__(self, aip_url, api_key=None):
        self.url = aip_url
        self.api_key = api_key
    
    def send_result(self, result):
        payload = {
            "syndar_drone_id": result["drone_id"],
            "syndar_timestamp_ms": result["timestamp_ms"],
            "field_id": result["prediction"]["field_id"],
            "latitude": result["position"][0],
            "longitude": result["position"][1],
            "land_value_score": result["prediction"]["land_value_score"],
            "nutrients": result["prediction"]["nutrients"],
            "contamination_detected": result["prediction"]["contamination_detected"],
            "spectral_hash": result["prediction"]["spectral_hash"],
            "signature": "stub-signature",  # Add real PGP later
        }
        
        # Simple POST with retry
        for attempt in range(3):
            try:
                resp = requests.post(f"{self.url}/api/syndar/ingest", 
                                   json=payload, 
                                   timeout=30)
                resp.raise_for_status()
                return True
            except Exception as e:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)  # exponential backoff
        
        return False
```

**If this breaks:** The whole system is useless commercially.

---

### 5. Drift Aggregation (your only "smart" feature)

**Even in v1, prove the concept:**

```python
class SimpleDriftMonitor:
    def __init__(self, threshold=0.5, min_nodes=3):
        self.threshold = threshold
        self.min_nodes = min_nodes
        self.recent_drifts = []  # list of {drone_id, field_id, drift_e, timestamp}
    
    def report(self, drone_id, field_id, drift_e):
        self.recent_drifts.append({
            "drone_id": drone_id,
            "field_id": field_id, 
            "drift_e": drift_e,
            "timestamp_ms": now()
        })
        
        # Keep only last 60 seconds
        cutoff = now() - 60000
        self.recent_drifts = [d for d in self.recent_drifts if d["timestamp_ms"] > cutoff]
        
        # Check for cluster
        field_drifts = [d for d in self.recent_drifts if d["field_id"] == field_id]
        high_drifts = [d for d in field_drifts if d["drift_e"] > self.threshold]
        
        if len(high_drifts) >= self.min_nodes:
            return {
                "alert": "possible_anomaly",
                "field_id": field_id,
                "affected_drones": [d["drone_id"] for d in high_drifts],
                "avg_drift_e": sum(d["drift_e"] for d in high_drifts) / len(high_drifts)
            }
        
        return None
```

**No need for sophistication yet** — just prove that spatial correlation works.

---

## What You Should Build Right Now

### Phase 1 — "It Works" System (1–2 days)

**Files:**

```python
# mesh.py
class SimpleMesh:
    """In-memory drone registry + pub/sub"""
    def __init__(self):
        self.drones = {}
        self.callbacks = []
    
    def update(self, drone_id, state):
        self.drones[drone_id] = state
        for cb in self.callbacks:
            cb(drone_id, state)
    
    def subscribe(self, callback):
        self.callbacks.append(callback)
    
    def get(self, drone_id):
        return self.drones.get(drone_id)
    
    def get_all(self):
        return list(self.drones.values())
    
    def get_available(self):
        return [d for d in self.drones.values() if d.get("status") == "idle"]
```

```python
# task_allocator.py  
def assign_task_simple(task, mesh):
    """Nearest available drone"""
    available = mesh.get_available()
    if not available:
        return None
    
    # Simple distance calculation
    def distance(drone):
        dx = drone["position"]["lat"] - task["location"]["lat"]
        dy = drone["position"]["lng"] - task["location"]["lng"]
        return (dx**2 + dy**2) ** 0.5
    
    return min(available, key=distance)
```

```python
# node_agent.py
class SimpleNodeAgent:
    """Execute task loop: capture -> HSI -> emit"""
    
    def __init__(self, drone_id, mesh, hsi_client, arthedain_client=None):
        self.drone_id = drone_id
        self.mesh = mesh
        self.hsi = hsi_client
        self.arthedain = arthedain_client
        self.running = False
    
    def start(self):
        self.running = True
        threading.Thread(target=self._loop).start()
    
    def _loop(self):
        while self.running:
            # Check for assigned tasks
            state = self.mesh.get(self.drone_id)
            if state and state.get("current_task_id"):
                task = state["current_task"]
                self.execute(task)
            time.sleep(1)
    
    def execute(self, task):
        # 1. Update status
        self._update_status("scanning", task["id"])
        
        # 2. Simulate capture
        time.sleep(2)  # Simulate flight + capture
        cube = {"field_id": task["field_id"], "data": "mock_spectral_cube"}
        
        # 3. Call HSI API
        prediction = self.hsi.predict(cube)
        
        # 4. Get drift signal
        drift_e = 0.0
        if self.arthedain:
            drift_e = self.arthedain.get_drift_e()
        
        # 5. Emit result
        result = {
            "drone_id": self.drone_id,
            "task_id": task["id"],
            "field_id": task["field_id"],
            "prediction": prediction,
            "drift_e": drift_e,
            "drift_flag": drift_e > 0.5,
            "timestamp_ms": int(time.time() * 1000)
        }
        self.mesh.emit_result(result)
        
        # 6. Back to idle
        self._update_status("idle", None)
        
        return result
    
    def _update_status(self, status, task_id):
        state = self.mesh.get(self.drone_id) or {}
        state["status"] = status
        state["current_task_id"] = task_id
        self.mesh.update(self.drone_id, state)
```

```python
# aip_bridge.py
import requests
import time

class SimpleAIPBridge:
    """POST to AIP with retry"""
    
    def __init__(self, base_url, api_key=None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
    
    def send(self, result):
        payload = {
            "syndar_drone_id": result["drone_id"],
            "syndar_timestamp_ms": result["timestamp_ms"],
            "field_id": result["field_id"],
            "latitude": result.get("position", {}).get("lat", 0),
            "longitude": result.get("position", {}).get("lng", 0),
            "land_value_score": result["prediction"].get("land_value_score", 0),
            "nutrients": result["prediction"].get("nutrients", {}),
            "contamination_detected": result["prediction"].get("contamination_detected", False),
            "spectral_hash": result["prediction"].get("spectral_hash", ""),
            "model_version": result["prediction"].get("model_version", ""),
            "drift_e": result.get("drift_e", 0),
            "drift_flag": result.get("drift_flag", False),
        }
        
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        
        for attempt in range(3):
            try:
                resp = requests.post(
                    f"{self.base_url}/api/syndar/ingest",
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                resp.raise_for_status()
                return {"success": True, "scan_id": result.get("task_id")}
            except Exception as e:
                if attempt == 2:
                    return {"success": False, "error": str(e)}
                time.sleep(2 ** attempt)
```

```python
# simulate_fleet.py
#!/usr/bin/env python3
"""Run minimal fleet simulation"""

import time
import random
import threading

from syndar_simple.mesh import SimpleMesh
from syndar_simple.task_allocator import assign_task_simple
from syndar_simple.node_agent import SimpleNodeAgent
from syndar_simple.aip_bridge import SimpleAIPBridge
from syndar_simple.drift_monitor import SimpleDriftMonitor

# Mock clients
class MockHSI:
    def predict(self, cube):
        return {
            "field_id": cube["field_id"],
            "nutrients": {"nitrogen": random.uniform(0.5, 1.0), "carbon": random.uniform(1.5, 3.0)},
            "land_value_score": random.uniform(0.6, 0.95),
            "contamination_detected": random.random() < 0.1,
            "spectral_hash": "mock_hash_123",
            "model_version": "v0.1.0"
        }

class MockArthedain:
    def get_drift_e(self):
        return random.random()

def main():
    # Setup
    mesh = SimpleMesh()
    hsi = MockHSI()
    arthedain = MockArthedain()
    aip = SimpleAIPBridge("http://localhost:3000")
    drift_monitor = SimpleDriftMonitor(threshold=0.5, min_nodes=3)
    
    # Create 3 drones
    drones = []
    for i in range(3):
        drone_id = f"drone-{i+1}"
        # Initialize position
        mesh.update(drone_id, {
            "drone_id": drone_id,
            "position": {"lat": 39.0 + i*0.01, "lng": -77.0, "alt": 50},
            "status": "idle",
            "current_task_id": None,
            "battery_pct": 100
        })
        
        # Start agent
        agent = SimpleNodeAgent(drone_id, mesh, hsi, arthedain)
        agent.start()
        drones.append(agent)
    
    # Subscribe results to AIP + drift monitor
    def on_result(result):
        print(f"Result from {result['drone_id']}: land_value={result['prediction']['land_value_score']:.2f}")
        
        # Send to AIP
        aip.send(result)
        
        # Check drift
        alert = drift_monitor.report(
            result["drone_id"],
            result["field_id"],
            result["drift_e"]
        )
        if alert:
            print(f"🚨 DRIFT ALERT: {alert}")
    
    mesh.subscribe_to_results(on_result)
    
    # Create tasks
    tasks = [
        {"id": "task-1", "field_id": "field-001", "location": {"lat": 39.0, "lng": -77.0}},
        {"id": "task-2", "field_id": "field-002", "location": {"lat": 39.02, "lng": -77.0}},
        {"id": "task-3", "field_id": "field-003", "location": {"lat": 39.04, "lng": -77.0}},
    ]
    
    # Assign tasks
    for task in tasks:
        drone_state = assign_task_simple(task, mesh)
        if drone_state:
            drone_id = drone_state["drone_id"]
            mesh.update(drone_id, {
                **drone_state,
                "status": "assigned",
                "current_task_id": task["id"],
                "current_task": task
            })
            print(f"Assigned {task['id']} to {drone_id}")
        else:
            print(f"No drone available for {task['id']}")
    
    # Run for 30 seconds
    print("\nRunning simulation...")
    time.sleep(30)
    
    # Stop
    for d in drones:
        d.running = False
    
    print("\nDone!")

if __name__ == "__main__":
    main()
```

---

## Run It

```bash
# 1. Start mock AIP server (in terminal 1)
python -m sandbox.mock_aip_server --port 3000

# 2. Run simulation (in terminal 2)
python SYNDAR_MVP.py

# 3. Check AIP received data
curl http://localhost:3000/api/syndar/stats
```

---

## What You Get

✅ Drones accept tasks  
✅ Drones execute scan loop  
✅ Results POST to AIP  
✅ Basic drift detection  
✅ End-to-end working system  

**Total code:** ~300 lines vs 3000+ lines in full version.

---

## Next Steps (After MVP Works)

1. Replace mock HSI with real Hyperspectral-Restruct API
2. Replace mock Arthedain with real SNN client
3. Add PGP signing to AIP payloads
4. Add real drone flight controller interface
5. Improve task allocation (auctions, preemption)
6. Add gossip mesh for distributed operation
7. Add gRPC for performance
8. Add WebSocket streaming for dashboards

But only after the MVP works end-to-end.
