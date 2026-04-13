#!/usr/bin/env python3
"""MVP Fleet Simulator - Runs end-to-end in seconds

Usage:
    python -m syndar.mvp.simulate

This spins up fake drones, assigns tasks, runs the scan loop,
and delivers results to AIP.
"""

import time
import random
import argparse
from typing import Optional


def run_simulation(
    num_drones: int = 3,
    num_tasks: int = 3,
    duration_s: float = 30.0,
    aip_url: str = "http://localhost:3000"
) -> None:
    """Run minimal fleet simulation"""
    
    print(f"\n🚁 Vegard MVP Simulation")
    print(f"   Drones: {num_drones}")
    print(f"   Tasks: {num_tasks}")
    print(f"   AIP: {aip_url}")
    print(f"   Duration: {duration_s}s")
    print("=" * 50)
    
    # Import MVP modules
    from syndar.mvp.mesh import SimpleMesh, Position
    from syndar.mvp.task_allocator import Task, SimpleTaskAllocator
    from syndar.mvp.node_agent import SimpleNodeAgent
    from syndar.mvp.aip_bridge import SimpleAIPBridge
    from syndar.mvp.drift_monitor import SimpleDriftMonitor
    
    # Setup
    mesh = SimpleMesh()
    allocator = SimpleTaskAllocator(mesh)
    aip = SimpleAIPBridge(aip_url)
    drift_monitor = SimpleDriftMonitor(threshold=0.5, min_nodes=3)
    
    # Check AIP health
    print("\n📡 Checking AIP...")
    if aip.check_health():
        print("   ✓ AIP reachable")
    else:
        print("   ⚠ AIP not responding (will queue locally)")
    
    # Create mock clients
    class MockHSI:
        """Mock Hyperspectral-Restruct client"""
        def predict(self, cube: dict) -> dict:
            return {
                "field_id": cube.get("field_id", "unknown"),
                "nutrients": {
                    "nitrogen": round(random.uniform(0.5, 1.0), 2),
                    "carbon": round(random.uniform(1.5, 3.0), 2),
                },
                "land_value_score": round(random.uniform(0.6, 0.95), 2),
                "remediation_priority": round(random.uniform(0.1, 0.5), 2),
                "contamination_detected": random.random() < 0.1,
                "spectral_hash": f"hash_{random.randint(1000, 9999)}",
                "model_version": "v0.1.0-mock"
            }
    
    class MockArthedain:
        """Mock Arthedain client - generates drift signals"""
        def __init__(self, drone_id: str):
            self.drone_id = drone_id
        
        def get_drift_e(self) -> float:
            # Simulate: some drones have correlated drift
            base = 0.3
            if "drone-1" in self.drone_id or "drone-2" in self.drone_id:
                base = 0.65  # Higher drift for first 2 drones
            return base + random.gauss(0, 0.1)
    
    hsi = MockHSI()
    
    # Create drones
    print(f"\n🛸 Initializing {num_drones} drones...")
    drones = []
    for i in range(num_drones):
        drone_id = f"drone-{i+1}"
        lat = 39.0 + i * 0.01
        lng = -77.0
        
        agent = SimpleNodeAgent(
            drone_id=drone_id,
            mesh=mesh,
            hsi_client=hsi,
            arthedain_client=MockArthedain(drone_id),
            start_position=(lat, lng, 50.0)
        )
        agent.start()
        drones.append(agent)
        print(f"   ✓ {drone_id} at ({lat:.3f}, {lng:.3f})")
    
    # Setup result pipeline: mesh -> AIP + drift monitor
    results_sent = 0
    alerts_triggered = 0
    
    def on_result(result: dict):
        nonlocal results_sent, alerts_triggered
        
        drone_id = result.get("drone_id")
        field_id = result.get("field_id")
        prediction = result.get("prediction", {})
        drift_e = result.get("drift_e", 0.0)
        
        print(f"\n📊 Result from {drone_id}")
        print(f"   Field: {field_id}")
        print(f"   Land Value: {prediction.get('land_value_score', 0):.2f}")
        print(f"   Drift E: {drift_e:.2f} {'⚠' if drift_e > 0.5 else '✓'}")
        
        # Send to AIP
        send_result = aip.send(result)
        if send_result["success"]:
            results_sent += 1
            print(f"   ✓ Sent to AIP")
        else:
            print(f"   ✗ AIP failed: {send_result.get('error', 'unknown')}")
        
        # Check drift
        alert = drift_monitor.report(drone_id, field_id, drift_e)
        if alert:
            alerts_triggered += 1
            print(f"\n🚨 DRIFT ALERT: {alert['message']}")
    
    mesh.subscribe_to_results(on_result)
    
    # Create tasks
    print(f"\n📋 Creating {num_tasks} tasks...")
    tasks = []
    for i in range(num_tasks):
        task = Task(
            task_id=f"task-{i+1}",
            field_id=f"field-{i+1:03d}",
            lat=39.0 + i * 0.015,
            lng=-77.0,
            priority=0.7
        )
        allocator.add_task(task)
        tasks.append(task)
        print(f"   ✓ {task.id} -> {task.field_id}")
    
    # Assign tasks
    print("\n🎯 Assigning tasks...")
    assignments = allocator.process_pending()
    for task, drone in assignments:
        print(f"   ✓ {task.id} -> {drone.drone_id}")
    
    if len(assignments) < len(tasks):
        print(f"   ⚠ {len(tasks) - len(assignments)} tasks unassigned (no drones available)")
    
    # Run
    print(f"\n⏳ Running simulation for {duration_s}s...")
    print("-" * 50)
    
    start = time.time()
    try:
        while time.time() - start < duration_s:
            # Process any new pending tasks
            allocator.process_pending()
            time.sleep(0.5)
            
    except KeyboardInterrupt:
        print("\n\n⚠ Interrupted by user")
    
    # Cleanup
    print("\n" + "=" * 50)
    print("🛑 Stopping drones...")
    for d in drones:
        d.stop()
    
    # Final stats
    print("\n📈 Final Statistics")
    print("-" * 50)
    print(f"   Results sent to AIP: {results_sent}")
    print(f"   Drift alerts: {alerts_triggered}")
    print(f"   Fleet status: {mesh.get_fleet_status()}")
    print(f"   AIP bridge: {aip.get_stats()}")
    print(f"   Drift monitor: {drift_monitor.get_stats()}")
    
    # Check AIP received data
    try:
        import requests
        resp = requests.get(f"{aip_url}/api/syndar/stats", timeout=5)
        if resp.status_code == 200:
            stats = resp.json()
            print(f"\n📡 AIP Server Stats:")
            for k, v in stats.items():
                print(f"   {k}: {v}")
    except Exception as e:
        print(f"\n⚠ Could not fetch AIP stats: {e}")
    
    print("\n✅ Simulation complete!")
    print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Vegard MVP Fleet Simulator")
    parser.add_argument("--drones", type=int, default=3, help="Number of drones")
    parser.add_argument("--tasks", type=int, default=3, help="Number of tasks")
    parser.add_argument("--duration", type=float, default=30.0, help="Duration in seconds")
    parser.add_argument("--aip-url", default="http://localhost:3000", help="AIP server URL")
    
    args = parser.parse_args()
    
    run_simulation(
        num_drones=args.drones,
        num_tasks=args.tasks,
        duration_s=args.duration,
        aip_url=args.aip_url
    )


if __name__ == "__main__":
    main()
