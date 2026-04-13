"""Fleet Simulator - Syndar Sandbox

Simulate N virtual drones scanning a field polygon.
Tests task allocation, mesh gossip, drift correlation without hardware.

Usage:
    python sandbox/simulate_fleet.py --drones 4 --field configs/test_field.geojson
"""

import argparse
import asyncio
import json
import random
import time
from pathlib import Path
from typing import Optional

import structlog

# Setup logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.dev.ConsoleRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def simulate_fleet(
    drone_count: int = 4,
    field_path: Optional[Path] = None,
    duration_s: float = 300.0,
) -> None:
    """Run fleet simulation"""
    logger.info(
        "Starting fleet simulation",
        drones=drone_count,
        field=field_path,
        duration=duration_s,
    )

    # Load field polygon
    field_polygon = await load_field_polygon(field_path)

    # Create simulated mesh
    from syndar.fabric.mesh import Mesh, MeshConfig
    from syndar.fabric.task_allocator import TaskAllocator
    from syndar.fabric.drift_monitor import DriftMonitor, DriftThresholds
    from syndar.fabric.transport import TransportBus, TransportConfig

    # Setup components
    mesh_config = MeshConfig(fanout=3, gossip_interval_ms=500)
    mesh = Mesh(config=mesh_config)

    transport = TransportBus(config=TransportConfig(use_mqtt=False))
    allocator = TaskAllocator()
    drift_monitor = DriftMonitor(thresholds=DriftThresholds(per_node_e_threshold=0.4))

    # Start mesh
    await mesh.start()
    await transport.start()

    # Create simulated drones
    drones = []
    for i in range(drone_count):
        drone = SimulatedDrone(
            entity_id=f"drone:us-east:{i+1:03d}",
            mesh=mesh,
            allocator=allocator,
            drift_monitor=drift_monitor,
            start_pos=field_polygon[0] if field_polygon else (39.0, -77.0),
        )
        drones.append(drone)
        await drone.start()

    logger.info("Fleet initialized", drone_count=len(drones))

    # Create a mission
    from syndar.command.mission import MissionPlanner, FieldInfo

    mission_planner = MissionPlanner(task_allocator=allocator)

    field = FieldInfo(
        field_id="test-field-001",
        polygon=field_polygon or generate_test_polygon(),
        contract_value=100000.0,
        contract_priority=0.8,
        uncertainty_score=0.6,
        contamination_risk=0.2,
    )

    mission_id = await mission_planner.plan_mission(
        name="Test Field Survey",
        fields=[field],
    )

    logger.info("Mission planned", mission_id=mission_id)

    # Run simulation
    start_time = time.time()
    iteration = 0

    try:
        while time.time() - start_time < duration_s:
            await asyncio.sleep(1.0)
            iteration += 1

            # Print status every 10 seconds
            if iteration % 10 == 0:
                fop = await mesh.get_fused_picture()
                stats = await allocator.get_stats()
                drift_stats = await drift_monitor.get_stats()

                logger.info(
                    "Simulation status",
                    iteration=iteration,
                    drones=fop["drone_count"],
                    active_tasks=fop["active_tasks"],
                    drift_alerts=fop["drift_alerts"],
                    tasks_completed=stats["completed_tasks"],
                )

    except asyncio.CancelledError:
        logger.info("Simulation cancelled")
    finally:
        # Cleanup
        for drone in drones:
            await drone.stop()

        await mesh.stop()
        await transport.stop()

        logger.info("Fleet simulation complete")

        # Print final stats
        print("\n" + "=" * 50)
        print("FINAL STATISTICS")
        print("=" * 50)
        drift_stats = await drift_monitor.get_stats()
        for key, value in drift_stats.items():
            print(f"  {key}: {value}")


class SimulatedDrone:
    """Virtual drone for simulation"""

    def __init__(
        self,
        entity_id: str,
        mesh,
        allocator,
        drift_monitor,
        start_pos: tuple[float, float],
    ):
        self.entity_id = entity_id
        self.mesh = mesh
        self.allocator = allocator
        self.drift_monitor = drift_monitor

        from syndar.fabric.mesh import EntityState, Position, SoilPrediction

        self.position = Position(lat=start_pos[0], lng=start_pos[1], alt=50.0)
        self.battery_pct = 100.0
        self.current_task_id: Optional[str] = None
        self.running = False
        self.update_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start drone simulation"""
        self.running = True
        self.update_task = asyncio.create_task(self._update_loop())
        logger.debug("Drone started", entity_id=self.entity_id)

    async def stop(self):
        """Stop drone simulation"""
        self.running = False
        if self.update_task:
            self.update_task.cancel()
            try:
                await self.update_task
            except asyncio.CancelledError:
                pass
        logger.debug("Drone stopped", entity_id=self.entity_id)

    async def _update_loop(self):
        """Periodic state update"""
        while self.running:
            try:
                await asyncio.sleep(2.0)

                # Update position (random walk)
                self.position.lat += random.gauss(0, 0.001)
                self.position.lng += random.gauss(0, 0.001)

                # Battery drain
                self.battery_pct -= 0.5
                if self.battery_pct < 0:
                    self.battery_pct = 100.0  # Recharge for simulation

                # Check for assigned tasks
                assignment = await self.allocator.get_assignment(self.entity_id)
                if assignment:
                    self.current_task_id = assignment.task_id
                else:
                    self.current_task_id = None

                # Generate drift signal
                drift_e = random.random()
                drift_exceeded = drift_e > 0.5

                from syndar.fabric.drift_monitor import NodeDriftSignal

                signal = NodeDriftSignal(
                    entity_id=self.entity_id,
                    e_fast=random.random(),
                    e_slow=random.random(),
                    combined_e=drift_e,
                    threshold=0.5,
                    exceeded=drift_exceeded,
                    lat=self.position.lat,
                    lng=self.position.lng,
                    field_id="test-field-001",
                    task_id=self.current_task_id or "",
                )
                await self.drift_monitor.report_signal(signal)

                # Create soil prediction (sometimes)
                soil = None
                if random.random() < 0.1:  # 10% chance per update
                    from syndar.fabric.mesh import SoilPrediction

                    soil = SoilPrediction(
                        field_id="test-field-001",
                        nutrients={
                            "nitrogen": random.uniform(0.5, 1.0),
                            "carbon": random.uniform(1.5, 3.0),
                        },
                        land_value_score=random.uniform(0.6, 0.95),
                        contamination_detected=random.random() < 0.05,
                    )

                # Publish entity state
                from syndar.fabric.mesh import EntityState

                entity = EntityState(
                    entity_id=self.entity_id,
                    entity_type="drone",
                    position=self.position,
                    soil=soil,
                    drift_score=drift_e,
                    drift_flag=drift_exceeded,
                    battery_pct=self.battery_pct,
                    task_id=self.current_task_id,
                )
                await self.mesh.store.update(entity)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Drone update error", entity_id=self.entity_id)


async def load_field_polygon(field_path: Optional[Path]) -> list[tuple[float, float]]:
    """Load field polygon from GeoJSON"""
    if not field_path or not field_path.exists():
        return generate_test_polygon()

    try:
        with open(field_path) as f:
            data = json.load(f)

        # Extract coordinates from GeoJSON
        if data.get("type") == "FeatureCollection":
            coords = data["features"][0]["geometry"]["coordinates"][0]
        elif data.get("type") == "Feature":
            coords = data["geometry"]["coordinates"][0]
        else:
            coords = data["coordinates"][0]

        return [(lat, lng) for lng, lat in coords]  # GeoJSON is [lng, lat]

    except Exception as e:
        logger.error("Failed to load field polygon", error=str(e))
        return generate_test_polygon()


def generate_test_polygon() -> list[tuple[float, float]]:
    """Generate a test field polygon"""
    # Simple square around Maryland test coordinates
    base_lat, base_lng = 39.0, -77.0
    size = 0.01
    return [
        (base_lat, base_lng),
        (base_lat + size, base_lng),
        (base_lat + size, base_lng + size),
        (base_lat, base_lng + size),
        (base_lat, base_lng),
    ]


async def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Syndar Fleet Simulator")
    parser.add_argument(
        "--drones",
        type=int,
        default=4,
        help="Number of simulated drones",
    )
    parser.add_argument(
        "--field",
        type=Path,
        default=None,
        help="Path to GeoJSON field polygon",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=300.0,
        help="Simulation duration in seconds",
    )

    args = parser.parse_args()

    await simulate_fleet(
        drone_count=args.drones,
        field_path=args.field,
        duration_s=args.duration,
    )


if __name__ == "__main__":
    asyncio.run(main())
