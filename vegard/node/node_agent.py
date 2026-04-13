"""NodeAgent - Drone ↔ fabric bridge

Receives scan tasks from fabric, executes hyperspectral capture sequence,
processes through Arthedain SNN, gets soil predictions from HSI model,
publishes signed EntityState to mesh.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import structlog
from pydantic import BaseModel

from vegard.fabric.attestation import AttestationService, SignedPayload
from vegard.fabric.mesh import EntityState, Position
from vegard.fabric.task_allocator import TaskProgress, TaskResult
from vegard.fabric.transport import TransportBus

logger = structlog.get_logger()


class DroneConfig(BaseModel):
    """Drone hardware configuration"""

    max_flight_speed_m_s: float = 20.0
    max_altitude_m: float = 120.0
    battery_capacity_wh: float = 500.0
    power_consumption_hover_w: float = 200.0
    power_consumption_flight_w: float = 300.0
    spectral_payload_mass_kg: float = 2.0


@dataclass
class NodeAgentConfig:
    """Node agent configuration"""

    region: str = "us-east"
    serial: str = "001"
    enable_attestation: bool = True
    drift_threshold: float = 0.5
    auto_accept_tasks: bool = True
    update_interval_ms: int = 1000


class NodeAgent:
    """Drone node agent - bridges drone hardware to Vegard fabric"""

    def __init__(
        self,
        config: NodeAgentConfig,
        drone_config: DroneConfig,
        transport: TransportBus,
        attestation: Optional[AttestationService] = None,
        arthedain_client=None,
        hsi_client=None,
    ):
        self.config = config
        self.drone_config = drone_config
        self.transport = transport
        self.attestation = attestation
        self._arthedain = arthedain_client
        self._hsi = hsi_client

        self.entity_id = f"drone:{config.region}:{config.serial}"
        self._position = Position(lat=0.0, lng=0.0, alt=0.0)
        self._battery_pct = 100.0
        self._current_task_id: Optional[str] = None
        self._drift_e: float = 0.0
        self._drift_flag: bool = False

        self._running = False
        self._update_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start node agent"""
        self._running = True

        # Generate or load identity
        if self.attestation and self.config.enable_attestation:
            identity = self.attestation.load_identity(self.entity_id)
            if not identity:
                identity = self.attestation.generate_identity(
                    self.config.region, self.config.serial
                )
            logger.info(
                "Node identity loaded",
                entity_id=self.entity_id,
                fingerprint=identity.fingerprint,
            )

        # Subscribe to task channel
        self.transport.subscribe(f"tasks/{self.entity_id}", self._on_task_message)

        # Start update loop
        self._update_task = asyncio.create_task(self._update_loop())

        logger.info("Node agent started", entity_id=self.entity_id)

    async def stop(self) -> None:
        """Stop node agent"""
        self._running = False
        if self._update_task:
            self._update_task.cancel()
        self.transport.unsubscribe(f"tasks/{self.entity_id}", self._on_task_message)
        logger.info("Node agent stopped", entity_id=self.entity_id)

    async def _update_loop(self) -> None:
        """Periodic state publication"""
        while self._running:
            try:
                await asyncio.sleep(self.config.update_interval_ms / 1000)
                await self._publish_state()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Update loop error", entity_id=self.entity_id)

    async def _publish_state(self) -> None:
        """Publish current entity state to mesh"""
        # Get soil prediction from last scan
        soil = None
        if self._hsi:
            try:
                soil = await self._hsi.get_last_prediction()
            except Exception:
                pass

        # Get drift signal from Arthedain
        if self._arthedain:
            try:
                drift_signal = await self._arthedain.get_drift_signal()
                self._drift_e = drift_signal.combined_e
                self._drift_flag = drift_signal.exceeded
            except Exception:
                pass

        entity = EntityState(
            entity_id=self.entity_id,
            entity_type="drone",
            position=self._position,
            soil=soil,
            drift_score=self._drift_e,
            drift_flag=self._drift_flag,
            battery_pct=self._battery_pct,
            task_id=self._current_task_id,
            timestamp_ms=int(time.time() * 1000),
        )

        # Sign if attestation enabled
        if self.attestation and self.config.enable_attestation:
            try:
                payload = self._entity_to_signable(entity)
                signed = self.attestation.sign(self.entity_id, payload)
                entity.signature = signed.signature
            except Exception as e:
                logger.warning("Failed to sign entity state", error=str(e))

        # Publish
        await self.transport.publish(
            f"mesh/entities/{self.entity_id}", entity.model_dump()
        )

    def _entity_to_signable(self, entity: EntityState) -> str:
        """Convert entity to signable string"""
        import json

        return json.dumps(
            {
                "entity_id": entity.entity_id,
                "position": {
                    "lat": entity.position.lat,
                    "lng": entity.position.lng,
                    "alt": entity.position.alt,
                },
                "timestamp_ms": entity.timestamp_ms,
                "drift_score": entity.drift_score,
            },
            sort_keys=True,
        )

    def _on_task_message(self, payload: dict) -> None:
        """Handle incoming task message"""
        logger.info("Received task message", entity_id=self.entity_id, payload=payload)

        # TODO: Parse task and decide whether to bid
        if self.config.auto_accept_tasks:
            asyncio.create_task(self._evaluate_task(payload))

    async def _evaluate_task(self, task_payload: dict) -> None:
        """Evaluate and bid on a task"""
        from vegard.fabric.task_allocator import TaskBid, TaskRequest

        # Parse task request
        try:
            task = TaskRequest(**task_payload)
        except Exception as e:
            logger.error("Failed to parse task", error=str(e))
            return

        # Calculate bid
        current_entity = EntityState(
            entity_id=self.entity_id,
            position=self._position,
            battery_pct=self._battery_pct,
            task_id=self._current_task_id,
        )

        # TODO: Import TaskAllocator to calculate bid
        # For now, calculate manually
        bid = self._calculate_bid(current_entity, task)

        # Submit bid
        await self.transport.publish(
            f"bids/{task.task_id}", bid.model_dump(), protocol="mqtt"
        )

        logger.info(
            "Submitted bid",
            task_id=task.task_id,
            bid_cost=bid.bid_cost,
            entity_id=self.entity_id,
        )

    def _calculate_bid(
        self, entity: EntityState, task: "TaskRequest"
    ) -> "TaskBid":
        """Calculate bid cost for a task"""
        from vegard.fabric.task_allocator import TaskBid

        # Calculate distance to task center
        if task.target_polygon:
            lats = [p[0] for p in task.target_polygon]
            lngs = [p[1] for p in task.target_polygon]
            center_lat = sum(lats) / len(lats)
            center_lng = sum(lngs) / len(lngs)
        else:
            center_lat, center_lng = 0.0, 0.0

        dist_deg = (
            (entity.position.lat - center_lat) ** 2
            + (entity.position.lng - center_lng) ** 2
        ) ** 0.5
        dist_m = dist_deg * 111000

        # Calculate costs
        travel_time_s = dist_m / 15.0
        est_battery_drain = task.estimated_duration_s / 3600 * 20
        battery_cost = max(0, 100 - (entity.battery_pct - est_battery_drain))
        load_cost = 0.0 if not entity.task_id else 50.0
        priority_bonus = -task.priority * 20.0

        total_cost = travel_time_s + battery_cost + load_cost + priority_bonus

        return TaskBid(
            task_id=task.task_id,
            entity_id=self.entity_id,
            bid_cost=total_cost,
            estimated_arrival_s=travel_time_s,
            battery_at_completion_pct=entity.battery_pct - est_battery_drain,
            current_position=entity.position,
        )

    async def execute_scan(
        self,
        task_id: str,
        field_id: str,
        polygon: list[tuple[float, float]],
        spectral_config: Optional[dict] = None,
    ) -> TaskResult:
        """Execute a field scan task"""
        logger.info(
            "Starting scan execution",
            task_id=task_id,
            field_id=field_id,
            entity_id=self.entity_id,
        )

        self._current_task_id = task_id
        start_time = time.time()

        try:
            # 1. Navigate to field
            await self._navigate_to_field(polygon)

            # 2. Execute flight pattern
            await self._execute_flight_pattern(polygon, spectral_config or {})

            # 3. Process spectral data
            soil_prediction = await self._process_spectral_data(field_id)

            # 4. Return result
            duration = time.time() - start_time
            battery_drain = duration / 3600 * 20

            return TaskResult(
                task_id=task_id,
                entity_id=self.entity_id,
                success=True,
                completed_at_ms=int(time.time() * 1000),
                battery_at_completion_pct=self._battery_pct - battery_drain,
                final_position=self._position,
            )

        except Exception as e:
            logger.exception("Scan execution failed", task_id=task_id)
            return TaskResult(
                task_id=task_id,
                entity_id=self.entity_id,
                success=False,
                failure_reason=str(e),
                completed_at_ms=int(time.time() * 1000),
                battery_at_completion_pct=self._battery_pct,
                final_position=self._position,
            )
        finally:
            self._current_task_id = None

    async def _navigate_to_field(self, polygon: list[tuple[float, float]]) -> None:
        """Navigate drone to field boundary"""
        if not polygon:
            return

        # Navigate to first point
        target = polygon[0]
        logger.info("Navigating to field", target=target)

        # Simulate navigation (in real implementation, interface to flight controller)
        await asyncio.sleep(2.0)  # Simulated travel time

        self._position = Position(lat=target[0], lng=target[1], alt=50.0)
        self._battery_pct -= 2.0

    async def _execute_flight_pattern(
        self, polygon: list[tuple[float, float]], config: dict
    ) -> None:
        """Execute lawnmower scan pattern over field"""
        if not polygon:
            return

        logger.info("Executing flight pattern", points=len(polygon))

        # Simulate scan pattern
        for i, point in enumerate(polygon[1:]):
            # Move to next point
            self._position = Position(lat=point[0], lng=point[1], alt=50.0)
            self._battery_pct -= 0.5

            # Capture spectral data (simulated)
            await asyncio.sleep(0.1)

            # Report progress
            progress = (i + 1) / len(polygon) * 100
            await self._report_progress(self._current_task_id, progress)

    async def _report_progress(self, task_id: str, progress_pct: float) -> None:
        """Report task progress"""
        progress = TaskProgress(
            task_id=task_id,
            entity_id=self.entity_id,
            status="scanning",
            progress_pct=progress_pct,
            current_position=self._position,
        )
        await self.transport.publish(
            f"progress/{task_id}", progress.model_dump(), protocol="mqtt"
        )

    async def _process_spectral_data(self, field_id: str) -> Optional[dict]:
        """Process spectral data through Arthedain + HSI model"""
        # TODO: Implement actual spectral processing pipeline
        # 1. Get spike stream from Arthedain
        # 2. Buffer into spectral cube via SpectralBridge
        # 3. Call HSI model for soil prediction
        # 4. Sign and return prediction

        logger.info("Processing spectral data (stub)", field_id=field_id)

        # Return stub prediction
        return {
            "field_id": field_id,
            "nutrients": {"nitrogen": 0.8, "carbon": 2.5},
            "land_value_score": 0.75,
            "contamination_detected": False,
        }

    def get_status(self) -> dict:
        """Get current node status"""
        return {
            "entity_id": self.entity_id,
            "position": {
                "lat": self._position.lat,
                "lng": self._position.lng,
                "alt": self._position.alt,
            },
            "battery_pct": self._battery_pct,
            "current_task": self._current_task_id,
            "drift_e": self._drift_e,
            "drift_flag": self._drift_flag,
        }
