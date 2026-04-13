"""Mission Planner - Goal allocation and scan priority

Converts AIP's farm contracting goals into concrete scan tasks.
Generates scan priorities based on contract value, uncertainty, contamination risk.
"""

import time
from dataclasses import dataclass
from typing import Optional

import structlog
from pydantic import BaseModel, Field

from vegard.fabric.task_allocator import (
    SpectralConfig,
    TaskAllocator,
    TaskRequest,
)

logger = structlog.get_logger()


class FieldInfo(BaseModel):
    """Field information from AIP"""

    field_id: str
    polygon: list[tuple[float, float]]
    contract_value: float = 0.0
    contract_priority: float = 0.5
    uncertainty_score: float = 0.5
    contamination_risk: float = 0.1
    days_since_last_scan: Optional[int] = None
    target_crop: str = "hemp"


class MissionConfig(BaseModel):
    """Mission planner configuration"""

    default_spectral: SpectralConfig = Field(default_factory=SpectralConfig)
    base_scan_duration_s: int = 600
    priority_formula: str = "contract * uncertainty * (1 + contamination)"
    max_concurrent_tasks: int = 10
    deadline_buffer_s: int = 300


@dataclass
class MissionPlannerState:
    """Mission planner state"""

    missions_created: int = 0
    tasks_generated: int = 0
    fields_prioritized: int = 0


class MissionPlanner:
    """Converts AIP goals into concrete scan tasks"""

    def __init__(
        self,
        task_allocator: TaskAllocator,
        config: Optional[MissionConfig] = None,
    ):
        self.task_allocator = task_allocator
        self.config = config or MissionConfig()
        self.state = MissionPlannerState()
        self._missions: dict[str, dict] = {}

    def calculate_priority(self, field: FieldInfo) -> float:
        """Calculate scan priority for a field"""
        # Priority formula: contract value × uncertainty × (1 + contamination risk)
        # Higher = more urgent
        base_priority = (
            field.contract_priority * field.uncertainty_score * (1 + field.contamination_risk)
        )

        # Boost for stale data
        if field.days_since_last_scan is not None:
            if field.days_since_last_scan > 30:
                base_priority *= 1.5
            elif field.days_since_last_scan > 14:
                base_priority *= 1.2

        # Cap at 1.0
        return min(1.0, base_priority)

    async def plan_mission(
        self,
        name: str,
        fields: list[FieldInfo],
        mission_id: Optional[str] = None,
    ) -> str:
        """Plan a new mission for field scanning"""
        mission_id = mission_id or f"mission-{int(time.time() * 1000)}"

        # Calculate priorities
        prioritized_fields = []
        for field in fields:
            priority = self.calculate_priority(field)
            prioritized_fields.append((field, priority))

        # Sort by priority (descending)
        prioritized_fields.sort(key=lambda x: x[1], reverse=True)

        # Generate tasks
        task_ids = []
        for field, priority in prioritized_fields:
            task = await self._generate_task(field, priority, mission_id)
            if task:
                task_ids.append(task.task_id)
                self.state.tasks_generated += 1

        # Store mission
        self._missions[mission_id] = {
            "mission_id": mission_id,
            "name": name,
            "task_ids": task_ids,
            "field_count": len(fields),
            "created_at_ms": int(time.time() * 1000),
            "status": "active",
        }

        self.state.missions_created += 1
        self.state.fields_prioritized += len(fields)

        logger.info(
            "Mission planned",
            mission_id=mission_id,
            name=name,
            tasks=len(task_ids),
            fields=len(fields),
        )

        return mission_id

    async def _generate_task(
        self,
        field: FieldInfo,
        priority: float,
        mission_id: str,
    ) -> Optional[TaskRequest]:
        """Generate scan task for a field"""
        # Calculate scan parameters based on field properties
        spectral = self._configure_spectral(field)

        # Estimate duration based on polygon area
        duration = self._estimate_duration(field.polygon)

        # Set deadline
        deadline_ms = int(time.time() * 1000) + (duration + self.config.deadline_buffer_s) * 1000

        task = TaskRequest(
            task_id=f"task-{field.field_id}-{int(time.time() * 1000)}",
            field_id=field.field_id,
            target_polygon=field.polygon,
            priority=priority,
            deadline_ms=deadline_ms,
            estimated_duration_s=duration,
            spectral=spectral,
            mission_id=mission_id,
            requested_by="mission_planner",
            preemptible=priority < 0.7,  # High priority tasks can't be preempted
        )

        # Publish task to allocator
        await self.task_allocator.publish_task(task)

        return task

    def _configure_spectral(self, field: FieldInfo) -> SpectralConfig:
        """Configure spectral settings for field"""
        spectral = SpectralConfig()

        # Adjust based on contamination risk
        if field.contamination_risk > 0.5:
            # Higher resolution for contaminated fields
            spectral.resolution_m = 0.3
            spectral.exposure_ms = 15.0

        # Adjust based on crop type
        if field.target_crop == "hemp":
            # Hemp requires specific band range focus
            spectral.band_start_nm = 400
            spectral.band_end_nm = 2500

        return spectral

    def _estimate_duration(self, polygon: list[tuple[float, float]]) -> int:
        """Estimate scan duration from polygon area"""
        if len(polygon) < 3:
            return self.config.base_scan_duration_s

        # Rough area calculation using shoelace formula
        area = 0.0
        n = len(polygon)
        for i in range(n):
            j = (i + 1) % n
            area += polygon[i][0] * polygon[j][1]
            area -= polygon[j][0] * polygon[i][1]
        area = abs(area) / 2.0

        # Convert to approximate square meters (rough conversion)
        area_m2 = area * 111000 * 111000

        # Estimate: 1 hectare takes ~10 minutes at 50m altitude
        duration_per_hectare = 600  # seconds
        hectares = area_m2 / 10000

        return int(max(self.config.base_scan_duration_s, hectares * duration_per_hectare))

    async def get_mission_status(self, mission_id: str) -> Optional[dict]:
        """Get mission status"""
        mission = self._missions.get(mission_id)
        if not mission:
            return None

        # Query task allocator for actual status
        completed_count = 0
        total_tasks = len(mission["task_ids"])

        for task_id in mission["task_ids"]:
            task = self.task_allocator.get_task(task_id)
            if task and task.status in ["complete"]:
                completed_count += 1

        completion_pct = (completed_count / total_tasks * 100) if total_tasks > 0 else 0.0

        return {
            **mission,
            "tasks_total": total_tasks,
            "tasks_completed": completed_count,
            "estimated_completion_pct": completion_pct,
        }

    async def cancel_mission(self, mission_id: str) -> bool:
        """Cancel a mission and its tasks"""
        mission = self._missions.get(mission_id)
        if not mission:
            return False

        mission["status"] = "cancelled"

        # Cancel all tasks through task allocator
        for task_id in mission["task_ids"]:
            await self.task_allocator.cancel_task(task_id)

        logger.info("Mission cancelled", mission_id=mission_id)
        return True

    def get_active_missions(self) -> list[dict]:
        """Get list of active missions"""
        return [
            m for m in self._missions.values() if m["status"] == "active"
        ]

    def get_stats(self) -> dict:
        """Get planner statistics"""
        return {
            "missions_created": self.state.missions_created,
            "tasks_generated": self.state.tasks_generated,
            "fields_prioritized": self.state.fields_prioritized,
            "active_missions": len(self.get_active_missions()),
        }
