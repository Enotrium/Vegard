"""Auction-based task allocator - distributes scan tasks across fleet

No central coordination - drones bid based on local state.
Supports preemption for priority tasks.
"""

import asyncio
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

import structlog
from pydantic import BaseModel, Field

from syndar.logging_config import get_logger, bind_context
from syndar.fabric.mesh import EntityState, Position

logger = get_logger(__name__)

class SpectralConfig(BaseModel):
    resolution_m: float = 0.5
    altitude_m: float = 50.0
    band_start_nm: int = 400
    band_end_nm: int = 2500
    band_count: int = 200
    exposure_ms: float = 10.0


class TaskRequest(BaseModel):
    task_id: str
    field_id: str
    target_polygon: list[tuple[float, float]]  # [(lat, lng), ...]
    priority: float = Field(ge=0.0, le=1.0, default=0.5)
    deadline_ms: int
    estimated_duration_s: int = 600
    spectral: SpectralConfig = Field(default_factory=SpectralConfig)
    mission_id: Optional[str] = None
    requested_by: str = "system"
    created_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    preemptible: bool = True
    min_priority_to_preempt: float = 0.8


class TaskBid(BaseModel):
    task_id: str
    entity_id: str
    bid_cost: float
    estimated_arrival_s: float
    battery_at_completion_pct: float
    current_position: Position
    timestamp_ms: int = Field(default_factory=lambda: int(time.time() * 1000))


class TaskAssignment(BaseModel):
    task_id: str
    entity_id: str
    assigned_at_ms: int
    deadline_ms: int
    status: str = "assigned"


class TaskResult(BaseModel):
    task_id: str
    entity_id: str
    success: bool
    failure_reason: Optional[str] = None
    completed_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    battery_at_completion_pct: float = 0.0
    final_position: Optional[Position] = None


class TaskStatus(str, Enum):
    """Task execution status - matches proto definition"""
    UNSPECIFIED = "unspecified"
    PENDING = "pending"
    ASSIGNED = "assigned"
    IN_TRANSIT = "in_transit"
    SCANNING = "scanning"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"
    PREEMPTED = "preempted"
    CANCELLED = "cancelled"


class TaskProgress(BaseModel):
    """Task progress update - matches proto definition"""
    task_id: str
    entity_id: str
    status: TaskStatus = TaskStatus.PENDING
    progress_pct: float = Field(ge=0.0, le=100.0, default=0.0)
    current_position: Optional[Position] = None
    updated_at_ms: int = Field(default_factory=lambda: int(time.time() * 1000))
    status_message: str = ""


@dataclass
class AuctionState:
    request: TaskRequest
    bids: list[TaskBid] = None
    winner: Optional[str] = None
    assigned_at_ms: Optional[int] = None

    def __post_init__(self):
        if self.bids is None:
            self.bids = []


class TaskAllocatorConfig(BaseModel):
    auction_duration_ms: int = 5000


class TaskAllocator:
    """Auction-based distributed task allocation"""

    def __init__(self, config: Optional[TaskAllocatorConfig] = None, database=None):
        self.config = config or TaskAllocatorConfig()
        self._tasks: dict[str, TaskRequest] = {}
        self._auctions: dict[str, AuctionState] = {}
        self._bids: dict[str, list[TaskBid]] = {}
        self._assignments: dict[str, TaskAssignment] = {}
        self._completed: list[TaskResult] = []
        self._subscribers: list[Callable] = []
        self._lock = asyncio.Lock()
        self._database = database
        self._auction_duration_ms = self.config.auction_duration_ms

    async def start(self) -> None:
        """Start task allocator and load from database"""
        await self.load_from_database()

    async def publish_task(self, request: TaskRequest) -> None:
        """Publish new task for bidding"""
        async with self._lock:
            self._auctions[request.task_id] = AuctionState(request=request)
        logger.info("Task published", task_id=request.task_id, priority=request.priority)

        # Persist to database
        if self._database:
            await self._database.upsert_task(request)

        # Start auction timer
        asyncio.create_task(self._close_auction(request.task_id))

    async def submit_bid(self, bid: TaskBid) -> bool:
        """Submit bid for a task"""
        async with self._lock:
            auction = self._auctions.get(bid.task_id)
            if not auction:
                logger.warning("Bid for unknown task", task_id=bid.task_id)
                return False

        # Persist bid to database
        if self._database:
            await self._database.record_bid(bid)
            if auction.winner:
                logger.warning("Auction already closed", task_id=bid.task_id)
                return False

            auction.bids.append(bid)
            logger.debug("Bid received", task_id=bid.task_id, entity_id=bid.entity_id, cost=bid.bid_cost)
            return True

    async def _close_auction(self, task_id: str) -> None:
        """Close auction and assign to lowest bidder"""
        await asyncio.sleep(self._auction_duration_ms / 1000)

        async with self._lock:
            auction = self._auctions.get(task_id)
            if not auction or auction.winner:
                return

            if not auction.bids:
                logger.warning("No bids for task", task_id=task_id)
                return

            # Select winner (lowest bid cost)
            winner_bid = min(auction.bids, key=lambda b: b.bid_cost)

            # Check preemption
            current_task = self._active_tasks.get(winner_bid.entity_id)
            if current_task:
                current_auction = self._auctions.get(current_task)
                if current_auction and not self._can_preempt(
                    auction.request, current_auction.request
                ):
                    logger.info(
                        "Cannot preempt current task",
                        entity_id=winner_bid.entity_id,
                        current_task=current_task,
                        new_task=task_id,
                    )
                    # Try next best bid
                    remaining = [b for b in auction.bids if b.entity_id != winner_bid.entity_id]
                    if remaining:
                        winner_bid = min(remaining, key=lambda b: b.bid_cost)
                    else:
                        logger.warning("No eligible bids after preemption check", task_id=task_id)
                        return

            # Assign task
            auction.winner = winner_bid.entity_id
            auction.assigned_at_ms = int(time.time() * 1000)
            self._active_tasks[winner_bid.entity_id] = task_id

            assignment = TaskAssignment(
                task_id=task_id,
                entity_id=winner_bid.entity_id,
                assigned_at_ms=auction.assigned_at_ms,
                deadline_ms=auction.request.deadline_ms,
            )
            self._assignments[task_id] = assignment

        logger.info(
            "Task assigned",
            task_id=task_id,
            entity_id=winner_bid.entity_id,
            bid_cost=winner_bid.bid_cost,
        )

    def _can_preempt(self, new_task: TaskRequest, current_task: TaskRequest) -> bool:
        """Check if new task can preempt current task"""
        if not current_task.preemptible:
            return False
        return new_task.priority >= current_task.min_priority_to_preempt

    async def complete_task(self, result: TaskResult) -> None:
        """Mark task as complete"""
        async with self._lock:
            if result.task_id in self._assignments:
                del self._assignments[result.task_id]
            if result.entity_id in self._active_tasks:
                del self._active_tasks[result.entity_id]
            self._completed.append(result)

        # Persist task completion to database
        if self._database:
            await self._database.upsert_task(result)

        logger.info(
            "Task completed",
            task_id=result.task_id,
            entity_id=result.entity_id,
            success=result.success,
        )

    async def get_assignment(self, entity_id: str) -> Optional[TaskAssignment]:
        """Get current assignment for entity"""
        task_id = self._active_tasks.get(entity_id)
        if task_id:
            return self._assignments.get(task_id)
        return None

    async def get_task_for_entity(self, entity_id: str) -> Optional[TaskRequest]:
        """Get task request for entity"""
        task_id = self._active_tasks.get(entity_id)
        if task_id:
            auction = self._auctions.get(task_id)
            if auction:
                return auction.request
        return None

    def calculate_bid(
        self, entity: EntityState, task: TaskRequest
    ) -> Optional[TaskBid]:
        """Calculate bid cost for a task based on local state"""
        # Distance cost
        task_center = self._polygon_center(task.target_polygon)
        dist_deg = (
            (entity.position.lat - task_center[0]) ** 2
            + (entity.position.lng - task_center[1]) ** 2
        ) ** 0.5
        dist_m = dist_deg * 111000  # Rough conversion
        dist_cost = dist_m / 20.0  # Assume 20 m/s flight speed

        # Battery cost
        est_battery_drain = task.estimated_duration_s / 3600 * 20  # 20%/hour
        battery_cost = max(0, 100 - (entity.battery_pct - est_battery_drain))

        # Load cost - prefer less loaded drones
        load_cost = 0.0 if not entity.task_id else 50.0

        # Priority bonus - higher priority tasks get lower bids
        priority_bonus = -task.priority * 20.0

        total_cost = dist_cost + battery_cost + load_cost + priority_bonus

        # Estimate arrival time
        arrival_s = dist_m / 15.0  # Conservative 15 m/s

        # Battery at completion
        battery_at_completion = entity.battery_pct - est_battery_drain

        return TaskBid(
            task_id=task.task_id,
            entity_id=entity.entity_id,
            bid_cost=total_cost,
            estimated_arrival_s=arrival_s,
            battery_at_completion_pct=battery_at_completion,
            current_position=entity.position,
        )

    @staticmethod
    def _polygon_center(polygon: list[tuple[float, float]]) -> tuple[float, float]:
        """Calculate centroid of polygon"""
        if not polygon:
            return (0.0, 0.0)
        lats = [p[0] for p in polygon]
        lngs = [p[1] for p in polygon]
        return (sum(lats) / len(lats), sum(lngs) / len(lngs))

    async def get_stats(self) -> dict:
        """Get allocator statistics"""
        async with self._lock:
            return {
                "active_auctions": len([a for a in self._auctions.values() if not a.winner]),
                "active_assignments": len(self._assignments),
                "completed_tasks": len(self._completed),
                "success_rate": (
                    sum(1 for r in self._completed if r.success) / len(self._completed)
                    if self._completed
                    else 0.0
                ),
            }

    def get_task(self, task_id: str) -> Optional[TaskAssignment]:
        """Get task by ID"""
        return self._assignments.get(task_id)

    async def load_from_database(self) -> None:
        """Load tasks from database on startup"""
        if not self._database:
            return

        try:
            # Load active tasks
            tasks_data = await self._database.list_tasks(status="pending")
            tasks_data.extend(await self._database.list_tasks(status="assigned"))
            
            for task_data in tasks_data:
                task = TaskRequest(
                    task_id=task_data["task_id"],
                    field_id=task_data["field_id"],
                    target_polygon=json.loads(task_data["target_polygon"]),
                    priority=task_data["priority"],
                    deadline_ms=task_data["deadline_ms"],
                    estimated_duration_s=task_data["estimated_duration_s"],
                    spectral=SpectralConfig(),
                    mission_id=task_data.get("mission_id"),
                    requested_by=task_data.get("requested_by"),
                    preemptible=True,
                )
                
                # Create assignment if entity_id is set
                if task_data.get("entity_id"):
                    assignment = TaskAssignment(
                        task_id=task.task_id,
                        entity_id=task_data["entity_id"],
                        assigned_at_ms=task_data.get("assigned_at_ms", 0),
                        deadline_ms=task_data["deadline_ms"],
                        status=task_data["status"],
                    )
                    self._assignments[task.task_id] = assignment
                    self._active_tasks[task_data["entity_id"]] = task.task_id

                # Create auction state
                self._auctions[task.task_id] = AuctionState(request=task)

            logger.info("Loaded tasks from database", count=len(self._auctions))
        except Exception as e:
            logger.error("Failed to load tasks from database", error=str(e))

    def list_tasks(
        self, status: Optional[str] = None, field_id: Optional[str] = None, limit: int = 100
    ) -> list[TaskAssignment]:
        """List tasks with optional filtering"""
        tasks = list(self._assignments.values())
        
        if status:
            tasks = [t for t in tasks if t.status == status]
        
        if field_id:
            tasks = [t for t in tasks if t.task.field_id == field_id]
        
        return tasks[:limit]

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a task"""
        if task_id not in self._assignments:
            return False
        
        assignment = self._assignments[task_id]
        if assignment.status in ["complete", "failed", "cancelled"]:
            return False
        
        # Update status
        assignment.status = "cancelled"
        assignment.completed_at_ms = int(time.time() * 1000)
        
        logger.info("Task cancelled", task_id=task_id, entity_id=assignment.entity_id)
        return True
