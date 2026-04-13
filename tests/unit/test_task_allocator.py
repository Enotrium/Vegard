"""Unit tests for TaskAllocator component"""

import pytest
import time

from syndar.fabric.task_allocator import (
    TaskAllocator,
    TaskRequest,
    TaskBid,
    SpectralConfig,
    TaskAllocatorConfig,
)
from syndar.fabric.mesh import EntityState, Position


@pytest.fixture
def task_allocator():
    """Create task allocator for testing"""
    config = TaskAllocatorConfig(auction_duration_ms=100)
    return TaskAllocator(config=config)


def test_task_allocator_initialization(task_allocator):
    """Test task allocator initialization"""
    assert task_allocator is not None
    assert task_allocator.config.auction_duration_ms == 100


@pytest.mark.asyncio
async def test_task_allocator_publish_task(task_allocator):
    """Test task publishing"""
    task = TaskRequest(
        task_id="task-001",
        field_id="field-001",
        target_polygon=[(40.0, -74.0), (40.01, -74.0)],
        priority=0.8,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await task_allocator.publish_task(task)
    
    # Wait for auction to start
    assert "task-001" in task_allocator._auctions


@pytest.mark.asyncio
async def test_task_allocator_calculate_bid(task_allocator):
    """Test bid calculation"""
    task = TaskRequest(
        task_id="task-001",
        field_id="field-001",
        target_polygon=[(40.0, -74.0), (40.01, -74.0)],
        priority=0.8,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    entity = EntityState(
        entity_id="drone:001",
        entity_type="drone",
        position=Position(lat=40.005, lng=-74.005, alt=100.0),
        timestamp_ms=int(time.time() * 1000),
        battery_pct=95.0,
    )
    
    bid = task_allocator.calculate_bid(entity, task)
    
    assert bid is not None
    assert bid.task_id == "task-001"
    assert bid.entity_id == "drone:001"
    assert bid.bid_cost > 0


@pytest.mark.asyncio
async def test_task_allocator_submit_bid(task_allocator):
    """Test bid submission"""
    task = TaskRequest(
        task_id="task-001",
        field_id="field-001",
        target_polygon=[(40.0, -74.0), (40.01, -74.0)],
        priority=0.8,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await task_allocator.publish_task(task)
    
    bid = TaskBid(
        task_id="task-001",
        entity_id="drone:001",
        bid_cost=10.0,
        estimated_arrival_s=60.0,
        battery_at_completion_pct=85.0,
        current_position=Position(lat=40.005, lng=-74.005, alt=100.0),
    )
    
    success = await task_allocator.submit_bid(bid)
    assert success is True


@pytest.mark.asyncio
async def test_task_allocator_get_assignment(task_allocator):
    """Test getting assignment for entity"""
    # Manually set an assignment
    from syndar.fabric.task_allocator import TaskAssignment
    
    assignment = TaskAssignment(
        task_id="task-001",
        entity_id="drone:001",
        assigned_at_ms=int(time.time() * 1000),
        deadline_ms=int(time.time() * 1000) + 3600000,
    )
    
    task_allocator._assignments["task-001"] = assignment
    task_allocator._active_tasks["drone:001"] = "task-001"
    
    retrieved = task_allocator.get_assignment("drone:001")
    assert retrieved is not None
    assert retrieved.task_id == "task-001"


def test_task_allocator_list_tasks(task_allocator):
    """Test task listing"""
    from syndar.fabric.task_allocator import TaskAssignment
    
    # Add some assignments
    assignment1 = TaskAssignment(
        task_id="task-001",
        entity_id="drone:001",
        assigned_at_ms=int(time.time() * 1000),
        deadline_ms=int(time.time() * 1000) + 3600000,
        status="assigned",
    )
    
    assignment2 = TaskAssignment(
        task_id="task-002",
        entity_id="drone:002",
        assigned_at_ms=int(time.time() * 1000),
        deadline_ms=int(time.time() * 1000) + 3600000,
        status="pending",
    )
    
    task_allocator._assignments["task-001"] = assignment1
    task_allocator._assignments["task-002"] = assignment2
    
    all_tasks = task_allocator.list_tasks()
    assert len(all_tasks) == 2
    
    assigned_tasks = task_allocator.list_tasks(status="assigned")
    assert len(assigned_tasks) == 1


@pytest.mark.asyncio
async def test_task_allocator_cancel_task(task_allocator):
    """Test task cancellation"""
    from syndar.fabric.task_allocator import TaskAssignment
    
    assignment = TaskAssignment(
        task_id="task-001",
        entity_id="drone:001",
        assigned_at_ms=int(time.time() * 1000),
        deadline_ms=int(time.time() * 1000) + 3600000,
        status="assigned",
    )
    
    task_allocator._assignments["task-001"] = assignment
    task_allocator._active_tasks["drone:001"] = "task-001"
    
    success = await task_allocator.cancel_task("task-001")
    assert success is True
    
    cancelled_task = task_allocator.get_task("task-001")
    assert cancelled_task.status == "cancelled"


def test_spectral_config():
    """Test SpectralConfig model"""
    config = SpectralConfig()
    assert config is not None
    assert config.bands == 200
