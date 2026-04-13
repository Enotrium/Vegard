"""Integration tests for TaskAllocator component"""

import asyncio
import time
import pytest

from syndar.fabric.mesh import EntityState, Mesh, MeshConfig, Position
from syndar.fabric.task_allocator import (
    SpectralConfig,
    TaskAllocator,
    TaskBid,
    TaskRequest,
    TaskStatus,
)


@pytest.mark.asyncio
async def test_task_allocator_publish_and_bid():
    """Test task publishing and bidding workflow"""
    allocator = TaskAllocator()
    await allocator.start()
    
    # Create task
    task = TaskRequest(
        task_id="task-test-001",
        field_id="field-001",
        target_polygon=[(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
        priority=0.8,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    # Publish task
    await allocator.publish_task(task)
    
    # Verify task in allocator
    retrieved_task = allocator.get_task("task-test-001")
    assert retrieved_task is not None
    assert retrieved_task.task_id == "task-test-001"
    
    # Create entity for bidding
    entity = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.005, lng=-74.005, alt=100.0),
        timestamp_ms=int(time.time() * 1000),
        battery_pct=95.0,
    )
    
    # Calculate and submit bid
    bid = allocator.calculate_bid(entity, task)
    assert bid is not None
    assert bid.task_id == "task-test-001"
    assert bid.entity_id == "drone:test:001"
    
    await allocator.submit_bid(bid)
    
    # Verify bid
    auction = allocator._auctions.get("task-test-001")
    assert auction is not None
    assert len(auction.bids) >= 1
    
    await allocator.stop()


@pytest.mark.asyncio
async def test_task_allocator_auction_resolution():
    """Test auction resolution and task assignment"""
    allocator = TaskAllocator()
    await allocator.start()
    
    # Create task
    task = TaskRequest(
        task_id="task-test-002",
        field_id="field-002",
        target_polygon=[(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
        priority=0.9,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await allocator.publish_task(task)
    
    # Submit multiple bids
    entity1 = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.005, lng=-74.005, alt=100.0),
        timestamp_ms=int(time.time() * 1000),
        battery_pct=95.0,
    )
    
    entity2 = EntityState(
        entity_id="drone:test:002",
        entity_type="drone",
        position=Position(lat=40.006, lng=-74.006, alt=100.0),
        timestamp_ms=int(time.time() * 1000),
        battery_pct=85.0,
    )
    
    bid1 = allocator.calculate_bid(entity1, task)
    bid2 = allocator.calculate_bid(entity2, task)
    
    await allocator.submit_bid(bid1)
    await allocator.submit_bid(bid2)
    
    # Resolve auction
    assignment = await allocator.resolve_auction("task-test-002")
    assert assignment is not None
    assert assignment.task_id == "task-test-002"
    assert assignment.entity_id in [bid1.entity_id, bid2.entity_id]
    
    await allocator.stop()


@pytest.mark.asyncio
async def test_task_allocator_completion():
    """Test task completion workflow"""
    allocator = TaskAllocator()
    await allocator.start()
    
    # Create and assign task
    task = TaskRequest(
        task_id="task-test-003",
        field_id="field-003",
        target_polygon=[(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
        priority=0.7,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await allocator.publish_task(task)
    
    entity = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.005, lng=-74.005, alt=100.0),
        timestamp_ms=int(time.time() * 1000),
        battery_pct=95.0,
    )
    
    bid = allocator.calculate_bid(entity, task)
    await allocator.submit_bid(bid)
    assignment = await allocator.resolve_auction("task-test-003")
    
    # Complete task
    from syndar.fabric.task_allocator import TaskResult
    
    result = TaskResult(
        task_id="task-test-003",
        entity_id="drone:test:001",
        success=True,
        soil_data=None,
        completed_at_ms=int(time.time() * 1000),
        battery_at_completion_pct=85.0,
        final_position=entity.position,
    )
    
    await allocator.complete_task(result)
    
    # Verify completion
    completed_task = allocator.get_task("task-test-003")
    assert completed_task is not None
    assert completed_task.status == "complete"
    
    await allocator.stop()


@pytest.mark.asyncio
async def test_task_allocator_cancellation():
    """Test task cancellation"""
    allocator = TaskAllocator()
    await allocator.start()
    
    # Create task
    task = TaskRequest(
        task_id="task-test-004",
        field_id="field-004",
        target_polygon=[(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
        priority=0.5,
        deadline_ms=int(time.time() * 1000) + 3600000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await allocator.publish_task(task)
    
    # Cancel task
    success = await allocator.cancel_task("task-test-004")
    assert success is True
    
    # Verify cancellation
    cancelled_task = allocator.get_task("task-test-004")
    assert cancelled_task is not None
    assert cancelled_task.status == "cancelled"
    
    await allocator.stop()


@pytest.mark.asyncio
async def test_task_allocator_list_tasks():
    """Test task listing with filtering"""
    allocator = TaskAllocator()
    await allocator.start()
    
    # Create multiple tasks
    for i in range(3):
        task = TaskRequest(
            task_id=f"task-test-{i:03d}",
            field_id=f"field-{i:03d}",
            target_polygon=[(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
            priority=0.5 + i * 0.1,
            deadline_ms=int(time.time() * 1000) + 3600000,
            estimated_duration_s=600,
            spectral=SpectralConfig(),
            requested_by="test",
        )
        await allocator.publish_task(task)
    
    # List all tasks
    all_tasks = allocator.list_tasks()
    assert len(all_tasks) == 3
    
    # Filter by field
    field_tasks = allocator.list_tasks(field_id="field-001")
    assert len(field_tasks) == 1
    
    await allocator.stop()
