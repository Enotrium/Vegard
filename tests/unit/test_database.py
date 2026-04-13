"""Unit tests for Database component"""

import pytest
import tempfile
from pathlib import Path

from vegard.fabric.database import Database, DatabaseConfig


@pytest.fixture
def temp_db():
    """Create temporary database for testing"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    config = DatabaseConfig(path=db_path)
    db = Database(config)
    yield db
    # Cleanup
    Path(db_path).unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_database_initialization(temp_db):
    """Test database initialization"""
    await temp_db.initialize()
    assert temp_db._initialized is True


@pytest.mark.asyncio
async def test_database_entity_upsert(temp_db):
    """Test entity upsert"""
    await temp_db.initialize()
    
    from vegard.fabric.mesh import EntityState, Position
    
    entity = EntityState(
        entity_id="test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=95.0,
    )
    
    await temp_db.upsert_entity(entity)
    
    retrieved = await temp_db.get_entity("test:001")
    assert retrieved is not None
    assert retrieved["entity_id"] == "test:001"
    assert retrieved["lat"] == 40.0


@pytest.mark.asyncio
async def test_database_entity_history(temp_db):
    """Test entity history recording"""
    await temp_db.initialize()
    
    from vegard.fabric.mesh import EntityState, Position
    
    entity = EntityState(
        entity_id="test:002",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=95.0,
    )
    
    await temp_db.record_entity_history(entity)
    
    history = await temp_db.get_entity_history("test:002", hours=24)
    assert len(history) == 1


@pytest.mark.asyncio
async def test_database_task_upsert(temp_db):
    """Test task upsert"""
    await temp_db.initialize()
    
    from vegard.fabric.task_allocator import TaskRequest, SpectralConfig
    
    task = TaskRequest(
        task_id="task-001",
        field_id="field-001",
        target_polygon=[(40.0, -74.0), (40.01, -74.0)],
        priority=0.8,
        deadline_ms=1234567890000,
        estimated_duration_s=600,
        spectral=SpectralConfig(),
        requested_by="test",
    )
    
    await temp_db.upsert_task(task)
    
    retrieved = await temp_db.get_task("task-001")
    assert retrieved is not None
    assert retrieved["task_id"] == "task-001"


@pytest.mark.asyncio
async def test_database_cleanup(temp_db):
    """Test old data cleanup"""
    await temp_db.initialize()
    
    from vegard.fabric.mesh import EntityState, Position
    
    entity = EntityState(
        entity_id="test:003",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=95.0,
    )
    
    await temp_db.record_entity_history(entity)
    
    # Cleanup should work without error
    deleted = await temp_db.cleanup_old_data(days=1)
    assert deleted >= 0


@pytest.mark.asyncio
async def test_database_stats(temp_db):
    """Test database statistics"""
    await temp_db.initialize()
    
    stats = await temp_db.get_stats()
    assert "entity_count" in stats
    assert "task_count" in stats
    assert stats["entity_count"] == 0
    assert stats["task_count"] == 0
