"""Unit tests for Mesh component"""

import pytest
import asyncio

from vegard.fabric.mesh import EntityStore, EntityState, Position, Mesh, MeshConfig


@pytest.fixture
def entity_store():
    """Create entity store for testing"""
    return EntityStore()


@pytest.mark.asyncio
async def test_entity_store_update(entity_store):
    """Test entity update"""
    entity = EntityState(
        entity_id="test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=95.0,
    )
    
    await entity_store.update(entity)
    
    retrieved = await entity_store.get("test:001")
    assert retrieved is not None
    assert retrieved.entity_id == "test:001"
    assert retrieved.battery_pct == 95.0


@pytest.mark.asyncio
async def test_entity_store_get_all(entity_store):
    """Test getting all entities"""
    entity1 = EntityState(
        entity_id="test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
    )
    
    entity2 = EntityState(
        entity_id="test:002",
        entity_type="sensor",
        position=Position(lat=40.01, lng=-74.01, alt=50.0),
        timestamp_ms=1234567890,
    )
    
    await entity_store.update(entity1)
    await entity_store.update(entity2)
    
    all_entities = await entity_store.get_all()
    assert len(all_entities) == 2
    
    drone_entities = await entity_store.get_all(entity_type="drone")
    assert len(drone_entities) == 1


@pytest.mark.asyncio
async def test_entity_store_subscribe(entity_store):
    """Test entity subscription"""
    called = []
    
    def callback(entity):
        called.append(entity)
    
    entity_store.subscribe(callback)
    
    entity = EntityState(
        entity_id="test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
    )
    
    await entity_store.update(entity)
    
    assert len(called) == 1
    assert called[0].entity_id == "test:001"


@pytest.mark.asyncio
async def test_entity_store_remove_stale(entity_store):
    """Test removing stale entities"""
    import time
    
    entity = EntityState(
        entity_id="test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=int(time.time() * 1000) - 20000,  # 20 seconds ago
    )
    
    await entity_store.update(entity)
    
    # Set short timeout
    entity_store.config.heartbeat_timeout_ms = 15000
    
    removed = await entity_store.remove_stale()
    assert "test:001" in removed
    
    retrieved = await entity_store.get("test:001")
    assert retrieved is None


@pytest.mark.asyncio
async def test_mesh_initialization():
    """Test mesh initialization"""
    config = MeshConfig(fanout=3)
    mesh = Mesh(config=config)
    
    assert mesh.config.fanout == 3
    assert mesh.store is not None


@pytest.mark.asyncio
async def test_mesh_start_stop():
    """Test mesh start and stop"""
    config = MeshConfig(fanout=3)
    mesh = Mesh(config=config)
    
    await mesh.start()
    assert mesh._running is True
    
    await mesh.stop()
    assert mesh._running is False


@pytest.mark.asyncio
async def test_mesh_join():
    """Test mesh peer joining"""
    config = MeshConfig(fanout=3)
    mesh = Mesh(config=config)
    
    await mesh.join("peer1:50051")
    await mesh.join("peer2:50051")
    
    assert "peer1:50051" in mesh._peers
    assert "peer2:50051" in mesh._peers
    assert len(mesh._peers) == 2
