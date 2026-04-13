"""Integration tests for Mesh component"""

import asyncio
import pytest

from syndar.fabric.mesh import EntityStore, EntityState, Mesh, MeshConfig, Position
from syndar.fabric.transport import TransportBus, TransportConfig


@pytest.mark.asyncio
async def test_mesh_entity_lifecycle():
    """Test entity lifecycle in mesh"""
    store = EntityStore()
    
    entity = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=95.0,
    )
    
    # Store entity
    await store.update(entity)
    
    # Retrieve entity
    retrieved = await store.get("drone:test:001")
    assert retrieved is not None
    assert retrieved.entity_id == "drone:test:001"
    assert retrieved.battery_pct == 95.0
    
    # Get all entities
    all_entities = await store.get_all()
    assert len(all_entities) == 1
    assert all_entities[0].entity_id == "drone:test:001"


@pytest.mark.asyncio
async def test_mesh_gossip_with_transport():
    """Test mesh gossip with transport bus"""
    config = MeshConfig(fanout=3, gossip_interval_ms=100)
    transport_config = TransportConfig(use_mqtt=False, use_grpc=False)
    
    transport = TransportBus(config=transport_config)
    mesh = Mesh(config=config, transport=transport)
    
    # Start mesh
    await mesh.start()
    
    # Create entity
    entity = EntityState(
        entity_id="drone:test:002",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
        battery_pct=90.0,
    )
    
    # Update entity (triggers gossip)
    await mesh.store.update(entity)
    
    # Wait for gossip
    await asyncio.sleep(0.2)
    
    # Stop mesh
    await mesh.stop()
    
    # Verify entity stored
    retrieved = await mesh.store.get("drone:test:002")
    assert retrieved is not None


@pytest.mark.asyncio
async def test_entity_store_query_nearby():
    """Test entity store nearby query"""
    store = EntityStore()
    
    # Add entities at different locations
    entity1 = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
    )
    entity2 = EntityState(
        entity_id="drone:test:002",
        entity_type="drone",
        position=Position(lat=40.01, lng=-74.01, alt=100.0),
        timestamp_ms=1234567890,
    )
    entity3 = EntityState(
        entity_id="drone:test:003",
        entity_type="drone",
        position=Position(lat=41.0, lng=-75.0, alt=100.0),
        timestamp_ms=1234567890,
    )
    
    await store.update(entity1)
    await store.update(entity2)
    await store.update(entity3)
    
    # Query nearby
    nearby = await store.query_nearby(40.0, -74.0, 2000)
    assert len(nearby) >= 2  # Should find at least entity1 and entity2


@pytest.mark.asyncio
async def test_entity_store_cleanup():
    """Test entity store cleanup of stale entities"""
    store = EntityStore(stale_timeout_ms=100)
    
    entity = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=40.0, lng=-74.0, alt=100.0),
        timestamp_ms=1234567890,
    )
    
    await store.update(entity)
    
    # Entity should exist
    retrieved = await store.get("drone:test:001")
    assert retrieved is not None
    
    # Wait for stale timeout
    await asyncio.sleep(0.15)
    
    # Trigger cleanup
    await store.remove_stale()
    
    # Entity should be removed
    retrieved = await store.get("drone:test:001")
    assert retrieved is None
