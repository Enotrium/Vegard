"""Tests for Fabric Mesh layer"""

import pytest
import asyncio
from syndar.fabric.mesh import Mesh, MeshConfig, EntityState, Position


@pytest.mark.asyncio
async def test_mesh_initialization():
    """Test mesh can be initialized"""
    config = MeshConfig(fanout=3)
    mesh = Mesh(config=config)
    assert mesh.config.fanout == 3
    assert mesh.store is not None


@pytest.mark.asyncio
async def test_entity_store_update():
    """Test entity store can update and retrieve"""
    from syndar.fabric.mesh import EntityStore

    store = EntityStore()

    entity = EntityState(
        entity_id="drone:test:001",
        entity_type="drone",
        position=Position(lat=39.0, lng=-77.0, alt=50.0),
        battery_pct=85.0,
    )

    await store.update(entity)
    retrieved = await store.get("drone:test:001")

    assert retrieved is not None
    assert retrieved.entity_id == "drone:test:001"
    assert retrieved.battery_pct == 85.0


@pytest.mark.asyncio
async def test_entity_store_get_all():
    """Test getting all entities"""
    from syndar.fabric.mesh import EntityStore

    store = EntityStore()

    # Add multiple entities
    for i in range(3):
        entity = EntityState(
            entity_id=f"drone:test:{i+1:03d}",
            entity_type="drone",
            position=Position(lat=39.0 + i * 0.01, lng=-77.0, alt=50.0),
        )
        await store.update(entity)

    all_entities = await store.get_all()
    assert len(all_entities) == 3

    drones = await store.get_all("drone")
    assert len(drones) == 3


@pytest.mark.asyncio
async def test_entity_store_nearby_query():
    """Test nearby entity query"""
    from syndar.fabric.mesh import EntityStore

    store = EntityStore()

    # Add entities at different locations
    await store.update(
        EntityState(
            entity_id="drone:near:001",
            position=Position(lat=39.0, lng=-77.0),
        )
    )
    await store.update(
        EntityState(
            entity_id="drone:far:002",
            position=Position(lat=40.0, lng=-78.0),  # ~150km away
        )
    )

    nearby = await store.query_nearby(39.0, -77.0, radius_m=10000)
    assert len(nearby) == 1
    assert nearby[0][0].entity_id == "drone:near:001"
