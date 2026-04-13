"""Tests for AIP Bridge integration"""

import pytest
import asyncio
from syndar.command.aip_bridge import AIPBridge, AIPBridgeConfig
from syndar.fabric.mesh import EntityState, Position, SoilPrediction


@pytest.mark.asyncio
async def test_bridge_initialization():
    """Test AIP bridge can be initialized"""
    config = AIPBridgeConfig(base_url="http://localhost:3000")
    bridge = AIPBridge(config=config)
    assert bridge.config.base_url == "http://localhost:3000"


@pytest.mark.asyncio
async def test_entity_to_payload_conversion():
    """Test Syndar entity converts to AIP payload"""
    config = AIPBridgeConfig()
    bridge = AIPBridge(config=config)

    entity = EntityState(
        entity_id="drone:test:001",
        position=Position(lat=39.0, lng=-77.0, alt=50.0, accuracy=1.0),
        soil=SoilPrediction(
            field_id="test-field",
            nutrients={"nitrogen": 0.85, "carbon": 2.5},
            land_value_score=0.78,
            contamination_detected=False,
            spectral_hash="abc123",
            model_version="v0.1.0",
        ),
        timestamp_ms=1234567890,
    )

    payload = bridge._convert_entity_to_aip(entity)

    assert payload.syndar_drone_id == "drone:test:001"
    assert payload.field_id == "test-field"
    assert payload.nitrogen_mg_kg == 0.85
    assert payload.carbon_percent == 2.5
    assert payload.land_value_score == 0.78
    assert payload.latitude == 39.0
    assert payload.longitude == -77.0


@pytest.mark.asyncio
async def test_contaminant_mapping():
    """Test contaminant types map correctly to AIP schema"""
    from syndar.fabric.mesh import SoilPrediction
    from syndar.fabric.mesh import SoilPrediction as SP  # for Contaminant import

    config = AIPBridgeConfig()
    bridge = AIPBridge(config=config)

    # Create entity with contaminants
    soil = SoilPrediction(
        field_id="test-field",
        nutrients={},
        contaminants=[],
    )
    # Note: Contaminant list is empty in this test structure
    # Full contaminant testing would need proper Contaminant objects

    entity = EntityState(
        entity_id="drone:test:001",
        position=Position(lat=39.0, lng=-77.0),
        soil=soil,
    )

    payload = bridge._convert_entity_to_aip(entity)

    # Should handle empty contaminant list
    assert payload.microplastic_detected == False
    assert payload.pfas_detected == False


@pytest.mark.asyncio
async def test_batch_aggregation():
    """Test batch aggregation of payloads"""
    config = AIPBridgeConfig(batch_size=3)
    bridge = AIPBridge(config=config)

    # Add entities without starting client
    for i in range(5):
        entity = EntityState(
            entity_id=f"drone:test:{i+1:03d}",
            position=Position(lat=39.0, lng=-77.0),
            soil=SoilPrediction(
                field_id=f"field-{i+1}",
                nutrients={"nitrogen": 0.8},
                land_value_score=0.7,
            ),
        )
        await bridge.ingest_entity(entity)

    # Check batch state
    stats = bridge.get_stats()
    assert stats["pending_batch"] == 5  # All queued since no client
