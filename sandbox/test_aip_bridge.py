"""Test AIP Bridge - End-to-end Syndar→AIP integration test

Tests AIP integration without live AIP by using mock server.
"""

import argparse
import asyncio
import time
from pathlib import Path

import httpx
import structlog

from syndar.command.aip_bridge import AIPBridge, AIPBridgeConfig
from syndar.fabric.attestation import AttestationService
from syndar.fabric.mesh import EntityState, Position, SoilPrediction

logger = structlog.get_logger()


async def test_single_ingest(mock_aip_url: str) -> bool:
    """Test single payload ingest"""
    logger.info("Testing single payload ingest")

    config = AIPBridgeConfig(
        base_url=mock_aip_url,
        ingest_endpoint="/api/syndar/ingest",
    )

    bridge = AIPBridge(config=config)
    await bridge.start()

    try:
        # Create test entity with soil prediction
        entity = EntityState(
            entity_id="drone:us-east:001",
            entity_type="drone",
            position=Position(lat=39.0, lng=-77.0, alt=50.0),
            soil=SoilPrediction(
                field_id="test-field-001",
                nutrients={"nitrogen": 0.85, "carbon": 2.5},
                land_value_score=0.78,
                contamination_detected=False,
                spectral_hash="abc123",
                model_version="v0.1.0",
            ),
            drift_score=0.3,
            drift_flag=False,
            battery_pct=85.0,
            timestamp_ms=int(time.time() * 1000),
        )

        # Ingest
        success = await bridge.ingest_entity(entity)

        if success:
            logger.info("Single ingest test PASSED")
        else:
            logger.error("Single ingest test FAILED")

        return success

    finally:
        await bridge.stop()


async def test_batch_ingest(mock_aip_url: str) -> bool:
    """Test batch payload ingest"""
    logger.info("Testing batch payload ingest")

    config = AIPBridgeConfig(
        base_url=mock_aip_url,
        ingest_endpoint="/api/syndar/ingest",
        batch_size=5,
        batch_interval_s=1.0,
    )

    bridge = AIPBridge(config=config)
    await bridge.start()

    try:
        # Create multiple test entities
        for i in range(7):  # More than batch_size to trigger batch send
            entity = EntityState(
                entity_id=f"drone:us-east:{i+1:03d}",
                entity_type="drone",
                position=Position(lat=39.0 + i * 0.01, lng=-77.0, alt=50.0),
                soil=SoilPrediction(
                    field_id=f"test-field-{i+1:03d}",
                    nutrients={"nitrogen": 0.7 + i * 0.05, "carbon": 2.0 + i * 0.1},
                    land_value_score=0.6 + i * 0.05,
                    contamination_detected=i % 3 == 0,
                    spectral_hash=f"hash-{i}",
                    model_version="v0.1.0",
                ),
                timestamp_ms=int(time.time() * 1000),
            )

            await bridge.ingest_entity(entity)

        # Force flush
        await asyncio.sleep(2.0)

        stats = bridge.get_stats()
        success = stats["payloads_sent"] >= 5

        if success:
            logger.info("Batch ingest test PASSED", stats=stats)
        else:
            logger.error("Batch ingest test FAILED", stats=stats)

        return success

    finally:
        await bridge.stop()


async def test_signed_ingest(mock_aip_url: str) -> bool:
    """Test signed payload ingest with attestation"""
    logger.info("Testing signed payload ingest")

    # Create attestation service
    attestation = AttestationService()

    # Generate test identity
    identity = attestation.generate_identity("us-east", "test-001")

    config = AIPBridgeConfig(
        base_url=mock_aip_url,
        ingest_endpoint="/api/syndar/ingest",
    )

    bridge = AIPBridge(config=config, attestation=attestation)
    await bridge.start()

    try:
        # Create test entity
        entity = EntityState(
            entity_id=identity.entity_id,
            entity_type="drone",
            position=Position(lat=39.0, lng=-77.0, alt=50.0),
            soil=SoilPrediction(
                field_id="test-field-signed",
                nutrients={"nitrogen": 0.9, "carbon": 3.0},
                land_value_score=0.85,
                contamination_detected=False,
                spectral_hash="signed-hash-123",
                model_version="v0.1.0-signed",
            ),
            timestamp_ms=int(time.time() * 1000),
        )

        # Sign the payload
        payload = bridge._convert_entity_to_aip(entity)
        signed = attestation.sign(
            identity.entity_id,
            bridge._convert_entity_to_aip(entity).model_dump_json(),
        )

        # Ingest signed
        success = await bridge.ingest_soil_prediction(entity, signed)

        if success:
            logger.info("Signed ingest test PASSED", fingerprint=identity.fingerprint[:16])
        else:
            logger.error("Signed ingest test FAILED")

        return success

    finally:
        await bridge.stop()


async def test_field_verification(mock_aip_url: str) -> bool:
    """Test field verification against AIP"""
    logger.info("Testing field verification")

    config = AIPBridgeConfig(base_url=mock_aip_url)
    bridge = AIPBridge(config=config)
    await bridge.start()

    try:
        # First ingest some data
        entity = EntityState(
            entity_id="drone:us-east:001",
            entity_type="drone",
            position=Position(lat=39.0, lng=-77.0),
            soil=SoilPrediction(
                field_id="verify-test-field",
                nutrients={"nitrogen": 0.8},
                land_value_score=0.75,
            ),
            timestamp_ms=int(time.time() * 1000),
        )

        await bridge.ingest_entity(entity)
        await asyncio.sleep(1.0)  # Let it send

        # Now verify
        exists = await bridge.verify_field_exists("verify-test-field")

        if exists:
            logger.info("Field verification test PASSED")
        else:
            logger.error("Field verification test FAILED")

        return exists

    finally:
        await bridge.stop()


async def verify_mock_server(mock_aip_url: str) -> bool:
    """Verify mock AIP server is running"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{mock_aip_url}/health")
            if response.status_code == 200:
                data = response.json()
                logger.info("Mock AIP server healthy", status=data)
                return True
    except Exception as e:
        logger.error("Cannot connect to mock AIP server", error=str(e))
        return False

    return False


async def run_all_tests(mock_aip_url: str) -> dict:
    """Run all AIP bridge tests"""
    results = {
        "server_available": False,
        "single_ingest": False,
        "batch_ingest": False,
        "signed_ingest": False,
        "field_verification": False,
    }

    # Check server
    results["server_available"] = await verify_mock_server(mock_aip_url)
    if not results["server_available"]:
        logger.error("Mock AIP server not available, aborting tests")
        return results

    # Reset mock server
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"{mock_aip_url}/api/syndar/reset")
    except Exception as e:
        logger.warning("Failed to reset mock server", error=str(e))

    # Run tests
    results["single_ingest"] = await test_single_ingest(mock_aip_url)
    results["batch_ingest"] = await test_batch_ingest(mock_aip_url)
    results["signed_ingest"] = await test_signed_ingest(mock_aip_url)
    results["field_verification"] = await test_field_verification(mock_aip_url)

    return results


def print_results(results: dict) -> None:
    """Print test results"""
    print("\n" + "=" * 60)
    print("AIP BRIDGE TEST RESULTS")
    print("=" * 60)

    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {test_name:25s} {status}")

    total = len([r for r in results.values() if r is not None])
    passed = sum(1 for r in results.values() if r)

    print("-" * 60)
    print(f"  TOTAL: {passed}/{total} tests passed")
    print("=" * 60)


async def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(description="Test Syndar AIP Bridge")
    parser.add_argument(
        "--aip-url",
        default="http://localhost:3000",
        help="Mock AIP server URL",
    )
    parser.add_argument(
        "--start-mock",
        action="store_true",
        help="Start mock AIP server before testing",
    )

    args = parser.parse_args()

    # Optionally start mock server
    if args.start_mock:
        logger.info("Starting mock AIP server...")
        import subprocess
        import sys

        proc = subprocess.Popen(
            [sys.executable, "-m", "sandbox.mock_aip_server", "--port", "3000"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        await asyncio.sleep(2.0)  # Wait for server to start

    try:
        results = await run_all_tests(args.aip_url)
        print_results(results)

        # Export final stats from mock server
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{args.aip_url}/api/syndar/stats")
                if response.status_code == 200:
                    stats = response.json()
                    print("\nMock AIP Server Stats:")
                    for key, value in stats.items():
                        print(f"  {key}: {value}")
        except Exception as e:
            logger.error("Failed to get final stats", error=str(e))

        return 0 if all(results.values()) else 1

    finally:
        if args.start_mock:
            proc.terminate()
            proc.wait()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
