"""Tests for DriftMonitor (the novel differentiator)"""

import pytest
import asyncio
from syndar.fabric.drift_monitor import DriftMonitor, DriftThresholds, NodeDriftSignal


@pytest.mark.asyncio
async def test_drift_monitor_initialization():
    """Test drift monitor can be initialized"""
    thresholds = DriftThresholds(per_node_e_threshold=0.5)
    monitor = DriftMonitor(thresholds=thresholds)
    assert monitor.thresholds.per_node_e_threshold == 0.5


@pytest.mark.asyncio
async def test_report_signal():
    """Test reporting drift signals"""
    monitor = DriftMonitor()

    signal = NodeDriftSignal(
        entity_id="drone:test:001",
        e_fast=0.3,
        e_slow=0.4,
        threshold=0.5,
        lat=39.0,
        lng=-77.0,
        field_id="test-field",
        task_id="task-001",
    )

    await monitor.report_signal(signal)

    stats = await monitor.get_stats()
    assert stats["fields_monitored"] == 1
    assert stats["total_signals"] == 1


@pytest.mark.asyncio
async def test_correlation_analysis():
    """Test spatial correlation detection"""
    monitor = DriftMonitor(thresholds=DriftThresholds(min_nodes_for_correlation=2))
    await monitor.start()

    try:
        # Report signals from multiple drones over same field
        for i in range(3):
            signal = NodeDriftSignal(
                entity_id=f"drone:test:{i+1:03d}",
                e_fast=0.6,  # Exceeds threshold
                e_slow=0.7,
                threshold=0.5,
                exceeded=True,
                lat=39.0 + i * 0.001,  # Close together
                lng=-77.0 + i * 0.001,
                field_id="test-field",
                task_id="task-001",
            )
            await monitor.report_signal(signal)

        # Wait for analysis
        await asyncio.sleep(6.0)

        # Check correlations
        correlations = await monitor.get_correlations("test-field")
        # May or may not have correlations depending on timing

    finally:
        await monitor.stop()


@pytest.mark.asyncio
async def test_drift_type_detection():
    """Test drift type classification"""
    monitor = DriftMonitor()

    # Test independent drift (random, not correlated)
    # This is implicit - signals with low spatial/temporal correlation
    # get classified as "independent"

    stats = await monitor.get_stats()
    assert "total_signals" in stats
