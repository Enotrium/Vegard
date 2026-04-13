"""Unit tests for DriftMonitor component"""

import pytest

from vegard.fabric.drift_monitor import DriftMonitor, DriftThresholds


@pytest.fixture
def drift_monitor():
    """Create drift monitor for testing"""
    thresholds = DriftThresholds(
        per_node_e_threshold=0.4,
        spatial_correlation_threshold=0.7,
        temporal_correlation_threshold=0.6,
        min_nodes_for_correlation=3,
        max_signal_age_ms=30000,
    )
    return DriftMonitor(thresholds=thresholds)


def test_drift_monitor_initialization(drift_monitor):
    """Test drift monitor initialization"""
    assert drift_monitor is not None
    assert drift_monitor.thresholds.per_node_e_threshold == 0.4
    assert drift_monitor.thresholds.min_nodes_for_correlation == 3


def test_drift_thresholds_initialization():
    """Test drift thresholds initialization"""
    thresholds = DriftThresholds()
    assert thresholds.per_node_e_threshold == 0.5
    assert thresholds.spatial_correlation_threshold == 0.7
    assert thresholds.temporal_correlation_threshold == 0.8
    assert thresholds.min_nodes_for_correlation == 3
    assert thresholds.max_signal_age_ms == 60000


def test_drift_thresholds_custom():
    """Test custom drift thresholds"""
    thresholds = DriftThresholds(
        per_node_e_threshold=0.5,
        spatial_correlation_threshold=0.8,
        temporal_correlation_threshold=0.7,
        min_nodes_for_correlation=5,
        max_signal_age_ms=60000,
    )
    assert thresholds.per_node_e_threshold == 0.5
    assert thresholds.spatial_correlation_threshold == 0.8
    assert thresholds.temporal_correlation_threshold == 0.7
    assert thresholds.min_nodes_for_correlation == 5
    assert thresholds.max_signal_age_ms == 60000


@pytest.mark.asyncio
async def test_drift_monitor_start_stop(drift_monitor):
    """Test drift monitor start and stop"""
    await drift_monitor.start()
    assert drift_monitor._running is True
    
    await drift_monitor.stop()
    assert drift_monitor._running is False


@pytest.mark.asyncio
async def test_drift_monitor_report_signal(drift_monitor):
    """Test reporting drift signal"""
    from vegard.fabric.drift_monitor import NodeDriftSignal
    
    signal = NodeDriftSignal(
        entity_id="drone:001",
        e_fast=0.3,
        e_slow=0.6,
        combined_e=0.5,
        threshold=0.5,
        exceeded=True,
        timestamp_ms=1234567890,
        lat=40.0,
        lng=-74.0,
        field_id="field-001",
        task_id="task-001",
    )
    
    await drift_monitor.report_signal(signal)
    
    # Signal should be stored
    assert "field-001" in drift_monitor._signals


@pytest.mark.asyncio
async def test_drift_monitor_get_alerts(drift_monitor):
    """Test getting drift alerts"""
    alerts = await drift_monitor.get_alerts()
    
    # Should return list of alerts
    assert isinstance(alerts, list)


@pytest.mark.asyncio
async def test_drift_monitor_get_correlations(drift_monitor):
    """Test getting drift correlations"""
    correlations = await drift_monitor.get_correlations("field-001")
    
    # Should return list of correlations
    assert isinstance(correlations, list)


@pytest.mark.asyncio
async def test_drift_monitor_get_stats(drift_monitor):
    """Test getting drift monitor statistics"""
    stats = await drift_monitor.get_stats()
    
    assert "total_signals" in stats
    assert "total_alerts" in stats
    assert "fields_monitored" in stats
