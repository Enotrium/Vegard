"""Unit tests for DriftMonitor component"""

import pytest

from syndar.fabric.drift_monitor import DriftMonitor, DriftThresholds


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
    assert thresholds.per_node_e_threshold == 0.4
    assert thresholds.spatial_correlation_threshold == 0.7
    assert thresholds.temporal_correlation_threshold == 0.6
    assert thresholds.min_nodes_for_correlation == 3
    assert thresholds.max_signal_age_ms == 30000


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
async def test_drift_monitor_record_signal(drift_monitor):
    """Test recording drift signal"""
    signal = {
        "entity_id": "drone:001",
        "e_value": 0.5,
        "timestamp_ms": 1234567890,
        "position": {"lat": 40.0, "lng": -74.0, "alt": 100.0},
    }
    
    await drift_monitor.record_signal(signal)
    
    # Signal should be stored
    assert "drone:001" in drift_monitor._signals


@pytest.mark.asyncio
async def test_drift_monitor_correlate_signals(drift_monitor):
    """Test signal correlation"""
    import time
    
    # Record multiple signals from same entity
    for i in range(3):
        signal = {
            "entity_id": f"drone:00{i}",
            "e_value": 0.5,
            "timestamp_ms": int(time.time() * 1000),
            "position": {"lat": 40.0 + i * 0.01, "lng": -74.0 + i * 0.01, "alt": 100.0},
        }
        await drift_monitor.record_signal(signal)
    
    # Correlate signals
    alerts = await drift_monitor.correlate_signals()
    
    # Should return list of alerts (may be empty depending on correlation logic)
    assert isinstance(alerts, list)


@pytest.mark.asyncio
async def test_drift_monitor_cleanup_old_signals(drift_monitor):
    """Test cleanup of old signals"""
    import time
    
    # Add an old signal
    old_signal = {
        "entity_id": "drone:001",
        "e_value": 0.5,
        "timestamp_ms": int(time.time() * 1000) - 40000,  # 40 seconds ago
        "position": {"lat": 40.0, "lng": -74.0, "alt": 100.0},
    }
    
    await drift_monitor.record_signal(old_signal)
    
    # Cleanup old signals
    await drift_monitor.cleanup_old_signals()
    
    # Old signal should be removed
    assert "drone:001" not in drift_monitor._signals


def test_drift_monitor_get_stats(drift_monitor):
    """Test getting drift monitor statistics"""
    stats = drift_monitor.get_stats()
    
    assert "total_signals" in stats
    assert "active_alerts" in stats
    assert "correlations_performed" in stats
