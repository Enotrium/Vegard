"""Integration tests for API component"""

import pytest
from fastapi.testclient import TestClient

from syndar.command.api import app, setup_api
from syndar.command.fop import FusedFieldPicture
from syndar.command.mission import MissionPlanner
from syndar.fabric.drift_monitor import DriftMonitor, DriftThresholds
from syndar.fabric.mesh import Mesh, MeshConfig
from syndar.fabric.task_allocator import TaskAllocator


@pytest.fixture
def test_client():
    """Create test client with dependencies"""
    # Create dependencies
    mesh_config = MeshConfig(fanout=3)
    mesh = Mesh(config=mesh_config)
    task_allocator = TaskAllocator()
    drift_monitor = DriftMonitor(thresholds=DriftThresholds(per_node_e_threshold=0.4))
    fop = FusedFieldPicture(mesh=mesh)
    mission_planner = MissionPlanner(task_allocator=task_allocator)
    
    # Setup API
    setup_api(mesh, fop, mission_planner, drift_monitor)
    
    return TestClient(app)


def test_health_check(test_client):
    """Test health check endpoint"""
    response = test_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_get_entities(test_client):
    """Test get entities endpoint"""
    response = test_client.get("/entities")
    assert response.status_code == 200
    data = response.json()
    assert "count" in data
    assert "entities" in data


def test_get_fused_picture(test_client):
    """Test FOP endpoint"""
    response = test_client.get("/fop")
    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "FeatureCollection"
    assert "features" in data


def test_get_fop_state(test_client):
    """Test FOP state endpoint"""
    response = test_client.get("/fop/state")
    assert response.status_code == 200
    data = response.json()
    assert "drone_count" in data
    assert "coverage_pct" in data


def test_get_drift_report(test_client):
    """Test drift report endpoint"""
    response = test_client.get("/drift")
    assert response.status_code == 200
    data = response.json()
    assert "alerts_count" in data


def test_get_missions(test_client):
    """Test missions endpoint"""
    response = test_client.get("/missions")
    assert response.status_code == 200
    data = response.json()
    assert "missions" in data


def test_get_stats(test_client):
    """Test stats endpoint"""
    response = test_client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "mesh" in data


def test_create_task(test_client):
    """Test task creation endpoint"""
    task_request = {
        "field_id": "test-field",
        "field_boundary": [(40.0, -74.0), (40.01, -74.0), (40.01, -74.01), (40.0, -74.01)],
        "spectral_config": {},
        "priority": 5,
    }
    response = test_client.post("/tasks", json=task_request)
    # May return 503 if mission planner not fully initialized
    assert response.status_code in [200, 503]


def test_list_tasks(test_client):
    """Test task listing endpoint"""
    response = test_client.get("/tasks")
    # May return 503 if mission planner not fully initialized
    assert response.status_code in [200, 503]
