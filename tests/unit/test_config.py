"""Unit tests for Configuration component"""

import pytest
import tempfile
from pathlib import Path

from vegard.config import ConfigLoader


@pytest.fixture
def temp_config():
    """Create temporary config file for testing"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
transport:
  grpc_port: 50051
  mqtt_broker: localhost

mesh:
  fanout: 3
  gossip_interval_ms: 1000

task_allocator:
  auction_duration_ms: 5000
""")
        config_path = f.name
    yield config_path
    # Cleanup
    Path(config_path).unlink(missing_ok=True)


def test_config_loader_initialization(temp_config):
    """Test config loader initialization"""
    config = ConfigLoader(config_path=temp_config)
    assert config.config_path == temp_config
    assert config._config is not None


def test_config_loader_get(temp_config):
    """Test getting config values"""
    config = ConfigLoader(config_path=temp_config)
    
    grpc_port = config.get("transport.grpc_port")
    assert grpc_port == 50051
    
    fanout = config.get("mesh.fanout")
    assert fanout == 3


def test_config_loader_get_with_default(temp_config):
    """Test getting config values with default"""
    config = ConfigLoader(config_path=temp_config)
    
    # Non-existent key with default
    value = config.get("nonexistent.key", default="default_value")
    assert value == "default_value"


def test_config_loader_get_transport_config(temp_config):
    """Test getting transport configuration"""
    config = ConfigLoader(config_path=temp_config)
    
    transport_config = config.get_transport_config()
    assert transport_config["grpc_port"] == 50051
    assert transport_config["mqtt_broker"] == "localhost"


def test_config_loader_get_mesh_config(temp_config):
    """Test getting mesh configuration"""
    config = ConfigLoader(config_path=temp_config)
    
    mesh_config = config.get_mesh_config()
    assert mesh_config["fanout"] == 3
    assert mesh_config["gossip_interval_ms"] == 1000


def test_config_loader_reload(temp_config):
    """Test configuration reload"""
    config = ConfigLoader(config_path=temp_config)
    
    # Modify config file
    with open(temp_config, "a") as f:
        f.write("\ntest_key: test_value\n")
    
    config.reload_config()
    
    value = config.get("test_key")
    assert value == "test_value"


def test_config_loader_env_override():
    """Test environment variable override"""
    import os
    
    os.environ["VEGARD_CONFIG"] = "/nonexistent/path.yaml"
    config = ConfigLoader()
    # Should use environment variable
    assert config.config_path == "/nonexistent/path.yaml"
    
    # Cleanup
    del os.environ["VEGARD_CONFIG"]


def test_config_loader_nested_access(temp_config):
    """Test nested config access"""
    config = ConfigLoader(config_path=temp_config)
    
    # Test nested access
    value = config.get("transport.grpc_port")
    assert value == 50051


def test_config_loader_get_all(temp_config):
    """Test getting all config"""
    config = ConfigLoader(config_path=temp_config)
    
    all_config = config.get_all()
    assert "transport" in all_config
    assert "mesh" in all_config
