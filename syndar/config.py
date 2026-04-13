"""Configuration loader for Syndar

Loads configuration from YAML files with environment variable overrides.
"""

import os
from pathlib import Path
from typing import Any, Optional

import structlog
import yaml

logger = structlog.get_logger()


class ConfigLoader:
    """Load and manage configuration from YAML files"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._find_config_file()
        self._config: dict[str, Any] = {}
        self._load_config()

    def _find_config_file(self) -> str:
        """Find configuration file in standard locations"""
        # Check environment variable first
        env_config = os.getenv("SYNDAR_CONFIG")
        if env_config:
            return env_config

        # Check standard locations
        possible_paths = [
            "configs/default.yaml",
            "configs/default.yml",
            "/etc/syndar/config.yaml",
            os.path.expanduser("~/.syndar/config.yaml"),
        ]

        for path in possible_paths:
            if Path(path).exists():
                return path

        # Fall back to default
        return "configs/default.yaml"

    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning(
                    "Config file not found, using defaults",
                    path=self.config_path,
                )
                self._config = {}
                return

            with open(config_file) as f:
                self._config = yaml.safe_load(f) or {}

            # Apply profile if specified
            profile = os.getenv("SYNDAR_ENV")
            if profile and profile in self._config.get("profiles", {}):
                profile_config = self._config["profiles"][profile]
                self._config = self._merge_config(self._config, profile_config)

            # Apply environment variable overrides
            self._apply_env_overrides()

            logger.info(
                "Configuration loaded",
                path=self.config_path,
                profile=profile,
            )
        except Exception as e:
            logger.error("Failed to load configuration", error=str(e))
            self._config = {}

    def _merge_config(self, base: dict, override: dict) -> dict:
        """Deep merge two configuration dictionaries"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(result[key], value)
            else:
                result[key] = value
        return result

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to config"""
        env_mappings = {
            "SYNDAR_GRPC_PORT": ("transport", "grpc", "port"),
            "SYNDAR_MQTT_BROKER": ("transport", "mqtt", "broker"),
            "SYNDAR_MQTT_PORT": ("transport", "mqtt", "port"),
            "SYNDAR_MESH_FANOUT": ("mesh", "fanout"),
            "ARTHEDAIN_PATH": ("integrations", "arthedain", "path"),
            "HSI_API_URL": ("integrations", "hsi_model", "api_url"),
            "HSI_MODEL_VERSION": ("integrations", "hsi_model", "model_version"),
            "AIP_API_URL": ("aip_bridge", "base_url"),
            "MAPBOX_ACCESS_TOKEN": ("api", "mapbox_token"),
        }

        for env_var, config_path in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                self._set_nested_value(config_path, value)

    def _set_nested_value(self, path: tuple, value: Any) -> None:
        """Set a nested configuration value from a path tuple"""
        current = self._config
        for key in path[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        current[path[-1]] = value

    def get(self, *path: str, default: Any = None) -> Any:
        """Get configuration value by path"""
        current = self._config
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def get_transport_config(self) -> dict:
        """Get transport configuration"""
        return self.get("transport", default={})

    def get_mesh_config(self) -> dict:
        """Get mesh configuration"""
        return self.get("mesh", default={})

    def get_task_allocator_config(self) -> dict:
        """Get task allocator configuration"""
        return self.get("task_allocator", default={})

    def get_drift_monitor_config(self) -> dict:
        """Get drift monitor configuration"""
        return self.get("drift_monitor", default={})

    def get_node_agent_config(self) -> dict:
        """Get node agent configuration"""
        return self.get("node_agent", default={})

    def get_mission_planner_config(self) -> dict:
        """Get mission planner configuration"""
        return self.get("mission_planner", default={})

    def get_api_config(self) -> dict:
        """Get API server configuration"""
        return self.get("api", default={})

    def get_aip_bridge_config(self) -> dict:
        """Get AIP bridge configuration"""
        return self.get("aip_bridge", default={})

    def get_spectral_bridge_config(self) -> dict:
        """Get spectral bridge configuration"""
        return self.get("spectral_bridge", default={})

    def get_attestation_config(self) -> dict:
        """Get attestation configuration"""
        return self.get("attestation", default={})

    def get_integrations_config(self) -> dict:
        """Get integrations configuration"""
        return self.get("integrations", default={})

    def get_logging_config(self) -> dict:
        """Get logging configuration"""
        return self.get("logging", default={})

    def reload(self) -> None:
        """Reload configuration from file"""
        logger.info("Reloading configuration")
        self._load_config()


# Global configuration instance
_config: Optional[ConfigLoader] = None


def get_config() -> ConfigLoader:
    """Get global configuration instance"""
    global _config
    if _config is None:
        _config = ConfigLoader()
    return _config


def reload_config() -> None:
    """Reload global configuration"""
    global _config
    if _config is not None:
        _config.reload()
