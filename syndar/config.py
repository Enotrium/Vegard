"""Configuration loader for Syndar

Loads configuration from YAML files with environment variable overrides.
Supports hot-reload when configuration file changes.
"""

import asyncio
import os
from pathlib import Path
from typing import Any, Optional, Callable

import structlog
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

from syndar.logging_config import get_logger

logger = get_logger(__name__)


class ConfigFileHandler(FileSystemEventHandler):
    """Handler for configuration file changes"""

    def __init__(self, config_loader: "ConfigLoader"):
        self.config_loader = config_loader

    def on_modified(self, event):
        """Handle file modification event"""
        if event.src_path == self.config_loader.config_path:
            logger.info("Configuration file changed, reloading")
            self.config_loader.reload()


class ConfigLoader:
    """Load and manage configuration from YAML files with hot-reload support"""

    def __init__(self, config_path: Optional[str] = None, enable_hot_reload: bool = False):
        self.config_path = config_path or self._find_config_file()
        self._config: dict[str, Any] = {}
        self._load_config()
        
        self._observer: Optional[Observer] = None
        self._reload_callbacks: list[Callable[[], None]] = []
        
        if enable_hot_reload:
            self._enable_hot_reload()

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

    def _enable_hot_reload(self) -> None:
        """Enable file watching for configuration hot-reload"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                logger.warning("Config file not found, hot-reload disabled", path=self.config_path)
                return

            self._observer = Observer()
            handler = ConfigFileHandler(self)
            self._observer.schedule(handler, path=str(config_file.parent), recursive=False)
            self._observer.start()
            logger.info("Configuration hot-reload enabled", path=self.config_path)
        except Exception as e:
            logger.error("Failed to enable hot-reload", error=str(e))

    def disable_hot_reload(self) -> None:
        """Disable configuration hot-reload"""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None
            logger.info("Configuration hot-reload disabled")

    def on_reload(self, callback: Callable[[], None]) -> None:
        """Register callback to be called when configuration is reloaded"""
        self._reload_callbacks.append(callback)

    def reload_config(self) -> None:
        """Reload configuration from file"""
        logger.info("Reloading configuration")
        self._load_config()
        
        # Notify callbacks
        for callback in self._reload_callbacks:
            try:
                callback()
            except Exception as e:
                logger.error("Reload callback failed", error=str(e))


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
