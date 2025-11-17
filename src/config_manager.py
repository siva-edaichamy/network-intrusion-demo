"""
Configuration Manager
Loads and manages configuration from .env and config.yaml files
UPDATED: Device names changed to match new dashboard
"""

import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Dict, Optional


class ConfigManager:
    """Centralized configuration management"""

    def __init__(self, env_path: str = ".env", config_path: str = "config/config.yaml"):
        self.env_path = env_path
        self.config_path = config_path
        self.env_vars = {}
        self.yaml_config = {}

        self._load_env()
        self._load_yaml()

    def _load_env(self):
        """Load environment variables from .env file"""
        if os.path.exists(self.env_path):
            load_dotenv(self.env_path)
            # Store in dict for easy access
            with open(self.env_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        self.env_vars[key.strip()] = value.strip()
        else:
            print(
                f"Warning: {self.env_path} not found. Using environment variables only."
            )

    def _load_yaml(self):
        """Load YAML configuration file"""
        if os.path.exists(self.config_path):
            with open(self.config_path, "r") as f:
                self.yaml_config = yaml.safe_load(f) or {}
        else:
            print(f"Warning: {self.config_path} not found. Using defaults.")
            self.yaml_config = {}

    def get(self, key: str, default: Any = None, source: str = "env") -> Any:
        """
        Get configuration value

        Args:
            key: Configuration key
            default: Default value if key not found
            source: 'env' for environment variables, 'yaml' for YAML config
        """
        if source == "env":
            return os.getenv(key, self.env_vars.get(key, default))
        elif source == "yaml":
            return self._get_nested_yaml(key, default)
        return default

    def _get_nested_yaml(self, key: str, default: Any = None) -> Any:
        """Get nested YAML value using dot notation (e.g., 'simulation.num_devices')"""
        keys = key.split(".")
        value = self.yaml_config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_rabbitmq_config(self) -> Dict[str, Any]:
        """Get RabbitMQ configuration"""
        return {
            "host": self.get("RABBITMQ_HOST"),
            "port": int(self.get("RABBITMQ_PORT", 5672)),
            "username": self.get("RABBITMQ_USERNAME"),
            "password": self.get("RABBITMQ_PASSWORD"),
            "vhost": self.get("RABBITMQ_VHOST", "/"),
        }

    def get_greenplum_config(self) -> Dict[str, Any]:
        """Get Greenplum configuration"""
        return {
            "host": self.get("GREENPLUM_HOST"),
            "port": int(self.get("GREENPLUM_PORT", 5432)),
            "database": self.get("GREENPLUM_DATABASE"),
            "user": self.get("GREENPLUM_USER"),
            "password": self.get("GREENPLUM_PASSWORD"),
            "schema": self.get("GREENPLUM_SCHEMA", "public"),
            "table": self.get("GREENPLUM_TABLE", "kdd_data_demo"),
        }

    def get_gemfire_config(self) -> Dict[str, Any]:
        """Get Gemfire configuration"""
        public_ip = self.get("GEMFIRE_PUBLIC_IP")
        rest_port = self.get("GEMFIRE_REST_PORT", "8080")

        return {
            "public_ip": public_ip,
            "locator": self.get("GEMFIRE_LOCATOR"),  # Used for SSH region creation (optional)
            "rest_url": f"http://{public_ip}:{rest_port}/gemfire-api/v1",
            "model_region": self.get("GEMFIRE_MODEL_REGION", "MLModels"),
            "model_key": self.get("GEMFIRE_MODEL_KEY", "kdd_intrusion_model_v1"),
            "ssh_user": self.get("GEMFIRE_SSH_USER"),
            "ssh_key_path": self.get("GEMFIRE_SSH_KEY_PATH"),
        }

    def get_gpss_config(self) -> Dict[str, Any]:
        """Get GPSS configuration"""
        return {
            "host": self.get("GPSS_HOST", "127.0.0.1"),
            "port": int(self.get("GPSS_PORT", 5000)),
            "job_name": self.get("GPSS_JOB_NAME", "load_kdd"),
            "gpss_dir": self.get("GPSS_REMOTE_DIR", "/home/gpadmin/gpss"),
        }

    def get_dataset_path(self) -> str:
        """Get dataset file path (default/legacy - returns simulation dataset)"""
        return self.get_simulation_dataset_path()
    
    def get_training_dataset_path(self) -> str:
        """Get training dataset file path"""
        return self.get("KDD_DATASET_TRAINING_PATH", "./data/kdd_cup_data_history.csv")
    
    def get_simulation_dataset_path(self) -> str:
        """Get simulation dataset file path"""
        return self.get("KDD_DATASET_SIMULATION_PATH", "./data/kdd_cup_data_stream.csv")

    def get_web_config(self) -> Dict[str, Any]:
        """Get web server configuration"""
        return {
            "port": int(self.get("WEB_PORT", 8080)),
            "websocket_port": int(self.get("WEBSOCKET_PORT", 8765)),
            "host": "0.0.0.0",
        }

    def get_simulation_config(self) -> Dict[str, Any]:
        """Get simulation configuration from YAML - UPDATED FOR NEW DASHBOARD"""
        # Get per-device rates if configured, otherwise distribute evenly
        total_rate = self.get("messages_per_second", 5.0, "yaml") or 5.0

        # Check if individual device rates are configured
        device_rates = self.get("device_rates", None, "yaml")

        if device_rates and isinstance(device_rates, dict):
            # Use configured rates for each device
            rates = {
                "Cell Tower": device_rates.get("celltower", 1.0),
                "Mobile Phone": device_rates.get("mobile", 1.0),
                "Laptop": device_rates.get("laptop", 1.0),
                "IoT Device": device_rates.get("iot", 1.0),
                "C2 Center": device_rates.get("c2", 1.0),
            }
        else:
            # Distribute total rate evenly across all devices
            per_device_rate = total_rate / 5
            rates = {
                "Cell Tower": per_device_rate,
                "Mobile Phone": per_device_rate,
                "Laptop": per_device_rate,
                "IoT Device": per_device_rate,
                "C2 Center": per_device_rate,
            }

        return {
            "num_devices": 5,  # Fixed to 5 specific devices
            "messages_per_second": total_rate,
            "device_names": [
                "Cell Tower",
                "Mobile Phone",
                "Laptop",
                "IoT Device",
                "C2 Center",
            ],
            "device_rates": rates,  # Individual rates per device
            "loop_dataset": self.get("loop_dataset", True, "yaml"),
        }

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate required configuration
        Returns: (is_valid, list_of_errors)
        """
        errors = []

        # Required environment variables
        required_env = [
            "RABBITMQ_HOST",
            "RABBITMQ_USERNAME",
            "RABBITMQ_PASSWORD",
            "GREENPLUM_HOST",
            "GREENPLUM_USER",
            "GREENPLUM_PASSWORD",
            "GEMFIRE_PUBLIC_IP",
        ]

        for key in required_env:
            if not self.get(key):
                errors.append(f"Missing required environment variable: {key}")

        # Check dataset paths
        simulation_dataset = self.get_simulation_dataset_path()
        if not os.path.exists(simulation_dataset):
            errors.append(f"Simulation dataset file not found: {simulation_dataset}")
        
        training_dataset = self.get_training_dataset_path()
        if not os.path.exists(training_dataset):
            errors.append(f"Training dataset file not found: {training_dataset}")

        return (len(errors) == 0, errors)

    def get_all_config(self) -> Dict[str, Any]:
        """Get all configuration as a dictionary"""
        return {
            "rabbitmq": self.get_rabbitmq_config(),
            "greenplum": self.get_greenplum_config(),
            "gemfire": self.get_gemfire_config(),
            "gpss": self.get_gpss_config(),
            "dataset_path": self.get_dataset_path(),  # Legacy - returns simulation dataset
            "simulation_dataset_path": self.get_simulation_dataset_path(),
            "training_dataset_path": self.get_training_dataset_path(),
            "web": self.get_web_config(),
            "simulation": self.get_simulation_config(),
            "model": {
                "sample_path": self.get(
                    "model.sample_model_path", "./models/sample_kdd_model.pkl", "yaml"
                ),
                "cache_in_memory": self.get("model.cache_in_memory", True, "yaml"),
            },
        }


# Singleton instance
_config_manager = None


def get_config() -> ConfigManager:
    """Get singleton ConfigManager instance"""
    global _config_manager
    if _config_manager is None:
        _config_manager = ConfigManager()
    return _config_manager
