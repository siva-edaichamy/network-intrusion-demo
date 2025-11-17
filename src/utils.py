"""
Utility Functions
Helper functions used across the application
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def setup_logging(log_level: str = "INFO", log_file: str = None):
    """
    Setup logging configuration

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Path to log file (optional)
    """
    # Create logs directory if it doesn't exist
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

    # Configure logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=log_format,
        handlers=[
            logging.StreamHandler(),  # Console output
            logging.FileHandler(log_file) if log_file else logging.NullHandler(),
        ],
    )

    return logging.getLogger(__name__)


def ensure_directory_exists(path: str):
    """Ensure a directory exists, create if it doesn't"""
    Path(path).mkdir(parents=True, exist_ok=True)


def load_json_file(file_path: str) -> Dict[str, Any]:
    """Load JSON file"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON file {file_path}: {str(e)}")
        return {}


def save_json_file(file_path: str, data: Dict[str, Any]):
    """Save data to JSON file"""
    try:
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save JSON file {file_path}: {str(e)}")
        return False


def get_timestamp() -> str:
    """Get current timestamp in ISO format"""
    return datetime.now().isoformat()


def sanitize_config_for_display(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sanitize configuration for display (hide passwords)

    Args:
        config: Configuration dictionary

    Returns:
        Sanitized configuration
    """
    sanitized = config.copy()

    # List of keys that should be masked
    sensitive_keys = [
        "password",
        "passwd",
        "pwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "key",
        "credentials",
    ]

    def mask_value(key: str, value: Any) -> Any:
        """Mask sensitive values"""
        key_lower = key.lower()

        if any(sensitive in key_lower for sensitive in sensitive_keys):
            if isinstance(value, str) and value:
                # Show first 2 and last 2 characters
                if len(value) > 8:
                    return f"{value[:2]}{'*' * 8}{value[-2:]}"
                else:
                    return "*" * len(value)

        if isinstance(value, dict):
            return {k: mask_value(k, v) for k, v in value.items()}

        return value

    return {k: mask_value(k, v) for k, v in sanitized.items()}


def validate_file_path(file_path: str, must_exist: bool = True) -> tuple[bool, str]:
    """
    Validate file path

    Args:
        file_path: Path to validate
        must_exist: Whether file must exist

    Returns:
        (is_valid, error_message)
    """
    if not file_path:
        return False, "File path is empty"

    path = Path(file_path)

    if must_exist and not path.exists():
        return False, f"File does not exist: {file_path}"

    if must_exist and not path.is_file():
        return False, f"Path is not a file: {file_path}"

    # Check if path is absolute or relative
    if not path.is_absolute():
        # Check if relative path is valid
        abs_path = Path.cwd() / path
        if must_exist and not abs_path.exists():
            return False, f"File does not exist: {abs_path}"

    return True, ""

