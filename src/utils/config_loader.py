# src/utils/config_loader.py
"""
Configuration loader for YAML config files.
"""

from pathlib import Path
from typing import Dict, Any, Optional

# YAML import with error handling
try:
    import yaml
except ImportError:
    yaml = None
    import warnings

    warnings.warn(
        "PyYAML is not installed. Config loader will have limited functionality. "
        "Install with: pip install pyyaml"
    )


class ConfigLoader:
    """Load and manage configuration from YAML files."""

    def __init__(self, config_dir: str = "config"):
        """
        Initialize config loader.

        Args:
            config_dir: Directory containing YAML config files
        """
        self.config_dir = Path(config_dir)
        self._configs = {}

    def load(self, config_name: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.

        Args:
            config_name: Name of config file (without .yaml extension)

        Returns:
            Configuration dictionary

        Raises:
            FileNotFoundError: If config file doesn't exist
            ImportError: If PyYAML is not installed
        """
        if yaml is None:
            raise ImportError(
                "PyYAML is required to load config files. Install with: pip install pyyaml"
            )

        if config_name in self._configs:
            return self._configs[config_name]

        config_path = self.config_dir / f"{config_name}.yaml"

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Cache the config
        self._configs[config_name] = config
        return config

    def get(self, config_name: str, key: str, default: Any = None) -> Any:
        """
        Get a specific configuration value.

        Args:
            config_name: Name of config file
            key: Configuration key (supports dot notation, e.g., 'engine.version')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        config = self.load(config_name)

        # Support dot notation for nested keys
        keys = key.split('.')
        value = config

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value

    def get_engine_config(self) -> Dict[str, Any]:
        """
        Get engine configuration.

        Returns:
            Engine config dictionary
        """
        return self.load('engine')

    def get_scenarios(self) -> list:
        """
        Get stress test scenarios.

        Returns:
            List of scenario dictionaries
        """
        config = self.load('scenarios')
        return config.get('scenarios', [])

    def get_logging_config(self) -> Dict[str, Any]:
        """
        Get logging configuration.

        Returns:
            Logging config dictionary (uses defaults if file doesn't exist or is empty)
        """
        try:
            config = self.load('logging')
            # If file is empty, use defaults
            if not config:
                return self._get_default_logging_config()
            return config
        except (FileNotFoundError, ImportError):
            # Return default logging config if file doesn't exist
            return self._get_default_logging_config()

    @staticmethod
    def _get_default_logging_config() -> Dict[str, Any]:
        """Return default logging configuration."""
        return {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S'
        }

    def reload(self, config_name: Optional[str] = None):
        """
        Reload configuration(s) from disk.

        Args:
            config_name: Specific config to reload, or None to reload all
        """
        if config_name:
            if config_name in self._configs:
                del self._configs[config_name]
                self.load(config_name)
        else:
            self._configs.clear()


# Global config loader instance
config_loader = ConfigLoader()


def get_config(config_name: str) -> Dict[str, Any]:
    """
    Helper function to get configuration.

    Args:
        config_name: Name of config file

    Returns:
        Configuration dictionary
    """
    return config_loader.load(config_name)


def get_config_value(config_name: str, key: str, default: Any = None) -> Any:
    """
    Helper function to get a specific configuration value.

    Args:
        config_name: Name of config file
        key: Configuration key (supports dot notation)
        default: Default value if key not found

    Returns:
        Configuration value or default
    """
    return config_loader.get(config_name, key, default)


# Example usage
if __name__ == "__main__":
    # Test the config loader
    loader = ConfigLoader()

    # Load engine config
    try:
        engine_config = loader.get_engine_config()
        print("Engine Config:")
        print(f"  Name: {engine_config.get('engine', {}).get('name')}")
        print(f"  Version: {engine_config.get('engine', {}).get('version')}")
    except FileNotFoundError as e:
        print(f"Config file not found: {e}")

    # Load scenarios
    try:
        scenarios = loader.get_scenarios()
        print(f"\nScenarios: {len(scenarios)}")
        for scenario in scenarios:
            print(f"  - {scenario.get('name')}: {scenario.get('start')} to {scenario.get('end')}")
    except FileNotFoundError as e:
        print(f"Config file not found: {e}")

    # Get specific value with dot notation
    try:
        engine_name = loader.get('engine', 'engine.name', 'Unknown')
        print(f"\nEngine name: {engine_name}")
    except FileNotFoundError as e:
        print(f"Config file not found: {e}")
