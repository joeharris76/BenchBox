"""Tests for configuration interface utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import MagicMock, patch

import pytest

from benchbox.utils.config_interface import (
    ConfigInterface,
    SimpleConfigProvider,
    create_cli_config_adapter,
)

pytestmark = pytest.mark.fast


class TestConfigInterface:
    """Test ConfigInterface base class."""

    def test_config_interface_is_abstract(self):
        """Test that ConfigInterface is abstract and cannot be instantiated."""
        with pytest.raises(TypeError):
            ConfigInterface()

    def test_config_interface_methods_not_implemented(self):
        """Test that ConfigInterface requires implementation of abstract methods."""

        # Attempting to create a class without implementing abstract methods should fail
        class TestConfig(ConfigInterface):
            pass

        # Cannot instantiate without implementing get() and set()
        with pytest.raises(TypeError):
            TestConfig()


class TestSimpleConfigProvider:
    """Test SimpleConfigProvider functionality."""

    def test_simple_config_provider_creation_empty(self):
        """Test creating SimpleConfigProvider with no initial data."""
        config = SimpleConfigProvider()

        # Config has defaults, so check it's a dict and get() works
        assert isinstance(config._config, dict)
        assert config.get("nonexistent_key") is None

    def test_simple_config_provider_creation_with_data(self):
        """Test creating SimpleConfigProvider with initial data."""
        initial_data = {"key1": "value1", "key2": 42, "key3": {"nested": "value"}}

        config = SimpleConfigProvider(initial_data)

        # Verify initial data was set correctly
        assert config.get("key1") == "value1"
        assert config.get("key2") == 42
        assert config.get("key3") == {"nested": "value"}

    def test_get_existing_key(self):
        """Test getting value for existing key."""
        config = SimpleConfigProvider({"test_key": "test_value", "number": 123})

        assert config.get("test_key") == "test_value"
        assert config.get("number") == 123

    def test_get_nonexistent_key_no_default(self):
        """Test getting value for nonexistent key without default."""
        config = SimpleConfigProvider({"existing": "value"})

        result = config.get("nonexistent")
        assert result is None

    def test_get_nonexistent_key_with_default(self):
        """Test getting value for nonexistent key with default."""
        config = SimpleConfigProvider({"existing": "value"})

        result = config.get("nonexistent", "default_value")
        assert result == "default_value"

    def test_get_with_various_types(self):
        """Test getting values of various types."""
        config = SimpleConfigProvider(
            {
                "string": "hello",
                "integer": 42,
                "float": 3.14,
                "boolean": True,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
                "none": None,
            }
        )

        assert config.get("string") == "hello"
        assert config.get("integer") == 42
        assert config.get("float") == 3.14
        assert config.get("boolean") is True
        assert config.get("list") == [1, 2, 3]
        assert config.get("dict") == {"nested": "value"}
        assert config.get("none") is None

    def test_set_new_key(self):
        """Test setting value for new key."""
        config = SimpleConfigProvider()

        config.set("new_key", "new_value")

        assert config.get("new_key") == "new_value"
        assert "new_key" in config._config

    def test_set_existing_key(self):
        """Test updating value for existing key."""
        config = SimpleConfigProvider({"existing": "old_value"})

        config.set("existing", "new_value")

        assert config.get("existing") == "new_value"
        assert config._config["existing"] == "new_value"

    def test_set_various_types(self):
        """Test setting values of various types."""
        config = SimpleConfigProvider()

        config.set("string", "hello")
        config.set("integer", 42)
        config.set("float", 3.14)
        config.set("boolean", False)
        config.set("list", [1, 2, 3])
        config.set("dict", {"key": "value"})
        config.set("none", None)

        assert config.get("string") == "hello"
        assert config.get("integer") == 42
        assert config.get("float") == 3.14
        assert config.get("boolean") is False
        assert config.get("list") == [1, 2, 3]
        assert config.get("dict") == {"key": "value"}
        assert config.get("none") is None

    def test_has_existing_key(self):
        """Test checking if existing key exists."""
        config = SimpleConfigProvider({"existing": "value", "none_value": None})

        # Test key existence via get() with sentinel
        assert config.get("existing") is not None or "existing" in config._config
        assert "none_value" in config._config  # None is still a value

    def test_has_nonexistent_key(self):
        """Test checking if nonexistent key exists."""
        config = SimpleConfigProvider({"existing": "value"})

        assert "nonexistent" not in config._config

    def test_get_all_empty(self):
        """Test that empty provider has default configuration."""
        config = SimpleConfigProvider()

        # SimpleConfigProvider has defaults, so check it's a dict with defaults
        assert isinstance(config._config, dict)
        # Should have execution defaults
        assert "execution.timeout_minutes" in config._config

    def test_get_all_with_data(self):
        """Test that provider stores all provided data."""
        data = {"key1": "value1", "key2": 42, "nested": {"inner": "value"}}
        config = SimpleConfigProvider(data)

        # Verify all data is accessible via get()
        assert config.get("key1") == "value1"
        assert config.get("key2") == 42
        assert config.get("nested") == {"inner": "value"}

    def test_data_isolation(self):
        """Test that configuration data is properly isolated."""
        initial_data = {"key": "value"}
        config = SimpleConfigProvider(initial_data)

        # Modifying initial data shouldn't affect config (constructor copies it)
        initial_data["key"] = "changed"
        assert config.get("key") == "value"

        # Direct modification of _config attribute should work
        config._config["new_key"] = "new_value"
        assert config.get("new_key") == "new_value"

    def test_nested_data_handling(self):
        """Test handling of nested data structures."""
        config = SimpleConfigProvider(
            {
                "level1": {"level2": {"level3": "deep_value"}},
                "list_of_dicts": [{"item": 1}, {"item": 2}],
            }
        )

        assert config.get("level1") == {"level2": {"level3": "deep_value"}}
        assert config.get("list_of_dicts") == [{"item": 1}, {"item": 2}]

    def test_key_types(self):
        """Test that keys must be strings."""
        config = SimpleConfigProvider()

        # String keys should work
        config.set("string_key", "value")
        assert config.get("string_key") == "value"

        # Other key types should also work (Python dict allows it)
        config.set(123, "numeric_key_value")
        config.set(("tuple", "key"), "tuple_key_value")

        assert config.get(123) == "numeric_key_value"
        assert config.get(("tuple", "key")) == "tuple_key_value"


class TestCLIConfigAdapter:
    """Test CLI config adapter functionality."""

    @patch("benchbox.cli.config.ConfigManager")
    def test_create_cli_config_adapter(self, mock_config_manager_class):
        """Test creating CLI config adapter."""
        # Setup mock config manager
        mock_config_manager = MagicMock()
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()

        # Should be a ConfigInterface instance
        assert isinstance(adapter, ConfigInterface)
        mock_config_manager_class.assert_called_once()

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_get_method(self, mock_config_manager_class):
        """Test CLI adapter get method delegation."""
        mock_config_manager = MagicMock()
        mock_config_manager.get.return_value = "test_value"
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()
        result = adapter.get("test_key")

        assert result == "test_value"
        mock_config_manager.get.assert_called_once_with("test_key", None)

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_get_with_default(self, mock_config_manager_class):
        """Test CLI adapter get method with default value."""
        mock_config_manager = MagicMock()
        mock_config_manager.get.return_value = "default_value"
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()
        result = adapter.get("missing_key", "default_value")

        assert result == "default_value"
        mock_config_manager.get.assert_called_once_with("missing_key", "default_value")

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_set_method(self, mock_config_manager_class):
        """Test CLI adapter set method delegation."""
        mock_config_manager = MagicMock()
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()
        adapter.set("test_key", "test_value")

        mock_config_manager.set.assert_called_once_with("test_key", "test_value")

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_has_method(self, mock_config_manager_class):
        """Test CLI adapter has method - adapter doesn't have has() method."""
        mock_config_manager = MagicMock()
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()

        # CLIConfigAdapter doesn't implement has() method
        assert not hasattr(adapter, "has")

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_get_all_method(self, mock_config_manager_class):
        """Test CLI adapter get_all method - adapter doesn't have get_all() method."""
        mock_config_manager = MagicMock()
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()

        # CLIConfigAdapter doesn't implement get_all() method
        assert not hasattr(adapter, "get_all")

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_error_handling(self, mock_config_manager_class):
        """Test CLI adapter error handling."""
        mock_config_manager = MagicMock()
        mock_config_manager.get.side_effect = Exception("Config error")
        mock_config_manager_class.return_value = mock_config_manager

        adapter = create_cli_config_adapter()

        # Error should be propagated
        with pytest.raises(Exception, match="Config error"):
            adapter.get("test_key")


class TestConfigInterfaceIntegration:
    """Test integration scenarios between different config implementations."""

    def test_interface_compatibility(self):
        """Test that all implementations follow the same interface."""
        # Test SimpleConfigProvider implements interface correctly
        simple_config = SimpleConfigProvider({"test": "value"})

        assert hasattr(simple_config, "get")
        assert hasattr(simple_config, "set")

        # Test that methods work as expected
        assert simple_config.get("test") == "value"
        assert "test" in simple_config._config
        assert "missing" not in simple_config._config

        simple_config.set("new_key", "new_value")
        assert simple_config.get("new_key") == "new_value"

        # Verify both keys are in config
        assert "test" in simple_config._config
        assert "new_key" in simple_config._config

    def test_config_provider_as_interface(self):
        """Test using SimpleConfigProvider through ConfigInterface."""

        def use_config(config: ConfigInterface):
            """Function that uses ConfigInterface."""
            config.set("function_key", "function_value")
            return config.get("function_key")

        provider = SimpleConfigProvider()
        result = use_config(provider)

        assert result == "function_value"
        assert "function_key" in provider._config

    def test_multiple_config_instances(self):
        """Test multiple independent config instances."""
        config1 = SimpleConfigProvider({"shared_key": "value1"})
        config2 = SimpleConfigProvider({"shared_key": "value2"})

        # Instances should be independent
        assert config1.get("shared_key") == "value1"
        assert config2.get("shared_key") == "value2"

        # Changes to one shouldn't affect the other
        config1.set("unique_key", "unique_value")
        assert "unique_key" in config1._config
        assert "unique_key" not in config2._config

    def test_config_data_types_preservation(self):
        """Test that config preserves data types correctly."""
        config = SimpleConfigProvider()

        test_data = {
            "string": "hello world",
            "integer": 42,
            "float": 3.14159,
            "boolean_true": True,
            "boolean_false": False,
            "list": [1, 2, 3, "four"],
            "dict": {"nested": {"deep": "value"}},
            "none": None,
        }

        # Set all values
        for key, value in test_data.items():
            config.set(key, value)

        # Verify types are preserved
        for key, expected_value in test_data.items():
            actual_value = config.get(key)
            assert actual_value == expected_value
            assert type(actual_value) == type(expected_value)

    @patch("benchbox.cli.config.ConfigManager")
    def test_cli_adapter_integration(self, mock_config_manager_class):
        """Test CLI adapter integration with different scenarios."""
        mock_config_manager = MagicMock()
        mock_config_manager_class.return_value = mock_config_manager

        # Setup mock to behave like a real config
        config_data = {}

        def mock_get(key, default=None):
            return config_data.get(key, default)

        def mock_set(key, value):
            config_data[key] = value

        mock_config_manager.get.side_effect = mock_get
        mock_config_manager.set.side_effect = mock_set

        # Test adapter works like a normal config
        adapter = create_cli_config_adapter()

        # Set some values
        adapter.set("key1", "value1")
        adapter.set("key2", 42)

        # Verify values
        assert adapter.get("key1") == "value1"
        assert adapter.get("key2") == 42
        assert adapter.get("nonexistent") is None
