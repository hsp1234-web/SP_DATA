import pytest
import yaml
import os
from src.sp_data_v16.core.config import load_config

# Define a temporary config file name for testing
TEST_CONFIG_FILENAME = "test_config_v16.yaml"
MALFORMED_CONFIG_FILENAME = "malformed_config_v16.yaml"

@pytest.fixture(scope="function")
def temp_config_file(tmp_path):
    """ Creates a temporary valid config file for testing """
    config_content = {
        "database": {
            "manifest_db_path": "data/v16/manifest.db",
            "raw_lake_db_path": "data/v16/raw_lake.db",
            "processed_db_path": "data/v16/processed_data.db"
        },
        "logging": {
            "level": "DEBUG",
            "format": "[%(asctime)s] - %(message)s"
        },
        "paths": {
            "input_directory": "TEST_DATA/01_input_files_test"
        }
    }
    config_file_path = tmp_path / TEST_CONFIG_FILENAME
    with open(config_file_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)
    return config_file_path

@pytest.fixture(scope="function")
def malformed_config_file(tmp_path):
    """ Creates a temporary malformed config file for testing """
    config_file_path = tmp_path / MALFORMED_CONFIG_FILENAME
    with open(config_file_path, 'w', encoding='utf-8') as f:
        f.write("database: manifest_db_path: data/v16/manifest.db\n  raw_lake_db_path: data/v16/raw_lake.db") # Intentionally malformed
    return config_file_path

def test_load_config_success(temp_config_file):
    """ Tests successful loading of a valid config file. """
    config = load_config(str(temp_config_file))
    assert config["database"]["manifest_db_path"] == "data/v16/manifest.db"
    assert config["logging"]["level"] == "DEBUG"
    assert config["paths"]["input_directory"] == "TEST_DATA/01_input_files_test"

def test_load_config_file_not_found():
    """ Tests that FileNotFoundError is raised for a non-existent config file. """
    with pytest.raises(FileNotFoundError) as excinfo:
        load_config("non_existent_config_v16.yaml")
    assert "Configuration file not found at: non_existent_config_v16.yaml" in str(excinfo.value)

# Optional: Test for malformed YAML, if load_config is expected to handle it gracefully
# or if PyYAML's behavior in such cases needs to be confirmed.
# The current load_config will likely let PyYAML raise its own error (e.g., yaml.YAMLError).
def test_load_malformed_config(malformed_config_file):
    """ Tests that yaml.YAMLError (or a more specific error) is raised for a malformed config file. """
    with pytest.raises(yaml.YAMLError):
        load_config(str(malformed_config_file))
