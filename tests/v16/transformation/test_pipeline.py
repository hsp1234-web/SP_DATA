import pytest
import duckdb
import yaml
import json
import pathlib
from src.sp_data_v16.transformation.pipeline import TransformationPipeline

@pytest.fixture(scope="function")
def transformation_pipeline_env(tmp_path):
    """
    Sets up a temporary environment for TransformationPipeline integration tests.
    - Creates dummy config_v16.yaml
    - Creates dummy schemas.json
    - Initializes a manifest.db with test data
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    config_file_path = tmp_path / "config_v16.yaml"
    schema_file_path = tmp_path / "schemas.json"

    manifest_db_path = data_dir / "manifest.db"
    raw_lake_db_path = data_dir / "raw_lake.db" # Needs to exist for init
    processed_db_path = data_dir / "processed.db" # Needs to exist for init

    # Create dummy schemas.json
    dummy_schemas = {}
    with open(schema_file_path, 'w') as f:
        json.dump(dummy_schemas, f)

    # Create dummy config_v16.yaml
    dummy_config = {
        "database": {
            "manifest_db_path": str(manifest_db_path),
            "raw_lake_db_path": str(raw_lake_db_path),
            "processed_db_path": str(processed_db_path),
        },
        "paths": {
            "schema_config_path": str(schema_file_path),
            "input_directory": "dummy_input_not_used_in_this_test"
        }
    }
    with open(config_file_path, 'w') as f:
        yaml.dump(dummy_config, f)

    # Initialize and populate manifest.db
    conn = duckdb.connect(str(manifest_db_path))
    conn.execute("""
        CREATE TABLE file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            file_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
    """)

    test_data = [
        ("hash_registered_01", "/path/file_reg_01.txt", "registered"),
        ("hash_loaded_01", "/path/file_load_01.txt", "loaded_to_raw_lake"), # Expected
        ("hash_loaded_02", "/path/file_load_02.csv", "loaded_to_raw_lake"), # Expected
        ("hash_processed_01", "/path/file_proc_01.json", "processed"),
        ("hash_loaded_03", "/path/file_load_03.parquet", "loaded_to_raw_lake"), # Expected
    ]

    expected_hashes = []
    for data in test_data:
        conn.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)", data)
        if data[2] == 'loaded_to_raw_lake':
            expected_hashes.append(data[0])

    conn.close()

    # Create dummy raw_lake.db and processed.db so pipeline can connect
    # These are needed for TransformationPipeline.__init__
    duckdb.connect(str(raw_lake_db_path)).close()
    duckdb.connect(str(processed_db_path)).close()

    yield {
        "config_path": str(config_file_path),
        "expected_hashes": sorted(expected_hashes)
    }

    # Teardown (files in tmp_path are auto-cleaned by pytest)

def test_find_pending_files(transformation_pipeline_env):
    """
    Tests the find_pending_files method of TransformationPipeline.
    """
    config_path = transformation_pipeline_env["config_path"]
    expected_hashes = transformation_pipeline_env["expected_hashes"]

    pipeline = None # Initialize pipeline to None for robust cleanup
    try:
        pipeline = TransformationPipeline(config_path=config_path)

        pending_files_dicts = pipeline.find_pending_files()

        assert len(pending_files_dicts) == len(expected_hashes), \
            f"Expected {len(expected_hashes)} files, but got {len(pending_files_dicts)}"

        returned_hashes = sorted([item['file_hash'] for item in pending_files_dicts])

        assert returned_hashes == expected_hashes, \
            f"Returned file hashes do not match expected hashes.\nExpected: {expected_hashes}\nGot: {returned_hashes}"

        # Also check if other columns are present (as per the method's query)
        # and that status is correct
        for item in pending_files_dicts:
            assert 'file_path' in item
            assert 'status' in item
            assert item['status'] == 'loaded_to_raw_lake' # Verify status
            assert 'registration_timestamp' in item

    finally:
        if pipeline:
            pipeline.close()
