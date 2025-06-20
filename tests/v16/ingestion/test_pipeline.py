import pytest
import pathlib
import yaml
import duckdb
import hashlib # Added for SHA256 calculation
from src.sp_data_v16.ingestion.pipeline import IngestionPipeline
from src.sp_data_v16.ingestion.manifest import ManifestManager # For direct DB check

@pytest.fixture
def temp_pipeline_env(tmp_path: pathlib.Path):
    """
    Sets up a temporary environment for pipeline integration testing:
    - Temporary input directory with some files.
    - Temporary data directory for the manifest.db.
    - Temporary config file pointing to these temporary paths.
    """
    # 1. Create temporary directories
    input_dir = tmp_path / "pipeline_input"
    input_dir.mkdir()

    data_dir = tmp_path / "pipeline_data" / "v16"
    data_dir.mkdir(parents=True, exist_ok=True)

    manifest_db_file = data_dir / "manifest.db"
    raw_lake_db_file = data_dir / "raw_lake.db" # Path for raw_lake.db

    # 2. Create some dummy files in the input directory
    (input_dir / "fileA.txt").write_text("Content of file A")
    (input_dir / "fileB.log").write_text("Log data for file B")
    sub_input_dir = input_dir / "subfolder"
    sub_input_dir.mkdir()
    (sub_input_dir / "fileC.dat").write_bytes(b"Binary data for C")

    expected_files_count = 3

    # 3. Create a temporary config_v16.yaml
    temp_config_path = tmp_path / "temp_config_v16.yaml"
    config_content = {
        "database": {
            "manifest_db_path": str(manifest_db_file),
            "raw_lake_db_path": str(raw_lake_db_file), # Use the defined path
            "processed_db_path": str(data_dir / "processed_data.db") # Placeholder
        },
        "logging": {
            "level": "DEBUG",
            "format": "[%(asctime)s] - %(message)s"
        },
        "paths": {
            "input_directory": str(input_dir)
        }
    }
    with open(temp_config_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)

    return temp_config_path, manifest_db_file, raw_lake_db_file, expected_files_count, input_dir

def count_manifest_records(db_path: pathlib.Path) -> int:
    """Helper function to count records in the manifest table."""
    if not db_path.exists():
        return 0
    # Use ManifestManager's connection logic, or connect directly for test simplicity
    con = None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        count_result = con.execute("SELECT COUNT(*) FROM file_manifest;").fetchone()
        return count_result[0] if count_result else 0
    except Exception as e:
        print(f"Error counting records: {e}")
        return -1 # Indicate error
    finally:
        if con:
            con.close()

def test_pipeline_run(temp_pipeline_env):
    """
    Integration test for IngestionPipeline.run().
    Tests initial run (all files new) and a second run (all files should be skipped).
    """
    config_path, manifest_db_file, raw_lake_db_file, expected_files_count, input_dir = temp_pipeline_env

    # --- First Run ---
    # print(f"Starting first pipeline run. Config: {config_path}, DB: {manifest_db_file}") # Original line
    print(f"Starting first pipeline run. Config: {config_path}, Manifest DB: {manifest_db_file}, Raw Lake DB: {raw_lake_db_file}")
    pipeline_run1 = IngestionPipeline(config_path=str(config_path))
    pipeline_run1.run() # Errors within run should be caught by pytest if they occur

    # Verify manifest content after first run
    # All files should have been registered
    records_after_run1 = count_manifest_records(manifest_db_file)
    assert records_after_run1 == expected_files_count,         f"After first run, expected {expected_files_count} records in manifest, found {records_after_run1}"

    # Verify Raw Lake content after first run
    assert count_raw_lake_records(raw_lake_db_file) == expected_files_count, \
        "After first run, raw_files table record count mismatch"

    # Compare content of one file in Raw Lake
    file_a_path = input_dir / "fileA.txt"
    file_a_hash = get_file_sha256(file_a_path)
    original_content_a = file_a_path.read_bytes()

    con_raw_lake = None
    try:
        con_raw_lake = duckdb.connect(database=str(raw_lake_db_file), read_only=True)
        result = con_raw_lake.execute("SELECT raw_content FROM raw_files WHERE file_hash = ?", (file_a_hash,)).fetchone()
        assert result is not None, f"File with hash {file_a_hash} (fileA.txt) not found in raw_lake.db"
        assert result[0] == original_content_a, "Content of fileA.txt in raw_lake.db does not match original"
    finally:
        if con_raw_lake:
            con_raw_lake.close()

    print(f"Content of fileA.txt (hash: {file_a_hash[:8]}...) successfully verified in Raw Lake.")

    # Optional: Check specific statuses or file hashes if needed,
    # but count is a good primary indicator for this integration test.

    # --- Second Run ---
    print("\nStarting second pipeline run (expecting files to be skipped).")
    # Re-initialize pipeline to simulate a new execution context but using the same config/DB
    pipeline_run2 = IngestionPipeline(config_path=str(config_path))
    pipeline_run2.run()

    # Verify manifest content after second run
    # No new files should have been added, so count should remain the same
    records_after_run2 = count_manifest_records(manifest_db_file)
    assert records_after_run2 == expected_files_count,         f"After second run, expected {expected_files_count} records (no change), found {records_after_run2}"

    # Verify Raw Lake content after second run (should also be unchanged)
    assert count_raw_lake_records(raw_lake_db_file) == expected_files_count, \
        "After second run, raw_files table record count should not change"

    print("Pipeline integration test completed.")

# Helper function to count records in raw_lake.db
def count_raw_lake_records(db_path: pathlib.Path) -> int:
    """Helper function to count records in the raw_files table."""
    if not db_path.exists():
        return 0
    con = None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        count_result = con.execute("SELECT COUNT(*) FROM raw_files;").fetchone()
        return count_result[0] if count_result else 0
    except Exception as e:
        print(f"Error counting raw_lake records: {e}")
        return -1 # Indicate error
    finally:
        if con:
            con.close()

# Helper function to calculate SHA256 hash (consistent with FileScanner)
def get_file_sha256(file_path: pathlib.Path) -> str:
    """Calculates the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
