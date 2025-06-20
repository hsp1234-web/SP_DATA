import pytest
import duckdb
import os
import datetime
from src.sp_data_v16.ingestion.manifest import ManifestManager

@pytest.fixture
def in_memory_manager():
    """Provides a ManifestManager instance using an in-memory DuckDB."""
    manager = ManifestManager(db_path=':memory:')
    yield manager
    manager.close()

@pytest.fixture
def temp_db_manager(tmp_path):
    """Provides a ManifestManager instance using a temporary DB file."""
    db_file = tmp_path / "test_manifest.db"
    manager = ManifestManager(db_path=str(db_file))
    yield manager
    manager.close()

def test_initialize_schema(in_memory_manager: ManifestManager):
    """Tests that the file_manifest table is created upon initialization."""
    # Check if the table exists by trying to query it
    try:
        in_memory_manager.con.execute("SELECT * FROM file_manifest LIMIT 1;")
    except duckdb.CatalogException:
        pytest.fail("Table 'file_manifest' was not created or not found.")

    # More specific check for table columns (optional, but good for thoroughness)
    table_info = in_memory_manager.con.execute("PRAGMA table_info('file_manifest');").fetchall()
    expected_columns = {
        'file_hash': 'VARCHAR',
        'source_path': 'VARCHAR',
        'status': 'VARCHAR',
        'ingestion_timestamp': 'TIMESTAMP'
    }
    actual_columns = {row[1]: row[2] for row in table_info} # name: type

    assert len(actual_columns) == len(expected_columns)
    for col_name, col_type in expected_columns.items():
        assert col_name in actual_columns
        # DuckDB's PRAGMA table_info might return types like 'TIMESTAMP WITH TIME ZONE'
        # So we check if the expected type is a substring of the actual type.
        assert col_type.upper() in actual_columns[col_name].upper()

    # Check primary key
    pk_column_index = -1
    for i, row_detail in enumerate(table_info):
        if row_detail[1] == 'file_hash': # column name
            pk_column_index = i
            break
    assert pk_column_index != -1, "file_hash column not found for PK check"
    assert table_info[pk_column_index][5] == 1, "file_hash is not the primary key" # pk flag is at index 5


def test_register_file_and_hash_exists(temp_db_manager: ManifestManager):
    """Tests registering a new file and then checking if its hash exists."""
    file_hash = "test_hash_123"
    source_path = "/path/to/some/file.txt"

    assert not temp_db_manager.hash_exists(file_hash), "Hash should not exist before registration."

    temp_db_manager.register_file(file_hash, source_path)
    assert temp_db_manager.hash_exists(file_hash), "Hash should exist after registration."

    # Verify content
    result = temp_db_manager.con.execute("SELECT source_path, status FROM file_manifest WHERE file_hash = ?", [file_hash]).fetchone()
    assert result is not None, "Record not found after registration."
    assert result[0] == source_path
    assert result[1] == 'registered'
    # Timestamp check can be tricky due to microseconds, so check it's a datetime object
    ts_result = temp_db_manager.con.execute("SELECT ingestion_timestamp FROM file_manifest WHERE file_hash = ?", [file_hash]).fetchone()
    assert isinstance(ts_result[0], datetime.datetime), "Timestamp was not recorded correctly."


def test_hash_exists_for_unregistered_hash(in_memory_manager: ManifestManager):
    """Tests that hash_exists returns False for a hash that hasn't been registered."""
    assert not in_memory_manager.hash_exists("unregistered_hash_456")

def test_register_duplicate_hash_raises_exception(temp_db_manager: ManifestManager):
    """
    Tests that attempting to register the same file_hash twice raises a ConstraintException.
    This verifies the primary key constraint on file_hash for de-duplication.
    """
    file_hash = "duplicate_hash_789"
    source_path_1 = "/path/to/fileA.txt"
    source_path_2 = "/path/to/fileB.txt" # Different source, same hash

    temp_db_manager.register_file(file_hash, source_path_1) # First registration should succeed

    with pytest.raises(duckdb.ConstraintException) as excinfo:
        temp_db_manager.register_file(file_hash, source_path_2) # Second registration should fail

    error_message_lower = str(excinfo.value).lower()
    assert "duplicate key" in error_message_lower and \
           ("primary key" in error_message_lower or "unique constraint" in error_message_lower)


# Example of an additional test for update_file_status (not explicitly required by issue but good practice)
def test_update_file_status(temp_db_manager: ManifestManager):
    file_hash = "status_update_hash"
    source_path = "/path/to/status_file.txt"
    temp_db_manager.register_file(file_hash, source_path)

    updated = temp_db_manager.update_file_status(file_hash, "processed")
    assert updated is True

    status_result = temp_db_manager.con.execute("SELECT status FROM file_manifest WHERE file_hash = ?", [file_hash]).fetchone()
    assert status_result[0] == "processed"

    # Test updating a non-existent hash
    updated_non_existent = temp_db_manager.update_file_status("non_existent_hash_for_status", "error")
    assert updated_non_existent is False

# Example of an additional test for get_file_status
def test_get_file_status(temp_db_manager: ManifestManager):
    file_hash = "get_status_hash"
    source_path = "/path/to/get_status_file.txt"
    temp_db_manager.register_file(file_hash, source_path)

    status = temp_db_manager.get_file_status(file_hash)
    assert status == "registered"

    temp_db_manager.update_file_status(file_hash, "error")
    status = temp_db_manager.get_file_status(file_hash)
    assert status == "error"

    status_non_existent = temp_db_manager.get_file_status("non_existent_for_get_status")
    assert status_non_existent is None
