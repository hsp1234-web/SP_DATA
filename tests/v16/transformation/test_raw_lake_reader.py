import pytest
import pathlib
import duckdb
from src.sp_data_v16.transformation.raw_lake_reader import RawLakeReader

@pytest.fixture(scope="function")
def setup_dummy_raw_lake_db(tmp_path: pathlib.Path):
    """
    Sets up a temporary DuckDB database for RawLakeReader tests.
    Creates a 'raw_files' table and populates it with sample data.
    Yields the path to the dummy database file.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    db_path = data_dir / "test_raw_lake.db"

    conn = None
    try:
        conn = duckdb.connect(database=str(db_path))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            );
        """)
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                       ('hash_exists', b'actual content'))
        conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)",
                       ('hash_empty_content', b''))
        conn.commit() # Ensure data is written
    finally:
        if conn:
            conn.close()

    yield db_path
    # tmp_path fixture handles cleanup of the directory and file

def test_get_raw_content_exists(setup_dummy_raw_lake_db):
    """
    Tests retrieving existing raw content.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_exists')
        assert content == b'actual content'
    finally:
        if reader:
            reader.close()

def test_get_raw_content_empty(setup_dummy_raw_lake_db):
    """
    Tests retrieving existing raw content that is an empty byte string.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_empty_content')
        assert content == b''
    finally:
        if reader:
            reader.close()

def test_get_raw_content_not_exists(setup_dummy_raw_lake_db):
    """
    Tests retrieving non-existent raw content.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        content = reader.get_raw_content('hash_not_exists')
        assert content is None
    finally:
        if reader:
            reader.close()

def test_connection_error_invalid_path():
    """
    Tests that a ConnectionError is raised when an invalid database path is provided.
    Note: DuckDB might create a file if the directory exists, even in read_only.
    So, using a path where directory creation would fail is more robust for this test.
    """
    invalid_path = "/path/to/non_existent_dir/non_existent_and_uncreatable.db"
    # Ensure the parent directory does not exist and cannot be created by normal user

    with pytest.raises(ConnectionError) as excinfo:
        # RawLakeReader's __init__ catches duckdb.Error and raises ConnectionError
        RawLakeReader(db_path=invalid_path)

    assert "Could not connect to Raw Lake database" in str(excinfo.value)

def test_connection_error_unreadable_path(tmp_path):
    """
    Tests connection error for a path that exists but might be unreadable (e.g. a directory).
    DuckDB specific behavior for connecting to a directory path in read_only might vary,
    but generally it should fail if it's not a valid database file.
    """
    # Create a directory where a db file would be expected
    path_is_a_directory = tmp_path / "i_am_a_directory"
    path_is_a_directory.mkdir()

    with pytest.raises(ConnectionError) as excinfo:
        RawLakeReader(db_path=str(path_is_a_directory))

    assert "Could not connect to Raw Lake database" in str(excinfo.value)


def test_get_raw_content_after_close(setup_dummy_raw_lake_db):
    """
    Tests that attempting to get content after closing the reader returns None.
    """
    db_path = setup_dummy_raw_lake_db
    reader = RawLakeReader(db_path=str(db_path)) # Connection successful
    reader.close() # Close the connection

    # Try to get content after connection is closed
    content = reader.get_raw_content('hash_exists')
    assert content is None # Expect None as per RawLakeReader implementation

def test_close_idempotency(setup_dummy_raw_lake_db):
    """
    Tests that calling close() multiple times does not cause an error.
    """
    db_path = setup_dummy_raw_lake_db
    reader = None
    try:
        reader = RawLakeReader(db_path=str(db_path))
        reader.close()
        reader.close() # Call close again
    except Exception as e:
        pytest.fail(f"Calling close() multiple times raised an exception: {e}")
    finally:
        # Ensure connection is attempted to be closed if reader was initialized
        if reader and hasattr(reader, 'con') and reader.con: # Removed .is_closed() check
            reader.close()

def test_init_handles_non_string_path_conversion(tmp_path: pathlib.Path):
    """
    Tests that __init__ correctly handles path-like objects and also
    how it (indirectly via pathlib) rejects completely invalid path types.
    """
    # Test 1: Pass an integer as db_path, expecting TypeError from pathlib.Path()
    with pytest.raises(TypeError) as excinfo_type:
        RawLakeReader(db_path=12345)
    assert "argument should be a str or an os.PathLike object" in str(excinfo_type.value)

    # Test 2: Pass a pathlib.Path object for a non-existent database.
    # Expect ConnectionError from RawLakeReader, as DuckDB will fail to connect (read_only=True needs existing file).
    non_existent_db_path_obj = tmp_path / "data" / "non_existent_db_via_path_obj.db"
    # Ensure parent directory exists for a cleaner test of file non-existence vs path non-existence
    (tmp_path / "data").mkdir(exist_ok=True)

    with pytest.raises(ConnectionError) as excinfo_conn_non_existent:
        RawLakeReader(db_path=non_existent_db_path_obj)
    assert "Could not connect to Raw Lake database" in str(excinfo_conn_non_existent.value)

    # Test 3: Pass a pathlib.Path object for an existing (dummy) database.
    # Expect successful connection.
    existent_db_path_obj = tmp_path / "data" / "existent_db_via_path_obj.db"
    conn = None
    try:
        conn = duckdb.connect(str(existent_db_path_obj))
        conn.execute("CREATE TABLE IF NOT EXISTS dummy_table (id INTEGER);")
        conn.close()
        conn = None # Mark as closed for clarity

        reader = RawLakeReader(db_path=existent_db_path_obj)
        assert reader.db_path == existent_db_path_obj # Check path stored correctly
        reader.close()
    finally:
        if conn: # If conn is not None, it means it wasn't closed successfully above
             conn.close()
        if existent_db_path_obj.exists():
            existent_db_path_obj.unlink()
