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
    Includes dummy config files, schema files, and databases with pre-populated data.
    """
    tmp_data_path = tmp_path / "data"
    tmp_config_path_dir = tmp_path / "config"
    tmp_data_path.mkdir()
    tmp_config_path_dir.mkdir()

    config_file_path = tmp_config_path_dir / "test_config_v16.yaml"
    schema_file_path = tmp_config_path_dir / "test_schemas.json"

    manifest_db_path = tmp_data_path / "manifest.db"
    raw_lake_db_path = tmp_data_path / "raw_lake.db"
    processed_db_path = tmp_data_path / "processed.db"

    # Create test_schemas.json
    schemas_content = {
        "csv_ok": {
            "keywords": ["ok_data_file"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "columns": ["id", "value"]
        },
        "csv_bad_encoding_schema": { # Schema expects utf-8
            "keywords": ["bad_encoding_file"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "columns": ["key", "data"]
        }
    }
    with open(schema_file_path, 'w', encoding='utf-8') as f:
        json.dump(schemas_content, f)

    # Create test_config_v16.yaml
    config_content = {
        "database": {
            "manifest_db_path": str(manifest_db_path),
            "raw_lake_db_path": str(raw_lake_db_path),
            "processed_db_path": str(processed_db_path),
        },
        "paths": {
            "schema_config_path": str(schema_file_path),
            "input_directory": "dummy_input_not_used_by_pipeline_directly"
        }
    }
    with open(config_file_path, 'w', encoding='utf-8') as f:
        yaml.dump(config_content, f)

    # Setup manifest.db
    m_conn = duckdb.connect(str(manifest_db_path))
    m_conn.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            file_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
    """)
    manifest_test_data = [
        ('hash_ok', '/fake/ok_file.csv', 'loaded_to_raw_lake'),
        ('hash_no_content', '/fake/no_content.csv', 'loaded_to_raw_lake'),
        ('hash_no_schema', '/fake/no_schema.csv', 'loaded_to_raw_lake'),
        ('hash_bad_encoding', '/fake/bad_encoding.csv', 'loaded_to_raw_lake'),
        ('hash_parser_error', '/fake/parser_error.csv', 'loaded_to_raw_lake'),
        ('hash_already_processed', '/fake/processed.csv', 'processed')
    ]
    for record in manifest_test_data:
        m_conn.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)", record)
    m_conn.close()

    # Setup raw_lake.db
    rl_conn = duckdb.connect(str(raw_lake_db_path))
    rl_conn.execute("CREATE TABLE IF NOT EXISTS raw_files (file_hash VARCHAR PRIMARY KEY, raw_content BLOB);")
    raw_lake_test_data = [
        ('hash_ok', b"ok_data_file\nid1,value1\nid2,value2"), # Contains keyword "ok_data_file"
        ('hash_no_schema', b"unknown_content_type\ndata1,data2"), # No matching keywords
        ('hash_bad_encoding', "bad_encoding_file\n測試鍵,測試值".encode('big5')), # Keyword for csv_bad_encoding_schema (expects utf-8)
        # Use an unclosed quote to ensure a CParserError / ParserError for hash_parser_error
        ('hash_parser_error', b"ok_data_file\n\"id1,value1\nid2,value2") # Matches csv_ok schema keywords
    ]
    # hash_no_content is intentionally omitted from raw_files table
    for record in raw_lake_test_data:
        rl_conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)", record)
    rl_conn.close()

    # Setup processed.db (empty, just needs to exist)
    p_conn = duckdb.connect(str(processed_db_path))
    p_conn.close()

    # Expected statuses after pipeline run
    expected_statuses = {
        'hash_ok': 'parsed',
        'hash_no_content': 'parse_error_no_content',
        'hash_no_schema': 'parse_error_schema_not_identified',
        'hash_bad_encoding': 'parse_error_parser_failed', # DataParser fails on decode
        'hash_parser_error': 'parse_error_parser_failed', # DataParser fails on parsing
        'hash_already_processed': 'processed' # Should remain unchanged
    }

    # For the original test_find_pending_files
    # It expects 'expected_hashes' for files that are 'loaded_to_raw_lake'
    # These are: hash_ok, hash_no_content, hash_no_schema, hash_bad_encoding, hash_parser_error
    expected_hashes_for_find_pending = sorted([
        'hash_ok', 'hash_no_content', 'hash_no_schema',
        'hash_bad_encoding', 'hash_parser_error'
    ])

    yield {
        "config_path": str(config_file_path),
        "manifest_db_path": str(manifest_db_path),
        "expected_statuses": expected_statuses,
        "expected_hashes_for_find_pending": expected_hashes_for_find_pending
    }
    # tmp_path fixture handles cleanup

def test_find_pending_files(transformation_pipeline_env):
    """
    Tests the find_pending_files method of TransformationPipeline to ensure it correctly
    identifies files that are in 'loaded_to_raw_lake' status.
    """
    config_path = transformation_pipeline_env["config_path"]
    # This test specifically uses the 'expected_hashes_for_find_pending' part of the fixture
    expected_hashes = transformation_pipeline_env["expected_hashes_for_find_pending"]

    pipeline = None
    try:
        pipeline = TransformationPipeline(config_path=config_path)

        pending_files_dicts = pipeline.find_pending_files()

        assert len(pending_files_dicts) == len(expected_hashes), \
            f"Expected {len(expected_hashes)} files, but got {len(pending_files_dicts)}"

        returned_hashes = sorted([item['file_hash'] for item in pending_files_dicts])

        assert returned_hashes == expected_hashes, \
            f"Returned file hashes do not match expected hashes.\nExpected: {expected_hashes}\nGot: {returned_hashes}"

        # Also check if other columns are present (as per the method's query)
        # and that status is correct for all files returned by find_pending_files
        for item in pending_files_dicts:
            assert 'file_path' in item
            assert 'status' in item
            assert item['status'] == 'loaded_to_raw_lake' # Verify status from query
            assert 'registration_timestamp' in item

    finally:
        if pipeline:
            pipeline.close()

def test_pipeline_run_updates_statuses(transformation_pipeline_env):
    """
    Tests the end-to-end run method of TransformationPipeline and verifies
    that file statuses in the manifest database are updated correctly based on processing outcomes.
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]
    expected_statuses = transformation_pipeline_env["expected_statuses"]

    pipeline = None
    try:
        pipeline = TransformationPipeline(config_path=config_path)
        pipeline.run()
    except Exception as e:
        # If pipeline.run() itself raises an unhandled error, fail the test
        pytest.fail(f"Pipeline run failed with an exception: {e}")
    finally:
        if pipeline:
            pipeline.close()

    # Verify statuses in manifest.db after run
    m_conn = None
    queried_statuses = {}
    try:
        m_conn = duckdb.connect(str(manifest_db_path), read_only=True)
        results = m_conn.execute("SELECT file_hash, status FROM file_manifest").fetchall()
        for row in results:
            queried_statuses[row[0]] = row[1]
    except Exception as e:
        pytest.fail(f"Failed to query manifest DB after pipeline run: {e}")
    finally:
        if m_conn:
            m_conn.close()

    assert queried_statuses == expected_statuses, \
        f"Mismatch in final statuses.\nExpected: {expected_statuses}\nGot: {queried_statuses}"
