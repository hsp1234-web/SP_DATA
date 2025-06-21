import pytest
import pandas as pd
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
        "csv_valid_data": {
            "table_name": "valid_data_table",
            "keywords": ["valid_data_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1, # Skip keyword line
            "unique_key": ["id"], # 加入 unique_key
            "columns": {
                "id": {"dtype": "integer", "nullable": False, "db_type": "INTEGER"},
                "name": {"dtype": "string", "nullable": True, "db_type": "VARCHAR"},
                "value": {"dtype": "float", "nullable": True, "db_type": "DOUBLE"}
            }
        },
        "csv_validation_error": {
            "table_name": "validation_error_table",
            "keywords": ["validation_error_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {
                "id": {"dtype": "integer", "nullable": False}, # This field will cause validation error
                "description": {"dtype": "string", "nullable": True}
            }
        },
         "csv_parser_error_schema": { # Schema for the file that will cause a CParserError
            "table_name": "parser_error_table",
            "keywords": ["parser_error_keywords"],
            "file_type": "csv",
            "encoding": "utf-8",
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {
                "colA": {"dtype": "string"},
                "colB": {"dtype": "string"}
            }
        },
        "csv_bad_encoding_schema": { # Schema expects utf-8, data will be big5
            "table_name": "bad_encoding_table",
            "keywords": ["bad_encoding_keywords"],
            "file_type": "csv",
            "encoding": "utf-8", # Schema expects utf-8
            "delimiter": ",",
            "csv_skip_rows": 1,
            "columns": {"key": {"dtype":"string"}, "data":{"dtype":"string"}}
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
        ('hash_valid_data_csv', '/fake/valid_data.csv', 'loaded_to_raw_lake'),
        ('hash_validation_err_csv', '/fake/validation_error.csv', 'loaded_to_raw_lake'),
        ('hash_parser_err_csv', '/fake/parser_error.csv', 'loaded_to_raw_lake'),
        ('hash_schema_not_found_txt', '/fake/no_schema.txt', 'loaded_to_raw_lake'),
        ('hash_bad_encoding_csv', '/fake/bad_encoding.csv', 'loaded_to_raw_lake'),
        ('hash_no_content_csv', '/fake/no_content.csv', 'loaded_to_raw_lake'),
        ('hash_already_processed_csv', '/fake/already_processed.csv', 'processed')
    ]
    for record in manifest_test_data:
        m_conn.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)", record)
    m_conn.close()

    # Setup raw_lake.db
    rl_conn = duckdb.connect(str(raw_lake_db_path))
    rl_conn.execute("CREATE TABLE IF NOT EXISTS raw_files (file_hash VARCHAR PRIMARY KEY, raw_content BLOB);")
    raw_lake_test_data = [
        ('hash_valid_data_csv', b"valid_data_keywords\n1,Alice,100.5\n2,Bob,200.0\n3,Charlie,NaN"),
        ('hash_validation_err_csv', b"validation_error_keywords\nnot_an_int,Test Data"), # "not_an_int" for non-nullable integer
        ('hash_parser_err_csv', b"parser_error_keywords\n\"unterminated_quote,valueA\ncol2,valueB"), # Matches csv_parser_error_schema
        ('hash_schema_not_found_txt', b"some_random_text_content\nthat_matches_no_schema"),
        ('hash_bad_encoding_csv', "bad_encoding_keywords\n測試鍵,測試值".encode('big5')), # For csv_bad_encoding_schema (expects utf-8)
    ]
    # hash_no_content_csv is intentionally omitted from raw_files table
    for record in raw_lake_test_data:
        rl_conn.execute("INSERT INTO raw_files (file_hash, raw_content) VALUES (?, ?)", record)
    rl_conn.close()

    # Setup processed.db (empty, ProcessedDBLoader will create tables if needed)
    p_conn = duckdb.connect(str(processed_db_path)) # Ensure it's created for pipeline init
    p_conn.close()

    # Expected statuses after pipeline run
    expected_statuses = {
        'hash_valid_data_csv': 'processed',
        'hash_validation_err_csv': 'validation_error',
        'hash_parser_err_csv': 'parse_error_parser_failed',
        'hash_schema_not_found_txt': 'parse_error_schema_not_identified',
        'hash_bad_encoding_csv': 'parse_error_parser_failed', # DataParser fails on decode for this
        'hash_no_content_csv': 'parse_error_no_content',
        'hash_already_processed_csv': 'processed' # Should remain unchanged
    }

    # For the original test_find_pending_files
    # It expects 'expected_hashes' for files that are 'loaded_to_raw_lake'
    expected_hashes_for_find_pending = sorted([
        'hash_valid_data_csv', 'hash_validation_err_csv', 'hash_parser_err_csv',
        'hash_schema_not_found_txt', 'hash_bad_encoding_csv', 'hash_no_content_csv'
    ])

    yield {
        "config_path": str(config_file_path),
        "manifest_db_path": str(manifest_db_path),
        "processed_db_path": str(processed_db_path), # Added for verification
        "expected_statuses": expected_statuses,
        "expected_hashes_for_find_pending": expected_hashes_for_find_pending,
        "valid_data_table_name": schemas_content["csv_valid_data"]["table_name"] # Pass for verification
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
    manifest statuses and data loaded into the processed database.
    """
    config_path = transformation_pipeline_env["config_path"]
    manifest_db_path = transformation_pipeline_env["manifest_db_path"]
    processed_db_path = transformation_pipeline_env["processed_db_path"]
    expected_statuses = transformation_pipeline_env["expected_statuses"]
    valid_data_table = transformation_pipeline_env["valid_data_table_name"]

    pipeline = None
    try:
        pipeline = TransformationPipeline(config_path=config_path)
        pipeline.run()
    except Exception as e:
        pytest.fail(f"Pipeline run failed with an exception: {e}")
    finally:
        if pipeline:
            pipeline.close()

    # 1. Verify statuses in manifest.db after run
    m_conn_check = None
    queried_statuses = {}
    try:
        m_conn_check = duckdb.connect(str(manifest_db_path), read_only=True)
        results = m_conn_check.execute("SELECT file_hash, status FROM file_manifest").fetchall()
        for row in results:
            queried_statuses[row[0]] = row[1]
    except Exception as e:
        pytest.fail(f"Failed to query manifest DB after pipeline run: {e}")
    finally:
        if m_conn_check:
            m_conn_check.close()

    assert queried_statuses == expected_statuses, \
        f"Mismatch in final manifest statuses.\nExpected: {expected_statuses}\nGot: {queried_statuses}"

    # 2. Verify content in processed.db for the successfully processed file
    p_conn_check = None
    try:
        p_conn_check = duckdb.connect(str(processed_db_path), read_only=True)

        # Check data for 'hash_valid_data_csv'
        loaded_df = p_conn_check.table(valid_data_table).df()

        assert len(loaded_df) == 3, f"Expected 3 rows in {valid_data_table}, got {len(loaded_df)}"

        # Verify dtypes (DuckDB types vs Pandas dtypes)
        # Example: DuckDB INTEGER maps to pandas int64 or Int64, VARCHAR to object/string, DOUBLE to float64
        # This requires knowing the schema of 'csv_valid_data'
        # id (integer, non-nullable), name (string, nullable), value (float, nullable)
        # For integer columns that are NOT NULL and contain no NaNs, pandas might read them back as int32 or int64 from DuckDB.
        # DataValidator prepares it as Int64, but after DB roundtrip, it can change if no NaNs.
        assert str(loaded_df['id'].dtype) in ('Int64', 'int64', 'int32'), \
            f"Expected id dtype Int64/int64/int32, got {loaded_df['id'].dtype}"
        assert str(loaded_df['name'].dtype) == 'object' or str(loaded_df['name'].dtype) == 'string', \
            f"Expected name dtype object/string, got {loaded_df['name'].dtype}"
        assert str(loaded_df['value'].dtype) in ('float64', 'float32'), \
            f"Expected value dtype float64/float32, got {loaded_df['value'].dtype}"

        # Verify content for a specific row (e.g., the first row)
        assert loaded_df['id'].iloc[0] == 1
        assert loaded_df['name'].iloc[0] == 'Alice'
        assert loaded_df['value'].iloc[0] == 100.5
        assert pd.isna(loaded_df['value'].iloc[2]) # For the "NaN" string in input

        # 3. Verify that tables for error files were not created or are empty
        # For this test, we'll just check that the specific validation_error_table is not in the list of tables
        # or if it exists, it's empty. A more robust check might be to list all tables.
        all_tables_query = p_conn_check.execute("SHOW TABLES;").fetchall()
        all_table_names = [table[0] for table in all_tables_query]

        # Table for validation error data should ideally not exist if validation happens before table creation attempt.
        # Or if schema identification fails, or parsing fails.
        # The ProcessedDBLoader.load_dataframe uses 'append', so table might be created by a previous successful run
        # if names were reused. Here, table names are distinct.
        assert "validation_error_table" not in all_table_names, \
            f"Table 'validation_error_table' should not exist for validation error case."
        assert "parser_error_table" not in all_table_names, \
            f"Table 'parser_error_table' should not exist for parser error case."
        # Add similar checks for tables related to other errorneous files if they have distinct table names in schema

    except Exception as e:
        pytest.fail(f"Failed to query or verify processed DB: {e}")
    finally:
        if p_conn_check:
            p_conn_check.close()
