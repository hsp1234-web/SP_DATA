import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.sp_data_v16.transformation.processed_loader import ProcessedDBLoader

@pytest.fixture
def mock_duckdb_connection(mocker):
    """Fixture 來模擬 duckdb 連線。"""
    mock_con = MagicMock()
    # 模擬 execute 方法，使其可以鏈式呼叫 fetchone() 或其他方法
    mock_con.execute.return_value.fetchone.return_value = None # 預設 table 不存在
    mocker.patch('duckdb.connect', return_value=mock_con)
    return mock_con

@pytest.fixture
def processed_loader_instance(mocker, mock_duckdb_connection):
    """Fixture 來建立 ProcessedDBLoader 的實例，並注入模擬的 duckdb 連線。"""
    # 模擬 os.makedirs，避免實際建立目錄
    mocker.patch('os.makedirs')
    loader = ProcessedDBLoader(db_path="dummy_path/test_processed.db")
    return loader

@pytest.fixture
def sample_dataframe():
    """Fixture 來提供一個範例 pandas DataFrame。"""
    data = {
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10.0, 20.0, 30.0]
    }
    return pd.DataFrame(data)

def test_processed_loader_init_success(processed_loader_instance, mock_duckdb_connection):
    """測試 ProcessedDBLoader 的 __init__ 方法是否成功初始化。"""
    assert processed_loader_instance.db_path == "dummy_path/test_processed.db"
    # duckdb.connect 已經在 fixture 中被 mock_duckdb_connection mock 並驗證
    # os.makedirs 也已經在 fixture 中被 mock
    # 驗證 connect 被呼叫
    import duckdb # 為了存取 duckdb.connect
    duckdb.connect.assert_called_once_with(database="dummy_path/test_processed.db", read_only=False)

def test_load_dataframe_append_mode_success(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 未提供 unique_key 時，使用 append 模式成功載入 DataFrame。"""
    table_name = "test_table_append"
    schema_definition_no_unique_key = {
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"},
            "value": {"db_type": "DOUBLE"}
        }
    } # 沒有 unique_key

    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_no_unique_key)

        # 驗證 to_sql 被呼叫，並帶有 'append' 模式
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )

def test_load_dataframe_upsert_mode_success(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 提供 unique_key 時，使用 upsert 模式成功載入 DataFrame。"""
    table_name = "test_table_upsert"
    schema_definition_with_unique_key = {
        "unique_key": ["id"],
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"},
            "value": {"db_type": "DOUBLE"}
        }
    }

    # 模擬資料表不存在，這樣 CREATE TABLE 語句會被執行
    mock_duckdb_connection.execute.return_value.fetchone.return_value = None

    processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_with_unique_key)

    # 驗證 register 被呼叫以建立暫存視圖
    mock_duckdb_connection.register.assert_called_once()
    temp_view_name_arg = mock_duckdb_connection.register.call_args[0][0]
    assert temp_view_name_arg.startswith(f"temp_view_{table_name}")

    # 驗證 execute 被多次呼叫 (檢查資料表是否存在、CREATE TABLE、ALTER TABLE、INSERT ... ON CONFLICT)
    execute_calls = mock_duckdb_connection.execute.call_args_list

    # 1. 檢查資料表是否存在
    assert any(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table_name}'" in call[0][0] for call in execute_calls)

    # 2. CREATE TABLE (因為 fetchone 回傳 None)
    # 預期的 CREATE TABLE 語句可能很複雜，這裡簡化檢查
    # 根據 unique_key 和 columns 產生預期的 CREATE TABLE
    # "CREATE TABLE IF NOT EXISTS \"test_table_upsert\" (\"id\" INTEGER, \"name\" VARCHAR, \"value\" DOUBLE);"
    expected_create_table_sql = 'CREATE TABLE IF NOT EXISTS "test_table_upsert" ("id" INTEGER, "name" VARCHAR, "value" DOUBLE);'
    assert any(expected_create_table_sql in call[0][0] for call in execute_calls), \
        f"Expected CREATE TABLE call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 3. ALTER TABLE ADD PRIMARY KEY
    expected_alter_table_sql = 'ALTER TABLE "test_table_upsert" ADD PRIMARY KEY ("id");'
    assert any(expected_alter_table_sql in call[0][0] for call in execute_calls), \
        f"Expected ALTER TABLE call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 4. Upsert SQL
    # INSERT INTO "test_table_upsert" SELECT * FROM temp_view_test_table_upsert...
    # ON CONFLICT (id) DO UPDATE SET "id" = excluded."id", "name" = excluded."name", "value" = excluded."value";
    assert any(f'INSERT INTO "{table_name}"' in call[0][0] and \
               f'ON CONFLICT (id) DO UPDATE SET "id" = excluded."id", "name" = excluded."name", "value" = excluded."value"' in call[0][0]
               for call in execute_calls), \
        f"Expected Upsert SQL call not found. Calls: {[call[0][0] for call in execute_calls]}"

    # 驗證 unregister 被呼叫以清理暫存視圖
    mock_duckdb_connection.unregister.assert_called_once_with(temp_view_name_arg)

def test_load_dataframe_empty_dataframe(processed_loader_instance, mock_duckdb_connection):
    """測試當傳入空的 DataFrame 時，load_dataframe 是否能正確處理。"""
    empty_df = pd.DataFrame()
    table_name = "empty_table"
    schema_definition = {"columns": {"col1": {"db_type": "VARCHAR"}}} # 任意 schema

    processed_loader_instance.load_dataframe(empty_df, table_name, schema_definition)

    # 驗證沒有呼叫 to_sql 或 register/execute (因為 DataFrame 是空的)
    mock_duckdb_connection.execute.assert_not_called() # 除了可能的 init 中的 execute
    mock_duckdb_connection.register.assert_not_called()
    # 如果有 to_sql mock，也可以驗證 to_sql.assert_not_called()

def test_close_success(processed_loader_instance, mock_duckdb_connection):
    """測試 close 方法是否成功關閉資料庫連線。"""
    processed_loader_instance.close()
    # 驗證資料庫連線的 close 方法被呼叫
    mock_duckdb_connection.close.assert_called_once()

def test_init_db_connection_error(mocker):
    """測試資料庫連線失敗時，__init__ 是否會引發例外。"""
    mocker.patch('os.makedirs') # Mock os.makedirs 避免它失敗
    mocker.patch('duckdb.connect', side_effect=Exception("DB Connection Error"))
    with pytest.raises(Exception, match="DB Connection Error"):
        ProcessedDBLoader(db_path="dummy_path/test_processed.db")

def test_load_dataframe_append_mode_no_unique_key_in_schema(processed_loader_instance, mock_duckdb_connection, sample_dataframe):
    """測試 load_dataframe 在 schema_definition 中 'unique_key' 為空或不存在時，使用 append 模式。"""
    table_name = "test_table_append_no_key"
    schema_definition_no_key = {
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"}
        }
        # 'unique_key' 欄位不存在
    }

    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_no_key)
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )

    # 測試 'unique_key' 為空列表的情況
    schema_definition_empty_key = {
        "unique_key": [],
        "columns": {
            "id": {"db_type": "INTEGER"},
            "name": {"db_type": "VARCHAR"}
        }
    }
    with patch.object(sample_dataframe, 'to_sql') as mock_to_sql:
        processed_loader_instance.load_dataframe(sample_dataframe, table_name, schema_definition_empty_key)
        mock_to_sql.assert_called_once_with(
            name=table_name,
            con=mock_duckdb_connection,
            if_exists='append',
            index=False
        )
