import pytest
from unittest.mock import MagicMock, mock_open
import pathlib
from src.sp_data_v16.ingestion.raw_loader import RawLakeLoader

@pytest.fixture
def mock_duckdb_connection(mocker):
    """Fixture 來模擬 duckdb 連線。"""
    mock_con = MagicMock()
    mocker.patch('duckdb.connect', return_value=mock_con)
    return mock_con

@pytest.fixture
def raw_loader_instance(mocker, mock_duckdb_connection):
    """Fixture 來建立 RawLakeLoader 的實例，並注入模擬的 duckdb 連線。"""
    # 模擬 pathlib.Path，使其在測試中可控
    mocker.patch('pathlib.Path.read_bytes', return_value=b"test data")
    loader = RawLakeLoader(db_path="dummy_path/test_raw_lake.db")
    return loader

def test_raw_lake_loader_init_success(raw_loader_instance, mock_duckdb_connection):
    """測試 RawLakeLoader 的 __init__ 方法是否成功初始化並建立資料表。"""
    # 驗證 duckdb.connect 是否以正確的路徑被呼叫
    # duckdb.connect 的呼叫在 fixture 中已經 patch，這裡檢查 fixture 是否正確設定
    assert raw_loader_instance.db_path == pathlib.Path("dummy_path/test_raw_lake.db")
    # 驗證 _initialize_schema 是否被呼叫（隱含地透過 execute 被呼叫）
    mock_duckdb_connection.execute.assert_any_call(
        """
            CREATE TABLE IF NOT EXISTS raw_files (
                file_hash VARCHAR PRIMARY KEY,
                raw_content BLOB
            )
            """
    )

def test_save_file_success(raw_loader_instance, mock_duckdb_connection, mocker):
    """測試 save_file 方法是否成功儲存檔案內容到資料庫。"""
    mock_file_path = MagicMock(spec=pathlib.Path)
    mock_file_path.read_bytes.return_value = b"binary file content"
    file_hash = "test_file_hash_123"

    raw_loader_instance.save_file(mock_file_path, file_hash)

    # 驗證檔案的 read_bytes 方法被呼叫
    mock_file_path.read_bytes.assert_called_once()
    # 驗證資料庫 execute 和 commit 方法被呼叫
    mock_duckdb_connection.execute.assert_called_with(
        "INSERT INTO raw_files VALUES (?, ?)", (file_hash, b"binary file content")
    )
    mock_duckdb_connection.commit.assert_called_once()

def test_close_success(raw_loader_instance, mock_duckdb_connection):
    """測試 close 方法是否成功關閉資料庫連線。"""
    raw_loader_instance.close()
    # 驗證資料庫連線的 close 方法被呼叫
    mock_duckdb_connection.close.assert_called_once()

def test_init_db_connection_error(mocker):
    """測試資料庫連線失敗時，__init__ 是否會引發例外。"""
    mocker.patch('duckdb.connect', side_effect=Exception("DB Connection Error"))
    with pytest.raises(Exception, match="DB Connection Error"):
        RawLakeLoader(db_path="dummy_path/test_raw_lake.db")
