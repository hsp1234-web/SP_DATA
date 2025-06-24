import pytest
import sqlite3
from pathlib import Path
from datetime import datetime

from src.taifex_pipeline.database.metadata_manager import MetadataManager

@pytest.fixture
def temp_db(tmp_path: Path) -> Path:
    """提供一個暫時的資料庫路徑供測試使用"""
    return tmp_path / "test_metadata.db"

@pytest.fixture
def manager(temp_db: Path) -> MetadataManager:
    """提供一個 MetadataManager 實例，並在測試後關閉連線"""
    m = MetadataManager(temp_db)
    yield m
    m.close()

def test_initialization(temp_db: Path):
    """測試初始化是否會建立資料庫檔案"""
    assert not temp_db.exists()
    manager = MetadataManager(temp_db)
    assert temp_db.exists()
    manager.close()

def test_setup_tables(manager: MetadataManager):
    """測試 setup_tables 是否能正確建立資料表和索引"""
    manager.setup_tables()

    with sqlite3.connect(manager.db_path) as conn:
        cursor = conn.cursor()
        # 檢查 `files` 表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='files';")
        assert cursor.fetchone() is not None
        # 檢查 `data_map` 表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='data_map';")
        assert cursor.fetchone() is not None
        # 檢查索引
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_data_map_date';")
        assert cursor.fetchone() is not None

def test_register_file_batch(manager: MetadataManager):
    """測試批次註冊功能"""
    manager.setup_tables()

    mock_file_data = [
        {
            "file_name": "tx_2023_q1.parquet",
            "gdrive_path": "/gdrive/tx_2023_q1.parquet",
            "last_modified": datetime(2023, 4, 1).isoformat(),
            "file_size_bytes": 1024,
            "mappings": [
                {"symbol": "TX", "data_date": "2023-01-03"},
                {"symbol": "TX", "data_date": "2023-01-04"},
            ]
        },
        {
            "file_name": "mtx_2023_q1.parquet",
            "gdrive_path": "/gdrive/mtx_2023_q1.parquet",
            "file_size_bytes": 512,
            "mappings": [
                {"symbol": "MTX", "data_date": "2023-01-03"},
            ]
        }
    ]

    manager.register_file_batch(mock_file_data)

    # 驗證資料是否已寫入
    with sqlite3.connect(manager.db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files")
        assert cursor.fetchone()[0] == 2
        cursor.execute("SELECT COUNT(*) FROM data_map")
        assert cursor.fetchone()[0] == 3
        cursor.execute("SELECT * FROM data_map WHERE symbol = 'TX'")
        assert len(cursor.fetchall()) == 2

def test_find_files_for_query(manager: MetadataManager):
    """測試查詢功能是否能返回正確的檔案路徑列表"""
    manager.setup_tables()
    mock_data = [
        {
            "file_name": "file1.parquet",
            "gdrive_path": "/path/to/file1.parquet",
            "mappings": [
                {"symbol": "TX", "data_date": "2024-01-02"},
                {"symbol": "TX", "data_date": "2024-01-03"},
                {"symbol": "MTX", "data_date": "2024-01-02"},
            ]
        },
        {
            "file_name": "file2.parquet",
            "gdrive_path": "/path/to/file2.parquet",
            "mappings": [
                {"symbol": "TX", "data_date": "2024-01-10"},
                {"symbol": "TE", "data_date": "2024-01-10"},
            ]
        },
        {
            "file_name": "file3.parquet",
            "gdrive_path": "/path/to/file3.parquet",
            "mappings": [
                {"symbol": "TX", "data_date": "2024-02-01"},
            ]
        }
    ]
    manager.register_file_batch(mock_data)

    # 案例 1: 查詢單一商品在特定範圍的檔案
    result1 = manager.find_files_for_query(symbols=["TX"], start_date="2024-01-01", end_date="2024-01-05")
    assert result1 == ["/path/to/file1.parquet"]

    # 案例 2: 查詢多個商品，應返回不重複的檔案列表
    result2 = manager.find_files_for_query(symbols=["TX", "MTX"], start_date="2024-01-01", end_date="2024-01-05")
    assert result2 == ["/path/to/file1.parquet"]

    # 案例 3: 查詢跨越多個檔案的日期範圍
    result3 = manager.find_files_for_query(symbols=["TX"], start_date="2024-01-01", end_date="2024-01-31")
    assert sorted(result3) == sorted(["/path/to/file1.parquet", "/path/to/file2.parquet"])

    # 案例 4: 查詢無結果的範圍
    result4 = manager.find_files_for_query(symbols=["NONEXIST"], start_date="2024-01-01", end_date="2024-01-31")
    assert result4 == []

    # 案例 5: 查詢空的商品列表
    result5 = manager.find_files_for_query(symbols=[], start_date="2024-01-01", end_date="2024-01-31")
    assert result5 == []
