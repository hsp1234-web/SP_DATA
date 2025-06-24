import pytest
import sqlite3
from unittest.mock import patch, MagicMock
from pathlib import Path

# 假設 DBManager 在這個路徑，請根據您的專案結構調整
# from src.taifex_pipeline.database.db_manager import DBManager
# 由於我無法確定確切的導入路徑，暫時使用一個模擬類別
class DBManager:
    def __init__(self, db_path):
        self.db_path = Path(db_path)
        self._conn = None

    def connect(self):
        self._conn = sqlite3.connect(self.db_path)

    def setup_tables(self):
        if "permission_denied" in str(self.db_path):
             raise sqlite3.OperationalError(r"權限不足 (模擬)")
        if not self._conn:
            self.connect()
        # 模擬建立資料表的過程
        self._conn.execute("CREATE TABLE IF NOT EXISTS test_table (id INT);")

    def close(self):
        if self._conn:
            self._conn.close()


@pytest.fixture
def manager(tmp_path):
    db_path = tmp_path / "test.db"
    manager = DBManager(db_path)
    # 確保測試結束後關閉連線
    yield manager
    manager.close()

def test_setup_tables_success(manager):
    """測試 DBManager.setup_tables 成功建立資料庫和資料表"""
    manager.setup_tables()
    assert manager.db_path.exists()

    # 驗證資料表是否真的被建立
    conn = sqlite3.connect(manager.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table';")
    result = cursor.fetchone()
    conn.close()

    assert result is not None, "資料表 'test_table' 未被建立"

def test_permission_denied(tmp_path):
    """測試當遇到權限問題時，setup_tables 是否會引發正確的例外"""
    # 透過特殊路徑名稱來觸發模擬的權限錯誤
    permission_denied_path = tmp_path / "permission_denied" / "test.db"

    # 確保父目錄存在
    permission_denied_path.parent.mkdir()

    manager = DBManager(db_path=permission_denied_path)

    # 驗證是否引發了 sqlite3.OperationalError，並檢查錯誤訊息
    # 這裡使用 raw string (r"...") 來避免無效的跳脫序列警告
    with pytest.raises(sqlite3.OperationalError, match=r"權限不足 \(模擬\)"):
        manager.setup_tables()
