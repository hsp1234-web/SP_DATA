import sqlite3
import logging
from pathlib import Path
from typing import List, Dict, Any, Tuple

from .constants import METADATA_TABLE_DEFINITIONS

# 取得 logger
logger = logging.getLogger(__name__)

class MetadataManager:
    """
    管理元數據索引資料庫 (metadata.db)，負責：
    1. 建立資料表 (files, data_map)。
    2. 批次註冊 Parquet 檔案及其數據映射。
    3. 根據查詢條件（商品代號、日期範圍）查找對應的 Parquet 檔案。
    """

    def __init__(self, db_path: str | Path):
        """
        初始化並連接到元數據資料庫。

        Args:
            db_path (str | Path): SQLite 資料庫檔案的路徑。
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        logger.info(f"成功連接到元數據資料庫: {self.db_path}")

    def setup_tables(self) -> None:
        """
        根據 constants.py 中的定義，創建 `files` 和 `data_map` 表格及索引。
        """
        try:
            with self._conn:
                for name, ddl in METADATA_TABLE_DEFINITIONS.items():
                    self._conn.execute(ddl)
                    logger.debug(f"成功執行 DDL: {name}")
            logger.info("元數據資料表與索引已成功設定。")
        except sqlite3.Error as e:
            logger.error(f"建立元數據資料表時發生錯誤: {e}", exc_info=True)
            raise

    def register_file_batch(self, file_data: List[Dict[str, Any]]) -> None:
        """
        批次註冊 Parquet 檔案及其包含的數據映射，以提高寫入效率。

        Args:
            file_data (List[Dict[str, Any]]): 一個檔案資訊的列表。
                範例:
                [
                    {
                        "file_name": "data_2023.parquet",
                        "gdrive_path": "/gdrive/path/to/data_2023.parquet",
                        "last_modified": "2024-06-25 10:00:00",
                        "file_size_bytes": 1024,
                        "mappings": [
                            {"symbol": "TX", "data_date": "2023-01-01"},
                            {"symbol": "TX", "data_date": "2023-01-02"}
                        ]
                    },
                    # ... 其他檔案
                ]
        """
        if not file_data:
            logger.warning("嘗試註冊空的檔案列表，操作已跳過。")
            return

        files_to_insert: List[Tuple] = []
        maps_to_insert: List[Tuple] = []

        try:
            with self._conn:
                for item in file_data:
                    # 1. 註冊檔案並取得 file_id
                    cursor = self._conn.execute(
                        "INSERT OR IGNORE INTO files (file_name, gdrive_path, last_modified, file_size_bytes) VALUES (?, ?, ?, ?)",
                        (item['file_name'], item['gdrive_path'], item.get('last_modified'), item.get('file_size_bytes'))
                    )

                    cursor = self._conn.execute("SELECT file_id FROM files WHERE file_name = ?", (item['file_name'],))
                    file_id_row = cursor.fetchone()

                    if not file_id_row:
                        logger.error(f"無法為檔案 {item['file_name']} 創建或找到 file_id，已跳過此檔案的映射。")
                        continue

                    file_id = file_id_row['file_id']

                    # 2. 準備數據映射
                    for mapping in item.get("mappings", []):
                        maps_to_insert.append((
                            mapping['symbol'],
                            mapping['data_date'],
                            file_id
                        ))

                # 3. 批次插入 data_map
                if maps_to_insert:
                    self._conn.executemany(
                        "INSERT OR IGNORE INTO data_map (symbol, data_date, file_id) VALUES (?, ?, ?)",
                        maps_to_insert
                    )
            logger.info(f"成功批次註冊 {len(file_data)} 個檔案和 {len(maps_to_insert)} 筆數據映射。")
        except sqlite3.Error as e:
            logger.error(f"批次註冊檔案時發生錯誤: {e}", exc_info=True)
            raise

    def find_files_for_query(self, symbols: List[str], start_date: str, end_date: str) -> List[str]:
        """
        根據傳入的商品代號和日期範圍，查詢並返回所需 Parquet 檔案路徑的唯一列表。

        Args:
            symbols (List[str]): 商品代號列表 (例如: ["TX", "MTX"])。
            start_date (str): 開始日期 (格式: "YYYY-MM-DD")。
            end_date (str): 結束日期 (格式: "YYYY-MM-DD")。

        Returns:
            List[str]: 一個包含所需 Parquet 檔案路徑 (`gdrive_path`) 的唯一列表。
        """
        if not symbols:
            return []

        query = f"""
        SELECT DISTINCT f.gdrive_path
        FROM files f
        JOIN data_map dm ON f.file_id = dm.file_id
        WHERE dm.symbol IN ({','.join('?' for _ in symbols)})
          AND dm.data_date BETWEEN ? AND ?
        ORDER BY f.gdrive_path;
        """
        params = symbols + [start_date, end_date]

        try:
            with self._conn:
                cursor = self._conn.execute(query, params)
                rows = cursor.fetchall()
                paths = [row['gdrive_path'] for row in rows]
            logger.info(f"為查詢 (symbols={symbols}, dates={start_date}-{end_date}) 找到 {len(paths)} 個檔案。")
            return paths
        except sqlite3.Error as e:
            logger.error(f"查詢檔案時發生錯誤: {e}", exc_info=True)
            raise

    def close(self):
        """關閉資料庫連線"""
        if self._conn:
            self._conn.close()
            logger.info(f"元數據資料庫連線已關閉: {self.db_path}")
