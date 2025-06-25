import duckdb
import pandas as pd
from typing import Dict, Any, Optional, List
import os

class DatabaseManager:
    def __init__(self, config: Dict[str, Any]):
        self.db_dir = config['database']['db_directory']
        self.db_name = config['database']['financial_data_db_name']
        self.db_path = os.path.join(self.db_dir, self.db_name)
        self.conn = None

    def connect(self):
        os.makedirs(self.db_dir, exist_ok=True)
        self.conn = duckdb.connect(database=self.db_path, read_only=False)
        print(f"成功連接到資料庫: {self.db_path}")

    def disconnect(self):
        if self.conn:
            self.conn.close()
            print("資料庫連接已關閉。")

    def write_data(self, table_name: str, df: pd.DataFrame, is_incremental: bool = False):
        if self.conn is None: raise ConnectionError("資料庫未連接。")
        if df.empty: return

        try:
            if is_incremental:
                # 增量寫入/更新邏輯，對於宏觀數據合併很有用
                # 使用暫存表和合併語句
                self.conn.register('temp_df', df)

                # 簡單的合併：刪除現有衝突數據，再插入新的
                pk_map = {'fact_macro_economic_data': 'metric_name'}
                pk = pk_map.get(table_name)
                if pk:
                    unique_keys = "','".join(df[pk].unique())
                    self.conn.execute(f"DELETE FROM {table_name} WHERE {pk} IN ('{unique_keys}')")

                self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")
                print(f"成功將 {len(df)} 行數據增量寫入到 '{table_name}'。")

            else:
                 # 全量替換
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                print(f"成功將 {len(df)} 行數據全量寫入到 '{table_name}'。")

        except Exception as e:
            print(f"寫入數據到 '{table_name}' 失敗: {e}")

    def fetch_data(self, query: str) -> Optional[pd.DataFrame]:
        if self.conn is None: raise ConnectionError("資料庫未連接。")
        try:
            return self.conn.execute(query).fetchdf()
        except Exception as e:
            print(f"從資料庫讀取數據失敗: {e}")
            return None
