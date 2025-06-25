import sqlite3
import pandas as pd
import logging
from typing import List # Added List for type hinting

#模組級 logger
# logger = logging.getLogger(__name__) # Using passed logger as per user's code

class DatabaseWriter:
    def __init__(self, db_path: str, logger: logging.Logger):
        """
        初始化 DatabaseWriter。

        Args:
            db_path (str): SQLite 資料庫文件的路徑。
            logger (logging.Logger): 外部傳入的日誌記錄器實例。
        """
        self.db_path = db_path
        self.logger = logger # Use the passed logger
        self.conn = None
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.logger.info(f"DatabaseWriter: 已成功連接到 SQLite 資料庫: {self.db_path}")
        except sqlite3.Error as e:
            self.logger.error(f"DatabaseWriter: 連接到 SQLite 資料庫 {self.db_path} 失敗: {e}", exc_info=True)
            raise # Re-raise the exception so the caller knows connection failed

    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, primary_keys: List[str]):
        """
        將 DataFrame 的數據 "Upsert" 到指定的資料庫表中。
        如果表不存在，會嘗試創建它（依賴於 df.to_sql 的行為，但最好是預先創建表）。

        Args:
            df (pd.DataFrame): 需要寫入的 Pandas DataFrame。
            table_name (str): 目標資料庫表的名稱。
            primary_keys (List[str]): 組成主鍵的欄位名稱列表。
        """
        if not self.conn:
            self.logger.error("DatabaseWriter: 資料庫連接未建立，無法執行 upsert 操作。")
            return

        if df.empty:
            self.logger.info(f"DataFrame 為空，跳過對 {table_name} 的寫入。")
            return

        if not primary_keys:
            self.logger.error(f"Upsert 操作需要定義主鍵 (primary_keys)，但未提供給表 {table_name}。")
            # 或者可以選擇退回到簡單的 append 操作，但這可能會導致重複
            # raise ValueError(f"Primary keys must be provided for upsert operation on table {table_name}")
            return

        temp_table = f"temp_{table_name}_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S%f')}" # 更唯一的臨時表名

        try:
            # 將數據寫入臨時表
            df.to_sql(temp_table, self.conn, if_exists='replace', index=False)
            self.logger.debug(f"數據已寫入臨時表 {temp_table}。")

            # 準備 SQL 語法
            cols = [f'"{c}"' for c in df.columns] # 確保欄位名被正確引用
            update_cols_set = [f'"{col.strip()}" = excluded."{col.strip()}"' for col in df.columns if col.strip() not in primary_keys]

            if not update_cols_set: # 如果所有列都是主鍵的一部分
                self.logger.warning(f"表 {table_name} 的所有列都在主鍵中，沒有可更新的列。將執行 INSERT OR IGNORE 類似操作。")
                # 這種情況下，ON CONFLICT DO UPDATE SET ... 沒有意義
                # 可以改為 INSERT OR IGNORE，或者讓它自然失敗（如果主鍵衝突）
                # 或者，如果只想插入新行，可以這樣：
                # upsert_sql = f"""
                # INSERT OR IGNORE INTO "{table_name}" ({', '.join(cols)})
                # SELECT {', '.join(cols)} FROM "{temp_table}";
                # """
                # 但為了保持 Upsert 的語義（即使是只更新時間戳等元數據），我們還是構造一個 DO NOTHING (如果所有列都是主鍵)
                # 或者，如果我們期望至少有一個非主鍵列（例如 last_updated_timestamp），那麼 update_cols_set 不應為空
                # 這裡我們假設總是期望有可更新的列，如果沒有，是配置問題
                 upsert_sql = f"""
                 INSERT INTO "{table_name}" ({', '.join(cols)})
                 SELECT {', '.join(cols)} FROM "{temp_table}"
                 ON CONFLICT({', '.join(f'"{pk}"' for pk in primary_keys)}) DO NOTHING;
                 """
                 self.logger.info(f"因所有列均為主鍵，對表 {table_name} 採用 INSERT ... ON CONFLICT DO NOTHING 策略。")
            else:
                upsert_sql = f"""
                INSERT INTO "{table_name}" ({', '.join(cols)})
                SELECT {', '.join(cols)} FROM "{temp_table}"
                ON CONFLICT({', '.join(f'"{pk}"' for pk in primary_keys)}) DO UPDATE SET
                {', '.join(update_cols_set)};
                """

            self.logger.debug(f"準備執行的 Upsert SQL for table {table_name}: {upsert_sql}")
            cursor = self.conn.cursor()
            cursor.execute(upsert_sql)
            self.conn.commit()
            self.logger.info(f"成功將 {len(df)} 筆數據寫入/更新至 {table_name}。")

        except sqlite3.Error as e:
            self.logger.error(f"寫入 {table_name} 時發生 SQLite 錯誤: {e}", exc_info=True)
            if self.conn: # 確保連接仍然有效才回滾
                self.conn.rollback()
        except Exception as e: # 捕獲其他潛在錯誤，例如 Pandas 相關的
            self.logger.error(f"寫入 {table_name} 時發生非 SQLite 錯誤: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
        finally:
            # 確保臨時表總是能被嘗試刪除
            try:
                if self.conn:
                    cursor = self.conn.cursor()
                    cursor.execute(f"DROP TABLE IF EXISTS \"{temp_table}\"") # 引用臨時表名
                    self.logger.debug(f"臨時表 {temp_table} 已刪除。")
            except sqlite3.Error as e:
                self.logger.error(f"刪除臨時表 {temp_table} 時失敗: {e}", exc_info=True)


    def close(self):
        """關閉資料庫連接。"""
        if self.conn:
            try:
                self.conn.close()
                self.logger.info(f"資料庫連接 {self.db_path} 已成功關閉。")
                self.conn = None
            except sqlite3.Error as e:
                self.logger.error(f"關閉資料庫連接 {self.db_path} 時失敗: {e}", exc_info=True)

    def __enter__(self):
        # For context manager support
        # Connection is already established in __init__ if successful
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # For context manager support
        self.close()
