import duckdb
import os
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import pytz # 用於處理時區

from .constants import (
    FileStatus,
    COLUMN_FILE_HASH, COLUMN_ORIGINAL_PATH, COLUMN_STATUS,
    COLUMN_DISCOVERY_TIMESTAMP, COLUMN_INGESTION_TIMESTAMP,
    COLUMN_TRANSFORMATION_START_TIMESTAMP, COLUMN_TRANSFORMATION_END_TIMESTAMP,
    COLUMN_ERROR_MESSAGE, COLUMN_SOURCE_SYSTEM, COLUMN_FILE_SIZE_BYTES,
    COLUMN_LAST_MODIFIED_AT_SOURCE, COLUMN_NOTES,
    TABLE_RAW_FILES, TABLE_FILE_MANIFEST
)

# 取得 logger
# 假設 logger 已經在應用程式的某個早期點被初始化
# from taifex_pipeline.core import setup_logger
# logger = setup_logger(__name__) # 或者 getLogger
logger = logging.getLogger("taifex_pipeline.database.db_manager")


class DBManager:
    """
    統一管理所有資料庫操作的類別，目前使用 DuckDB。
    """
    _TAIPEI_TZ = pytz.timezone("Asia/Taipei")

    def __init__(self, db_path: str = "data/pipeline.duckdb"):
        """
        初始化 DBManager。

        Args:
            db_path (str): DuckDB 資料庫檔案的路徑。
                           如果設為 ":memory:"，則使用記憶體資料庫。
        """
        self.db_path = db_path
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

        if self.db_path != ":memory:":
            # 確保資料庫檔案的目錄存在
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                try:
                    os.makedirs(db_dir)
                    logger.info(f"已建立資料庫目錄: {db_dir}")
                except OSError as e:
                    logger.error(f"建立資料庫目錄 {db_dir} 失敗: {e}")
                    # 根據需求，這裡可以拋出異常或允許繼續 (但連線可能會失敗)
                    raise # 決定拋出異常，因為無法建立目錄是個嚴重問題

        try:
            logger.info(f"正在連線到 DuckDB 資料庫: {self.db_path}")
            self.conn = duckdb.connect(database=self.db_path, read_only=False)
            logger.info(f"成功連線到 DuckDB 資料庫: {self.db_path}")
        except Exception as e: # DuckDB 的 connect 可能會拋出各種異常
            logger.error(f"連線到 DuckDB 資料庫 {self.db_path} 失敗: {e}")
            raise # 重新拋出，讓呼叫者知道連線失敗

    def setup_tables(self) -> None:
        """
        建立資料庫核心表格 (raw_files, file_manifest)，如果它們不存在的話。
        """
        if not self.conn:
            logger.error("資料庫未連線，無法設定表格。")
            raise ConnectionError("資料庫未連線。")

        try:
            cursor = self.conn.cursor()

            # 建立 raw_files 表
            # file_hash TEXT PRIMARY KEY, raw_content BLOB
            sql_create_raw_files = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RAW_FILES} (
                {COLUMN_FILE_HASH} TEXT PRIMARY KEY,
                raw_content BLOB
            );
            """
            cursor.execute(sql_create_raw_files)
            logger.info(f"表格 '{TABLE_RAW_FILES}' 已確認/建立。")

            # 建立 file_manifest 表
            # 欄位定義參考 constants.py 和計畫
            sql_create_file_manifest = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_FILE_MANIFEST} (
                {COLUMN_FILE_HASH} TEXT PRIMARY KEY,
                {COLUMN_ORIGINAL_PATH} TEXT NOT NULL,
                {COLUMN_STATUS} TEXT NOT NULL,
                {COLUMN_DISCOVERY_TIMESTAMP} TIMESTAMPTZ,
                {COLUMN_INGESTION_TIMESTAMP} TIMESTAMPTZ,
                {COLUMN_TRANSFORMATION_START_TIMESTAMP} TIMESTAMPTZ,
                {COLUMN_TRANSFORMATION_END_TIMESTAMP} TIMESTAMPTZ,
                {COLUMN_ERROR_MESSAGE} TEXT,
                {COLUMN_SOURCE_SYSTEM} TEXT,
                {COLUMN_FILE_SIZE_BYTES} BIGINT,
                {COLUMN_LAST_MODIFIED_AT_SOURCE} TIMESTAMPTZ,
                {COLUMN_NOTES} TEXT
            );
            """
            cursor.execute(sql_create_file_manifest)
            logger.info(f"表格 '{TABLE_FILE_MANIFEST}' 已確認/建立。")

            self.conn.commit() # DuckDB 預設是自動 commit DDL，但明確一點比較好
            logger.info("資料庫表格設定完成。")
        except Exception as e:
            logger.error(f"設定資料庫表格時發生錯誤: {e}")
            # DuckDB 的 cursor/connection 在發生錯誤時的狀態管理可能需要注意
            # 但通常 DDL 錯誤會直接拋出，這裡可以選擇是否 rollback (如果 DDL 在交易中)
            # DuckDB 的 DDL 通常是原子性的或不在顯式交易中
            raise

    def check_hash_exists(self, file_hash: str) -> bool:
        """
        查詢 file_manifest 表，檢查一個檔案雜湊值是否存在。

        Args:
            file_hash (str): 要檢查的檔案雜湊值。

        Returns:
            bool: 如果雜湊值存在則返回 True，否則返回 False。
        """
        if not self.conn:
            logger.error("資料庫未連線，無法檢查雜湊值。")
            raise ConnectionError("資料庫未連線。")

        try:
            cursor = self.conn.cursor()
            # 使用參數化查詢以避免 SQL 注入 (雖然 file_hash 通常是安全的)
            sql = f"SELECT COUNT(*) FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH} = ?;"
            result = cursor.execute(sql, (file_hash,)).fetchone()

            if result and result[0] > 0:
                logger.debug(f"雜湊值 '{file_hash}' 已存在於 {TABLE_FILE_MANIFEST}。")
                return True
            else:
                logger.debug(f"雜湊值 '{file_hash}' 不存在於 {TABLE_FILE_MANIFEST}。")
                return False
        except Exception as e:
            logger.error(f"檢查雜湊值 '{file_hash}' 時發生錯誤: {e}")
            raise # 重新拋出，讓呼叫者處理

    def store_raw_file(self, file_hash: str, raw_content: bytes) -> None:
        """
        將檔案的原始二進位內容 (BLOB) 以交易方式安全地存入 raw_files 表。

        Args:
            file_hash (str): 檔案的雜湊值。
            raw_content (bytes): 檔案的原始二進位內容。

        Raises:
            duckdb.IntegrityError: 如果 file_hash 已存在 (違反主鍵約束)。
            Exception: 其他資料庫錯誤。
        """
        if not self.conn:
            logger.error("資料庫未連線，無法儲存原始檔案。")
            raise ConnectionError("資料庫未連線。")

        try:
            # DuckDB 的 Python API 會自動處理交易的開始和提交/回滾
            # 對於單一 INSERT，如果沒有顯式交易，它會是原子性的
            # 但為了明確和未來可能的多語句操作，可以使用顯式交易
            logger.info(f"開始儲存原始檔案，雜湊值: {file_hash}...")
            with self.conn.cursor() as cursor: # 使用 with 陳述式確保 cursor 被關閉
                # 在 DuckDB 中，BLOB 可以直接作為參數傳遞
                sql = f"INSERT INTO {TABLE_RAW_FILES} ({COLUMN_FILE_HASH}, raw_content) VALUES (?, ?);"
                cursor.execute(sql, (file_hash, raw_content))
            # self.conn.commit() # 如果沒有用 with self.conn.transaction(): 的話，可能需要 commit
            # DuckDB Python client 預設是 autocommit 模式，除非你明確開始一個交易
            # 為了確保，我們可以在 __init__ 設定 self.conn.autocommit = False 然後手動 commit
            # 或者，更簡單的方式是依賴其預設行為，或者使用 cursor.connection.commit()
            # 這裡我們假設預設的 autocommit 或 Python DB API 的隱含交易行為
            logger.info(f"成功儲存原始檔案，雜湊值: {file_hash}，大小: {len(raw_content)} bytes。")
        except duckdb.IntegrityError as e: # 主鍵衝突
            logger.error(f"儲存原始檔案失敗 (雜湊值 '{file_hash}' 已存在): {e}")
            raise
        except Exception as e:
            logger.error(f"儲存原始檔案 (雜湊值 '{file_hash}') 時發生未預期錯誤: {e}")
            # self.conn.rollback() # 如果在交易中
            raise

    def add_manifest_record(
        self,
        file_hash: str,
        original_path: str,
        file_size_bytes: Optional[int] = None,
        # last_modified_at_source: Optional[datetime] = None, # DuckDB TIMESTAMPTZ 通常接受 datetime 物件
        last_modified_at_source_str: Optional[str] = None, # 或者接受 ISO 格式字串
        source_system: Optional[str] = None,
        notes: Optional[str] = None,
        discovery_timestamp_str: Optional[str] = None # 探勘階段的時間戳
    ) -> None:
        """
        在 file_manifest 表中插入一條新的紀錄。
        初始狀態設為 RAW_INGESTED，並記錄汲取時間戳 (台北時區)。

        Args:
            file_hash (str): 檔案的雜湊值。
            original_path (str): 檔案的原始路徑。
            file_size_bytes (Optional[int]): 原始檔案大小 (bytes)。
            last_modified_at_source_str (Optional[str]): 原始檔案在來源處的最後修改時間 (ISO 格式字串)。
            source_system (Optional[str]): 標示數據來源。
            notes (Optional[str]): 其他備註。
            discovery_timestamp_str (Optional[str]): 檔案被首次發現的時間 (ISO 格式字串)。
        """
        if not self.conn:
            logger.error("資料庫未連線，無法新增 manifest 紀錄。")
            raise ConnectionError("資料庫未連線。")

        ingestion_ts_utc = datetime.now(timezone.utc)
        # DuckDB 的 TIMESTAMPTZ 類型會儲存 UTC，並在讀取時根據會話時區轉換
        # 或者，我們可以儲存帶有明確時區的 datetime 物件
        # ingestion_ts_taipei = datetime.now(self._TAIPEI_TZ)
        # 為了與 DuckDB TIMESTAMPTZ 的行為一致，通常建議傳入 UTC 的 datetime 物件
        # 或者傳入帶有正確時區資訊的 datetime 物件

        # DuckDB 接受 ISO 8601 格式的字串作為 TIMESTAMPTZ
        ingestion_ts_iso = ingestion_ts_utc.isoformat()

        status = FileStatus.RAW_INGESTED.value

        try:
            logger.info(f"開始新增 manifest 紀錄，雜湊值: {file_hash}...")
            with self.conn.cursor() as cursor:
                sql = f"""
                INSERT INTO {TABLE_FILE_MANIFEST} (
                    {COLUMN_FILE_HASH}, {COLUMN_ORIGINAL_PATH}, {COLUMN_STATUS},
                    {COLUMN_INGESTION_TIMESTAMP}, {COLUMN_FILE_SIZE_BYTES},
                    {COLUMN_LAST_MODIFIED_AT_SOURCE}, {COLUMN_SOURCE_SYSTEM}, {COLUMN_NOTES},
                    {COLUMN_DISCOVERY_TIMESTAMP}
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """
                params = (
                    file_hash, original_path, status,
                    ingestion_ts_iso, file_size_bytes,
                    last_modified_at_source_str, source_system, notes,
                    discovery_timestamp_str
                )
                cursor.execute(sql, params)
            logger.info(f"成功新增 manifest 紀錄，雜湊值: {file_hash}, 狀態: {status}")
        except duckdb.IntegrityError as e: # 主鍵衝突
            logger.error(f"新增 manifest 紀錄失敗 (雜湊值 '{file_hash}' 已存在): {e}")
            # 這裡可能需要更新現有紀錄的狀態或時間戳，而不是直接失敗
            # 但依照目前方法定義，是插入新紀錄，所以主鍵衝突是個錯誤
            raise
        except Exception as e:
            logger.error(f"新增 manifest 紀錄 (雜湊值 '{file_hash}') 時發生未預期錯誤: {e}")
            raise

    def close(self) -> None:
        """
        關閉資料庫連線。
        """
        if self.conn:
            try:
                self.conn.close()
                logger.info(f"已關閉 DuckDB 資料庫連線: {self.db_path}")
                self.conn = None
            except Exception as e:
                logger.error(f"關閉 DuckDB 連線時發生錯誤: {e}")
        else:
            logger.info("資料庫連線本來就未開啟或已關閉。")

    def __enter__(self):
        # 允許 DBManager 作為 context manager 使用
        # 如果連線在 __init__ 中失敗，這裡 self.conn 可能是 None
        if not self.conn:
             # 嘗試重新連線或拋出錯誤
             # 為了簡單起見，如果 __init__ 失敗，這裡也應該失敗
             raise ConnectionError(f"DBManager 未成功初始化連線 ({self.db_path})")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # 在 context manager 結束時自動關閉連線
        self.close()


if __name__ == '__main__':
    # 簡易測試 (更完整的測試應在 test_db_manager.py 中)
    # 設定一個臨時 logger
    if not logging.getLogger("taifex_pipeline").hasHandlers():
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # logger = logging.getLogger("taifex_pipeline.database.db_manager_test") # 更新 logger

    # 使用記憶體資料庫進行測試
    test_db_path = ":memory:"
    # test_db_path = "data/test_pipeline_main.duckdb" # 或檔案路徑
    # if os.path.exists(test_db_path) and test_db_path != ":memory:":
    #     os.remove(test_db_path)

    logger.info(f"--- 開始 DBManager 簡易測試 ({test_db_path}) ---")

    try:
        with DBManager(db_path=test_db_path) as db_manager:
            logger.info("DBManager 實例化並進入 context manager。")

            # 1. 設定表格
            db_manager.setup_tables()
            logger.info("setup_tables 已呼叫。")

            # 2. 測試 raw_file 和 manifest 記錄
            test_hash = "testhash123"
            test_content = b"This is some raw binary content."
            test_original_path = "/path/to/original/file.zip"
            test_file_size = len(test_content)
            # test_last_modified = datetime.now(DBManager._TAIPEI_TZ) - timedelta(days=1)
            # test_last_modified_str = test_last_modified.isoformat()
            test_last_modified_str = datetime.now(timezone.utc).isoformat() # 模擬
            test_source_system = "TEST_RUNNER"
            test_notes = "簡易測試筆記"
            test_discovery_ts_str = (datetime.now(timezone.utc)).isoformat()


            # 2a. 檢查雜湊是否存在 (應為 False)
            exists_before = db_manager.check_hash_exists(test_hash)
            logger.info(f"雜湊值 '{test_hash}' 存在 (之前): {exists_before}")
            assert not exists_before, "新雜湊不應存在"

            # 2b. 儲存原始檔案
            db_manager.store_raw_file(test_hash, test_content)
            logger.info(f"已呼叫 store_raw_file for '{test_hash}'.")

            # 2c. 新增 manifest 記錄
            db_manager.add_manifest_record(
                file_hash=test_hash,
                original_path=test_original_path,
                file_size_bytes=test_file_size,
                last_modified_at_source_str=test_last_modified_str,
                source_system=test_source_system,
                notes=test_notes,
                discovery_timestamp_str=test_discovery_ts_str
            )
            logger.info(f"已呼叫 add_manifest_record for '{test_hash}'.")

            # 2d. 再次檢查雜湊是否存在 (應為 True)
            exists_after = db_manager.check_hash_exists(test_hash)
            logger.info(f"雜湊值 '{test_hash}' 存在 (之後): {exists_after}")
            assert exists_after, "儲存後雜湊應存在"

            # 2e. 嘗試儲存重複的原始檔案 (應引發 IntegrityError)
            try:
                logger.info(f"嘗試儲存重複的原始檔案 (雜湊值: {test_hash})...")
                db_manager.store_raw_file(test_hash, b"different content")
                logger.error("錯誤：儲存重複的原始檔案未引發 IntegrityError。")
                assert False, "儲存重複原始檔案應引發 IntegrityError"
            except duckdb.IntegrityError:
                logger.info(f"成功：儲存重複的原始檔案引發了 IntegrityError。")
            except Exception as e_dup_raw:
                logger.error(f"錯誤：儲存重複的原始檔案引發了非預期的錯誤: {e_dup_raw}")
                assert False, f"儲存重複原始檔案引發非預期錯誤: {e_dup_raw}"


            # 2f. 嘗試新增重複的 manifest 記錄 (應引發 IntegrityError)
            try:
                logger.info(f"嘗試新增重複的 manifest 記錄 (雜湊值: {test_hash})...")
                db_manager.add_manifest_record(test_hash, "/another/path.zip")
                logger.error("錯誤：新增重複的 manifest 記錄未引發 IntegrityError。")
                assert False, "新增重複 manifest 記錄應引發 IntegrityError"
            except duckdb.IntegrityError:
                logger.info(f"成功：新增重複的 manifest 記錄引發了 IntegrityError。")
            except Exception as e_dup_manifest:
                logger.error(f"錯誤：新增重複的 manifest 記錄引發了非預期的錯誤: {e_dup_manifest}")
                assert False, f"新增重複 manifest 記錄引發非預期錯誤: {e_dup_manifest}"

            # 可以加入更多 SELECT 查詢來驗證資料
            if db_manager.conn: # 確保連線仍然存在
                raw_file_check = db_manager.conn.execute(f"SELECT COUNT(*) FROM {TABLE_RAW_FILES} WHERE {COLUMN_FILE_HASH}=?", (test_hash,)).fetchone()
                assert raw_file_check and raw_file_check[0] == 1, "raw_files 中應有一條記錄"

                manifest_record = db_manager.conn.execute(f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (test_hash,)).fetchone()
                assert manifest_record is not None, "manifest_record 中應有一條記錄"
                # 可以進一步檢查 manifest_record 的欄位值
                # 例如： manifest_record[2] (status) 應為 FileStatus.RAW_INGESTED.value
                # DuckDB fetchone() 返回元組，你需要知道欄位順序或使用 fetch_df() / fetch_arrow()
                # 或者使用 SELECT col1, col2... FROM ... 然後按名稱訪問
                manifest_df = db_manager.conn.execute(f"SELECT * FROM {TABLE_FILE_MANIFEST} WHERE {COLUMN_FILE_HASH}=?", (test_hash,)).fetchdf()
                assert not manifest_df.empty
                assert manifest_df.iloc[0][COLUMN_STATUS] == FileStatus.RAW_INGESTED.value
                assert manifest_df.iloc[0][COLUMN_ORIGINAL_PATH] == test_original_path
                logger.info(f"Manifest 記錄內容驗證部分通過。狀態: {manifest_df.iloc[0][COLUMN_STATUS]}")


        logger.info(f"DBManager context manager 退出。連線應已關閉。")
        # db_manager.conn 在 __exit__ 中應被設為 None
        # assert db_manager.conn is None, "連線在退出 context manager 後應為 None"
        # ^^^ 這裡 db_manager 已經不在作用域了，所以這個 assert 不能這樣寫
        # 需要在 with 區塊外重新實例化 (或不使用 with) 來檢查

    except Exception as e:
        logger.error(f"DBManager 簡易測試過程中發生錯誤: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # 如果是檔案型資料庫，可以在此處刪除測試檔案
        # if os.path.exists(test_db_path) and test_db_path != ":memory:":
        #     logger.info(f"正在刪除測試資料庫檔案: {test_db_path}")
        #     os.remove(test_db_path)
        #     db_dir = os.path.dirname(test_db_path)
        #     if db_dir and not os.listdir(db_dir): # 如果目錄為空則刪除
        #         os.rmdir(db_dir)
        logger.info(f"--- DBManager 簡易測試結束 ---")
