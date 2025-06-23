import logging
import os
import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from taifex_pipeline.database.db_manager import DBManager

# 取得 logger
# 假設應用程式的進入點 (如 run.py) 已經呼叫了 setup_logger
logger = logging.getLogger("taifex_pipeline.ingestion.pipeline")

class IngestionPipeline:
    """
    一個汲取管線，用於掃描來源目錄，並將新檔案處理到資料庫中。
    """

    def __init__(self, db_manager: "DBManager", source_directory: str):
        """
        初始化 IngestionPipeline。

        Args:
            db_manager (DBManager): DBManager 的實例，用於資料庫操作。
            source_directory (str): 要掃描檔案的來源目錄路徑。
        """
        if db_manager is None:
            logger.error("DBManager 實例不能為 None。")
            raise ValueError("DBManager 實例不能為 None。")
        if not source_directory:
            logger.error("來源目錄路徑不能為空。")
            raise ValueError("來源目錄路徑不能為空。")
        if not os.path.isdir(source_directory):
            logger.error(f"指定的來源目錄不存在或不是一個目錄: {source_directory}")
            raise FileNotFoundError(f"指定的來源目錄不存在或不是一個目錄: {source_directory}")

        self.db_manager = db_manager
        self.source_directory = Path(source_directory) # 使用 pathlib 進行路徑操作
        logger.info(f"IngestionPipeline 初始化完成。來源目錄: '{self.source_directory}'")

    def _calculate_sha256(self, file_path: Path) -> str:
        """
        計算檔案內容的 SHA256 雜湊值。

        Args:
            file_path (Path): 檔案的路徑。

        Returns:
            str: 檔案內容的 SHA256 雜湊值 (十六進位字串)。
        """
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                # 一塊一塊讀取檔案，避免一次載入大檔案到記憶體
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            hex_digest = sha256_hash.hexdigest()
            logger.debug(f"計算檔案 '{file_path}' 的 SHA256 雜湊值為: {hex_digest}")
            return hex_digest
        except FileNotFoundError:
            logger.error(f"計算雜湊值時找不到檔案: {file_path}")
            raise
        except Exception as e:
            logger.error(f"計算檔案 '{file_path}' 的雜湊值時發生錯誤: {e}")
            raise

    def run(self):
        """
        執行汲取管線：掃描來源目錄，處理新檔案並將其儲存到資料庫。
        """
        logger.info(f"開始執行汲取管線，掃描目錄: {self.source_directory}")
        total_files_scanned = 0
        new_files_ingested = 0
        files_skipped = 0
        files_failed = 0

        # 使用 pathlib.Path.rglob 來遞迴掃描所有檔案
        # 如果只想掃描頂層目錄，可以使用 self.source_directory.glob('*')
        for file_path in self.source_directory.rglob('*'):
            if file_path.is_file(): # 只處理檔案，忽略目錄
                total_files_scanned += 1
                logger.info(f"掃描到檔案: {file_path}")
                try:
                    # 1. 計算檔案雜湊值
                    file_hash = self._calculate_sha256(file_path)
                    logger.debug(f"檔案 '{file_path}' 的 SHA256 雜湊值: {file_hash}")

                    # 2. 檢查檔案是否已存在
                    if self.db_manager.check_hash_exists(file_hash):
                        logger.info(f"檔案 '{file_path}' (雜湊: {file_hash}) 已存在於資料庫中，跳過。")
                        files_skipped += 1
                        continue

                    # 3. 如果是新檔案，處理入庫
                    logger.info(f"偵測到新檔案: '{file_path}' (雜湊: {file_hash})，開始處理...")

                    # 3a. 讀取檔案內容
                    try:
                        with open(file_path, "rb") as f:
                            raw_content = f.read()
                    except Exception as e:
                        logger.error(f"讀取檔案 '{file_path}' 內容時失敗: {e}")
                        files_failed += 1
                        continue # 跳過這個檔案，處理下一個

                    # 3b. 儲存原始檔案
                    try:
                        self.db_manager.store_raw_file(file_hash, raw_content)
                        logger.info(f"已成功將檔案 '{file_path}' 的原始內容儲存到 raw_files (雜湊: {file_hash})。")
                    except Exception as e: # 例如 IntegrityError 或其他 DB 錯誤
                        logger.error(f"儲存檔案 '{file_path}' (雜湊: {file_hash}) 到 raw_files 時失敗: {e}")
                        files_failed += 1
                        # 如果 store_raw_file 失敗，我們可能不應該繼續 add_manifest_record
                        continue

                    # 3c. 新增 manifest 記錄
                    try:
                        file_size = file_path.stat().st_size
                        # original_path 可以是相對於 source_directory 的路徑，或絕對路徑
                        # 這裡使用絕對路徑字串
                        self.db_manager.add_manifest_record(
                            file_hash=file_hash,
                            original_path=str(file_path.resolve()), # 使用絕對路徑
                            file_size_bytes=file_size,
                            source_system="IngestionPipeline" # 可以根據需要設定來源系統
                            # discovery_timestamp 和 last_modified_at_source 可以考慮加入
                        )
                        logger.info(f"已成功為檔案 '{file_path}' (雜湊: {file_hash}) 新增 manifest 記錄。")
                        new_files_ingested += 1
                    except Exception as e:
                        logger.error(f"為檔案 '{file_path}' (雜湊: {file_hash}) 新增 manifest 記錄時失敗: {e}")
                        files_failed += 1
                        # 注意：此時 raw_file 可能已儲存，但 manifest 未記錄
                        # 根據需求，可能需要一個補償機制或更複雜的交易管理
                        # 但目前依照指示，僅記錄錯誤並繼續
                        continue

                except FileNotFoundError:
                    # _calculate_sha256 可能會拋出，或者 rglob 後檔案被刪除
                    logger.warning(f"處理檔案 '{file_path}' 時找不到檔案，可能已被移動或刪除。")
                    files_failed +=1
                except Exception as e:
                    logger.error(f"處理檔案 '{file_path}' 時發生未預期錯誤: {e}", exc_info=True)
                    files_failed += 1
            elif file_path.is_dir():
                logger.debug(f"掃描到目錄: {file_path} (跳過)。")
            else:
                logger.debug(f"掃描到一個非檔案非目錄的特殊路徑: {file_path} (跳過)。")


        logger.info("--- 汲取管線執行摘要 ---")
        logger.info(f"總共掃描檔案數: {total_files_scanned}")
        logger.info(f"新汲取的檔案數: {new_files_ingested}")
        logger.info(f"因已存在而跳過的檔案數: {files_skipped}")
        logger.info(f"處理失敗的檔案數: {files_failed}")
        logger.info("--- 汲取管線執行完畢 ---")
