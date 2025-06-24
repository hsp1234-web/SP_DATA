import argparse
import logging
from pathlib import Path
from typing import List, Dict, Any, Iterator
import pandas as pd
import os
from datetime import datetime

# 由於 metadata_manager 和 constants 在 database 子目錄下，需要調整導入路徑
# 假設執行的根目錄是專案的根目錄
from taifex_pipeline.database.metadata_manager import MetadataManager
from taifex_pipeline.database.constants import METADATA_TABLE_DEFINITIONS # 雖然沒直接用，但 manager 會用

# 設定基礎日誌
# 讓 main 函式來設定最終的日誌級別
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__) # 獲取名為 __name__ (即 src.taifex_pipeline.scripts.metadata_scanner) 的 logger


class MetadataScanner:
    def __init__(self, parquet_dir: Path, db_path: Path, batch_size: int = 100):
        self.parquet_dir = parquet_dir
        # db_path 在 MetadataManager 初始化時會處理父目錄的建立
        self.metadata_manager = MetadataManager(db_path)
        self.batch_size = batch_size
        logger.info(f"MetadataScanner 初始化完成。Parquet 目錄: {parquet_dir}, 資料庫路徑: {db_path}, 批次大小: {batch_size}")

    def _extract_metadata_from_file(self, file_path: Path) -> Dict[str, Any] | None:
        """
        從單一 Parquet 檔案讀取並提取元數據。
        返回包含檔案資訊和數據映射的字典，如果失敗則返回 None。
        """
        try:
            logger.debug(f"正在處理檔案: {file_path}")
            # 僅讀取需要的欄位以節省記憶體
            df = pd.read_parquet(file_path, columns=['symbol', 'data_date'])

            if 'symbol' not in df.columns or 'data_date' not in df.columns:
                logger.warning(f"檔案 {file_path} 缺少 'symbol' 或 'data_date' 欄位，已跳過。")
                return None

            # 確保 data_date 欄位格式正確 (YYYY-MM-DD)
            # 如果 data_date 已經是 datetime 物件，轉換它
            # 如果是字串，嘗試解析，如果解析失敗則記錄錯誤
            try:
                df['data_date'] = pd.to_datetime(df['data_date']).dt.strftime('%Y-%m-%d')
            except Exception as date_conv_err:
                logger.warning(f"檔案 {file_path} 中的 'data_date' 欄位無法轉換為 YYYY-MM-DD 格式，已跳過此檔案。錯誤: {date_conv_err}")
                return None

            mappings = df[['symbol', 'data_date']].drop_duplicates().to_dict(orient='records')

            if not mappings:
                logger.info(f"檔案 {file_path} 未包含有效的 symbol-date 組合，但仍會記錄檔案本身。")
                # 即使沒有映射，檔案本身也可能需要被記錄

            stat = file_path.stat()
            file_info = {
                "file_name": file_path.name,
                "gdrive_path": str(file_path.resolve()),
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "file_size_bytes": stat.st_size,
                "mappings": mappings
            }
            logger.debug(f"成功提取檔案 {file_path} 的元數據。")
            return file_info
        except FileNotFoundError:
            logger.error(f"檔案不存在: {file_path}")
            return None
        except pd.errors.EmptyDataError:
            logger.warning(f"檔案 {file_path} 為空或非有效 Parquet 格式，已跳過。")
            return None
        except Exception as e:
            logger.error(f"處理檔案 {file_path} 時發生未預期錯誤: {e}", exc_info=True)
            return None

    def _scan_parquet_files(self) -> Iterator[Path]:
        """
        遞迴掃描指定目錄下的所有 .parquet 檔案。
        """
        logger.info(f"開始從目錄 {self.parquet_dir} 掃描 Parquet 檔案...")
        found_count = 0
        for root, _, files in os.walk(self.parquet_dir):
            for file in files:
                if file.lower().endswith(".parquet"):
                    found_count +=1
                    yield Path(root) / file
        logger.info(f"掃描完成，在 {self.parquet_dir} 共找到 {found_count} 個 .parquet 檔案。")


    def run_scan(self) -> None:
        """
        執行掃描並將元數據註冊到資料庫。
        """
        logger.info(f"開始執行元數據掃描程序。")
        try:
            self.metadata_manager.setup_tables()
        except Exception as e:
            logger.critical(f"無法設定元數據資料庫表格，掃描中止。錯誤: {e}", exc_info=True)
            return # 如果資料庫無法設定，則不應繼續

        file_data_batch: List[Dict[str, Any]] = []
        files_registered_count = 0
        total_files_iterated = 0

        for file_path in self._scan_parquet_files():
            total_files_iterated +=1
            metadata = self._extract_metadata_from_file(file_path)
            if metadata: # 即使 mappings 為空，只要 metadata 字典被建立就加入
                file_data_batch.append(metadata)

            if len(file_data_batch) >= self.batch_size:
                logger.info(f"達到批次大小 ({self.batch_size})，正在註冊 {len(file_data_batch)} 個檔案的元數據...")
                try:
                    self.metadata_manager.register_file_batch(file_data_batch)
                    files_registered_count += len(file_data_batch)
                    logger.info(f"成功註冊 {len(file_data_batch)} 個檔案。")
                except Exception as e:
                    logger.error(f"註冊批次時發生錯誤: {e}", exc_info=True)
                    # 決定是否要中止，或只是記錄錯誤並繼續處理下一批
                    # 目前選擇記錄並繼續
                file_data_batch = [] # 清空批次，無論成功與否

        if file_data_batch: # 處理剩餘的批次
            logger.info(f"正在註冊最後 {len(file_data_batch)} 個檔案的元數據...")
            try:
                self.metadata_manager.register_file_batch(file_data_batch)
                files_registered_count += len(file_data_batch)
                logger.info(f"成功註冊最後 {len(file_data_batch)} 個檔案。")
            except Exception as e:
                logger.error(f"註冊最後一批次時發生錯誤: {e}", exc_info=True)

        logger.info(f"元數據掃描完成。共迭代 {total_files_iterated} 個 Parquet 檔案路徑，成功註冊了 {files_registered_count} 個檔案的元數據。")

        try:
            self.metadata_manager.close()
            logger.info("元數據資料庫連線已關閉。")
        except Exception as e:
            logger.error(f"關閉元數據資料庫連線時發生錯誤: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="掃描 Parquet 檔案並建立元數據索引。",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter # 自動顯示預設值
    )
    parser.add_argument(
        "--parquet-dir",
        type=Path,
        default=Path("output/"),
        help="Parquet 檔案所在的目錄路徑。"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/metadata.db"),
        help="元數據資料庫 (metadata.db) 的檔案路徑。"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="一次批次處理並註冊到資料庫的檔案數量。"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="設定日誌記錄的級別。"
    )

    args = parser.parse_args()

    # 設定全域日誌級別
    numeric_level = getattr(logging, args.log_level.upper(), None)
    if not isinstance(numeric_level, int):
        # 這不應該發生，因為 choices 限制了選項
        print(f"錯誤的日誌級別: {args.log_level}")
        parser.print_help()
        return

    logging.basicConfig(level=numeric_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s', force=True)
    logger.setLevel(numeric_level) # 確保我們模組的 logger 也遵循設定的級別

    # 在 MetadataManager 初始化之前，它的父目錄就會被建立
    # 但 parquet_dir 需要手動確保存在，因為我們要從這裡讀取
    if not args.parquet_dir.exists():
        logger.warning(f"Parquet 目錄 {args.parquet_dir} 不存在。腳本將會執行，但可能找不到任何檔案。")
        # 根據需求，也可以選擇在此處建立它，或者直接退出
        # args.parquet_dir.mkdir(parents=True, exist_ok=True) # 如果希望自動建立

    # db_path 的父目錄由 MetadataManager 處理
    # args.db_path.parent.mkdir(parents=True, exist_ok=True)


    logger.info(f"元數據掃描器啟動，參數: {args}")

    try:
        scanner = MetadataScanner(
            parquet_dir=args.parquet_dir,
            db_path=args.db_path,
            batch_size=args.batch_size
        )
        scanner.run_scan()
    except Exception as e:
        logger.critical(f"執行元數據掃描器時發生未處理的嚴重錯誤: {e}", exc_info=True)
    finally:
        logging.shutdown() # 確保所有日誌都已刷出

if __name__ == "__main__":
    main()
