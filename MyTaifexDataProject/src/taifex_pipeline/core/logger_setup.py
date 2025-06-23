# -*- coding: utf-8 -*-
"""
核心共用模組：日誌設定 (Logger Setup)

實現雙軌制日誌系統：
1. 主控台即時報告 (Console Output): 人類易讀、簡潔的狀態更新。
2. 結構化日誌檔案 (Structured Log File): JSON 格式，包含詳細資訊，便於機器分析。
"""
import logging
import sys
import json
import datetime
import pytz
import uuid
import os
from pathlib import Path

# --- 全域設定 ---
TAIPEI_TZ = pytz.timezone('Asia/Taipei')
LOG_FORMAT_CONSOLE = "%(asctime)s [%(levelname)s] [%(module)s:%(lineno)d] %(message)s"
LOG_DATE_FORMAT_CONSOLE = "%Y-%m-%d %H:%M:%S"

# 全局唯一的執行 ID，在 logger 首次初始化時設定
EXECUTION_ID = str(uuid.uuid4())

# --- 結構化日誌 JSON 格式化器 ---
class JsonFormatter(logging.Formatter):
    """
    自訂 JSON 格式化器，用於將日誌記錄轉換為 JSON 字串。
    """
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.datetime.fromtimestamp(record.created, tz=TAIPEI_TZ).isoformat(),
            "execution_id": EXECUTION_ID,
            "level": record.levelname,
            "module": record.module,
            "funcName": record.funcName,
            "lineno": record.lineno,
            "file_hash": getattr(record, "file_hash", "N/A"), # 允許傳遞 file_hash
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "extra_info") and isinstance(record.extra_info, dict): # type: ignore
            log_entry.update(record.extra_info) # type: ignore
        return json.dumps(log_entry, ensure_ascii=False)

# --- 日誌設定函式 ---
_logger_initialized = False
_root_logger: logging.Logger = logging.getLogger("taifex_pipeline")

def setup_global_logger(
    log_level_console: int = logging.INFO,
    log_level_file: int = logging.DEBUG,
    log_dir: Path = Path("logs") # 相對於專案根目錄
) -> logging.Logger:
    """
    設定全域的 taifex_pipeline logger。

    Args:
        log_level_console (int): 主控台輸出的日誌級別。
        log_level_file (int): 檔案輸出的日誌級別。
        log_dir (Path): 結構化日誌檔案的存放目錄。

    Returns:
        logging.Logger: 設定完成的 logger 實例。
    """
    global _logger_initialized
    global _root_logger
    global EXECUTION_ID # 確保 EXECUTION_ID 在此函式作用域內可被更新 (雖然通常只在模組加載時設定一次)

    if _logger_initialized:
        return _root_logger

    # 確保 EXECUTION_ID 在首次設定時產生
    if EXECUTION_ID is None: # 理論上模組加載時已產生，此為防禦性程式碼
        EXECUTION_ID = str(uuid.uuid4())

    _root_logger.setLevel(min(log_level_console, log_level_file)) # 設定 logger 的最低處理級別
    _root_logger.handlers = [] # 清除已有的 handlers，避免重複添加

    # 1. 主控台 Handler (Console Output)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level_console)
    console_formatter = logging.Formatter(LOG_FORMAT_CONSOLE, datefmt=LOG_DATE_FORMAT_CONSOLE)
    # 可考慮使用 RichHandler (from rich.logging import RichHandler) 替換 StreamHandler 以獲得更美觀的輸出
    # console_handler = RichHandler(rich_tracebacks=True, markup=True)
    console_handler.setFormatter(console_formatter)
    _root_logger.addHandler(console_handler)

    # 2. 結構化日誌檔案 Handler (Structured Log File)
    try:
        # 專案根目錄的確定方式，這裡假設 logger_setup.py 在 src/taifex_pipeline/core/
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        log_file_dir = project_root / log_dir
        log_file_dir.mkdir(parents=True, exist_ok=True)

        current_time_str = datetime.datetime.now(TAIPEI_TZ).strftime("%Y%m%d_%H%M%S")
        log_file_name = f"pipeline_run_{current_time_str}_{EXECUTION_ID[:8]}.log.json"
        log_file_path = log_file_dir / log_file_name

        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(log_level_file)
        json_formatter = JsonFormatter()
        file_handler.setFormatter(json_formatter)
        _root_logger.addHandler(file_handler)

        _root_logger.info(f"結構化日誌將寫入: {log_file_path}")
    except Exception as e:
        _root_logger.error(f"設定檔案日誌 Handler 失敗: {e}", exc_info=True)


    _logger_initialized = True
    _root_logger.info(f"Logger 初始化完成。Execution ID: {EXECUTION_ID}")
    return _root_logger

def get_logger(name: str | None = None) -> logging.Logger:
    """
    獲取一個 logger 實例。如果全域 logger 尚未初始化，則先進行初始化。

    Args:
        name (str, optional): Logger 的名稱。如果為 None，返回根 logger。
                              通常建議使用 __name__ 以便追溯日誌來源模組。

    Returns:
        logging.Logger: Logger 實例。
    """
    if not _logger_initialized:
        setup_global_logger() # 使用預設參數初始化

    if name:
        return logging.getLogger(name)
    return _root_logger

# --- 範例使用 (可於其他模組中這樣使用) ---
if __name__ == "__main__":
    # 在主腳本或應用程式入口處呼叫一次 setup_global_logger
    # setup_global_logger(log_level_console=logging.DEBUG) # 可以調整級別

    # 然後在各個模組中，透過 get_logger 獲取 logger
    logger = get_logger(__name__)

    logger.debug("這是一條 DEBUG 訊息。")
    logger.info("這是一條 INFO 訊息。")
    logger.warning("這是一條 WARNING 訊息。")
    logger.error("這是一條 ERROR 訊息。", extra_info={"custom_field": "some_value"}) # type: ignore

    try:
        1 / 0
    except ZeroDivisionError:
        logger.critical("這是一條 CRITICAL 訊息，包含異常堆疊。", exc_info=True, extra_info={"file_hash": "example_hash_123"}) # type: ignore

    # 測試 file_hash 傳遞
    logger.info("處理檔案相關操作。", extra_info={"file_hash": "another_file_hash_abc"}) # type: ignore

    # 假設這是另一個模組
    # import logger_setup
    # module_logger = logger_setup.get_logger("my_module")
    # module_logger.info("來自 my_module 的訊息")
    print(f"日誌 EXECUTION_ID: {EXECUTION_ID}")
    print(f"請檢查 'logs' 資料夾下的 .log.json 檔案。")
