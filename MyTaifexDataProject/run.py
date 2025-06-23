# -*- coding: utf-8 -*-
"""
數據管道主啟動腳本 (Main Execution Script)

提供命令行介面，用於啟動和控制數據汲取與轉換管線。
支援的操作包括：
- `ingest`: 僅執行數據汲取。
- `transform`: 僅執行數據轉換，可選擇重新處理隔離檔案。
- `run_all`: 完整執行汲取與轉換流程。
- `init_db`: 初始化資料庫結構。
- `show_config`: 顯示當前的格式目錄設定。

可透過命令行參數配置日誌級別、來源目錄、工作進程數等。
"""
import argparse
import sys
import time
from pathlib import Path
import logging # 用於設定日誌級別的常數
import json # 用於 show_config

# 設定 sys.path 以便從根目錄執行時能找到 src 下的模組
PROJECT_ROOT_RUNPY = Path(__file__).resolve().parent
SRC_DIR_RUNPY = PROJECT_ROOT_RUNPY / "src"
if str(SRC_DIR_RUNPY) not in sys.path:
    sys.path.insert(0, str(SRC_DIR_RUNPY))

try:
    from taifex_pipeline.core.logger_setup import setup_global_logger, get_logger, EXECUTION_ID
    from taifex_pipeline.database import db_manager
    from taifex_pipeline.ingestion.pipeline import IngestionPipeline
    from taifex_pipeline.transformation.pipeline import TransformationPipeline
    from taifex_pipeline.core.config_loader import get_format_catalog, clear_config_cache
except ImportError as e:
    print(f"[CRITICAL] 核心模組導入失敗，無法啟動管道。請檢查環境與 PYTHONPATH。錯誤: {e}", file=sys.stderr)
    sys.exit(1)

logger: Optional[logging.Logger] = None # 將在 main 中初始化

def main():
    """
    主函式，解析命令行參數並執行相應的管道操作。
    """
    global logger

    parser = argparse.ArgumentParser(
        description="TAIFEX 數據管道主啟動腳本。\n"
                    "提供數據汲取、轉換、資料庫初始化及設定查看等功能。",
        formatter_class=argparse.RawTextHelpFormatter # 允許在 help 字串中使用換行
    )

    parser.add_argument(
        "action",
        choices=["ingest", "transform", "run_all", "show_config", "init_db"],
        help="要執行的操作:\n"
             "  ingest        - 只執行汲取管線。\n"
             "  transform     - 只執行轉換管線。\n"
             "  run_all       - 依次執行汲取和轉換管線 (最常用)。\n"
             "  show_config   - 顯示當前加載的格式目錄設定內容。\n"
             "  init_db       - 初始化資料庫 (建表等)。"
    )
    parser.add_argument(
        "--reprocess-quarantined",
        action="store_true",
        help="在執行 'transform' 或 'run_all' 操作時，\n重新處理狀態為 'QUARANTINED' 的檔案。"
    )
    parser.add_argument(
        "--source-dirs",
        nargs="+",
        metavar="DIR",
        help="指定一個或多個要掃描的來源資料夾路徑列表 (覆蓋預設值)。\n路徑相對於專案根目錄。"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        metavar="N",
        help="轉換管線平行處理的最大工作進程數 (覆蓋預設的 CPU 核心數)。"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="設定主控台日誌的輸出級別 (預設: INFO)。"
    )
    parser.add_argument(
        "--log-file-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="DEBUG",
        help="設定檔案日誌的輸出級別 (預設: DEBUG)。"
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        metavar="PATH",
        help="設定檔 (format_catalog.json) 所在的目錄名稱\n(相對於專案根目錄, 預設: config)。"
    )
    parser.add_argument(
        "--catalog-file",
        default="format_catalog.json",
        metavar="FILENAME",
        help="格式目錄設定檔的名稱 (預設: format_catalog.json)。"
    )

    args = parser.parse_args()

    # 設定日誌
    console_log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    file_log_level = getattr(logging, args.log_file_level.upper(), logging.DEBUG)
    logs_dir_main = Path("logs")

    logger = setup_global_logger(
        log_level_console=console_log_level,
        log_level_file=file_log_level,
        log_dir=logs_dir_main
    )

    logger.info(f"*** TAIFEX 數據管道啟動 (Execution ID: {EXECUTION_ID}) ***")
    logger.info(f"執行操作: {args.action}")
    if args.action in ["transform", "run_all"]:
        logger.info(f"  重新處理隔離檔案: {'是' if args.reprocess_quarantined else '否'}")
        if args.max_workers is not None: # 只有當使用者明確指定時才記錄
            logger.info(f"  最大工作進程數 (使用者指定): {args.max_workers}")
    if args.source_dirs and args.action in ["ingest", "run_all"]:
        logger.info(f"  指定來源目錄: {args.source_dirs}")
    logger.info(f"  主控台日誌級別: {args.log_level}, 檔案日誌級別: {args.log_file_level}")
    logger.info(f"  設定檔目錄: {args.config_dir}, 格式目錄檔案: {args.catalog_file}")

    overall_start_time = time.time()

    try:
        if args.action == "init_db":
            logger.info("正在執行資料庫初始化...")
            db_manager.initialize_databases()
            logger.info("資料庫初始化完成。")

        elif args.action == "show_config":
            logger.info(f"正在顯示 '{args.config_dir}/{args.catalog_file}' 內容...")
            clear_config_cache()
            try:
                catalog = get_format_catalog(config_file_name=args.catalog_file, config_dir_name=args.config_dir)
                # 使用 logger 輸出 JSON 可能會因換行符導致格式不佳，直接 print 可能更好
                # 或將 JSON 格式化後逐行 logger.info
                formatted_catalog_str = json.dumps(catalog, indent=2, ensure_ascii=False)
                logger.info(f"\n--- Format Catalog ({args.config_dir}/{args.catalog_file}) ---\n"
                            f"{formatted_catalog_str}\n"
                            f"--- End of Format Catalog ---")
            except FileNotFoundError:
                logger.error(f"錯誤：設定檔 '{PROJECT_ROOT_RUNPY / args.config_dir / args.catalog_file}' 未找到。")
            except json.JSONDecodeError:
                logger.error(f"錯誤：設定檔 '{PROJECT_ROOT_RUNPY / args.config_dir / args.catalog_file}' JSON 格式無效。")


        elif args.action == "ingest":
            ingest_pipeline = IngestionPipeline(source_directories=args.source_dirs) # source_dirs可以是None
            ingest_pipeline.run()

        elif args.action == "transform":
            transform_pipeline = TransformationPipeline(
                reprocess_quarantined=args.reprocess_quarantined,
                max_workers=args.max_workers # max_workers可以是None
            )
            transform_pipeline.run()

        elif args.action == "run_all":
            logger.info("--- 階段一：執行汲取管線 ---")
            ingest_pipeline = IngestionPipeline(source_directories=args.source_dirs)
            ingested_count, scanned_count = ingest_pipeline.run()
            logger.info(f"--- 汲取管線完成。掃描 {scanned_count} 檔案，新汲取 {ingested_count} 檔案。 ---")

            logger.info("\n--- 階段二：執行轉換管線 ---")
            transform_pipeline = TransformationPipeline(
                reprocess_quarantined=args.reprocess_quarantined,
                max_workers=args.max_workers
            )
            transform_pipeline.run()
            logger.info("--- 轉換管線完成。 ---")

    except FileNotFoundError as fnf_err:
        logger.critical(f"嚴重錯誤：找不到執行所需的檔案或目錄: {fnf_err}", exc_info=True)
        sys.exit(2)
    except Exception as e:
        logger.critical(f"管道執行過程中發生未預期的嚴重錯誤: {e}", exc_info=True)
        sys.exit(3)
    finally:
        db_manager.close_all_connections()
        overall_duration = time.time() - overall_start_time
        logger.info(f"*** TAIFEX 數據管道執行完畢 (總耗時: {overall_duration:.2f} 秒, Execution ID: {EXECUTION_ID}) ***")

if __name__ == "__main__":
    main()
