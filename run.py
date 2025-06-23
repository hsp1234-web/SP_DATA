import logging
import os
import json # 確保 json 被匯入
from taifex_pipeline.core import setup_logger, load_format_catalog, clear_config_cache

if __name__ == "__main__":
    # 1. 設定日誌系統
    project_root = os.path.dirname(os.path.abspath(__file__))
    log_file_path = os.path.join(project_root, "logs", "run_main.log")

    log_dir = os.path.dirname(log_file_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
        # 使用 print 因為 logger 還沒設定好
        print(f"已建立日誌目錄: {log_dir}")

    logger = setup_logger("main_runner", log_level=logging.DEBUG, log_file_path=log_file_path)

    logger.info("主程式 run.py 啟動。")
    logger.debug("這是一條 DEBUG 等級的日誌訊息，用於測試日誌系統。")

    # 2. 測試設定檔讀取器
    logger.info("開始測試設定檔讀取器...")

    config_file_path = os.path.join(project_root, "config", "format_catalog.json")

    # 確保設定檔目錄存在
    config_dir = os.path.dirname(config_file_path)
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        logger.info(f"已建立設定檔目錄: {config_dir}")

    # 測試案例 1: 檔案不存在 (先移除，再讀取，再建立空的)
    logger.info("測試案例 2.1: 嘗試讀取不存在的設定檔。")
    if os.path.exists(config_file_path):
        os.remove(config_file_path)
        logger.info(f"已暫時移除設定檔 '{config_file_path}' 以進行測試。")

    catalog_non_existent = load_format_catalog()
    if catalog_non_existent is None:
        logger.info("成功測試：當設定檔不存在時，load_format_catalog 返回 None。")
    else:
        logger.error(f"測試失敗：當設定檔不存在時，期望返回 None，但得到: {catalog_non_existent}")

    clear_config_cache() # 清除快取，因為 load_format_catalog 內部可能會因 FileNotFoundError 而快取 None (雖然目前實作是直接返回)

    # 建立一個空的設定檔
    with open(config_file_path, 'w', encoding='utf-8') as f:
        json.dump({}, f)
    logger.info(f"已建立空的設定檔: {config_file_path} 以進行後續測試。")

    # 測試案例 2.2: 第一次讀取 (空的 JSON)
    logger.info("測試案例 2.2: 第一次讀取空的設定檔。")
    format_catalog_empty = load_format_catalog()
    if format_catalog_empty is not None:
        logger.info(f"成功讀取空的 format_catalog (第一次): {format_catalog_empty}")
    else:
        logger.error("第一次讀取空的 format_catalog 失敗。")

    # 測試案例 2.3: 驗證快取 (讀取應為相同物件)
    logger.info("測試案例 2.3: 驗證快取機制。")
    format_catalog_cached_empty = load_format_catalog()
    if format_catalog_cached_empty is format_catalog_empty:
        logger.info(f"成功驗證快取：第二次讀取返回與第一次相同的物件 (內容: {format_catalog_cached_empty})。")
    else:
        logger.error("快取驗證失敗：第二次讀取未返回與第一次相同的物件。")

    # 測試案例 2.4: 清除快取並讀取修改後的內容
    logger.info("測試案例 2.4: 清除快取並讀取修改後的內容。")
    # 修改檔案內容
    updated_content = {"key1": "value1", "source": "run.py_test"}
    with open(config_file_path, 'w', encoding='utf-8') as f:
        json.dump(updated_content, f)
    logger.info(f"已更新設定檔 '{config_file_path}' 內容為: {updated_content}")

    clear_config_cache()
    logger.info("快取已清除。")

    format_catalog_updated = load_format_catalog()
    if format_catalog_updated == updated_content:
        logger.info(f"成功讀取更新後的設定檔內容: {format_catalog_updated}")
    else:
        logger.error(f"讀取更新後的設定檔失敗。期望: {updated_content}, 得到: {format_catalog_updated}")

    # 測試案例 2.5: 錯誤的 JSON 格式
    logger.info("測試案例 2.5: 測試讀取格式錯誤的 JSON 檔案。")
    invalid_json_content = "{'key': 'value',,}" # Python dict-like, but invalid JSON
    with open(config_file_path, 'w', encoding='utf-8') as f:
        f.write(invalid_json_content)
    logger.info(f"已將 '{config_file_path}' 內容修改為無效 JSON: {invalid_json_content}")

    clear_config_cache() # 清除上一輪的有效快取
    format_catalog_invalid = load_format_catalog()
    if format_catalog_invalid is None:
        logger.info("成功測試：當 JSON 格式錯誤時，load_format_catalog 返回 None。")
    else:
        logger.error(f"測試失敗：當 JSON 格式錯誤時，期望返回 None，但得到: {format_catalog_invalid}")

    # 恢復 config/format_catalog.json 為空的 JSON {}
    with open(config_file_path, 'w', encoding='utf-8') as f:
        json.dump({}, f)
    logger.info(f"已將設定檔 '{config_file_path}' 重設為空的 JSON 物件。")

    logger.info("設定檔讀取器測試完畢。")
    logger.info("主程式 run.py 執行完畢。請檢查 'logs/run_main.log' 的日誌輸出。")

    print(f"\n主程式執行完畢。請檢查主控台輸出以及日誌檔案: {os.path.abspath(log_file_path)}")
