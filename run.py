import logging
import os
# from taifex_pipeline.core import setup_logger # 暫時不使用我們自訂的 logger，以防問題出在那裡

if __name__ == "__main__":
    # 使用最基本的 logging 設定
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format, stream=sys.stdout) # 直接輸出到 stdout
    logger = logging.getLogger("simple_test_runner")

    logger.info("簡單測試：主程式 run.py 啟動。")
    print("標準輸出：簡單測試 run.py 啟動。")

    try:
        logger.debug("這是一條 DEBUG 日誌。")
        print("標準輸出：這是一條 print 語句。")

        # 引入 duckdb 並嘗試連線，但將其放在 try-except 中，以便觀察是否在此處卡住
        # logger.info("嘗試匯入 duckdb...")
        # print("標準輸出：嘗試匯入 duckdb...")
        # import duckdb
        # logger.info("duckdb 匯入成功。嘗試連線...")
        # print("標準輸出：duckdb 匯入成功。嘗試連線...")
        # con = duckdb.connect(database=':memory:', read_only=False)
        # logger.info(f"DuckDB 連線成功: {con}")
        # print(f"標準輸出：DuckDB 連線成功: {con}")
        # version = con.execute("SELECT version();").fetchone()
        # logger.info(f"DuckDB 版本: {version[0] if version else '未知'}")
        # print(f"標準輸出：DuckDB 版本: {version[0] if version else '未知'}")
        # con.close()
        # logger.info("DuckDB 連線已關閉。")
        # print("標準輸出：DuckDB 連線已關閉。")

    except Exception as e:
        logger.error(f"執行過程中發生錯誤: {e}", exc_info=True)
        print(f"標準輸出錯誤：執行過程中發生錯誤: {e}")

    logger.info("簡單測試：主程式 run.py 執行完畢。")
    print("標準輸出：簡單測試 run.py 執行完畢。")

# 需要 sys 來設定 basicConfig 的 stream
import sys
