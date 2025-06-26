import os
import sys
import json
from datetime import datetime, timedelta, timezone

# ---路徑修正，確保能導入項目內的模塊---
# 假設此文件在 project_root/src/main_simulation.py
# project_root = AI_Assisted_Historical_Backtesting
current_script_dir = os.path.dirname(os.path.abspath(__file__)) # .../src
project_root_dir = os.path.dirname(current_script_dir) # AI_Assisted_Historical_Backtesting
project_root_parent_dir = os.path.dirname(project_root_dir) # Parent of AI_Assisted_Historical_Backtesting

if project_root_parent_dir not in sys.path:
    sys.path.insert(0, project_root_parent_dir)
# ---路徑修正結束---

from AI_Assisted_Historical_Backtesting.src.utils.logger import setup_logger, PROJECT_LOGGER_NAME, get_logger
from AI_Assisted_Historical_Backtesting.src.database.db_manager import DatabaseManager
from AI_Assisted_Historical_Backtesting.src.connectors.fred_connector import FredConnector
from AI_Assisted_Historical_Backtesting.src.connectors.yfinance_connector import YFinanceConnector
from AI_Assisted_Historical_Backtesting.src.connectors.finmind_connector import FinMindConnector
from AI_Assisted_Historical_Backtesting.src.data_processing.cleaners import (
    clean_yfinance_csv_row, clean_fred_observation, clean_finmind_data_item,
    standardize_datetime_str_to_iso_utc
)
from AI_Assisted_Historical_Backtesting.src.data_processing.aligners import (
    align_ohlcv_data, get_target_period_start_utc
)
from AI_Assisted_Historical_Backtesting.src.data_processing.feature_calculator import (
    calculate_all_features # 假設 OHLCV 鍵名為 'open', 'high', 'low', 'close', 'volume'
)
from AI_Assisted_Historical_Backtesting.src.ai_logic.prompt_generator import PromptGenerator
from AI_Assisted_Historical_Backtesting.src.ai_logic.llama_agent import LlamaOllamaAgent

# --- 日誌設置 ---
# 在腳本開始時配置一次根日誌記錄器
setup_logger(PROJECT_LOGGER_NAME, level="INFO") # 主模擬使用 INFO，具體模塊可用 DEBUG
logger = get_logger(__name__) # main_simulation.py 自己的 logger

# --- 全局配置 (可以來自配置文件或命令行參數) ---
DB_FILEPATH = os.path.join(project_root_dir, "data", "project_data.sqlite") # 主數據庫路徑
SIMULATION_STEP_HOURS = 12 # 每次回溯的時間步長 (小時)
DEFAULT_TARGET_SYMBOL = "AAPL" # 主要分析的股票代碼 (示例)
# FRED 序列 (示例，可以配置更多)
FRED_SERIES_TO_FETCH = {
    "CPI": "CPIAUCSL",
    "UNRATE": "UNRATE",
    "FEDFUNDS": "FEDFUNDS"
}
# FinMind 數據集 (示例)
FINMIND_CHIP_DATASETS = {
    "InstitutionalInvestors": "InstitutionalInvestorsBuySell",
    "MarginTrading": "MarginPurchaseShortSale"
}

# 技術指標計算所需的歷史數據窗口長度 (以分析週期為單位)
# 例如，SMA20 需要 20 個週期的數據，RSI14 需要 14+1 個週期的價格變化
# 我們取一個較大的值以覆蓋常用指標，例如 60 個週期 (如果週期是12h，則為30天)
FEATURE_CALC_HISTORY_WINDOW_SIZE = 60


class HistoricalSimulation:
    def __init__(self, start_datetime_utc_iso: str, end_datetime_utc_iso: str, target_symbol: str):
        """
        初始化歷史回溯模擬。
        Args:
            start_datetime_utc_iso (str): 回溯的開始時間點 (最新的時間點, ISO UTC格式)。
            end_datetime_utc_iso (str): 回溯的結束時間點 (最早的時間點, ISO UTC格式)。
            target_symbol (str): 主要分析的股票/產品代碼。
        """
        self.start_dt_utc = self._parse_iso_datetime(start_datetime_utc_iso)
        self.end_dt_utc = self._parse_iso_datetime(end_datetime_utc_iso)
        self.target_symbol = target_symbol

        if not self.start_dt_utc or not self.end_dt_utc:
            raise ValueError("無效的開始或結束日期時間格式。")
        if self.start_dt_utc <= self.end_dt_utc:
            raise ValueError("回溯開始時間必須晚於結束時間。")

        logger.info(f"歷史回溯模擬初始化: 標的={target_symbol}, "
                    f"開始(最新)={self.start_dt_utc.isoformat()}, 結束(最早)={self.end_dt_utc.isoformat()}")

        # 初始化核心組件
        self.db_manager = DatabaseManager(db_path=DB_FILEPATH)

        # API Keys/Tokens 應從環境變量讀取 (連接器內部會處理)
        # FredConnector 不需要 key 也能獲取部分公開數據，但最好提供
        self.fred_connector = FredConnector()
        self.yf_connector = YFinanceConnector() # 快取目錄等使用預設
        self.fm_connector = FinMindConnector() # Token 從環境變量讀取

        self.prompt_generator = PromptGenerator()
        # TODO: Llama 模型名稱應可配置
        self.llama_agent = LlamaOllamaAgent(model_name="llama3:8b-instruct-q4_K_M")

        # 用於存儲最近 N 個週期的特徵數據，以便計算需要歷史窗口的指標
        # key 是 symbol，value 是 deque of feature dicts
        self._historical_features_cache = {}

    def _parse_iso_datetime(self, iso_str: str) -> datetime | None:
        try:
            # 嘗試處理帶 Z 和不帶 Z，以及有無毫秒的情況
            # standardize_datetime_str_to_iso_utc 輸出的是 YYYY-MM-DDTHH:MM:SS.sssZ
            dt_obj = datetime.strptime(iso_str, "%Y-%m-%dT%H:%M:%S.%f%z") # 假設帶 Z
            return dt_obj.astimezone(timezone.utc)
        except ValueError:
            try:
                dt_obj = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
                if dt_obj.tzinfo is None: dt_obj = dt_obj.replace(tzinfo=timezone.utc)
                return dt_obj.astimezone(timezone.utc)
            except ValueError as e:
                logger.error(f"無法解析 ISO 日期時間字符串: {iso_str}. 錯誤: {e}")
                return None

    def _get_historical_data_for_features(self, symbol: str, current_period_start_dt: datetime, window_size: int) -> list[dict]:
        """
        從數據庫或內存快取中獲取計算特徵所需的歷史 OHLCV 數據。
        返回按時間升序排列的數據列表，最新的在末尾。
        """
        # 簡化實現：假設 processed_features_hourly 已經存儲了 OHLCV
        # 並且我們需要查詢過去 window_size 個週期的數據
        # 實際中，這裡可能需要更複雜的邏輯來確保數據的連續性和質量

        # 嘗試從內存快取獲取
        if symbol in self._historical_features_cache and len(self.self._historical_features_cache[symbol]) >= window_size:
            # 如果快取足夠，並且快取的最後一條數據對應 current_period_start_dt 的上一個週期
            # （這部分檢查比較複雜，暫時簡化為只要快取夠長就用）
            # 更精確的快取管理需要確保數據與當前模擬時間點同步
            # logger.debug(f"從內存快取獲取 {symbol} 的歷史特徵數據。")
            # return list(self._historical_features_cache[symbol]) # 返回副本
            pass # 先禁用內存快取，每次都查數據庫，以簡化初始實現

        logger.info(f"正在為 {symbol} 從數據庫查詢過去 {window_size} 個週期的歷史特徵數據，截止到 {current_period_start_dt.isoformat()}")

        # 計算查詢的起始時間
        # window_size 個週期，每個週期 SIMULATION_STEP_HOURS 小時
        history_needed_start_dt = current_period_start_dt - timedelta(hours=(window_size -1) * SIMULATION_STEP_HOURS)

        query = f"""
        SELECT timestamp_period_start_utc, price_open, price_high, price_low, price_close, volume_total
        FROM processed_features_hourly
        WHERE symbol = ?
          AND timestamp_period_start_utc >= ?
          AND timestamp_period_start_utc <= ?
        ORDER BY timestamp_period_start_utc ASC
        LIMIT ?;
        """
        # SQLite 的 TIMESTAMP (TEXT) 可以直接比較字符串
        params = [
            symbol,
            history_needed_start_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z", # ISO UTC
            current_period_start_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            window_size
        ]

        results = self.db_manager.execute_query(query, params)
        if results:
            # 將元組列表轉換為字典列表，鍵名對應 calculate_all_features 的期望
            # (open, high, low, close, volume)
            # 假設 schema.sql 中 processed_features_hourly 的列名是 price_open 等
            # 而 calculate_all_features 期望的是 open, high 等
            # 這裡需要一個映射
            historical_data = []
            for row in results:
                # row[0] is timestamp_period_start_utc
                historical_data.append({
                    "timestamp_utc": row[0], # 保留原始時間戳用於可能的調試或排序
                    "open": row[1], "high": row[2], "low": row[3],
                    "close": row[4], "volume": row[5]
                })

            # 更新內存快取 (如果啟用)
            # if symbol not in self._historical_features_cache:
            #     self._historical_features_cache[symbol] = deque(maxlen=window_size + 10) # 比窗口稍大
            # self._historical_features_cache[symbol].extend(historical_data) # 這裡需要確保不重複添加

            return historical_data
        else:
            logger.warning(f"未能為 {symbol} 在 {current_period_start_dt.isoformat()} 之前找到足夠的歷史特徵數據。")
            return []

    def _update_historical_features_cache(self, symbol: str, features_for_current_period: dict):
        """將當前週期的特徵（包含OHLCV）更新到內存快取。"""
        if symbol not in self._historical_features_cache:
            self._historical_features_cache[symbol] = deque(maxlen=FEATURE_CALC_HISTORY_WINDOW_SIZE + 5) # 稍大於窗口

        # 構建一個包含 OHLCV 的字典加入快取，以便下次 get_historical_data_for_features 能用
        cache_entry = {
            "timestamp_utc": features_for_current_period.get("timestamp_period_start_utc"), # 必須有
            "open": features_for_current_period.get("price_open"),
            "high": features_for_current_period.get("price_high"),
            "low": features_for_current_period.get("price_low"),
            "close": features_for_current_period.get("price_close"),
            "volume": features_for_current_period.get("volume_total")
        }
        if cache_entry["timestamp_utc"]: # 只有在時間戳有效時才加入
            self._historical_features_cache[symbol].append(cache_entry)


    def run_simulation_period(self, current_period_start_dt: datetime, symbol: str):
        """
        為單個時間週期執行模擬步驟。
        """
        current_period_start_iso = current_period_start_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        logger.info(f"--- 開始處理週期: {symbol} @ {current_period_start_iso} ---")

        # 1. 數據獲取 (簡化：假設我們主要關注 target_symbol 的 YFinance 數據)
        #    真實情況下，這裡會根據 T_current 和 symbol 調用多個連接器
        #    例如，獲取 target_symbol 的 OHLCV，獲取相關的 FRED 指標，獲取 FinMind 籌碼等
        #    這裡我們只做一個 yfinance 的示例

        # 計算獲取 yfinance 數據的日期範圍 (例如，獲取包含 current_period_start_dt 這一天的數據)
        # 如果 current_period_start_dt 是 00:00，我們可能需要獲取前一天的日線數據
        # 如果是12:00，可能也是前一天的日線，或者當天的部分數據（如果API支持）
        # 假設我們的週期是12小時，processed_features_hourly 代表的是過去12小時的聚合
        # 所以，如果 current_period_start_dt 是 2023-10-27T00:00:00Z，它代表 26日12:00 到 27日00:00 的數據
        # 我們需要獲取能覆蓋這個窗口的原始數據。
        # 為了簡化，我們假設 get_historical_data_csv 能拿到日線，然後 align_ohlcv_data 將其切分和聚合。

        # 示例：獲取過去幾天的日線數據用於對齊和特徵計算的歷史窗口
        # (這部分邏輯需要非常小心，確保獲取到正確的原始數據窗口)
        yf_end_date = current_period_start_dt.strftime("%Y-%m-%d")
        yf_start_date = (current_period_start_dt - timedelta(days=FEATURE_CALC_HISTORY_WINDOW_SIZE + 5)).strftime("%Y-%m-%d") # 多取幾天以防萬一

        logger.info(f"從 YFinance 獲取 {symbol} 的日線數據: {yf_start_date} 到 {yf_end_date}")
        raw_yf_csv_data = self.yf_connector.get_historical_data_csv(symbol, yf_start_date, yf_end_date, interval="1d")

        if raw_yf_csv_data:
            # 存儲原始數據 (簡化，只存儲 payload)
            self.db_manager.execute_modification(
                "INSERT INTO raw_market_data (source, symbol_or_series_id, data_payload) VALUES (?, ?, ?)",
                [f"yfinance_csv_daily_{symbol}", symbol, raw_yf_csv_data]
            )

            # 2. 數據清洗 (CSV -> list of dicts, 清洗值)
            lines = raw_yf_csv_data.strip().splitlines()
            if len(lines) > 1:
                headers = [h.strip('"') for h in lines[0].split(',')]
                cleaned_ohlcv_list = []
                for line in lines[1:]:
                    values = [v.strip('"') for v in line.split(',')]
                    if len(values) == len(headers):
                        row_dict = dict(zip(headers, values))
                        cleaned_row = clean_yfinance_csv_row(row_dict) # 應返回標準化鍵名
                        if cleaned_row and cleaned_row.get("Date"): # Date 已被標準化為 ISO UTC
                            # 將 Date (YYYY-MM-DDTHH:MM:SSZ) 轉為 datetime 對象用於後續對齊
                            # clean_yfinance_csv_row 返回的 Date 應是 timestamp_utc_str
                            # 為了 align_ohlcv_data，我們需要原始的 Date (YYYY-MM-DD) 和 OHLCV
                            # 這裡的清洗和對齊流程需要重新設計得更細緻
                            # 假設 cleaned_row 的 Date 是 YYYY-MM-DDTHH:MM:SSZ (當天0點)
                            # 而 OHLCV 是日線數據。
                            # 我們需要將這些日線數據對齊到12小時間隔。
                            # align_ohlcv_data 期望的輸入是更細粒度的數據，或者它自己能處理日線的拆分。
                            # 為了演示，我們假設這裡的 cleaned_row 已經是可以被 align_ohlcv_data 使用的結構
                            # 但實際上，從日線到12h週期需要一個轉換步驟（例如，將日OHLCV視為兩個12h週期的相同值，或基於成交量分配）
                            # 這裡簡化：直接將日線數據傳給對齊器，對齊器內部可能需要特殊處理
                            # 或者，我們假設 `align_ohlcv_data` 的輸入是已經清洗過的、帶標準時間戳的列表

                            # 修正：clean_yfinance_csv_row 應返回包含標準化時間戳的字典
                            # 我們用它來構建傳遞給 align_ohlcv_data 的列表
                            # 鍵名應與 align_ohlcv_data 的期望匹配
                            ohlcv_item = {
                                "timestamp_utc": cleaned_row.get("Date"), # 已是 ISO UTC 字符串
                                "open": cleaned_row.get("Open"),
                                "high": cleaned_row.get("High"),
                                "low": cleaned_row.get("Low"),
                                "close": cleaned_row.get("Close"),
                                "volume": cleaned_row.get("Volume"),
                                "adj_close": cleaned_row.get("adj_close")
                            }
                            if ohlcv_item["timestamp_utc"]: # 確保時間戳有效
                                cleaned_ohlcv_list.append(ohlcv_item)

                # 3. 數據對齊 (到12h週期)
                # aligned_12h_ohlcv 是字典: {period_start_iso: {open:..., high:..., ...}}
                aligned_12h_ohlcv = align_ohlcv_data(
                    cleaned_ohlcv_list,
                    timestamp_key="timestamp_utc", # 與上面 ohlcv_item 的鍵匹配
                    period_hours=SIMULATION_STEP_HOURS
                )

                # 4. 特徵計算 & 存儲
                # 我們需要遍歷 aligned_12h_ohlcv 中的每個週期，如果它落在我們的模擬時間範圍內
                # 並且是我們關心的 current_period_start_iso

                if current_period_start_iso in aligned_12h_ohlcv:
                    ohlcv_for_current_period = aligned_12h_ohlcv[current_period_start_iso]

                    # 獲取歷史數據用於指標計算
                    # 這裡的 historical_ohlcv_series 應該是按時間順序排列的、已對齊到12h週期的OHLCV數據
                    # 我們需要從數據庫或內存中構建這個序列
                    # 這部分邏輯是核心且複雜：需要維護一個滑動窗口的歷史 "processed_features_hourly"
                    # 或者在每次迭代時從數據庫查詢。
                    # 為了簡化，我們先假設能獲取到 (可能通過 _get_historical_data_for_features)

                    # 假設 ohlcv_for_current_period 就是 calculate_all_features 需要的 aligned_ohlcv_period_data
                    # 而 historical_ohlcv_series 需要單獨構建

                    # 構建用於特徵計算的歷史序列 (這一步非常關鍵)
                    # 我們需要過去 N 個週期的 OHLCV (已對齊到12h)
                    # 這裡的 cleaned_ohlcv_list 是日線，aligned_12h_ohlcv 是轉換後的12h週期數據
                    # 我們需要一個包含 current_period_start_iso 及其之前的12h週期數據的列表

                    # 簡化：假設 aligned_12h_ohlcv 的鍵是時間有序的
                    # 我們需要找到 current_period_start_iso 在其中的位置，並取其之前的N-1條
                    sorted_periods = sorted(aligned_12h_ohlcv.keys())
                    try:
                        current_idx = sorted_periods.index(current_period_start_iso)
                        hist_start_idx = max(0, current_idx - FEATURE_CALC_HISTORY_WINDOW_SIZE + 1)
                        # 構建傳遞給 calculate_all_features 的 historical_ohlcv_series
                        # 它期望列表中的每個元素是 {open, high, low, close, volume}
                        historical_series_for_calc = [aligned_12h_ohlcv[p_key] for p_key in sorted_periods[hist_start_idx : current_idx + 1]]
                    except ValueError:
                        logger.warning(f"當前週期 {current_period_start_iso} 未在對齊數據中找到，無法計算特徵。")
                        historical_series_for_calc = []


                    if len(historical_series_for_calc) >= 1: # 至少要有當前週期的數據
                        all_features_for_period = calculate_all_features(
                            ohlcv_for_current_period, # 當前週期的 OHLCV
                            historical_series_for_calc  # 包含當前和歷史的 OHLCV 序列
                        )

                        # 補充其他必需的字段
                        all_features_for_period["timestamp_period_start_utc"] = current_period_start_iso
                        all_features_for_period["symbol"] = symbol
                        # data_source_references, feature_generated_at_utc 等

                        # 將特徵存入 processed_features_hourly
                        # 構建 INSERT 語句的列名和占位符
                        # 這裡需要確保 all_features_for_period 的鍵與表列名匹配
                        # 並且處理 None 值 (SQLite 中存儲為 NULL)
                        # 為了簡化，假設鍵名已匹配，並且 execute_modification 能處理 None
                        columns = ', '.join(all_features_for_period.keys())
                        placeholders = ', '.join(['?'] * len(all_features_for_period))
                        values = list(all_features_for_period.values())
                        insert_feature_sql = f"INSERT OR REPLACE INTO processed_features_hourly ({columns}) VALUES ({placeholders});"
                        # 使用 INSERT OR REPLACE 避免主鍵衝突 (timestamp, symbol)
                        self.db_manager.execute_modification(insert_feature_sql, values)
                        logger.info(f"已為 {symbol} @ {current_period_start_iso} 計算並存儲特徵。")

                        # --- AI 決策部分 ---
                        # 5. 市場簡報生成
                        # 這裡的 processed_features_current_period 應該是 all_features_for_period
                        # qualitative_info 需要從其他地方獲取 (例如，新聞摘要)
                        qual_info_example = {"news_summary": f"關於 {symbol} 的最新消息..."} # 示例

                        briefing_json_str, llm_prompt = self.prompt_generator.generate_market_briefing_json_and_prompt(
                            current_period_start_iso,
                            symbol,
                            all_features_for_period, # 使用剛計算的所有特徵
                            qual_info_example
                        )

                        # 6. AI 決策獲取
                        # TODO: 增加一個配置項，是否真的調用 LLM，或者使用 mock/dummy 響應進行測試
                        # if self.config.get("use_real_llm", False):
                        llm_response_dict = self.llama_agent.send_prompt_to_ollama(llm_prompt)
                        # else:
                        #    llm_response_dict = {"decision_category": "觀望 (mock)", ...} # mock

                        # 7. 決策日誌記錄
                        if llm_response_dict:
                            judgment_fields = self.llama_agent.parse_llm_response_to_judgment_fields(llm_response_dict)

                            # 補充 judgment 表的其他字段
                            judgment_fields["judgment_timestamp_utc"] = current_period_start_iso # 或 T 時刻
                            judgment_fields["market_briefing_json"] = briefing_json_str
                            judgment_fields["ai_model_name"] = self.llama_agent.model_name
                            judgment_fields["feature_period_start_utc"] = current_period_start_iso
                            judgment_fields["symbol_judged"] = symbol
                            # processing_time_seconds (可以計算)
                            # log_created_at_utc (數據庫 DEFAULT)

                            # 構建 INSERT
                            # 過濾掉值为 None 的字段，或者确保 execute_modification 可以处理
                            final_judgment_data = {k: v for k, v in judgment_fields.items() if v is not None}
                            cols_judgment = ', '.join(final_judgment_data.keys())
                            place_judgment = ', '.join(['?'] * len(final_judgment_data))
                            val_judgment = list(final_judgment_data.values())

                            insert_judgment_sql = f"INSERT INTO ai_historical_judgments ({cols_judgment}) VALUES ({place_judgment});"
                            self.db_manager.execute_modification(insert_judgment_sql, val_judgment)
                            logger.info(f"已為 {symbol} @ {current_period_start_iso} 記錄 AI 決策。")
                        else:
                            logger.error(f"未能從 LLM 獲取 {symbol} @ {current_period_start_iso} 的決策。")
                    else: # historical_series_for_calc 長度不足
                        logger.warning(f"歷史數據不足，無法為 {symbol} @ {current_period_start_iso} 計算特徵。")
                else: # current_period_start_iso 不在對齊數據中
                    logger.warning(f"當前目標週期 {current_period_start_iso} 的對齊後OHLCV數據未找到。跳過特徵計算和AI決策。")
            else: # lines <= 1
                logger.warning(f"從 YFinance 獲取的 {symbol} CSV 數據為空或只有頭部。")
        else: # raw_yf_csv_data is None
            logger.error(f"未能從 YFinance 獲取 {symbol} 的原始數據。")

        logger.info(f"--- 完成處理週期: {symbol} @ {current_period_start_iso} ---")


    def run(self):
        """
        執行完整的回溯模擬。
        """
        logger.info(f"===== 開始完整歷史回溯模擬 =====")
        logger.info(f"從 {self.start_dt_utc.isoformat()} 回溯到 {self.end_dt_utc.isoformat()}，步長 {SIMULATION_STEP_HOURS} 小時。")

        current_t = self.start_dt_utc
        while current_t > self.end_dt_utc:
            # 獲取這個 T 時刻對應的12小時週期的開始時間
            # 例如，如果 T 是 2023-10-27T13:00:00Z，週期是12h，那麼它屬於 T_period_start = 2023-10-27T12:00:00Z
            # 我們處理的是以 T_period_start 為起始的那個週期的數據和決策
            # 或者，T 代表的是數據的截止時間，那麼 T_period_start 是 T - 12h
            # 根據我們之前的定義：processed_features_hourly.timestamp_period_start_utc 是特徵窗口的開始時間
            # ai_historical_judgments.judgment_timestamp_utc 是 AI 做出判斷的時間 (歷史回溯的 T 時刻)
            # 這意味著，如果 judgment_timestamp_utc 是 T，那麼它基於的 features 是在 T 之前的那個窗口生成的
            # 即 features 的 timestamp_period_start_utc 是 T 減去一個窗口期 (例如 T-12h)

            # 讓我們重新定義 T 的含義：T 是我們“站立”的時間點，我們要分析的是 T 之前的那個剛結束的窗口。
            # 所以，如果 current_t 是 2023-10-27T12:00:00Z，那麼我們分析的是
            # 從 2023-10-27T00:00:00Z 到 2023-10-27T12:00:00Z 這個窗口。
            # 特徵表的 timestamp_period_start_utc 就是 2023-10-27T00:00:00Z。
            # AI 判斷的 judgment_timestamp_utc 就是 current_t (2023-10-27T12:00:00Z)。

            # 所以，我們需要計算 current_t 所對應的 "剛結束的窗口的開始時間"
            # 這可以通過 get_target_period_start_utc(current_t - epsilon, period_hours) 得到，
            # 或者更直接地，如果 current_t 本身就是週期的邊界點：
            # period_start_for_features = current_t - timedelta(hours=SIMULATION_STEP_HOURS)
            # 但如果 current_t 不是精確的週期邊界，用 get_target_period_start_utc(current_t) 得到的是 current_t 所在週期的開始時間
            # 我們需要的是 current_t *之前* 那個完整週期的開始時間。

            # 簡化：讓 current_t 就是我們要分析的週期的 *開始時間*。
            # 這樣，processed_features_hourly.timestamp_period_start_utc = current_t
            # ai_historical_judgments.judgment_timestamp_utc 也是 current_t (表示基於 current_t 開始的窗口數據做的判斷)
            # 或者 judgment_timestamp_utc = current_t + timedelta(hours=SIMULATION_STEP_HOURS) (在窗口結束時做判斷)
            # 我們選擇前者：current_t 是窗口開始時間，AI判斷也記錄為此時間。

            # 所以，循環中的 current_t 直接作為 run_simulation_period 的 current_period_start_dt

            self.run_simulation_period(current_t, self.target_symbol)

            # 更新到上一個週期的開始時間
            current_t -= timedelta(hours=SIMULATION_STEP_HOURS)

        logger.info(f"===== 歷史回溯模擬結束 =====")
        self.db_manager.close_connection()


if __name__ == "__main__":
    # --- 主程序入口 ---
    # 實際使用時，開始/結束日期和股票代碼可以通過命令行參數傳入

    # 示例：回溯 AAPL 最近幾天 (假設今天比 2023-10-28 晚)
    # 注意：回溯是從 start_datetime (較近的時間) 向 end_datetime (較早的時間)
    sim_start_iso = "2023-10-28T00:00:00.000Z" # 回溯的起點 (最新的時間)
    sim_end_iso = "2023-10-26T00:00:00.000Z"   # 回溯的終點 (最早的時間)
    # 這意味著會處理 2023-10-27T12:00:00Z, 2023-10-27T00:00:00Z, 2023-10-26T12:00:00Z, 2023-10-26T00:00:00Z 這幾個週期的開始點

    target_sym = DEFAULT_TARGET_SYMBOL

    # 清理之前的數據庫文件以便測試 (可選)
    if os.path.exists(DB_FILEPATH):
        logger.warning(f"發現已存在的數據庫文件 {DB_FILEPATH}，將其刪除以便進行乾淨的模擬測試。")
        os.remove(DB_FILEPATH)

    # 確保數據庫已初始化 (如果 initialize_database.sh 未運行過)
    # 這裡可以選擇調用 init_db.py 中的函數，或者依賴外部腳本
    from AI_Assisted_Historical_Backtesting.src.database.db_manager import initialize_database_from_schema
    schema_path = os.path.join(project_root_dir, "config", "schema.sql")
    if not initialize_database_from_schema(DB_FILEPATH, schema_path):
        logger.error("主模擬開始前數據庫初始化失敗，程序退出。")
        sys.exit(1)
    logger.info("主模擬開始前數據庫已確認/初始化。")


    simulation = HistoricalSimulation(
        start_datetime_utc_iso=sim_start_iso,
        end_datetime_utc_iso=sim_end_iso,
        target_symbol=target_sym
    )

    try:
        simulation.run()
    except Exception as e:
        logger.error(f"主模擬運行期間發生頂層錯誤: {e}", exc_info=True)
    finally:
        logger.info("主模擬 __main__ 結束。")
