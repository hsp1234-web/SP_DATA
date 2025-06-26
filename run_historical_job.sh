#!/bin/bash
# run_historical_job.sh - 原子化執行契約，用於單個歷史日期的作業
# 此腳本是 run_prototype.sh 的副本，但設計為接收一個日期參數

# === 階段一：環境清理 (可選) ===
echo "Phase 1: Cleaning up previous artifacts (Historical Job)..."
# rm -rf src/ data/ market_briefing_log.txt api_test_logs/ # 通常不由單個歷史作業清理共享日誌

# === 階段二：專案建構 (核心步驟) ===
echo "Phase 2: Building project structure and files (Historical Job)..."

# 2.1 創建目錄結構
echo "Creating directory structure..."
mkdir -p src/configs
mkdir -p src/connectors
mkdir -p src/database
mkdir -p src/engine
mkdir -p src/scripts
# Removed mkdir -p src/ai_agent as ai_agent.py is a file, not a directory.

# Explicitly remove potentially problematic __init__.py if it exists from previous runs or misconfigurations
rm -f src/ai_agent/__init__.py

echo "Creating configuration file (src/configs/project_config.yaml)..."
cat <<EOF > src/configs/project_config.yaml
# Configuration for the Financial Data Processing Prototype

database:
  path: "data/financial_data.duckdb" # Relative to project root for the atomic script
  # schema_file: "src/configs/database_schemas.json" # Optional, if we define schemas externally

data_fetch_range:
  start_date: "2020-01-01"
  # end_date is now primarily controlled by the --execution_date argument to main.py
  # end_date: "YYYY-MM-DD" # Optional: If empty and no --execution_date, main.py will use current date

api_endpoints:
  fred:
    api_key_env: "FRED_API_KEY"
    base_url: "https://api.stlouisfed.org/fred/"

target_metrics:
  fred_series_ids:
    - "DGS10"    # 10-Year Treasury Constant Maturity Rate
    - "DGS2"     # 2-Year Treasury Constant Maturity Rate
    - "SOFR"     # Secured Overnight Financing Rate
    - "VIXCLS"   # CBOE Volatility Index
    - "WRESBAL"  # Reserves Balance with Federal Reserve Banks
  yfinance_tickers:
    - "^MOVE"    # ICE BofA MOVE Index (Treasury Volatility)

nyfed_primary_dealer_urls:
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2024.xlsx"
    file_pattern: "prideal2024.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx"
    file_pattern: "prideal2023.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx"
    file_pattern: "prideal2022.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"

nyfed_format_recipes:
  "PD_STATS_FORMAT_2013_ONWARDS":
    header_row: 3
    date_column: "As of Date"
    columns_to_sum:
      - "U.S. Treasury coupons"
      - "U.S. Treasury bills"
      - "U.S. Treasury floating rate notes (FRNs)"
      - "Federal agency debt securities (MBS)"
      - "Federal agency debt securities (non-MBS)"
      - "Commercial paper"
      - "Certificates of deposit"
      - "Bankers acceptances"
      - "Equities"
      - "Corporate bonds (investment grade)"
      - "Corporate bonds (below investment grade)"
      - "Municipal securities"
      - "Other assets"
    data_unit_multiplier: 1000000

indicator_engine_params:
  rolling_window_days: 252
  stress_index_weights:
    sofr_deviation: 0.20
    spread_10y2y: 0.20
    primary_dealer_position: 0.15
    move_index: 0.25
    vix_index: 0.15
    pos_res_ratio: 0.05
  stress_threshold_moderate: 40
  stress_threshold_high: 60
  stress_threshold_extreme: 80
  min_periods_ratio_for_rolling: 0.5 # Added for IndicatorEngine percentile calculation robustness

requests_config:
  max_retries: 3
  base_backoff_seconds: 1
  timeout: 30
  download_timeout: 120

ai_agent_mock_params: # Configuration for MockAIAgent
  simulate_network_latency_max_sec: 0.1 # Max seconds for simulated latency
  simulate_failure_rate: 0.02 # 2% chance of simulated failure
EOF

echo "Creating requirements.txt file..."
cat <<EOF > requirements.txt
pandas
pyyaml
duckdb
SQLAlchemy
fredapi
yfinance
requests
openpyxl
tqdm
argparse
EOF

echo "Creating src/connectors/base.py..."
cat <<EOF > src/connectors/base.py
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class BaseConnector(ABC):
    """
    所有數據連接器的抽象基類。
    定義了標準接口，確保所有 Connector 的行為一致。
    """

    def __init__(self, config: Dict[str, Any], source_api_name: str = "Unknown"):
        self.config = config
        self.source_api_name = source_api_name
        # Logger can be passed by the child class or DataManager for better context
        # For now, child classes will initialize their own loggers or use a global one.

    @abstractmethod
    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        從 API 或數據源獲取原始數據並進行初步轉換成 DataFrame (通常是長表格式)。

        Args:
            start_date (Optional[str]): 數據獲取的開始日期 (YYYY-MM-DD)。
            end_date (Optional[str]): 數據獲取的結束日期 (YYYY-MM-DD)。此日期為數據上限，不應包含此日期之後的數據。
            **kwargs: 特定 connector 需要的參數 (例如 series_ids, tickers)。

        Returns:
            一個包含 (DataFrame, error_message) 的元組。
            成功時，DataFrame 包含獲取和初步標準化的數據，error_message 為 None。
            失敗時，DataFrame 為 None 或空的 DataFrame (帶有預期欄位)，error_message 包含錯誤信息。
            DataFrame 應包含 'source_api' 和 'data_snapshot_timestamp' (UTC) 欄位。
            對於時間序列數據，應有 'metric_date' 或 'price_date'。
            對於宏觀/因子數據，應有 'metric_name'。
            對於股價數據，應有 'security_id'。
        """
        pass

    def get_source_name(self) -> str:
        """返回數據源的名稱。"""
        return self.source_api_name
EOF

echo "Creating src/connectors/nyfed_connector.py..."
cat <<EOF > src/connectors/nyfed_connector.py
import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import logging
from datetime import datetime, timezone
import sys
import time
import random

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class NYFedConnector(BaseConnector):
    """從紐約聯儲網站獲取並解析一級交易商持倉數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="NYFED")

        self.urls_config = self.config.get('nyfed_primary_dealer_urls', [])
        self.recipes = self.config.get('nyfed_format_recipes', {})
        self.requests_config = self.config.get('requests_config', {})

        if not self.urls_config:
            self.logger.warning("No URLs configured for NYFed (nyfed_primary_dealer_urls).")
        if not self.recipes:
            self.logger.warning("No recipes for NYFed formats (nyfed_format_recipes).")

    def _download_excel_with_retries(self, url:str) -> Optional[BytesIO]:
        retries = self.requests_config.get('max_retries', 3)
        base_backoff = self.requests_config.get('base_backoff_seconds', 1)
        timeout_sec = self.requests_config.get('download_timeout', self.requests_config.get('timeout', 60))

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        for attempt in range(retries):
            try:
                self.logger.debug(f"Attempt {attempt + 1}/{retries} to download NYFed Excel from {url}")
                response = requests.get(url, timeout=timeout_sec, headers=headers)
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '')
                self.logger.info(f"Successfully downloaded from NYFed URL {url} (status {response.status_code}). Content-Type: {content_type}. Size: {len(response.content)} bytes.")
                self.logger.debug(f"NYFed downloaded content head (first 100 bytes): {response.content[:100]}")

                if not any(ct in content_type.lower() for ct in ['excel', 'spreadsheetml', 'officedocument']):
                    self.logger.error(f"Downloaded content from {url} does not appear to be an Excel file based on Content-Type: '{content_type}'. Skipping.")
                    return None

                return BytesIO(response.content)
            except requests.exceptions.HTTPError as e:
                self.logger.warning(f"HTTP error on attempt {attempt + 1}/{retries} for NYFed URL '{url}': {e.response.status_code if e.response else 'N/A'} - {e.response.text[:100] if e.response else 'N/A'}")
                if e.response is not None and 400 <= e.response.status_code < 500 and e.response.status_code != 429:
                    self.logger.error(f"Client error {e.response.status_code} for NYFed URL '{url}', not retrying this file.")
                    return None
                if attempt == retries - 1:
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with HTTPError: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"RequestException on attempt {attempt + 1}/{retries} for NYFed URL '{url}': {e}")
                if attempt == retries - 1:
                    self.logger.error(f"Final attempt failed for NYFed URL '{url}' with RequestException: {e}")
                    return None

            wait_time = (base_backoff * (2 ** attempt)) + random.uniform(0, 0.5 * base_backoff)
            self.logger.info(f"Retrying download from NYFed URL '{url}' in {wait_time:.2f} seconds...")
            time.sleep(wait_time)

        self.logger.error(f"All download attempts failed for NYFed URL '{url}'.")
        return None

    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        # start_date is not typically used by NYFed as we download full files, but kept for interface consistency.
        all_positions_data_list = []
        self.logger.info(f"Fetching NYFed data from {len(self.urls_config)} configured URLs. Effective end_date for filtering: {end_date}")

        if not self.urls_config:
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "No NYFed URLs configured."

        for file_info in self.urls_config:
            url = file_info.get('url')
            format_type = file_info.get('format_type')
            file_log_name = file_info.get('file_pattern', url)

            if not url or not format_type:
                self.logger.warning(f"Skipping invalid NYFed URL config: {file_info}.")
                continue

            self.logger.info(f"Processing NYFed file: {file_log_name} from URL: {url} (format: {format_type})")
            excel_file_content = self._download_excel_with_retries(url)
            if not excel_file_content:
                continue

            recipe = self.recipes.get(format_type)
            if not recipe:
                self.logger.warning(f"No recipe for '{format_type}' (file: {file_log_name}). Skipping.")
                continue

            try:
                header_idx = recipe.get('header_row', 1) - 1
                df = pd.read_excel(excel_file_content, header=header_idx, engine='openpyxl')

                date_col_name = recipe.get('date_column')
                if not date_col_name or date_col_name not in df.columns:
                    self.logger.error(f"Date column '{date_col_name}' not in {file_log_name}. Cols: {df.columns.tolist()}")
                    continue

                df.rename(columns={date_col_name: 'metric_date'}, inplace=True)
                df['metric_date'] = pd.to_datetime(df['metric_date'], errors='coerce')
                df.dropna(subset=['metric_date'], inplace=True)

                if df.empty:
                    self.logger.warning(f"No valid dates in {file_log_name} after processing."); continue

                sum_cols_cfg = recipe.get('columns_to_sum', [])
                actual_cols_to_sum = [col for col in sum_cols_cfg if col in df.columns]

                missing_cols = set(sum_cols_cfg) - set(actual_cols_to_sum)
                if missing_cols:
                    self.logger.warning(f"Missing cols in {file_log_name} for recipe '{format_type}': {missing_cols}. Summing available: {actual_cols_to_sum}")

                if not actual_cols_to_sum:
                    self.logger.warning(f"No columns to sum were found in {file_log_name} based on recipe."); continue

                for col in actual_cols_to_sum:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                sum_series = df[actual_cols_to_sum].sum(axis=1, skipna=True)
                proc_df = df[['metric_date']].copy()
                proc_df['metric_value'] = sum_series

                multiplier = recipe.get('data_unit_multiplier', 1)
                proc_df['metric_value'] *= multiplier
                proc_df.dropna(subset=['metric_value'], inplace=True)

                if proc_df.empty:
                    self.logger.warning(f"No valid summed data for {file_log_name} after processing."); continue

                proc_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
                proc_df['source_api'] = self.source_api_name
                proc_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                all_positions_data_list.append(proc_df[['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']])
                self.logger.debug(f"Processed NYFed file: {file_log_name}, {len(proc_df)} rows.")
            except Exception as e:
                self.logger.error(f"Error processing Excel {file_log_name}: {e}", exc_info=True)
                continue

        if not all_positions_data_list:
            self.logger.warning("No data from any NYFed files.")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "No data from NYFed."

        combo_df = pd.concat(all_positions_data_list, ignore_index=True)
        if combo_df.empty:
            self.logger.warning("NYFed data empty after concat.");
            return combo_df, "NYFed data empty post-concat."

        combo_df.sort_values('metric_date', inplace=True)
        combo_df.drop_duplicates(subset=['metric_date'], keep='last', inplace=True)

        if combo_df.empty:
            self.logger.warning("NYFed data empty after dedup.");
            return combo_df, "NYFed data empty post-dedup."

        combo_df.set_index('metric_date', inplace=True)
        if not combo_df.index.is_monotonic_increasing:
             self.logger.warning("NYFed index not monotonic after initial sort/dedup, re-sorting.");
             combo_df.sort_index(inplace=True)

        if combo_df.empty:
            self.logger.warning("NYFed data empty after index operations.");
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "NYFed data empty post-index ops."

        min_d, max_d = combo_df.index.min(), combo_df.index.max()
        if pd.isna(min_d) or pd.isna(max_d):
            self.logger.error(f"Invalid date range for NYFed. Min: {min_d}, Max: {max_d}")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "Invalid date range for NYFed data."

        daily_idx = pd.date_range(start=min_d, end=max_d, freq='D')
        daily_df = combo_df.reindex(daily_idx).ffill()
        daily_df.index.name = 'metric_date'
        daily_df.reset_index(inplace=True)

        final_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
        for col in final_cols:
            if col not in daily_df.columns:
                daily_df[col] = pd.NA

        if not daily_df.empty:
            daily_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
            daily_df['source_api'] = self.source_api_name
            daily_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

        self.logger.info(f"Processed {len(daily_df)} total NYFed records after daily ffill.")
        # --- 行動項目 2.1: 實現「時間點數據獲取」 ---
        if end_date and not daily_df.empty:
            try:
                effective_end_date = pd.to_datetime(end_date).normalize()
                self.logger.info(f"NYFedConnector: Applying end_date filter: <= {effective_end_date.strftime('%Y-%m-%d')}")
                daily_df['metric_date'] = pd.to_datetime(daily_df['metric_date']).dt.normalize()
                daily_df_filtered = daily_df[daily_df['metric_date'] <= effective_end_date].copy()

                self.logger.info(f"NYFedConnector: Filtered data from {len(daily_df)} to {len(daily_df_filtered)} rows using end_date: {end_date}.")
                if daily_df_filtered.empty and not daily_df.empty:
                    self.logger.warning(f"NYFedConnector: All data filtered out by end_date {end_date}. Original date range: {daily_df['metric_date'].min()} to {daily_df['metric_date'].max()}")
                daily_df = daily_df_filtered
            except Exception as e_filter:
                self.logger.error(f"NYFedConnector: Error applying end_date filter ({end_date}): {e_filter}", exc_info=True)

        return daily_df[final_cols], None

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_ny = logging.getLogger("NYFedConnectorTestRun_Atomic")
    if not test_logger_ny.handlers:
        ch_ny = logging.StreamHandler(sys.stdout)
        ch_ny.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_ny.addHandler(ch_ny)
        test_logger_ny.propagate = False

    test_cfg = {
        'requests_config': {'max_retries': 2, 'base_backoff_seconds': 0.5, 'timeout': 15, 'download_timeout': 45},
        'nyfed_primary_dealer_urls': [
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx", "file_pattern": "prideal2023.xlsx", "format_type": "TEST_PD_FORMAT"},
        ],
        'nyfed_format_recipes': {
            "TEST_PD_FORMAT": {
                "header_row": 3,
                "date_column": "As of Date",
                "columns_to_sum": ["U.S. Treasury coupons", "U.S. Treasury bills"],
                "data_unit_multiplier": 1000
            }
        }
    }

    test_logger_ny.info("--- Starting NYFedConnector Test (with end_date filtering) ---")
    ny_conn = NYFedConnector(config=test_cfg, logger_instance=test_logger_ny)

    # Test Case 1: Fetch with an end_date that should return some data
    test_end_date_1 = "2023-06-30"
    test_logger_ny.info(f"Test Case 1: Fetching with end_date = {test_end_date_1}")
    ny_df_res_1, ny_err_1 = ny_conn.fetch_data(end_date=test_end_date_1)

    if ny_err_1:
        test_logger_ny.error(f"NYFed Test Case 1 failed with error: {ny_err_1}")
    elif ny_df_res_1 is not None:
        test_logger_ny.info(f"NYFed Test Case 1 successful. Fetched data shape: {ny_df_res_1.shape}")
        if not ny_df_res_1.empty:
            test_logger_ny.info(f"NYFed Data (end_date={test_end_date_1}) head:\n{ny_df_res_1.head().to_string()}")
            test_logger_ny.info(f"NYFed Data (end_date={test_end_date_1}) tail:\n{ny_df_res_1.tail().to_string()}")
            assert ny_df_res_1['metric_date'].max() <= pd.to_datetime(test_end_date_1).normalize(), "Data after end_date found!"
        else:
            test_logger_ny.info(f"NYFed Test Case 1: Returned DataFrame is empty for end_date {test_end_date_1}.")

    # Test Case 2: Fetch with an end_date that should return no data (e.g., before any data in the file)
    test_end_date_2 = "2020-01-01" # Assuming prideal2023.xlsx starts much later
    test_logger_ny.info(f"\nTest Case 2: Fetching with end_date = {test_end_date_2} (expected empty)")
    ny_df_res_2, ny_err_2 = ny_conn.fetch_data(end_date=test_end_date_2)
    if ny_err_2:
         test_logger_ny.error(f"NYFed Test Case 2 failed with error: {ny_err_2}")
    elif ny_df_res_2 is not None:
        test_logger_ny.info(f"NYFed Test Case 2 successful. Fetched data shape: {ny_df_res_2.shape}")
        assert ny_df_res_2.empty, f"Test Case 2 FAILED: Expected empty DataFrame for end_date {test_end_date_2}, but got {len(ny_df_res_2)} rows."
        if ny_df_res_2.empty:
             test_logger_ny.info(f"NYFed Test Case 2: Correctly returned empty DataFrame for end_date {test_end_date_2}.")

    test_logger_ny.info("--- NYFedConnector Test Finished ---")
EOF

echo "Creating src/connectors/fred_connector.py..."
cat <<EOF > src/connectors/fred_connector.py
from fredapi import Fred
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import os
import sys # For test block

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class FredConnector(BaseConnector):
    """
    使用 fredapi 函式庫從 FRED (Federal Reserve Economic Data) 獲取經濟數據。
    """

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="FRED")

        self.api_key_env_var = self.config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY')
        self.api_key = os.getenv(self.api_key_env_var)

        if not self.api_key:
            self.logger.critical(f"FRED API key not found in environment variable '{self.api_key_env_var}'. FREDConnector will not be able to fetch data.")
            self.fred_client = None
        else:
            try:
                self.fred_client = Fred(api_key=self.api_key)
                self.logger.info("FredConnector initialized successfully with API key.")
            except Exception as e:
                self.logger.critical(f"Failed to initialize Fred client with API key: {e}", exc_info=True)
                self.fred_client = None

    def fetch_data(self, series_ids: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        # The 'end_date' parameter is now part of the method signature as per Phase II requirements.
        # For FRED, the fredapi library handles date filtering directly via 'observation_end'.
        if self.fred_client is None:
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "FRED client not initialized due to missing API key or initialization error."

        if not series_ids:
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "No series_ids provided to FredConnector."

        self.logger.info(f"Fetching FRED data for series_ids: {series_ids} from {start_date} to {end_date}.")

        all_series_data_list = []
        error_messages = []

        for series_id in series_ids:
            try:
                self.logger.debug(f"Fetching data for FRED series_id: {series_id}")
                # Pass end_date to observation_end
                series_data = self.fred_client.get_series(series_id, observation_start=start_date, observation_end=end_date)

                if series_data.empty:
                    self.logger.warning(f"No data returned for FRED series_id: {series_id} for the given date range (start={start_date}, end={end_date}).")
                    continue

                df_series = series_data.reset_index()
                df_series.columns = ['metric_date', 'metric_value']

                df_series['metric_date'] = pd.to_datetime(df_series['metric_date']).dt.date
                df_series['metric_name'] = f"FRED/{series_id}"
                df_series['source_api'] = self.source_api_name
                df_series['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                df_series['metric_value'] = pd.to_numeric(df_series['metric_value'], errors='coerce')
                df_series.dropna(subset=['metric_value'], inplace=True)

                all_series_data_list.append(df_series[['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']])
                self.logger.debug(f"Successfully fetched and processed FRED series_id: {series_id}, {len(df_series)} rows.")

            except Exception as e:
                error_msg = f"Error fetching/processing FRED series_id {series_id}: {e}"
                self.logger.error(error_msg, exc_info=True)
                error_messages.append(error_msg)

        if not all_series_data_list:
            final_error_message = "No data successfully fetched for any FRED series_ids."
            if error_messages:
                final_error_message += " Errors encountered: " + "; ".join(error_messages)
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), final_error_message

        final_df = pd.concat(all_series_data_list, ignore_index=True)

        if final_df.empty:
            schema_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=schema_cols), "Combined FRED data is empty after processing all series."

        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from FRED for series_ids: {series_ids}.")

        full_error_summary = "; ".join(error_messages) if error_messages else None
        return final_df, full_error_summary


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_fred = logging.getLogger("FredConnectorTestRun_Atomic_Main")
    if not test_logger_fred.handlers:
        ch_fred = logging.StreamHandler(sys.stdout)
        ch_fred.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_fred.addHandler(ch_fred)
        test_logger_fred.propagate = False

    sample_fred_config = {
        "api_endpoints": { "fred": { "api_key_env": "FRED_API_KEY_TEST_HIST" } }
    }

    api_key_to_use = os.getenv("FRED_API_KEY") # Try to use the main key from environment for testing
    if not api_key_to_use:
        test_logger_fred.error("FRED_API_KEY environment variable not set. Cannot run FredConnector test.")
        sys.exit(1)
    os.environ["FRED_API_KEY_TEST_HIST"] = api_key_to_use # Set it to the specific var the test config uses

    test_logger_fred.info("--- Starting FredConnector Test (with end_date) ---")
    fred_conn_test = FredConnector(config=sample_fred_config, logger_instance=test_logger_fred)

    if fred_conn_test.fred_client is not None:
        test_series_list = ["DGS10", "UNRATE"]
        test_start = "2022-01-01"
        test_end_date_filter = "2022-03-15" # Specific end date for filtering

        test_logger_fred.info(f"Testing fetch_data for series: {test_series_list} from {test_start} to {test_end_date_filter}")
        fred_df, fred_err = fred_conn_test.fetch_data(series_ids=test_series_list, start_date=test_start, end_date=test_end_date_filter)

        if fred_err:
            test_logger_fred.warning(f"FredConnector test fetch_data completed with error(s): {fred_err}")

        if fred_df is not None and not fred_df.empty:
            test_logger_fred.info(f"FredConnector test fetch_data returned data. Shape: {fred_df.shape}")
            test_logger_fred.info(f"Result head:\n{fred_df.head().to_string()}")
            test_logger_fred.info(f"Result tail:\n{fred_df.tail().to_string()}")

            max_date_in_df = fred_df['metric_date'].max()
            test_logger_fred.info(f"Max date in returned FRED data: {max_date_in_df.strftime('%Y-%m-%d')}")
            assert max_date_in_df <= pd.to_datetime(test_end_date_filter).date(), f"Data found after specified end_date {test_end_date_filter}! Max date was {max_date_in_df}"
            test_logger_fred.info(f"FRED data correctly filtered by end_date {test_end_date_filter}.")
        elif fred_df is not None and fred_df.empty:
             test_logger_fred.warning(f"FredConnector test fetch_data returned an empty DataFrame for period up to {test_end_date_filter}.")
        else:
             test_logger_fred.error(f"FredConnector test fetch_data returned None for data. Error was: {fred_err}")
    else:
        test_logger_fred.error("FredConnector client (self.fred_client) was not initialized in test. API key issue likely.")
    test_logger_fred.info("--- FredConnector Test Finished ---")
EOF

echo "Creating src/database/database_manager.py..."
# No changes needed for DatabaseManager.py for Action Item 2.1, as it already supports date filtering.
# Re-writing it identically to ensure it's part of the script if it was missed or for completeness.
cat <<EOF > src/database/database_manager.py
import duckdb
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path
import os

class DatabaseManager:
    """
    管理與 DuckDB 資料庫的連接和操作。
    """
    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, project_root_dir: Optional[str] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        self.db_config = config.get('database', {})
        db_path_str = self.db_config.get('path', 'data/default_financial_data.duckdb')

        if project_root_dir:
            self.db_file = Path(project_root_dir) / db_path_str
        else:
            self.db_file = Path(db_path_str)
            self.logger.warning(f"project_root_dir not provided to DatabaseManager. Database path resolved to: {self.db_file.resolve()}")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger.info(f"DatabaseManager initialized. DB file target: {self.db_file.resolve()}")

    def connect(self):
        """建立與 DuckDB 資料庫的連接。"""
        if self.conn is not None:
            try:
                self.conn.execute("SELECT 1")
                self.logger.info("Database connection already active and valid.")
                return
            except Exception as e:
                self.logger.warning(f"Existing connection object found but it's not usable ({e}). Will try to reconnect.")
                self.conn = None

        try:
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(database=str(self.db_file), read_only=False)
            self.logger.info(f"Successfully connected to DuckDB database: {self.db_file.resolve()}")
            self._create_tables_if_not_exist()
        except Exception as e:
            self.logger.critical(f"Failed to connect to DuckDB database at {self.db_file.resolve()}: {e}", exc_info=True)
            self.conn = None
            raise

    def disconnect(self):
        """關閉資料庫連接。"""
        if self.conn is not None:
            try:
                self.conn.close()
                self.logger.info(f"Disconnected from DuckDB database: {self.db_file.resolve()}")
            except Exception as e:
                self.logger.error(f"Error while closing DuckDB connection: {e}", exc_info=True)
        else:
            self.logger.info("Database connection already None or not established.")
        self.conn = None


    def _create_tables_if_not_exist(self):
        """如果表不存在，則創建它們。"""
        if self.conn is None:
            self.logger.error("Cannot create tables: Database connection is None.")
            return

        try:
            self.logger.info("Ensuring tables exist (will not drop if already present)...")
            # self.conn.execute("DROP TABLE IF EXISTS fact_macro_economic_data;") # Keep for re-creation
            # self.conn.execute("DROP TABLE IF EXISTS fact_stock_price;")
            # self.conn.execute("DROP TABLE IF EXISTS log_ai_decision;")
            # self.logger.info("Old tables (if any) dropped for fresh schema.")


            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_macro_economic_data (
                    metric_date DATE,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (metric_date, metric_name, source_api)
                );
            """)
            self.logger.info("Table 'fact_macro_economic_data' checked/created.")

            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_stock_price (
                    price_date DATE,
                    security_id VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    adj_close_price DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE,
                    stock_splits DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (price_date, security_id, source_api)
                );
            """)
            self.logger.info("Table 'fact_stock_price' checked/created.")

            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS log_ai_decision (
                    simulation_timestamp TIMESTAMP,
                    market_brief_json TEXT,
                    ai_response_text TEXT,
                    strategy_summary TEXT,
                    key_factors TEXT,
                    PRIMARY KEY (simulation_timestamp)
                );
            """)
            self.logger.info("Table 'log_ai_decision' checked/created.")

        except Exception as e:
            self.logger.error(f"Error creating tables: {e}", exc_info=True)

    def bulk_insert_or_replace(self, table_name: str, df: pd.DataFrame, unique_cols: List[str]):
        if self.conn is None:
            self.logger.error(f"Cannot insert into {table_name}: Database connection is None.")
            return False
        if df.empty:
            self.logger.info(f"DataFrame for table {table_name} is empty. Nothing to insert.")
            return True

        self.logger.debug(f"Attempting to bulk insert/replace into {table_name}, {len(df)} rows. Unique cols: {unique_cols}")

        try:
            temp_table_name = f"temp_{table_name}_{os.urandom(4).hex()}"
            self.conn.register(temp_table_name, df)

            if not unique_cols:
                raise ValueError("unique_cols must be provided for upsert operation.")

            conflict_target = ", ".join(unique_cols)
            update_cols = [col for col in df.columns if col not in unique_cols]

            if not update_cols:
                 sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO NOTHING;"
                 self.logger.debug(f"Executing SQL (INSERT OR IGNORE style as no update_cols): {sql}")
            else:
                set_statements = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
                sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO UPDATE SET {set_statements};"
                self.logger.debug(f"Executing SQL (UPSERT style): {sql}")

            self.conn.execute(sql)
            self.conn.unregister(temp_table_name)
            self.logger.info(f"Successfully inserted/replaced {len(df)} rows into {table_name}.")
            return True
        except Exception as e:
            self.logger.error(f"Error during bulk insert/replace into {table_name}: {e}", exc_info=True)
            if 'temp_table_name' in locals() and self.conn: # Check if conn still exists
                try:
                    # Check if temp table exists before trying to unregister
                    # This might require a query like "SHOW TABLES LIKE 'temp_table_name'" or similar depending on DB
                    # For DuckDB, conn.table(temp_table_name) would raise if not exists.
                    # A safer check might be to query information_schema.tables.
                    # However, for simplicity, we'll rely on the try-except for unregister.
                    self.conn.unregister(temp_table_name)
                except Exception as e_unreg:
                    self.logger.error(f"Failed to unregister temp table {temp_table_name} on error: {e_unreg}")
            return False

    def fetch_all_for_engine(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_column: str = 'metric_date') -> Optional[pd.DataFrame]:
        if self.conn is None:
            self.logger.error(f"Cannot fetch from {table_name}: Database connection is None.")
            return None

        self.logger.debug(f"Fetching all data for engine from {table_name}, date_col: {date_column}, start: {start_date}, end: {end_date}")

        query = f"SELECT * FROM {table_name}"
        params = []
        conditions = []

        if start_date:
            conditions.append(f"{date_column} >= ?")
            params.append(start_date)
        if end_date:
            conditions.append(f"{date_column} <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {date_column}" # Ensure data is sorted for engine

        try:
            result_df = self.conn.execute(query, params).fetchdf()
            self.logger.info(f"Successfully fetched {len(result_df)} rows from {table_name} for range {start_date}-{end_date}.")
            return result_df
        except Exception as e:
            self.logger.error(f"Error fetching data from {table_name} for range {start_date}-{end_date}: {e}", exc_info=True)
            return None

    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[pd.DataFrame]:
        if self.conn is None:
            self.logger.error("Cannot execute query: Database connection is None.")
            return None
        try:
            self.logger.debug(f"Executing custom query: {query} with params: {params}")
            return self.conn.execute(query, params).fetchdf()
        except Exception as e:
            self.logger.error(f"Error executing custom query '{query}': {e}", exc_info=True)
            return None

    def close(self):
        self.disconnect()

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_db = logging.getLogger("DatabaseManagerTestRun_Atomic_Historical")
    if not test_logger_db.handlers:
        ch_db = logging.StreamHandler(sys.stdout)
        ch_db.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_db.addHandler(ch_db)
        test_logger_db.propagate = False

    test_db_config = {
        "database": {
            "path": "data/test_hist_job_db.duckdb" # Use a different DB for this test
        }
    }
    test_project_root = str(Path(".").resolve())
    old_db_file = Path(test_project_root) / test_db_config["database"]["path"]
    if old_db_file.exists(): old_db_file.unlink()

    test_logger_db.info("--- Starting DatabaseManager Test (Historical Job Context) ---")
    db_man = DatabaseManager(config=test_db_config, logger_instance=test_logger_db, project_root_dir=test_project_root)

    try:
        db_man.connect()
        assert db_man.conn is not None, "Connection failed"
        test_logger_db.info("DB Connection successful for historical job test.")

        # Test AI log table creation
        tables_df = db_man.execute_query("SHOW TABLES;")
        assert 'log_ai_decision' in tables_df['name'].tolist(), "log_ai_decision table not created"
        test_logger_db.info("'log_ai_decision' table confirmed.")

        # Test fetch_all_for_engine with date filtering
        # (Assuming fact_macro_economic_data exists and might have some data from a previous run or needs sample data)
        # For a clean test, one might insert sample data first.
        # Here, we'll just test the query construction.
        test_start_fetch = "2022-01-01"
        test_end_fetch = "2022-01-15"
        test_logger_db.info(f"Testing fetch_all_for_engine for 'fact_macro_economic_data' from {test_start_fetch} to {test_end_fetch}")

        # Create dummy data for testing fetch_all_for_engine
        sample_macro_data = []
        for i in range(20):
            sample_macro_data.append({
                'metric_date': (pd.to_datetime("2022-01-01") + pd.Timedelta(days=i)).date(),
                'metric_name': 'DGS10_Test', 'metric_value': 2.0 + i*0.01,
                'source_api': 'TestFRED', 'data_snapshot_timestamp': datetime.now(timezone.utc)
            })
        sample_macro_df = pd.DataFrame(sample_macro_data)
        db_man.bulk_insert_or_replace('fact_macro_economic_data', sample_macro_df, unique_cols=['metric_date', 'metric_name', 'source_api'])

        fetched_df = db_man.fetch_all_for_engine('fact_macro_economic_data',
                                                 start_date=test_start_fetch,
                                                 end_date=test_end_fetch,
                                                 date_column='metric_date')
        if fetched_df is not None:
            test_logger_db.info(f"Fetched {len(fetched_df)} rows. Head:\n{fetched_df.head().to_string()}")
            if not fetched_df.empty:
                assert fetched_df['metric_date'].min() >= pd.to_datetime(test_start_fetch).date()
                assert fetched_df['metric_date'].max() <= pd.to_datetime(test_end_fetch).date()
                test_logger_db.info("Date filtering in fetch_all_for_engine seems correct.")
            else:
                test_logger_db.info("fetch_all_for_engine returned empty (might be expected if no data in range).")

        test_logger_db.info("DatabaseManager tests (Historical Job Context) passed.")

    except Exception as e_test_hist:
        test_logger_db.error(f"DatabaseManager test (Historical Job Context) failed: {e_test_hist}", exc_info=True)
    finally:
        db_man.disconnect()
        test_logger_db.info("--- DatabaseManager Test (Historical Job Context) Finished ---")
        # if old_db_file.exists(): old_db_file.unlink(missing_ok=True) # Clean up
EOF

echo "Creating src/connectors/yfinance_connector.py..."
cat <<EOF > src/connectors/yfinance_connector.py
import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import sys
import requests

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class YFinanceConnector(BaseConnector):
    """使用 yfinance 獲取股價和指數數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, session: Optional[requests.Session] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="yfinance")

    def fetch_data(self, tickers: List[str], start_date: str, end_date: Optional[str] = None,
                   interval: str = "1d", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"Fetching yfinance data for tickers: {tickers} from {start_date} to {end_date} with interval {interval}.")

        if not tickers:
            self.logger.warning("No tickers provided to YFinanceConnector fetch_data.")
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), "No tickers provided."

        all_ticker_data_list = []

        self.logger.info(f"YFinanceConnector: Requesting data for tickers {tickers} up to end_date: {end_date}")

        for ticker_symbol in tickers:
            self.logger.debug(f"Fetching yfinance data for: {ticker_symbol}")
            try:
                ticker_obj = yf.Ticker(ticker_symbol)

                hist_df = ticker_obj.history(
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=False,
                    actions=True,
                )

                if hist_df.empty:
                    self.logger.warning(f"yfinance returned no data for ticker: {ticker_symbol} (start: {start_date}, end: {end_date}, interval: {interval}).")
                    continue

                hist_df.reset_index(inplace=True)

                date_col_name = None
                if 'Datetime' in hist_df.columns: date_col_name = 'Datetime'
                elif 'Date' in hist_df.columns: date_col_name = 'Date'

                if not date_col_name:
                    self.logger.error(f"Date column ('Date' or 'Datetime') not found in yfinance data for {ticker_symbol}. Columns: {hist_df.columns.tolist()}")
                    continue

                rename_map = {
                    date_col_name: 'price_date', 'Open': 'open_price', 'High': 'high_price',
                    'Low': 'low_price', 'Close': 'close_price', 'Adj Close': 'adj_close_price',
                    'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
                }
                current_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
                df_renamed = hist_df.rename(columns=current_rename_map)

                df_renamed['price_date'] = pd.to_datetime(df_renamed['price_date'])
                if df_renamed['price_date'].dt.tz is not None:
                    df_renamed['price_date'] = df_renamed['price_date'].dt.tz_localize(None)
                df_renamed['price_date'] = df_renamed['price_date'].dt.normalize().dt.date

                df_renamed['security_id'] = ticker_symbol
                df_renamed['source_api'] = self.source_api_name
                df_renamed['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                                   'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                                   'source_api', 'data_snapshot_timestamp']

                for fc_col in final_cols_spec:
                    if fc_col not in df_renamed.columns:
                        default_val = 0.0 if fc_col in ['dividends', 'stock_splits'] else pd.NA
                        df_renamed[fc_col] = default_val

                all_ticker_data_list.append(df_renamed[final_cols_spec])
                self.logger.debug(f"Processed yfinance data for {ticker_symbol}, {len(df_renamed)} rows.")

            except Exception as e:
                self.logger.error(f"Error fetching/processing yfinance for {ticker_symbol}: {e}", exc_info=True)

        if not all_ticker_data_list:
            self.logger.warning(f"No data successfully fetched for any yfinance tickers: {tickers}")
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), f"No data from yfinance for any of the tickers: {tickers}."

        final_df = pd.concat(all_ticker_data_list, ignore_index=True)

        if final_df.empty:
             self.logger.warning("Final combined yfinance data is empty (all tickers failed or returned no data).")
             return final_df, "Final combined yfinance data is empty."

        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'dividends', 'stock_splits']
        for col_to_num in numeric_cols:
            if col_to_num in final_df.columns:
                final_df[col_to_num] = pd.to_numeric(final_df[col_to_num], errors='coerce')
        if 'volume' in final_df.columns:
            final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce').astype('Int64')

        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from yfinance for tickers: {tickers}.")
        return final_df, None


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_yf = logging.getLogger("YFinanceConnectorTestRun_Atomic_Historical")
    if not test_logger_yf.handlers:
        ch_yf = logging.StreamHandler(sys.stdout)
        ch_yf.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_yf.addHandler(ch_yf)
        test_logger_yf.propagate = False

    sample_config_yf = {}
    yf_connector = YFinanceConnector(config=sample_config_yf, logger_instance=test_logger_yf)

    test_logger_yf.info("\n--- Testing YFinanceConnector for ^MOVE with end_date filter ---")
    test_start_yf = "2023-01-01"
    test_end_yf_filter = "2023-01-15" # Filter up to this date
    move_df, move_err = yf_connector.fetch_data(tickers=["^MOVE"], start_date=test_start_yf, end_date=test_end_yf_filter)
    if move_err:
        test_logger_yf.error(f"^MOVE Test Error: {move_err}")
    elif move_df is not None:
        test_logger_yf.info(f"^MOVE Test OK. Shape: {move_df.shape}")
        if not move_df.empty:
            test_logger_yf.info(f"^MOVE Head (filtered to {test_end_yf_filter}):\n{move_df.head().to_string()}")
            test_logger_yf.info(f"^MOVE Tail (filtered to {test_end_yf_filter}):\n{move_df.tail().to_string()}")
            max_date_move = pd.to_datetime(move_df['price_date']).max().date() # Ensure it's date for comparison
            assert max_date_move <= pd.to_datetime(test_end_yf_filter).date(), f"YFinance data for ^MOVE found after specified end_date {test_end_yf_filter}! Max date was {max_date_move}"
            test_logger_yf.info(f"YFinance data for ^MOVE correctly filtered by end_date {test_end_yf_filter}.")
        else:
            test_logger_yf.info(f"YFinance for ^MOVE returned empty for period up to {test_end_yf_filter} (this might be expected or indicate test data range issues).")

    test_logger_yf.info("--- YFinanceConnector Test (Historical) Finished ---")
EOF

echo "Creating src/engine/indicator_engine.py..."
# No changes needed for IndicatorEngine.py for Action Item 2.1, as data filtering happens before it.
# Re-writing it identically.
cat <<EOF > src/engine/indicator_engine.py
import pandas as pd
from typing import Dict, Any, Optional
import numpy as np
import logging
import sys

logger = logging.getLogger(f"project_logger.{__name__}")
if not logger.handlers and not logging.getLogger().hasHandlers():
    logger.addHandler(logging.NullHandler())
    logger.debug(f"Logger for {__name__} (IndicatorEngine module) configured with NullHandler for atomic script.")

class IndicatorEngine:
    """
    封裝計算衍生指標，特別是「債券壓力指標」的邏輯。
    """
    def __init__(self, data_frames: Dict[str, pd.DataFrame], params: Optional[Dict[str, Any]] = None, logger_instance: Optional[logging.Logger] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                 self.logger.addHandler(logging.NullHandler())
                 self.logger.debug(f"Instance logger for {self.__class__.__name__} using NullHandler for atomic script.")

        self.raw_macro_df = data_frames.get('macro', pd.DataFrame())
        self.raw_move_df = data_frames.get('move', pd.DataFrame())
        self.params = params if params is not None else {}
        self.df_prepared: Optional[pd.DataFrame] = None

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'macro' data is missing or empty.")
        if self.raw_move_df.empty:
            self.logger.warning("IndicatorEngine initialized: 'move' data (for ^MOVE) is missing or empty.")

    def _prepare_data(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Preparing data for stress index calculation...")

        if self.raw_macro_df.empty:
            self.logger.warning("IndicatorEngine: Macro data (raw_macro_df) is empty. Proceeding without macro indicators for pivot.")
            if self.raw_move_df.empty:
                self.logger.error("IndicatorEngine: Both macro and MOVE data are empty. Cannot prepare data.")
                return None
            macro_wide_df = pd.DataFrame()
        else:
            try:
                current_macro_df = self.raw_macro_df.copy()
                if 'metric_date' not in current_macro_df.columns:
                    self.logger.error("IndicatorEngine: 'metric_date' column missing in macro data.")
                    return None
                current_macro_df['metric_date'] = pd.to_datetime(current_macro_df['metric_date'], errors='coerce')
                current_macro_df.dropna(subset=['metric_date'], inplace=True)

                if current_macro_df.empty:
                    self.logger.error("IndicatorEngine: Macro data has no valid 'metric_date' entries after coercion.")
                    return None

                if not all(col in current_macro_df.columns for col in ['metric_name', 'metric_value']):
                    self.logger.error("IndicatorEngine: 'metric_name' or 'metric_value' missing for pivot.")
                    return None
                macro_wide_df = current_macro_df.pivot_table(
                    index='metric_date', columns='metric_name', values='metric_value'
                )
                macro_wide_df.index.name = 'date'
                self.logger.debug(f"IndicatorEngine: Pivoted macro data shape: {macro_wide_df.shape}")
            except Exception as e:
                self.logger.error(f"IndicatorEngine: Failed to pivot macro_df: {e}", exc_info=True)
                return None

        move_wide_df = pd.DataFrame()
        if not self.raw_move_df.empty:
            if all(col in self.raw_move_df.columns for col in ['price_date', 'close_price', 'security_id']):
                move_df_filtered = self.raw_move_df[self.raw_move_df['security_id'] == '^MOVE'].copy()
                if not move_df_filtered.empty:
                    move_df_filtered['price_date'] = pd.to_datetime(move_df_filtered['price_date'], errors='coerce')
                    move_df_filtered.dropna(subset=['price_date'], inplace=True)
                    if not move_df_filtered.empty:
                        move_wide_df = move_df_filtered.set_index('price_date')[['close_price']].rename(columns={'close_price': '^MOVE'})
                        move_wide_df.index.name = 'date'
                        self.logger.debug(f"IndicatorEngine: Prepared ^MOVE index data. Non-NaN count: {move_wide_df['^MOVE'].notna().sum()}")
                    else:
                        self.logger.warning("IndicatorEngine: ^MOVE data had no valid 'price_date' entries after coercion.")
                else:
                    self.logger.warning("IndicatorEngine: ^MOVE security_id not found in provided yfinance data (raw_move_df).")
            else:
                self.logger.warning("IndicatorEngine: ^MOVE DataFrame (raw_move_df) missing required columns (price_date, close_price, security_id).")
        else:
            self.logger.warning("IndicatorEngine: ^MOVE data (raw_move_df) is missing or empty. ^MOVE index will be NaN if not in macro_wide_df.")

        if macro_wide_df.empty and move_wide_df.empty:
            self.logger.error("IndicatorEngine: Both pivoted macro and MOVE data are empty. Cannot combine.")
            return None
        elif macro_wide_df.empty:
            combined_df = move_wide_df
            self.logger.warning("IndicatorEngine: Pivoted macro data was empty, using only MOVE data for combined_df.")
        elif move_wide_df.empty:
            combined_df = macro_wide_df
            if '^MOVE' not in combined_df.columns:
                combined_df['^MOVE'] = np.nan
            self.logger.warning("IndicatorEngine: MOVE data was empty, using only macro data for combined_df.")
        else:
            combined_df = pd.merge(macro_wide_df, move_wide_df, left_index=True, right_index=True, how='outer')
            self.logger.debug(f"IndicatorEngine: Combined macro and MOVE data. Shape: {combined_df.shape}")

        if '^MOVE' not in combined_df.columns:
                combined_df['^MOVE'] = np.nan

        combined_df.sort_index(inplace=True)
        combined_df = combined_df.ffill(limit=7).bfill(limit=7)
        combined_df.dropna(how='all', inplace=True)

        if combined_df.empty:
            self.logger.error("IndicatorEngine: Prepared data is empty after merge and fill operations.")
            return None

        self.logger.info(f"IndicatorEngine: Data preparation complete. Final shape: {combined_df.shape}")
        return combined_df

    def calculate_dealer_stress_index(self) -> Optional[pd.DataFrame]:
        self.logger.info("IndicatorEngine: Calculating Dealer Stress Index...")
        current_prepared_data = self._prepare_data()

        if current_prepared_data is None or current_prepared_data.empty:
            self.logger.error("IndicatorEngine: Prepared data is None or empty. Cannot calculate stress index.")
            self.df_prepared = current_prepared_data
            return None

        self.df_prepared = current_prepared_data.copy()
        df = self.df_prepared.copy()

        window = self.params.get('rolling_window_days', 252)
        weights_config = self.params.get('stress_index_weights', {})
        min_periods_ratio = self.params.get('min_periods_ratio_for_rolling', 0.5)

        component_map = {
            'sofr_deviation': 'FRED/SOFR_Dev',
            'spread_10y2y': 'spread_10y2y',
            'primary_dealer_position': 'NYFED/PRIMARY_DEALER_NET_POSITION',
            'move_index': '^MOVE',
            'vix_index': 'FRED/VIXCLS',
            'pos_res_ratio': 'pos_res_ratio'
        }
        self.logger.debug(f"IndicatorEngine: Stress Index Params: Window={window}, Weights={weights_config}, MinPeriodsRatio={min_periods_ratio}")

        if 'FRED/DGS10' in df.columns and 'FRED/DGS2' in df.columns:
            df['spread_10y2y'] = df['FRED/DGS10'] - df['FRED/DGS2']
        else:
            df['spread_10y2y'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/DGS10 or FRED/DGS2 missing. 'spread_10y2y' will be NaN.")

        if 'FRED/SOFR' in df.columns and df['FRED/SOFR'].notna().sum() >= 20:
             df['FRED/SOFR_MA20'] = df['FRED/SOFR'].rolling(window=20, min_periods=15).mean()
             df['FRED/SOFR_Dev'] = df['FRED/SOFR'] - df['FRED/SOFR_MA20']
        else:
            df['FRED/SOFR_Dev'] = np.nan
            self.logger.warning("IndicatorEngine: FRED/SOFR has insufficient data for 20-day MA or is missing. 'FRED/SOFR_Dev' will be NaN.")

        if 'NYFED/PRIMARY_DEALER_NET_POSITION' in df.columns and 'FRED/WRESBAL' in df.columns:
            res_safe = df['FRED/WRESBAL'].replace(0, np.nan)
            df['pos_res_ratio'] = df['NYFED/PRIMARY_DEALER_NET_POSITION'] / res_safe
            df['pos_res_ratio'].replace([np.inf, -np.inf], np.nan, inplace=True)
        else:
            df['pos_res_ratio'] = np.nan
            self.logger.warning("IndicatorEngine: NYFED/PRIMARY_DEALER_NET_POSITION or FRED/WRESBAL missing. 'pos_res_ratio' will be NaN.")

        self.df_prepared = df.copy()

        percentiles_df = pd.DataFrame(index=df.index)
        active_component_weights = {}

        min_rolling_periods = max(2, int(window * min_periods_ratio))

        for key, col_name in component_map.items():
            if weights_config.get(key, 0) == 0:
                self.logger.debug(f"IndicatorEngine: Skipping rank for {key} ({col_name}) due to zero weight.")
                percentiles_df[f"{key}_pct_rank"] = np.nan
                continue

            if col_name in df.columns and df[col_name].notna().any():
                series_to_rank = df[col_name]
                if series_to_rank.notna().sum() >= min_rolling_periods:
                    rolling_percentile = series_to_rank.rolling(window=window, min_periods=min_rolling_periods).apply(
                        lambda x_window: pd.Series(x_window).rank(pct=True).iloc[-1] if pd.Series(x_window).notna().any() else np.nan,
                        raw=False
                    )
                    percentiles_df[f"{key}_pct_rank"] = (1.0 - rolling_percentile) if key == 'spread_10y2y' else rolling_percentile
                    active_component_weights[key] = weights_config[key]
                    self.logger.debug(f"IndicatorEngine: Calculated rolling percentile for {key} ({col_name}).")
                else:
                    self.logger.warning(f"IndicatorEngine: Insufficient data for {col_name} (key: {key}) for rolling rank. Window: {window}, MinPeriods: {min_rolling_periods}, Available: {series_to_rank.notna().sum()}. Skipping rank.")
                    percentiles_df[f"{key}_pct_rank"] = np.nan
            else:
                self.logger.warning(f"IndicatorEngine: Component {key} ({col_name}) not found in prepared data or is all NaN. Skipping rank.")
                percentiles_df[f"{key}_pct_rank"] = np.nan

        if not active_component_weights:
            self.logger.error("IndicatorEngine: No active components with valid data and non-zero weights for stress index calculation.")
            return None

        total_active_weight = sum(active_component_weights.values())
        if total_active_weight == 0:
            self.logger.error("IndicatorEngine: Sum of active component weights is zero. Cannot normalize.")
            return None

        normalized_weights = {k: w / total_active_weight for k, w in active_component_weights.items()}
        self.logger.info(f"IndicatorEngine: Normalized Stress Index Weights (for active components): {normalized_weights}")

        final_stress_index_series = pd.Series(0.0, index=df.index)
        sum_of_effective_weights = pd.Series(0.0, index=df.index)

        for component_key, weight in normalized_weights.items():
            percentile_col_name = f"{component_key}_pct_rank"
            if percentile_col_name in percentiles_df.columns and percentiles_df[percentile_col_name].notna().any():
                component_contribution = percentiles_df[percentile_col_name].fillna(0.5) * weight
                final_stress_index_series = final_stress_index_series.add(component_contribution, fill_value=0)
                sum_of_effective_weights = sum_of_effective_weights.add(percentiles_df[percentile_col_name].notna() * weight, fill_value=0)
            else:
                self.logger.warning(f"IndicatorEngine: Percentile rank column {percentile_col_name} for component {component_key} is missing or all NaN. This component will not contribute to the index.")

        adjusted_stress_index = final_stress_index_series.divide(sum_of_effective_weights.replace(0, np.nan))
        final_stress_index_scaled = (adjusted_stress_index * 100).clip(0, 100)

        result_df = pd.DataFrame({'DealerStressIndex': final_stress_index_scaled}, index=df.index)
        result_df = result_df.join(percentiles_df)

        final_result_df = result_df.dropna(subset=['DealerStressIndex'])

        if final_result_df.empty:
            self.logger.warning("IndicatorEngine: Dealer Stress Index is all NaN after calculation and processing.")
            return None

        self.logger.info(f"IndicatorEngine: Dealer Stress Index calculated successfully. Final shape: {final_result_df.shape}")
        return final_result_df

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_eng_main = logging.getLogger("IndicatorEngineTestRun_Atomic_Historical")
    if not test_logger_eng_main.handlers:
        ch_eng_main = logging.StreamHandler(sys.stdout)
        ch_eng_main.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_eng_main.addHandler(ch_eng_main)
        test_logger_eng_main.propagate = False

    dates_sample = pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05',
                                   '2023-01-06', '2023-01-07', '2023-01-08', '2023-01-09', '2023-01-10'])

    macro_data_test = {
        'metric_date': list(dates_sample) * 6,
        'metric_name': (['FRED/DGS10'] * len(dates_sample) + ['FRED/DGS2'] * len(dates_sample) +
                        ['FRED/SOFR'] * len(dates_sample) + ['FRED/VIXCLS'] * len(dates_sample) +
                        ['NYFED/PRIMARY_DEALER_NET_POSITION'] * len(dates_sample) + ['FRED/WRESBAL'] * len(dates_sample)),
        'metric_value': (
            list(np.linspace(3.0, 3.5, len(dates_sample))) +
            list(np.linspace(2.0, 2.5, len(dates_sample))) +
            list(np.linspace(1.0, 1.2, len(dates_sample))) +
            list(np.linspace(15, 25, len(dates_sample))) +
            list(np.linspace(1000e6, 1200e6, len(dates_sample))) +
            list(np.linspace(2.5e12, 2.7e12, len(dates_sample)))
        )
    }
    sample_macro_df = pd.DataFrame(macro_data_test)

    move_data_test = {
        'price_date': dates_sample,
        'security_id': ['^MOVE'] * len(dates_sample),
        'close_price': np.linspace(80, 95, len(dates_sample))
    }
    sample_move_df = pd.DataFrame(move_data_test)

    engine_params_config = {
        'rolling_window_days': 5,
        'min_periods_ratio_for_rolling': 0.6,
        'stress_index_weights': {
            'sofr_deviation': 0.20, 'spread_10y2y': 0.20,
            'primary_dealer_position': 0.15, 'move_index': 0.25,
            'vix_index': 0.15, 'pos_res_ratio': 0.05
        }
    }

    test_logger_eng_main.info("\n--- Test IndicatorEngine Full Calculation (Historical Context) ---")
    engine_instance = IndicatorEngine(
        data_frames={'macro': sample_macro_df, 'move': sample_move_df},
        params=engine_params_config,
        logger_instance=test_logger_eng_main
    )

    stress_index_output = engine_instance.calculate_dealer_stress_index()

    if stress_index_output is not None and not stress_index_output.empty:
        test_logger_eng_main.info(f"Stress Index Output Shape: {stress_index_output.shape}")
        test_logger_eng_main.info(f"Stress Index Output Head:\n{stress_index_output.head().to_string()}")
        assert 'DealerStressIndex' in stress_index_output.columns, "Test Failed: DealerStressIndex column missing"
    elif stress_index_output is not None and stress_index_output.empty:
         test_logger_eng_main.warning("Stress Index calculation resulted in an empty DataFrame.")
    else:
        test_logger_eng_main.error("Stress Index calculation failed and returned None.")

    test_logger_eng_main.info("--- IndicatorEngine Test (Historical Context) Finished ---")
EOF

echo "Creating src/scripts/initialize_global_log.py..."
# No changes needed for initialize_global_log.py for Action Item 2.1. Re-writing identically.
cat <<EOF > src/scripts/initialize_global_log.py
import logging
from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import sys
from typing import Optional, Any

LOG_DIR_NAME = "api_test_logs"
LOG_FILE_PATH: Optional[str] = None
_global_logger_initialized_flag = False

_bootstrap_logger = logging.getLogger("BootstrapLogger")
if not _bootstrap_logger.handlers and not logging.getLogger().hasHandlers():
    _ch_bootstrap = logging.StreamHandler(sys.stdout)
    _ch_bootstrap.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(name)s - %(message)s (bootstrap)'))
    _bootstrap_logger.addHandler(_ch_bootstrap)
    _bootstrap_logger.setLevel(logging.INFO)
    _bootstrap_logger.propagate = False

def get_taipei_time() -> datetime:
    """Returns the current time in Taipei timezone (UTC+8)."""
    return datetime.now(timezone.utc) + timedelta(hours=8)

class TaipeiTimeFormatter(logging.Formatter):
    """Custom formatter to add Taipei time to log records."""
    def format(self, record: logging.LogRecord) -> str:
        record.taipei_time_str = get_taipei_time().strftime('%Y-%m-%d %H:%M:%S %Z%z')
        return super().format(record)

def initialize_log_file(
    log_dir_override: Optional[str] = None,
    force_reinit: bool = False,
    project_root_path: Optional[Path] = None
) -> Optional[str]:
    global LOG_FILE_PATH, _global_logger_initialized_flag

    current_project_root: Path
    if project_root_path:
        current_project_root = project_root_path
    else:
        try:
            current_project_root = Path(__file__).resolve().parent.parent
        except NameError:
            current_project_root = Path(".").resolve()
            _bootstrap_logger.warning(f"__file__ not defined, using CWD '{current_project_root}' as project root for log path determination.")

    current_log_dir_path: Path
    if log_dir_override:
        current_log_dir_path = Path(log_dir_override)
    else:
        current_log_dir_path = current_project_root / LOG_DIR_NAME

    if _global_logger_initialized_flag and not force_reinit and LOG_FILE_PATH:
        if Path(LOG_FILE_PATH).parent == current_log_dir_path.resolve():
            _bootstrap_logger.debug(f"Global logger already initialized. Log file: {LOG_FILE_PATH}")
            return LOG_FILE_PATH
        else:
            _bootstrap_logger.warning(
                f"Log directory has changed or re-initialization is forced. "
                f"Old log dir: {Path(LOG_FILE_PATH).parent}, New log dir: {current_log_dir_path.resolve()}. Forcing re-init."
            )
            force_reinit = True

    try:
        current_log_dir_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to create log directory '{current_log_dir_path}': {e}", exc_info=True)
        return None

    utc_now = datetime.now(timezone.utc)
    timestamp_filename_str = utc_now.strftime("%Y-%m-%dT%H%M%SZ")
    log_filename = f"{timestamp_filename_str}_application_log.txt"
    current_log_file_full_path = current_log_dir_path / log_filename

    try:
        file_log_format_str = '%(asctime)s (Taipei: %(taipei_time_str)s) [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s'
        file_formatter = TaipeiTimeFormatter(file_log_format_str)
        file_handler = logging.FileHandler(current_log_file_full_path, mode='w', encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)

        console_log_format_str = '[%(levelname)s] %(name)s: %(message)s'
        console_formatter = logging.Formatter(console_log_format_str)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        console_handler.setLevel(logging.INFO)

        root_logger = logging.getLogger()

        if force_reinit and root_logger.hasHandlers():
            _bootstrap_logger.info("Forcing re-initialization of root logger handlers.")
            for handler_to_remove in root_logger.handlers[:]:
                root_logger.removeHandler(handler_to_remove)
                handler_to_remove.close()

        if not root_logger.handlers or force_reinit:
            root_logger.addHandler(file_handler)
            root_logger.addHandler(console_handler)
            root_logger.setLevel(logging.DEBUG)

            _global_logger_initialized_flag = True
            LOG_FILE_PATH = str(current_log_file_full_path)
            logging.getLogger("GlobalLogSetup").info(f"Global logger initialized. Log file: {LOG_FILE_PATH}")
        else:
            _bootstrap_logger.info("Root logger already has handlers and not forcing re-init. Current setup maintained.")
            if LOG_FILE_PATH is None:
                 LOG_FILE_PATH = str(current_log_file_full_path)
                 _bootstrap_logger.warning(f"LOG_FILE_PATH was None but logger seemed initialized. Set to: {LOG_FILE_PATH}")

    except Exception as e:
        _bootstrap_logger.error(f"Failed to configure logging to file '{current_log_file_full_path}': {e}", exc_info=True)
        LOG_FILE_PATH = None
        _global_logger_initialized_flag = False
        return None

    return LOG_FILE_PATH

def log_message(
    message: str,
    level: str = "INFO",
    logger_name: Optional[str] = None,
    exc_info: bool = False,
    **kwargs: Any
):
    effective_logger: logging.Logger
    if not _global_logger_initialized_flag or LOG_FILE_PATH is None:
        effective_logger = _bootstrap_logger
        if not hasattr(log_message, "_bootstrap_warning_issued_for_general_use"):
            effective_logger.warning(
                f"Global logger not fully initialized (Log file path: {LOG_FILE_PATH}). "
                f"Logging message ('{message[:50]}...') with bootstrap logger as fallback."
            )
            setattr(log_message, "_bootstrap_warning_issued_for_general_use", True)
    else:
        effective_logger = logging.getLogger(logger_name if logger_name else "project_logger.general")

    level_upper = level.upper()
    log_level_int = logging.getLevelName(level_upper)
    log_method = getattr(effective_logger, level_upper.lower(), effective_logger.info)
    should_pass_exc_info = exc_info and (isinstance(log_level_int, int) and log_level_int >= logging.ERROR)

    try:
        log_method(message, exc_info=should_pass_exc_info, extra=kwargs if kwargs else None)
    except Exception as e:
        _bootstrap_logger.error(f"Failed to log message with '{effective_logger.name}'. Original message: '{message}'. Error: {e}", exc_info=True)

if __name__ == "__main__":
    main_script_project_root_path = Path(__file__).resolve().parent.parent
    log_file_path_main = initialize_log_file(force_reinit=True, project_root_path=main_script_project_root_path)

    if log_file_path_main:
        log_message("Info message from __main__ of initialize_global_log (Historical).", "INFO", logger_name="TestInitializeGlobalLogHist")
        log_message(f"Global log file for this direct run is confirmed at: {LOG_FILE_PATH}", "CRITICAL", logger_name="TestInitializeGlobalLogHist.CriticalSub")
        print(f"Script execution finished. Log file should be at: {LOG_FILE_PATH}")
    else:
        print("Failed to initialize the log file in __main__ of initialize_global_log (Historical). Check console for bootstrap logger errors.")

EOF

echo "Creating src/main.py..."
cat <<EOF > src/main.py
import yaml
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import logging
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional

if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s (main-pre-init)',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
pre_init_logger = logging.getLogger("MainPreInit")

try:
    PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
except NameError:
    PROJECT_ROOT = str(Path(".").resolve())
    pre_init_logger.warning(f"__file__ not defined in main.py, PROJECT_ROOT set to CWD: {PROJECT_ROOT}")

DETAILED_LOG_FILENAME = os.path.join(PROJECT_ROOT, "market_briefing_log.txt") # This will be per-run if main is called multiple times by historical sim

SOURCE_ROOT = str(Path(__file__).resolve().parent)
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)
    pre_init_logger.info(f"Inserted SOURCE_ROOT ({SOURCE_ROOT}) into sys.path for relative imports.")

pre_init_logger.info(f"main.py: __file__ is {Path(__file__).resolve() if '__file__' in locals() else 'not_defined'}")
pre_init_logger.info(f"main.py: PROJECT_ROOT (parent of src): {PROJECT_ROOT}")
pre_init_logger.info(f"main.py: SOURCE_ROOT (src directory): {SOURCE_ROOT}")
pre_init_logger.info(f"main.py: sys.path for module import: {sys.path}")

global_log = None
init_global_log_function = None
global_log_file_path_imported = None
get_taipei_time_func_imported = None

try:
    from connectors.base import BaseConnector
    from connectors.fred_connector import FredConnector
    from connectors.nyfed_connector import NYFedConnector
    from connectors.yfinance_connector import YFinanceConnector
    from database.database_manager import DatabaseManager
    from engine.indicator_engine import IndicatorEngine
    from ai_agent import MockAIAgent

    from scripts.initialize_global_log import log_message, get_taipei_time, LOG_FILE_PATH as GLOBAL_LOG_FILE_PATH_FROM_MODULE, initialize_log_file
    import argparse

    global_log = log_message
    init_global_log_function = initialize_log_file
    global_log_file_path_imported = GLOBAL_LOG_FILE_PATH_FROM_MODULE
    get_taipei_time_func_imported = get_taipei_time

    if init_global_log_function is not None:
        try:
            log_dir_for_global_logger = Path(PROJECT_ROOT) / "api_test_logs"
            # For historical runs, maybe append execution_date to log filename if passed, or use a different sub-folder.
            # For now, it uses the standard timestamped name.
            actual_log_file = init_global_log_function(
                log_dir_override=str(log_dir_for_global_logger),
                force_reinit=True, # Force reinit for each historical job run to get a new log file.
                project_root_path=Path(PROJECT_ROOT)
            )
            if actual_log_file:
                global_log(f"main.py: Global application logger explicitly initialized. Log file: {actual_log_file}", "INFO", logger_name="MainApp.Setup")
            else:
                global_log("main.py: Global application logger initialization returned no path.", "ERROR", logger_name="MainApp.Setup")
        except Exception as e_log_init_main:
            pre_init_logger.error(f"main.py: Failed to explicitly initialize global application logger: {e_log_init_main}", exc_info=True)
            if global_log is None:
                 global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback) {msg}")
            global_log("main.py: Using pre_init_logger or fallback due to global_log explicit init failure.", "WARNING", logger_name="MainApp.Setup")
    else:
        pre_init_logger.error("main.py: initialize_global_log_file function was not imported.")
        if global_log is None:
            global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback_no_init) {msg}")

except ImportError as e_imp:
    pre_init_logger.error(f"Failed to import custom modules: {e_imp}. Current sys.path: {sys.path}", exc_info=True)
    if global_log is None: print(f"CRITICAL IMPORT ERROR (main.py, global_log unavailable): {e_imp}.")
    else: global_log(f"CRITICAL: Failed to import custom modules in main.py: {e_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1)
except Exception as e_general_imp:
    pre_init_logger.error(f"General error during import phase: {e_general_imp}", exc_info=True)
    if global_log is None: print(f"CRITICAL GENERAL IMPORT ERROR (main.py, global_log unavailable): {e_general_imp}.")
    else: global_log(f"CRITICAL: General error during import phase in main.py: {e_general_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1)

def load_config(config_path_relative_to_project_root="src/configs/project_config.yaml") -> Dict[str, Any]:
    full_config_path = Path(PROJECT_ROOT) / config_path_relative_to_project_root
    global_log(f"Loading project configuration from: {full_config_path}", "INFO", logger_name="MainApp.ConfigLoader")
    try:
        with open(full_config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        global_log(f"Project configuration loaded successfully from {full_config_path}.", "INFO", logger_name="MainApp.ConfigLoader")
        if not isinstance(config_data, dict):
            global_log(f"Config file {full_config_path} did not load as a dictionary.", "ERROR", logger_name="MainApp.ConfigLoader")
            raise ValueError(f"Configuration file {full_config_path} is not a valid YAML dictionary.")
        return config_data
    except FileNotFoundError:
        global_log(f"Config file not found: {full_config_path}. Exiting.", "CRITICAL", logger_name="MainApp.ConfigLoader")
        raise
    except Exception as e_conf:
        global_log(f"Error loading or parsing config from {full_config_path}: {e_conf}", "CRITICAL", logger_name="MainApp.ConfigLoader", exc_info=True)
        raise

def main():
    detailed_run_log_handler = None
    # For historical runs, the DETAILED_LOG_FILENAME might need to be unique per execution_date
    # This is a simple implementation; more robust would involve passing date to logger setup or using subdirs.
    # For now, it will overwrite if multiple main.py runs happen in quick succession without date in filename.
    # However, run_historical_simulation.sh should call this with different dates, so logs will be distinct IF
    # the DETAILED_LOG_FILENAME is made unique per run (e.g., by appending args.execution_date if present).
    # Let's modify DETAILED_LOG_FILENAME based on execution_date if provided.

    # Parse args again here just for main() scope, though already parsed globally for early setup.
    # This is slightly redundant but ensures main() has direct access to its specific invocation args.
    parser_main = argparse.ArgumentParser(description="Main execution parser")
    parser_main.add_argument("--execution_date", type=str, default=None)
    args_main, _ = parser_main.parse_known_args() # Parse known args to avoid conflict if other args are passed by shell

    current_detailed_log_filename = DETAILED_LOG_FILENAME
    if args_main.execution_date:
        try: # Validate date format before using in filename
            datetime.strptime(args_main.execution_date, '%Y-%m-%d')
            current_detailed_log_filename = os.path.join(PROJECT_ROOT, f"market_briefing_log_{args_main.execution_date}.txt")
        except ValueError:
            global_log(f"Invalid execution_date '{args_main.execution_date}' for detailed log filename. Using default.", "WARNING", logger_name="MainApp.Setup")
            # Default DETAILED_LOG_FILENAME will be used.

    try:
        detailed_run_log_handler = logging.FileHandler(current_detailed_log_filename, mode='w', encoding='utf-8')
        detailed_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
        detailed_run_log_handler.setFormatter(detailed_formatter)
        detailed_run_log_handler.setLevel(logging.DEBUG)
        root_logger_for_detailed = logging.getLogger()
        root_logger_for_detailed.addHandler(detailed_run_log_handler)
        global_log(f"Detailed execution transcript for this run ALSO saved to: {current_detailed_log_filename}", "INFO", logger_name="MainApp.Setup")
    except Exception as e_detail_log:
        err_msg = f"Failed to set up detailed run log at {current_detailed_log_filename}: {e_detail_log}"
        if global_log: global_log(err_msg, "ERROR", logger_name="MainApp.Setup", exc_info=True)
        else: pre_init_logger.error(err_msg, exc_info=True)

    global_log(f"--- 開始執行端到端金融數據處理原型 (Execution Date: {args_main.execution_date if args_main.execution_date else 'Default'}) ---", "INFO", logger_name="MainApp.main_flow")

    config: Dict[str, Any] = {}
    try:
        config = load_config(config_path_relative_to_project_root="src/configs/project_config.yaml")
        start_date_cfg = config.get('data_fetch_range', {}).get('start_date', "2020-01-01")

        end_date_to_use: str
        if args_main.execution_date: # Use args_main here as it's specific to this main() call
            try:
                datetime.strptime(args_main.execution_date, '%Y-%m-%d')
                end_date_to_use = args_main.execution_date
                global_log(f"Using execution_date from command line: {end_date_to_use}", "INFO", logger_name="MainApp.Setup")
            except ValueError: # Should have been caught by global arg parsing, but double check
                global_log(f"Invalid execution_date format in main(): '{args_main.execution_date}'. Exiting.", "CRITICAL", logger_name="MainApp.Setup")
                sys.exit(1)
        else:
            end_date_cfg = config.get('data_fetch_range', {}).get('end_date')
            if end_date_cfg:
                end_date_to_use = end_date_cfg
                global_log(f"Using end_date from config file: {end_date_to_use}", "INFO", logger_name="MainApp.Setup")
            else:
                try:
                    end_date_to_use = get_taipei_time_func_imported().strftime('%Y-%m-%d') if get_taipei_time_func_imported else datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    global_log(f"Using current date as end_date: {end_date_to_use}", "INFO", logger_name="MainApp.Setup")
                except Exception as e_time_local:
                    end_date_to_use = datetime.now(timezone.utc).strftime('%Y-%m-%d')
                    global_log(f"Using UTC for 'today's date' as get_taipei_time function failed or was unavailable: {e_time_local}", "WARNING", logger_name="MainApp.Setup")

        global_log(f"Data fetch range: Start='{start_date_cfg}', End (effective simulation date)='{end_date_to_use}'.", "INFO", logger_name="MainApp.main_flow")

        fred_api_key_env_name = config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY')
        user_provided_fred_key = "78ea51fb13b546d89f1a683cb4ba26f5"
        os.environ[fred_api_key_env_name] = user_provided_fred_key
        global_log(f"Temporarily set environment variable '{fred_api_key_env_name}' for FRED API access.", "DEBUG", logger_name="MainApp.main_flow")

        db_logger = logging.getLogger("project_logger.DatabaseManager")
        fred_logger = logging.getLogger("project_logger.FredConnector")
        nyfed_logger = logging.getLogger("project_logger.NYFedConnector")
        yf_logger = logging.getLogger("project_logger.YFinanceConnector")
        engine_logger = logging.getLogger("project_logger.IndicatorEngine")

        db_manager = DatabaseManager(config, logger_instance=db_logger, project_root_dir=PROJECT_ROOT)
        db_manager.connect()

        data_fetch_status = {'fred': False, 'nyfed': False, 'yfinance_move': False}
        macro_unique_cols = ['metric_date', 'metric_name', 'source_api']
        stock_unique_cols = ['price_date', 'security_id', 'source_api']

        global_log(f"\n--- 階段 1: 數據獲取 (截止日期: {end_date_to_use}) ---", "INFO", logger_name="MainApp.main_flow")

        fred_conn = FredConnector(config, logger_instance=fred_logger)
        fred_series_ids = config.get('target_metrics', {}).get('fred_series_ids', [])
        # Pass end_date_to_use to FredConnector
        fred_data_df, fred_error_msg = fred_conn.fetch_data(series_ids=fred_series_ids, start_date=start_date_cfg, end_date=end_date_to_use)
        if fred_error_msg and (fred_data_df is None or fred_data_df.empty):
            global_log(f"FRED Data Fetching Error: {fred_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False
        elif fred_data_df is not None and not fred_data_df.empty:
            global_log(f"Fetched {len(fred_data_df)} FRED records.", "INFO", logger_name="MainApp.main_flow")
            if fred_error_msg:
                 global_log(f"FRED Data Fetching completed with some errors: {fred_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', fred_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['fred'] = True
        else:
            global_log("FRED Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False

        nyfed_conn = NYFedConnector(config, logger_instance=nyfed_logger)
        # Pass end_date_to_use to NYFedConnector
        nyfed_data_df, nyfed_error_msg = nyfed_conn.fetch_data(start_date=start_date_cfg, end_date=end_date_to_use)
        if nyfed_error_msg and (nyfed_data_df is None or nyfed_data_df.empty):
            global_log(f"NYFed Data Fetching Error: {nyfed_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['nyfed'] = False
        elif nyfed_data_df is not None and not nyfed_data_df.empty:
            global_log(f"Fetched {len(nyfed_data_df)} NYFed records.", "INFO", logger_name="MainApp.main_flow")
            if nyfed_error_msg:
                 global_log(f"NYFed Data Fetching completed with some errors: {nyfed_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', nyfed_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['nyfed'] = True
        else:
            global_log("NYFed Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['nyfed'] = False

        yf_conn = YFinanceConnector(config, logger_instance=yf_logger)
        yfinance_tickers_list = config.get('target_metrics', {}).get('yfinance_tickers', [])
        # Pass end_date_to_use to YFinanceConnector
        yf_data_df, yf_error_msg = yf_conn.fetch_data(tickers=yfinance_tickers_list, start_date=start_date_cfg, end_date=end_date_to_use)
        if yf_error_msg and (yf_data_df is None or yf_data_df.empty):
            global_log(f"YFinance Data Fetching Error for {yfinance_tickers_list}: {yf_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['yfinance_move'] = False
        elif yf_data_df is not None and not yf_data_df.empty:
            global_log(f"Fetched {len(yf_data_df)} YFinance records for {yfinance_tickers_list}.", "INFO", logger_name="MainApp.main_flow")
            if yf_error_msg:
                global_log(f"YFinance Data Fetching for {yfinance_tickers_list} completed with some errors: {yf_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_stock_price', yf_data_df, unique_cols=stock_unique_cols)
            data_fetch_status['yfinance_move'] = True
        else:
            global_log(f"YFinance Connector returned no data for {yfinance_tickers_list}.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['yfinance_move'] = False

        global_log(f"\n--- 階段 2 & 3: 指標計算與市場簡報 (數據截止於 {end_date_to_use}) ---", "INFO", logger_name="MainApp.main_flow")

        current_macro_data_for_engine = db_manager.fetch_all_for_engine('fact_macro_economic_data', start_date_cfg, end_date_to_use, date_column='metric_date')
        current_stock_data_for_engine = db_manager.fetch_all_for_engine('fact_stock_price', start_date_cfg, end_date_to_use, date_column='price_date')

        if (current_macro_data_for_engine is None or current_macro_data_for_engine.empty) and \
           (current_stock_data_for_engine is None or current_stock_data_for_engine.empty):
            global_log("IndicatorEngine: Insufficient data from DB for calculation. Skipping stress index.", "ERROR", logger_name="MainApp.main_flow")
        else:
            current_macro_data_for_engine = current_macro_data_for_engine if current_macro_data_for_engine is not None else pd.DataFrame()
            current_stock_data_for_engine = current_stock_data_for_engine if current_stock_data_for_engine is not None else pd.DataFrame()

            move_data_for_engine = pd.DataFrame()
            if not current_stock_data_for_engine.empty and 'security_id' in current_stock_data_for_engine.columns:
                move_data_for_engine = current_stock_data_for_engine[current_stock_data_for_engine['security_id'] == '^MOVE']

            if move_data_for_engine.empty and '^MOVE' in yfinance_tickers_list : # Check if MOVE was expected
                global_log("IndicatorEngine: ^MOVE data not found in DB stock data or stock data was empty (for MOVE).", "WARNING", logger_name="MainApp.main_flow")

            engine_input_data = {'macro': current_macro_data_for_engine, 'move': move_data_for_engine}
            engine_params_from_config = config.get('indicator_engine_params', {})

            indicator_engine_instance = IndicatorEngine(engine_input_data, params=engine_params_from_config, logger_instance=engine_logger)
            stress_index_df = indicator_engine_instance.calculate_dealer_stress_index()

            if stress_index_df is None or stress_index_df.empty:
                global_log(f"Dealer Stress Index calculation resulted in no data or all NaN values for date {end_date_to_use}.", "ERROR", logger_name="MainApp.main_flow")
                # Create a dummy market_briefing_output for AI if stress index fails, to still log an AI attempt
                market_briefing_output = {
                    "briefing_date": end_date_to_use,
                    "data_window_end_date": end_date_to_use,
                    "dealer_stress_index": {"current_value_description": "Calculation Failed", "trend_approximation": "N/A"},
                    "key_financial_components_latest": [],
                    "broader_market_context_latest": {},
                    "summary_narrative": f"市場壓力指數 ({end_date_to_use}): 計算失敗，無法生成簡報。"
                }
                global_log("Generated dummy market briefing due to stress index calculation failure.", "WARNING", logger_name="MainApp.Briefing")

            else:
                global_log(f"Dealer Stress Index calculated. Shape: {stress_index_df.shape}. Latest date in index: {stress_index_df.index[-1].strftime('%Y-%m-%d') if not stress_index_df.empty else 'N/A'}", "INFO", logger_name="MainApp.main_flow")
                global_log(f"Stress Index Tail (for {end_date_to_use}):\n{stress_index_df.tail().to_string()}", "INFO", logger_name="MainApp.main_flow")

                briefing_date = stress_index_df.index[-1] # This should be <= end_date_to_use
                briefing_date_str = briefing_date.strftime('%Y-%m-%d')
                latest_stress_value = stress_index_df['DealerStressIndex'].iloc[-1]

                stress_level_desc = "N/A"
                if pd.notna(latest_stress_value):
                    threshold_moderate = engine_params_from_config.get('stress_threshold_moderate', 40)
                    threshold_high = engine_params_from_config.get('stress_threshold_high', 60)
                    threshold_extreme = engine_params_from_config.get('stress_threshold_extreme', 80)
                    if latest_stress_value >= threshold_extreme: stress_level_desc = f"{latest_stress_value:.2f} (極度緊張)"
                    elif latest_stress_value >= threshold_high: stress_level_desc = f"{latest_stress_value:.2f} (高度緊張)"
                    elif latest_stress_value >= threshold_moderate: stress_level_desc = f"{latest_stress_value:.2f} (中度緊張)"
                    else: stress_level_desc = f"{latest_stress_value:.2f} (正常)"

                stress_trend_desc = "N/A"
                if len(stress_index_df['DealerStressIndex'].dropna()) >= 2:
                    change_in_stress = stress_index_df['DealerStressIndex'].diff().iloc[-1]
                    if pd.notna(change_in_stress):
                        stress_trend_desc = "上升" if change_in_stress > 0.1 else ("下降" if change_in_stress < -0.1 else "穩定")

                engine_prepared_full_df = indicator_engine_instance.df_prepared
                latest_briefing_components_data = None
                if engine_prepared_full_df is not None and not engine_prepared_full_df.empty:
                    # Try to get data for the actual briefing_date (which is the latest date in stress_index_df)
                    if briefing_date in engine_prepared_full_df.index:
                        latest_briefing_components_data = engine_prepared_full_df.loc[briefing_date]
                    else:
                        try: # Fallback to string match if datetime object key fails
                           latest_briefing_components_data = engine_prepared_full_df.loc[briefing_date_str]
                        except KeyError:
                           global_log(f"Could not find briefing_date {briefing_date_str} or {briefing_date} in engine_prepared_df. Using last available row.", "WARNING", logger_name="MainApp.Briefing")
                           if not engine_prepared_full_df.empty: latest_briefing_components_data = engine_prepared_full_df.iloc[-1]

                def get_formatted_value(series_data, component_key, value_format="{:.2f}", not_available_str="N/A"):
                    if series_data is not None and component_key in series_data.index and pd.notna(series_data[component_key]):
                        val = series_data[component_key]
                        try:
                            return value_format.format(val) if isinstance(val, (int, float)) and pd.notna(val) else str(val)
                        except (ValueError, TypeError):
                            return str(val)
                    return not_available_str

                move_value_str = get_formatted_value(latest_briefing_components_data, '^MOVE')
                spread_10y2y_raw = latest_briefing_components_data['spread_10y2y'] if latest_briefing_components_data is not None and 'spread_10y2y' in latest_briefing_components_data else None
                spread_10y2y_str = f"{(spread_10y2y_raw * 100):.2f} bps" if pd.notna(spread_10y2y_raw) else "N/A"
                primary_dealer_pos_str = get_formatted_value(latest_briefing_components_data, 'NYFED/PRIMARY_DEALER_NET_POSITION', value_format="{:,.0f}")
                vix_value_str = get_formatted_value(latest_briefing_components_data, 'FRED/VIXCLS')
                sofr_dev_str = get_formatted_value(latest_briefing_components_data, 'FRED/SOFR_Dev')

                market_briefing_output = {
                    "briefing_date": briefing_date_str, # Date of the actual data point used for briefing
                    "data_window_end_date": end_date_to_use, # The requested end_date for the entire data window
                    "dealer_stress_index": {"current_value_description": stress_level_desc, "trend_approximation": stress_trend_desc},
                    "key_financial_components_latest": [
                        {"component_name": "MOVE Index (Bond Mkt Volatility)", "value_string": move_value_str},
                        {"component_name": "10Y-2Y Treasury Spread", "value_string": spread_10y2y_str},
                        {"component_name": "Primary Dealer Net Positions (Millions USD)", "value_string": primary_dealer_pos_str}
                    ],
                    "broader_market_context_latest": {
                        "vix_index (Equity Mkt Volatility)": vix_value_str,
                        "sofr_deviation_from_ma": sofr_dev_str
                    },
                    "summary_narrative": (
                        f"市場壓力指數 ({briefing_date_str}, 數據截止於 {end_date_to_use}): {stress_level_desc}. "
                        f"主要影響因素包括債券市場波動率 (MOVE Index: {move_value_str}) 及 "
                        f"10年期與2年期公債利差 ({spread_10y2y_str}). "
                        f"一級交易商淨持倉部位為 {primary_dealer_pos_str} 百萬美元。"
                    )
                }

            global_log(f"\n--- 市場簡報 (Market Briefing - JSON for {end_date_to_use}) ---", "INFO", logger_name="MainApp.Briefing")
            # Print to console for run_prototype.sh / run_historical_job.sh to capture
            # For historical runs, this might be too verbose in the main simulation log, consider conditional print or logging only.
            # print(f"\n--- 市場簡報 (Market Briefing - JSON for {end_date_to_use}) ---")
            # print(json.dumps(market_briefing_output, indent=2, ensure_ascii=False))
            global_log(json.dumps(market_briefing_output, indent=2, ensure_ascii=False), "INFO", logger_name="MainApp.BriefingOutput")

            # --- AI Agent Interaction and Logging ---
            global_log(f"\n--- 階段 4: AI 決策與日誌記錄 (模擬日期: {end_date_to_use}) ---", "INFO", logger_name="MainApp.AIInteraction")
            ai_agent_logger = logging.getLogger("project_logger.AIAgent")
            ai_agent_config_params = {
                'requests_config': config.get('requests_config', {}),
                'ai_agent_mock_config': config.get('ai_agent_mock_params', {
                    'simulate_network_latency_max_sec': 0.2,
                    'simulate_failure_rate': 0.05
                })
            }
            mock_ai_agent_instance = MockAIAgent(config=ai_agent_config_params, logger_instance=ai_agent_logger)

            market_brief_json_for_ai = json.dumps(market_briefing_output)

            ai_response_text, ai_error = mock_ai_agent_instance.get_decision(market_brief_json_for_ai)

            try:
                sim_timestamp_dt_object = datetime.strptime(end_date_to_use, '%Y-%m-%d')
                sim_timestamp = datetime(sim_timestamp_dt_object.year, sim_timestamp_dt_object.month, sim_timestamp_dt_object.day, 0, 0, 0, tzinfo=timezone.utc)
            except ValueError:
                global_log(f"Could not parse end_date_to_use '{end_date_to_use}' into datetime for simulation_timestamp. Using current UTC time as fallback.", "ERROR", logger_name="MainApp.AIInteraction")
                sim_timestamp = datetime.now(timezone.utc)

            if ai_error:
                global_log(f"AI Agent get_decision failed: {ai_error}", "ERROR", logger_name="MainApp.AIInteraction")
                db_manager.bulk_insert_or_replace(
                    'log_ai_decision',
                    pd.DataFrame([{
                        'simulation_timestamp': sim_timestamp,
                        'market_brief_json': market_brief_json_for_ai,
                        'ai_response_text': ai_response_text if ai_response_text else "AI Agent Error: " + ai_error,
                        'strategy_summary': "AI Error",
                        'key_factors': "AI Error"
                    }]),
                    unique_cols=['simulation_timestamp']
                )
            elif ai_response_text:
                global_log(f"AI Agent response received (for {end_date_to_use}):\n{ai_response_text}", "INFO", logger_name="MainApp.AIInteraction")
                strategy_summary_from_ai = "格式解析失敗"
                key_factors_from_ai_str = "格式解析失敗"
                try:
                    ai_decision_data = json.loads(ai_response_text)
                    strategy_summary_from_ai = ai_decision_data.get("strategy_summary", "未提供策略摘要")
                    key_factors_list = ai_decision_data.get("key_factors", ["未提供關鍵因子"])
                    key_factors_from_ai_str = json.dumps(key_factors_list, ensure_ascii=False)
                    global_log("AI response parsed successfully.", "INFO", logger_name="MainApp.AIInteraction")
                except json.JSONDecodeError:
                    global_log(f"Failed to parse AI response JSON: {ai_response_text}", "ERROR", logger_name="MainApp.AIInteraction")

                db_manager.bulk_insert_or_replace(
                    'log_ai_decision',
                    pd.DataFrame([{
                        'simulation_timestamp': sim_timestamp,
                        'market_brief_json': market_brief_json_for_ai,
                        'ai_response_text': ai_response_text,
                        'strategy_summary': strategy_summary_from_ai,
                        'key_factors': key_factors_from_ai_str
                    }]),
                    unique_cols=['simulation_timestamp']
                )
                global_log(f"AI decision for {end_date_to_use} logged to database.", "INFO", logger_name="MainApp.AIInteraction")
            else:
                 global_log(f"AI Agent returned no response and no error for {end_date_to_use}. This is unexpected.", "WARNING", logger_name="MainApp.AIInteraction")


    except FileNotFoundError as e_fnf:
        err_msg_fnf = f"CRITICAL FAILURE: Configuration file not found: {e_fnf}. Application cannot start."
        print(err_msg_fnf)
        if global_log: global_log(err_msg_fnf, "CRITICAL", logger_name="MainApp.main_flow", exc_info=False)
        else: pre_init_logger.critical(err_msg_fnf, exc_info=False)
        sys.exit(1) # Ensure script exits on critical config error
    except SystemExit as e_sys_exit: # Catch sys.exit() called due to bad args
        global_log(f"SystemExit called: {e_sys_exit}. This might be due to invalid command line arguments.", "CRITICAL", logger_name="MainApp.main_flow")
        raise # Re-raise to ensure the script actually exits
    except Exception as e_main_runtime:
        err_msg_runtime = f"主流程 main() 發生嚴重執行期錯誤 (Execution Date: {args_main.execution_date if args_main.execution_date else 'Default'}): {e_main_runtime}"
        print(err_msg_runtime)
        if global_log: global_log(err_msg_runtime, "CRITICAL", logger_name="MainApp.main_flow", exc_info=True)
        else: pre_init_logger.critical(err_msg_runtime, exc_info=True)
        sys.exit(1) # Ensure script exits on other critical errors
    finally:
        if 'db_manager' in locals() and db_manager is not None:
            db_manager.disconnect()
        else:
            global_log("DB Manager was not instantiated, skipping disconnect.", "DEBUG", logger_name="MainApp.main_flow")

        global_log(f"\n--- 端到端原型執行完畢 (Execution Date: {args_main.execution_date if args_main.execution_date else 'Default'}) ---", "INFO", logger_name="MainApp.main_flow")

        if detailed_run_log_handler is not None and 'root_logger_for_detailed' in locals():
            global_log(f"Removing detailed run log handler. Transcript saved to {current_detailed_log_filename}", "INFO", logger_name="MainApp.Cleanup")
            if hasattr(locals().get('root_logger_for_detailed'), 'removeHandler'):
                 root_logger_for_detailed.removeHandler(detailed_run_log_handler)
            detailed_run_log_handler.close()

if __name__ == "__main__":
    # Global arg parsing for early access if needed by pre-main logic (though not typical)
    parser_global = argparse.ArgumentParser(add_help=False) # add_help=False to avoid conflict if main also defines it
    parser_global.add_argument("--execution_date", type=str, default=None)
    cli_args, _ = parser_global.parse_known_args()


    if global_log is None:
        pre_init_logger.critical("global_log function was not assigned its fallback. Logging will be severely limited.")
        global_log = lambda msg, level="INFO", **kwargs: print(f"ULTRA_FALLBACK_LOG [{level.upper()}] {msg}")

    if global_log_file_path_imported:
        global_log(f"Confirmed global application log file from module: {global_log_file_path_imported}", "DEBUG", logger_name="MainApp.InitCheck")
    else:
        global_log("Global application log file path from module was not set. Bootstrap logger might be active for app logs.", "WARNING", logger_name="MainApp.InitCheck")

    # Pass all command line arguments to main. This is important if run_historical_job.sh passes --execution_date.
    # sys.argv includes the script name as the first element.
    # main() will re-parse them using its own ArgumentParser instance.
    try:
        main()
    except SystemExit as e:
        # This will catch sys.exit calls, e.g. from invalid --execution_date format.
        # The run_historical_simulation.sh script will check the exit code.
        if global_log: global_log(f"main.py exited with code {e.code}", "INFO", logger_name="MainApp.Exit")
        else: print(f"main.py exited with code {e.code}")
        sys.exit(e.code if e.code is not None else 1) # Propagate exit code
    except Exception as e_top_level:
        # Catch any other unhandled exception from main() that wasn't a SystemExit
        if global_log: global_log(f"Unhandled exception at top level of main.py: {e_top_level}", "CRITICAL", logger_name="MainApp.Unhandled", exc_info=True)
        else: print(f"CRITICAL UNHANDLED EXCEPTION in main.py: {e_top_level}")
        sys.exit(1) # Exit with error code 1 for unhandled exceptions

EOF

echo "Creating __init__.py files..."
# Ensure all __init__.py files are created or are correct
cat <<EOF > src/__init__.py
# This file makes 'src' a package.
EOF

cat <<EOF > src/configs/__init__.py
# This file makes 'src/configs' a package.
EOF

cat <<EOF > src/connectors/__init__.py
# This file makes 'src/connectors' a package.
from .base import BaseConnector
from .nyfed_connector import NYFedConnector
from .yfinance_connector import YFinanceConnector
from .fred_connector import FredConnector
EOF

cat <<EOF > src/database/__init__.py
# This file makes 'src/database' a package.
from .database_manager import DatabaseManager
EOF

cat <<EOF > src/engine/__init__.py
# This file makes 'src/engine' a package.
from .indicator_engine import IndicatorEngine
EOF

cat <<EOF > src/scripts/__init__.py
# This file makes 'src/scripts' a package.
from .initialize_global_log import initialize_log_file, log_message, get_taipei_time
EOF

# Ensuring no src/ai_agent/__init__.py is created. ai_agent.py is a direct module.

# === 階段三：依賴安裝 ===
echo ""
echo "Phase 3: Installing dependencies from requirements.txt (Historical Job)..."
pip install -r requirements.txt

# === 階段四：執行主流程 ===
echo ""
echo "Phase 4: Running the main application (src/main.py) for historical date: $1 (Historical Job)..."
# Pass the first command-line argument (the date) to main.py's --execution_date parameter

if [ -z "$1" ]; then
  echo "Historical date argument not provided to run_historical_job.sh. Running with default date logic."
  python src/main.py
else
  echo "Executing main.py with --execution_date $1"
  python src/main.py --execution_date "$1"
fi

# Capture exit code for run_historical_simulation.sh
exit_code=$?
echo "main.py exited with code: $exit_code"


echo ""
echo "Execution finished for date: $1 (Historical Job)."
# Detailed log (market_briefing_log_YYYY-MM-DD.txt) and application logs (in api_test_logs/) are generated by main.py
exit $exit_code # Propagate the exit code
