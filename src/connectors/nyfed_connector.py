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
