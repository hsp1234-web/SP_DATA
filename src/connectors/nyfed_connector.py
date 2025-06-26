import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional, List
from io import BytesIO
import logging
from datetime import datetime, timezone
import sys
import time
import random
from bs4 import BeautifulSoup # Added for HTML parsing
from urllib.parse import urljoin # Added for joining relative URLs

try:
    from .base import BaseConnector
except ImportError:
    # This fallback might be useful if running the script directly for testing
    # For the atomic script, this should ideally not be hit if structure is correct.
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
            if not self.logger.handlers and not logging.getLogger().hasHandlers(): # Basic NullHandler setup
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

    def _download_excel_with_retries(self, url_config: Dict[str, str]) -> Optional[BytesIO]: # Changed signature
        url = url_config.get('url') # Get URL from url_config
        if not url:
            self.logger.error("URL not found in url_config for _download_excel_with_retries.")
            return None

        retries = self.requests_config.get('max_retries', 3)
        base_backoff = self.requests_config.get('base_backoff_seconds', 1)
        timeout_sec = self.requests_config.get('download_timeout', self.requests_config.get('timeout', 60))

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        for attempt in range(retries):
            try:
                self.logger.debug(f"Attempt {attempt + 1}/{retries} to access NYFed resource page: {url}")
                page_response = requests.get(url, timeout=timeout_sec, headers=headers)
                self.logger.info(f"NYFed Page URL: {url}, Attempt: {attempt + 1}, Status: {page_response.status_code}, Content-Type: {page_response.headers.get('Content-Type')}")
                page_response.raise_for_status()

                # If the page itself is HTML, try to find an Excel link
                if 'text/html' in page_response.headers.get('Content-Type', '').lower():
                    self.logger.info(f"Content from {url} is HTML. Attempting to find Excel link...")
                    soup = BeautifulSoup(page_response.content, 'html.parser')
                    file_pattern_hint = url_config.get('file_pattern', '') # Now uses passed url_config
                    year_hint = ''.join(filter(str.isdigit, file_pattern_hint))

                    excel_link_found = None
                    # Try a few patterns for finding the link
                    possible_links = soup.find_all('a', href=True)
                    self.logger.debug(f"Found {len(possible_links)} links on page {url}. Checking for Excel files related to '{file_pattern_hint}'.")

                    for link_tag in possible_links:
                        href = link_tag['href']
                        link_text = link_tag.get_text(strip=True)
                        # Prioritize links containing the file_pattern directly or parts of it
                        if file_pattern_hint and file_pattern_hint.lower() in href.lower():
                             excel_link_found = href
                             self.logger.info(f"Found strong match for Excel link by file_pattern in href: {excel_link_found} for {url}")
                             break
                        if year_hint and year_hint in href.lower() and '.xlsx' in href.lower():
                             excel_link_found = href
                             self.logger.info(f"Found year match for Excel link in href: {excel_link_found} for {url}")
                             break
                        if 'prideal' in href.lower() and '.xlsx' in href.lower(): # General fallback
                             excel_link_found = href
                             self.logger.info(f"Found general 'prideal' Excel link in href: {excel_link_found} for {url}")
                             break
                        if link_text and file_pattern_hint and file_pattern_hint.lower() in link_text.lower() and '.xlsx' in href.lower():
                            excel_link_found = href
                            self.logger.info(f"Found Excel link by text match '{link_text}': {excel_link_found} for {url}")
                            break

                    if excel_link_found:
                        # Ensure the link is absolute
                        if not excel_link_found.startswith('http'):
                            from urllib.parse import urljoin
                            excel_download_url = urljoin(url, excel_link_found) # url is the base page URL
                        else:
                            excel_download_url = excel_link_found

                        self.logger.info(f"Attempting to download actual Excel file from scraped URL: {excel_download_url}")
                        response = requests.get(excel_download_url, timeout=timeout_sec, headers=headers)
                        self.logger.info(f"Scraped Excel URL: {excel_download_url}, Status: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}, Size: {len(response.content)} bytes")
                        response.raise_for_status()
                        # Now check content type of the actual downloaded file
                        content_type = response.headers.get('Content-Type', '').lower()
                        if any(ct in content_type for ct in ['excel', 'spreadsheetml', 'officedocument', 'application/octet-stream']):
                            self.logger.info(f"Successfully downloaded and verified Excel from scraped link: {excel_download_url}")
                            return BytesIO(response.content)
                        else:
                            self.logger.error(f"Scraped link {excel_download_url} provided non-Excel Content-Type: '{content_type}'. Skipping.")
                            return None
                    else:
                        self.logger.error(f"Could not find a suitable Excel download link on HTML page: {url} for pattern '{file_pattern_hint}'.")
                        self.logger.debug(f"Page content sample (first 1000 bytes of HTML from {url}): {page_response.content[:1000].decode('utf-8', errors='replace')}")
                        return None # No link found

                # If original URL was not HTML, or if it was HTML but logic above failed to return:
                # This part handles direct downloads if the URL itself is supposed to be an Excel file
                # (original logic before scraping attempt)
                # For safety, let's assume if it wasn't HTML and wasn't handled, it might be a direct file.
                # This might be redundant if the HTML path is robust.
                # However, the previous logic had a direct check for excel content type.
                # Let's ensure if it's not HTML, we still check its content type directly.
                elif any(ct in page_response.headers.get('Content-Type', '').lower() for ct in ['excel', 'spreadsheetml', 'officedocument', 'application/octet-stream']):
                    self.logger.info(f"URL {url} seems to be a direct Excel link (Content-Type: {page_response.headers.get('Content-Type', '')}). Using its content.")
                    return BytesIO(page_response.content)
                else: # Neither HTML with a good link, nor a direct Excel link by Content-Type
                    self.logger.error(f"Content from {url} is not HTML with a usable Excel link, nor a direct Excel file. Content-Type: {page_response.headers.get('Content-Type', '')}. Skipping.")
                    self.logger.debug(f"Content sample (first 500 bytes from {url}): {page_response.content[:500].decode('utf-8', errors='replace') if page_response.content else 'No content'}")
                    return None

            except requests.exceptions.HTTPError as e:
                # This error is for the page_response or the subsequent excel_response
                response_for_error_log = e.response # Could be page_response or the actual excel download response
                self.logger.warning(f"HTTP error on attempt {attempt + 1}/{retries} for NYFed process (URL: {url}): {response_for_error_log.status_code if response_for_error_log else 'N/A'} - {response_for_error_log.text[:200] if response_for_error_log and response_for_error_log.text else 'N/A'}")
                if response_for_error_log is not None:
                    self.logger.warning(f"Headers for error response (URL: {url}): {response_for_error_log.headers}")
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

    def fetch_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        all_positions_data_list = []
        self.logger.info(f"Fetching NYFed data from {len(self.urls_config)} configured URLs.")

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
            excel_file_content = self._download_excel_with_retries(file_info) # Pass the whole file_info dict
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
        # For positions, taking the latest report for a given day seems reasonable.
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
        if pd.isna(min_d) or pd.isna(max_d): # Should not happen if df is not empty and dates are valid
            self.logger.error(f"Invalid date range for NYFed. Min: {min_d}, Max: {max_d}")
            return pd.DataFrame(columns=['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']), "Invalid date range for NYFed data."

        daily_idx = pd.date_range(start=min_d, end=max_d, freq='D')
        daily_df = combo_df.reindex(daily_idx).ffill() # Forward fill missing daily values
        daily_df.index.name = 'metric_date'
        daily_df.reset_index(inplace=True)

        # Ensure required columns are present even if daily_df becomes empty after reindex/ffill (unlikely but safeguard)
        final_cols = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'data_snapshot_timestamp']
        for col in final_cols:
            if col not in daily_df.columns:
                daily_df[col] = pd.NA # Or appropriate default

        if not daily_df.empty: # Re-assign static values after ffill
            daily_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
            daily_df['source_api'] = self.source_api_name
            # Snapshot timestamp should ideally be per fetch, but for ffilled data, using current time is acceptable
            daily_df['data_snapshot_timestamp'] = datetime.now(timezone.utc)

        self.logger.info(f"Processed {len(daily_df)} total NYFed records after daily ffill.")
        return daily_df[final_cols], None

# Main block for testing if script is run directly
if __name__ == '__main__':
    # Setup basic logging for test execution
    if not logging.getLogger().hasHandlers(): # Ensure no duplicate handlers from other runs
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_ny = logging.getLogger("NYFedConnectorTestRun_Atomic") # Unique name for this test run
    if not test_logger_ny.handlers: # Avoid adding handlers multiple times
        ch_ny = logging.StreamHandler(sys.stdout)
        ch_ny.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_ny.addHandler(ch_ny)
        test_logger_ny.propagate = False # Prevent logging to root if it has other handlers

    # Test configuration
    test_cfg = {
        'requests_config': {'max_retries': 2, 'base_backoff_seconds': 0.5, 'timeout': 15, 'download_timeout': 45},
        'nyfed_primary_dealer_urls': [
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx", "file_pattern": "prideal2023.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx", "file_pattern": "prideal2022.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/non_existent_file_for_test.xlsx", "file_pattern": "non_existent.xlsx", "format_type": "TEST_PD_FORMAT"},
            {"url": "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2021.xlsx", "file_pattern": "prideal2021.xlsx", "format_type": "UNKNOWN_RECIPE"}
        ],
        'nyfed_format_recipes': {
            "TEST_PD_FORMAT": {
                "header_row": 3,
                "date_column": "As of Date",
                "columns_to_sum": [
                    "U.S. Treasury coupons", "U.S. Treasury bills",
                    "U.S. Treasury floating rate notes (FRNs)", "NonExistentColumnForTest" # Include a non-existent column for robustness testing
                ],
                "data_unit_multiplier": 1000 # Test with a different multiplier
            }
        }
    }

    test_logger_ny.info("--- Starting NYFedConnector Test ---")
    ny_conn = NYFedConnector(config=test_cfg, logger_instance=test_logger_ny)
    ny_df_res, ny_err = ny_conn.fetch_data()

    if ny_err:
        test_logger_ny.error(f"NYFed Test failed with error: {ny_err}")
    elif ny_df_res is not None:
        test_logger_ny.info(f"NYFed Test successful. Fetched data shape: {ny_df_res.shape}")
        if not ny_df_res.empty:
            test_logger_ny.info(f"NYFed Data head:\n{ny_df_res.head().to_string()}")
            test_logger_ny.info(f"NYFed Data tail:\n{ny_df_res.tail().to_string()}")
            unique_dates_ny = ny_df_res['metric_date'].nunique()
            if not ny_df_res['metric_date'].empty:
                expected_days_ny = (ny_df_res['metric_date'].max() - ny_df_res['metric_date'].min()).days + 1
                if unique_dates_ny == expected_days_ny:
                    test_logger_ny.info(f"NYFed data frequency appears to be daily ({unique_dates_ny} days).")
                else:
                    test_logger_ny.warning(f"NYFed data frequency not strictly daily: {unique_dates_ny} unique dates for {expected_days_ny} day span.")
            else:
                test_logger_ny.warning("NYFed data has no dates to check frequency.")
        else:
            test_logger_ny.info("NYFed Test: Returned DataFrame is empty, as might be expected if all sources failed or had no data.")
    else:
        test_logger_ny.error("NYFed Test failed: result DataFrame is None and no error message was returned (unexpected state).")
    test_logger_ny.info("--- NYFedConnector Test Finished ---")
