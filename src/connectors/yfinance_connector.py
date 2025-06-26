import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import sys
import requests # For session type hint, though not strictly used in __init__ here

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__':
        from base import BaseConnector
    else:
        raise

class YFinanceConnector(BaseConnector):
    """使用 yfinance 獲取股價和指數數據。"""

    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, session: Optional[requests.Session] = None): # session param kept for interface consistency if needed later
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        super().__init__(config, source_api_name="yfinance")
        # self.requests_session = session # Not actively used by yfinance Ticker object directly in its constructor like some other libs

    def fetch_data(self, tickers: List[str], start_date: str, end_date: Optional[str] = None,
                   interval: str = "1d", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        self.logger.info(f"Fetching yfinance data for tickers: {tickers} from {start_date} to {end_date} with interval {interval}.")

        if not tickers:
            self.logger.warning("No tickers provided to YFinanceConnector fetch_data.")
            # Return DataFrame with all expected columns for consistency
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), "No tickers provided."

        all_ticker_data_list = []
        # yfinance's Ticker object can accept a session for underlying requests,
        # but it's often managed internally or via its own mechanisms.
        # For this script, we'll let yfinance handle its session management unless a specific need arises.
        # session_to_use = kwargs.get('session', self.requests_session)

        for ticker_symbol in tickers:
            self.logger.debug(f"Fetching yfinance data for: {ticker_symbol}")
            try:
                ticker_obj = yf.Ticker(ticker_symbol) # Let yf.Ticker manage its session

                hist_df = ticker_obj.history(
                    start=start_date,
                    end=end_date,
                    interval=interval,
                    auto_adjust=False, # Important to get 'Adj Close' and 'Close' separately if needed, and splits/dividends
                    actions=True,      # To get dividends and stock splits
                    # progress=False,    # Removed: yfinance 0.2.x no longer supports 'progress' arg here
                )

                if hist_df.empty:
                    self.logger.warning(f"yfinance returned no data for ticker: {ticker_symbol} (start: {start_date}, end: {end_date}, interval: {interval}).")
                    continue

                hist_df.reset_index(inplace=True)

                # Determine the correct date column name (yfinance can vary this)
                date_col_name = None
                if 'Datetime' in hist_df.columns: date_col_name = 'Datetime' # Usually for intraday
                elif 'Date' in hist_df.columns: date_col_name = 'Date'       # Usually for daily

                if not date_col_name:
                    self.logger.error(f"Date column ('Date' or 'Datetime') not found in yfinance data for {ticker_symbol}. Columns: {hist_df.columns.tolist()}")
                    continue

                # Standardize column names
                rename_map = {
                    date_col_name: 'price_date', 'Open': 'open_price', 'High': 'high_price',
                    'Low': 'low_price', 'Close': 'close_price', 'Adj Close': 'adj_close_price',
                    'Volume': 'volume', 'Dividends': 'dividends', 'Stock Splits': 'stock_splits'
                }
                # Only rename columns that exist in the DataFrame
                current_rename_map = {k: v for k, v in rename_map.items() if k in hist_df.columns}
                df_renamed = hist_df.rename(columns=current_rename_map)

                # Convert price_date to just date (YYYY-MM-DD), removing time and timezone
                df_renamed['price_date'] = pd.to_datetime(df_renamed['price_date'])
                if df_renamed['price_date'].dt.tz is not None: # If timezone-aware
                    df_renamed['price_date'] = df_renamed['price_date'].dt.tz_localize(None) # Make timezone-naive
                df_renamed['price_date'] = df_renamed['price_date'].dt.normalize().dt.date # Get date part

                # Add standard metadata columns
                df_renamed['security_id'] = ticker_symbol
                df_renamed['source_api'] = self.source_api_name
                df_renamed['data_snapshot_timestamp'] = datetime.now(timezone.utc)

                # Ensure all expected final columns are present
                final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                                   'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                                   'source_api', 'data_snapshot_timestamp']

                for fc_col in final_cols_spec:
                    if fc_col not in df_renamed.columns:
                        # Default to 0.0 for dividends/splits, NA for others
                        default_val = 0.0 if fc_col in ['dividends', 'stock_splits'] else pd.NA
                        df_renamed[fc_col] = default_val

                all_ticker_data_list.append(df_renamed[final_cols_spec])
                self.logger.debug(f"Processed yfinance data for {ticker_symbol}, {len(df_renamed)} rows.")

            except Exception as e: # Catch broader exceptions from yfinance
                self.logger.error(f"Error fetching/processing yfinance for {ticker_symbol}: {e}", exc_info=True)

        if not all_ticker_data_list:
            self.logger.warning(f"No data successfully fetched for any yfinance tickers: {tickers}")
            final_cols_spec = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price',
                               'close_price', 'adj_close_price', 'volume', 'dividends', 'stock_splits',
                               'source_api', 'data_snapshot_timestamp']
            return pd.DataFrame(columns=final_cols_spec), f"No data from yfinance for any of the tickers: {tickers}."

        final_df = pd.concat(all_ticker_data_list, ignore_index=True)

        if final_df.empty: # Should be caught by the above, but as a safeguard
             self.logger.warning("Final combined yfinance data is empty (all tickers failed or returned no data).")
             return final_df, "Final combined yfinance data is empty."

        # Ensure correct dtypes for numeric columns
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'dividends', 'stock_splits']
        for col_to_num in numeric_cols:
            if col_to_num in final_df.columns:
                final_df[col_to_num] = pd.to_numeric(final_df[col_to_num], errors='coerce')
        if 'volume' in final_df.columns: # Volume should be integer
            final_df['volume'] = pd.to_numeric(final_df['volume'], errors='coerce').astype('Int64') # Use nullable Int64

        self.logger.info(f"Successfully fetched and processed {len(final_df)} total records from yfinance for tickers: {tickers}.")
        return final_df, None


if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_yf = logging.getLogger("YFinanceConnectorTestRun_Atomic")
    if not test_logger_yf.handlers:
        ch_yf = logging.StreamHandler(sys.stdout)
        ch_yf.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_yf.addHandler(ch_yf)
        test_logger_yf.propagate = False

    sample_config_yf = {} # YFinanceConnector doesn't use much from config in this version
    yf_connector = YFinanceConnector(config=sample_config_yf, logger_instance=test_logger_yf)

    test_logger_yf.info("\n--- Testing YFinanceConnector for ^MOVE ---")
    move_df, move_err = yf_connector.fetch_data(tickers=["^MOVE"], start_date="2024-01-01", end_date="2024-01-15")
    if move_err:
        test_logger_yf.error(f"^MOVE Test Error: {move_err}")
    elif move_df is not None:
        test_logger_yf.info(f"^MOVE Test OK. Shape: {move_df.shape}")
        if not move_df.empty: test_logger_yf.info(f"^MOVE Head:\n{move_df.head().to_string()}")

    test_logger_yf.info("\n--- Testing YFinanceConnector for AAPL, NONEXISTENTTICKERXYZ ---")
    # Test with a mix of valid and potentially invalid tickers
    mixed_tickers = ["AAPL", "NONEXISTENTTICKERXYZ"]
    stocks_df, stocks_err = yf_connector.fetch_data(tickers=mixed_tickers, start_date="2024-01-01", end_date="2024-01-05")
    if stocks_err: # An error message might be returned if ALL fail, or partial data with warnings logged
        test_logger_yf.warning(f"Mixed Stocks Test potentially completed with issues: {stocks_err}")

    if stocks_df is not None:
        test_logger_yf.info(f"Mixed Stocks Test Data Shape: {stocks_df.shape}")
        if not stocks_df.empty:
            test_logger_yf.info(f"Mixed Stocks Data Head:\n{stocks_df.head().to_string()}")
            unique_tickers_found_mixed = stocks_df['security_id'].unique()
            test_logger_yf.info(f"Found data for tickers: {unique_tickers_found_mixed}")
            if "AAPL" in unique_tickers_found_mixed:
                test_logger_yf.info("AAPL data was found.")
            if "NONEXISTENTTICKERXYZ" not in unique_tickers_found_mixed:
                 test_logger_yf.info("NONEXISTENTTICKERXYZ correctly did not return data or was skipped.")
        else:
            test_logger_yf.info("Mixed Stocks Test returned an empty DataFrame (e.g., if AAPL also had no data for the period or all failed).")

    test_logger_yf.info("\n--- Testing YFinanceConnector with empty ticker list ---")
    empty_df, empty_err = yf_connector.fetch_data(tickers=[], start_date="2024-01-01")
    if empty_err == "No tickers provided." and (empty_df is not None and empty_df.empty):
        test_logger_yf.info(f"OK (empty ticker list): Error='{empty_err}', DataFrame is empty as expected.")
    else:
        test_logger_yf.error(f"Fail (empty ticker list): err='{empty_err}', df_empty={empty_df.empty if empty_df is not None else 'N/A'}")

    test_logger_yf.info("--- YFinanceConnector Test Finished ---")
