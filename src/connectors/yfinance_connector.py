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
