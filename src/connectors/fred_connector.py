from fredapi import Fred
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timezone
import logging
import os # For accessing API key from environment variable

try:
    from .base import BaseConnector
except ImportError:
    if __name__ == '__main__': # For standalone testing
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
        if self.fred_client is None:
            # Return an empty DataFrame with the expected schema and an error message
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
                series_data = self.fred_client.get_series(series_id, observation_start=start_date, observation_end=end_date)

                if series_data.empty:
                    self.logger.warning(f"No data returned for FRED series_id: {series_id} for the given date range.")
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

        if final_df.empty: # Should be caught if all_series_data_list is empty
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
        "api_endpoints": { "fred": { "api_key_env": "FRED_API_KEY_TEST" } } # Use a distinct env var for testing
    }

    # For testing, explicitly set the API key if you have one, or mock Fred()
    # IMPORTANT: Do not commit real API keys.
    MOCK_API_KEY_FOR_TEST = "YOUR_TEST_API_KEY_OR_MOCK"
    # os.environ["FRED_API_KEY_TEST"] = MOCK_API_KEY_FOR_TEST # FredConnector will pick this up

    if not os.getenv(sample_fred_config['api_endpoints']['fred']['api_key_env']):
        # Fallback: Try the main key if test key not set (for user convenience during dev)
        main_api_key_env = "FRED_API_KEY" # As defined in project_config.yaml template
        if os.getenv(main_api_key_env):
            test_logger_fred.warning(f"Test-specific FRED API key env var '{sample_fred_config['api_endpoints']['fred']['api_key_env']}' not set. Falling back to main key '{main_api_key_env}' for this test run.")
            os.environ[sample_fred_config['api_endpoints']['fred']['api_key_env']] = os.getenv(main_api_key_env)
        else:
             test_logger_fred.error(f"Cannot run FredConnector test: Neither test env var '{sample_fred_config['api_endpoints']['fred']['api_key_env']}' nor main env var '{main_api_key_env}' for FRED API key is set.")
             sys.exit(1) # Exit if no key can be found for testing

    test_logger_fred.info("--- Starting FredConnector Test ---")
    fred_conn_test = FredConnector(config=sample_fred_config, logger_instance=test_logger_fred)

    if fred_conn_test.fred_client is not None:
        test_series_list = ["DGS10", "FEDFUNDS", "UNRATE", "NONEXISTENTSERIESXYZ"]
        test_start = "2023-01-01"
        test_end = "2023-02-28"

        test_logger_fred.info(f"Testing fetch_data for series: {test_series_list} from {test_start} to {test_end}")
        fred_df, fred_err = fred_conn_test.fetch_data(series_ids=test_series_list, start_date=test_start, end_date=test_end)

        if fred_err:
            test_logger_fred.warning(f"FredConnector test fetch_data completed with error(s): {fred_err}")

        if fred_df is not None and not fred_df.empty:
            test_logger_fred.info(f"FredConnector test fetch_data returned data. Shape: {fred_df.shape}")
            test_logger_fred.info(f"Result head:\n{fred_df.head().to_string()}")
            test_logger_fred.info(f"Result tail:\n{fred_df.tail().to_string()}")
            actual_metrics = set(fred_df['metric_name'].unique())
            test_logger_fred.info(f"Metrics returned: {actual_metrics}")
            if "FRED/NONEXISTENTSERIESXYZ" not in actual_metrics:
                test_logger_fred.info("Correctly did not return data for 'NONEXISTENTSERIESXYZ'.")
        elif fred_df is not None and fred_df.empty:
             test_logger_fred.warning("FredConnector test fetch_data returned an empty DataFrame. This might be due to all series failing or returning no data for the period.")
        else: # fred_df is None
             test_logger_fred.error(f"FredConnector test fetch_data returned None for data. Error was: {fred_err}")
    else:
        test_logger_fred.error("FredConnector client (self.fred_client) was not initialized in test. API key issue likely.")
    test_logger_fred.info("--- FredConnector Test Finished ---")
