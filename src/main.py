import yaml
import pandas as pd
from datetime import datetime, timezone, timedelta
import os
import logging
import json
import sys
from pathlib import Path
from typing import Dict, Any, Optional # Ensure Dict, Any, Optional are imported

# --- Early Path Setup & Pre-Init Logger ---
# This initial logger is basic and will be superseded by the global logger from initialize_global_log.py
# once that module is successfully imported and initialized.
if not logging.getLogger().hasHandlers(): # Check if any handlers are configured on the root logger
    logging.basicConfig(
        level=logging.DEBUG, # Capture all levels initially
        format='%(asctime)s [%(levelname)s] %(name)s - %(module)s.%(funcName)s:%(lineno)d - %(message)s (main-pre-init)',
        handlers=[logging.StreamHandler(sys.stdout)] # Log to console
    )
pre_init_logger = logging.getLogger("MainPreInit")

# PROJECT_ROOT should point to the 'src' directory's parent for atomic script.
# When run_prototype.sh executes 'python src/main.py', __file__ will be 'src/main.py'.
# So, Path(__file__).resolve().parent.parent gives the actual project root where run_prototype.sh is.
try:
    PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
except NameError: # Fallback if __file__ is not defined (e.g. interactive, though unlikely for atomic script)
    PROJECT_ROOT = str(Path(".").resolve())
    pre_init_logger.warning(f"__file__ not defined in main.py, PROJECT_ROOT set to CWD: {PROJECT_ROOT}")


# DETAILED_LOG_FILENAME will be created in the PROJECT_ROOT (where run_prototype.sh is)
# This specific log file is for the detailed transcript as requested by the user.
# The global logger (initialize_global_log) will create its own timestamped logs in api_test_logs/.
DETAILED_LOG_FILENAME = os.path.join(PROJECT_ROOT, "market_briefing_log.txt")


# PROJECT_ROOT is defined above.
SOURCE_ROOT = str(Path(__file__).resolve().parent)  # This is /app/src

# Explicitly add SOURCE_ROOT to sys.path to allow direct imports of modules within src
if SOURCE_ROOT not in sys.path:
    sys.path.insert(0, SOURCE_ROOT)
    pre_init_logger.info(f"Inserted SOURCE_ROOT ({SOURCE_ROOT}) into sys.path for direct module imports from 'src'.")

pre_init_logger.info(f"main.py: __file__ is {Path(__file__).resolve() if '__file__' in locals() else 'not_defined'}")
pre_init_logger.info(f"main.py: PROJECT_ROOT (parent of src): {PROJECT_ROOT}")
pre_init_logger.info(f"main.py: SOURCE_ROOT (src directory, added to sys.path): {SOURCE_ROOT}")
pre_init_logger.info(f"main.py: sys.path for module import: {sys.path}")


# --- Module Imports ---
# With SOURCE_ROOT in sys.path, modules directly under src should be importable.
global_log = None
init_global_log_function = None
global_log_file_path_imported = None
get_taipei_time_func_imported = None

try:
    # Direct imports for modules within src
    from connectors.base import BaseConnector
    from connectors.fred_connector import FredConnector
    from connectors.nyfed_connector import NYFedConnector
    from connectors.yfinance_connector import YFinanceConnector
    from database.database_manager import DatabaseManager
    from engine.indicator_engine import IndicatorEngine
    from ai_agent import RemoteAIAgent, AIDecisionModel # Direct import, expecting ai_agent.py in SOURCE_ROOT

    # For scripts, if they are in a sub-package of src (e.g., src.scripts)
    from scripts.initialize_global_log import log_message, get_taipei_time, LOG_FILE_PATH as GLOBAL_LOG_FILE_PATH_FROM_MODULE, initialize_log_file

    global_log = log_message
    init_global_log_function = initialize_log_file
    global_log_file_path_imported = GLOBAL_LOG_FILE_PATH_FROM_MODULE # Use the path from the module
    get_taipei_time_func_imported = get_taipei_time

    if init_global_log_function is not None:
        try:
            # The global logger will create its logs in <PROJECT_ROOT>/api_test_logs/
            # PROJECT_ROOT here is the actual root where run_prototype.sh resides.
            log_dir_for_global_logger = Path(PROJECT_ROOT) / "api_test_logs"

            actual_log_file = init_global_log_function(
                log_dir_override=str(log_dir_for_global_logger),
                force_reinit=True,
                project_root_path=Path(PROJECT_ROOT)
            )
            if actual_log_file:
                global_log(f"main.py: Global application logger (from initialize_global_log) explicitly initialized. Log file: {actual_log_file}", "INFO", logger_name="MainApp.Setup")
            else:
                global_log("main.py: Global application logger initialization returned no path. Check bootstrap logs.", "ERROR", logger_name="MainApp.Setup")
        except Exception as e_log_init_main:
            pre_init_logger.error(f"main.py: Failed to explicitly initialize global application logger: {e_log_init_main}", exc_info=True)
            if global_log is None: # Fallback if global_log assignment failed
                 global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback) {msg}")
            global_log("main.py: Using pre_init_logger or fallback due to global_log explicit init failure.", "WARNING", logger_name="MainApp.Setup")
    else:
        pre_init_logger.error("main.py: initialize_global_log_file function was not imported. Global application logging will be compromised.")
        if global_log is None: # Ensure global_log is callable
            global_log = lambda msg, level="INFO", **kwargs: pre_init_logger.log(logging.getLevelName(level.upper()), f"(global_log_fallback_no_init) {msg}")

except ImportError as e_imp:
    pre_init_logger.error(f"Failed to import custom modules: {e_imp}. Current sys.path: {sys.path}", exc_info=True)
    if global_log is None: print(f"CRITICAL IMPORT ERROR (main.py, global_log unavailable): {e_imp}.")
    else: global_log(f"CRITICAL: Failed to import custom modules in main.py: {e_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1) # Critical failure
except Exception as e_general_imp: # Catch any other error during imports
    pre_init_logger.error(f"General error during import phase: {e_general_imp}", exc_info=True)
    if global_log is None: print(f"CRITICAL GENERAL IMPORT ERROR (main.py, global_log unavailable): {e_general_imp}.")
    else: global_log(f"CRITICAL: General error during import phase in main.py: {e_general_imp}.", "ERROR", logger_name="MainApp.ImportError")
    sys.exit(1) # Critical failure

def load_config(config_path_relative_to_project_root="src/configs/project_config.yaml") -> Dict[str, Any]:
    """Loads the YAML configuration file."""
    # Path is now relative to PROJECT_ROOT (where run_prototype.sh is)
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
        raise # Re-raise to be caught by main's try-except
    except Exception as e_conf:
        global_log(f"Error loading or parsing config from {full_config_path}: {e_conf}", "CRITICAL", logger_name="MainApp.ConfigLoader", exc_info=True)
        raise # Re-raise

def main():
    # --- Setup detailed file logger for this specific run (market_briefing_log.txt) ---
    # This is separate from the application's global, timestamped logger.
    detailed_run_log_handler = None
    try:
        detailed_run_log_handler = logging.FileHandler(DETAILED_LOG_FILENAME, mode='w', encoding='utf-8')
        detailed_formatter = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s')
        detailed_run_log_handler.setFormatter(detailed_formatter)
        detailed_run_log_handler.setLevel(logging.DEBUG) # Capture all levels for this detailed log

        # Add this handler to the root logger to capture logs from all modules
        root_logger_for_detailed = logging.getLogger()
        root_logger_for_detailed.addHandler(detailed_run_log_handler)

        # Ensure console output from the global logger still works if it was set up
        global_log(f"Detailed execution transcript for this run will ALSO be saved to: {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Setup")
    except Exception as e_detail_log:
        # If detailed log setup fails, use pre_init_logger or global_log if available
        err_msg = f"Failed to set up detailed run log at {DETAILED_LOG_FILENAME}: {e_detail_log}"
        if global_log: global_log(err_msg, "ERROR", logger_name="MainApp.Setup", exc_info=True)
        else: pre_init_logger.error(err_msg, exc_info=True)
        # Continue execution even if this specific log fails. The global logger should still work.

    global_log("--- 開始執行端到端金融數據處理原型 (Atomic Script Version) ---", "INFO", logger_name="MainApp.main_flow")

    config: Dict[str, Any] = {}
    try:
        # Load configuration using the path relative to project root
        config = load_config(config_path_relative_to_project_root="src/configs/project_config.yaml")

        start_date_cfg = config.get('data_fetch_range', {}).get('start_date', "2020-01-01")
        end_date_cfg = config.get('data_fetch_range', {}).get('end_date') # Can be None

        current_date_for_end_calc = ""
        try:
            # Use the imported get_taipei_time function
            current_date_for_end_calc = get_taipei_time_func_imported().strftime('%Y-%m-%d') if get_taipei_time_func_imported else datetime.now(timezone.utc).strftime('%Y-%m-%d')
        except Exception as e_time_local: # Catch error if get_taipei_time_func_imported fails
            current_date_for_end_calc = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            global_log(f"Using UTC for 'today's date' as get_taipei_time function failed or was unavailable: {e_time_local}", "WARNING", logger_name="MainApp.main_flow")

        end_date_to_use = end_date_cfg if end_date_cfg else current_date_for_end_calc
        global_log(f"Data fetch range: Start='{start_date_cfg}', End='{end_date_to_use}'.", "INFO", logger_name="MainApp.main_flow")

        # FRED API Key Handling (as per original logic, user provided key is hardcoded for this task)
        fred_api_key_env_name = config.get('api_endpoints', {}).get('fred', {}).get('api_key_env', 'FRED_API_KEY') # Default if not in config
        user_provided_fred_key = "78ea51fb13b546d89f1a683cb4ba26f5" # User-provided key for the task
        os.environ[fred_api_key_env_name] = user_provided_fred_key
        global_log(f"Temporarily set environment variable '{fred_api_key_env_name}' for FRED API access.", "DEBUG", logger_name="MainApp.main_flow")

        # Instantiate loggers for different components
        db_logger = logging.getLogger("project_logger.DatabaseManager")
        fred_logger = logging.getLogger("project_logger.FredConnector")
        nyfed_logger = logging.getLogger("project_logger.NYFedConnector")
        yf_logger = logging.getLogger("project_logger.YFinanceConnector")
        engine_logger = logging.getLogger("project_logger.IndicatorEngine")

        # Initialize DatabaseManager
        # Pass PROJECT_ROOT so DatabaseManager can resolve relative db path correctly
        db_manager = DatabaseManager(config, logger_instance=db_logger, project_root_dir=PROJECT_ROOT)
        db_manager.connect() # This will also create tables if they don't exist

        data_fetch_status = {'fred': False, 'nyfed': False, 'yfinance_move': False}
        # Define unique columns for upsert operations
        macro_unique_cols = ['metric_date', 'metric_name', 'source_api']
        stock_unique_cols = ['price_date', 'security_id', 'source_api']

        global_log("\n--- 階段 1: 數據獲取 ---", "INFO", logger_name="MainApp.main_flow")

        # --- FRED Data Fetching ---
        fred_conn = FredConnector(config, logger_instance=fred_logger)
        fred_series_ids = config.get('target_metrics', {}).get('fred_series_ids', [])
        fred_data_df, fred_error_msg = fred_conn.fetch_data(series_ids=fred_series_ids, start_date=start_date_cfg, end_date=end_date_to_use)
        if fred_error_msg and (fred_data_df is None or fred_data_df.empty): # If error and no data, it's a failure
            global_log(f"FRED Data Fetching Error: {fred_error_msg}", "ERROR", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False
        elif fred_data_df is not None and not fred_data_df.empty:
            global_log(f"Fetched {len(fred_data_df)} FRED records.", "INFO", logger_name="MainApp.main_flow")
            if fred_error_msg: # Partial success with some errors
                 global_log(f"FRED Data Fetching completed with some errors: {fred_error_msg}", "WARNING", logger_name="MainApp.main_flow")
            db_manager.bulk_insert_or_replace('fact_macro_economic_data', fred_data_df, unique_cols=macro_unique_cols)
            data_fetch_status['fred'] = True
        else: # No data, no specific error message from connector (might have been logged internally)
            global_log("FRED Connector returned no data or an empty DataFrame.", "WARNING", logger_name="MainApp.main_flow")
            data_fetch_status['fred'] = False

        # --- NYFed Data Fetching ---
        nyfed_conn = NYFedConnector(config, logger_instance=nyfed_logger)
        nyfed_data_df, nyfed_error_msg = nyfed_conn.fetch_data()
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

        # --- YFinance Data Fetching ---
        yf_conn = YFinanceConnector(config, logger_instance=yf_logger)
        yfinance_tickers_list = config.get('target_metrics', {}).get('yfinance_tickers', [])
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

        global_log("\n--- 階段 2 & 3: 指標計算與市場簡報 ---", "INFO", logger_name="MainApp.main_flow")

        # Fetch data from DB for IndicatorEngine
        current_macro_data_for_engine = db_manager.fetch_all_for_engine('fact_macro_economic_data', start_date_cfg, end_date_to_use, date_column='metric_date')
        current_stock_data_for_engine = db_manager.fetch_all_for_engine('fact_stock_price', start_date_cfg, end_date_to_use, date_column='price_date')

        if (current_macro_data_for_engine is None or current_macro_data_for_engine.empty) and            (current_stock_data_for_engine is None or current_stock_data_for_engine.empty):
            global_log("IndicatorEngine: Insufficient data from DB (both macro and stock are empty/None). Skipping stress index calculation.", "ERROR", logger_name="MainApp.main_flow")
        else:
            # Ensure DataFrames are not None before use, default to empty if None
            current_macro_data_for_engine = current_macro_data_for_engine if current_macro_data_for_engine is not None else pd.DataFrame()
            current_stock_data_for_engine = current_stock_data_for_engine if current_stock_data_for_engine is not None else pd.DataFrame()

            move_data_for_engine = pd.DataFrame()
            if not current_stock_data_for_engine.empty and 'security_id' in current_stock_data_for_engine.columns:
                move_data_for_engine = current_stock_data_for_engine[current_stock_data_for_engine['security_id'] == '^MOVE']

            if move_data_for_engine.empty:
                global_log("IndicatorEngine: ^MOVE data not found in DB stock data or stock data was empty.", "WARNING", logger_name="MainApp.main_flow")

            engine_input_data = {'macro': current_macro_data_for_engine, 'move': move_data_for_engine}
            engine_params_from_config = config.get('indicator_engine_params', {})

            indicator_engine_instance = IndicatorEngine(engine_input_data, params=engine_params_from_config, logger_instance=engine_logger)
            stress_index_df = indicator_engine_instance.calculate_dealer_stress_index()

            if stress_index_df is None or stress_index_df.empty:
                global_log("Dealer Stress Index calculation resulted in no data or all NaN values.", "ERROR", logger_name="MainApp.main_flow")
            else:
                global_log(f"Dealer Stress Index calculated. Shape: {stress_index_df.shape}. Latest date: {stress_index_df.index[-1].strftime('%Y-%m-%d') if not stress_index_df.empty else 'N/A'}", "INFO", logger_name="MainApp.main_flow")
                global_log(f"Stress Index Tail:\n{stress_index_df.tail().to_string()}", "INFO", logger_name="MainApp.main_flow")

                # --- Iterating through each date in stress_index_df for AI Decision ---
                global_log(f"Starting AI decision generation for {len(stress_index_df)} historical dates.", "INFO", logger_name="MainApp.HistoricalLoop")

                ai_api_call_count = 0
                # Prepare AI Agent once if configured
                ai_agent = None
                ai_config = config.get('ai_service', {})
                api_key_env = ai_config.get('api_key_env')
                api_key = os.getenv(api_key_env) if api_key_env else None
                api_endpoint = ai_config.get('api_endpoint')
                default_model = ai_config.get('default_model')
                ai_max_retries = ai_config.get('max_retries', 3)
                ai_retry_delay = ai_config.get('retry_delay_seconds', 5)
                ai_call_delay = ai_config.get('api_call_delay_seconds', 1.0)

                if not api_key or not api_endpoint:
                    global_log(f"AI Service API key (env var: {api_key_env}) or endpoint ({api_endpoint}) is not configured. Skipping AI decision loop.", "WARNING", logger_name="MainApp.HistoricalLoop")
                else:
                    ai_agent = RemoteAIAgent(
                        api_key=api_key, api_endpoint=api_endpoint, default_model=default_model,
                        max_retries=ai_max_retries, retry_delay_seconds=ai_retry_delay, api_call_delay_seconds=ai_call_delay
                    )
                    global_log(f"RemoteAIAgent initialized for historical loop.", "INFO", logger_name="MainApp.HistoricalLoop")

                # Determine a start date for AI decisions to avoid processing very old data if stress index has a long history
                # For example, start from the 'start_date_cfg' or a specific fixed date like '2020-01-01'
                # This ensures we only make AI calls for the relevant period defined in the task.
                ai_decision_start_date = pd.to_datetime(start_date_cfg) # Use the overall fetch start date from config

                for decision_date, row_data in stress_index_df.iterrows():
                    # Ensure decision_date is timezone-naive if it's a Timestamp for comparison
                    current_decision_date_naive = pd.to_datetime(decision_date).tz_localize(None) if isinstance(decision_date, pd.Timestamp) and decision_date.tzinfo is not None else pd.to_datetime(decision_date)

                    if current_decision_date_naive < ai_decision_start_date:
                        global_log(f"Skipping AI decision for date {decision_date.strftime('%Y-%m-%d')} as it's before the configured AI decision start date {ai_decision_start_date.strftime('%Y-%m-%d')}.", "DEBUG", logger_name="MainApp.HistoricalLoop")
                        continue

                    briefing_date_str = decision_date.strftime('%Y-%m-%d')
                    current_stress_value = row_data['DealerStressIndex']

                    stress_level_desc = "N/A"
                    if pd.notna(current_stress_value):
                        threshold_moderate = engine_params_from_config.get('stress_threshold_moderate', 40)
                        threshold_high = engine_params_from_config.get('stress_threshold_high', 60)
                        threshold_extreme = engine_params_from_config.get('stress_threshold_extreme', 80)
                        if current_stress_value >= threshold_extreme: stress_level_desc = f"{current_stress_value:.2f} (極度緊張)"
                        elif current_stress_value >= threshold_high: stress_level_desc = f"{current_stress_value:.2f} (高度緊張)"
                        elif current_stress_value >= threshold_moderate: stress_level_desc = f"{current_stress_value:.2f} (中度緊張)"
                        else: stress_level_desc = f"{current_stress_value:.2f} (正常)"

                    stress_trend_desc = "N/A"
                    # For trend, we need to look at the previous day's stress index.
                    # stress_index_df is sorted by date. .diff() can be used.
                    stress_diff_series = stress_index_df['DealerStressIndex'].diff()
                    if decision_date in stress_diff_series.index:
                        change_in_stress = stress_diff_series.loc[decision_date]
                        if pd.notna(change_in_stress):
                            stress_trend_desc = "上升" if change_in_stress > 0.1 else ("下降" if change_in_stress < -0.1 else "穩定")

                    engine_prepared_full_df = indicator_engine_instance.df_prepared # This df contains all components
                    current_components_data = None
                    if engine_prepared_full_df is not None and not engine_prepared_full_df.empty:
                        if decision_date in engine_prepared_full_df.index:
                            current_components_data = engine_prepared_full_df.loc[decision_date]
                        # else: # If exact match fails, could try to find nearest, but for daily, it should match.
                            # global_log(f"Data for {briefing_date_str} not found in engine_prepared_full_df for briefing components.", "WARNING", logger_name="MainApp.HistoricalLoop")

                    def get_formatted_value(series_data, component_key, value_format="{:.2f}", not_available_str="N/A"):
                        if series_data is not None and component_key in series_data.index and pd.notna(series_data[component_key]):
                            val = series_data[component_key]
                            try: return value_format.format(val) if isinstance(val, (int, float)) and pd.notna(val) else str(val)
                            except (ValueError, TypeError): return str(val)
                        return not_available_str

                    move_value_str = get_formatted_value(current_components_data, '^MOVE')
                    spread_10y2y_raw = current_components_data['spread_10y2y'] if current_components_data is not None and 'spread_10y2y' in current_components_data else None
                    spread_10y2y_str = f"{(spread_10y2y_raw * 100):.2f} bps" if pd.notna(spread_10y2y_raw) else "N/A"
                    primary_dealer_pos_str = get_formatted_value(current_components_data, 'NYFED/PRIMARY_DEALER_NET_POSITION', value_format="{:,.0f}")
                    vix_value_str = get_formatted_value(current_components_data, 'FRED/VIXCLS')
                    sofr_dev_str = get_formatted_value(current_components_data, 'FRED/SOFR_Dev')

                    # Generate market briefing for this specific date (optional, can be skipped if only AI decision is needed)
                    # For now, we'll generate it to pass to AI
                    market_briefing_for_ai = {
                        "briefing_date": briefing_date_str,
                        "dealer_stress_index_value": current_stress_value if pd.notna(current_stress_value) else None,
                        "dealer_stress_index_level": stress_level_desc.split('(')[-1].replace(')', '').strip() if stress_level_desc != "N/A" else None,
                        "dealer_stress_index_trend": stress_trend_desc,
                        "key_indicators": {
                            "MOVE_Index": move_value_str, "10Y_2Y_Treasury_Spread": spread_10y2y_str,
                            "Primary_Dealer_Net_Positions_USD_Millions": primary_dealer_pos_str,
                            "VIX_Index": vix_value_str, "SOFR_Deviation": sofr_dev_str
                        }
                    }
                    # Log the last day's market briefing to console as before for compatibility with run_prototype.sh output expectation
                    if decision_date == stress_index_df.index[-1]:
                        global_log("\n--- 最新市場簡報 (Market Briefing - JSON) ---", "INFO", logger_name="MainApp.Briefing")
                        print("\n--- 最新市場簡報 (Market Briefing - JSON) ---")
                        # Construct the full briefing for the last day
                        full_latest_briefing = {
                            "briefing_date": briefing_date_str, "data_window_end_date": briefing_date_str,
                            "dealer_stress_index": {"current_value_description": stress_level_desc, "trend_approximation": stress_trend_desc},
                            "key_financial_components_latest": [
                                {"component_name": "MOVE Index (Bond Mkt Volatility)", "value_string": move_value_str},
                                {"component_name": "10Y-2Y Treasury Spread", "value_string": spread_10y2y_str},
                                {"component_name": "Primary Dealer Net Positions (Millions USD)", "value_string": primary_dealer_pos_str}
                            ],
                            "broader_market_context_latest": {"vix_index (Equity Mkt Volatility)": vix_value_str, "sofr_deviation_from_ma": sofr_dev_str},
                            "summary_narrative": (
                                f"市場壓力指數 ({briefing_date_str}): {stress_level_desc}. "
                                f"主要影響因素包括債券市場波動率 (MOVE Index: {move_value_str}) 及 "
                                f"10年期與2年期公債利差 ({spread_10y2y_str}). "
                                f"一級交易商淨持倉部位為 {primary_dealer_pos_str} 百萬美元。"
                            )
                        }
                        print(json.dumps(full_latest_briefing, indent=2, ensure_ascii=False))
                        global_log(json.dumps(full_latest_briefing, indent=2, ensure_ascii=False), "INFO", logger_name="MainApp.BriefingOutput")


                    if ai_agent: # If AI agent is configured and initialized
                        global_log(f"Requesting AI decision for date: {briefing_date_str}", "DEBUG", logger_name="MainApp.HistoricalLoop")
                        ai_decision_model_instance, raw_ai_response = ai_agent.get_decision(market_data=market_briefing_for_ai)
                        ai_api_call_count += 1

                        if ai_decision_model_instance:
                            decision_log_data = {
                                "decision_date": decision_date.date(), # Ensure it's a date object
                                "stress_index_value": current_stress_value if pd.notna(current_stress_value) else None,
                                "strategy_summary": ai_decision_model_instance.strategy_summary,
                                "key_factors": json.dumps(ai_decision_model_instance.key_factors, ensure_ascii=False),
                                "confidence_score": ai_decision_model_instance.confidence_score,
                                "raw_ai_response": raw_ai_response,
                                "decision_timestamp": datetime.now(timezone.utc)
                            }
                            ai_decision_log_df = pd.DataFrame([decision_log_data])
                            success_ai_log = db_manager.bulk_insert_or_replace('log_ai_decision', ai_decision_log_df, unique_cols=['decision_date'])
                            if success_ai_log:
                                global_log(f"AI decision for {briefing_date_str} logged. API Calls: {ai_api_call_count}", "INFO", logger_name="MainApp.HistoricalLoop")
                            else:
                                global_log(f"Failed to log AI decision for {briefing_date_str}.", "ERROR", logger_name="MainApp.HistoricalLoop")
                        else:
                            global_log(f"Failed to get valid AI decision for {briefing_date_str}. Raw AI response: {raw_ai_response}", "ERROR", logger_name="MainApp.HistoricalLoop")
                    else:
                        # This case is hit if AI is not configured. Log once or periodically.
                        if ai_api_call_count == 0: # Log this only once if AI is not configured
                             global_log("AI agent not configured, skipping AI decision logging for all historical dates.", "INFO", logger_name="MainApp.HistoricalLoop")
                        ai_api_call_count +=1 # Increment to prevent re-logging the message

                global_log(f"Finished historical AI decision loop. Total AI API calls attempted: {ai_api_call_count}", "INFO", logger_name="MainApp.HistoricalLoop")


    except FileNotFoundError as e_fnf: # Specifically for config loading
        err_msg_fnf = f"CRITICAL FAILURE: Configuration file not found: {e_fnf}. Application cannot start."
        print(err_msg_fnf) # Print to console as logger might not be fully up
        if global_log: global_log(err_msg_fnf, "CRITICAL", logger_name="MainApp.main_flow", exc_info=False)
        else: pre_init_logger.critical(err_msg_fnf, exc_info=False)
    except Exception as e_main_runtime:
        err_msg_runtime = f"主流程 main() 發生嚴重執行期錯誤: {e_main_runtime}"
        print(err_msg_runtime)
        if global_log: global_log(err_msg_runtime, "CRITICAL", logger_name="MainApp.main_flow", exc_info=True)
        else: pre_init_logger.critical(err_msg_runtime, exc_info=True)
    finally:
        # --- Database Disconnection (Commented out) ---
        # if 'db_manager' in locals() and hasattr(db_manager, 'conn') and db_manager.conn is not None:
        #     if not db_manager.conn.isclosed(): db_manager.disconnect()
        # else:
        # --- Database Disconnection ---
        if 'db_manager' in locals() and db_manager is not None: # Check if db_manager was instantiated
            db_manager.disconnect()
        else:
            global_log("DB Manager was not instantiated, skipping disconnect.", "DEBUG", logger_name="MainApp.main_flow")

        global_log("\n--- 端到端原型執行完畢 (Atomic Script Version) ---", "INFO", logger_name="MainApp.main_flow")

        # --- Clean up detailed file logger (market_briefing_log.txt) ---
        if detailed_run_log_handler is not None and 'root_logger_for_detailed' in locals(): # Ensure handler was created
            global_log(f"Removing detailed run log handler. Transcript saved to {DETAILED_LOG_FILENAME}", "INFO", logger_name="MainApp.Cleanup")
            if hasattr(locals().get('root_logger_for_detailed'), 'removeHandler'): # Check if logger has removeHandler
                 root_logger_for_detailed.removeHandler(detailed_run_log_handler)
            detailed_run_log_handler.close()

if __name__ == "__main__":
    # This initial global_log check is for the very unlikely case that imports failed so badly
    # that global_log wasn't even assigned its fallback lambda.
    if global_log is None:
        pre_init_logger.critical("global_log function was not assigned its fallback. Logging will be severely limited.")
        # Define an ultra-fallback if pre_init_logger itself is somehow problematic (highly unlikely)
        global_log = lambda msg, level="INFO", **kwargs: print(f"ULTRA_FALLBACK_LOG [{level.upper()}] {msg}")

    # The global application logger (timestamped file in api_test_logs) should have been initialized
    # during the import phase. If not, log_message will use the bootstrap logger.
    # The detailed_run_log_handler (for market_briefing_log.txt) is set up inside main().

    # A final check on global logger initialization path from the module.
    if global_log_file_path_imported:
        global_log(f"Confirmed global application log file from module: {global_log_file_path_imported}", "DEBUG", logger_name="MainApp.InitCheck")
    else:
        global_log("Global application log file path from module was not set. Bootstrap logger might be active for app logs.", "WARNING", logger_name="MainApp.InitCheck")

    main()
