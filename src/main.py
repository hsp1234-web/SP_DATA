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

        if (current_macro_data_for_engine is None or current_macro_data_for_engine.empty) and            (current_stock_data_for_engine is None or current_stock_data_for_engine.empty):
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

