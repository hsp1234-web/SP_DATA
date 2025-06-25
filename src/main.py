import yaml
import pandas as pd
import json
from datetime import datetime, date
import os
import sys

# 為了在本地正確運行，將 src 目錄加入 Python 路徑
# 在標準的套件安裝或 IDE 中，這通常不是必要的
# 但對於直接運行腳本的場景，這是一個可靠的解決方案
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.connectors.fred_connector import FredConnector
from src.connectors.nyfed_connector import NYFedConnector
from src.connectors.yfinance_connector import YFinanceConnector
from src.database.database_manager import DatabaseManager
from src.engine.indicator_engine import IndicatorEngine
from src.scripts.initialize_database import initialize_database

def load_config(path="src/configs/project_config.yaml"):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def format_briefing(stress_index_df, prepared_data):
    if stress_index_df.empty:
        return {"error": "無法生成市場簡報，壓力指數無數據。"}

    latest = stress_index_df.iloc[-1]
    latest_date = latest.name.strftime('%Y-%m-%d')
    latest_stress_value = latest['DealerStressIndex']

    # 判斷壓力等級
    if latest_stress_value > 0.8:
        rank = f"{latest_stress_value:.2%} (極度緊張)"
    elif latest_stress_value > 0.6:
        rank = f"{latest_stress_value:.2%} (高度緊張)"
    else:
        rank = f"{latest_stress_value:.2%} (正常)"

    # 獲取最新成分數據
    latest_full_data = prepared_data.loc[latest.name]

    briefing = {
        "briefing_date": latest_date,
        "dealer_stress_index": {
            "current_value": round(latest_stress_value, 4),
            "current_percentile_rank": rank,
        },
        "key_components": [
            {"name": "MOVE Index", "value": round(latest_full_data.get('move_index', 0), 2)},
            {"name": "10Y-2Y Spread", "value": round(latest_full_data.get('spread_10y2y', 0), 4)},
            {"name": "Primary Dealer Positions ($M)", "value": int(latest_full_data.get('primary_dealer_position', 0))},
            {"name": "VIX Index", "value": round(latest_full_data.get('vix_index', 0), 2)},
        ],
        "summary_narrative": "摘要：市場壓力指標已生成，請結合各項指標進行綜合判斷。"
    }
    return briefing

def main():
    print("--- 開始執行端到端金融數據與壓力指標原型 ---")
    config = load_config()

    # 1. 初始化資料庫
    db_path = os.path.join(config['database']['db_directory'], config['database']['financial_data_db_name'])
    initialize_database(db_path)

    db_manager = DatabaseManager(config)
    db_manager.connect()

    start_date = config['data_fetch_range']['start_date']
    end_date = config['data_fetch_range']['end_date'] or datetime.now().strftime('%Y-%m-%d')

    try:
        # 2. 數據獲取與儲存
        print("\n--- 階段 1: 數據獲取與儲存 ---")
        # FRED
        fred_connector = FredConnector(config)
        fred_data, fred_err = fred_connector.fetch_data(config['target_metrics']['fred_series_ids'], start_date, end_date)
        if fred_err: print(f"FRED 錯誤: {fred_err}")
        else: db_manager.write_data('fact_macro_economic_data', fred_data, is_incremental=False)

        # NYFed
        nyfed_connector = NYFedConnector(config)
        nyfed_data, nyfed_err = nyfed_connector.fetch_data()
        if nyfed_err: print(f"NYFed 錯誤: {nyfed_err}")
        else: db_manager.write_data('fact_macro_economic_data', nyfed_data, is_incremental=True)

        # yfinance
        yf_connector = YFinanceConnector(config)
        yf_data, yf_err = yf_connector.fetch_data(config['target_metrics']['yfinance_tickers'], start_date, end_date)
        if yf_err: print(f"yfinance 錯誤: {yf_err}")
        else: db_manager.write_data('fact_stock_price', yf_data)

        # 3. 指標計算
        print("\n--- 階段 2: 指標計算 ---")
        macro_data = db_manager.fetch_data("SELECT * FROM fact_macro_economic_data")
        move_data = db_manager.fetch_data("SELECT * FROM fact_stock_price WHERE security_id = '^MOVE'")

        if macro_data is not None and move_data is not None:
            engine = IndicatorEngine(config['indicator_engine_params'], {'macro': macro_data, 'move': move_data})
            stress_index_df = engine.calculate_dealer_stress_index()

            # 4. 生成並打印市場簡報
            print("\n--- 階段 3: 生成市場簡報 ---")
            if stress_index_df is not None and not stress_index_df.empty:
                prepared_data_for_briefing = engine._prepare_data() # 獲取用於簡報的寬表數據
                briefing = format_briefing(stress_index_df, prepared_data_for_briefing)
                print("\n--- 最終市場簡報 (JSON) ---")
                print(json.dumps(briefing, indent=4, ensure_ascii=False))
            else:
                print("\n錯誤: 無法計算壓力指數，無法生成簡報。")
        else:
            print("\n錯誤: 從資料庫獲取數據失敗，無法進行計算。")

    finally:
        db_manager.disconnect()
        print("\n--- 執行完畢 ---")

if __name__ == "__main__":
    main()
