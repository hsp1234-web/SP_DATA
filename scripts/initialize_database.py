import duckdb
import json
import logging
import yaml # Using PyYAML
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timezone # For prefilling timestamp
import re # For robust default value parsing

# 配置日誌
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("initialize_database_v2") # Changed logger name for clarity

# --- SQL DDL 定義 ---

SQL_DDL_FINANCIAL_DATA = """
CREATE SEQUENCE IF NOT EXISTS dim_security_internal_id_seq START 1;

CREATE TABLE IF NOT EXISTS dim_security (
    internal_id BIGINT PRIMARY KEY DEFAULT nextval('dim_security_internal_id_seq'),
    security_id VARCHAR UNIQUE NOT NULL,
    name VARCHAR,
    asset_class VARCHAR,
    exchange VARCHAR,
    currency VARCHAR,
    country VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    description TEXT,
    first_seen_date DATE,
    last_seen_date DATE,
    delisted_date DATE,
    source_api_info JSON,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp
);
CREATE INDEX IF NOT EXISTS idx_dim_security_security_id ON dim_security(security_id);
CREATE INDEX IF NOT EXISTS idx_dim_security_asset_class ON dim_security(asset_class);

CREATE TABLE IF NOT EXISTS fact_stock_price (
    price_date DATE NOT NULL,
    security_id VARCHAR NOT NULL,
    open_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    close_price DOUBLE,
    adj_close_price DOUBLE,
    volume BIGINT,
    turnover DOUBLE,
    dividends DOUBLE DEFAULT 0.0,
    stock_splits DOUBLE DEFAULT 1.0,
    vwap DOUBLE,
    transactions INTEGER,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (price_date, security_id)
);
CREATE INDEX IF NOT EXISTS idx_fact_stock_price_security_id_date ON fact_stock_price(security_id, price_date DESC);

CREATE TABLE IF NOT EXISTS dim_financial_metric (
    source_api VARCHAR NOT NULL,
    source_metric_name VARCHAR NOT NULL,
    canonical_metric_name VARCHAR NOT NULL,
    metric_description TEXT,
    metric_unit VARCHAR,
    statement_type_hint VARCHAR,
    is_growth_metric BOOLEAN DEFAULT FALSE,
    -- last_updated_in_db_timestamp: Added for consistency in prefill function
    last_updated_in_db_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (source_api, source_metric_name),
    UNIQUE (canonical_metric_name)
);
CREATE INDEX IF NOT EXISTS idx_dim_financial_metric_canonical_name ON dim_financial_metric(canonical_metric_name);

CREATE TABLE IF NOT EXISTS fact_financial_statement (
    security_id VARCHAR NOT NULL,
    fiscal_period VARCHAR NOT NULL,
    announcement_date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE,
    currency VARCHAR,
    report_date DATE NOT NULL,
    filing_date DATE,
    statement_type VARCHAR,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (security_id, fiscal_period, announcement_date, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_financial_statement_sec_period_ann_metric ON fact_financial_statement(security_id, fiscal_period, announcement_date, metric_name);
CREATE INDEX IF NOT EXISTS idx_fact_financial_statement_sec_metric_ann ON fact_financial_statement(security_id, metric_name, announcement_date DESC);

CREATE TABLE IF NOT EXISTS fact_macro_economic_data (
    metric_date DATE NOT NULL,
    metric_name VARCHAR NOT NULL,
    metric_value DOUBLE,
    frequency VARCHAR,
    unit VARCHAR,
    notes TEXT,
    source_api VARCHAR NOT NULL,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (metric_date, metric_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_macro_metric_name_date ON fact_macro_economic_data(metric_name, metric_date DESC);

CREATE TABLE IF NOT EXISTS fact_alternative_data (
    data_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    security_id VARCHAR,
    factor_name VARCHAR NOT NULL,
    factor_value_numeric DOUBLE,
    factor_value_text TEXT,
    factor_value_json JSON,
    source_calculation_id VARCHAR,
    data_snapshot_timestamp TIMESTAMP WITH TIME ZONE DEFAULT current_timestamp,
    PRIMARY KEY (data_timestamp, security_id, factor_name)
);
CREATE INDEX IF NOT EXISTS idx_fact_alt_data_sec_factor_time ON fact_alternative_data(security_id, factor_name, data_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fact_alt_data_factor_time ON fact_alternative_data(factor_name, data_timestamp DESC);
"""

SQL_DDL_AI_JUDGMENTS = """
CREATE TABLE IF NOT EXISTS ai_simulation_log (
    simulation_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
    market_briefing TEXT NOT NULL,
    ai_model_used VARCHAR NOT NULL,
    prompt_details TEXT,
    ai_raw_response TEXT,
    extracted_strategy JSON,
    extracted_factors JSON,
    processing_time_seconds DOUBLE,
    backtest_result_summary JSON
);
CREATE INDEX IF NOT EXISTS idx_ai_simulation_log_model_time ON ai_simulation_log(ai_model_used, simulation_timestamp DESC);
"""

# Default paths (can be overridden by config.yaml in a more advanced setup)
# Assuming this script is in project_root/scripts/
DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
FINANCIAL_DATA_DB_PATH = DEFAULT_DATA_DIR / "financial_data.duckdb"
AI_JUDGMENTS_DB_PATH = DEFAULT_DATA_DIR / "ai_historical_judgments.duckdb"


def prefill_dim_financial_metric(con: duckdb.DuckDBPyConnection):
    """預填充 dim_financial_metric 表的初始數據。"""
    logger.info("開始預填充 dim_financial_metric 表...")
    # (Content of prefill_dim_financial_metric from previous version, with minor adjustments)
    initial_metrics = [
        # Income Statement - FinMind (example, verify actual source_metric_name from FinMind SDK/API)
        {"source_api": "finmind", "source_metric_name": "營業收入", "canonical_metric_name": "revenue", "metric_description": "Total operating revenue", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "營業毛利（毛損）", "canonical_metric_name": "gross_profit", "metric_description": "Gross profit or loss", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "營業利益（損失）", "canonical_metric_name": "operating_income", "metric_description": "Operating income or loss", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "繼續營業單位稅前淨利（淨損）", "canonical_metric_name": "pretax_income", "metric_description": "Income before tax from continuing operations", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "本期淨利（淨損）歸屬於母公司業主", "canonical_metric_name": "net_income_parent", "metric_description": "Net income attributable to owners of parent", "metric_unit": "TWD", "statement_type_hint": "income_statement"},
        {"source_api": "finmind", "source_metric_name": "基本每股盈餘", "canonical_metric_name": "eps_basic", "metric_description": "Basic earnings per share", "metric_unit": "TWD", "statement_type_hint": "income_statement"},

        # Balance Sheet - FinMind (example)
        {"source_api": "finmind", "source_metric_name": "流動資產", "canonical_metric_name": "current_assets", "metric_description": "Total current assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "非流動資產", "canonical_metric_name": "non_current_assets", "metric_description": "Total non-current assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "資產總計", "canonical_metric_name": "total_assets", "metric_description": "Total assets", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "流動負債", "canonical_metric_name": "current_liabilities", "metric_description": "Total current liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "非流動負債", "canonical_metric_name": "non_current_liabilities", "metric_description": "Total non-current liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "負債總計", "canonical_metric_name": "total_liabilities", "metric_description": "Total liabilities", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "歸屬於母公司業主之權益合計", "canonical_metric_name": "equity_parent", "metric_description": "Equity attributable to owners of parent", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},
        {"source_api": "finmind", "source_metric_name": "權益總計", "canonical_metric_name": "total_equity", "metric_description": "Total equity", "metric_unit": "TWD", "statement_type_hint": "balance_sheet"},

        # Cash Flow - FinMind (example)
        {"source_api": "finmind", "source_metric_name": "營業活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_ops", "metric_description": "Net cash flow from operating activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "投資活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_inv", "metric_description": "Net cash flow from investing activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "籌資活動之淨現金流入（流出）", "canonical_metric_name": "net_cash_fin", "metric_description": "Net cash flow from financing activities", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},
        {"source_api": "finmind", "source_metric_name": "本期現金及約當現金增加（減少）數", "canonical_metric_name": "net_change_cash", "metric_description": "Net change in cash and cash equivalents", "metric_unit": "TWD", "statement_type_hint": "cash_flow_statement"},

        # Common Ratios (illustrative - source_metric_name would be how they are derived or named if from API)
        {"source_api": "calculated", "source_metric_name": "ROE", "canonical_metric_name": "roe", "metric_description": "Return on Equity", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "ROA", "canonical_metric_name": "roa", "metric_description": "Return on Assets", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "GrossProfitMargin", "canonical_metric_name": "gross_profit_margin", "metric_description": "Gross Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "OperatingProfitMargin", "canonical_metric_name": "operating_profit_margin", "metric_description": "Operating Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
        {"source_api": "calculated", "source_metric_name": "NetProfitMargin", "canonical_metric_name": "net_profit_margin", "metric_description": "Net Profit Margin", "metric_unit": "%", "statement_type_hint": "ratios"},
    ]
    now_utc = datetime.now(timezone.utc)
    data_to_insert = [
        (
            item["source_api"],
            item["source_metric_name"],
            item["canonical_metric_name"],
            item.get("metric_description"),
            item.get("metric_unit"),
            item.get("statement_type_hint"),
            item.get("is_growth_metric", False),
            now_utc
        ) for item in initial_metrics
    ]
    insert_sql = """
    INSERT INTO dim_financial_metric
        (source_api, source_metric_name, canonical_metric_name, metric_description, metric_unit, statement_type_hint, is_growth_metric, last_updated_in_db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (source_api, source_metric_name) DO UPDATE SET
        canonical_metric_name = excluded.canonical_metric_name,
        metric_description = excluded.metric_description,
        metric_unit = excluded.metric_unit,
        statement_type_hint = excluded.statement_type_hint,
        is_growth_metric = excluded.is_growth_metric,
        last_updated_in_db_timestamp = excluded.last_updated_in_db_timestamp
    WHERE dim_financial_metric.canonical_metric_name != excluded.canonical_metric_name
       OR dim_financial_metric.metric_description IS DISTINCT FROM excluded.metric_description
       OR dim_financial_metric.metric_unit IS DISTINCT FROM excluded.metric_unit
       OR dim_financial_metric.statement_type_hint IS DISTINCT FROM excluded.statement_type_hint
       OR dim_financial_metric.is_growth_metric IS DISTINCT FROM excluded.is_growth_metric;
    """
    # Also ensure canonical_metric_name uniqueness if a different source_api/source_metric_name maps to an existing canonical_metric_name.
    # This might need a separate UPSERT or a more complex ON CONFLICT for the UNIQUE constraint on canonical_metric_name.
    # For now, this handles updates if source_api/source_metric_name pair is the same.
    # A simpler approach for now if canonical_metric_name is UNIQUE and is the main key for lookup from other tables:
    # Make canonical_metric_name the primary key for prefill, and then map source to it.
    # The DDL has (source_api, source_metric_name) as PK and canonical_metric_name as UNIQUE.
    # The prefill logic should align. Let's assume we insert or ignore for canonical_metric_name conflict first,
    # then handle the source mapping.
    # Simpler prefill:
    # canonical_metric_name is UNIQUE. source_api + source_metric_name is PK.
    # This means one canonical_metric_name can only be defined once.
    # And one source_api/source_metric_name can only map to one canonical_metric_name.

    # Corrected prefill logic: Upsert based on canonical_metric_name, then map sources.
    # For simplicity, we'll just insert. If a canonical_metric_name is already defined by another source,
    # this insert might fail or be skipped depending on how we handle the UNIQUE constraint on canonical_metric_name.
    # The DDL has UNIQUE(canonical_metric_name).
    # Let's use ON CONFLICT (canonical_metric_name) DO NOTHING for the first pass of unique canonical names,
    # then handle source-specific mappings.
    #
    # A more robust prefill would be to:
    # 1. Insert unique canonical_metric_names if they don't exist.
    # 2. Separately manage the mapping from (source_api, source_metric_name) to canonical_metric_name,
    #    perhaps in a different table or ensure the (source_api, source_metric_name) PK + unique canonical_metric_name
    #    logic in dim_financial_metric handles all cases.
    #
    # Given the current DDL: (source_api, source_metric_name) is PK, canonical_metric_name is UNIQUE.
    # The insert should be on (source_api, source_metric_name) and update other fields.
    # The UNIQUE constraint on canonical_metric_name will prevent two different source metrics from mapping to the SAME canonical name if their source_api/source_metric_name are different.
    # This is generally desired.

    # The previous insert_sql was for a different PK. Correcting for (source_api, source_metric_name) PK.
    insert_sql_corrected = """
    INSERT INTO dim_financial_metric
        (source_api, source_metric_name, canonical_metric_name, metric_description, metric_unit, statement_type_hint, is_growth_metric, last_updated_in_db_timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT (source_api, source_metric_name) DO UPDATE SET
        canonical_metric_name = excluded.canonical_metric_name,
        metric_description = excluded.metric_description,
        metric_unit = excluded.metric_unit,
        statement_type_hint = excluded.statement_type_hint,
        is_growth_metric = excluded.is_growth_metric,
        last_updated_in_db_timestamp = excluded.last_updated_in_db_timestamp;
    """
    # This will still fail if a new (source_api, source_metric_name) tries to use an existing canonical_metric_name.
    # This is good as it forces unique canonical names.

    try:
        con.executemany(insert_sql_corrected, data_to_insert)
        logger.info(f"成功插入/更新 {len(data_to_insert)} 筆初始數據到 dim_financial_metric。")
    except duckdb.Error as e:
        logger.error(f"預填充 dim_financial_metric 時發生錯誤: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"預填充 dim_financial_metric 時發生未預期錯誤: {e}", exc_info=True)


def initialize_all_databases():
    """Initializes all necessary databases and their schemas."""
    logger.info("開始所有資料庫的初始化...")

    # Create data directory if it doesn't exist
    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"數據目錄 '{DEFAULT_DATA_DIR}' 已確認/創建。")

    # Initialize financial_data.duckdb
    logger.info(f"初始化資料庫: {FINANCIAL_DATA_DB_PATH}")
    con_financial = None
    try:
        con_financial = duckdb.connect(database=str(FINANCIAL_DATA_DB_PATH), read_only=False)
        logger.info(f"成功連接到/創建 {FINANCIAL_DATA_DB_PATH}")
        con_financial.execute(SQL_DDL_FINANCIAL_DATA)
        logger.info(f"已在 {FINANCIAL_DATA_DB_PATH} 中執行核心表和索引的 DDL。")
        prefill_dim_financial_metric(con_financial) # Prefill after table creation
        logger.info(f"{FINANCIAL_DATA_DB_PATH} 初始化完成。")
    except duckdb.Error as e:
        logger.error(f"初始化 {FINANCIAL_DATA_DB_PATH} 時發生 DuckDB 錯誤: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"初始化 {FINANCIAL_DATA_DB_PATH} 時發生未預期錯誤: {e}", exc_info=True)
    finally:
        if con_financial:
            con_financial.close()
            logger.info(f"與 {FINANCIAL_DATA_DB_PATH} 的連接已關閉。")

    # Initialize ai_historical_judgments.duckdb
    logger.info(f"初始化資料庫: {AI_JUDGMENTS_DB_PATH}")
    con_ai = None
    try:
        con_ai = duckdb.connect(database=str(AI_JUDGMENTS_DB_PATH), read_only=False)
        logger.info(f"成功連接到/創建 {AI_JUDGMENTS_DB_PATH}")
        con_ai.execute(SQL_DDL_AI_JUDGMENTS)
        logger.info(f"已在 {AI_JUDGMENTS_DB_PATH} 中執行核心表和索引的 DDL。")
        logger.info(f"{AI_JUDGMENTS_DB_PATH} 初始化完成。")
    except duckdb.Error as e:
        logger.error(f"初始化 {AI_JUDGMENTS_DB_PATH} 時發生 DuckDB 錯誤: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"初始化 {AI_JUDGMENTS_DB_PATH} 時發生未預期錯誤: {e}", exc_info=True)
    finally:
        if con_ai:
            con_ai.close()
            logger.info(f"與 {AI_JUDGMENTS_DB_PATH} 的連接已關閉。")

    logger.info("所有資料庫初始化流程結束。")


if __name__ == "__main__":
    # Removed config and schema loading from here as DDLs are now hardcoded for this script's purpose
    # The config for DB paths could still be used if desired, but for simplicity, using defaults here.
    initialize_all_databases()
