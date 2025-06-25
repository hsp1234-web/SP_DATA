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
logger = logging.getLogger("initialize_database")

# 數據類型映射 (可以根據需要擴展)
# schemas.json type -> DuckDB SQL type
TYPE_MAPPING = {
    "VARCHAR": "VARCHAR", # TEXT is an alias for VARCHAR in DuckDB
    "TEXT": "VARCHAR",
    "REAL": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP WITH TIME ZONE", # TIMESTAMPTZ
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMP WITH TIME ZONE",
    "INTEGER": "BIGINT",
    "BIGINT": "BIGINT",
    "JSON": "JSON",
    "UUID": "UUID",
    "BOOLEAN": "BOOLEAN"
    # Add other types as needed
}

def load_config(config_path: Path) -> Dict[str, Any]:
    """加載 YAML 配置文件。"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        logger.info(f"成功從 {config_path} 加載配置。")
        return config_data
    except FileNotFoundError:
        logger.error(f"配置文件 {config_path} 未找到。")
        raise
    except yaml.YAMLError as e:
        logger.error(f"解析配置文件 {config_path} 時出錯: {e}")
        raise

def load_schemas(schemas_path: Path) -> Dict[str, Any]:
    """加載 JSON schema 文件。"""
    try:
        with open(schemas_path, 'r', encoding='utf-8') as f:
            schemas_data = json.load(f)
        logger.info(f"成功從 {schemas_path} 加載 schema。")
        return schemas_data
    except FileNotFoundError:
        logger.error(f"Schema 文件 {schemas_path} 未找到。")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"解析 schema 文件 {schemas_path} 時出錯: {e}")
        raise

def build_create_table_sql(table_name: str, table_schema: Dict[str, Any]) -> str:
    """根據 schema 構建 CREATE TABLE SQL 語句。"""
    columns_sql_parts = []
    for col_def in table_schema.get("columns", []):
        col_name = f'"{col_def["name"]}"'
        col_type_key = col_def["type"].upper()
        col_type_sql = TYPE_MAPPING.get(col_type_key, "VARCHAR")

        col_sql_part = f"{col_name} {col_type_sql}"

        constraints_value = col_def.get("constraints", "")

        # Handle DEFAULT constraint
        if "DEFAULT" in constraints_value.upper():
            # Regex to find DEFAULT value, handling simple numbers and quoted strings
            # This regex assumes default value is the first part after "DEFAULT " and before any other constraint like "NOT NULL"
            match = re.search(r"DEFAULT\s+((?:'(?:[^']|'')*'|[^'\s]+))", constraints_value, re.IGNORECASE)
            if match:
                actual_default_value = match.group(1)
                col_sql_part += f" DEFAULT {actual_default_value}"

        # Handle NOT NULL constraint
        if "NOT NULL" in constraints_value.upper():
            col_sql_part += " NOT NULL"

        columns_sql_parts.append(col_sql_part)

    # 主鍵約束
    primary_keys = table_schema.get("primary_keys", [])
    if primary_keys:
        pk_cols = ', '.join([f'"{pk}"' for pk in primary_keys])
        columns_sql_parts.append(f"PRIMARY KEY ({pk_cols})")

    # 唯一約束
    unique_constraints = table_schema.get("unique_constraints", [])
    for uc in unique_constraints:
        uc_name = f'"{uc["name"]}"'
        uc_cols = ', '.join([f'"{col}"' for col in uc["columns"]])
        columns_sql_parts.append(f"CONSTRAINT {uc_name} UNIQUE ({uc_cols})")

    sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" (\n  " + ",\n  ".join(columns_sql_parts) + "\n);"
    return sql

def build_create_index_sql(table_name: str, index_def: Dict[str, Any]) -> str:
    """根據 schema 構建 CREATE INDEX SQL 語句。"""
    index_name = f'"{index_def["name"]}"'
    idx_cols = ', '.join([f'"{col}"' for col in index_def["columns"]])
    sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON \"{table_name}\" ({idx_cols});"
    return sql

def initialize_database(config_path_str: str, schemas_path_str: str):
    """
    初始化 DuckDB 資料庫：連接、創建表、主鍵、唯一約束和索引。
    """
    # Determine project root assuming this script is in project_root/scripts/
    # This allows the script to be called from any directory.
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    config_path = project_root / config_path_str
    schemas_path = project_root / schemas_path_str

    logger.info("開始資料庫初始化...")

    config = load_config(config_path)
    schemas = load_schemas(schemas_path)

    db_file_path_str = config.get("project", {}).get("database_path")
    if not db_file_path_str:
        logger.error("配置中未找到 project.database_path。")
        return

    # If db_file_path_str is absolute, Path will handle it correctly.
    # If it's relative, it should be relative to the project root.
    if not Path(db_file_path_str).is_absolute():
        db_file_path = project_root / db_file_path_str
    else:
        db_file_path = Path(db_file_path_str)

    # 確保資料庫文件的父目錄存在
    db_file_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"將連接/創建 DuckDB 資料庫於: {db_file_path}")

    con = None # Initialize con to None
    try:
        con = duckdb.connect(database=str(db_file_path), read_only=False)
        logger.info("成功連接到 DuckDB。")

        for table_name, table_schema in schemas.items():
            logger.info(f"準備處理表: {table_name}")

            # 1. 創建表
            create_table_sql = build_create_table_sql(table_name, table_schema)
            logger.debug(f"將執行 CREATE TABLE SQL:\n{create_table_sql}")
            try:
                con.execute(create_table_sql)
                logger.info(f"表 \"{table_name}\" 已成功創建或已存在。")
            except duckdb.Error as e:
                logger.error(f"創建表 \"{table_name}\" 時發生錯誤: {e}")
                continue # 跳過此表的索引創建

            # 2. 創建索引
            indexes = table_schema.get("indexes", [])
            if indexes:
                logger.info(f"為表 \"{table_name}\" 創建索引...")
                for index_def in indexes:
                    try: # Add try-except for individual index creation
                        create_index_sql = build_create_index_sql(table_name, index_def)
                        logger.debug(f"將執行 CREATE INDEX SQL:\n{create_index_sql}")
                        con.execute(create_index_sql)
                        logger.info(f"索引 \"{index_def['name']}\" 已成功創建或已存在於表 \"{table_name}\"。")
                    except duckdb.Error as e:
                        logger.error(f"為表 \"{table_name}\" 創建索引 \"{index_def['name']}\" 時發生錯誤: {e}")
            else:
                logger.info(f"表 \"{table_name}\" 沒有定義索引。")

        logger.info("所有表和索引處理完畢。")

    except duckdb.Error as e:
        logger.error(f"連接到 DuckDB 或執行操作時發生錯誤: {e}")
    except Exception as e:
        logger.error(f"發生未預期的錯誤: {e}", exc_info=True)
    finally:
        if con: # Check if con was successfully assigned
            con.close()
            logger.info("DuckDB 連接已關閉。")
        logger.info("資料庫初始化完成。")

if __name__ == "__main__":
    default_config_path = "config/config.yaml"
    default_schemas_path = "config/schemas.json"

    initialize_database(default_config_path, default_schemas_path)
def prefill_dim_financial_metric(con: duckdb.DuckDBPyConnection):
    """預填充 dim_financial_metric 表的初始數據。"""
    logger.info("開始預填充 dim_financial_metric 表...")

    initial_metrics = [
        {
            "source_name": "finmind", "source_metric_name": "營業收入",
            "canonical_metric_name": "revenue", "metric_description": "Total operating revenue"
        },
        {
            "source_name": "finmind", "source_metric_name": "營業毛利（毛損）",
            "canonical_metric_name": "gross_profit", "metric_description": "Gross profit or loss"
        },
        {
            "source_name": "finmind", "source_metric_name": "營業利益（損失）",
            "canonical_metric_name": "operating_income", "metric_description": "Operating income or loss"
        },
        {
            "source_name": "finmind", "source_metric_name": "稅前淨利（淨損）", # FinMind 實際為 "綜合損益總額" or "繼續營業單位稅前淨利（淨損）"
            "canonical_metric_name": "pretax_income", "metric_description": "Income before tax"
        }, # Source name for '稅前淨利（淨損）' might need verification from actual FinMind output for income statements
        {
            "source_name": "finmind", "source_metric_name": "本期淨利（淨損）", # FinMind 實際為 "本期淨利（淨損）歸屬於母公司業主"
            "canonical_metric_name": "net_income", "metric_description": "Net income for the period (attributable to parent)"
        },
        {
            "source_name": "finmind", "source_metric_name": "基本每股盈餘",
            "canonical_metric_name": "eps", "metric_description": "Basic earnings per share"
        },
        # Assets - Balance Sheet
        {
            "source_name": "finmind", "source_metric_name": "流動資產", # Example, actual name might vary
            "canonical_metric_name": "current_assets", "metric_description": "Total current assets"
        },
        {
            "source_name": "finmind", "source_metric_name": "非流動資產", # Example
            "canonical_metric_name": "non_current_assets", "metric_description": "Total non-current assets"
        },
        {
            "source_name": "finmind", "source_metric_name": "資產總計",  # Or "資產總額"
            "canonical_metric_name": "total_assets", "metric_description": "Total assets"
        },
        # Liabilities - Balance Sheet
        {
            "source_name": "finmind", "source_metric_name": "流動負債", # Example
            "canonical_metric_name": "current_liabilities", "metric_description": "Total current liabilities"
        },
        {
            "source_name": "finmind", "source_metric_name": "非流動負債", # Example
            "canonical_metric_name": "non_current_liabilities", "metric_description": "Total non-current liabilities"
        },
        {
            "source_name": "finmind", "source_metric_name": "負債總計", # Or "負債總額"
            "canonical_metric_name": "total_liabilities", "metric_description": "Total liabilities"
        },
        # Equity - Balance Sheet
        {
            "source_name": "finmind", "source_metric_name": "歸屬於母公司業主之權益合計", # Common full name
            "canonical_metric_name": "equity_attributable_to_owners_of_parent", "metric_description": "Equity attributable to owners of parent"
        },
        {
            "source_name": "finmind", "source_metric_name": "權益總計", # Or "權益總額"
            "canonical_metric_name": "total_equity", "metric_description": "Total equity"
        },
        # Cash Flow Statement
        {
            "source_name": "finmind", "source_metric_name": "營業活動之淨現金流入(流出)", # Verify actual FinMind name
            "canonical_metric_name": "net_cash_flow_from_operating_activities", "metric_description": "Net cash flow from operating activities"
        },
        {
            "source_name": "finmind", "source_metric_name": "投資活動之淨現金流入(流出)", # Verify actual FinMind name
            "canonical_metric_name": "net_cash_flow_from_investing_activities", "metric_description": "Net cash flow from investing activities"
        },
        {
            "source_name": "finmind", "source_metric_name": "籌資活動之淨現金流入(流出)", # Verify actual FinMind name
            "canonical_metric_name": "net_cash_flow_from_financing_activities", "metric_description": "Net cash flow from financing activities"
        },
        {
            "source_name": "finmind", "source_metric_name": "本期現金及約當現金增加(減少)數", # Verify actual FinMind name
            "canonical_metric_name": "net_change_in_cash_and_cash_equivalents", "metric_description": "Net change in cash and cash equivalents"
        },
        # Chip Data - Institutional Investors (examples, assuming transform_chip_data_to_canonical will use these as metric_name)
        # These are illustrative; the actual metric_names will depend on the melt logic in transform_chip_data_to_canonical
        {
            "source_name": "finmind", "source_metric_name": "Foreign_Investor_buy_shares", # This would be a generated metric_name after melt
            "canonical_metric_name": "institutional_foreign_investor_buy_shares", "metric_description": "Foreign Investor Buy Shares"
        },
        {
            "source_name": "finmind", "source_metric_name": "Foreign_Investor_sell_shares",
            "canonical_metric_name": "institutional_foreign_investor_sell_shares", "metric_description": "Foreign Investor Sell Shares"
        },
        {
            "source_name": "finmind", "source_metric_name": "Foreign_Investor_net_shares",
            "canonical_metric_name": "institutional_foreign_investor_net_shares", "metric_description": "Foreign Investor Net Buy/Sell Shares"
        },
         # Similar entries for Investment_Trust, Dealer_Proprietary, Dealer_Hedging for buy, sell, net_shares

        # Chip Data - Margin Trading (examples)
        {
            "source_name": "finmind", "source_metric_name": "margin_purchase_balance", # Assuming transform converts original to this
            "canonical_metric_name": "margin_purchase_balance_shares", "metric_description": "Margin Purchase Balance (Shares)"
        },
        {
            "source_name": "finmind", "source_metric_name": "short_sale_balance", # Assuming transform converts original to this
            "canonical_metric_name": "short_sale_balance_shares", "metric_description": "Short Sale Balance (Shares)"
        },

        # Chip Data - Shareholding (examples)
        {
            "source_name": "finmind", "source_metric_name": "foreign_investment_shares_ratio", # Assuming transform converts original to this
            "canonical_metric_name": "foreign_ownership_ratio", "metric_description": "Foreign Investment Ownership Ratio (%)"
        },

        # Event Data - Monthly Revenue
        {
            "source_name": "finmind", "source_metric_name": "monthly_revenue", # Assuming 'revenue' column from API is mapped to this in transform
            "canonical_metric_name": "monthly_revenue_twd", "metric_description": "Monthly Revenue (TWD)"
        },
        # Event Data - Dividends (examples)
        {
            "source_name": "finmind", "source_metric_name": "cash_earnings_distribution", # Assuming transform maps original to this
            "canonical_metric_name": "cash_dividend_per_share_from_earnings", "metric_description": "Cash Dividend Per Share from Earnings (TWD)"
        },
        {
            "source_name": "finmind", "source_metric_name": "stock_earnings_distribution", # Assuming transform maps original to this
            "canonical_metric_name": "stock_dividend_ratio_from_earnings", "metric_description": "Stock Dividend Ratio from Earnings (%)"
        }
        # TODO: Add more specific mappings as transform logic for chip and event data is finalized
    ]

    now_utc = datetime.now(timezone.utc)

    # Prepare data for executemany
    data_to_insert = [
        (
            item["canonical_metric_name"],
            item["source_name"],
            item["source_metric_name"],
            item["metric_description"],
            now_utc
        ) for item in initial_metrics
    ]

    # DuckDB's ON CONFLICT syntax for primary key 'canonical_metric_name'
    # For a table with a single primary key 'canonical_metric_name',
    # ON CONFLICT (canonical_metric_name) DO NOTHING ensures idempotency.
    # If other columns should be updated on conflict, use DO UPDATE SET ...
    insert_sql = """
    INSERT INTO dim_financial_metric
        (canonical_metric_name, source_name, source_metric_name, metric_description, last_updated_in_db_timestamp)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT (canonical_metric_name) DO UPDATE SET
        source_name = excluded.source_name,
        source_metric_name = excluded.source_metric_name,
        metric_description = excluded.metric_description,
        last_updated_in_db_timestamp = excluded.last_updated_in_db_timestamp;
    """
    # Using DO UPDATE to ensure existing records are updated with potentially new descriptions or source mappings.

    try:
        con.executemany(insert_sql, data_to_insert)
        logger.info(f"成功插入/更新 {len(data_to_insert)} 筆初始數據到 dim_financial_metric。")
    except duckdb.Error as e:
        logger.error(f"預填充 dim_financial_metric 時發生錯誤: {e}", exc_info=True)
    except Exception as e: # Catch any other unexpected error
        logger.error(f"預填充 dim_financial_metric 時發生未預期錯誤: {e}", exc_info=True)


def initialize_database(config_path_str: str, schemas_path_str: str):
    # ... (previous content of initialize_database) ...
    try:
        con = duckdb.connect(database=str(db_file_path), read_only=False)
        logger.info("成功連接到 DuckDB。")

        for table_name, table_schema in schemas.items():
            # ... (table and index creation logic) ...
            pass # Placeholder for existing loop

        logger.info("所有表和索引處理完畢。")

        # **新增：預填充 dim_financial_metric 表**
        if "dim_financial_metric" in schemas: # Ensure the table was defined and potentially created
            prefill_dim_financial_metric(con)
        else:
            logger.warning("Schema 'dim_financial_metric' 未在 schemas.json 中定義，跳過預填充。")

    except duckdb.Error as e:
        logger.error(f"連接到 DuckDB 或執行操作時發生錯誤: {e}")
    except Exception as e:
        logger.error(f"發生未預期的錯誤: {e}", exc_info=True)
    finally:
        if con:
            con.close()
            logger.info("DuckDB 連接已關閉。")
        logger.info("資料庫初始化完成。")

if __name__ == "__main__":
    default_config_path = "config/config.yaml"
    default_schemas_path = "config/schemas.json"

    initialize_database(default_config_path, default_schemas_path)
```
**在生成程式碼時，我做了一些微調和改進**：
*   **`build_create_table_sql` 中對 `DEFAULT` 約束的處理**：使其能更通用地處理 `DEFAULT value` 和 `NOT NULL DEFAULT value` 兩種情況，並將 `DEFAULT` 子句放在類型之後、`NOT NULL` 之前，這更符合標準 SQL 語法。
*   **路徑解析**：在 `initialize_database` 函數中，修改了 `project_root` 的確定方式，使其更可靠，無論腳本從何處調用。同時，處理了 `database_path` 可能是絕對路徑或相對路徑的情況。
*   **`con` 的初始化和 `finally` 塊**：將 `con` 初始化為 `None`，並在 `finally` 塊中檢查 `con` 是否已成功賦值後再嘗試關閉，以避免在連接失敗時 `con.close()` 出錯。
*   **索引創建的錯誤處理**：為單個索引的創建也添加了 `try-except`，這樣一個索引創建失敗不會阻止其他索引的嘗試。

這個腳本現在應該能夠根據我們詳細定義的 `schemas.json` 文件來正確初始化 DuckDB 資料庫了。
