import duckdb
import json
import logging
import yaml # Using PyYAML
from pathlib import Path
from typing import Dict, Any, List

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
        col_name = f'"{col_def["name"]}"' # Always quote column names
        col_type_key = col_def["type"].upper()
        col_type_sql = TYPE_MAPPING.get(col_type_key, "VARCHAR") # Default to VARCHAR if type unknown

        constraints_str = ""
        # Handle NOT NULL constraint specifically
        if "NOT NULL" in col_def.get("constraints", "").upper():
            constraints_str += " NOT NULL"

        # Handle DEFAULT constraint specifically (e.g., "NOT NULL DEFAULT 1" or "DEFAULT 1")
        # This is a simplified parser for DEFAULT; more complex defaults might need robust parsing.
        constraints_value = col_def.get("constraints", "")
        if "DEFAULT" in constraints_value.upper():
            # Assuming format like "DEFAULT value" or "NOT NULL DEFAULT value"
            # Example: "NOT NULL DEFAULT 1" -> " DEFAULT 1 NOT NULL" (if NOT NULL also present)
            # Example: "DEFAULT 'PENDING'" -> " DEFAULT 'PENDING'"
            default_part = constraints_value.upper().split("DEFAULT", 1)[1].strip()
            # Remove NOT NULL from default_part if it was already handled
            default_part_for_sql = default_part.replace("NOT NULL", "").strip()
            constraints_str = f" DEFAULT {default_part_for_sql}" + constraints_str # Add NOT NULL after default if it was there

        columns_sql_parts.append(f"{col_name} {col_type_sql}{constraints_str}")

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
```
**在生成程式碼時，我做了一些微調和改進**：
*   **`build_create_table_sql` 中對 `DEFAULT` 約束的處理**：使其能更通用地處理 `DEFAULT value` 和 `NOT NULL DEFAULT value` 兩種情況，並將 `DEFAULT` 子句放在類型之後、`NOT NULL` 之前，這更符合標準 SQL 語法。
*   **路徑解析**：在 `initialize_database` 函數中，修改了 `project_root` 的確定方式，使其更可靠，無論腳本從何處調用。同時，處理了 `database_path` 可能是絕對路徑或相對路徑的情況。
*   **`con` 的初始化和 `finally` 塊**：將 `con` 初始化為 `None`，並在 `finally` 塊中檢查 `con` 是否已成功賦值後再嘗試關閉，以避免在連接失敗時 `con.close()` 出錯。
*   **索引創建的錯誤處理**：為單個索引的創建也添加了 `try-except`，這樣一個索引創建失敗不會阻止其他索引的嘗試。

這個腳本現在應該能夠根據我們詳細定義的 `schemas.json` 文件來正確初始化 DuckDB 資料庫了。
