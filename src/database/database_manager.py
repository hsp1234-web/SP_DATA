import duckdb
import pandas as pd
from typing import Dict, Any, Optional, List
import logging
from pathlib import Path
import os

class DatabaseManager:
    """
    管理與 DuckDB 資料庫的連接和操作。
    """
    def __init__(self, config: Dict[str, Any], logger_instance: Optional[logging.Logger] = None, project_root_dir: Optional[str] = None):
        if logger_instance:
            self.logger = logger_instance
        else:
            self.logger = logging.getLogger(f"project_logger.{self.__class__.__name__}")
            if not self.logger.handlers and not logging.getLogger().hasHandlers():
                self.logger.addHandler(logging.NullHandler())
                self.logger.debug(f"Logger for {self.__class__.__name__} configured with NullHandler for atomic script.")

        self.db_config = config.get('database', {})
        db_path_str = self.db_config.get('path', 'data/default_financial_data.duckdb')

        if project_root_dir:
            self.db_file = Path(project_root_dir) / db_path_str
        else:
            self.db_file = Path(db_path_str)
            self.logger.warning(f"project_root_dir not provided to DatabaseManager. Database path resolved to: {self.db_file.resolve()}")

        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self.logger.info(f"DatabaseManager initialized. DB file target: {self.db_file.resolve()}")

    def connect(self):
        """建立與 DuckDB 資料庫的連接。"""
        if self.conn is not None:
            try:
                self.conn.execute("SELECT 1")
                self.logger.info("Database connection already active and valid.")
                return
            except Exception as e:
                self.logger.warning(f"Existing connection object found but it's not usable ({e}). Will try to reconnect.")
                self.conn = None

        try:
            self.db_file.parent.mkdir(parents=True, exist_ok=True)
            self.conn = duckdb.connect(database=str(self.db_file), read_only=False)
            self.logger.info(f"Successfully connected to DuckDB database: {self.db_file.resolve()}")
            self._create_tables_if_not_exist()
        except Exception as e:
            self.logger.critical(f"Failed to connect to DuckDB database at {self.db_file.resolve()}: {e}", exc_info=True)
            self.conn = None
            raise

    def disconnect(self):
        """關閉資料庫連接。"""
        if self.conn is not None:
            try:
                self.conn.close()
                self.logger.info(f"Disconnected from DuckDB database: {self.db_file.resolve()}")
            except Exception as e:
                self.logger.error(f"Error while closing DuckDB connection: {e}", exc_info=True)
        else:
            self.logger.info("Database connection already None or not established.")
        self.conn = None


    def _create_tables_if_not_exist(self):
        """如果表不存在，則創建它們。"""
        if self.conn is None:
            self.logger.error("Cannot create tables: Database connection is None.")
            return

        try:
            self.logger.info("Ensuring tables exist (will not drop if already present)...")
            # self.conn.execute("DROP TABLE IF EXISTS fact_macro_economic_data;") # Keep for re-creation
            # self.conn.execute("DROP TABLE IF EXISTS fact_stock_price;")
            # self.conn.execute("DROP TABLE IF EXISTS log_ai_decision;")
            # self.logger.info("Old tables (if any) dropped for fresh schema.")


            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_macro_economic_data (
                    metric_date DATE,
                    metric_name VARCHAR,
                    metric_value DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (metric_date, metric_name, source_api)
                );
            """)
            self.logger.info("Table 'fact_macro_economic_data' checked/created.")

            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fact_stock_price (
                    price_date DATE,
                    security_id VARCHAR,
                    open_price DOUBLE,
                    high_price DOUBLE,
                    low_price DOUBLE,
                    close_price DOUBLE,
                    adj_close_price DOUBLE,
                    volume BIGINT,
                    dividends DOUBLE,
                    stock_splits DOUBLE,
                    source_api VARCHAR,
                    data_snapshot_timestamp TIMESTAMP,
                    PRIMARY KEY (price_date, security_id, source_api)
                );
            """)
            self.logger.info("Table 'fact_stock_price' checked/created.")

            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS log_ai_decision (
                    simulation_timestamp TIMESTAMP,
                    market_brief_json TEXT,
                    ai_response_text TEXT,
                    strategy_summary TEXT,
                    key_factors TEXT,
                    PRIMARY KEY (simulation_timestamp)
                );
            """)
            self.logger.info("Table 'log_ai_decision' checked/created.")

        except Exception as e:
            self.logger.error(f"Error creating tables: {e}", exc_info=True)

    def bulk_insert_or_replace(self, table_name: str, df: pd.DataFrame, unique_cols: List[str]):
        if self.conn is None:
            self.logger.error(f"Cannot insert into {table_name}: Database connection is None.")
            return False
        if df.empty:
            self.logger.info(f"DataFrame for table {table_name} is empty. Nothing to insert.")
            return True

        self.logger.debug(f"Attempting to bulk insert/replace into {table_name}, {len(df)} rows. Unique cols: {unique_cols}")

        try:
            temp_table_name = f"temp_{table_name}_{os.urandom(4).hex()}"
            self.conn.register(temp_table_name, df)

            if not unique_cols:
                raise ValueError("unique_cols must be provided for upsert operation.")

            conflict_target = ", ".join(unique_cols)
            update_cols = [col for col in df.columns if col not in unique_cols]

            if not update_cols:
                 sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO NOTHING;"
                 self.logger.debug(f"Executing SQL (INSERT OR IGNORE style as no update_cols): {sql}")
            else:
                set_statements = ", ".join([f"{col} = excluded.{col}" for col in update_cols])
                sql = f"INSERT INTO {table_name} SELECT * FROM {temp_table_name} ON CONFLICT ({conflict_target}) DO UPDATE SET {set_statements};"
                self.logger.debug(f"Executing SQL (UPSERT style): {sql}")

            self.conn.execute(sql)
            self.conn.unregister(temp_table_name)
            self.logger.info(f"Successfully inserted/replaced {len(df)} rows into {table_name}.")
            return True
        except Exception as e:
            self.logger.error(f"Error during bulk insert/replace into {table_name}: {e}", exc_info=True)
            if 'temp_table_name' in locals() and self.conn: # Check if conn still exists
                try:
                    # Check if temp table exists before trying to unregister
                    # This might require a query like "SHOW TABLES LIKE 'temp_table_name'" or similar depending on DB
                    # For DuckDB, conn.table(temp_table_name) would raise if not exists.
                    # A safer check might be to query information_schema.tables.
                    # However, for simplicity, we'll rely on the try-except for unregister.
                    self.conn.unregister(temp_table_name)
                except Exception as e_unreg:
                    self.logger.error(f"Failed to unregister temp table {temp_table_name} on error: {e_unreg}")
            return False

    def fetch_all_for_engine(self, table_name: str, start_date: Optional[str] = None, end_date: Optional[str] = None, date_column: str = 'metric_date') -> Optional[pd.DataFrame]:
        if self.conn is None:
            self.logger.error(f"Cannot fetch from {table_name}: Database connection is None.")
            return None

        self.logger.debug(f"Fetching all data for engine from {table_name}, date_col: {date_column}, start: {start_date}, end: {end_date}")

        query = f"SELECT * FROM {table_name}"
        params = []
        conditions = []

        if start_date:
            conditions.append(f"{date_column} >= ?")
            params.append(start_date)
        if end_date:
            conditions.append(f"{date_column} <= ?")
            params.append(end_date)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += f" ORDER BY {date_column}" # Ensure data is sorted for engine

        try:
            result_df = self.conn.execute(query, params).fetchdf()
            self.logger.info(f"Successfully fetched {len(result_df)} rows from {table_name} for range {start_date}-{end_date}.")
            return result_df
        except Exception as e:
            self.logger.error(f"Error fetching data from {table_name} for range {start_date}-{end_date}: {e}", exc_info=True)
            return None

    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[pd.DataFrame]:
        if self.conn is None:
            self.logger.error("Cannot execute query: Database connection is None.")
            return None
        try:
            self.logger.debug(f"Executing custom query: {query} with params: {params}")
            return self.conn.execute(query, params).fetchdf()
        except Exception as e:
            self.logger.error(f"Error executing custom query '{query}': {e}", exc_info=True)
            return None

    def close(self):
        self.disconnect()

if __name__ == '__main__':
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s - %(name)s [%(levelname)s] - %(module)s.%(funcName)s:%(lineno)d - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])

    test_logger_db = logging.getLogger("DatabaseManagerTestRun_Atomic_Historical")
    if not test_logger_db.handlers:
        ch_db = logging.StreamHandler(sys.stdout)
        ch_db.setFormatter(logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] - %(message)s'))
        test_logger_db.addHandler(ch_db)
        test_logger_db.propagate = False

    test_db_config = {
        "database": {
            "path": "data/test_hist_job_db.duckdb" # Use a different DB for this test
        }
    }
    test_project_root = str(Path(".").resolve())
    old_db_file = Path(test_project_root) / test_db_config["database"]["path"]
    if old_db_file.exists(): old_db_file.unlink()

    test_logger_db.info("--- Starting DatabaseManager Test (Historical Job Context) ---")
    db_man = DatabaseManager(config=test_db_config, logger_instance=test_logger_db, project_root_dir=test_project_root)

    try:
        db_man.connect()
        assert db_man.conn is not None, "Connection failed"
        test_logger_db.info("DB Connection successful for historical job test.")

        # Test AI log table creation
        tables_df = db_man.execute_query("SHOW TABLES;")
        assert 'log_ai_decision' in tables_df['name'].tolist(), "log_ai_decision table not created"
        test_logger_db.info("'log_ai_decision' table confirmed.")

        # Test fetch_all_for_engine with date filtering
        # (Assuming fact_macro_economic_data exists and might have some data from a previous run or needs sample data)
        # For a clean test, one might insert sample data first.
        # Here, we'll just test the query construction.
        test_start_fetch = "2022-01-01"
        test_end_fetch = "2022-01-15"
        test_logger_db.info(f"Testing fetch_all_for_engine for 'fact_macro_economic_data' from {test_start_fetch} to {test_end_fetch}")

        # Create dummy data for testing fetch_all_for_engine
        sample_macro_data = []
        for i in range(20):
            sample_macro_data.append({
                'metric_date': (pd.to_datetime("2022-01-01") + pd.Timedelta(days=i)).date(),
                'metric_name': 'DGS10_Test', 'metric_value': 2.0 + i*0.01,
                'source_api': 'TestFRED', 'data_snapshot_timestamp': datetime.now(timezone.utc)
            })
        sample_macro_df = pd.DataFrame(sample_macro_data)
        db_man.bulk_insert_or_replace('fact_macro_economic_data', sample_macro_df, unique_cols=['metric_date', 'metric_name', 'source_api'])

        fetched_df = db_man.fetch_all_for_engine('fact_macro_economic_data',
                                                 start_date=test_start_fetch,
                                                 end_date=test_end_fetch,
                                                 date_column='metric_date')
        if fetched_df is not None:
            test_logger_db.info(f"Fetched {len(fetched_df)} rows. Head:\n{fetched_df.head().to_string()}")
            if not fetched_df.empty:
                assert fetched_df['metric_date'].min() >= pd.to_datetime(test_start_fetch).date()
                assert fetched_df['metric_date'].max() <= pd.to_datetime(test_end_fetch).date()
                test_logger_db.info("Date filtering in fetch_all_for_engine seems correct.")
            else:
                test_logger_db.info("fetch_all_for_engine returned empty (might be expected if no data in range).")

        test_logger_db.info("DatabaseManager tests (Historical Job Context) passed.")

    except Exception as e_test_hist:
        test_logger_db.error(f"DatabaseManager test (Historical Job Context) failed: {e_test_hist}", exc_info=True)
    finally:
        db_man.disconnect()
        test_logger_db.info("--- DatabaseManager Test (Historical Job Context) Finished ---")
        # if old_db_file.exists(): old_db_file.unlink(missing_ok=True) # Clean up
