import duckdb
import os

def initialize_database(db_path):
    print(f"開始初始化資料庫於: {db_path} ...")

    # 確保目錄存在
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"已創建目錄: {db_dir}")

    conn = duckdb.connect(db_path)

    try:
        # 使用 DDL 字符串，方便管理
        ddl_statements = """
            -- 表格 1: fact_stock_price (股價行情數據)
            CREATE OR REPLACE TABLE fact_stock_price (
                price_date              DATE NOT NULL,
                security_id             VARCHAR NOT NULL,
                open_price              DOUBLE,
                high_price              DOUBLE,
                low_price               DOUBLE,
                close_price             DOUBLE,
                adj_close_price         DOUBLE,
                volume                  BIGINT,
                source_api              VARCHAR NOT NULL,
                data_snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY(price_date, security_id)
            );

            -- 表格 2: fact_macro_economic_data (宏觀經濟數據)
            CREATE OR REPLACE TABLE fact_macro_economic_data (
                metric_date             DATE NOT NULL,
                metric_name             VARCHAR NOT NULL,
                metric_value            DOUBLE,
                source_api              VARCHAR NOT NULL,
                data_snapshot_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY(metric_date, metric_name)
            );
        """
        conn.execute(ddl_statements)
        print("核心表格 'fact_stock_price' 和 'fact_macro_economic_data' 已成功創建或替換。")

    except Exception as e:
        print(f"資料庫初始化過程中發生錯誤: {e}")
    finally:
        conn.close()
        print("資料庫連接已關閉。")

if __name__ == '__main__':
    # 這使得腳本可以直接運行，也可以被其他模組導入和調用
    # 注意：在實際應用中，路徑應該來自設定檔
    default_path = os.path.join('data', 'financial_data.duckdb')
    initialize_database(default_path)
