import pandas as pd
import duckdb
import os

class ProcessedDBLoader:
    """
    A class to load processed pandas DataFrames into a DuckDB database.
    """

    def __init__(self, db_path: str):
        """
        Initializes the ProcessedDBLoader with the path to the DuckDB database.

        Args:
            db_path: The file path for the DuckDB database.
                     Example: 'data/processed/processed_data.db'
        """
        self.db_path = db_path
        self.con = None

        try:
            # Ensure the parent directory for the database file exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir: # Check if db_dir is not an empty string (e.g. if db_path is just a filename)
                os.makedirs(db_dir, exist_ok=True)

            # Establish connection to DuckDB
            self.con = duckdb.connect(database=self.db_path, read_only=False)
            print(f"Successfully connected to DuckDB at '{self.db_path}'")
        except duckdb.Error as e:
            print(f"Error connecting to DuckDB at '{self.db_path}': {e}")
            # Potentially re-raise or handle more gracefully depending on application needs
            raise
        except OSError as e:
            print(f"Error creating directory for DuckDB database '{self.db_path}': {e}")
            raise

    def load_dataframe(self, dataframe: pd.DataFrame, table_name: str):
        """
        Loads a pandas DataFrame into the specified table in the DuckDB database.

        Args:
            dataframe: The pandas DataFrame to load.
            table_name: The name of the target table in the database.
        """
        if not self.con:
            print("Error: Database connection is not established. Cannot load DataFrame.")
            return

        try:
            # Using DuckDB's capabilities to register and insert from a DataFrame
            # This is often more efficient than to_sql for DuckDB
            # self.con.register('temp_df_view', dataframe)
            # self.con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM temp_df_view")
            # self.con.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df_view")
            # self.con.unregister('temp_df_view')

            # Or using pandas .to_sql for simplicity and broader compatibility if needed elsewhere
            dataframe.to_sql(name=table_name, con=self.con, if_exists='append', index=False)
            print(f"Successfully loaded DataFrame into table '{table_name}'.")
        except duckdb.Error as e:
            print(f"DuckDB Error loading DataFrame into table '{table_name}': {e}")
        except AttributeError as e:
            # This might happen if self.con is None due to an initialization error
            print(f"AttributeError: Could not operate on database connection (is it initialized?): {e}")
        except Exception as e:
            print(f"An unexpected error occurred while loading DataFrame into table '{table_name}': {e}")

    def close(self):
        """
        Closes the connection to the DuckDB database.
        """
        if self.con:
            try:
                self.con.close()
                print(f"Successfully closed connection to DuckDB at '{self.db_path}'.")
            except duckdb.Error as e:
                print(f"Error closing DuckDB connection: {e}")
        else:
            print("No active DuckDB connection to close.")

# Example Usage (optional, for testing)
if __name__ == '__main__':
    # Define a path for the test database
    test_db_file = 'test_processed_data.db'

    # Clean up any existing test database file from previous runs
    if os.path.exists(test_db_file):
        os.remove(test_db_file)
    if os.path.exists(test_db_file + ".wal"): # DuckDB Write-Ahead Log file
        os.remove(test_db_file + ".wal")

    # Create a loader instance
    try:
        loader = ProcessedDBLoader(db_path=test_db_file)
    except Exception as e:
        print(f"Failed to initialize loader: {e}")
        exit()

    # Create a sample DataFrame
    sample_data = {
        'id': [1, 2, 3, 4],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor'],
        'price': [1200.00, 25.00, 75.00, 300.00],
        'quantity': [10, 50, 30, 15]
    }
    df_products = pd.DataFrame(sample_data)

    sample_data_sales = {
        'sale_id': ['S1001', 'S1002', 'S1003'],
        'product_id': [1, 2, 1],
        'sale_date': pd.to_datetime(['2023-01-15', '2023-01-17', '2023-01-18']),
        'amount': [1200.00, 25.00, 1200.00]
    }
    df_sales = pd.DataFrame(sample_data_sales)

    # Load the DataFrames into different tables
    if loader.con: # Proceed only if connection was successful
        loader.load_dataframe(df_products, 'products')
        loader.load_dataframe(df_sales, 'sales')

        # Example of trying to load an empty DataFrame
        print("\nAttempting to load an empty DataFrame...")
        empty_df = pd.DataFrame()
        loader.load_dataframe(empty_df, 'empty_table') # Should still create the table via to_sql

        # Verify data by reading it back (optional)
        print("\nVerifying data from 'products' table:")
        try:
            result_products = loader.con.execute("SELECT * FROM products").fetchdf()
            print(result_products)
        except duckdb.Error as e:
            print(f"Error reading from 'products' table: {e}")

        print("\nVerifying data from 'sales' table:")
        try:
            result_sales = loader.con.execute("SELECT * FROM sales").fetchdf()
            print(result_sales)
        except duckdb.Error as e:
            print(f"Error reading from 'sales' table: {e}")

        print("\nVerifying 'empty_table' structure:")
        try:
            result_empty = loader.con.execute("DESCRIBE empty_table").fetchdf() # DuckDB command to get schema
            print(result_empty)
        except duckdb.Error as e:
            print(f"Error describing 'empty_table': {e}")


    # Close the connection
    loader.close()

    # Test case: loader initialization fails (e.g. permission denied if db_path was in a restricted area)
    print("\n--- Test Case: Loader initialization failure (simulated by using an invalid path) ---")
    # This is a bit hard to simulate reliably without changing file permissions,
    # but we can check if the error handling in __init__ for OSError works if directory creation fails.
    # For now, the existing __init__ handles duckdb connection errors.
    # If os.makedirs were to fail (e.g. due to permissions), it should be caught.

    # Clean up the test database file after example run
    if os.path.exists(test_db_file):
        os.remove(test_db_file)
    if os.path.exists(test_db_file + ".wal"):
        os.remove(test_db_file + ".wal")

    print("\nExample run finished.")
