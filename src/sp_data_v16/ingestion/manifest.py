import duckdb
import datetime

class ManifestManager:
    def __init__(self, db_path: str):
        """
        Initializes the ManifestManager with a connection to a DuckDB database.

        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = db_path
        # Connect to the database. If the file doesn't exist, it will be created.
        self.con = duckdb.connect(database=self.db_path, read_only=False)
        self._initialize_schema()

    def _initialize_schema(self):
        """
        Initializes the 'file_manifest' table in the database if it doesn't exist.
        """
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            source_path VARCHAR,
            status VARCHAR,
            ingestion_timestamp TIMESTAMP
        );
        """
        self.con.execute(create_table_sql)
        self.con.commit() # Commit schema changes

    def hash_exists(self, file_hash: str) -> bool:
        """
        Checks if a given file_hash already exists in the 'file_manifest' table.

        Args:
            file_hash: The SHA256 hash of the file.

        Returns:
            True if the hash exists, False otherwise.
        """
        query = "SELECT COUNT(*) FROM file_manifest WHERE file_hash = ?;"
        result = self.con.execute(query, [file_hash]).fetchone()
        return result[0] > 0 if result else False

    def register_file(self, file_hash: str, source_path: str):
        """
        Registers a new file in the 'file_manifest' table.

        Args:
            file_hash: The SHA256 hash of the file.
            source_path: The original path of the file.

        Raises:
            duckdb.ConstraintException: If the file_hash (primary key) already exists.
        """
        current_timestamp = datetime.datetime.now()
        insert_sql = """
        INSERT INTO file_manifest (file_hash, source_path, status, ingestion_timestamp)
        VALUES (?, ?, ?, ?);
        """
        try:
            self.con.execute(insert_sql, [file_hash, source_path, 'registered', current_timestamp])
            self.con.commit() # Commit the transaction
        except duckdb.Error as e: # Catch DuckDB specific errors
            # It's good practice to roll back on error, though for simple inserts
            # and auto-commit behavior of DuckDB, it might not be strictly necessary
            # self.con.rollback() # DuckDB connection object doesn't have rollback directly, managed by transactions
            raise e # Re-raise the exception to be handled by the caller

    def update_file_status(self, file_hash: str, status: str):
        """
        Updates the status of an existing file in the manifest.

        Args:
            file_hash: The SHA256 hash of the file to update.
            status: The new status (e.g., 'processed', 'error').

        Returns:
            True if the update was successful, False if the hash was not found.
        """
        if not self.hash_exists(file_hash):
            return False

        update_sql = "UPDATE file_manifest SET status = ? WHERE file_hash = ?;"
        try:
            self.con.execute(update_sql, [status, file_hash])
            self.con.commit()
            return True
        except duckdb.Error as e:
            # Log error or handle as needed
            print(f"Error updating status for {file_hash}: {e}")
            return False

    def get_file_status(self, file_hash: str) -> str | None:
        """
        Retrieves the status of a file by its hash.

        Args:
            file_hash: The SHA256 hash of the file.

        Returns:
            The status of the file, or None if the file is not found.
        """
        query = "SELECT status FROM file_manifest WHERE file_hash = ?;"
        result = self.con.execute(query, [file_hash]).fetchone()
        return result[0] if result else None

    def close(self):
        """
        Closes the database connection.
        """
        if self.con:
            self.con.close()

if __name__ == '__main__':
    # Example Usage (for direct testing of this script)
    # Using an in-memory database for this example
    manager = ManifestManager(db_path=':memory:')

    # Test case 1: Register a new file
    file_hash_1 = "hash123"
    source_path_1 = "/path/to/file1.txt"
    print(f"Registering file: {file_hash_1}")
    manager.register_file(file_hash_1, source_path_1)
    print(f"Hash exists ({file_hash_1}): {manager.hash_exists(file_hash_1)}")
    print(f"Status of {file_hash_1}: {manager.get_file_status(file_hash_1)}")

    # Test case 2: Update status
    print(f"Updating status of {file_hash_1} to 'processed'")
    manager.update_file_status(file_hash_1, 'processed')
    print(f"Status of {file_hash_1}: {manager.get_file_status(file_hash_1)}")

    # Test case 3: Try to register the same file (should raise ConstraintException)
    try:
        print(f"Attempting to re-register file: {file_hash_1}")
        manager.register_file(file_hash_1, source_path_1)
    except duckdb.ConstraintException as e:
        print(f"Caught expected error for re-registering: {e}")

    # Test case 4: Check non-existent hash
    non_existent_hash = "hash_unknown"
    print(f"Hash exists ({non_existent_hash}): {manager.hash_exists(non_existent_hash)}")
    print(f"Status of {non_existent_hash}: {manager.get_file_status(non_existent_hash)}")

    # Test case 5: Update status for non-existent hash
    print(f"Attempting to update status for {non_existent_hash}: {manager.update_file_status(non_existent_hash, 'processed')}")

    manager.close()
    print("ManifestManager example usage complete.")
