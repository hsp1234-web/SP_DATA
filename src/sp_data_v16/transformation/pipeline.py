import duckdb
import pathlib
from .schema_manager import SchemaManager
from src.sp_data_v16.core.config import load_config

class TransformationPipeline:
    def __init__(self, config_path: str = "config_v16.yaml"):
        self.config = load_config(config_path)

        db_config = self.config.get("database", {})
        paths_config = self.config.get("paths", {})

        self.manifest_db_path = db_config.get("manifest_db_path")
        self.raw_lake_db_path = db_config.get("raw_lake_db_path")
        self.processed_db_path = db_config.get("processed_db_path")
        self.schema_config_path = paths_config.get("schema_config_path")

        if not all([self.manifest_db_path, self.raw_lake_db_path, self.processed_db_path, self.schema_config_path]):
            missing = [
                path_name for path_name, path_val in {
                    "manifest_db_path": self.manifest_db_path,
                    "raw_lake_db_path": self.raw_lake_db_path,
                    "processed_db_path": self.processed_db_path,
                    "schema_config_path": self.schema_config_path
                }.items() if not path_val
            ]
            raise ValueError(f"Missing required paths in configuration: {', '.join(missing)}")

        for db_path_str in [self.manifest_db_path, self.raw_lake_db_path, self.processed_db_path]:
            if db_path_str: # Should always be true due to the check above, but good practice
                db_parent_dir = pathlib.Path(db_path_str).parent
                db_parent_dir.mkdir(parents=True, exist_ok=True)

        # Ensure schema_config_path parent directory exists if it's specified and not just a filename
        if self.schema_config_path:
            schema_parent_dir = pathlib.Path(self.schema_config_path).parent
            if not schema_parent_dir.exists(): # only create if it's a real path, not current dir
                 schema_parent_dir.mkdir(parents=True, exist_ok=True)


        self.manifest_con = duckdb.connect(database=str(self.manifest_db_path), read_only=False)
        self.raw_lake_con = duckdb.connect(database=str(self.raw_lake_db_path), read_only=True)
        self.processed_con = duckdb.connect(database=str(self.processed_db_path), read_only=False)

        self.schema_manager = SchemaManager(schema_path=str(self.schema_config_path))

        print(f"TransformationPipeline initialized. Manifest: {self.manifest_db_path}, RawLake: {self.raw_lake_db_path}, ProcessedDB: {self.processed_db_path}, Schemas: {self.schema_config_path}")

    def run(self):
        try:
            pending_files = self.manifest_con.execute(
                "SELECT file_hash, file_path FROM file_manifest WHERE status = 'loaded_to_raw_lake'"
            ).fetchall()

            if not pending_files:
                print("目前沒有待處理的檔案。")
                return

            for file_hash, file_path in pending_files:
                print(f"找到待處理檔案：{file_path} (Hash: {file_hash[:8]})")
                # Placeholder for further processing logic:
                # 1. Read raw_content from raw_lake_con using file_hash
                #    raw_content_result = self.raw_lake_con.execute(
                #        "SELECT raw_content FROM raw_files WHERE file_hash = ?", (file_hash,)
                #    ).fetchone()
                #    if not raw_content_result:
                #        print(f"Warning: Raw content for {file_hash} not found in Raw Lake. Skipping.")
                #        continue
                #    raw_content = raw_content_result[0]
                #
                # 2. Identify schema
                #    schema_name = self.schema_manager.identify_schema_from_content(raw_content)
                #    if not schema_name:
                #        print(f"Warning: Could not identify schema for {file_path} (Hash: {file_hash[:8]}). Skipping.")
                #        # Optionally, update manifest status to 'schema_not_identified' or similar
                #        # self.manifest_con.execute("UPDATE file_manifest SET status = 'schema_not_identified' WHERE file_hash = ?", (file_hash,))
                #        # self.manifest_con.commit()
                #        continue
                #    print(f"Identified schema '{schema_name}' for {file_path}")
                #
                # 3. Transform and load to processed_con based on schema_name
                #    # This part will be highly dependent on the actual schema definitions and transformation logic
                #    print(f"Placeholder: Transform and load data for {file_path} using schema {schema_name}.")
                #
                # 4. Update manifest status
                #    # Example: self.manifest_con.execute("UPDATE file_manifest SET status = 'processed' WHERE file_hash = ?", (file_hash,))
                #    # self.manifest_con.commit()
                #    print(f"Placeholder: Updated manifest status for {file_hash} to 'processed'.")
                pass # End of placeholder
        finally:
            self.close()

    def close(self):
        if hasattr(self, 'manifest_con') and self.manifest_con:
            self.manifest_con.close()
        if hasattr(self, 'raw_lake_con') and self.raw_lake_con:
            self.raw_lake_con.close()
        if hasattr(self, 'processed_con') and self.processed_con:
            self.processed_con.close()
        print("TransformationPipeline connections closed.")

if __name__ == '__main__':
    # Basic example for quick testing.
    # This requires setting up a dummy config, schema file, and dummy DBs.

    current_dir = pathlib.Path(__file__).parent
    example_config_path = current_dir / "temp_transform_config.yaml"
    example_data_dir = current_dir / "example_transform_data"
    example_schema_path = current_dir / "example_schemas.json"

    # Clean up from previous runs
    if example_config_path.exists(): example_config_path.unlink()
    if example_schema_path.exists(): example_schema_path.unlink()
    # Simplified cleanup for example_data_dir
    if example_data_dir.exists():
        for item in example_data_dir.rglob('*'):
            if item.is_file(): item.unlink()
        for item in sorted(list(example_data_dir.rglob('*')), key=lambda p: len(p.parts), reverse=True):
            if item.is_dir(): item.rmdir() # rmdir only if empty
        if example_data_dir.exists() and not list(example_data_dir.iterdir()): # Check if empty before removing
             example_data_dir.rmdir()


    example_data_dir.mkdir(parents=True, exist_ok=True)

    manifest_db = example_data_dir / "manifest.db"
    raw_lake_db = example_data_dir / "raw_lake.db"
    processed_db = example_data_dir / "processed.db"

    # Create dummy schema file
    dummy_schemas = {
        "typeA": {"keywords": ["report type a", "alpha version"]},
        "typeB": {"keywords": ["beta report", "type b data"]}
    }
    with open(example_schema_path, 'w', encoding='utf-8') as f:
        json.dump(dummy_schemas, f)

    # Create dummy config
    example_config_content = f"""
database:
  manifest_db_path: "{manifest_db.as_posix()}"
  raw_lake_db_path: "{raw_lake_db.as_posix()}"
  processed_db_path: "{processed_db.as_posix()}"
paths:
  schema_config_path: "{example_schema_path.as_posix()}"
  input_directory: "dummy_input" # Not used by TransformationPipeline directly
    """
    with open(example_config_path, 'w', encoding='utf-8') as f:
        f.write(example_config_content)

    # Initialize dummy databases and insert sample data
    # Manifest DB
    con_m = duckdb.connect(str(manifest_db))
    con_m.execute("""
        CREATE TABLE IF NOT EXISTS file_manifest (
            file_hash VARCHAR PRIMARY KEY,
            file_path VARCHAR,
            registration_timestamp TIMESTAMP DEFAULT current_timestamp,
            status VARCHAR DEFAULT 'registered'
        );
    """)
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash123abc", "/path/to/file1.txt", "loaded_to_raw_lake"))
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash456def", "/path/to/file2.csv", "loaded_to_raw_lake"))
    con_m.execute("INSERT INTO file_manifest (file_hash, file_path, status) VALUES (?, ?, ?)",
                  ("hash789ghi", "/path/to/file3.json", "registered")) # This one should be ignored
    con_m.close()

    # Raw Lake DB (just needs to exist for connection, content reading is placeholder)
    con_rl = duckdb.connect(str(raw_lake_db))
    con_rl.execute("CREATE TABLE IF NOT EXISTS raw_files (file_hash VARCHAR PRIMARY KEY, raw_content BLOB)")
    # In a real test, you'd insert raw_content corresponding to hashes for schema identification
    con_rl.close()

    # Processed DB (just needs to exist)
    con_p = duckdb.connect(str(processed_db))
    con_p.close()


    print("Running TransformationPipeline example...")
    try:
        pipeline = TransformationPipeline(config_path=str(example_config_path))
        pipeline.run()
    except ValueError as ve:
        print(f"Configuration Error: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("\nTransformation example finished.")
        # Optional: Add more cleanup here if needed
        if example_config_path.exists(): example_config_path.unlink(missing_ok=True)
        if example_schema_path.exists(): example_schema_path.unlink(missing_ok=True)
        # db files will be cleaned on next run or manually
import json # ensure json is imported for the __main__ block if not already at top
