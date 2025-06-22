# Next Generation Financial Data Platform (Simplified)

This project is a data pipeline designed to ingest, process, and curate various types of data, including financial data, documents, images, and multimedia files. The goal is to create a high-quality, integrated database suitable for machine learning, backtesting, and visualization.

This version focuses on a simplified, three-stage architecture using DuckDB for local data management.

## Project Structure

```
your_project_name/
├── data_pipeline.py       # Main processing logic
├── config.py              # Configuration (paths, logging, etc.)
├── requirements.txt       # Python dependencies
├── README.md              # This file
├── .gitignore             # Git ignore settings
|
├── Data test/             # Input test files (images, CSVs, etc.)
│   ├── image1.jpg
│   └── document1.csv
|
├── database/              # DuckDB database files
│   ├── manifest.db
│   ├── raw_lake.db
│   └── curated_mart.db
|
└── logs/                  # Execution log files
    └── pipeline_run_YYYYMMDD_HHMMSS.log
```

## Core Architecture: Three-Database Model

1.  **`manifest.db` (Brain/Catalog)**: Records metadata for all incoming files (hash, path, status, basic metadata). Acts as the single source of truth for file processing status.
2.  **`raw_lake.db` (Raw Storage)**: Stores the original, unaltered content of all incoming files (e.g., as BLOBs). The principle is "get the data in" reliably.
3.  **`curated_mart.db` (Processed/Showcase)**: Stores cleaned, transformed, and enriched data ready for analysis.

## Pipeline Stages

The `data_pipeline.py` script executes the following stages:

1.  **Stage 1: Ingest and Register**:
    *   Scans the input directory (`Data test/`).
    *   For each new file:
        *   Calculates its SHA256 hash.
        *   Stores the raw file content into `raw_lake.db`.
        *   Extracts basic metadata (filename, MIME type, file system dates).
        *   Registers the file and its metadata in `manifest.db` with status `'raw_stored'`.

2.  **Stage 2: Derive Date**:
    *   Scans `manifest.db` for files with status `'raw_stored'`.
    *   Attempts to derive a primary date (`derived_date`) for each file from its metadata (e.g., file modification date, EXIF date).
    *   Updates the `derived_date` in `manifest.db`.

3.  **Stage 3: Curate Data**:
    *   Scans `manifest.db` for files ready for curation (e.g., status `'raw_stored'` or `'date_derived'`).
    *   Based on the file's `raw_content_type`, a specific "processor" is chosen:
        *   **CSV/Excel Processor**: (Inspired by v8.0 logic) Attempts to read with Pandas, perform basic cleaning/transformation, and load into a table in `curated_mart.db`.
        *   **Image Processor**: Extracts basic image features (e.g., dimensions) and stores them in `curated_mart.db`.
        *   Other processors can be added for different file types.
    *   **Error Handling**: If a file cannot be processed by any known processor, its status is updated to `'unsupported_type'`. If a processor encounters an error, the status is updated to `'curation_error'` with an error message. The pipeline will not crash due to individual file errors.
    *   Successfully processed files are marked as `'curated'`.

## Setup

1.  **Clone the repository (if applicable).**
2.  **Create a Python virtual environment (recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: `python-magic` might have system-level dependencies. Refer to its documentation for installation on your OS (e.g., `libmagic` on Linux, or install via Homebrew on macOS).*
4.  **Prepare `Data test/` folder:** Place some sample files (CSVs, JPEGs, etc.) into the `Data test/` directory.
5.  **Review `config.py`:** Ensure paths and settings are appropriate for your environment (defaults should work for a standard setup).

## Running the Pipeline

Execute the main script from the project root directory:

```bash
python data_pipeline.py
```

Logs will be generated in the `logs/` directory, and also printed to the console.
Database files will be created/updated in the `database/` directory (or project root if `DATABASE_DIR` in `config.py` is changed).

## Development Notes

*   **Idempotency**: The pipeline aims to be idempotent. Re-running it should not duplicate data if files haven't changed (due to hash checking).
*   **Extensibility**: New file type processors can be added in Stage 3 by creating new functions and mapping them in the `CONTENT_PROCESSORS` dictionary (to be implemented).
*   **Error Resilience**: The pipeline is designed to skip problematic files and log errors, rather than crashing. Check `manifest.db` statuses and logs to identify and address issues.
```
