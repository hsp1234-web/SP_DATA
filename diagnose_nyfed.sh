#!/bin/bash

# 清理任何先前存在的相關檔案和目錄
echo "--- 清理舊的診斷檔案 (如果存在) ---"
rm -rf src run_nyfed_test.py

# 確保必要的工具已安裝
echo "--- 檢查並安裝依賴 ---"
pip install requests pandas openpyxl PyYAML

# 創建臨時的 src 目錄結構
mkdir -p src/connectors
mkdir -p src/configs
mkdir -p src/logs # For potential logging from connector

echo "--- 正在創建空的 src/connectors/__init__.py ---"
touch src/connectors/__init__.py

echo "--- 正在創建精簡版 NYFed 連接器 (src/connectors/nyfed_connector.py) ---"
cat << 'EOF' > src/connectors/nyfed_connector.py
import requests
import pandas as pd
import logging
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional

# 基本日誌設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NYFedConnectorDiagnosis")

class NYFedConnectorDiagnosis:
    def __init__(self, config: Dict[str, Any]):
        self.urls_config: List[Dict[str, str]] = config.get('nyfed_primary_dealer_urls', [])
        self.requests_config: Dict[str, Any] = config.get('requests_config', {})
        self.raw_data_frames: Dict[str, pd.DataFrame] = {}
        self.processed_data: Optional[pd.DataFrame] = None
        logger.info(f"NYFedConnectorDiagnosis initialized with {len(self.urls_config)} URLs.")

    def _download_file(self, url_config: Dict[str, str]) -> Optional[bytes]:
        url = url_config.get('url')
        if not url:
            logger.error("URL not found in url_config.")
            return None

        timeout = self.requests_config.get('download_timeout', 120)
        max_retries = self.requests_config.get('max_retries', 3)

        logger.info(f"Attempting to download from: {url}")

        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'}) # Added User-Agent
                logger.info(f"URL: {url}, Status Code: {response.status_code}, Content-Type: {response.headers.get('Content-Type')}, Size: {len(response.content)} bytes")
                response.raise_for_status() # Raises HTTPError for bad responses (4XX or 5XX)

                content_type = response.headers.get('Content-Type', '').lower()
                # Check for common Excel content types
                if 'excel' in content_type or 'spreadsheetml' in content_type or 'officedocument' in content_type:
                    logger.info(f"Content-Type '{content_type}' appears to be Excel for URL: {url}.")
                    return response.content
                else:
                    logger.warning(f"Downloaded content from {url} has unexpected Content-Type: '{content_type}'. Expected Excel.")
                    logger.info("First 500 bytes of content:")
                    try:
                        logger.info(response.content[:500].decode('utf-8', errors='replace'))
                    except Exception as e:
                        logger.error(f"Could not decode first 500 bytes as UTF-8: {e}")
                        logger.info(response.content[:500])
                    return None # Treat as failure if not expected content type

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTPError for {url} (Attempt {attempt + 1}/{max_retries}): {e}")
                if response is not None:
                     logger.error(f"Response headers: {response.headers}")
                     logger.error(f"Response content preview (first 500 bytes): {response.content[:500]}")
            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException for {url} (Attempt {attempt + 1}/{max_retries}): {e}")

            if attempt < max_retries - 1:
                logger.info(f"Retrying download for {url}...")
            else:
                logger.error(f"Failed to download {url} after {max_retries} attempts.")
        return None

    def fetch_data(self):
        if not self.urls_config:
            logger.warning("No URLs configured for NYFed data.")
            return

        # For diagnosis, let's try only the first URL
        if self.urls_config:
            first_url_config = self.urls_config[0]
            logger.info(f"Diagnosing with first URL: {first_url_config.get('url')}")
            file_content = self._download_file(first_url_config)
            if file_content:
                logger.info(f"Successfully downloaded content for {first_url_config.get('url')}. Length: {len(file_content)} bytes.")
                # Try to parse with pandas to see if it's a valid Excel file
                try:
                    # For .xlsx files, engine='openpyxl' is needed.
                    # For older .xls, engine='xlrd' might be needed, but openpyxl should handle modern ones.
                    df = pd.read_excel(file_content, engine='openpyxl')
                    logger.info(f"Successfully parsed downloaded content as Excel for {first_url_config.get('url')}. Shape: {df.shape}")
                    self.raw_data_frames[first_url_config.get('file_pattern', 'unknown_file')] = df
                except Exception as e:
                    logger.error(f"Failed to parse downloaded content as Excel for {first_url_config.get('url')}: {e}")
                    logger.error("This indicates the downloaded file is not a valid Excel file that pandas can read with openpyxl.")
            else:
                logger.error(f"Failed to download or validate content for {first_url_config.get('url')}.")
        else:
            logger.info("No URLs to diagnose.")
        logger.info("NYFedConnectorDiagnosis fetch_data completed.")

EOF

echo "--- 正在創建精簡版設定檔 (src/configs/project_config.yaml) ---"
cat << 'EOF' > src/configs/project_config.yaml
# Simplified config for NYFed Diagnosis
requests_config:
  max_retries: 3
  base_backoff_seconds: 1 # Not used in this simplified connector
  timeout: 30
  download_timeout: 120

nyfed_primary_dealer_urls:
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2024.xlsx"
    file_pattern: "prideal2024.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2023.xlsx"
    file_pattern: "prideal2023.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
  - url: "https://www.newyorkfed.org/medialibrary/media/markets/prideal/prideal2022.xlsx"
    file_pattern: "prideal2022.xlsx"
    format_type: "PD_STATS_FORMAT_2013_ONWARDS"
EOF

echo "--- 正在創建診斷驅動腳本 (run_nyfed_test.py) ---"
cat << 'EOF' > run_nyfed_test.py
import yaml
from pathlib import Path
from connectors.nyfed_connector import NYFedConnectorDiagnosis # Adjusted import
import logging

# Setup basic logging for the test script itself
logger = logging.getLogger("NYFedTestDriver")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_config(config_path_str: str) -> dict:
    config_path = Path(config_path_str)
    logger.info(f"Loading configuration from: {config_path.resolve()}")
    if not config_path.exists():
        logger.error(f"Configuration file {config_path} not found.")
        return {}
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading YAML configuration from {config_path}: {e}")
        return {}

def main():
    logger.info("--- Starting NYFed Connector Diagnosis ---")

    # Path assumes script is run from project root where diagnose_nyfed.sh creates src/
    config_file = "src/configs/project_config.yaml"
    config = load_config(config_file)

    if not config:
        logger.error("Failed to load configuration. Exiting diagnosis.")
        return

    # Ensure 'nyfed_primary_dealer_urls' and 'requests_config' are present
    if 'nyfed_primary_dealer_urls' not in config or 'requests_config' not in config:
        logger.error("Essential configuration ('nyfed_primary_dealer_urls' or 'requests_config') missing. Exiting.")
        return

    try:
        connector = NYFedConnectorDiagnosis(config=config)
        connector.fetch_data()
    except Exception as e:
        logger.error(f"An error occurred during NYFedConnectorDiagnosis execution: {e}", exc_info=True)

    logger.info("--- NYFed Connector Diagnosis Finished ---")

if __name__ == "__main__":
    main()
EOF

echo "--- 執行診斷腳本 ---"
export PYTHONPATH=$PWD/src:$PYTHONPATH
python run_nyfed_test.py

echo "--- 清理臨時檔案 ---"
# rm -rf src run_nyfed_test.py run_nyfed_test.py
# We might want to keep them for inspection if the tool allows, otherwise, uncomment to clean.
echo "--- 診斷結束 ---"
