import unittest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import date, datetime, timezone
import logging
import requests # For requests.exceptions

from src.connectors.dbnomics_connector import DBnomicsConnector

MOCK_CONFIG = {
    "requests_config": {
        "timeout": 10,
        "max_retries": 2,
        "base_headers": {"User-Agent": "TestAgent/1.0"}
    }
}

class TestDBnomicsConnector(unittest.TestCase):
    """
    對 DBnomicsConnector 的單元測試。
    """

    def setUp(self):
        self.patcher_dbnomics_logger = patch('src.connectors.dbnomics_connector.logger', spec=True)
        self.patcher_base_logger = patch('src.connectors.base_connector.logger', spec=True)

        self.mock_dbnomics_logger = self.patcher_dbnomics_logger.start()
        self.mock_base_logger = self.patcher_base_logger.start()

        self.addCleanup(self.patcher_dbnomics_logger.stop)
        self.addCleanup(self.patcher_base_logger.stop)

        self.connector = DBnomicsConnector(config=MOCK_CONFIG)

        self.series_id_valid = 'FRED/FEDFUNDS' # Changed from dgs10 for generic valid id
        self.series_id_dgs10 = 'FRED/DGS10'   # Kept for specific response if needed

        self.mock_api_success_response = {
            "series": {"docs": [{"period": ["2025-06-23", "2025-06-24"], "value": ["4.25", "4.28"]}]}
        }
        self.mock_api_response_with_invalid_data = {
            "series": {"docs": [{"period": ["2023-01-01", "invalid-date", "2023-01-03", "2023-01-04"], "value": ["4.33", "4.33", "not-a-number", "4.30"]}]}
        }
        self.mock_api_response_mismatched_lengths = {
            "series": {"docs": [{"period": ["2023-01-01", "2023-01-02"], "value": ["4.33"]}]}
        }
        self.mock_api_response_empty_docs = {"series": {"docs": []}}
        self.mock_api_response_no_series_key = {"foo": "bar"} # Malformed: missing 'series' key
        self.mock_api_response_no_docs_key = {"series": {"foo": "bar"}} # Malformed: missing 'docs' key
        self.mock_api_response_no_period_key = {"series": {"docs": [{"value": ["1.0"]}]}} # Malformed
        self.mock_api_response_empty_data_arrays = {"series": {"docs": [{"period": [], "value": []}]}}


    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_happy_path_single_series(self, mock_session_request):
        series_to_test = [self.series_id_valid]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_success_response
        mock_session_request.return_value = mock_response

        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)

        self.assertIsNone(error)
        self.assertIsNotNone(result_df)
        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertFalse(result_df.empty)
        self.assertEqual(len(result_df), 2)
        expected_columns = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']
        self.assertListEqual(list(result_df.columns), expected_columns)
        self.assertEqual(result_df['metric_name'].iloc[0], self.series_id_valid)
        self.assertEqual(result_df['source_api'].iloc[0], 'dbnomics')
        self.assertEqual(result_df['metric_date'].iloc[0], date(2025, 6, 23))
        self.assertEqual(result_df['metric_value'].dtype, 'float64')
        self.assertAlmostEqual(result_df['metric_value'].iloc[1], 4.28)
        self.assertIsInstance(result_df['last_updated_timestamp'].iloc[0], datetime)
        self.assertEqual(result_df['last_updated_timestamp'].iloc[0].tzinfo, timezone.utc)

        provider_code, series_code = self.series_id_valid.split('/', 1)
        expected_url = f"{self.connector.BASE_URL}/{provider_code}/{series_code}"
        mock_session_request.assert_called_once_with(
            "GET", expected_url, params=None, json=None,
            headers=MOCK_CONFIG["requests_config"]["base_headers"],
            timeout=MOCK_CONFIG["requests_config"]["timeout"]
        )
        self.mock_dbnomics_logger.info.assert_any_call(f"[{self.connector.source_name}] Successfully transformed data for {self.series_id_valid}. Shape: (2, 5)")

    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_api_returns_empty_data_arrays(self, mock_session_request):
        """
        測試情境 2：API 返回空數據數組。
        驗證方法能否優雅地處理並返回一個空的 DataFrame。
        (根據新的 get_multiple_series 邏輯進行調整)
        """
        # 1. 準備 (Arrange)
        series_to_test = [self.series_id_valid] # 使用 setUp 中定義的 self.series_id_valid
        # mock_api_response_empty_data_arrays 已在 setUp 中定義

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_response_empty_data_arrays
        mock_session_request.return_value = mock_response

        # 2. 執行 (Act)
        # 注意：您的草案中調用的是 self.connector.get_data，我們這裡統一為 get_multiple_series
        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)

        # 3. 斷言 (Assert) - 根據我們的新邏輯
        # 如果這是唯一處理的系列，且沒有其他 fetch/transform 錯誤，error 應該是 None
        self.assertIsNone(error, "在空數據情境下 (無其他錯誤)，錯誤訊息應該為 None")
        self.assertIsNotNone(result_df, "即使數據為空，也應返回一個 DataFrame 物件，而不是 None")
        self.assertIsInstance(result_df, pd.DataFrame, "返回的應為 Pandas DataFrame")
        self.assertTrue(result_df.empty, "返回的 DataFrame 應該為空")

        # 驗證返回的空 DataFrame 仍然包含正確的欄位
        expected_columns = [
            'metric_date', 'metric_name', 'metric_value',
            'source_api', 'last_updated_timestamp'
        ]
        self.assertListEqual(list(result_df.columns), expected_columns, "空的 DataFrame 仍應包含標準的欄位")

        # 驗證日誌有被記錄
        # transform_to_canonical 應記錄 info
        self.mock_dbnomics_logger.info.assert_any_call(
            f"[{self.connector.source_name}] 'period' and 'value' arrays are empty for {self.series_id_valid}. Returning empty DataFrame."
        )
        # get_multiple_series 處理空 df 時的 info 日誌
        self.mock_dbnomics_logger.info.assert_any_call(
            f"[{self.connector.source_name}] Transformation for {self.series_id_valid} resulted in an empty DataFrame (e.g. all data invalid or no data points). Not appending."
        )
        # get_multiple_series 因 all_data_frames 為空而返回空 df 時的 warning 日誌
        self.mock_dbnomics_logger.warning.assert_any_call(
            f"[{self.connector.source_name}] 未能成功處理任何系列數據或所有系列數據均為空，將返回一個空的 DataFrame。"
        )


    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_malformed_json_no_series_key(self, mock_session_request):
        """測試 API 返回的 JSON 缺少 'series' 鍵"""
        series_to_test = [self.series_id_valid]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_response_no_series_key
        mock_session_request.return_value = mock_response

        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)

        self.assertIsNone(result_df)
        self.assertIsNotNone(error)
        self.assertIn("Transform error", error)
        self.assertIn("No 'series.docs' found", error)
        self.mock_dbnomics_logger.warning.assert_any_call(f"[{self.connector.source_name}] No 'series.docs' found in raw_data for {self.series_id_valid}. Raw data: {str(self.mock_api_response_no_series_key)[:500]}")

    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_malformed_json_missing_docs(self, mock_session_request): # Renamed for clarity
        """
        測試情境 3：API 返回格式錯誤的 JSON (缺少 'docs' 鍵)。
        驗證方法能否正確處理並返回錯誤。
        """
        # 1. 準備 (Arrange)
        series_to_test = [self.series_id_valid] # Using self.series_id_valid from setUp

        # mock_api_response_no_docs_key is already defined in setUp as:
        # self.mock_api_response_no_docs_key = {"series": {"foo": "bar"}}
        # This response is actually missing 'docs' under 'series'.
        # The existing mock self.mock_api_response_no_docs_key is {"series": {"foo": "bar"}} which is fine.
        # Let's use a more explicit mock as per your draft for this specific test:
        mock_malformed_api_response_missing_docs = {
            "series": {
                # "docs" key is intentionally missing
                "some_other_key": "some_value"
            }
        }

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_malformed_api_response_missing_docs
        mock_session_request.return_value = mock_response

        # 2. 執行 (Act)
        # Calling get_multiple_series as this is the public interface of the connector being tested
        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)

        # 3. 斷言 (Assert)
        self.assertIsNone(result_df, "在格式錯誤情境下 (缺少 docs)，DataFrame 應該為 None")
        self.assertIsNotNone(error, "在格式錯誤情境下 (缺少 docs)，錯誤訊息不應為 None")

        # 檢查錯誤訊息是否包含了我們預期的內容
        # transform_to_canonical will return "No 'series.docs' found..."
        # get_multiple_series will wrap this into "Transform error for FRED/FEDFUNDS: No 'series.docs' found..."
        self.assertIn(f"Transform error for {self.series_id_valid}", error, "錯誤訊息應包含系列ID")
        self.assertIn("No 'series.docs' found", error, "錯誤訊息應指出 'docs' 鍵缺失")

        # 驗證日誌記錄
        # transform_to_canonical logs a warning for this specific case
        self.mock_dbnomics_logger.warning.assert_any_call(
            f"[{self.connector.source_name}] No 'series.docs' found in raw_data for {self.series_id_valid}. Raw data: {str(mock_malformed_api_response_missing_docs)[:500]}"
        )
        # get_multiple_series logs an error when a transform_error occurs for a series
        self.mock_dbnomics_logger.error.assert_any_call(
            f"[{self.connector.source_name}] Failed to transform data for {self.series_id_valid}: No 'series.docs' found in raw_data for {self.series_id_valid}."
        )
        # And get_multiple_series logs a final warning as no dataframes were processed.
        self.mock_dbnomics_logger.warning.assert_any_call(
            f"[{self.connector.source_name}] No dataframes were successfully processed. Errors: Transform error for {self.series_id_valid}: No 'series.docs' found in raw_data for {self.series_id_valid}."
        )

        # 驗證網路請求仍然被發出了一次
        mock_session_request.assert_called_once()

    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_malformed_json_no_period_key(self, mock_session_request):
        """測試 API 返回的 JSON series.docs[0] 中缺少 'period' 鍵"""
        series_to_test = [self.series_id_valid]
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_response_no_period_key
        mock_session_request.return_value = mock_response

        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)
        self.assertIsNone(result_df)
        self.assertIsNotNone(error)
        self.assertIn("'period' or 'value' array not found", error)


    @patch('src.connectors.base_connector.requests.Session.request')
    def test_get_multiple_series_network_request_failure(self, mock_session_request):
        """測試當底層的 _make_request 由於網路問題徹底失敗時，get_multiple_series 的行為"""
        series_to_test = [self.series_id_valid]

        mock_session_request.side_effect = requests.exceptions.ConnectionError("Simulated connection error")

        result_df, error = self.connector.get_multiple_series(series_ids=series_to_test)

        self.assertIsNone(result_df, "DataFrame 應該為 None 因為請求失敗")
        self.assertIsNotNone(error, "應該有錯誤信息")
        expected_fetch_error_msg_part = f"Failed to fetch data from {self.connector.BASE_URL}/{self.series_id_valid.replace('/', '/')} after {self.connector.max_retries} attempts."
        self.assertIn(expected_fetch_error_msg_part, error) # Error from get_multiple_series includes this

        self.mock_base_logger.error.assert_any_call(
            f"Failed to fetch data from {self.connector.BASE_URL}/{self.series_id_valid.replace('/', '/')} after {self.connector.max_retries} attempts."
        )
        self.mock_dbnomics_logger.error.assert_any_call(
            f"[{self.connector.source_name}] Error fetching data for {self.series_id_valid}: {expected_fetch_error_msg_part}"
        )
        self.mock_dbnomics_logger.warning.assert_any_call(
    f"[{self.connector.source_name}] No dataframes were successfully processed. Errors: Fetch error for {self.series_id_valid}: {expected_fetch_error_msg_part}"
)


    def test_transform_to_canonical_with_invalid_data_points(self):
        df, error = self.connector.transform_to_canonical(self.mock_api_response_with_invalid_data, self.series_id_valid)
        self.assertIsNone(error)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 2)
        self.assertEqual(df['metric_value'].iloc[0], 4.33)
        self.assertEqual(df['metric_value'].iloc[1], 4.30)
        self.mock_dbnomics_logger.info.assert_any_call(f"[{self.connector.source_name}] Dropped 2 rows with NaT dates or NaN values for {self.series_id_valid}.")


    def test_transform_to_canonical_mismatched_lengths(self):
        df, error = self.connector.transform_to_canonical(self.mock_api_response_mismatched_lengths, self.series_id_valid)
        self.assertIsNone(df)
        self.assertIsNotNone(error)
        self.assertIn("do not match", error)

if __name__ == '__main__':
    unittest.main()
