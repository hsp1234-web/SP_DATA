import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from datetime import date, datetime, timezone
import requests # Required for requests.exceptions.HTTPError

# 調整導入路徑以匹配項目結構
# 如果 tests 和 src 在同一父目錄下:
from src.connectors.dbnomics_connector import DBnomicsConnector
# 如果執行時遇到 ModuleNotFoundError，可能需要調整 PYTHONPATH 或使用相對導入

class TestDBnomicsConnector(unittest.TestCase):

    def setUp(self):
        """測試開始前的設置"""
        self.mock_config = {
            "requests_config": {
                "timeout": 10,
                "max_retries": 2, # BaseConnector's _make_request will try this many times
                "base_headers": {"User-Agent": "TestAgent/1.0"}
            }
        }
        self.connector = DBnomicsConnector(config=self.mock_config)
        self.series_id_valid = "FRED/FEDFUNDS"
        self.series_id_invalid_format = "FRED-FEDFUNDS"

        # 準備一個模擬的成功 API 回應
        self.mock_api_success_response = {
            "series": {
                "docs": [
                    {
                        "dataset_name": "Federal Funds Effective Rate",
                        "series_name": "Federal Funds Effective Rate",
                        "period": ["2023-01-01", "2023-01-02", "2023-01-03"],
                        "value": ["4.33", "4.33", "4.32"],
                        "PROVIDER_CODE": "FRED",
                        "SERIES_CODE": "FEDFUNDS"
                        # ... 其他元數據
                    }
                ]
            }
        }
        # 準備一個API回應，其中包含無效數據點
        self.mock_api_response_with_invalid_data = {
            "series": {
                "docs": [
                    {
                        "period": ["2023-01-01", "invalid-date", "2023-01-03", "2023-01-04"],
                        "value": ["4.33", "4.33", "not-a-number", "4.30"],
                    }
                ]
            }
        }
        # 準備一個API回應，其中 period 和 value 長度不匹配
        self.mock_api_response_mismatched_lengths = {
            "series": {
                "docs": [
                    {
                        "period": ["2023-01-01", "2023-01-02"],
                        "value": ["4.33"],
                    }
                ]
            }
        }
        # API 回應，但 docs 為空
        self.mock_api_response_empty_docs = {"series": {"docs": []}}
        # API 回應，但 period/value 為空列表
        self.mock_api_response_empty_data_arrays = {"series": {"docs": [{"period": [], "value": []}]}}


    @patch('src.connectors.base_connector.requests.Session.request')
    def test_fetch_data_success(self, mock_request_method):
        """測試 fetch_data 成功獲取數據"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.mock_api_success_response
        mock_request_method.return_value = mock_response

        data, error = self.connector.fetch_data(self.series_id_valid)

        self.assertIsNone(error)
        self.assertIsNotNone(data)
        self.assertEqual(data, self.mock_api_success_response)
        provider_code, series_code = self.series_id_valid.split('/', 1)
        expected_url = f"{self.connector.BASE_URL}/{provider_code}/{series_code}"
        mock_request_method.assert_called_once_with(
            "GET", expected_url, params=None, json=None,
            headers=self.connector.session.headers, timeout=self.connector.timeout
        )

    @patch('src.connectors.base_connector.requests.Session.request')
    def test_fetch_data_api_error(self, mock_request_method):
        """測試 fetch_data 處理 API HTTP 錯誤"""
        mock_response = MagicMock()
        mock_response.status_code = 500
        # Configure the mock to re-raise the exception when raise_for_status is called
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("Server Error", response=mock_response)
        mock_response.text = "Internal Server Error" # for error message in _make_request
        mock_request_method.return_value = mock_response

        data, error = self.connector.fetch_data(self.series_id_valid)

        self.assertIsNone(data)
        self.assertIsNotNone(error)
        self.assertIn(f"Failed to fetch data from {self.connector.BASE_URL}/FRED/FEDFUNDS after {self.connector.max_retries} attempts.", error)
        self.assertEqual(mock_request_method.call_count, self.mock_config["requests_config"]["max_retries"])


    def test_fetch_data_invalid_series_id_format(self):
        """測試 fetch_data 處理無效的 series_id 格式"""
        data, error = self.connector.fetch_data(self.series_id_invalid_format)
        self.assertIsNone(data)
        self.assertIsNotNone(error)
        self.assertIn("Invalid series_id format", error)

    def test_transform_to_canonical_success(self):
        """測試 transform_to_canonical 成功轉換數據"""
        df, error = self.connector.transform_to_canonical(self.mock_api_success_response, self.series_id_valid)

        self.assertIsNone(error)
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 3)

        expected_columns = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']
        self.assertListEqual(list(df.columns), expected_columns)

        self.assertEqual(df['metric_date'].iloc[0], date(2023, 1, 1))
        self.assertEqual(df['metric_name'].iloc[0], self.series_id_valid)
        self.assertEqual(df['metric_value'].iloc[0], 4.33)
        self.assertEqual(df['source_api'].iloc[0], "dbnomics")
        self.assertIsInstance(df['last_updated_timestamp'].iloc[0], datetime)
        self.assertEqual(df['last_updated_timestamp'].iloc[0].tzinfo, timezone.utc)


    def test_transform_to_canonical_with_invalid_data_points(self):
        """測試 transform_to_canonical 處理並清洗無效數據點"""
        df, error = self.connector.transform_to_canonical(self.mock_api_response_with_invalid_data, self.series_id_valid)

        self.assertIsNone(error)
        self.assertIsNotNone(df)
        # 預期 "invalid-date" 和 "not-a-number" 會被移除
        self.assertEqual(len(df), 2)
        self.assertEqual(df['metric_value'].iloc[0], 4.33)
        self.assertEqual(df['metric_value'].iloc[1], 4.30)

    def test_transform_to_canonical_mismatched_lengths(self):
        """測試 transform_to_canonical 處理 period 和 value 長度不匹配"""
        df, error = self.connector.transform_to_canonical(self.mock_api_response_mismatched_lengths, self.series_id_valid)
        self.assertIsNone(df)
        self.assertIsNotNone(error)
        self.assertIn("do not match", error)

    def test_transform_to_canonical_empty_docs(self):
        """測試 transform_to_canonical 處理 series.docs 為空的情況"""
        df, error = self.connector.transform_to_canonical(self.mock_api_response_empty_docs, self.series_id_valid)
        self.assertIsNone(df)
        self.assertIsNotNone(error)
        self.assertIn("No 'series.docs' found", error)

    def test_transform_to_canonical_empty_data_arrays(self):
        """測試 transform_to_canonical 處理 period/value 為空列表的情況"""
        df, error = self.connector.transform_to_canonical(self.mock_api_response_empty_data_arrays, self.series_id_valid)
        self.assertIsNone(error)
        self.assertIsNotNone(df)
        self.assertTrue(df.empty)
        expected_columns = ['metric_date', 'metric_name', 'metric_value', 'source_api', 'last_updated_timestamp']
        self.assertListEqual(list(df.columns), expected_columns)


    @patch.object(DBnomicsConnector, 'fetch_data')
    @patch.object(DBnomicsConnector, 'transform_to_canonical')
    def test_get_multiple_series_success(self, mock_transform, mock_fetch):
        """測試 get_multiple_series 成功處理多個系列"""
        series_ids = ["FRED/S1", "FRED/S2"]

        mock_fetch.side_effect = [
            ({"series_data_1": "foo"}, None),
            ({"series_data_2": "bar"}, None)
        ]

        df1 = pd.DataFrame({'metric_name': ['FRED/S1'], 'metric_value': [1.0]})
        df2 = pd.DataFrame({'metric_name': ['FRED/S2'], 'metric_value': [2.0]})
        mock_transform.side_effect = [
            (df1, None),
            (df2, None)
        ]

        result_df, error = self.connector.get_multiple_series(series_ids)

        self.assertIsNone(error)
        self.assertIsNotNone(result_df)
        self.assertEqual(len(result_df), 2) # df1 and df2 concat
        self.assertEqual(mock_fetch.call_count, 2)
        self.assertEqual(mock_transform.call_count, 2)
        mock_fetch.assert_any_call(series_id="FRED/S1")
        mock_fetch.assert_any_call(series_id="FRED/S2")
        mock_transform.assert_any_call(raw_data={"series_data_1": "foo"}, series_id="FRED/S1")
        mock_transform.assert_any_call(raw_data={"series_data_2": "bar"}, series_id="FRED/S2")


    @patch.object(DBnomicsConnector, 'fetch_data')
    def test_get_multiple_series_fetch_error_one_series(self, mock_fetch):
        """測試 get_multiple_series 當一個系列 fetch 失敗時的行為"""
        series_ids = ["FRED/S1", "FRED/S2_error"]

        # S1 成功時返回的 mock_api_success_response 包含3條數據
        mock_fetch.side_effect = [
            (self.mock_api_success_response, None),
            (None, "Fetch failed for S2_error")
        ]

        with patch.object(self.connector, 'transform_to_canonical') as mock_transform:
            # 模擬 transform_to_canonical 對 S1 的成功轉換
            # 假設 mock_api_success_response 會轉換成一個有3行的DataFrame
            transformed_s1_df = pd.DataFrame({
                'metric_date': [date(2023,1,1), date(2023,1,2), date(2023,1,3)],
                'metric_name': ["FRED/S1"]*3,
                'metric_value': [1.0, 1.1, 1.2]
            })
            mock_transform.return_value = (transformed_s1_df, None)

            result_df, error = self.connector.get_multiple_series(series_ids)

            self.assertIsNotNone(result_df)
            self.assertIsNotNone(error)
            self.assertIn("Partial success", error)
            self.assertIn("Fetch error for FRED/S2_error", error)
            self.assertEqual(len(result_df), 3)
            self.assertEqual(result_df['metric_name'].iloc[0], 'FRED/S1')
            mock_transform.assert_called_once_with(raw_data=self.mock_api_success_response, series_id="FRED/S1")

    @patch.object(DBnomicsConnector, 'fetch_data')
    @patch.object(DBnomicsConnector, 'transform_to_canonical')
    def test_get_multiple_series_transform_error_one_series(self, mock_transform, mock_fetch):
        """測試 get_multiple_series 當一個系列 transform 失敗時的行為"""
        series_ids = ["FRED/S1", "FRED/S2_transform_error"]

        mock_fetch.side_effect = [
            (self.mock_api_success_response, None),
            (self.mock_api_success_response, None)
        ]
        # S1 成功轉換，假設返回一個有3行的DataFrame
        transformed_s1_df = pd.DataFrame({
            'metric_date': [date(2023,1,1), date(2023,1,2), date(2023,1,3)],
            'metric_name': ['FRED/S1']*3,
            'metric_value': [1.0, 1.1, 1.2]
        })
        mock_transform.side_effect = [
            (transformed_s1_df, None),
            (None, "Transform failed for S2_transform_error")
        ]

        result_df, error = self.connector.get_multiple_series(series_ids)

        self.assertIsNotNone(result_df)
        self.assertIsNotNone(error)
        self.assertIn("Partial success", error)
        self.assertIn("Transform failed for FRED/S2_transform_error", error)
        self.assertEqual(len(result_df), 3)
        self.assertEqual(result_df['metric_name'].iloc[0], 'FRED/S1')
        self.assertEqual(mock_transform.call_count, 2)


    @patch.object(DBnomicsConnector, 'fetch_data')
    def test_get_multiple_series_all_fail(self, mock_fetch):
        """測試 get_multiple_series 當所有系列都失敗時的行為"""
        series_ids = ["FRED/S1_error", "FRED/S2_error"]
        mock_fetch.side_effect = [
            (None, "Fetch failed for S1_error"),
            (None, "Fetch failed for S2_error")
        ]

        result_df, error = self.connector.get_multiple_series(series_ids)

        self.assertIsNone(result_df)
        self.assertIsNotNone(error)
        self.assertIn("No dataframes were successfully processed", error)
        self.assertIn("Fetch failed for S1_error", error)
        self.assertIn("Fetch failed for S2_error", error)

if __name__ == '__main__':
    unittest.main()
