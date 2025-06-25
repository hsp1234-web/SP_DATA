import os
import requests
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from .base import BaseConnector

class FredConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.source_api_name = "FRED"
        self.base_url = self.config['api_endpoints']['fred']['base_url']
        self.api_key = os.getenv(self.config['api_endpoints']['fred']['api_key_env'])
        if not self.api_key:
            raise ValueError(f"請設定環境變數 {self.config['api_endpoints']['fred']['api_key_env']}")

    def fetch_data(self, series_ids: List[str], start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        all_series_data = []
        try:
            for series_id in series_ids:
                params = {
                    'series_id': series_id,
                    'api_key': self.api_key,
                    'file_type': 'json',
                    'observation_start': start_date,
                    'observation_end': end_date,
                }
                response = requests.get(self.base_url, params=params)
                response.raise_for_status()
                data = response.json()
                observations = data.get('observations', [])
                if not observations:
                    print(f"警告: FRED 未返回序列 '{series_id}' 的數據。")
                    continue

                df = pd.DataFrame(observations)
                df = df[['date', 'value']]
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna()
                df = df.rename(columns={'value': series_id, 'date': 'metric_date'})
                df['metric_date'] = pd.to_datetime(df['metric_date'])
                all_series_data.append(df.set_index('metric_date'))

            if not all_series_data:
                return None, "所有請求的 FRED 序列均未返回數據。"

            merged_df = pd.concat(all_series_data, axis=1).ffill().reset_index()
            long_df = merged_df.melt(id_vars=['metric_date'], var_name='metric_name', value_name='metric_value')
            long_df['metric_name'] = self.source_api_name + '/' + long_df['metric_name']
            return long_df, None
        except requests.exceptions.RequestException as e:
            return None, f"從 FRED API 獲取數據時發生網路錯誤: {e}"
        except Exception as e:
            return None, f"處理 FRED 數據時發生未知錯誤: {e}"
