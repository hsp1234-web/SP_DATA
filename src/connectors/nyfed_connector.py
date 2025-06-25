import requests
import pandas as pd
from typing import Dict, Any, Tuple, Optional
from io import BytesIO
from .base import BaseConnector

class NYFedConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.source_api_name = "NYFED"
        self.urls_config = self.config.get('nyfed_primary_dealer_urls', [])
        self.recipes = self.config.get('nyfed_format_recipes', {})

    def fetch_data(self) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        all_positions_df = []
        try:
            for file_info in self.urls_config:
                url, format_type = file_info['url'], file_info['format_type']
                print(f"正在從 NYFed 下載: {url}")
                response = requests.get(url, allow_redirects=True) # Added allow_redirects
                print(f"NYFed Response Status Code: {response.status_code}")
                content_type = response.headers.get('content-type', '').lower()
                print(f"NYFed Response Content-Type: {content_type}")
                response.raise_for_status()

                # 檢查 Content-Type 是否為 Excel
                if not any(excel_type in content_type for excel_type in ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/octet-stream']):
                    # 'application/octet-stream' 有時也用於二進制檔案如下載
                    print(f"警告: 下載的檔案 {url} Content-Type ({content_type}) 非預期 Excel 格式，跳過。")
                    # 不要立即 continue，而是讓後續的 recipe 檢查也執行，或者可以選擇在這裡 return/continue
                    # 為了保持與原邏輯相似，讓它嘗試讀取，如果失敗則由 try-except 捕獲
                    # 或者更主動地跳過：
                    # continue

                recipe = self.recipes.get(format_type)
                if not recipe:
                    print(f"警告: 找不到檔案 {url} 的處理配方 '{format_type}'，跳過。")
                    continue

                # 只有當 Content-Type 看起來合理時才嘗試讀取
                if not any(excel_type in content_type for excel_type in ['application/vnd.ms-excel', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'application/octet-stream']):
                    print(f"由於 Content-Type 不匹配，實際跳過處理 NYFed 檔案: {url}")
                    continue

                excel_file = BytesIO(response.content)
                df = pd.read_excel(excel_file, header=recipe['header_row'] - 1, engine='openpyxl')
                df = df.rename(columns={recipe['date_column']: 'metric_date'})
                df['metric_date'] = pd.to_datetime(df['metric_date'], errors='coerce').dt.date
                df = df.dropna(subset=['metric_date'])

                cols_to_sum = [col for col in recipe['columns_to_sum'] if col in df.columns]
                for col in cols_to_sum:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

                df['metric_value'] = df[cols_to_sum].sum(axis=1) * 1000
                all_positions_df.append(df[['metric_date', 'metric_value']].copy())

            if not all_positions_df:
                return None, "未能成功處理任何 NYFed 檔案。"

            combined_df = pd.concat(all_positions_df).sort_values('metric_date').drop_duplicates(subset=['metric_date'], keep='last')
            combined_df = combined_df.set_index('metric_date')
            date_range = pd.date_range(start=combined_df.index.min(), end=combined_df.index.max(), freq='D')
            daily_df = combined_df.reindex(date_range).ffill().reset_index().rename(columns={'index': 'metric_date'})
            daily_df['metric_name'] = f"{self.source_api_name}/PRIMARY_DEALER_NET_POSITION"
            return daily_df, None
        except requests.exceptions.RequestException as e:
            return None, f"下載 NYFed 數據時發生網路錯誤: {e}"
        except Exception as e:
            return None, f"處理 NYFed Excel 檔案時發生錯誤: {e}"
