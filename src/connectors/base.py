from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class BaseConnector(ABC):
    """
    所有數據連接器的抽象基類。
    定義了標準接口，確保所有 Connector 的行為一致。
    """

    def __init__(self, config: Dict[str, Any], source_api_name: str = "Unknown"):
        self.config = config
        self.source_api_name = source_api_name
        # Logger can be passed by the child class or DataManager for better context
        # For now, child classes will initialize their own loggers or use a global one.

    @abstractmethod
    def fetch_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        從 API 或數據源獲取原始數據並進行初步轉換成 DataFrame (通常是長表格式)。

        Args:
            start_date (Optional[str]): 數據獲取的開始日期 (YYYY-MM-DD)。
            end_date (Optional[str]): 數據獲取的結束日期 (YYYY-MM-DD)。此日期為數據上限，不應包含此日期之後的數據。
            **kwargs: 特定 connector 需要的參數 (例如 series_ids, tickers)。

        Returns:
            一個包含 (DataFrame, error_message) 的元組。
            成功時，DataFrame 包含獲取和初步標準化的數據，error_message 為 None。
            失敗時，DataFrame 為 None 或空的 DataFrame (帶有預期欄位)，error_message 包含錯誤信息。
            DataFrame 應包含 'source_api' 和 'data_snapshot_timestamp' (UTC) 欄位。
            對於時間序列數據，應有 'metric_date' 或 'price_date'。
            對於宏觀/因子數據，應有 'metric_name'。
            對於股價數據，應有 'security_id'。
        """
        pass

    def get_source_name(self) -> str:
        """返回數據源的名稱。"""
        return self.source_api_name
