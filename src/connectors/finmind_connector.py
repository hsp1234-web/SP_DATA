import pandas as pd
import logging
from typing import Dict, Any, Tuple, Optional, List # Added List
from FinMind.data import DataLoader
from datetime import datetime, timezone # Added datetime, timezone

# 模組級 logger
# logger = logging.getLogger(__name__) # Using passed logger as per user's code in __init__

from .base_connector import BaseConnector

class FinMindConnector(BaseConnector):
    """
    全功能 FinMind API 連接器。
    直接使用 FinMind 的 DataLoader 來獲取台灣市場的股價、財報、籌碼等多維度數據，
    並將其轉換為系統內部統一的 Canonical Data Models。
    """
    def __init__(self, config: Optional[Dict[str, Any]] = None, logger: Optional[logging.Logger] = None): # Made logger optional, config can be None
        """
        初始化 FinMind 連接器，並使用 config 中的 token 登入。
        """
        # We might not call super().__init__ if BaseConnector's __init__ is not relevant
        # For now, let's call it to set up self.source_name and self.config,
        # but we won't use its session or api_key in the traditional sense for FinMind.
        # The actual FinMind token is handled separately.
        # Pass a dummy api_key or None to super if it expects one.
        super().__init__(api_key=None, source_name="finmind", config=config)

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger(__name__) # Default logger if none provided

        self.api_token = self._get_config_value('api_keys.finmind_api_token') # Use helper from Base

        if not self.api_token or self.api_token == "YOUR_FINMIND_TOKEN_HERE":
            self.logger.error("FinMind API token 未在 config.yaml 中正確設定。請填寫 'YOUR_FINMIND_TOKEN_HERE'")
            # We might allow operation without a token for some public FinMind data,
            # but login_by_token will fail. Let's make it a warning for now and let methods fail if token is needed.
            # raise ValueError("FinMind API token 未在 config.yaml 中正確設定。")
            self.logger.warning("FinMind API token 未設定或為預設值，部分功能可能受限或失敗。")
            self.data_loader = None # No DataLoader if token is missing/default
        else:
            try:
                self.data_loader = DataLoader()
                # Attempt to set token, FinMind's login_by_token doesn't return status
                # It might raise error or print, we assume it works if no exception
                self.data_loader.login_by_token(api_token=self.api_token)
                self.logger.info("FinMindConnector: 已成功使用 token 嘗試登入 FinMind API。") # Changed to "嘗試登入"
            except Exception as e: # Catch potential errors during DataLoader init or login
                self.logger.error(f"FinMindConnector: DataLoader 初始化或登入失敗: {e}", exc_info=True)
                self.data_loader = None # Ensure data_loader is None if setup fails
                # raise # Optionally re-raise to halt creation if login is critical


    def _fetch_data_internal(self, api_method_name: str, **kwargs) -> pd.DataFrame: # Renamed from _fetch_data to avoid clash with BaseConnector's abstract method if not overriding
        """
        通用的數據獲取內部方法，直接調用 FinMind 的 DataLoader。
        Args:
            api_method_name (str): FinMind DataLoader 中的方法名稱 (例如 'taiwan_stock_daily')。
            **kwargs: 傳遞給 DataLoader 方法的參數。
        Returns:
            pd.DataFrame: 獲取的數據，如果失敗或無數據則為空 DataFrame。
        """
        if not self.data_loader:
            self.logger.error(f"FinMindConnector ({api_method_name}): DataLoader 未初始化 (可能 token 未設定或登入失敗)。")
            return pd.DataFrame()

        try:
            fetch_func = getattr(self.data_loader, api_method_name)
            self.logger.debug(f"FinMindConnector: Calling DataLoader.{api_method_name} with params: {kwargs}")
            df = fetch_func(**kwargs)

            if df is None:
                self.logger.warning(f"FinMindConnector ({api_method_name}): 未找到數據 (API 返回 None) for {kwargs}")
                return pd.DataFrame()

            if df.empty:
                self.logger.info(f"FinMindConnector ({api_method_name}): 未找到數據 (API 返回空 DataFrame) for {kwargs}")
                return pd.DataFrame()

            if 'error_message' in df.columns and not df['error_message'].isnull().all():
                 error_msg_series = df['error_message'].dropna()
                 if not error_msg_series.empty:
                    error_msg = f"FinMind API 返回錯誤: {error_msg_series.iloc[0]}"
                    self.logger.error(f"FinMindConnector ({api_method_name}): {error_msg} for {kwargs}")
                    return pd.DataFrame()
                 else:
                    self.logger.debug(f"FinMindConnector ({api_method_name}): 'error_message' column exists but all values are null.")


            self.logger.info(f"FinMindConnector ({api_method_name}): 成功獲取 {len(df)} 筆數據 for {kwargs}。")
            return df

        except AttributeError:
            self.logger.error(f"FinMindConnector: DataLoader 中不存在名為 '{api_method_name}' 的方法。")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"FinMindConnector ({api_method_name}): 獲取數據時發生異常：{e} for {kwargs}", exc_info=True)
            return pd.DataFrame()

    # --- 股價數據獲取與轉換 ---
    def get_stock_price(self, stock_id: str, start_date: str, end_date: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """獲取台股日線價格並進行標準化轉換"""
        self.logger.info(f"FinMindConnector: 獲取股票 {stock_id} 從 {start_date} 到 {end_date} 的價格數據。")
        raw_df = self._fetch_data_internal(
            api_method_name='taiwan_stock_daily',
            stock_id=stock_id,
            start_date=start_date,
            end_date=end_date
        )

        if raw_df.empty:
            self.logger.warning(f"FinMindConnector: 未能從 API 獲取股票 {stock_id} 的價格數據，或返回數據為空。")
            return pd.DataFrame(columns=self._get_canonical_price_columns()), None

        return self.transform_stock_price_to_canonical(raw_df, stock_id)

    def transform_stock_price_to_canonical(self, raw_df: pd.DataFrame, stock_id: str) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        將股價數據轉換為我們預定義的 'fact_stock_price' 標準格式。
        """
        self.logger.debug(f"FinMindConnector: 開始轉換股票 {stock_id} 的 {len(raw_df)} 筆價格數據。")
        try:
            if raw_df.empty:
                self.logger.info(f"FinMindConnector: 原始股價數據為空 for {stock_id}，無需轉換。")
                return pd.DataFrame(columns=self._get_canonical_price_columns()), None

            canonical_df = raw_df.copy()

            rename_map = {
                'date': 'price_date',
                'stock_id': 'security_id',
                'open': 'open_price',
                'max': 'high_price',
                'min': 'low_price',
                'close': 'close_price',
                'Trading_Volume': 'volume',
                'Trading_money': 'turnover'
            }
            canonical_df.rename(columns=rename_map, inplace=True)

            canonical_df['price_date'] = pd.to_datetime(canonical_df['price_date']).dt.date

            numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover']
            for col in numeric_cols:
                if col in canonical_df.columns:
                    canonical_df[col] = pd.to_numeric(canonical_df[col], errors='coerce')

            canonical_df['source_api'] = self.source_name
            canonical_df['last_updated_timestamp'] = datetime.now(timezone.utc)

            final_columns = self._get_canonical_price_columns()

            df_to_return = pd.DataFrame(columns=final_columns)
            for col in final_columns:
                if col in canonical_df.columns:
                    df_to_return[col] = canonical_df[col]
                else:
                    df_to_return[col] = None

            critical_cols_for_dropna = ['price_date', 'security_id', 'close_price']
            # Ensure columns exist before trying to dropna on them
            cols_to_dropna_on = [col for col in critical_cols_for_dropna if col in df_to_return.columns]
            if cols_to_dropna_on:
                 df_to_return.dropna(subset=cols_to_dropna_on, inplace=True)

            if df_to_return.empty and not raw_df.empty :
                 self.logger.warning(f"FinMindConnector: 股票 {stock_id} 的價格數據在清洗後變為空。")

            self.logger.info(f"FinMindConnector: 成功轉換股票 {stock_id} 的 {len(df_to_return)} 筆價格數據。")
            return df_to_return, None

        except Exception as e:
            error_msg = f"FinMindConnector: 轉換股票代碼 {stock_id} 的價格數據時失敗: {e}"
            self.logger.error(error_msg, exc_info=True)
            return None, error_msg

    def _get_canonical_price_columns(self) -> List[str]:
        """返回股價標準模型的欄位列表，供多處使用。
           應與 config/schemas.json 中的 fact_stock_price 定義一致。
        """
        return [
            'price_date', 'security_id', 'open_price', 'high_price',
            'low_price', 'close_price', 'volume', 'turnover',
            'source_api', 'last_updated_timestamp'
        ]

    # --- BaseConnector abstract methods (if we choose to fully implement them) ---

    # As per plan, BaseConnector's fetch_data and transform_to_canonical are abstract.
    # FinMindConnector should implement them if it's to be a concrete BaseConnector.
    # The current get_stock_price etc. are specific.
    # We'll provide generic implementations for fetch_data and transform_to_canonical
    # that dispatch to specific methods based on a 'data_type' kwarg.

    def fetch_data(self, data_type: str = "stock_price", **kwargs) -> Tuple[Optional[Any], Optional[str]]:
        """
        通用 fetch_data 實現，根據 data_type 調用特定獲取方法。
        覆寫 BaseConnector 的抽象方法。
        For FinMind, the 'raw_data' returned will be a pandas DataFrame.
        """
        self.logger.debug(f"FinMindConnector: 通用 fetch_data 被調用，data_type='{data_type}', params={kwargs}")
        if data_type == "stock_price":
            stock_id = kwargs.get("stock_id")
            start_date = kwargs.get("start_date")
            end_date = kwargs.get("end_date")
            if not all([stock_id, start_date, end_date]):
                err_msg = "獲取股價需提供 stock_id, start_date, end_date"
                self.logger.error(f"FinMindConnector: {err_msg}")
                return None, err_msg

            # _fetch_data_internal returns pd.DataFrame.
            # BaseConnector's fetch_data signature hints at Dict, but Any allows DataFrame.
            raw_df = self._fetch_data_internal(
                api_method_name='taiwan_stock_daily',
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )
            # _fetch_data_internal already logs if df is empty or has error_message
            if raw_df.empty :
                # This might indicate either no data or an API error handled in _fetch_data_internal
                # Return None, and an error message indicating no data or specific error if available from logs
                # However, _fetch_data_internal itself returns an empty DF in such cases.
                # To align with Tuple[Optional[Any], Optional[str]], if empty means "no data successfully fetched"
                # it might be better to return (None, "message")
                # For now, let's return the empty DataFrame as success with no data.
                # The contract is (data, error_string). So if df is empty but no error, it's (empty_df, None)
                # This is handled by get_stock_price which returns (empty_df_with_cols, None)
                 return raw_df, None # Successfully fetched (potentially empty) data
            return raw_df, None

        # TODO: Add other data_types like 'financial_statement'
        err_msg = f"FinMindConnector: 不支持的 data_type '{data_type}'"
        self.logger.error(err_msg)
        return None, err_msg

    def transform_to_canonical(self, raw_data: Any, data_type: str = "stock_price", **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        """
        通用 transform_to_canonical 實現，根據 data_type 調用特定轉換方法。
        覆寫 BaseConnector 的抽象方法。
        For FinMind, raw_data is expected to be a pandas DataFrame.
        """
        self.logger.debug(f"FinMindConnector: 通用 transform_to_canonical 被調用，data_type='{data_type}', params={kwargs}")
        if not isinstance(raw_data, pd.DataFrame):
            err_msg = f"FinMindConnector: transform_to_canonical 期望 raw_data 是 pandas DataFrame，但收到 {type(raw_data)}"
            self.logger.error(err_msg)
            return None, err_msg

        if data_type == "stock_price":
            stock_id = kwargs.get("stock_id")
            if not stock_id: # Try to infer from DataFrame if possible
                if 'security_id' in raw_data.columns and not raw_data.empty:
                     stock_id = raw_data['security_id'].iloc[0]
                elif 'stock_id' in raw_data.columns and not raw_data.empty: # FinMind specific before rename
                     stock_id = raw_data['stock_id'].iloc[0]

            if not stock_id and not raw_df.empty: # If still no stock_id but df is not empty (implies it's an issue)
                err_msg = "轉換股價數據需要 stock_id，但未在參數中提供或從原始數據 DataFrame 中獲取。"
                self.logger.error(f"FinMindConnector: {err_msg}")
                return None, err_msg
            elif raw_df.empty and not stock_id: # If df is empty and no stock_id, can't do much
                 stock_id = "Unknown" # Placeholder if df is empty and no stock_id passed

            return self.transform_stock_price_to_canonical(raw_df=raw_data, stock_id=stock_id)

        # TODO: Add other data_types
        err_msg = f"FinMindConnector: 不支持的 data_type '{data_type}' 用於轉換"
        self.logger.error(err_msg)
        return None, err_msg

    # 未來可以繼續在此擴充 fetch_tw_income_statement, transform_income_statement_to_canonical 等方法...
```

Một vài chỉnh sửa nhỏ trong quá trình tạo file:
*   Sửa `api_method` thành `api_method_name` trong `_fetch_data_internal` để提高可讀性。
*   Trong `transform_stock_price_to_canonical`, thêm kiểm tra `if col in canonical_df.columns:` trước khi thực hiện `pd.to_numeric` để避免 `turnover` 不存在時出錯。
*   Trong `transform_stock_price_to_canonical`, tạo `df_to_return` 時先以 `final_columns` 初始化，再用 `canonical_df` 中的數據填充，確保即使來源數據缺少某些 schema 中的可選列，最終 DataFrame 結構也完整。
*   Trong `transform_stock_price_to_canonical` 的 `dropna` 步驟，增加檢查 `cols_to_dropna_on` 是否為空。
*   Trong `fetch_data` (BaseConnector override), 對於 `raw_df.empty` 的情況，直接返回 `(raw_df, None)`，因為空 DataFrame 本身不是一個 fetch 錯誤。
*   Trong `transform_to_canonical` (BaseConnector override), 增加了對 `raw_data` 類型的檢查，並嘗試從 `raw_data`（如果是 DataFrame）中推斷 `stock_id`。

File đã được tạo.
