import yfinance as yf
import pandas as pd
from typing import List, Dict, Any, Tuple, Optional
from .base import BaseConnector

class YFinanceConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.source_api_name = "yfinance"

    def fetch_data(self, tickers: List[str], start_date: str, end_date: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        try:
            tickers_str = " ".join(tickers)
            # auto_adjust=True by default with yfinance >0.2.30, explicitly set for clarity
            # group_by='ticker' is convenient for iterating through multi-ticker results
            data = yf.download(tickers_str, start=start_date, end=end_date, progress=False, auto_adjust=True, group_by='ticker')

            if data.empty:
                return None, f"yfinance 未返回任何代碼的數據: {tickers}"

            all_dfs = []
            if len(tickers) == 1:
                ticker_name = tickers[0]
                df = data.copy()
                # 如果 yf.download 對單 ticker 且 group_by='ticker' 時返回 MultiIndex 列 (ticker_name, measure)
                # 則需要扁平化列名
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [col[1] if isinstance(col, tuple) and len(col) > 1 else col for col in df.columns]
                df['Ticker'] = ticker_name
                df = df.reset_index() # Date 索引變為列 'Date'
                all_dfs.append(df)
            else: # 多個 tickers
                # data.columns 應該是 MultiIndex: (ticker, OHLCV)
                if isinstance(data.columns, pd.MultiIndex) and data.columns.nlevels > 1:
                    returned_tickers = data.columns.levels[0]
                    for ticker_name in tickers:
                        if ticker_name in returned_tickers:
                            ticker_df = data[ticker_name].copy() # 列是 OHLCV (字符串)
                            ticker_df['Ticker'] = ticker_name
                            ticker_df = ticker_df.reset_index() # Date 索引變為列 'Date'
                            all_dfs.append(ticker_df)
                else: # 預期外的列結構 (例如，多 tickers 但返回了扁平列)
                    print(f"警告: yfinance 為多個 tickers 返回了非預期的列結構: {data.columns}")
                    # 可以嘗試基於現有列進行處理，或直接返回錯誤/空數據
                    # 為簡單起見，如果結構不符預期，這裡可能導致 all_dfs 為空

            if not all_dfs:
                return None, f"yfinance 處理後，沒有任何請求代碼的有效數據: {tickers}"

            final_df = pd.concat(all_dfs, ignore_index=True)
            print(f"yfinance_connector: final_df columns AFTER concat: {final_df.columns.tolist()}")

            # 重命名列以匹配資料庫模式
            final_df = final_df.rename(columns={
                'Date': 'price_date',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price', # auto_adjust=True 使 Close 價格已調整
                'Volume': 'volume',
                'Ticker': 'security_id'
            })

            # 由於 auto_adjust=True，'close_price' 已經是調整後的價格。
            # 資料庫綱要期望有 'adj_close_price'，我們將其設為與 'close_price' 相同。
            if 'close_price' in final_df.columns:
                final_df['adj_close_price'] = final_df['close_price']
            else:
                # 如果連 close_price 都沒有，這是不正常的，但為了避免 KeyError，填充 NaN
                final_df['adj_close_price'] = pd.NA


            # 選擇並排序最終的列，以符合資料庫綱要
            # 確保所有綱要中定義的列都存在，如果原始數據沒有某列（例如 Volume），則填充 pd.NA
            schema_cols = ['price_date', 'security_id', 'open_price', 'high_price', 'low_price', 'close_price', 'adj_close_price', 'volume']

            print(f"yfinance_connector: final_df columns before schema adjustment: {final_df.columns.tolist()}")
            print(f"yfinance_connector: final_df is empty before schema adjustment: {final_df.empty}")
            if not final_df.empty:
                print(f"yfinance_connector: final_df length before schema adjustment: {len(final_df)}")
                # print(f"yfinance_connector: final_df info before schema adjustment:")
                # final_df.info() # .info() prints to stdout, might be too verbose for direct return

            for col in schema_cols:
                if col not in final_df.columns:
                    final_df[col] = pd.NA # 或 np.nan，根據下游期望的類型

            print(f"yfinance_connector: final_df columns after adding missing schema cols: {final_df.columns.tolist()}")

            final_df = final_df[schema_cols] # 按綱要順序選擇列

            print(f"yfinance_connector: final_df columns after selecting schema_cols: {final_df.columns.tolist()}")

            final_df['price_date'] = pd.to_datetime(final_df['price_date']).dt.date

            # 移除所有列都為 NA 的行 (如果有的話)
            final_df.dropna(subset=schema_cols, how='all', inplace=True)

            if final_df.empty:
                return None, f"yfinance 數據處理完畢後為空，代碼: {tickers}"

            return final_df, None
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            detailed_error_msg = f"使用 yfinance 獲取數據時發生意外錯誤: {type(e).__name__} - {e}. Traceback: {tb_str}"
            print(detailed_error_msg) # 打印詳細錯誤以供調試
            return None, detailed_error_msg
