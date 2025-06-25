import pandas as pd
import numpy as np
from typing import Dict, Any, Optional

class IndicatorEngine:
    def __init__(self, params: Dict[str, Any], data_frames: Dict[str, pd.DataFrame]):
        self.params = params
        self.macro_df = data_frames.get('macro')
        self.move_df = data_frames.get('move')
        self.window = self.params.get('rolling_window_days', 252)
        self.weights = self.params.get('stress_index_weights', {})

    def _prepare_data(self) -> pd.DataFrame:
        macro_wide = self.macro_df.pivot_table(index='metric_date', columns='metric_name', values='metric_value').rename_axis('date')
        move_wide = self.move_df.rename(columns={'price_date': 'date', 'close_price': '^MOVE'}).set_index('date')[['^MOVE']]

        df = pd.merge(macro_wide, move_wide, on='date', how='left').sort_index()
        df = df.ffill().dropna(how='all')
        return df

    def calculate_dealer_stress_index(self) -> Optional[pd.DataFrame]:
        if self.macro_df is None or self.move_df is None or self.macro_df.empty or self.move_df.empty:
            print("錯誤: 缺少計算壓力指數所需的數據。")
            return None

        df = self._prepare_data()

        # 衍生指標計算
        df['spread_10y2y'] = df.get('FRED/DGS10', np.nan) - df.get('FRED/DGS2', np.nan)
        sofr_ma = df.get('FRED/SOFR', pd.Series(dtype=float)).rolling(window=20).mean()
        df['sofr_deviation'] = df.get('FRED/SOFR', np.nan) - sofr_ma
        df['pos_res_ratio'] = df.get('NYFED/PRIMARY_DEALER_NET_POSITION', np.nan) / df.get('FRED/WRESBAL', np.nan).replace(0, np.nan)
        df.rename(columns={'^MOVE': 'move_index', 'FRED/VIXCLS': 'vix_index', 'NYFED/PRIMARY_DEALER_NET_POSITION': 'primary_dealer_position'}, inplace=True)

        requested_indicator_cols = list(self.weights.keys())

        # 只選取實際存在於 df 中的指標列
        available_indicator_cols = [col for col in requested_indicator_cols if col in df.columns]

        if not available_indicator_cols:
            print("錯誤: 在DataFrame中找不到任何請求的指標列。")
            return None

        missing_cols = set(requested_indicator_cols) - set(available_indicator_cols)
        current_weights = self.weights.copy() # 使用副本以避免修改原始設定（如果引擎被多次調用）

        if missing_cols:
            print(f"警告: 以下指標列在數據中缺失，將不會用於計算: {missing_cols}")
            current_weights = {k: v for k, v in current_weights.items() if k in available_indicator_cols}

        if not current_weights or sum(current_weights.values()) == 0:
            print("錯誤: 清洗後沒有有效的權重可用於計算壓力指數。")
            return None

        # 使用實際可用的列進行後續計算
        indicators_df = df[available_indicator_cols].dropna(how='any') # drop rows if any of the available indicators are NaN

        if indicators_df.empty:
            print("錯誤: 清洗和準備後（dropna），沒有足夠的數據來計算指標。")
            return None

        # 計算滾動百分位
        percentile_df = indicators_df.rolling(window=self.window, min_periods=int(self.window/2)).apply(
            lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False
        )

        # 計算加權指數
        # weight_series = pd.Series(self.weights) # 舊的， self.weights 可能未被修改或修改不當
        weight_series = pd.Series(current_weights) # 使用從請求的權重過濾後得到的 current_weights

        # 確保 percentile_df 和 weight_series 的列/索引對齊
        # percentile_df 的列是 available_indicator_cols
        # weight_series 的索引也是 available_indicator_cols
        # 所以它們應該是對齊的

        stress_index = (percentile_df[available_indicator_cols] * weight_series[available_indicator_cols]).sum(axis=1) / sum(current_weights.values())

        result_df = pd.DataFrame({'DealerStressIndex': stress_index})
        return result_df.join(percentile_df.add_suffix('_pct')).dropna()
