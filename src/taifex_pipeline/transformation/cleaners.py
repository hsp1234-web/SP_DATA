import pandas as pd
import numpy as np
import logging

logger = logging.getLogger("taifex_pipeline.transformation.cleaners")

def clean_daily_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗並標準化每日行情數據 (Daily OHLCV) 的 DataFrame。

    Args:
        df (pd.DataFrame): 從原始檔案解析出來的 DataFrame。

    Returns:
        pd.DataFrame: 清洗和標準化後的 DataFrame。
    """
    if df.empty:
        logger.info("傳入的 DataFrame 為空，直接返回。")
        return df

    cleaned_df = df.copy()
    logger.info(f"開始清洗每日行情數據，原始 DataFrame shape: {cleaned_df.shape}")

    # 欄位對映字典 (持續擴充)
    # Key: 原始中文/非標準欄位名 (小寫以方便匹配)
    # Value: 標準英文欄位名
    COLUMN_MAP = {
        # 日期與契約
        '交易日期': 'trading_date',
        '日期': 'trading_date', # 常見的另一種表示
        '契約': 'product_id',
        '商品代號': 'product_id',
        '到期月份(週別)': 'contract_month_week', # 例如 202303, 202303W1
        '履約價': 'strike_price',
        '買賣權': 'option_type', # 買權/賣權

        # OHLCV + 結算價 + 未平倉
        '開盤價': 'open',
        '最高價': 'high',
        '最低價': 'low',
        '收盤價': 'close',
        '成交量': 'volume',
        '結算價': 'settlement_price',
        '未平倉契約量': 'open_interest',
        '最後最佳買價': 'best_bid_price', # 可能出現
        '最後最佳賣價': 'best_ask_price', # 可能出現

        # 漲跌
        '漲跌價': 'change', # 例如 +2.5, -1.0, 或直接是數字
        '漲跌%': 'percent_change', # 例如 +0.1%, -2.3%
        '漲跌': 'change', # 另一種簡寫

        # 其他可能欄位 (根據不同檔案格式補充)
        '商品名稱': 'product_name', # 例如 "臺股期貨"
        '中文簡稱': 'product_name_cn_short',
        '英文簡稱': 'product_name_en_short',
        '上市日期': 'listing_date',
        '最後交易日': 'last_trading_date',
        '交割月份': 'delivery_month', # 有些檔案可能用這個
        '身份碼': 'participant_id_code', # 例如在三大法人資料中
        # ... 更多可能的欄位
    }

    # 為了處理原始欄位名大小寫不一致的問題，先將 df 的欄位名轉為小寫進行匹配
    # 但重命名時要用原始的 case-insensitive map key
    # 或者，更好的方式是將 COLUMN_MAP 的 key 也都轉為小寫
    lower_case_column_map = {k.lower(): v for k, v in COLUMN_MAP.items()}

    # 收集需要重命名的欄位
    rename_dict = {}
    for col in cleaned_df.columns:
        col_lower = col.strip().lower() # 原始欄位名也去空白轉小寫
        if col_lower in lower_case_column_map:
            rename_dict[col] = lower_case_column_map[col_lower]

    if rename_dict:
        cleaned_df.rename(columns=rename_dict, inplace=True)
        logger.info(f"欄位已重命名: {rename_dict}")
    else:
        logger.info("沒有欄位需要重命名 (根據目前的 COLUMN_MAP)。")

    # 數值欄位列表 (標準化後的名稱)
    NUMERIC_COLS = [
        'open', 'high', 'low', 'close', 'volume',
        'settlement_price', 'open_interest', 'strike_price',
        'change',
        'best_bid_price', 'best_ask_price'
        # 'percent_change' 比較特殊，因包含 % 符號，需要先移除
    ]

    for col in NUMERIC_COLS:
        if col in cleaned_df.columns:
            logger.debug(f"嘗試轉換欄位 '{col}' 為數值型...")
            # 1. 轉換為字串以使用 .str accessor
            cleaned_df[col] = cleaned_df[col].astype(str)
            # 2. 移除千分位逗號 (,)
            cleaned_df[col] = cleaned_df[col].str.replace(',', '', regex=False)
            # 3. 將 '-' (或其他代表 NaN 的字元，例如 'NaN', 'null', 空字串) 替換為 np.nan
            #    pd.to_numeric 會自動處理空字串為 NaN (配合 errors='coerce')
            #    主要處理 '-'
            cleaned_df[col] = cleaned_df[col].replace({'-': np.nan, 'N/A': np.nan, '盤後': np.nan, ' ':np.nan}) # 擴充需要被視為 NaN 的值
            # 4. 轉換為數值，無法轉換的設為 NaN
            cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
            logger.debug(f"欄位 '{col}' 轉換後 dytpe: {cleaned_df[col].dtype}, "
                         f"NaN 數量: {cleaned_df[col].isna().sum()}")
        else:
            logger.debug(f"數值轉換目標欄位 '{col}' 不存在於 DataFrame 中，跳過。")

    # 特殊處理 'percent_change' 欄位 (如果存在)
    if 'percent_change' in cleaned_df.columns:
        logger.debug("嘗試轉換欄位 'percent_change'...")
        cleaned_df['percent_change'] = cleaned_df['percent_change'].astype(str)
        cleaned_df['percent_change'] = cleaned_df['percent_change'].str.replace(',', '', regex=False) # 移除千分位
        cleaned_df['percent_change'] = cleaned_df['percent_change'].str.rstrip('%') # 移除百分比符號
        cleaned_df['percent_change'] = cleaned_df['percent_change'].replace({'-': np.nan, 'N/A': np.nan, '盤後': np.nan, ' ':np.nan})
        cleaned_df['percent_change'] = pd.to_numeric(cleaned_df['percent_change'], errors='coerce')
        # 轉換為實際小數值 (例如 0.05 而非 5)
        # cleaned_df['percent_change'] = cleaned_df['percent_change'] / 100.0 # 視需求決定是否除以100
        logger.debug(f"欄位 'percent_change' 轉換後 dytpe: {cleaned_df['percent_change'].dtype}, "
                     f"NaN 數量: {cleaned_df['percent_change'].isna().sum()}")


    # 日期欄位轉換
    if 'trading_date' in cleaned_df.columns:
        logger.debug("嘗試轉換欄位 'trading_date' 為日期型...")
        # 先處理可能的 'YYYY年MM月DD日' 或 'YYY/MM/DD' (民國年轉西元)
        def convert_roc_date(date_str):
            if isinstance(date_str, str):
                if '年' in date_str and '月' in date_str and '日' in date_str:
                    parts = date_str.replace('年', '-').replace('月', '-').replace('日', '').split('-')
                    if len(parts) == 3:
                        try:
                            # 假設是民國年
                            year = int(parts[0])
                            if year < 1911: # 避免過小的年份被錯誤轉換
                                year += 1911
                            return f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                        except ValueError:
                            return date_str # 轉換失敗返回原值
                elif '/' in date_str: # 處理 YYY/MM/DD
                    parts = date_str.split('/')
                    if len(parts) == 3:
                        try:
                            year = int(parts[0])
                            if year < 1911: # 民國年
                                year += 1911
                            return f"{year}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                        except ValueError:
                            return date_str
            return date_str

        # cleaned_df['trading_date'] = cleaned_df['trading_date'].astype(str).apply(convert_roc_date)
        # pd.to_datetime 能夠處理多種格式，但指定 format 更安全
        # 嘗試幾種常見格式，或者讓 pandas 自動推斷
        # 常見格式: YYYYMMDD, YYYY/MM/DD, YYYY-MM-DD
        # 先將其轉為字串以防萬一
        cleaned_df['trading_date'] = cleaned_df['trading_date'].astype(str).str.strip()
        cleaned_df['trading_date'] = cleaned_df['trading_date'].str.replace(' ', '') # 移除所有空白
        cleaned_df['trading_date'] = cleaned_df['trading_date'].replace({'-':pd.NaT, '':pd.NaT})

        # 嘗試轉換，讓 errors='coerce' 處理無法解析的日期
        original_dates = cleaned_df['trading_date'].copy()
        converted_dates = pd.to_datetime(cleaned_df['trading_date'], errors='coerce')

        # 檢查是否有轉換失敗的 (NaT) 但原值並非 NaN 或空
        failed_conversion_mask = converted_dates.isna() & original_dates.notna() & (original_dates != '')
        if failed_conversion_mask.any():
            logger.warning(f"欄位 'trading_date' 中有 {failed_conversion_mask.sum()} 個值無法被 pd.to_datetime 自動轉換。 "
                           f"範例無法轉換值: {original_dates[failed_conversion_mask].unique()[:5]}")
            # 這裡可以加入更複雜的日期解析邏輯，或保留 NaT

        cleaned_df['trading_date'] = converted_dates
        logger.debug(f"欄位 'trading_date' 轉換後 dytpe: {cleaned_df['trading_date'].dtype}, "
                     f"NaT 數量: {cleaned_df['trading_date'].isna().sum()}")
    else:
        logger.warning("日期欄位 'trading_date' 不存在於 DataFrame 中。")


    # 內容標準化: option_type
    if 'option_type' in cleaned_df.columns:
        logger.debug("標準化欄位 'option_type'...")
        option_type_map = {
            '買權': 'C',
            '賣權': 'P',
            'CALL': 'C', # 處理英文情況
            'PUT': 'P',
            # 其他可能的表示，例如簡體中文或其他語言，可以加入
        }
        # 先將欄位轉為字串並去除空白
        cleaned_df['option_type'] = cleaned_df['option_type'].astype(str).str.strip().str.upper() # 統一轉大寫方便 map

        # 使用 .map() 或 .replace()。 .map() 對於不在 map 中的值會變 NaN，.replace() 則保留原值。
        # 這裡我們希望不在 map 中的值也保留，所以用 replace 更好，或者 map 後 fillna
        original_option_types = cleaned_df['option_type'].copy()
        cleaned_df['option_type'] = cleaned_df['option_type'].replace(option_type_map)

        # 檢查是否有未被成功映射的值 (非 'C' 或 'P'，且原值存在)
        unmapped_mask = ~cleaned_df['option_type'].isin(['C', 'P']) & \
                        original_option_types.notna() & \
                        (original_option_types != '') & \
                        (original_option_types.str.upper().isin(option_type_map.keys())) # 確保是我們試圖 map 的值

        # 更精確的 unmapped 檢查：如果原值在 map 的 key 中，但轉換後不是 C/P
        actually_unmapped_values = []
        for original_val, mapped_val in zip(original_option_types, cleaned_df['option_type']):
            if isinstance(original_val, str) and original_val.upper() in option_type_map and mapped_val not in ['C', 'P']:
                actually_unmapped_values.append(original_val)

        if actually_unmapped_values:
             logger.warning(f"欄位 'option_type' 中有 {len(actually_unmapped_values)} 個值未成功標準化為 'C' 或 'P'，"
                            f"儘管它們可能在映射表中。範例值: {list(set(actually_unmapped_values))[:5]}")
        logger.debug(f"欄位 'option_type' 標準化完成。 C: {(cleaned_df['option_type'] == 'C').sum()}, P: {(cleaned_df['option_type'] == 'P').sum()}")
    else:
        logger.debug("欄位 'option_type' 不存在，跳過標準化。")

    # 處理可選欄位：這部分主要是在前面的欄位檢查 (if col in cleaned_df.columns) 中體現。
    # 不需要額外的步驟，因為如果欄位不存在，前面的轉換就會跳過。

    # 移除無效數據
    # 關鍵必填欄位列表 (標準化後的名稱)
    # product_id 對於某些資料 (如大盤指數) 可能不存在，所以這裡只用 trading_date
    # 或是根據檔案類型決定 critical_cols
    CRITICAL_COLS = ['trading_date'] # 至少要有交易日期
    if 'product_id' in cleaned_df.columns: # 如果 product_id 存在，也將其視為關鍵
        CRITICAL_COLS.append('product_id')

    # 如果是選擇權，履約價和買賣權也應該是關鍵的
    # 但這裡的清洗函式是通用的每日行情，不特別為選擇權加規則
    # 可以由呼叫者根據 recipe 的類型決定更嚴格的 dropna 條件

    original_rows = len(cleaned_df)
    cleaned_df.dropna(subset=CRITICAL_COLS, how='any', inplace=True)
    rows_dropped = original_rows - len(cleaned_df)
    if rows_dropped > 0:
        logger.info(f"移除了 {rows_dropped} 行在關鍵欄位 ({CRITICAL_COLS}) 中包含 NaN 的數據。")
    else:
        logger.info(f"沒有行因關鍵欄位 ({CRITICAL_COLS}) 為 NaN 而被移除。")


    logger.info(f"每日行情數據清洗完成。處理後 DataFrame shape: {cleaned_df.shape}")
    return cleaned_df

if __name__ == '__main__':
    # 簡易測試 (更完整的測試應在 test_cleaners.py 中)
    logging.basicConfig(level=logging.DEBUG)

    sample_data = {
        '交易日期': ['2023/01/01', '20230102', '112年01月03日', '2023-01-04', None, '2023/01/06'],
        '契約': ['TXFA3', 'TXFB3', 'TXFC3', 'TXFD3', 'TXFE3', None],
        '開盤價': ['15,000', '15,100', '-', '15,300.50', '15400', '15500'],
        '收盤價': ['15,050', '-', '15,250', '15,350.00', '15450', '15550'],
        '成交量': ['1,234', '5,678', '0', '100', '200.0', '300'],
        '買賣權': ['買權', '賣權', 'CALL', 'PUT', '買權', 'N/A'],
        '漲跌%': ['+0.5%', '-0.2%', '0.0%', '1.0%', 'N/A', '-']
    }
    test_df = pd.DataFrame(sample_data)

    logger.info("--- 開始簡易測試 clean_daily_ohlc ---")
    cleaned = clean_daily_ohlc(test_df.copy()) # 傳入 copy 以免修改原始 test_df

    print("\n--- 清洗後的 DataFrame ---")
    print(cleaned)
    print("\n--- 清洗後 DataFrame 的 dtypes ---")
    print(cleaned.dtypes)

    # 預期結果:
    # trading_date: datetime64[ns]
    # product_id: object (或 string)
    # open, close, volume: float64 (或 int64 for volume if no NaNs and no decimals)
    # option_type: 'C' or 'P'
    # percent_change: float64
    # 行數：由於 trading_date 和 product_id 的 None，最後兩行應該被移除

    expected_rows = 4 # (2023/01/01, 20230102, 112年01月03日, 2023-01-04) 這四行有效
    assert len(cleaned) == expected_rows, f"預期 {expected_rows} 行, 實際 {len(cleaned)} 行"

    assert cleaned['trading_date'].dtype == 'datetime64[ns]', "trading_date 類型錯誤"
    assert cleaned['open'].dtype == 'float64', "open 類型錯誤"
    assert cleaned['close'].dtype == 'float64', "close 類型錯誤"
    assert cleaned['volume'].dtype == 'float64', "volume 類型錯誤" # pd.to_numeric often results in float if NaNs are present
    assert cleaned['percent_change'].dtype == 'float64', "percent_change 類型錯誤"

    # 檢查 option_type 的轉換
    expected_option_types = ['C', 'P', 'C', 'P'] # 對應有效的前四行
    assert cleaned['option_type'].tolist() == expected_option_types, "option_type 內容轉換錯誤"

    # 檢查 NaN 的引入
    assert pd.isna(cleaned.loc[cleaned['trading_date'] == pd.Timestamp('2023-01-02'), 'close'].iloc[0]), "收盤價 '-' 應轉為 NaN"
    assert pd.isna(cleaned.loc[cleaned['trading_date'] == pd.Timestamp('2023-01-03'), 'open'].iloc[0]), "開盤價 '-' 應轉為 NaN"

    logger.info("--- 簡易測試結束 ---")

    # 測試空 DataFrame
    empty_df = pd.DataFrame()
    cleaned_empty = clean_daily_ohlc(empty_df)
    assert cleaned_empty.empty, "清洗空 DataFrame 應返回空 DataFrame"
    logger.info("--- 空 DataFrame 測試結束 ---")

    # 測試只有部分欄位的 DataFrame
    partial_data = {
        '日期': ['2023/02/01'],
        '契約': ['TXFG3'],
        '收盤價': ['16000']
    }
    partial_df = pd.DataFrame(partial_data)
    cleaned_partial = clean_daily_ohlc(partial_df.copy())
    assert len(cleaned_partial) == 1
    assert 'close' in cleaned_partial.columns
    assert 'trading_date' in cleaned_partial.columns
    assert 'product_id' in cleaned_partial.columns
    assert cleaned_partial['close'].iloc[0] == 16000.0
    logger.info("--- 部分欄位 DataFrame 測試結束 ---")

    # 測試無須重命名的欄位
    pre_standardized_data = {
        'trading_date': ['2023/03/01'],
        'product_id': ['TXFH3'],
        'close': ['17000']
    }
    pre_standardized_df = pd.DataFrame(pre_standardized_data)
    cleaned_pre_standardized = clean_daily_ohlc(pre_standardized_df.copy())
    assert cleaned_pre_standardized['close'].iloc[0] == 17000.0
    logger.info("--- 標準化欄位 DataFrame 測試結束 ---")

    # 測試包含 '盤後' 字樣的數值欄位
    post_market_data = {
        '交易日期': ['2023/04/01'],
        '契約': ['TXFJ3'],
        '開盤價': ['盤後'],
        '收盤價': ['17500']
    }
    post_market_df = pd.DataFrame(post_market_data)
    cleaned_post_market = clean_daily_ohlc(post_market_df.copy())
    assert pd.isna(cleaned_post_market['open'].iloc[0]), "'盤後' 應轉換為 NaN"
    assert cleaned_post_market['close'].iloc[0] == 17500.0
    logger.info("--- 包含 '盤後' DataFrame 測試結束 ---")
