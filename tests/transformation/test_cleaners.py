import pytest
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal
import numpy as np
from datetime import datetime

# 被測模組
from taifex_pipeline.transformation.cleaners import clean_daily_ohlc

# --- Fixtures ---

@pytest.fixture
def sample_dirty_df() -> pd.DataFrame:
    """提供一個包含各種髒數據和待轉換情況的 DataFrame。"""
    data = {
        '交易日期': ['2023/01/01', '20230102', '112年01月03日', '2023-01-04', None, '2023/01/06', '無效日期'],
        '契約': ['TXFA3', 'TXFB3', 'TXFC3', 'TXFD3', 'TXFE3', None, 'TXFG3'],
        '開盤價': ['15,000', '15,100.0', '-', '15,300.50', '15400', '15500', '15600'],
        '最高價': ['15,050', '15,150', '15,280', '15,380.50', '-', '15580', '15680'],
        '最低價': ['14,950', '15,050', '15,180', '15,280.50', '15350', '-', '15580'],
        '收盤價': ['15,020.00', '-', '15,250', '15,350.00', '15450', '15550', '15650'],
        '成交量': ['1,234', '5,678', '0', '100', '200.0', '300', 'N/A'],
        '結算價': ['15,030', '15,130', '15,260', '盤後', '15460', '15560', '15660'],
        '未平倉契約量': ['10,000', '12,000', '11,000', '13,000', '14,000', ' ', '16000'],
        '買賣權': ['買權', '賣權', 'CALL', 'PUT', '買  權', 'N/A', '賣 權'], # 包含額外空格
        '履約價': ['15000', '15100', '-', '15300', '15400', '15500', '15600'],
        '漲跌價': ['+20.0', '-10', '0', '+5.5', ' ', '-', '+15'],
        '漲跌%': ['+0.13%', '-0.07%', '0.00%', '+0.04%', 'N/A', '-', '+0.1%'],
        '最後最佳買價': ['15010', '15110', '15240', '-', '15440', '15540', '15640'],
        '最後最佳賣價': ['15025', '15125', '15255', '15355', '15455', ' ', '15655'],
        '商品名稱': ['臺股期貨', '小型臺指', '股票期貨', 'ETF期貨', '黃金期貨', '原油期貨', '個股選擇權'] # 額外欄位，應保留但名稱不變 (除非在 map 中)
    }
    return pd.DataFrame(data)

@pytest.fixture
def expected_cleaned_df_structure() -> pd.DataFrame:
    """提供一個預期清洗後 DataFrame 的結構和部分數據 (用於比較)。"""
    # 預期只有前四行是有效的，第五行 trading_date is None, 第六行 product_id is None, 第七行 trading_date 無效
    data = {
        'trading_date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'], errors='coerce'),
        'product_id': ['TXFA3', 'TXFB3', 'TXFC3', 'TXFD3'],
        'open': [15000.0, 15100.0, np.nan, 15300.50],
        'high': [15050.0, 15150.0, 15280.0, 15380.50],
        'low': [14950.0, 15050.0, 15180.0, 15280.50],
        'close': [15020.0, np.nan, 15250.0, 15350.00],
        'volume': [1234.0, 5678.0, 0.0, 100.0],
        'settlement_price': [15030.0, 15130.0, 15260.0, np.nan], # '盤後' -> NaN
        'open_interest': [10000.0, 12000.0, 11000.0, 13000.0],
        'option_type': ['C', 'P', 'C', 'P'],
        'strike_price': [15000.0, 15100.0, np.nan, 15300.0],
        'change': [20.0, -10.0, 0.0, 5.5],
        'percent_change': [0.13, -0.07, 0.00, 0.04], # 假設 % 已移除，但未除以100
        'best_bid_price': [15010.0, 15110.0, 15240.0, np.nan],
        'best_ask_price': [15025.0, 15125.0, 15255.0, 15355.0],
        '商品名稱': ['臺股期貨', '小型臺指', '股票期貨', 'ETF期貨'] # 這個欄位不在 COLUMN_MAP 中，應保留原名
    }
    return pd.DataFrame(data)


# --- Test Cases ---

class TestCleanDailyOHLC:

    def test_empty_dataframe(self):
        """測試傳入空的 DataFrame。"""
        empty_df = pd.DataFrame()
        cleaned_df = clean_daily_ohlc(empty_df.copy())
        assert cleaned_df.empty
        assert_frame_equal(cleaned_df, empty_df)

    def test_column_renaming(self, sample_dirty_df: pd.DataFrame):
        """測試欄位是否被正確重命名。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())
        expected_renamed_cols = [
            'trading_date', 'product_id', 'open', 'high', 'low', 'close', 'volume',
            'settlement_price', 'open_interest', 'option_type', 'strike_price',
            'change', 'percent_change', 'best_bid_price', 'best_ask_price'
        ]
        # '商品名稱' 不在 map 中，應保留
        present_renamed_cols = [col for col in expected_renamed_cols if col in cleaned_df.columns]

        for col in present_renamed_cols:
            assert col in cleaned_df.columns

        assert '商品名稱' in cleaned_df.columns # 檢查未被重命名的欄位是否還在
        assert '交易日期' not in cleaned_df.columns # 檢查原始名稱是否已被移除

    def test_numeric_conversion_with_dirty_data(self, sample_dirty_df: pd.DataFrame, expected_cleaned_df_structure: pd.DataFrame):
        """測試數值欄位轉換，包括處理 '-' 和 ','。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())

        numeric_cols_to_check = ['open', 'high', 'low', 'close', 'volume',
                                 'settlement_price', 'open_interest', 'strike_price',
                                 'change', 'percent_change',
                                 'best_bid_price', 'best_ask_price']

        for col in numeric_cols_to_check:
            if col in expected_cleaned_df_structure.columns: # 只檢查預期結果中存在的欄位
                assert cleaned_df[col].dtype == 'float64', f"欄位 '{col}' 的 dtype 不是 float64"
                # 比較 NaN 的位置 (iloc[:4] 是因為預期只有前4行有效)
                expected_series = expected_cleaned_df_structure[col]
                actual_series = cleaned_df[col].iloc[:len(expected_series)] # 確保長度一致
                assert_series_equal(actual_series, expected_series, check_dtype=False, # dtype 可能因 NaN 存在而是 float
                                    check_names=False, # Series name 可能不同
                                    atol=1e-9, # 浮點數比較容忍度
                                    check_exact=False) # 允許 NaN 的比較

    def test_date_conversion(self, sample_dirty_df: pd.DataFrame, expected_cleaned_df_structure: pd.DataFrame):
        """測試 trading_date 欄位轉換為日期格式。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())
        assert cleaned_df['trading_date'].dtype == 'datetime64[ns]', "trading_date 的 dtype 不是 datetime64[ns]"

        expected_dates = expected_cleaned_df_structure['trading_date']
        actual_dates = cleaned_df['trading_date'].iloc[:len(expected_dates)]
        assert_series_equal(actual_dates, expected_dates, check_names=False)

    def test_option_type_standardization(self, sample_dirty_df: pd.DataFrame, expected_cleaned_df_structure: pd.DataFrame):
        """測試 option_type 欄位（買權/賣權）的標準化。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())
        expected_options = expected_cleaned_df_structure['option_type']
        actual_options = cleaned_df['option_type'].iloc[:len(expected_options)]
        assert_series_equal(actual_options, expected_options, check_names=False, check_dtype=False)

    def test_handling_optional_columns_missing(self):
        """測試當可選欄位 (如漲跌%) 不存在時，函式不會報錯。"""
        data_missing_percent_change = {
            '交易日期': ['2023/01/01'],
            '契約': ['TXFA3'],
            '收盤價': ['15,000'],
            '漲跌價': ['+20'] # 有漲跌價，但沒有漲跌%
        }
        df = pd.DataFrame(data_missing_percent_change)
        try:
            cleaned_df = clean_daily_ohlc(df.copy())
            assert 'percent_change' not in cleaned_df.columns # 確認它沒有被錯誤地加入
            assert 'change' in cleaned_df.columns # 確認存在的 '漲跌價' 被處理了
            assert cleaned_df['change'].iloc[0] == 20.0
        except Exception as e:
            pytest.fail(f"當缺少 '漲跌%' 欄位時，clean_daily_ohlc 引發了錯誤: {e}")

    def test_handling_optional_columns_present(self, sample_dirty_df: pd.DataFrame):
        """測試當可選欄位存在時，它們被正確處理。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())
        assert 'change' in cleaned_df.columns
        assert 'percent_change' in cleaned_df.columns
        assert cleaned_df['change'].dtype == 'float64'
        assert cleaned_df['percent_change'].dtype == 'float64'
        # 詳細的數值檢查已在 test_numeric_conversion_with_dirty_data 中完成

    def test_invalid_data_removal(self, sample_dirty_df: pd.DataFrame, expected_cleaned_df_structure: pd.DataFrame):
        """測試在關鍵欄位 (trading_date, product_id) 為空的資料行是否被移除。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())
        # 預期 sample_dirty_df 中第5行 (trading_date is None) 和 第6行 (product_id is None)
        # 以及第7行 (trading_date 轉換後為 NaT) 會被移除。
        # 所以預期結果是 4 行
        assert len(cleaned_df) == len(expected_cleaned_df_structure), \
            f"預期移除無效數據後有 {len(expected_cleaned_df_structure)} 行, 實際得到 {len(cleaned_df)} 行。"

    def test_full_cleaning_process(self, sample_dirty_df: pd.DataFrame, expected_cleaned_df_structure: pd.DataFrame):
        """對整個清洗流程進行一次綜合驗證。"""
        cleaned_df = clean_daily_ohlc(sample_dirty_df.copy())

        # 比較 DataFrame (忽略索引和欄位順序，但欄位名應匹配)
        # pandas.testing.assert_frame_equal 是個好工具，但需要欄位順序也一致
        # 我們可以先排序預期結果和實際結果的欄位
        expected_cols_ordered = sorted(expected_cleaned_df_structure.columns)
        actual_cols_ordered = sorted(cleaned_df.columns)

        # 檢查 '商品名稱' 是否存在於 actual_cols_ordered 但不存在於 expected_cols_ordered
        # 因為 expected_cleaned_df_structure 可能沒有包含所有未被映射的欄位
        # 我們的目標是檢查被清洗和標準化的欄位是否正確

        # 這裡我們只比較 expected_cleaned_df_structure 中定義的欄位
        # 並且確保 cleaned_df 中包含了這些欄位
        for col in expected_cleaned_df_structure.columns:
            assert col in cleaned_df.columns, f"預期欄位 '{col}' 在清洗後的 DataFrame 中缺失。"

        # 為了比較，我們只選取 expected_cleaned_df_structure 中存在的欄位，並按相同順序排列
        cleaned_df_for_comparison = cleaned_df[expected_cleaned_df_structure.columns].copy()

        # 由於 NaN 的比較和浮點數精度，直接比較 DataFrame 可能很棘手
        # assert_frame_equal 提供了這些功能
        assert_frame_equal(cleaned_df_for_comparison.reset_index(drop=True),
                           expected_cleaned_df_structure.reset_index(drop=True),
                           check_dtype=False, # 主要關心值，dtype 在前面已單獨測
                           atol=1e-9) # 浮點數容差

    def test_roc_date_conversion_in_trading_date(self):
        """專門測試民國年轉換。"""
        data = {
            '交易日期': ['110/05/10', '111年12月20日', '109/1/5'],
            '契約': ['A', 'B', 'C']
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_daily_ohlc(df.copy())

        expected_dates = pd.to_datetime(['2021-05-10', '2022-12-20', '2020-01-05'])
        assert_series_equal(cleaned_df['trading_date'], expected_dates, check_names=False)

    def test_numeric_cols_with_all_non_numeric(self):
        """測試當一個數值欄位完全由非數值字串組成時的情況。"""
        data = {
            '交易日期': ['2023/01/01'],
            '契約': ['TXFA3'],
            '開盤價': ['-', '-', '-'], # 整個欄位都是 '-'
            '成交量': ['N/A', '盤後', ' '] # 整個欄位都是各種 NaN 代表
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_daily_ohlc(df.copy())

        assert cleaned_df['open'].isna().all()
        assert cleaned_df['open'].dtype == 'float64'
        assert cleaned_df['volume'].isna().all()
        assert cleaned_df['volume'].dtype == 'float64'

    def test_option_type_with_mixed_case_and_spaces(self):
        """測試 option_type 包含不同大小寫和額外空格的情況。"""
        data = {
            '交易日期': ['2023/01/01', '2023/01/02', '2023/01/03', '2023/01/04'],
            '契約': ['A', 'B', 'C', 'D'],
            '買賣權': ['  買權  ', '賣 權', 'call  ', '  pUt']
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_daily_ohlc(df.copy())
        expected_options = pd.Series(['C', 'P', 'C', 'P'], name='option_type')
        assert_series_equal(cleaned_df['option_type'], expected_options, check_names=False)

    def test_percent_change_cleaning(self):
        """專門測試漲跌%的清洗。"""
        data = {
            '交易日期': ['2023/01/01', '2023/01/02', '2023/01/03', '2023/01/04'],
            '契約': ['A', 'B', 'C', 'D'],
            '漲跌%': ['+1.23%', '-0.5%', '  0.0% ', 'N/A']
        }
        df = pd.DataFrame(data)
        cleaned_df = clean_daily_ohlc(df.copy())

        expected_percent_change = pd.Series([1.23, -0.5, 0.0, np.nan], name='percent_change')
        assert_series_equal(cleaned_df['percent_change'], expected_percent_change, check_names=False)
        assert cleaned_df['percent_change'].dtype == 'float64'

# TODO:
# - 測試更複雜的日期格式 (如果 pd.to_datetime 不能自動處理)
# - 測試包含極端值或邊界值的數值轉換
# - 測試當 COLUMN_MAP 非常大或 DataFrame 欄位非常多時的性能 (可能超出單元測試範圍)
# - 測試不同種類的換行符或檔案編碼問題對欄位名提取的影響 (這更偏向 parser，但清洗前的 DataFrame 狀態可能受影響)
# - 測試當關鍵欄位 (如 trading_date, product_id) 本身就是 object 類型但包含 np.nan 或 None 時 dropna 的行為
