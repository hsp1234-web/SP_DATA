import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np # For np.nan
from src.sp_data_v16.transformation.parser import DataParser

@pytest.fixture(scope="module") # module scope is fine as DataParser is stateless
def data_parser():
    """
    Pytest fixture to provide an instance of DataParser.
    """
    return DataParser()

def test_parse_utf8_csv_success(data_parser):
    """
    Tests successful parsing of a standard UTF-8 encoded CSV.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1,val2\nval3,val4"
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

# --- Tests for csv_skip_rows functionality ---

def _get_base_schema_for_skip_rows_test(skip_rows_count: int):
    """Helper to create a basic schema for skip_rows tests."""
    return {
        'encoding': 'utf-8',
        'delimiter': ',',
        'columns': ['col_a', 'col_b', 'col_c'],
        'csv_skip_rows': skip_rows_count
    }

RAW_CSV_CONTENT_FOR_SKIP_TESTS = b"""header1,header2,header3
data1_1,data1_2,data1_3
data2_1,data2_2,data2_3
data3_1,data3_2,data3_3
"""
# Total 4 lines. Header + 3 data lines.

def test_parse_csv_skip_rows_zero(data_parser):
    """測試當 csv_skip_rows 為 0 時，沒有任何行被跳過。"""
    schema = _get_base_schema_for_skip_rows_test(0)

    # 由於 parse 方法會使用 schema 中的 columns 作為 header=None 的 names，
    # 所以預期的 DataFrame 會將 CSV 的第一行也視為數據行，並使用 schema 中的 names。
    expected_data = [
        ['header1', 'header2', 'header3'],
        ['data1_1', 'data1_2', 'data1_3'],
        ['data2_1', 'data2_2', 'data2_3'],
        ['data3_1', 'data3_2', 'data3_3']
    ]
    expected_df = pd.DataFrame(expected_data, columns=['col_a', 'col_b', 'col_c'])

    result_df = data_parser.parse(RAW_CSV_CONTENT_FOR_SKIP_TESTS, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)
    assert len(result_df) == 4, "當 csv_skip_rows 為 0 時，應包含所有 4 行"

def test_parse_csv_skip_rows_equals_total_lines(data_parser):
    """測試當 csv_skip_rows 等於 CSV 總行數時，解析結果為空 DataFrame。"""
    schema = _get_base_schema_for_skip_rows_test(4) # CSV 有 4 行

    # 預期結果是一個空的 DataFrame，但欄位與 schema 中定義的一致
    expected_df = pd.DataFrame(columns=['col_a', 'col_b', 'col_c'])

    result_df = data_parser.parse(RAW_CSV_CONTENT_FOR_SKIP_TESTS, schema)

    assert result_df is not None
    assert result_df.empty, "DataFrame 應為空"
    assert_frame_equal(result_df, expected_df, check_dtype=False) # 空 DF 的 dtype 可能不一致

def test_parse_csv_skip_rows_greater_than_total_lines(data_parser):
    """測試當 csv_skip_rows 大於 CSV 總行數時，解析結果為空 DataFrame，且不應拋出錯誤。"""
    schema = _get_base_schema_for_skip_rows_test(5) # CSV 只有 4 行

    expected_df = pd.DataFrame(columns=['col_a', 'col_b', 'col_c'])

    result_df = data_parser.parse(RAW_CSV_CONTENT_FOR_SKIP_TESTS, schema)

    assert result_df is not None
    assert result_df.empty, "DataFrame 應為空"
    # 對於空的 DataFrame，欄位順序和型態可能需要特別注意。
    # 如果 schema 中的 columns 定義了順序，那麼空的 DataFrame 應該遵循這個順序。
    # dtypes 可能都是 object。
    assert_frame_equal(result_df, expected_df, check_dtype=False)

def test_parse_big5_piped_success(data_parser):
    """
    Tests successful parsing of BIG5 encoded, pipe-delimited data.
    """
    schema = {'encoding': 'big5', 'delimiter': '|', 'columns': ['名稱', '值']}
    raw_content = "測試1|值1\n測試2|值2".encode('big5')
    expected_df = pd.DataFrame([['測試1', '值1'], ['測試2', '值2']], columns=['名稱', '值'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_default_encoding_delimiter_success(data_parser):
    """
    Tests parsing using default encoding (utf-8) and delimiter (,).
    """
    schema = {'columns': ['X', 'Y']} # Encoding and delimiter not specified
    raw_content = b"default1,default2\ndefault3,default4"
    expected_df = pd.DataFrame([['default1', 'default2'], ['default3', 'default4']], columns=['X', 'Y'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_missing_columns_in_schema(data_parser):
    """
    Tests that parsing fails (returns None) if 'columns' key is missing in schema.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ','} # 'columns' is missing
    raw_content = b"a,b"

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_empty_columns_in_schema(data_parser):
    """
    Tests that parsing fails (returns None) if 'columns' list is empty in schema.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': []} # 'columns' is empty
    raw_content = b"a,b"

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_malformed_csv_pandas_error(data_parser):
    """
    Tests that parsing malformed CSV content returns None.
    DataParser should catch pd.errors.ParserError.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    # Malformed CSV: unclosed quote causing CParserError
    raw_content = b'val1,val2\n"val3,val4'

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_unicode_decode_error(data_parser):
    """
    Tests that parsing content with encoding different from schema returns None.
    (e.g., BIG5 content with a UTF-8 schema).
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A']}
    raw_content = "測試".encode('big5') # BIG5 encoded content

    result_df = data_parser.parse(raw_content, schema) # Schema expects UTF-8

    assert result_df is None

def test_parse_empty_raw_content(data_parser):
    """
    Tests that parsing empty raw content returns None.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A']}
    raw_content = b""

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is None

def test_parse_different_delimiter_than_data(data_parser):
    """
    Tests parsing when the schema's delimiter does not match the actual data delimiter.
    Pandas read_csv behavior: if the specified delimiter isn't found, it often reads
    each line as a single field into the first column specified in 'names'.
    Subsequent columns defined in 'names' will be filled with NaN.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ';', 'columns': ['A', 'B']}
    raw_content = b"val1,val2\nval3,val4" # Data is comma-separated

    # Expected behavior: 'val1,val2' becomes the value for column 'A', column 'B' is NaN
    # Similarly for the second row.
    expected_data = {
        'A': ['val1,val2', 'val3,val4'],
        'B': [np.nan, np.nan] # Use np.nan for NaNs
    }
    expected_df = pd.DataFrame(expected_data, columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    # When comparing DataFrames with NaNs, dtypes can be tricky.
    # assert_frame_equal is generally good but might need check_dtype=False
    # or ensuring expected_df has precisely matching dtypes for NaN columns (often 'object' or specific float).
    assert_frame_equal(result_df, expected_df, check_dtype=True) # Be strict initially, relax if needed due to NaN typing

def test_parse_skipinitialspace(data_parser):
    """
    Tests that skipinitialspace=True works as expected (handled by DataParser).
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1, val2\nval3,val4 " # Note spaces around val2 and val4
    # Expected: "val2" (space trimmed) and "val4 " (trailing space may remain, depending on exact pandas handling for EOL)
    # pd.read_csv with skipinitialspace primarily affects space *after* delimiter.
    # Trailing spaces on the line for the last column are usually preserved unless other options like strip_ws are used.
    # The DataParser doesn't use strip_ws.
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4 ']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_more_columns_in_data_than_schema(data_parser):
    """
    Tests parsing when data has more columns than defined in schema.
    Pandas should only read up to the number of columns specified in 'names'.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B']}
    raw_content = b"val1,val2,extra_col_data\nval3,val4,more_extra"
    expected_df = pd.DataFrame([['val1', 'val2'], ['val3', 'val4']], columns=['A', 'B'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)

def test_parse_fewer_columns_in_data_than_schema(data_parser):
    """
    Tests parsing when data has fewer columns than defined in schema.
    Pandas should fill the missing columns with NaN.
    """
    schema = {'encoding': 'utf-8', 'delimiter': ',', 'columns': ['A', 'B', 'C']}
    raw_content = b"val1,val2\nval3" # Second row only has one value

    expected_data = {
        'A': ['val1', 'val3'],
        'B': ['val2', np.nan],
        'C': [np.nan, np.nan]
    }
    expected_df = pd.DataFrame(expected_data, columns=['A', 'B', 'C'])

    result_df = data_parser.parse(raw_content, schema)

    assert result_df is not None
    assert_frame_equal(result_df, expected_df)
