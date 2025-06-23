import pytest
import hashlib
from typing import Dict, Any, Optional, List

# 被測模組
from taifex_pipeline.transformation.format_detector import FormatDetector

# --- Helper Functions ---
def calculate_expected_fingerprint(header_str: str) -> str:
    """
    輔助函數，用於手動計算預期的指紋，與 FormatDetector._generate_fingerprint 的邏輯一致。
    """
    if not header_str:
        return ""
    fields = [field.strip().lower() for field in header_str.split(',') if field.strip()]
    fields.sort()
    normalized_string = "|".join(fields)
    if not normalized_string:
        return ""
    return hashlib.sha256(normalized_string.encode('utf-8')).hexdigest()

# --- Test Fixtures ---
@pytest.fixture
def detector() -> FormatDetector:
    """提供一個預設的 FormatDetector 實例。"""
    return FormatDetector()

@pytest.fixture
def sample_catalog_data() -> Dict[str, Any]:
    """提供一個範例格式目錄。"""
    header1 = "日期,商品代號,到期月份(週別)" # 到期月份(週別)|商品代號|日期
    fingerprint1 = calculate_expected_fingerprint(header1)

    header2 = "  FIELD B, field a  , Field C  " # field a|field b|field c
    fingerprint2 = calculate_expected_fingerprint(header2)

    header3 = "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價" # 契約|到期月份(w)|履約價|最新價|最高價|最低價|買賣權|開盤價
    fingerprint3 = calculate_expected_fingerprint(header3)

    return {
        fingerprint1: {"name": "Daily Futures TXF", "type": "csv", "encoding": "ms950", "parser_config": {"delimiter": ","}},
        fingerprint2: {"name": "Sample Options Data", "type": "csv", "encoding": "utf-8", "parser_config": {"delimiter": ","}},
        fingerprint3: {"name": "Daily Options MTX", "type": "csv", "encoding": "big5", "parser_config": {"delimiter": ","}},
    }

# --- Test Cases for _normalize_header_str and _generate_fingerprint ---

class TestFormatDetectorFingerprint:

    def test_normalize_header_str_simple(self, detector: FormatDetector):
        assert detector._normalize_header_str("field_a,field_b,field_c") == "field_a|field_b|field_c"

    def test_normalize_header_str_with_spaces_and_case(self, detector: FormatDetector):
        assert detector._normalize_header_str("  Field_B , field_A  , FIELD_C  ") == "field_a|field_b|field_c"

    def test_normalize_header_str_different_order(self, detector: FormatDetector):
        assert detector._normalize_header_str("field_c,field_a,field_b") == "field_a|field_b|field_c"

    def test_normalize_header_str_with_empty_fields(self, detector: FormatDetector):
        assert detector._normalize_header_str("field_a,,field_b,  ,field_c") == "field_a|field_b|field_c"

    def test_normalize_header_str_empty_input(self, detector: FormatDetector):
        assert detector._normalize_header_str("") == ""

    def test_normalize_header_str_single_field(self, detector: FormatDetector):
        assert detector._normalize_header_str("  MyField  ") == "myfield"

    def test_normalize_header_str_chinese_fields(self, detector: FormatDetector):
        # 預期排序: ['到期月份(週別)', '商品代號', '日期']
        assert detector._normalize_header_str("日期,商品代號,到期月份(週別)") == "到期月份(週別)|商品代號|日期"
        assert detector._normalize_header_str("  日期  , 到期月份(週別) ,商品代號  ") == "到期月份(週別)|商品代號|日期"


    def test_generate_fingerprint_stable_and_predictable(self, detector: FormatDetector):
        header1 = "日期,商品代號,到期月份(週別)"
        fp1_expected = calculate_expected_fingerprint(header1)
        assert detector._generate_fingerprint(header1) == fp1_expected

        header2 = "  FIELD B, field a  , Field C  "
        fp2_expected = calculate_expected_fingerprint(header2)
        assert detector._generate_fingerprint(header2) == fp2_expected

        # 即使順序和大小寫不同，指紋也應該相同
        header2_alt = "Field C, FIELD_A, field b"
        assert detector._generate_fingerprint(header2_alt) == fp2_expected

    def test_generate_fingerprint_empty_header(self, detector: FormatDetector):
        # _normalize_header_str("") is "", so _generate_fingerprint("") returns ""
        assert detector._generate_fingerprint("") == ""

    def test_generate_fingerprint_header_normalizes_to_empty(self, detector: FormatDetector):
        # E.g., header string with only commas or spaces
        assert detector._generate_fingerprint(",,, , ") == ""


# --- Test Cases for get_recipe ---
class TestFormatDetectorGetRecipe:

    def test_get_recipe_known_format_ms950(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "日期,商品代號,到期月份(週別)"
        file_content_str = header + "\r\n20230101,TXF,202301\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Daily Futures TXF"
        assert recipe["encoding"] == "ms950"

    def test_get_recipe_known_format_utf8(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "  FIELD B, field a  , Field C  "
        file_content_str = header + "\r\nvalB,valA,valC\r\n"
        file_content_bytes = file_content_str.encode('utf-8')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Sample Options Data"
        assert recipe["encoding"] == "utf-8"

    def test_get_recipe_known_format_big5(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "契約,到期月份(W),買賣權,履約價,開盤價,最高價,最低價,最新價"
        # 內容不重要，只要標頭能被 big5 解碼
        file_content_str = header + "\r\nTXO,202301W1,Call,18000,100,120,80,110\r\n"
        file_content_bytes = file_content_str.encode('big5') # big5 是 ms950 的父集

        detector_big5_first = FormatDetector(encodings=['big5', 'ms950', 'utf-8']) # 確保 big5 被嘗試
        recipe = detector_big5_first.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Daily Options MTX"
        # 注意：配方中的 "encoding" 欄位是我們預設的，不一定是偵測到的編碼
        # 如果需要在配方中包含偵測到的編碼，get_recipe 邏輯需要修改
        assert recipe["encoding"] == "big5"


    def test_get_recipe_unknown_format(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "This,Is,An,Unknown,Header"
        file_content_bytes = (header + "\r\ndata,data,data,data,data\r\n").encode('utf-8')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is None

    def test_get_recipe_decoding_failure_all_encodings(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        # 建立一個無法被預設編碼 (ms950, utf-8, big5) 正確解碼的位元組序列
        # 例如，使用一個在這些編碼中都無效的序列，或者來自不同編碼的混合
        # GBK (簡中) 字串 "你好世界"
        gbk_bytes = "你好世界".encode('gbk')

        recipe = detector.get_recipe(gbk_bytes, sample_catalog_data)
        assert recipe is None

    def test_get_recipe_header_on_second_line(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "日期,商品代號,到期月份(週別)"
        file_content_str = "Some garbage first line\r\n" + header + "\r\n20230101,TXF,202301\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Daily Futures TXF"

    def test_get_recipe_header_with_empty_first_line(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        header = "日期,商品代號,到期月份(週別)"
        file_content_str = "\r\n" + header + "\r\n20230101,TXF,202301\r\n" # 首行為空
        file_content_bytes = file_content_str.encode('ms950')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Daily Futures TXF"

    def test_get_recipe_file_content_too_short_for_header(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        # 檔案內容比 detector.header_read_bytes 短，但仍包含可識別的標頭
        header = "FIELD B,field a,Field C" # fingerprint2
        file_content_bytes = header.encode('utf-8') # 只有標頭，沒有換行和內容

        # 使用一個較小的 header_read_bytes 來測試，但仍大於標頭本身長度
        custom_detector = FormatDetector(header_read_bytes=len(file_content_bytes) + 5)
        recipe = custom_detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Sample Options Data"

        # 檔案內容比標頭本身還短
        short_content_bytes = header[:10].encode('utf-8')
        recipe_short = custom_detector.get_recipe(short_content_bytes, sample_catalog_data)
        assert recipe_short is None # 因為無法完整解碼或識別標頭

    def test_get_recipe_file_content_empty(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        recipe = detector.get_recipe(b"", sample_catalog_data)
        assert recipe is None

    def test_get_recipe_catalog_empty(self, detector: FormatDetector):
        header = "日期,商品代號,到期月份(週別)"
        file_content_bytes = (header + "\r\n20230101,TXF,202301\r\n").encode('ms950')
        recipe = detector.get_recipe(file_content_bytes, {}) # 空 catalog
        assert recipe is None

    def test_get_recipe_header_not_csv_like(self, detector: FormatDetector, sample_catalog_data: Dict[str, Any]):
        # 檔案內容看起來不像 CSV 標頭
        non_csv_header_line1 = "This is just a plain text line without commas."
        non_csv_header_line2 = "1234567890" # 全是數字
        header = "日期,商品代號,到期月份(週別)" # 真實標頭在後面

        file_content_str = non_csv_header_line1 + "\r\n" + \
                           non_csv_header_line2 + "\r\n" + \
                           header + "\r\n20230101,TXF,202301\r\n"
        file_content_bytes = file_content_str.encode('ms950')

        recipe = detector.get_recipe(file_content_bytes, sample_catalog_data)
        assert recipe is not None
        assert recipe["name"] == "Daily Futures TXF" # 應該能跳過前兩行找到真實標頭

    def test_get_recipe_uses_custom_init_params(self, sample_catalog_data: Dict[str, Any]):
        """測試 FormatDetector 是否正確使用 __init__ 中傳入的參數。"""
        header = "Test,Header"
        fp = calculate_expected_fingerprint(header)
        custom_catalog = {fp: {"name": "Custom Recipe"}}

        # 檔案內容只有一行標頭，使用特殊編碼 'latin1'
        file_content_bytes = header.encode('latin1')

        # 1. 測試自訂編碼
        detector_custom_encoding = FormatDetector(encodings=['latin1'])
        recipe = detector_custom_encoding.get_recipe(file_content_bytes, custom_catalog)
        assert recipe is not None
        assert recipe["name"] == "Custom Recipe"

        # 預設 detector 應該找不到，因為 'latin1' 不在預設列表
        default_detector = FormatDetector()
        recipe_default_enc = default_detector.get_recipe(file_content_bytes, custom_catalog)
        assert recipe_default_enc is None

        # 2. 測試自訂 header_read_bytes
        # 標頭很短，但我們設定一個更短的 read_bytes，使其無法讀取完整標頭
        short_read_bytes = len(header.encode('latin1')) - 2
        detector_short_read = FormatDetector(encodings=['latin1'], header_read_bytes=short_read_bytes)
        recipe_short = detector_short_read.get_recipe(file_content_bytes, custom_catalog)
        assert recipe_short is None # 因為標頭讀取不完整

        # 3. 測試自訂 max_header_lines
        # 標頭在第二行，但 max_header_lines=1
        file_content_multiline_str = "Junk\r\n" + header
        file_content_multiline_bytes = file_content_multiline_str.encode('latin1')
        detector_max_lines = FormatDetector(encodings=['latin1'], max_header_lines=1)
        recipe_max_lines = detector_max_lines.get_recipe(file_content_multiline_bytes, custom_catalog)
        assert recipe_max_lines is None # 因為只嗅探第一行

        detector_enough_lines = FormatDetector(encodings=['latin1'], max_header_lines=2)
        recipe_enough_lines = detector_enough_lines.get_recipe(file_content_multiline_bytes, custom_catalog)
        assert recipe_enough_lines is not None
        assert recipe_enough_lines["name"] == "Custom Recipe"

    def test_get_recipe_header_contains_problematic_chars_for_split(self, detector: FormatDetector):
        # 假設我們的 _normalize_header_str 依賴簡單的 split(',')
        # 如果欄位值本身包含逗號 (例如 "Field, with comma", value2)，會出錯
        # 目前的 _normalize_header_str 沒有處理 CSV 引號包覆的逗號
        # 這個測試是為了標記這個潛在問題，而不是期望它通過 (除非 _normalize_header_str 變複雜)

        header_with_quoted_comma = '"Product, Name","Category","Price"'
        # 預期正規化 (如果能正確處理CSV): "category|price|product, name"
        # 目前實作: '"product', ' name"|category|price'
        fp_problematic = detector._generate_fingerprint(header_with_quoted_comma)

        problematic_catalog = {fp_problematic: {"name": "Problematic CSV"}}
        file_content_bytes = header_with_quoted_comma.encode('utf-8')

        recipe = detector.get_recipe(file_content_bytes, problematic_catalog)
        # 取決於 _normalize_header_str 如何處理，這裡可能會匹配或不匹配
        # 鑑於目前 _normalize_header_str 的簡易性，它會按原樣 split
        # '"Product, Name"' -> '"product, name"'
        # '"Category"' -> '"category"'
        # '"Price"' -> '"price"'
        # sorted: ['"category"', '"price"', '"product, name"']
        # joined: '"category"|"price"|"product, name"'
        # 這個測試主要是為了記錄，如果未來需要處理複雜 CSV 標頭，_normalize_header_str 需要改進
        assert recipe is not None
        assert recipe["name"] == "Problematic CSV"
        # 如果我們希望它能正確解析CSV，則需要修改 _normalize_header_str 使用 csv.reader
        # 例如：
        # import csv
        # from io import StringIO
        # reader = csv.reader(StringIO(header_str))
        # fields = next(reader)
        # fields = [field.strip().lower() for field in fields if field.strip()]
        # ...
        # 這樣的話，fp_problematic 的計算方式會不同，catalog 也需要更新。
        # 目前保持現狀，因為任務描述沒有明確要求複雜 CSV 解析。

# TODO:
# - 測試標頭行非常長的情況 (超過 header_read_bytes 但在 max_header_lines 內)
# - 測試檔案內容剛好在邊界情況 (例如，剛好等於 header_read_bytes)
# - 測試不同種類的換行符 (\n, \r, \r\n) 如何影響 splitlines() 和 strip()
# - 測試當 catalog 的 recipe 為 None 或非字典時的情況 (雖然 typing 暗示是 Dict)
# - 如果 _normalize_header_str 未來支援更複雜的分隔符或CSV解析，需要更多測試。
