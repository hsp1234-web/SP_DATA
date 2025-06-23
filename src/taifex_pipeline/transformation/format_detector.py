import hashlib
import logging
from typing import Optional, List, Dict, Any

# 取得 logger
logger = logging.getLogger("taifex_pipeline.transformation.format_detector")

class FormatDetector:
    """
    根據檔案標頭識別其格式，並從目錄中查找對應處理配方的類別。
    """

    DEFAULT_ENCODINGS = ['ms950', 'utf-8', 'big5'] # 常用的編碼列表
    DEFAULT_HEADER_READ_BYTES = 2048  # 預設讀取檔案開頭的位元組數以尋找標頭
    DEFAULT_MAX_HEADER_LINES = 10     # 預設在讀取的位元組中最多嗅探的行數

    def __init__(self,
                 encodings: Optional[List[str]] = None,
                 header_read_bytes: Optional[int] = None,
                 max_header_lines: Optional[int] = None):
        """
        初始化 FormatDetector。

        Args:
            encodings (Optional[List[str]]): 解碼時嘗試的編碼列表。
                                             若為 None，則使用 DEFAULT_ENCODINGS。
            header_read_bytes (Optional[int]): 讀取檔案開頭多少位元組以尋找標頭。
                                               若為 None，則使用 DEFAULT_HEADER_READ_BYTES。
            max_header_lines (Optional[int]): 在讀取的 header_read_bytes 中最多嗅探多少行作為標頭。
                                             若為 None，則使用 DEFAULT_MAX_HEADER_LINES。
        """
        self.try_encodings = encodings if encodings is not None else self.DEFAULT_ENCODINGS
        self.header_read_bytes = header_read_bytes if header_read_bytes is not None else self.DEFAULT_HEADER_READ_BYTES
        self.max_header_lines = max_header_lines if max_header_lines is not None else self.DEFAULT_MAX_HEADER_LINES

        logger.info(f"FormatDetector 初始化。嘗試編碼: {self.try_encodings}, "
                    f"標頭讀取位元組: {self.header_read_bytes}, 最大標頭行數: {self.max_header_lines}")

    def _normalize_header_str(self, header_str: str) -> str:
        """
        正規化標頭字串：清除空白、轉小寫、依字母序排序欄位名、用 '|' 合併。
        假設 header_str 是以逗號分隔的欄位名稱。
        """
        if not header_str:
            return ""

        # 1. 以逗號分隔欄位 (或其他常見分隔符，這裡假設是逗號)
        #    考慮到欄位本身可能包含引號包覆的逗號，簡單的 split(',') 可能不夠穩健。
        #    但依照任務描述，似乎是先取得 "標頭字串"，再進行正規化。
        #    這裡假設 header_str 已經是 "乾淨" 的，只包含欄位名，以逗號分隔。
        #    如果 header_str 是原始行，需要先解析出欄位。
        #    為了簡化，我們先假設 header_str 是 "FieldA, FieldB, FieldC" 的形式。
        #    更穩健的方式是使用 csv.reader 來解析標頭行。
        #    暫時使用簡易 split。
        fields = [field.strip() for field in header_str.split(',')]

        # 2. 清除空白 (已在 split 後的 strip 完成部分) -> 轉為小寫
        normalized_fields = [field.lower() for field in fields if field] # 移除空欄位

        # 3. 將欄位名依字母序排序
        normalized_fields.sort()

        # 4. 用 "|" 作為分隔符合併
        return "|".join(normalized_fields)

    def _generate_fingerprint(self, header_str: str) -> str:
        """
        接收一個*原始*標頭字串，正規化後計算 SHA256 雜湊值。

        Args:
            header_str (str): 從檔案中讀取的原始標頭行字串。

        Returns:
            str: 最終的指紋雜湊字串 (十六進位)。
        """
        normalized_string = self._normalize_header_str(header_str)
        if not normalized_string:
            logger.warning("正規化後的標頭字串為空，無法產生指紋。")
            return "" # 或拋出錯誤

        sha256_hash = hashlib.sha256(normalized_string.encode('utf-8')).hexdigest()
        logger.debug(f"原始標頭: '{header_str}', 正規化後: '{normalized_string}', 指紋: '{sha256_hash}'")
        return sha256_hash

    def get_recipe(self, file_content: bytes, catalog: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        接收檔案的原始二進位內容和格式目錄，嘗試找到並返回處理配方。

        Args:
            file_content (bytes): 檔案的原始二進位內容。
            catalog (Dict[str, Any]): 已載入的「格式指紋目錄」，
                                     結構為 { "fingerprint_hash": recipe_object }。

        Returns:
            Optional[Dict[str, Any]]: 如果找到配方，則返回配方物件 (字典)；否則返回 None。
        """
        if not file_content:
            logger.warning("傳入的 file_content 為空，無法偵測格式。")
            return None
        if not catalog:
            logger.warning("傳入的 catalog 為空，無法查找配方。")
            return None

        # 提取檔案開頭部分用於嗅探
        header_data_blob = file_content[:self.header_read_bytes]

        potential_header_lines: List[str] = []
        decoded_successfully = False

        # 1. 嘗試用不同編碼解碼
        for encoding in self.try_encodings:
            try:
                decoded_header_text = header_data_blob.decode(encoding)
                # 將解碼後的文本按行分割，最多取 max_header_lines
                potential_header_lines = decoded_header_text.splitlines()[:self.max_header_lines]
                logger.debug(f"成功使用編碼 '{encoding}' 解碼標頭部分。")
                decoded_successfully = True
                break # 找到一個能成功解碼的編碼就停止
            except UnicodeDecodeError:
                logger.debug(f"使用編碼 '{encoding}' 解碼標頭失敗，嘗試下一個。")
            except Exception as e:
                logger.warning(f"使用編碼 '{encoding}' 解碼時發生非預期錯誤: {e}")
                # 可能不是 UnicodeDecodeError，例如記憶體問題等，雖然不太可能在小 blob 上發生

        if not decoded_successfully or not potential_header_lines:
            logger.warning("無法使用任何指定編碼成功解碼標頭部分，或解碼後無內容。")
            return None

        # 2. 多行嗅探策略 (簡易版：逐行嘗試)
        #    我們假設標頭是 CSV 格式，並且欄位不包含換行符。
        #    更複雜的策略可能包括：
        #    - 檢查一行是否包含多個分隔符 (如逗號)。
        #    - 檢查欄位是否主要是文字而非數字 (啟發式)。
        #    - 允許組合多行為一個標頭 (例如，期交所某些檔案標頭可能跨行)。
        #    目前，我們僅逐行嘗試，直到找到一個匹配的指紋。

        for line_num, header_candidate_str in enumerate(potential_header_lines):
            header_candidate_str = header_candidate_str.strip() # 去除前後空白，包括換行符本身可能產生的
            if not header_candidate_str: # 跳過空行
                continue

            logger.debug(f"嘗試第 {line_num + 1} 行作為標頭: '{header_candidate_str}'")

            # 檢查是否看起來像 CSV 標頭 (非常基礎的檢查)
            # 至少包含一個逗號，且不全是數字或單一長字串 (避免誤判純文字行)
            if ',' not in header_candidate_str and len(header_candidate_str.split()) < 2 : # split() 預設用空白分隔
                logger.debug(f"行 '{header_candidate_str}' 不太像 CSV 標頭 (缺少逗號或欄位數不足)，跳過。")
                continue

            try:
                fingerprint = self._generate_fingerprint(header_candidate_str)
                if not fingerprint: # 如果產生指紋失敗 (例如正規化後為空)
                    continue

                if fingerprint in catalog:
                    recipe = catalog[fingerprint]
                    logger.info(f"成功匹配到指紋! 標頭行: '{header_candidate_str}', "
                                f"指紋: '{fingerprint}', 配方: {recipe.get('name', '未命名配方') if recipe else 'N/A'}")
                    # 可以在配方中加入原始標頭行或使用的編碼等資訊
                    # recipe['_metadata'] = {'detected_header': header_candidate_str, 'detected_encoding': encoding}
                    return recipe
                else:
                    logger.debug(f"指紋 '{fingerprint}' (來自標頭 '{header_candidate_str}') 在目錄中未找到。")
            except Exception as e:
                logger.error(f"處理標頭候選字串 '{header_candidate_str}' 時產生指紋失敗: {e}", exc_info=True)
                # 繼續嘗試下一行

        logger.warning(f"已嘗試 {len(potential_header_lines)} 行標頭候選，但在目錄中均未找到對應指紋。")
        return None

if __name__ == '__main__':
    # 簡易測試 (更完整的測試應在 test_format_detector.py 中)
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    detector = FormatDetector()

    # 測試 _generate_fingerprint
    header1 = "日期,商品代號,到期月份(週別)"
    fp1 = detector._generate_fingerprint(header1)
    # 預期: '日期' -> '日期', '商品代號' -> '商品代號', '到期月份(週別)' -> '到期月份(週別)'
    # sort: ['商品代號', '到期月份(週別)', '日期']
    # lower: ['商品代號', '到期月份(週別)', '日期'] (假設輸入已是小寫或不在意大小寫，或者_normalize_header_str處理)
    # join: '商品代號|到期月份(週別)|日期'
    # 實際 _normalize_header_str 行為:
    # fields = ['日期', '商品代號', '到期月份(週別)']
    # normalized_fields = ['日期', '商品代號', '到期月份(週別)'] (假設都小寫)
    # .sort() -> ['商品代號', '日期', '到期月份(週別)'] (錯了，中文排序問題)
    # 需確認 _normalize_header_str 的排序是否符合預期，特別是中文
    # python 的 sort() 對字串是字典序
    # '日期'.lower() vs '商品代號'.lower() -> '日期' < '商品代號'
    # '商品代號'.lower() vs '到期月份(週別)'.lower() -> '商品代號' < '到期月份(週別)'
    # 所以排序後應為: ['商品代號', '到期月份(週別)', '日期'] -> _normalize_header_str 已更新為轉小寫再排序

    # 重新手動計算 fingerprint for "日期,商品代號,到期月份(週別)"
    # fields = ["日期", "商品代號", "到期月份(週別)"]
    # normalized_fields = [f.strip().lower() for f in fields] -> ['日期', '商品代號', '到期月份(週別)']
    # normalized_fields.sort() -> ['商品代號', '日期', '到期月份(週別)'] (根據Unicode碼點排序)
    # "商品代號|日期|到期月份(週別)"
    # SHA256 of "商品代號|日期|到期月份(週別)"
    expected_fp1_str = "商品代號|日期|到期月份(週別)" # 修正：中文欄位名在排序時會依Unicode
    # ['日期', '商品代號', '到期月份(週別)'] -> sort -> ['商品代號', '日期', '到期月份(週別)']
    # 應為： (商品代號).lower() (日期).lower() (到期月份(週別)).lower()
    # 'shang pindai hao' 'dao qi yue fen (zhou bie)' 'ri qi'
    # sorted: ['dao qi yue fen (zhou bie)', 'ri qi', 'shang pindai hao'] (如果按拼音)
    # 實際是按 Unicode 碼點:
    # '商品代號'.lower() -> '商品代號' (U+5546 U+54C1 U+4EE3 U+865F)
    # '日期'.lower() -> '日期' (U+65E5 U+671F)
    # '到期月份(週別)'.lower() -> '到期月份(週別)' (U+5230 U+671F U+6708 U+4EFD U+0028 U+9031 U+5225 U+0029)
    # sort: ['商品代號', '到期月份(週別)', '日期'] (錯)
    # 正確排序:
    # 到(U+5230) vs 日(U+65E5) vs 商(U+5546) -> 到, 商, 日
    # 所以是: ['到期月份(週別)', '商品代號', '日期']
    expected_fp1_normalized_str = "到期月份(週別)|商品代號|日期"
    expected_fp1_hash = hashlib.sha256(expected_fp1_normalized_str.encode('utf-8')).hexdigest()
    logger.info(f"測試指紋1 (預期正規化: {expected_fp1_normalized_str}): {fp1} (預期雜湊: {expected_fp1_hash})")
    assert fp1 == expected_fp1_hash

    header2 = "  FIELD B, field a  , Field C  " # 包含空白和不同大小寫
    # "field b", "field a", "field c" -> sort: "field a", "field b", "field c"
    # join: "field a|field b|field c"
    expected_fp2_normalized_str = "field a|field b|field c"
    expected_fp2_hash = hashlib.sha256(expected_fp2_normalized_str.encode('utf-8')).hexdigest()
    fp2 = detector._generate_fingerprint(header2)
    logger.info(f"測試指紋2 (預期正規化: {expected_fp2_normalized_str}): {fp2} (預期雜湊: {expected_fp2_hash})")
    assert fp2 == expected_fp2_hash


    # 測試 get_recipe
    sample_catalog = {
        expected_fp1_hash: {"name": "Daily Futures Recipe", "parser": "CSVParser", "columns": ["col1", "col2", "col3"]},
        expected_fp2_hash: {"name": "Options Recipe", "parser": "FixedWidthParser", "config": {}}
    }

    # 模擬檔案內容 (MS950/BIG5 編碼)
    # "日期,商品代號,到期月份(週別)\r\n20230101,TXF,202301\r\n"
    file_content_ms950_str = header1 + "\r\n20230101,TXF,202301\r\n"
    file_content_ms950_bytes = file_content_ms950_str.encode('ms950')

    recipe1 = detector.get_recipe(file_content_ms950_bytes, sample_catalog)
    logger.info(f"MS950 檔案配方: {recipe1}")
    assert recipe1 is not None
    assert recipe1["name"] == "Daily Futures Recipe"

    # 模擬檔案內容 (UTF-8 編碼)
    file_content_utf8_str = header2 + "\r\nvalue_b,value_a,value_c\r\n"
    file_content_utf8_bytes = file_content_utf8_str.encode('utf-8')

    recipe2 = detector.get_recipe(file_content_utf8_bytes, sample_catalog)
    logger.info(f"UTF-8 檔案配方: {recipe2}")
    assert recipe2 is not None
    assert recipe2["name"] == "Options Recipe"

    # 模擬未知格式
    unknown_header = "Unknown,Header,Fields"
    unknown_content_bytes = unknown_header.encode('utf-8')
    recipe_unknown = detector.get_recipe(unknown_content_bytes, sample_catalog)
    logger.info(f"未知格式配方: {recipe_unknown}")
    assert recipe_unknown is None

    # 模擬標頭在第二行，第一行為空或垃圾訊息
    content_header_on_second_line_str = "This is some garbage\r\n" + header1 + "\r\n20230101,TXF,202301\r\n"
    content_header_on_second_line_bytes = content_header_on_second_line_str.encode('ms950')
    recipe3 = detector.get_recipe(content_header_on_second_line_bytes, sample_catalog)
    logger.info(f"標頭在第二行配方: {recipe3}")
    assert recipe3 is not None
    assert recipe3["name"] == "Daily Futures Recipe"

    content_header_on_second_line_empty_first_str = "\r\n" + header1 + "\r\n20230101,TXF,202301\r\n"
    content_header_on_second_line_empty_first_bytes = content_header_on_second_line_empty_first_str.encode('ms950')
    recipe4 = detector.get_recipe(content_header_on_second_line_empty_first_bytes, sample_catalog)
    logger.info(f"標頭在第二行 (首行為空) 配方: {recipe4}")
    assert recipe4 is not None
    assert recipe4["name"] == "Daily Futures Recipe"

    logger.info("FormatDetector 簡易測試完成。")
