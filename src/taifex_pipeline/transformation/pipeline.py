import logging
import importlib
import pandas as pd
from io import BytesIO, StringIO
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import TYPE_CHECKING, Dict, Any, List, Optional

if TYPE_CHECKING:
    # from taifex_pipeline.database.db_manager import DBManager # 不再直接注入 DBManager 實例
    from taifex_pipeline.transformation.format_detector import FormatDetector
    # 假設 FileStatus Enum 在 constants 中定義
    from taifex_pipeline.database.constants import FileStatus

# DBManager 仍然需要被匯入，因為我們會在方法內部創建它的實例
from taifex_pipeline.database.db_manager import DBManager


logger = logging.getLogger("taifex_pipeline.transformation.pipeline")

# 預設的檔案狀態，如果 FileStatus 無法匯入，則使用字串
# 這只是為了讓程式碼在沒有完整 FileStatus Enum 的情況下也能運行，實際應確保 Enum 可用
DEFAULT_STATUS_RAW_INGESTED = "RAW_INGESTED"
DEFAULT_STATUS_TRANSFORMED_SUCCESS = "TRANSFORMED_SUCCESS"
DEFAULT_STATUS_QUARANTINED = "QUARANTINED"
DEFAULT_STATUS_TRANSFORMATION_FAILED = "TRANSFORMATION_FAILED"


class TransformationPipeline:
    """
    轉換管線，用於處理從 manifest 中取得的原始檔案，
    偵測格式、清洗數據，並將結果載入到目標表格。
    """

    def __init__(self,
                 db_path: str,  # <--- 修改：接收 db_path 而不是 db_manager 實例
                 format_detector: 'FormatDetector',
                 format_catalog: Dict[str, Any],
                 max_workers: Optional[int] = None):
        """
        初始化 TransformationPipeline。

        Args:
            db_path (str): 資料庫檔案的路徑。
            format_detector (FormatDetector): FormatDetector 的實例。
            format_catalog (Dict[str, Any]): 已載入的格式指紋目錄。
            max_workers (Optional[int]): ProcessPoolExecutor 的最大工人進程數。
                                         若為 None，則 ProcessPoolExecutor 會根據 CPU 核心數決定。
        """
        if not db_path: # db_path 是必需的
            raise ValueError("資料庫路徑 (db_path) 不能為空。")
        if format_detector is None:
            raise ValueError("FormatDetector 實例不能為 None。")
        if format_catalog is None: # format_catalog 可以是空的，但不應是 None
            raise ValueError("Format Catalog 不能為 None (可以是空字典)。")

        self.db_path = db_path # <--- 保存 db_path
        # self.db_manager 主進程用的 DBManager 將在 run() 方法中創建和管理
        self.format_detector = format_detector
        self.format_catalog = format_catalog
        self.max_workers = max_workers

        # 嘗試匯入 FileStatus Enum，如果失敗則使用預設字串
        try:
            from taifex_pipeline.database.constants import FileStatus
            self.STATUS_RAW_INGESTED = FileStatus.RAW_INGESTED.value
            self.STATUS_TRANSFORMED_SUCCESS = FileStatus.TRANSFORMED_SUCCESS.value
            self.STATUS_QUARANTINED = FileStatus.QUARANTINED.value
            self.STATUS_TRANSFORMATION_FAILED = FileStatus.TRANSFORMATION_FAILED.value
        except ImportError:
            logger.warning("無法從 taifex_pipeline.database.constants 匯入 FileStatus Enum，將使用預設字串狀態。")
            self.STATUS_RAW_INGESTED = DEFAULT_STATUS_RAW_INGESTED
            self.STATUS_TRANSFORMED_SUCCESS = DEFAULT_STATUS_TRANSFORMED_SUCCESS
            self.STATUS_QUARANTINED = DEFAULT_STATUS_QUARANTINED
            self.STATUS_TRANSFORMATION_FAILED = DEFAULT_STATUS_TRANSFORMATION_FAILED

        logger.info(f"TransformationPipeline 初始化完成。資料庫路徑: {self.db_path}, "
                    f"FormatDetector: {type(format_detector).__name__}, "
                    f"Format Catalog 條目數: {len(format_catalog)}, "
                    f"最大工人數: {self.max_workers or '預設'}")

    # run() 和 _process_file() 方法將在後續步驟中實作

    def _parse_raw_content(self, raw_content: bytes, recipe: Dict[str, Any]) -> pd.DataFrame:
        """
        根據配方中的 parser_config 解析原始內容為 DataFrame。
        """
        parser_type = recipe.get('parser_type', 'csv').lower()
        parser_config = recipe.get('parser_config', {})

        # 確保 parser_config 中的 encoding (如果存在) 被正確處理
        # FormatDetector 可能已指明了 encoding，但 recipe 中的 encoding 優先級更高
        # 如果 recipe 中沒有 encoding，可以考慮使用 FormatDetector 檢測到的 (如果傳遞過來)
        # 目前假設 parser_config 會包含 'encoding' (如果需要)

        # 預設使用 BytesIO 處理二進位內容
        data_io = BytesIO(raw_content)

        logger.debug(f"開始解析內容，Parser type: {parser_type}, Config: {parser_config}")

        if parser_type == 'csv':
            # CSV 可能需要 encoding，如果 BytesIO 直接傳給 read_csv，它會期望 text-like
            # 因此，我們先用 recipe 中指定的 encoding (或預設) 解碼 bytes 為 string
            # 如果 recipe 中沒有 encoding，可以嘗試用一個通用預設 (如 utf-8) 或從 detector 獲取
            encoding = parser_config.pop('encoding', self.format_detector.try_encodings[0] if self.format_detector.try_encodings else 'utf-8')
            try:
                decoded_content = raw_content.decode(encoding)
                data_io = StringIO(decoded_content)
                df = pd.read_csv(data_io, **parser_config)
            except UnicodeDecodeError as ude:
                logger.error(f"使用編碼 '{encoding}' 解碼 CSV 內容失敗: {ude}")
                raise # 重新拋出，讓 _process_file 捕獲
            except Exception as e:
                logger.error(f"pd.read_csv 執行失敗 (config: {parser_config}): {e}")
                raise
        elif parser_type == 'excel':
            # read_excel 可以直接處理 BytesIO
            try:
                df = pd.read_excel(data_io, **parser_config)
            except Exception as e:
                logger.error(f"pd.read_excel 執行失敗 (config: {parser_config}): {e}")
                raise
        elif parser_type == 'fixed_width':
            # read_fwf 通常也需要 text-like input
            encoding = parser_config.pop('encoding', self.format_detector.try_encodings[0] if self.format_detector.try_encodings else 'utf-8')
            try:
                decoded_content = raw_content.decode(encoding)
                data_io = StringIO(decoded_content)
                df = pd.read_fwf(data_io, **parser_config)
            except UnicodeDecodeError as ude:
                logger.error(f"使用編碼 '{encoding}' 解碼 FWF 內容失敗: {ude}")
                raise
            except Exception as e:
                logger.error(f"pd.read_fwf 執行失敗 (config: {parser_config}): {e}")
                raise
        # 可以根據需要擴展更多 parser_type (json, html, etc.)
        else:
            logger.error(f"不支援的 parser_type: {parser_type}")
            raise ValueError(f"不支援的 parser_type: {parser_type}")

        logger.info(f"內容解析完成，得到 DataFrame shape: {df.shape}")
        return df

    def run(self):
        """
        執行轉換管線的主方法。
        1. 從 DBManager 獲取狀態為 RAW_INGESTED 的檔案列表。
        2. 使用 ProcessPoolExecutor 平行處理這些檔案。
        3. 收集處理結果並更新 manifest 中的檔案狀態。
        """
        logger.info("開始執行轉換管線...")

        # 在 run 方法作用域內建立 DBManager 實例，用於主進程操作
        # 這個 db_manager_main 實例不會傳遞給子進程
        db_manager_main = None
        try:
            db_manager_main = DBManager(self.db_path)

            # 1a. 查詢待處理檔案列表
            try:
                if not hasattr(db_manager_main, 'get_manifest_records_by_status'):
                    logger.error("DBManager 缺少 'get_manifest_records_by_status' 方法。轉換管線將無法獲取待處理檔案。")
                    raise NotImplementedError("DBManager 必須實作 get_manifest_records_by_status(status) 方法。")
                files_to_process = db_manager_main.get_manifest_records_by_status(self.STATUS_RAW_INGESTED)
            except Exception as e:
                logger.error(f"從資料庫查詢待處理檔案時發生錯誤: {e}", exc_info=True)
                return # 無法獲取檔案，提前結束

            # 1b. 如果列表為空
            if not files_to_process:
                logger.info("在 manifest 中沒有找到狀態為 RAW_INGESTED 的檔案，轉換管線提前結束。")
                return

            logger.info(f"找到 {len(files_to_process)} 個狀態為 RAW_INGESTED 的檔案待處理。")

            results = []
            # 1c. 使用 ProcessPoolExecutor 平行處理
            # 現在 _process_file 將被設計為一個靜態方法或頂層函式，
            # 或者是一個可以安全傳遞的實例方法 (如果它不直接使用 self.db_manager)
            # 我們將把 db_path, format_detector, format_catalog 傳遞給 worker。

            # 為了讓 _process_file 能夠作為一個方法被 submit，但內部使用獨立的 DB 連線，
            # 它需要 self.db_path。 self.format_detector 和 self.format_catalog 假設是可序列化的。

            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                logger.info(f"使用 ProcessPoolExecutor (max_workers={self.max_workers or '預設'}) 提交 {len(files_to_process)} 個檔案進行處理。")

                # 傳遞給 _process_file 的參數現在應該是可序列化的。
                # self.format_detector 和 self.format_catalog 假設是。
                # self.db_path 也是。
                # _process_file 將在其內部使用 self.db_path 建立 DBManager。
                future_to_file_hash = {
                    executor.submit(self._process_file_worker, # 使用靜態/頂層 worker 或可安全傳遞的 self._process_file
                                    file_info,
                                    self.db_path,
                                    self.format_detector, # 假設可序列化
                                    self.format_catalog,  # 假設可序列化
                                    { # 將狀態值作為字典傳遞，避免在 worker 中依賴 self
                                        'STATUS_QUARANTINED': self.STATUS_QUARANTINED,
                                        'STATUS_TRANSFORMATION_FAILED': self.STATUS_TRANSFORMATION_FAILED,
                                        'STATUS_TRANSFORMED_SUCCESS': self.STATUS_TRANSFORMED_SUCCESS
                                    }
                                   ): file_info.get('file_hash', 'UNKNOWN_HASH')
                    for file_info in files_to_process
                }

                for future in as_completed(future_to_file_hash):
                    file_hash_completed = future_to_file_hash[future]
                    try:
                        result = future.result()
                        results.append(result)
                        logger.info(f"檔案 {file_hash_completed} 處理完成。結果: {result.get('status', 'UNKNOWN_STATUS')}")
                    except Exception as exc:
                        logger.error(f"檔案 {file_hash_completed} 在平行處理過程中產生了未捕獲的異常: {exc}", exc_info=True)
                        results.append({
                            'file_hash': file_hash_completed,
                            'status': self.STATUS_TRANSFORMATION_FAILED,
                            'error_message': f"平行處理異常: {exc}",
                            'processed_rows': 0
                        })

            logger.info(f"所有 {len(files_to_process)} 個檔案的平行處理階段已完成。共收到 {len(results)} 個結果。")
            self._update_manifest_with_results(db_manager_main, results)

        except Exception as e:
            logger.error(f"TransformationPipeline run 方法執行期間發生未預期錯誤: {e}", exc_info=True)
        finally:
            if db_manager_main:
                db_manager_main.close()
                logger.info("主進程 DBManager 連線已關閉。")

        logger.info("轉換管線執行完畢。")

    def _update_manifest_with_results(self, db_manager_instance: DBManager, results: List[Dict[str, Any]]):
        """
        根據處理結果更新 manifest 表中的檔案狀態。
        使用傳入的 db_manager_instance。
        """
        if not results:
            logger.info("沒有處理結果需要更新到 manifest。")
            return

        logger.info(f"開始將 {len(results)} 個處理結果更新到 manifest...")
        success_count = 0
        quarantined_count = 0
        failed_count = 0

        for result in results:
            file_hash = result.get('file_hash')
            status_val = result.get('status') # 'status' 是 result 中的 key，其值是狀態字串
            error_message = result.get('error_message')
            processed_rows = result.get('processed_rows')

            if not file_hash or not status_val:
                logger.error(f"結果缺少 file_hash 或 status，無法更新 manifest: {result}")
                continue

            try:
                if not hasattr(db_manager_instance, 'update_manifest_transformation_status'):
                    logger.error(f"DBManager 缺少 'update_manifest_transformation_status' 方法。無法更新檔案 '{file_hash}' 的狀態。")
                    failed_count +=1
                    continue
                else:
                     db_manager_instance.update_manifest_transformation_status(
                        file_hash=file_hash,
                        status=status_val, # 使用 status_val
                        error_message=error_message,
                        processed_rows=processed_rows
                    )

                if status_val == self.STATUS_TRANSFORMED_SUCCESS:
                    success_count += 1
                elif status_val == self.STATUS_QUARANTINED:
                    quarantined_count += 1
                elif status_val == self.STATUS_TRANSFORMATION_FAILED:
                    failed_count += 1
                logger.debug(f"Manifest 更新：檔案 {file_hash} 狀態更新為 {status_val}。")

            except Exception as e:
                logger.error(f"更新檔案 {file_hash} 在 manifest 中的狀態為 {status_val} 時發生錯誤: {e}", exc_info=True)
                failed_count +=1

        logger.info("--- 轉換結果摘要 ---")
        logger.info(f"成功轉換檔案數: {success_count}")
        logger.info(f"隔離檔案數: {quarantined_count}")
        logger.info(f"轉換失敗檔案數 (含 manifest 更新失敗): {failed_count}")
        logger.info("----------------------")

    @staticmethod
    def _process_file_worker(file_info: Dict[str, Any],
                             db_path: str,
                             format_detector_instance: 'FormatDetector',
                             format_catalog_instance: Dict[str, Any],
                             statuses: Dict[str,str]
                            ) -> Dict[str, Any]:
        """
        單個檔案的處理邏輯 (靜態方法，適合在 ProcessPoolExecutor 中執行)。
        """
        file_hash = file_info.get('file_hash')
        original_path = file_info.get('original_path', 'N/A')
        processed_rows = 0

        # 從 statuses 字典中獲取狀態值
        status_quarantined = statuses['STATUS_QUARANTINED']
        status_failed = statuses['STATUS_TRANSFORMATION_FAILED']
        status_success = statuses['STATUS_TRANSFORMED_SUCCESS']

        local_db_manager = None
        try:
            local_db_manager = DBManager(db_path)
            logger.info(f"[Worker:{original_path}] 開始處理。")

            if not hasattr(local_db_manager, 'get_raw_file_content'):
                raise NotImplementedError("DBManager 缺少 get_raw_file_content 方法。")
            raw_content = local_db_manager.get_raw_file_content(file_hash)

            if raw_content is None:
                logger.error(f"[Worker:{original_path}] 找不到原始內容。")
                return {'file_hash': file_hash, 'status': status_failed, 'error_message': "Raw content not found"}

            recipe = format_detector_instance.get_recipe(raw_content, format_catalog_instance)

            if recipe is None:
                logger.warning(f"[Worker:{original_path}] 未找到配方，隔離。")
                return {'file_hash': file_hash, 'status': status_quarantined, 'error_message': "No matching recipe found"}

            logger.info(f"[Worker:{original_path}] 匹配到配方: {recipe.get('name', '未命名')}")

            # 為了讓 _parse_raw_content 能被靜態方法調用，它也需要是靜態的或頂層的
            # 或者 TransformationPipeline._parse_raw_content 被傳入
            # 這裡假設 _parse_raw_content 是 TransformationPipeline 的一個 (可能是靜態的) 輔助方法
            # 為了簡單，我們先假設它可以被這樣訪問，或者將其邏輯內聯/傳遞
            # df = TransformationPipeline._parse_raw_content(raw_content, recipe)
            # ^^^ 這樣調用靜態方法是可以的，但 _parse_raw_content 目前是實例方法
            # 我們需要傳入 format_detector_instance.try_encodings 給 _parse_raw_content
            # 或者讓 _parse_raw_content 成為獨立函式

            # 暫時將 _parse_raw_content 的邏輯視為可以在此處調用
            # 實際上，我們需要將 _parse_raw_content 也改為靜態方法或傳遞 self (如果 self 可序列化)
            # 或者，更簡單的是，讓 TransformationPipeline 的實例在創建時就配置好 parser
            # 但這與動態配方不符。
            #
            # 正確做法: _parse_raw_content 也應是靜態的，或其邏輯在此處重現/調用。
            # 為了推進，我們假設可以調用一個等效的解析函式。
            # df = parse_raw_content_static(raw_content, recipe, format_detector_instance.try_encodings)
            #
            # 我們需要將 _parse_raw_content 移出或改為靜態。
            # 假設我們有一個靜態的 _static_parse_raw_content
            df = TransformationPipeline._static_parse_raw_content(raw_content, recipe, format_detector_instance.try_encodings)


            cleaner_function_name = recipe.get('cleaner_function')
            if not cleaner_function_name:
                raise ValueError("配方中未指定 cleaner_function。")

            cleaners_module = importlib.import_module('taifex_pipeline.transformation.cleaners')
            cleaner_function = getattr(cleaners_module, cleaner_function_name)

            logger.info(f"[Worker:{original_path}] 開始清洗...")
            cleaned_df = cleaner_function(df)
            processed_rows = len(cleaned_df)
            logger.info(f"[Worker:{original_path}] 清洗完成，共 {processed_rows} 行。")

            target_table = recipe.get('target_table')
            if not target_table:
                raise ValueError("配方中未指定 target_table。")

            if not hasattr(local_db_manager, 'load_dataframe_to_table'):
                raise NotImplementedError("DBManager 缺少 load_dataframe_to_table 方法。")

            load_options = recipe.get('load_options', {'if_exists': 'append'})
            local_db_manager.load_dataframe_to_table(cleaned_df, target_table, load_options)
            logger.info(f"[Worker:{original_path}] 數據已載入到 '{target_table}'。")

            return {
                'file_hash': file_hash,
                'status': status_success,
                'processed_rows': processed_rows,
                'target_table': target_table
            }
        except NotImplementedError as nie:
            logger.error(f"[Worker:{original_path}] 處理時發生錯誤: {nie}", exc_info=True)
            return {'file_hash': file_hash, 'status': status_failed, 'error_message': str(nie)}
        except Exception as e:
            logger.error(f"[Worker:{original_path}] 處理時發生未預期錯誤: {e}", exc_info=True)
            return {'file_hash': file_hash, 'status': status_failed, 'error_message': str(e)}
        finally:
            if local_db_manager:
                local_db_manager.close()
                logger.info(f"[Worker:{original_path}] 局域 DBManager 連線已關閉。")

    @staticmethod
    def _static_parse_raw_content(raw_content: bytes, recipe: Dict[str, Any], default_encodings: List[str]) -> pd.DataFrame:
        """
        (靜態版本) 根據配方中的 parser_config 解析原始內容為 DataFrame。
        """
        parser_type = recipe.get('parser_type', 'csv').lower()
        parser_config = recipe.get('parser_config', {})
        data_io = BytesIO(raw_content)
        logger.debug(f"[ParserWorker] 開始解析內容，Parser type: {parser_type}, Config: {parser_config}")

        if parser_type == 'csv':
            encoding = parser_config.pop('encoding', default_encodings[0] if default_encodings else 'utf-8')
            try:
                decoded_content = raw_content.decode(encoding)
                data_io = StringIO(decoded_content)
                df = pd.read_csv(data_io, **parser_config)
            except UnicodeDecodeError as ude:
                logger.error(f"[ParserWorker] 使用編碼 '{encoding}' 解碼 CSV 內容失敗: {ude}")
                raise
            except Exception as e:
                logger.error(f"[ParserWorker] pd.read_csv 執行失敗 (config: {parser_config}): {e}")
                raise
        elif parser_type == 'excel':
            try:
                df = pd.read_excel(data_io, **parser_config)
            except Exception as e:
                logger.error(f"[ParserWorker] pd.read_excel 執行失敗 (config: {parser_config}): {e}")
                raise
        elif parser_type == 'fixed_width':
            encoding = parser_config.pop('encoding', default_encodings[0] if default_encodings else 'utf-8')
            try:
                decoded_content = raw_content.decode(encoding)
                data_io = StringIO(decoded_content)
                df = pd.read_fwf(data_io, **parser_config)
            except UnicodeDecodeError as ude:
                logger.error(f"[ParserWorker] 使用編碼 '{encoding}' 解碼 FWF 內容失敗: {ude}")
                raise
            except Exception as e:
                logger.error(f"[ParserWorker] pd.read_fwf 執行失敗 (config: {parser_config}): {e}")
                raise
        else:
            logger.error(f"[ParserWorker] 不支援的 parser_type: {parser_type}")
            raise ValueError(f"不支援的 parser_type: {parser_type}")

        logger.info(f"[ParserWorker] 內容解析完成，得到 DataFrame shape: {df.shape}")
        return df

    # def _process_file(self, file_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        單個檔案的處理邏輯。
        此方法將在 ProcessPoolExecutor 的一個 worker process 中執行。
        注意：此方法內部不應直接使用 self.db_manager (如果它包含不可序列化的連線)。
              它應該通過 file_info 或其他方式獲取 db_path 並在內部建立 DBManager 連線，
              或者 DBManager 設計為可在子進程中安全使用。

              為了簡化，此處的實作暫時假設：
              1. self.format_detector 和 self.format_catalog 是可序列化的。
              2. self.db_manager 的操作 (get_raw_file_content, load_processed_data)
                 能夠在子進程中被正確調用 (可能意味著它們內部處理連線，或者 DuckDB 允許這樣)。
                 這在實際中通常需要特別處理 DB 連線的生命週期。
                 一個常見模式是在 _process_file 開始時建立一個新的 DBManager(self.db_manager.db_path)，
                 並在結束時關閉它。
        """
        file_hash = file_info.get('file_hash')
        original_path = file_info.get('original_path', 'N/A') # 用於日誌
        processed_rows = 0

        # 為了在子進程中安全使用 DBManager，我們應該在這裡重新實例化它
        # 這需要 db_path 能從 self.db_manager 獲取，或者在 __init__ 時也存儲 db_path
        # 假設 self.db_manager 有一個 .db_path 屬性
        # current_process_db_manager = DBManager(self.db_manager.db_path) # 錯誤: self.db_manager 不可用

        # 正確的方式是，主進程不傳遞 self.db_manager 給子進程調用的函數。
        # _process_file 應該是靜態的或頂層的，並接收 db_path, format_detector, format_catalog。
        #
        # 為了這個步驟的重點，我們將**暫時假設**以下程式碼在子進程中神奇地工作，
        # 或者 DBManager 內部有魔法。在實際部署時，這部分需要仔細審查和重構以確保進程安全。
        #
        # **實際的多進程DB處理策略**：
        # 1. 傳遞 db_path，在 _process_file 中 `db_man = DBManager(db_path)`。
        # 2. 如果使用資料庫連接池，從池中獲取連接。
        # DuckDB 對於同一個檔案的多個寫入連接（尤其來自不同進程）需要小心處理，
        # 預設情況下，一個 DuckDB 資料庫檔案一次只能有一個寫入程序。
        # 如果是多個讀取和少量、序列化的寫入（例如 manifest 更新），可能還好。
        # 但如果 load_processed_data 是並發寫入到同一個 DuckDB 檔案的不同表，會有問題。
        #
        # **目前的簡化假設**：self.db_manager 的方法是進程安全的，或者 DuckDB 能處理。
        # 這是一個強假設，很可能在實際運行中失敗或導致數據損壞。
        #
        # **更安全的臨時方案**：不在 `_process_file` 中做資料庫寫入 (`load_processed_data` 和 manifest 更新)。
        # `_process_file` 只返回 `cleaned_df` 和 `target_table`，由主進程統一寫入。
        # 但這會失去部分平行處理的好處，且傳輸大的 DataFrame 回主進程有開銷。
        #
        # 依照原始任務要求，`_process_file` 內部需要呼叫 `DBManager` 載入數據。
        # 這意味著我們需要一個方法來讓 `_process_file` 中的 `DBManager` 能夠工作。
        # 最直接（但不一定最優）的是在 `_process_file` 內創建一個新的 `DBManager` 實例。
        # 這就需要 `TransformationPipeline` 在初始化時也保存 `db_path`。
        #
        # 修改 __init__ 以保存 db_path:
        # self.db_path = db_manager.db_path (假設 db_manager 有此屬性)
        #
        # 然後在 _process_file 中:
        # local_db_manager = DBManager(self.db_path)
        # try:
        #    ... 使用 local_db_manager ...
        # finally:
        #    local_db_manager.close()
        #
        # 鑑於目前 `DBManager` 的 `__init__` 簽名是 `db_path: str`，這是可行的。
        # 我將假設 `TransformationPipeline.__init__` 會保存 `db_path`。
        # (這需要在上一個步驟或 __init__ 的實作中加入)
        #
        # **為了保持此步驟的重點，我將直接使用 self.db_manager，並在註解中強調這點的潛在問題。**
        # **真正的解決方案是傳遞db_path並在worker中創建新DBManager實例。**

        logger.info(f"[Worker] 開始處理檔案: {original_path} (hash: {file_hash})")

        try:
            # a. 讀取原始檔案內容
            # 假設 DBManager 有 get_raw_file_content(file_hash) -> bytes
            if not hasattr(self.db_manager, 'get_raw_file_content'):
                raise NotImplementedError(f"DBManager 缺少 get_raw_file_content 方法。 (在 _process_file 中針對 {file_hash})")
            raw_content = self.db_manager.get_raw_file_content(file_hash)
            if raw_content is None:
                logger.error(f"[Worker] 找不到檔案 {file_hash} 的原始內容。")
                return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': "Raw content not found"}

            # b. 獲取處理配方
            recipe = self.format_detector.get_recipe(raw_content, self.format_catalog)

            # c. 如果沒有配方
            if recipe is None:
                logger.warning(f"[Worker] 檔案 {file_hash} ({original_path}) 未找到處理配方，將其隔離。")
                return {'file_hash': file_hash, 'status': self.STATUS_QUARANTINED, 'error_message': "No matching recipe found"}

            logger.info(f"[Worker] 檔案 {file_hash} 匹配到配方: {recipe.get('name', '未命名')}")

            # d. 如果有配方
            # i. 解析 raw_content 為 DataFrame
            #    _parse_raw_content 已在類中定義
            df = self._parse_raw_content(raw_content, recipe)

            # ii. 獲取清洗函式名稱
            cleaner_function_name = recipe.get('cleaner_function')
            if not cleaner_function_name:
                logger.error(f"[Worker] 配方 {recipe.get('name')} 未指定 cleaner_function。")
                return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': "Cleaner function not specified in recipe"}

            # iii. 動態獲取清洗函式物件
            try:
                cleaners_module = importlib.import_module('taifex_pipeline.transformation.cleaners')
                cleaner_function = getattr(cleaners_module, cleaner_function_name)
            except (ImportError, AttributeError) as import_err:
                logger.error(f"[Worker] 無法載入清洗函式 '{cleaner_function_name}': {import_err}")
                return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': f"Failed to load cleaner: {cleaner_function_name}"}

            # iv. 呼叫清洗函式
            logger.info(f"[Worker] 檔案 {file_hash} 開始使用清洗函式 '{cleaner_function_name}' 進行清洗...")
            cleaned_df = cleaner_function(df)
            processed_rows = len(cleaned_df)
            logger.info(f"[Worker] 檔案 {file_hash} 清洗完成，處理後共 {processed_rows} 行數據。")

            # v. 載入清洗後的 DataFrame
            target_table = recipe.get('target_table')
            if not target_table:
                logger.error(f"[Worker] 配方 {recipe.get('name')} 未指定 target_table。")
                return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': "Target table not specified in recipe"}

            # 假設 DBManager 有 load_data_to_table(df, table_name, options)
            load_options = recipe.get('load_options', {'if_exists': 'append'}) # 預設追加

            if not hasattr(self.db_manager, 'load_dataframe_to_table'):
                 raise NotImplementedError(f"DBManager 缺少 load_dataframe_to_table 方法。 (在 _process_file 中針對 {file_hash})")

            self.db_manager.load_dataframe_to_table(cleaned_df, target_table, load_options)
            logger.info(f"[Worker] 檔案 {file_hash} 的已處理數據已成功載入到表格 '{target_table}'。")

            # vi. 返回成功結果
            return {
                'file_hash': file_hash,
                'status': self.STATUS_TRANSFORMED_SUCCESS,
                'processed_rows': processed_rows,
                'target_table': target_table
            }

        except NotImplementedError as nie: # 捕獲我們自己拋出的 NotImplementedError
            logger.error(f"[Worker] 處理檔案 {file_hash} ({original_path}) 時發生錯誤: {nie}", exc_info=True)
            return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': str(nie)}
        except Exception as e:
            logger.error(f"[Worker] 處理檔案 {file_hash} ({original_path}) 時發生未預期錯誤: {e}", exc_info=True)
            return {'file_hash': file_hash, 'status': self.STATUS_TRANSFORMATION_FAILED, 'error_message': str(e)}
        # finally:
            # 如果在 _process_file 中建立了 local_db_manager，則在此處關閉
            # if 'local_db_manager' in locals() and local_db_manager:
            #     local_db_manager.close()

# --- DBManager 接口假設 ---
# class DBManager:
#     def __init__(self, db_path: str):
#         self.db_path = db_path # TransformationPipeline 需要這個來在 worker 中重建 DBManager
#         # ...
#
#     def get_manifest_records_by_status(self, status: str) -> List[Dict[str, Any]]:
#         # 返回 [{'file_hash': 'h1', 'original_path': 'p1'}, ...]
#         raise NotImplementedError
#
#     def get_raw_file_content(self, file_hash: str) -> Optional[bytes]:
#         raise NotImplementedError
#
#     def load_dataframe_to_table(self, df: pd.DataFrame, table_name: str, options: Dict[str, Any]):
#         # options 例如 {'if_exists': 'append'}
#         raise NotImplementedError
#
#     def update_manifest_transformation_status(self, file_hash: str, status: str, error_message: Optional[str] = None, processed_rows: Optional[int] = None):
#         # 更新 manifest 表中特定 file_hash 記錄的轉換相關欄位
#         # 例如: status, error_message, processed_rows, transformation_start_timestamp, transformation_end_timestamp
#         raise NotImplementedError
#
#     def close(self):
#         pass
# --- End DBManager 接口假設 ---
