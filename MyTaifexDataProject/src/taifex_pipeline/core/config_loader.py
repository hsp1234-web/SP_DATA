# -*- coding: utf-8 -*-
"""
核心共用模組：設定檔讀取 (Config Loader)

實現讀取 `config/format_catalog.json` 的功能。
"""
import json
from pathlib import Path
from typing import Dict, Any, Optional

from .logger_setup import get_logger

logger = get_logger(__name__)

_config_cache: Optional[Dict[str, Any]] = None

def get_format_catalog(
    config_file_name: str = "format_catalog.json",
    config_dir_name: str = "config"
) -> Dict[str, Any]:
    """
    讀取並返回格式指紋目錄 (format_catalog.json) 的內容。

    為了效能，此函式會快取第一次讀取的結果。
    如果需要重新載入設定檔（例如，設定檔在運行時被修改），
    可以先呼叫 `clear_config_cache()`。

    Args:
        config_file_name (str): 設定檔的名稱。
        config_dir_name (str): 相對於專案根目錄的設定檔存放目錄名稱。

    Returns:
        Dict[str, Any]: 解析後的 JSON 物件 (字典)。

    Raises:
        FileNotFoundError: 如果設定檔不存在。
        json.JSONDecodeError: 如果設定檔內容不是有效的 JSON。
        Exception: 其他讀取或解析錯誤。
    """
    global _config_cache
    if _config_cache is not None:
        logger.debug("從快取返回 format_catalog 設定。")
        return _config_cache

    try:
        # 專案根目錄的確定方式，這裡假設 config_loader.py 在 src/taifex_pipeline/core/
        project_root = Path(__file__).resolve().parent.parent.parent.parent
        config_path = project_root / config_dir_name / config_file_name

        if not config_path.exists():
            logger.error(f"設定檔未找到: {config_path}")
            raise FileNotFoundError(f"設定檔未找到: {config_path}")

        logger.info(f"正在從 {config_path} 讀取 format_catalog 設定...")
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        _config_cache = config_data
        logger.info(f"成功讀取並快取 format_catalog，共 {len(config_data)} 個格式定義。")
        return config_data

    except json.JSONDecodeError as e:
        logger.error(f"解析設定檔 {config_path} 失敗: JSON 格式錯誤 - {e}", exc_info=True)
        raise
    except FileNotFoundError: # 已在前面處理，這裡再次捕獲以防萬一
        raise
    except Exception as e:
        logger.error(f"讀取設定檔 {config_path} 時發生未預期錯誤: {e}", exc_info=True)
        # 在實際應用中，可能需要更細緻的錯誤處理或預設配置
        raise

def clear_config_cache() -> None:
    """
    清除已快取的設定檔內容。
    下次呼叫 `get_format_catalog()` 時將會重新從檔案讀取。
    """
    global _config_cache
    _config_cache = None
    logger.info("format_catalog 設定快取已清除。")

# --- 範例使用 (通常在需要配方的模組中導入並使用) ---
if __name__ == "__main__":
    # 為了測試，我們先手動在 MyTaifexDataProject/config/ 目錄下創建一個假的 format_catalog.json
    # 實際運行時，此檔案應由使用者或格式註冊腳本維護
    try:
        demo_config_dir = Path(__file__).resolve().parent.parent.parent.parent / "config"
        demo_config_dir.mkdir(exist_ok=True)
        demo_file_path = demo_config_dir / "format_catalog.json"

        if not demo_file_path.exists():
            sample_catalog = {
                "fingerprint_test_123": {
                    "description": "測試用格式範例",
                    "target_table": "test_table",
                    "parser_config": {"sep": ","},
                    "cleaner_function": "clean_test_data",
                    "required_columns": ["colA", "colB"]
                }
            }
            with open(demo_file_path, 'w', encoding='utf-8') as f_demo:
                json.dump(sample_catalog, f_demo, indent=2, ensure_ascii=False)
            print(f"已創建範例設定檔: {demo_file_path}")

        catalog = get_format_catalog()
        print("\n成功讀取的 Format Catalog:")
        print(json.dumps(catalog, indent=2, ensure_ascii=False))

        # 測試快取
        print("\n再次讀取 (應從快取):")
        catalog_cached = get_format_catalog()
        assert id(catalog) == id(catalog_cached) # 驗證是否為同一物件
        print("從快取讀取成功。")

        # 測試清除快取並重新讀取
        clear_config_cache()
        print("\n清除快取後重新讀取:")
        catalog_reloaded = get_format_catalog()
        assert id(catalog_cached) != id(catalog_reloaded) # 應為不同物件
        print("重新讀取成功。")
        print(json.dumps(catalog_reloaded, indent=2, ensure_ascii=False))

    except Exception as e:
        print(f"範例執行發生錯誤: {e}")
    finally:
        # 清理範例檔案 (可選)
        # if demo_file_path.exists():
        #     demo_file_path.unlink()
        #     print(f"\n已刪除範例設定檔: {demo_file_path}")
        pass
