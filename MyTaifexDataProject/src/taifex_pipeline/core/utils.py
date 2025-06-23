# -*- coding: utf-8 -*-
"""
核心共用模組：通用工具函式 (Utilities)

放置可能被多個模組共用的輔助函式。
"""
import hashlib
import uuid
from pathlib import Path
from typing import Union

from .logger_setup import get_logger

logger = get_logger(__name__)

def generate_execution_id() -> str:
    """
    產生一個全域唯一的執行 ID (Execution ID)。

    Returns:
        str: UUID 字串。
    """
    return str(uuid.uuid4())

def calculate_file_sha256(file_path: Union[str, Path]) -> Optional[str]:
    """
    計算檔案內容的 SHA256 雜湊值。

    Args:
        file_path (Union[str, Path]): 要計算雜湊值的檔案路徑。

    Returns:
        Optional[str]: 檔案的 SHA256 雜湊值 (十六進位字串)。
                       如果檔案不存在或讀取錯誤，則返回 None。
    """
    try:
        path_obj = Path(file_path)
        if not path_obj.is_file():
            logger.error(f"計算 SHA256 失敗：檔案不存在或不是一個有效檔案 - {file_path}")
            return None

        sha256_hash = hashlib.sha256()
        with open(path_obj, "rb") as f:
            # 分塊讀取以處理大檔案
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        hex_digest = sha256_hash.hexdigest()
        logger.debug(f"檔案 {file_path} 的 SHA256 雜湊值: {hex_digest}")
        return hex_digest
    except IOError as e:
        logger.error(f"計算檔案 {file_path} SHA256 時發生 IO 錯誤: {e}", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"計算檔案 {file_path} SHA256 時發生未預期錯誤: {e}", exc_info=True)
        return None

def calculate_bytes_sha256(data_bytes: bytes) -> str:
    """
    計算位元組串的 SHA256 雜湊值。

    Args:
        data_bytes (bytes): 要計算雜湊值的位元組內容。

    Returns:
        str: 位元組內容的 SHA256 雜湊值 (十六進位字串)。
    """
    sha256_hash = hashlib.sha256()
    sha256_hash.update(data_bytes)
    hex_digest = sha256_hash.hexdigest()
    logger.debug(f"位元組內容的 SHA256 雜湊值 (前10位元組): {data_bytes[:10]}... -> {hex_digest}")
    return hex_digest


# --- 範例使用 ---
if __name__ == "__main__":
    # 測試 execution_id
    exec_id1 = generate_execution_id()
    exec_id2 = generate_execution_id()
    print(f"Execution ID 1: {exec_id1}")
    print(f"Execution ID 2: {exec_id2}")
    assert exec_id1 != exec_id2

    # 測試檔案 SHA256 計算
    # 創建一個臨時檔案來測試
    temp_file_dir = Path(__file__).parent
    temp_file_path = temp_file_dir / "temp_test_file_for_sha256.txt"

    try:
        with open(temp_file_path, "w", encoding="utf-8") as tf:
            tf.write("這是Jules用於測試SHA256計算的檔案內容。\n")
            tf.write("Hello, Taifex Pipeline!")

        file_hash = calculate_file_sha256(temp_file_path)
        print(f"\n檔案 '{temp_file_path.name}' 的 SHA256 雜湊值: {file_hash}")

        # 預期雜湊值 (根據內容 "這是Jules用於測試SHA256計算的檔案內容。\nHello, Taifex Pipeline!" 計算)
        # 注意：不同作業系統的換行符可能導致雜湊不同。確保一致性。
        # 如果使用 git，它可能會自動轉換換行符。
        # Python 的 open(..., "w") 在 Windows 上預設使用 \r\n，在 Linux/macOS 上使用 \n
        # 為了測試一致性，可以明確指定 newline='' 或 'wb' 模式寫入 bytes

        # 測試 bytes SHA256 計算
        byte_content = b"Hello, SHA256 for bytes!"
        bytes_hash = calculate_bytes_sha256(byte_content)
        print(f"位元組內容 '{byte_content!r}' 的 SHA256 雜湊值: {bytes_hash}")
        # 預期: '20130221c2c0f8ef997256c77a08a73c9320739007506311a5d9cb938ab19380'

    except Exception as e:
        print(f"工具函式範例執行錯誤: {e}")
    finally:
        if temp_file_path.exists():
            temp_file_path.unlink()
            print(f"已刪除臨時檔案: {temp_file_path.name}")

    print("\n核心工具函式模組測試完成。")
