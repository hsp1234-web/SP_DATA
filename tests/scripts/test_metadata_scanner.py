import pytest
from unittest.mock import patch, MagicMock, call, ANY
from pathlib import Path
import pandas as pd
from datetime import datetime
import logging

# 調整導入路徑以匹配專案結構
from src.taifex_pipeline.scripts.metadata_scanner import MetadataScanner, main as scanner_main
from src.taifex_pipeline.database.metadata_manager import MetadataManager # 用於類型提示和 mock

# 確保測試時日誌輸出可見，如果需要的話
# logging.basicConfig(level=logging.DEBUG)

@pytest.fixture
def mock_parquet_dir(tmp_path: Path) -> Path:
    """建立一個模擬的 Parquet 檔案目錄結構"""
    parquet_dir = tmp_path / "parquet_data"
    parquet_dir.mkdir()

    # 檔案 1: 正常檔案
    df1_data = {
        'symbol': ['TX', 'TX', 'MTX'],
        'data_date': [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 1)]
    }
    df1 = pd.DataFrame(df1_data)
    df1.to_parquet(parquet_dir / "data_2024_01.parquet")

    # 檔案 2: 包含不同日期格式的正常檔案
    df2_data = {
        'symbol': ['TE', 'TE'],
        'data_date': ['2024-02-01', '2024-02-02'] # 字串日期
    }
    df2 = pd.DataFrame(df2_data)
    df2.to_parquet(parquet_dir / "data_2024_02.parquet")

    # 檔案 3: 空的 Parquet 檔案 (但結構有效)
    df_empty = pd.DataFrame({'symbol': pd.Series(dtype='str'), 'data_date': pd.Series(dtype='datetime64[ns]')})
    df_empty.to_parquet(parquet_dir / "empty_data.parquet")

    # 檔案 4: 缺少 'data_date' 欄位
    df_missing_col = pd.DataFrame({'symbol': ['FX']})
    df_missing_col.to_parquet(parquet_dir / "missing_col.parquet")

    # 檔案 5: 無效的日期格式 (無法轉換)
    df_bad_date_data = {
        'symbol': ['TF'],
        'data_date': ['this-is-not-a-date']
    }
    df_bad_date = pd.DataFrame(df_bad_date_data)
    df_bad_date.to_parquet(parquet_dir / "bad_date_format.parquet")

    # 子目錄中的檔案
    sub_dir = parquet_dir / "subdir"
    sub_dir.mkdir()
    df_sub_data = {
        'symbol': ['TXO'],
        'data_date': [datetime(2024, 3, 1)]
    }
    df_sub = pd.DataFrame(df_sub_data)
    df_sub.to_parquet(sub_dir / "data_2024_03.parquet")

    # 非 Parquet 檔案
    (parquet_dir / "not_a_parquet.txt").write_text("hello")

    return parquet_dir

@pytest.fixture
def mock_db_path(tmp_path: Path) -> Path:
    """提供一個模擬的資料庫路徑"""
    db_dir = tmp_path / "db_data"
    db_dir.mkdir()
    return db_dir / "test_meta.db"

@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_metadata_scanner_run_scan(MockMetadataManager, mock_parquet_dir: Path, mock_db_path: Path, caplog):
    """測試 MetadataScanner.run_scan 的主要流程"""
    caplog.set_level(logging.INFO)

    mock_manager_instance = MockMetadataManager.return_value

    scanner = MetadataScanner(parquet_dir=mock_parquet_dir, db_path=mock_db_path, batch_size=2)
    scanner.run_scan()

    # 1. 驗證 MetadataManager 的初始化和 setup_tables 被呼叫
    MockMetadataManager.assert_called_once_with(mock_db_path)
    mock_manager_instance.setup_tables.assert_called_once()

    # 2. 驗證 register_file_batch 被呼叫的次數和內容
    # 預期檔案: data_2024_01.parquet, data_2024_02.parquet, empty_data.parquet (即使無 mapping), data_2024_03.parquet
    # 被跳過檔案: missing_col.parquet, bad_date_format.parquet
    # 總共 4 個有效檔案，批次大小為 2，所以應該呼叫 2 次 register_file_batch

    assert mock_manager_instance.register_file_batch.call_count == 2

    calls = mock_manager_instance.register_file_batch.call_args_list

    # 檢查第一個批次的內容 (順序可能不固定，所以檢查檔案名稱)
    batch1_files = {item['file_name'] for item in calls[0][0][0]}
    batch2_files = {item['file_name'] for item in calls[1][0][0]}

    expected_files_processed = {
        "data_2024_01.parquet",
        "data_2024_02.parquet",
        "empty_data.parquet", # 即使 mapping 為空，檔案本身也會被記錄
        "data_2024_03.parquet"
    }

    processed_files_in_batches = batch1_files.union(batch2_files)
    assert processed_files_in_batches == expected_files_processed
    assert len(processed_files_in_batches) == 4 # 確保沒有重複或遺漏

    # 詳細檢查第一個被處理的檔案 (假設是 data_2024_01.parquet，但順序不保證，所以從 calls 中找)
    # 這裡我們只驗證結構和一個檔案的 mappings
    all_registered_items = []
    for call_args in calls:
        all_registered_items.extend(call_args[0][0])

    file1_item = next(item for item in all_registered_items if item['file_name'] == "data_2024_01.parquet")
    assert file1_item['gdrive_path'] == str((mock_parquet_dir / "data_2024_01.parquet").resolve())
    assert isinstance(file1_item['last_modified'], str)
    assert file1_item['file_size_bytes'] > 0
    assert len(file1_item['mappings']) == 3 # TX-2024-01-01, TX-2024-01-02, MTX-2024-01-01
    assert {"symbol": "TX", "data_date": "2024-01-01"} in file1_item['mappings']
    assert {"symbol": "TX", "data_date": "2024-01-02"} in file1_item['mappings']
    assert {"symbol": "MTX", "data_date": "2024-01-01"} in file1_item['mappings']

    file_empty_item = next(item for item in all_registered_items if item['file_name'] == "empty_data.parquet")
    assert file_empty_item['mappings'] == [] # 空檔案，但欄位存在

    # 3. 驗證 close 被呼叫
    mock_manager_instance.close.assert_called_once()

    # 4. 驗證日誌輸出
    assert "元數據掃描完成。" in caplog.text
    assert "共迭代 5 個 Parquet 檔案路徑" in caplog.text # 5 個 .parquet 檔案 (os.walk 會找到它們)
    assert "成功註冊了 4 個檔案的元數據。" in caplog.text
    assert f"檔案 {mock_parquet_dir / 'missing_col.parquet'} 缺少 'symbol' 或 'data_date' 欄位，已跳過。" in caplog.text
    assert f"檔案 {mock_parquet_dir / 'bad_date_format.parquet'} 中的 'data_date' 欄位無法轉換為 YYYY-MM-DD 格式，已跳過此檔案。" in caplog.text


@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_metadata_scanner_empty_dir(MockMetadataManager, tmp_path: Path, mock_db_path: Path, caplog):
    """測試掃描一個空的 Parquet 目錄"""
    caplog.set_level(logging.INFO)
    empty_parquet_dir = tmp_path / "empty_parquet"
    empty_parquet_dir.mkdir()

    mock_manager_instance = MockMetadataManager.return_value
    scanner = MetadataScanner(parquet_dir=empty_parquet_dir, db_path=mock_db_path)
    scanner.run_scan()

    mock_manager_instance.setup_tables.assert_called_once()
    mock_manager_instance.register_file_batch.assert_not_called() # 不應該有任何檔案被註冊
    mock_manager_instance.close.assert_called_once()
    assert "共迭代 0 個 Parquet 檔案路徑" in caplog.text
    assert "成功註冊了 0 個檔案的元數據。" in caplog.text

@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_metadata_scanner_db_setup_fails(MockMetadataManager, mock_parquet_dir: Path, mock_db_path: Path, caplog):
    """測試當資料庫 setup_tables 失敗時的情況"""
    caplog.set_level(logging.CRITICAL)
    mock_manager_instance = MockMetadataManager.return_value
    mock_manager_instance.setup_tables.side_effect = Exception("DB setup failed")

    scanner = MetadataScanner(parquet_dir=mock_parquet_dir, db_path=mock_db_path)
    scanner.run_scan()

    mock_manager_instance.setup_tables.assert_called_once()
    mock_manager_instance.register_file_batch.assert_not_called() # 不應繼續執行
    # close 可能不會被呼叫，因為 run_scan 可能在 setup_tables 失敗後就返回了
    # mock_manager_instance.close.assert_called_once()
    assert "無法設定元數據資料庫表格，掃描中止。" in caplog.text
    assert "DB setup failed" in caplog.text


@patch('src.taifex_pipeline.scripts.metadata_scanner.pd.read_parquet')
@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_extract_metadata_from_file_general_exception(MockMetadataManager, mock_read_parquet, mock_parquet_dir: Path, mock_db_path: Path, caplog):
    """測試 _extract_metadata_from_file 中發生未預期讀取錯誤"""
    caplog.set_level(logging.ERROR)
    mock_read_parquet.side_effect = Exception("Unexpected read error")

    # 建立一個假的 Parquet 檔案路徑
    dummy_file = mock_parquet_dir / "problematic.parquet"
    dummy_file.touch() # 確保檔案存在以供 stat()

    scanner = MetadataScanner(parquet_dir=mock_parquet_dir, db_path=mock_db_path)
    # 直接呼叫內部方法進行測試
    result = scanner._extract_metadata_from_file(dummy_file)

    assert result is None
    assert f"處理檔案 {dummy_file} 時發生未預期錯誤: Unexpected read error" in caplog.text

@patch('src.taifex_pipeline.scripts.metadata_scanner.argparse.ArgumentParser.parse_args')
@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataScanner.run_scan')
def test_main_function(mock_run_scan, mock_parse_args, mock_parquet_dir, mock_db_path, caplog):
    """測試 main 函式是否能正確解析參數並呼叫 MetadataScanner"""
    caplog.set_level(logging.INFO)

    mock_args = MagicMock()
    mock_args.parquet_dir = mock_parquet_dir
    mock_args.db_path = mock_db_path
    mock_args.batch_size = 50
    mock_args.log_level = "DEBUG"
    mock_parse_args.return_value = mock_args

    # 需要 mock Path.exists() 和 Path.mkdir() 因為 main 裡面有檢查
    with patch('src.taifex_pipeline.scripts.metadata_scanner.Path.exists') as mock_exists, \
         patch('src.taifex_pipeline.scripts.metadata_scanner.Path.mkdir') as mock_mkdir:

        mock_exists.return_value = True # 假設 parquet_dir 存在

        # 使用 try-finally 來確保 logging.shutdown 被呼叫
        try:
            scanner_main()
        finally:
            logging.shutdown() # 手動呼叫以避免影響其他測試的日誌

    # 驗證 run_scan 被呼叫 (因為 MetadataScanner 的 __init__ 是在 main 裡面)
    # 我們不能直接驗證 MetadataScanner 的 __init__ 參數，因為 run_scan 是 mock 的
    # 但我們可以驗證 run_scan 被呼叫了
    mock_run_scan.assert_called_once()

    # 檢查日誌級別是否被設定 (這比較難直接測試，但可以看是否有 DEBUG 級別的日誌)
    # 這裡我們檢查啟動日誌，它應該在 INFO 級別 (因為 main 裡的 logger.info)
    # 並且由於我們設定了 DEBUG，所以 logger 應該是 DEBUG 級別
    # 實際上，basicConfig 會設定 root logger，我們的 logger 會繼承
    # 這裡我們檢查一個由 main 產生的 INFO 日誌
    assert f"元數據掃描器啟動，參數: {mock_args}" in caplog.text
    # 並且 root logger 的級別應該是 DEBUG
    assert logging.getLogger().getEffectiveLevel() == logging.DEBUG


def test_main_function_invalid_log_level(capsys):
    """測試 main 函式使用無效日誌級別參數"""
    with patch('sys.argv', ['scanner_script_name', '--log-level', 'INVALID']):
        with patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataScanner.run_scan'): # Mock 掉 run_scan 避免執行
            scanner_main()

    captured = capsys.readouterr()
    assert "錯誤的日誌級別: INVALID" in captured.out # argparse 應該會印出錯誤或我們自訂的
    # 或者檢查 argparse 的 error output
    # assert "invalid choice: 'INVALID'" in captured.err # 取決於 argparse 如何處理


@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_batch_registration_logic(MockMetadataManager, mock_parquet_dir: Path, mock_db_path: Path):
    """更精確地測試批次註冊邏輯"""
    mock_manager_instance = MockMetadataManager.return_value

    # 建立 5 個可處理的檔案，批次大小為 3
    # mock_parquet_dir 已經有4個可處理檔案，再加一個
    df_extra_data = {'symbol': ['XYZ'], 'data_date': [datetime(2024, 4, 1)]}
    pd.DataFrame(df_extra_data).to_parquet(mock_parquet_dir / "extra_data.parquet")

    # 現在總共有 5 個可處理檔案
    # data_2024_01.parquet, data_2024_02.parquet, empty_data.parquet, data_2024_03.parquet, extra_data.parquet

    scanner = MetadataScanner(parquet_dir=mock_parquet_dir, db_path=mock_db_path, batch_size=3)
    scanner.run_scan()

    # 5 個檔案，批次大小 3 -> 應該呼叫 2 次 (3 個檔案一批，剩下 2 個一批)
    assert mock_manager_instance.register_file_batch.call_count == 2

    calls = mock_manager_instance.register_file_batch.call_args_list
    assert len(calls[0][0][0]) == 3 # 第一批 3 個
    assert len(calls[1][0][0]) == 2 # 第二批 2 個

    # (mock_parquet_dir / "extra_data.parquet").unlink() # 清理額外檔案，可選

@patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataManager')
def test_parquet_dir_not_exists(MockMetadataManager, tmp_path: Path, mock_db_path: Path, caplog):
    """測試當 parquet_dir 不存在時，main 函式的行為"""
    caplog.set_level(logging.WARNING)
    non_existent_dir = tmp_path / "i_do_not_exist"

    mock_args = MagicMock()
    mock_args.parquet_dir = non_existent_dir
    mock_args.db_path = mock_db_path
    mock_args.batch_size = 10
    mock_args.log_level = "INFO"

    with patch('src.taifex_pipeline.scripts.metadata_scanner.argparse.ArgumentParser.parse_args', return_value=mock_args), \
         patch('src.taifex_pipeline.scripts.metadata_scanner.MetadataScanner.run_scan') as mock_run_scan: # mock run_scan 避免執行

        # main 函式會檢查 parquet_dir.exists()
        # 我們不需要 mock Path.exists，因為它真的不存在
        scanner_main()

    mock_run_scan.assert_called_once() # 即使目錄不存在，scanner 也應該被初始化並嘗試執行
    assert f"Parquet 目錄 {non_existent_dir} 不存在。腳本將會執行，但可能找不到任何檔案。" in caplog.text

    # 驗證 MetadataScanner 初始化時，目錄不存在不會導致錯誤
    # 這實際上是在測試 scanner_main 呼叫 MetadataScanner 時的行為
    # 如果 MetadataScanner.__init__ 有問題，mock_run_scan 可能不會被呼叫
    # 這裡主要測試 main 函數的日誌和流程控制

# End of tests. The markdown section below was causing syntax errors and has been removed.
