# 次世代金融數據平台 (簡化版)

本專案是一個數據管道，旨在接收、處理並管理各種類型的數據，包括金融數據、文件、圖片及多媒體檔案。最終目標是建立一個高品質、整合的資料庫，適用於機器學習分析、策略回測及視覺化報告。

此版本專注於一個簡化的三階段架構，使用 DuckDB 進行本地數據管理。

## 專案結構

```
your_project_name/
├── data_pipeline.py       # 主要處理邏輯
├── config.py              # 設定檔 (路徑、日誌等)
├── requirements.txt       # Python 依賴套件
├── README.md              # 本檔案 (說明文件)
├── .gitignore             # Git 忽略設定
|
├── Data test/             # 輸入的測試檔案 (圖片、CSV等)
│   ├── image1.jpg
│   └── document1.csv
|
├── database/              # DuckDB 資料庫檔案
│   ├── manifest.db
│   ├── raw_lake.db
│   └── curated_mart.db
|
└── logs/                  # 執行的日誌檔案
    └── pipeline_run_YYYYMMDD_HHMMSS.log
```

## 核心架構：三資料庫模型

1.  **`manifest.db` (大腦/目錄)**：記錄所有傳入檔案的元數據（雜湊值、路徑、狀態、基礎元數據）。作為檔案處理狀態的單一事實來源。
2.  **`raw_lake.db` (原始儲藏室)**：儲存所有傳入檔案的原始、未經修改的內容（例如，以 BLOB 形式）。原則是可靠地「接收數據」。
3.  **`curated_mart.db` (加工品/展示廳)**：儲存經過清理、轉換和豐富化後，可直接用於分析的數據。

## 管線階段

`data_pipeline.py` 腳本會執行以下階段：

1.  **階段一：接收與註冊 (Ingest and Register)**：
    *   掃描輸入目錄 (`Data test/`)。
    *   對於每個新檔案：
        *   計算其 SHA256 雜湊值。
        *   將原始檔案內容存入 `raw_lake.db`。
        *   提取基礎元數據（檔案名稱、MIME 類型、檔案系統日期）。
        *   在 `manifest.db` 中註冊檔案及其元數據，狀態為 `'raw_stored'`。

2.  **階段二：派生日期 (Derive Date)**：
    *   掃描 `manifest.db` 中狀態為 `'raw_stored'` 的檔案。
    *   嘗試從其元數據（例如檔案修改日期、EXIF 日期）中為每個檔案派生一個主要日期 (`derived_date`)。
    *   更新 `manifest.db` 中的 `derived_date`。

3.  **階段三：數據整理 (Curate Data)**：
    *   掃描 `manifest.db` 中準備進行整理的檔案（例如，狀態為 `'raw_stored'` 或 `'date_derived'`）。
    *   根據檔案的 `raw_content_type`，選擇一個特定的「處理器」：
        *   **CSV/Excel 處理器**：（參考 v8.0 邏輯）嘗試使用 Pandas 讀取，執行基礎的清理/轉換，並載入到 `curated_mart.db` 中的一個表格。
        *   **圖片處理器**：提取基礎的圖片特徵（例如尺寸）並儲存到 `curated_mart.db`。
        *   可為不同檔案類型添加其他處理器。
    *   **錯誤處理**：如果某檔案無法被任何已知處理器處理，其狀態將更新為 `'unsupported_type'`。如果處理器遇到錯誤，狀態將更新為 `'curation_error'` 並附帶錯誤訊息。管線不會因個別檔案錯誤而崩潰。
    *   成功處理的檔案標記為 `'curated'`。

## 設定步驟

1.  **複製儲存庫 (若適用)。**
2.  **建立 Python 虛擬環境 (建議)：**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Windows 環境: venv\Scripts\activate
    ```
3.  **安裝依賴套件：**
    ```bash
    pip install -r requirements.txt
    ```
    *注意：`python-magic` 可能有系統層級的依賴。請參考其文件以在您的作業系統上安裝（例如，Linux 上的 `libmagic`，或在 macOS 上透過 Homebrew 安裝）。*
4.  **準備 `Data test/` 資料夾：** 將一些範例檔案（CSV、JPEG 等）放入 `Data test/` 目錄。
5.  **檢閱 `config.py`：** 確保路徑和設定適合您的環境（預設值應適用於標準設定）。

## 執行管線

從專案根目錄執行主要腳本：

```bash
python data_pipeline.py
```

日誌將產生在 `logs/` 目錄中，同時也會打印到控制台。
資料庫檔案將在 `database/` 目錄中建立/更新（如果在 `config.py` 中的 `DATABASE_DIR` 被更改，則可能在專案根目錄）。

## 開發注意事項

*   **冪等性 (Idempotency)**：此管線設計目標是冪等的。如果檔案未更改（基於雜湊值檢查），重新執行不應導致數據重複。
*   **擴展性 (Extensibility)**：可以在階段三中通過創建新函數並將它們映射到 `CONTENT_PROCESSORS` 字典（待實現）來添加新的檔案類型處理器。
*   **錯誤彈性 (Error Resilience)**：管線設計為跳過有問題的檔案並記錄錯誤，而不是崩潰。檢查 `manifest.db` 的狀態和日誌以識別和解決問題。
```
