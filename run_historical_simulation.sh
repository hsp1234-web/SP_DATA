#!/bin/bash

# run_historical_simulation.sh
# 腳本目的：執行大規模歷史回溯，生成 AI 歷史決策日誌。

echo "--- 開始歷史回溯模擬 ---"

# 設定參數 (未來可以考慮從命令列參數讀取)
# 注意：此腳本假設 project_config.yaml 已手動設定為所需的完整歷史回溯期間。
# 例如，在 src/configs/project_config.yaml 中設定:
# data_fetch_range:
#   start_date: "2020-01-01"
#   end_date: "2023-12-31"

# 檢查必要的環境變數 (例如 AI 服務的 API 金鑰) 是否已設定
# 這裡假設使用者已在環境中設定了 project_config.yaml 中 ai_service.api_key_env 指定的變數
AI_KEY_ENV_NAME=$(grep -oP 'api_key_env:\s*"?\K[^"\s]+' src/configs/project_config.yaml | head -n 1)

if [ -z "$AI_KEY_ENV_NAME" ]; then
  echo "警告：無法從 src/configs/project_config.yaml 中讀取 AI API 金鑰的環境變數名稱 (ai_service.api_key_env)。"
  # 你可能還是想繼續，如果 AI 服務不是嚴格必須的，或者 main.py 會處理這個情況
elif [ -z "${!AI_KEY_ENV_NAME}" ]; then
  echo "錯誤：AI API 金鑰環境變數 '$AI_KEY_ENV_NAME' 未設定。"
  echo "請先設定此環境變數，然後再執行腳本。"
  echo "例如：export $AI_KEY_ENV_NAME=\"your_api_key_here\""
  exit 1
else
  echo "AI API 金鑰環境變數 '$AI_KEY_ENV_NAME' 已偵測到。"
fi

# 執行主應用程式
echo "執行 src/main.py 進行歷史數據處理與 AI 決策生成..."
# 假設 Python 環境和依賴已經準備好 (例如，已執行過 pip install -r requirements.txt)
python src/main.py

# 執行結束後的日誌記錄
# main.py 內部會記錄已處理的日期和 AI API 調用次數。
# 此腳本可以簡單地指示使用者查看日誌。
echo ""
echo "--- 歷史回溯模擬執行完畢 ---"
echo "詳細執行日誌請查看 market_briefing_log.txt (總體日誌) 以及 api_test_logs/ 目錄下的時間戳記日誌 (應用程式日誌)。"
echo "AI API 調用次數統計應記錄在應用程式日誌中。"

# 未來可擴展功能：
# 1. 從命令列接收開始和結束日期，並動態修改 project_config.yaml (較複雜，需注意備份和還原)。
# 2. 更精細的進度追蹤和錯誤處理。
# 3. 執行後自動從日誌中提取 AI API 調用總數並顯示。
# 4. 檢查 Python 和 pip 是否存在。
# 5. 檢查 requirements.txt 是否已安裝。

exit 0
