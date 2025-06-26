#!/bin/bash
# run_historical_simulation.sh - 批量執行歷史回溯模擬

# --- Configuration ---
START_DATE_ARG=$1
END_DATE_ARG=$2
HISTORICAL_JOB_SCRIPT="./run_historical_job.sh"
ERROR_LOG_FILE="historical_simulation_errors.log"
PROGRESS_LOG_FILE="historical_simulation_progress.log"

# --- Helper Functions ---
log_progress() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$PROGRESS_LOG_FILE"
}

log_error_to_file() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR on $1: $2" >> "$ERROR_LOG_FILE"
}

# --- Argument Validation ---
if [ -z "$START_DATE_ARG" ] || [ -z "$END_DATE_ARG" ]; then
    log_progress "錯誤：必須提供開始日期和結束日期。"
    log_progress "用法: bash run_historical_simulation.sh YYYY-MM-DD YYYY-MM-DD"
    exit 1
fi

if ! [[ "$START_DATE_ARG" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || \
   ! [[ "$END_DATE_ARG" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    log_progress "錯誤：日期格式必須是 YYYY-MM-DD。"
    exit 1
fi

if [[ "$(uname)" == "Darwin" ]]; then
    current_date_sec=$(gdate -d "$START_DATE_ARG" +%s 2>/dev/null)
    end_date_sec=$(gdate -d "$END_DATE_ARG" +%s 2>/dev/null)
    if [ -z "$current_date_sec" ] || [ -z "$end_date_sec" ]; then
        log_progress "錯誤：無法在 macOS 上解析日期。請確保已安裝 GNU date (gdate)。"
        log_progress "嘗試: brew install coreutils"
        exit 1
    fi
else
    current_date_sec=$(date -d "$START_DATE_ARG" +%s 2>/dev/null)
    end_date_sec=$(date -d "$END_DATE_ARG" +%s 2>/dev/null)
     if [ -z "$current_date_sec" ] || [ -z "$end_date_sec" ]; then
        log_progress "錯誤：無法在 Linux 上解析日期。請檢查日期格式和 'date' 命令。"
        exit 1
    fi
fi

if [ "$current_date_sec" -gt "$end_date_sec" ]; then
    log_progress "錯誤：開始日期 ($START_DATE_ARG) 不能晚於結束日期 ($END_DATE_ARG)。"
    exit 1
fi

# --- Main Simulation Loop ---
log_progress "=== 開始歷史回溯模擬 ==="
log_progress "時間區間: $START_DATE_ARG 到 $END_DATE_ARG"
log_progress "錯誤將記錄在: $ERROR_LOG_FILE"
log_progress "進度將記錄在: $PROGRESS_LOG_FILE"
# Initialize/Clear logs for this run
echo "Historical Simulation Run: $START_DATE_ARG to $END_DATE_ARG" > "$PROGRESS_LOG_FILE"
echo "Historical Simulation Errors:" > "$ERROR_LOG_FILE"

total_days=$(( (end_date_sec - current_date_sec) / 86400 + 1 ))
processed_days=0
successful_days=0
failed_days=0

while [ "$current_date_sec" -le "$end_date_sec" ]; do
    processed_days=$((processed_days + 1))
    if [[ "$(uname)" == "Darwin" ]]; then
        current_date_str=$(gdate -d "@$current_date_sec" +%Y-%m-%d)
    else
        current_date_str=$(date -d "@$current_date_sec" +%Y-%m-%d)
    fi

    log_progress "--- [Day $processed_days/$total_days] Processing $current_date_str ---"
    log_progress "DEBUG: HISTORICAL_JOB_SCRIPT is '$HISTORICAL_JOB_SCRIPT', current_date_str is '$current_date_str'" # More detailed debug

    bash "$HISTORICAL_JOB_SCRIPT" "$current_date_str"
    job_exit_code=$?

    if [ $job_exit_code -ne 0 ]; then
        log_progress "錯誤：日期 $current_date_str 的作業執行失敗，退出碼: $job_exit_code"
        log_error_to_file "$current_date_str" "作業執行失敗，退出碼: $job_exit_code"
        failed_days=$((failed_days + 1))
    else
        log_progress "日期 $current_date_str 的作業成功完成。"
        successful_days=$((successful_days + 1))
    fi

    current_date_sec=$((current_date_sec + 86400))
done

log_progress "=== 歷史回溯模擬完成 ==="
log_progress "總天數: $total_days"
log_progress "成功天數: $successful_days"
log_progress "失敗天數: $failed_days"
log_progress "詳細錯誤請查看: $ERROR_LOG_FILE"
log_progress "詳細進度請查看: $PROGRESS_LOG_FILE"

exit $failed_days # Exit with the number of failed days, 0 if all successful
