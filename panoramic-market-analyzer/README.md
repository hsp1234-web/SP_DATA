# 全景市場分析儀 (Panoramic Market Analyzer)

本專案旨在建立一個基於微服務架構的、穩健的金融數據處理管道。

## 架構核心思想

我們遵循「職責分離」原則，將系統解構成一系列獨立的命令列工具：

1.  **Fetcher Service**: 專職從外部 API 獲取原始數據。
2.  **Processor Service**: 專職處理數據並生成特徵。
3.  **Orchestrator (`run_pipeline.sh`)**: 作為總指揮，協調上述服務的執行流程。

這種架構確保了系統的高度可測試性與錯誤隔離能力。

## 安裝

```bash
pip install -r requirements.txt
```

## 如何執行

### 執行完整數據管道

```bash
bash run_pipeline.sh
```

### 執行所有測試

```bash
bash run_tests.sh
```
