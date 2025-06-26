# src/connectors/__init__.py
import logging # Added import for logger

# 使 Connector 更容易被導入
# 例如: from src.connectors import FinMindConnector

# 根據已建立的 Connector 檔案，取消註解並加入
from .alpha_vantage_connector import AlphaVantageConnector
# from .base_connector import BaseConnector # 舊的 BaseConnector 結構已移除或不直接使用
from .finlab_connector import FinLabConnector # 目前是佔位符
from .finmind_connector import FinMindConnector
from .finnhub_connector import FinnhubConnector
from .fmp_connector import FMPConnector
from .fred_connector import FredConnector
from .nyfed_connector import NYFedConnector
from .polygon_io_connector import PolygonIOConnector
from .yfinance_connector import YFinanceConnector

# 可以定義一個 __all__ 列表來控制 `from src.connectors import *` 的行為
__all__ = [
    "AlphaVantageConnector",
    "FinLabConnector", # 即使是佔位符，也先加入
    "FinMindConnector",
    "FinnhubConnector",
    "FMPConnector",
    "FredConnector",
    "NYFedConnector",
    "PolygonIOConnector",
    "YFinanceConnector",
]

# 可以在這裡添加一些通用的 Connector 輔助函數或常數 (如果需要)
# 例如，一個函數用來根據名稱動態載入 Connector:
SUPPORTED_CONNECTORS_MAP = {
    "alphavantage": AlphaVantageConnector,
    "finlab": FinLabConnector,
    "finmind": FinMindConnector,
    "finnhub": FinnhubConnector,
    "fmp": FMPConnector,
    "fred": FredConnector,
    "nyfed": NYFedConnector,
    "polygon_io": PolygonIOConnector, # 注意 python 模組名是 polygon_io_connector
    "yfinance": YFinanceConnector,
}

def get_connector_class(connector_name: str):
    """
    根據 connector_name (小寫，例如 'finmind') 返回對應的 Connector 類別。
    """
    connector_name_lower = connector_name.lower().replace('-', '_') # 處理 polygon-io 這種情況
    if connector_name_lower in SUPPORTED_CONNECTORS_MAP:
        return SUPPORTED_CONNECTORS_MAP[connector_name_lower]
    else:
        logger.error(f"Unsupported connector name: {connector_name}. Available: {list(SUPPORTED_CONNECTORS_MAP.keys())}")
        raise ValueError(f"Unsupported connector: {connector_name}")

logger = logging.getLogger(__name__) # Initialize logger for this module
logger.info(f"Connectors package initialized. Available (imported for __all__): {', '.join(__all__)}")
logger.info(f"Supported connectors map contains: {list(SUPPORTED_CONNECTORS_MAP.keys())}")
