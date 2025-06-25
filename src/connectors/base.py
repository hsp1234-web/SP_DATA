from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Tuple, Optional

class BaseConnector(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.source_api_name = "Unknown"

    @abstractmethod
    def fetch_data(self, **kwargs) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        pass
