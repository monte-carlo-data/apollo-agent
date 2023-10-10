from abc import ABC, abstractmethod
from typing import Optional, Any


class BaseProxyClient(ABC):
    @property
    @abstractmethod
    def wrapped_client(self):
        pass

    def get_error_type(self, error: Exception) -> Optional[str]:
        return None

    def process_result(self, value: Any) -> Any:
        return value
