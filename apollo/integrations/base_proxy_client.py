from abc import ABC, abstractmethod
from typing import Optional


class BaseProxyClient(ABC):
    @property
    @abstractmethod
    def wrapped_client(self):
        pass

    def get_error_type(self, error: Exception) -> Optional[str]:
        return None
