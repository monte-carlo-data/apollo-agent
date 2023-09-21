from abc import ABC, abstractmethod


class BaseProxyClient(ABC):
    @property
    @abstractmethod
    def wrapped_client(self):
        pass
