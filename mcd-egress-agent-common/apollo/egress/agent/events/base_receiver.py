from abc import ABC, abstractmethod
from typing import Callable, Dict


class BaseReceiver(ABC):
    """
    BaseReceiver class, receivers are used to produce events received from the backend.
    """

    @abstractmethod
    def start(
        self,
        handler: Callable[[Dict], None],
        connected_handler: Callable[[], None],
        disconnected_handler: Callable[[], None],
    ):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def restart(self):
        pass
