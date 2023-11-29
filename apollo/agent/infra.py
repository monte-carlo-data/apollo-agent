from abc import ABC, abstractmethod
from typing import Dict


class AgentInfraProvider(ABC):
    @abstractmethod
    def get_infra_details(self) -> Dict:
        pass
