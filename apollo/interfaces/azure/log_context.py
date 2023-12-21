from copy import deepcopy
from types import NoneType
from typing import Dict

from apollo.interfaces.generic.log_context import BaseLogContext


class AzureLogContext(BaseLogContext):
    def set_agent_context(self, context: Dict):
        self._context = self._filter_context(context)

    @staticmethod
    def _filter_context(context: Dict) -> Dict:
        return {
            key: value
            for key, value in context.items()
            if value is None or type(value) in (str, bytes, float, int, bool)
        }
