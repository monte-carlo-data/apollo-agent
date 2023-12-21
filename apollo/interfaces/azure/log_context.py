import json
from typing import Dict

from apollo.interfaces.generic.log_context import BaseLogContext


class AzureLogContext(BaseLogContext):
    def set_agent_context(self, context: Dict):
        self._context = self.filter_log_context(context)

    @staticmethod
    def filter_log_context(context: Dict) -> Dict:
        # open telemetry supports only: None, str, bytes, float, int and bool
        return {
            key: value
            if value is None or type(value) in (str, bytes, float, int, bool)
            else json.dumps(value)
            for key, value in context.items()
        }
