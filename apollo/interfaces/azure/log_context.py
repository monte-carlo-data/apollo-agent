import json
from typing import Dict, cast, Any

from apollo.interfaces.generic.log_context import BaseLogContext


class AzureLogContext(BaseLogContext):
    def set_agent_context(self, context: Dict):
        self._context = self.filter_log_context(context)

    @staticmethod
    def filter_log_context(context: Dict) -> Dict:
        # open telemetry supports only: None, str, bytes, float, int and bool
        return {
            key: value
            if value is None or isinstance(value, (str, float, int, bool))
            else json.dumps(value)
            for key, value in context.items()
        }

    def _filter(self, record: Any) -> Any:
        """
        Updates the log record with the agent context, for OpenTelemetry we don't set an "extra" attribute, we
        just set the attributes as individual attributes in record, we just make sure they are prefixed with mcd_
        """
        if not self._context:
            return record

        for key, value in cast(Dict[str, Any], self._context).items():
            setattr(record, key if key.startswith("mcd_") else f"mcd_{key}", value)
        return record
