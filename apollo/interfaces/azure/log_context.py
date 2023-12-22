import json
from typing import Dict, cast, Any

from apollo.interfaces.generic.log_context import BaseLogContext


class AzureLogContext(BaseLogContext):
    def set_agent_context(self, context: Dict):
        self._context = self.filter_log_context(context)

    @staticmethod
    def filter_log_context(context: Dict) -> Dict:
        # open telemetry supports only: None, str, float, int and bool
        # we're converting list and dictionaries to json and anything else to str.
        return {
            key: value
            if isinstance(value, (str, float, int, bool))
            else json.dumps(value)
            if isinstance(value, (list, dict, tuple))
            else str(value)
            for key, value in context.items()
            if value is not None
        }

    def _filter(self, record: Any) -> Any:
        """
        Updates the log record with the agent context, OpenTelemetry doesn't support an "extra" attribute, we
        set the attributes as individual attributes in `record` making sure they are prefixed with "mcd_".
        """
        if not self._context:
            return record

        for key, value in cast(Dict[str, Any], self._context).items():
            setattr(record, key if key.startswith("mcd_") else f"mcd_{key}", value)
        return record
