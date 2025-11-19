import json
from threading import local
from typing import Dict, cast, Any

from apollo.agent.redact import AgentRedactUtilities
from apollo.interfaces.generic.log_context import BaseLogContext

_context = local()


class AzureLogContext(BaseLogContext):
    """
    LogContext implementation for Azure, as Azure internally uses OpenTelemetry we need to:
    - filter "complex" objects from structured logs, only base data types (str, float, int, bool) are supported, for
      the other types we serialize list, dict and tuple to json and convert to string the rest.
    - set "extra" attributes individually in the LogRecord instead of setting an "extra" dict attribute (like AWS) or
      a "json_fields" dict attribute (like GCP), here we set the "extra" attributes as individual attributes making
      sure the attribute name is prefixed with "mcd_" for easier detection.
    """

    def set_agent_context(self, context: Dict):
        _context.value = self.filter_log_context(context)

    @staticmethod
    def filter_log_context(context: Dict) -> Dict:
        # open telemetry supports only: str, float, int and bool
        # we're converting list and dictionaries to json and anything else to str.
        filtered_context = {
            key: (
                value
                if isinstance(value, (str, float, int, bool))
                else (
                    json.dumps(value)
                    if isinstance(value, (list, dict, tuple))
                    else str(value)
                )
            )
            for key, value in context.items()
            if value is not None
        }
        return AgentRedactUtilities.standard_redact(filtered_context)

    def _filter(self, record: Any) -> Any:
        """
        Updates the log record with the agent context, OpenTelemetry doesn't support an "extra" attribute, we
        set the attributes as individual attributes in `record` making sure they are prefixed with "mcd_".
        """
        try:
            context = _context.value
        except AttributeError:
            context = None
        if not context:
            return record

        for key, value in cast(Dict[str, Any], context).items():
            setattr(record, key if key.startswith("mcd_") else f"mcd_{key}", value)
        return record
