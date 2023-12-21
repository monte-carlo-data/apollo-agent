import logging
from typing import Tuple, Dict, Optional

from apollo.agent.utils import AgentUtils
from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.generic import main

_DEFAULT_LOGS_LIMIT = 1000

logger = logging.getLogger(__name__)

app = main.app
agent = main.agent
execute_agent_operation = main.execute_agent_operation


# Azure is not including complex objects like lists in logs, as we want commands to be logged we're converting it
# to str here
def azure_filter_extra(extra: Optional[Dict]) -> Optional[Dict]:
    if extra and "commands" in extra:
        return {**extra, "commands": str(extra["commands"])}
    return extra


main.logging_utils.extra_filterer = azure_filter_extra


@app.route("/api/v1/azure/logs/query", methods=["GET", "POST"])
def gcp_logs_list() -> Tuple[Dict, int]:
    """
    Uses Azure Monitor Query client library to return a list of log events.
    https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-query-readme?view=azure-python
    Supported parameters (all optional):
    - trace_id
    - start_time (iso format), defaults to now - 10 minutes
    - end_time (iso format), defaults to now
    - query: a KQL query expression, see https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/.
    - limit: number of log events to return, defaults to 1,000
    :return: a dictionary with an "events" attribute containing the events returned by Azure, containing
        "message", "customDimensions" and "timestamp" attributes.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    end_time_str: Optional[str] = request_dict.get("end_time")
    limit_str = request_dict.get("limit")
    query: Optional[str] = request_dict.get("query")

    logger.info(
        "azure/logs/list requested",
        extra=main.logging_utils.build_extra(
            trace_id=trace_id,
            operation_name="azure/logs/list",
            extra=dict(
                query=query,
                start_time_str=start_time_str,
                end_time_str=end_time_str,
                limit=limit_str,
                mcd_trace_id=trace_id,
            ),
        ),
    )
    try:
        events = AzurePlatformProvider.get_logs(
            query=query,
            start_time_str=start_time_str,
            end_time_str=end_time_str,
            limit=int(limit_str) if limit_str else _DEFAULT_LOGS_LIMIT,
        )
        response = AgentUtils.agent_ok_response(
            {
                "events": events,
            },
            trace_id=trace_id,
        )
    except Exception:
        response = AgentUtils.agent_response_for_last_exception(trace_id=trace_id)

    return response.result, response.status_code
