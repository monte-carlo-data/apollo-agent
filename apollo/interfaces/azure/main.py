import json
import logging
import traceback
from typing import Tuple, Dict, Optional

from flask import request
from werkzeug.exceptions import InternalServerError

from apollo.agent.utils import AgentUtils
from apollo.interfaces.azure.azure_platform import AzurePlatformProvider
from apollo.interfaces.azure.log_context import AzureLogContext
from apollo.interfaces.generic import main

_DEFAULT_LOGS_LIMIT = 1000

logger = logging.getLogger(__name__)

app = main.app
agent = main.agent
execute_agent_operation = main.execute_agent_operation

main.swagger_security_settings = {
    "securityDefinitions": {
        "Azure App Key": {
            "type": "apiKey",
            "name": "x-functions-key",
            "in": "header",
            "description": "Enter the Azure Function App Key.",
        }
    },
    "security": [{"Azure App Key": []}],
}


# Azure is not including complex objects like lists in logs, as we want for example commands
# to be logged we're converting it to a json string here.
def azure_filter_extra(extra: Optional[Dict]) -> Optional[Dict]:
    return AzureLogContext.filter_log_context(extra) if extra else None


main.logging_utils.extra_filterer = azure_filter_extra


@app.route("/api/v1/azure/logs/query", methods=["GET"])
def azure_logs_query_get() -> Tuple[Dict, int]:
    """
    Returns a list of Azure log events.
    Uses Azure Monitor Query client library to return a list of log events.
    See: https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-query-readme?view=azure-python
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
        - in: query
          name: start_time
          description: The start time for the log events, a datetime in ISO format. Defaults to 10 minutes ago.
          schema:
              type: string
              example: "2023-12-25T12:31:45+00:00"
        - in: query
          name: end_time
          description: The start time for the log events, a datetime in ISO format. Defaults to now.
          schema:
              type: string
              example: "2023-12-26T13:00:00+00:00"
        - in: query
          name: limit
          type: integer
          description: Maximum number of events to return.
          default: 1000
        - in: query
          name: query
          type: string
          description: A KQL query expression, see https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/.
            If it starts with "traces" or "requests" it is expected to be a "full" query and will be sent "as is".
            Otherwise, if not empty it will be assumed to be only the filtering portion of a query
            (like "where message like pattern") and it will be added to the standard query that get traces
            in descending order by timestamp.
    definitions:
        - schema:
            id: AzureLogsResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        events:
                            type: array
                            items:
                                type: object
                                properties:
                                    timestamp:
                                        type: string
                                    customDimensions:
                                        type: object
                                    message:
                                        type: string
                    example:
                        events:
                            - timestamp: "2023-12-28T14:13:48.445000+00:00"
                              message: "Executing operation: snowflake/query"
                              customDimensions:
                                mcd_operation_name: snowflake/query
                                commands:
                                    - method: cursor
                                      store: _cursor
                                    - target: _cursor
                                      method: execute
                                      args: [
                                        "SELECT DATABASE_NAME FROM SNOWFLAKE.INFORMATION_SCHEMA.DATABASES"
                                      ]
                            - timestamp: "2023-12-28T14:13:47.445000+00:00"
                              message: Log message
                              customDimensions: null
    responses:
        200:
            description: Returns a list of Azure log events in the specified time period.
            schema:
                $ref: "#/definitions/AzureLogsResponse"

    :return: a dictionary with an "events" attribute containing the events returned by Azure, containing
        "message", "customDimensions" and "timestamp" attributes.
    """
    return _azure_logs_query()


@app.route("/api/v1/azure/logs/query", methods=["POST"])
def azure_logs_query_post() -> Tuple[Dict, int]:
    """
    Returns a list of Azure log events.
    Uses Azure Monitor Query client library to return a list of log events.
    See: https://learn.microsoft.com/en-us/python/api/overview/azure/monitor-query-readme?view=azure-python
    ---
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            properties:
                trace_id:
                  description: An optional trace_id
                  type: string
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                start_time:
                  description: The start time for the log events, a datetime in ISO format. Defaults to 10 minutes ago.
                  type: string
                  example: "2023-12-25T12:31:45+00:00"
                end_time:
                  description: The start time for the log events, a datetime in ISO format. Defaults to now.
                  type: string
                  example: "2023-12-26T13:00:00+00:00"
                limit:
                  type: integer
                  description: Maximum number of events to return.
                  default: 1000
                query:
                  type: string
                  description: A KQL query expression, see https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/.
                    If it starts with "traces" or "requests" it is expected to be a "full" query and will be sent "as is".
                    Otherwise, if not empty it will be assumed to be only the filtering portion of a query
                    (like "where message like pattern") and it will be added to the standard query that get traces
                    in descending order by timestamp.
    responses:
        200:
            description: Returns a list of Azure log events in the specified time period.
            schema:
                $ref: "#/definitions/AzureLogsResponse"

    :return: a dictionary with an "events" attribute containing the events returned by Azure, containing
        "message", "customDimensions" and "timestamp" attributes.
    """
    return _azure_logs_query()


def _azure_logs_query() -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    end_time_str: Optional[str] = request_dict.get("end_time")
    limit_str = request_dict.get("limit")
    query: Optional[str] = request_dict.get("query")

    logger.info(
        "azure/logs/query requested",
        extra=main.logging_utils.build_extra(
            trace_id=trace_id,
            operation_name="azure/logs/query",
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
    except Exception as exc:
        logger.error(f"Failed to get azure logs: {exc}")
        response = AgentUtils.agent_response_for_last_exception(trace_id=trace_id)

    return response.result, response.status_code


@app.errorhandler(InternalServerError)
def handle_internal_server_error(e: InternalServerError):
    """
    Flask error handler to log unhandled exceptions, without this code there was no log at all for unhandled
    exceptions and no helpful information in the response.
    This is also returning an "agent like" response (with __mcd_error__) and changing the status code to 200, if we
    return 500 the error response is ignored and an empty body returned.
    """
    error = e.original_exception if e.original_exception else e
    stack_trace = traceback.format_tb(error.__traceback__)  # type: ignore
    logger.error(
        f"Internal Server Error: {error}",
        extra={
            "stack_trace": json.dumps(
                stack_trace
            ),  # so it's easier to explore in Application Insights
        },
    )
    agent_response = AgentUtils.agent_response_for_error(
        f"Internal Server Error: {error}", stack_trace=stack_trace, status_code=200
    )
    return agent_response.result, agent_response.status_code
