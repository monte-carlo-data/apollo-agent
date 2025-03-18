import logging
import os
from typing import Tuple, Dict, Optional, cast, Callable, Union

from flask import request

from apollo.agent.constants import PLATFORM_AWS, PLATFORM_AWS_GENERIC
from apollo.agent.utils import AgentUtils
from apollo.interfaces.agent_response import AgentResponse
from apollo.interfaces.generic import main
from apollo.interfaces.aws.platform import (
    AwsPlatformProvider,
    AwsGenericPlatformProvider,
)
from apollo.interfaces.lambda_function.json_log_formatter import (
    JsonLogFormatter,
    ExtraLogger,
)
from apollo.agent.env_vars import (
    DEBUG_ENV_VAR,
)
from apollo.interfaces.generic.log_context import BaseLogContext

# set the logger class before any other apollo code
logging.setLoggerClass(ExtraLogger)

log_context = BaseLogContext()
log_context.install()

formatter = JsonLogFormatter()
root_logger = logging.getLogger()
for h in root_logger.handlers:
    h.setFormatter(formatter)

is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
root_logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

_DEFAULT_LOGS_LIMIT = 1000

app = main.app
agent = main.agent
main.agent.log_context = log_context
# default is AWS generic but it is overridden by the lambda handler when platform is 'AWS'
agent.platform_provider = AwsGenericPlatformProvider()

logger = logging.getLogger(__name__)


def _check_aws_platform(
    trace_id: Optional[str],
) -> Tuple[
    Optional[Union[AwsPlatformProvider, AwsGenericPlatformProvider]],
    Optional[AgentResponse],
]:
    if agent.platform == PLATFORM_AWS_GENERIC:
        return cast(AwsGenericPlatformProvider, agent.platform_provider), None
    elif agent.platform == PLATFORM_AWS:
        return cast(AwsPlatformProvider, agent.platform_provider), None
    else:
        return None, AgentUtils.agent_response_for_error(
            "Only supported for AWS platform", trace_id=trace_id
        )


def _perform_aws_operation(
    operation: str, method: Callable, trace_id: Optional[str], params: Dict
) -> AgentResponse:
    logger.info(
        f"{operation} requested",
        extra={
            **params,
            "mcd_trace_id": trace_id,
        },
    )
    aws, error_response = _check_aws_platform(trace_id)
    if error_response:
        return error_response

    try:
        result = method(aws, **params)
        response = AgentUtils.agent_ok_response(result, trace_id=trace_id)
    except Exception:
        response = AgentUtils.agent_response_for_last_exception(trace_id=trace_id)

    return response


# AWS specific endpoints


@app.route("/api/v1/aws/logs/filter", methods=["GET", "POST"])
def aws_logs_filter() -> Tuple[Dict, int]:
    """
    Uses CloudWatchLogs.filter_log_events API to return a list of log events in the specified time window.
    Documentation: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/filter_log_events.html
    Supported parameters (all optional):
    - trace_id
    - start_time (iso format), defaults to now - 10 minutes
    - end_time (iso format), defaults to now
    - pattern: optional filter pattern, for syntax see:
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html
    - limit: number of log events to return, defaults to 1,000
    :return: a dictionary with an "events" attribute containing the events returned by CloudWatch, containing
        for example "message" and "timestamp" attributes.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    end_time_str: Optional[str] = request_dict.get("end_time")
    pattern: Optional[str] = request_dict.get("pattern")
    limit_str = request_dict.get("limit")

    response = _perform_aws_operation(
        "/aws/logs/filter",
        method=AwsPlatformProvider.filter_log_events,
        trace_id=trace_id,
        params=dict(
            pattern=pattern,
            start_time_str=start_time_str,
            end_time_str=end_time_str,
            limit=int(limit_str) if limit_str else _DEFAULT_LOGS_LIMIT,
        ),
    )
    return response.result, response.status_code


@app.route("/api/v1/aws/logs/start_query", methods=["GET", "POST"])
def aws_logs_start_query() -> Tuple[Dict, int]:
    """
    Starts a new CloudWatch query using "CloudWatchLogs.Client.start_query".
    Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/start_query.html
    Required parameters:
    - query: required query, syntax:
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html
    Optional parameters:
    - trace_id
    - start_time (iso format), defaults to now - 10 minutes
    - end_time (iso format), defaults to now
    - limit: number of log events to return, defaults to 1,000
    :return: a dictionary with a "query_id" attribute that can be used to call aws/logs/query_results.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    end_time_str: Optional[str] = request_dict.get("end_time")
    query: Optional[str] = request_dict.get("query")
    limit_str = request_dict.get("limit")
    if not query:
        raise ValueError("query is a required parameter")

    response = _perform_aws_operation(
        "/aws/logs/start_query",
        method=AwsPlatformProvider.start_logs_query,
        trace_id=trace_id,
        params=dict(
            query=query,
            start_time_str=start_time_str,
            end_time_str=end_time_str,
            limit=int(limit_str) if limit_str else _DEFAULT_LOGS_LIMIT,
        ),
    )
    return response.result, response.status_code


@app.route("/api/v1/aws/logs/stop_query", methods=["GET", "POST"])
def aws_logs_stop_query() -> Tuple[Dict, int]:
    """
    Stops the query with the given ID, previously triggered using /aws/logs/start_query.
    Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/stop_query.html
    Required parameters:
    - query_id: the query ID returned by start_query.
    Optional parameters:
    - trace_id
    :return: a dictionary with a single boolean attribute: "success" as returned by `CloudWatchLogs.Client.stop_query`.
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    query_id: Optional[str] = request_dict.get("query_id")
    if not query_id:
        raise ValueError("query_id is a required parameter")

    response = _perform_aws_operation(
        "/aws/logs/stop_query",
        method=AwsPlatformProvider.stop_logs_query,
        trace_id=trace_id,
        params=dict(
            query_id=query_id,
        ),
    )
    return response.result, response.status_code


@app.route("/api/v1/aws/logs/query_results", methods=["GET", "POST"])
def aws_logs_get_query_results() -> Tuple[Dict, int]:
    """
    Returns the result of a query triggered using /aws/logs/start_query.
    Docs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/logs/client/get_query_results.html
    Required parameters:
    - query_id: the query ID returned by start_query.
    Optional parameters:
    - trace_id
    :return: a dictionary with "events" (the "results" attribute from the CW get_query_results response) and "status".
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    query_id: Optional[str] = request_dict.get("query_id")
    if not query_id:
        raise ValueError("query_id is a required parameter")

    response = _perform_aws_operation(
        "/aws/logs/get_query_results",
        method=AwsPlatformProvider.get_logs_query_results,
        trace_id=trace_id,
        params=dict(
            query_id=query_id,
        ),
    )
    return response.result, response.status_code
