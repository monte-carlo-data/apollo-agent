import io
import logging
import os
from typing import Dict, Tuple, Callable, Optional, Union, Any, BinaryIO

from flask import Flask, request, Response, send_file

from apollo.agent.agent import Agent
from apollo.agent.constants import TRACE_ID_HEADER
from apollo.agent.env_vars import DEBUG_ENV_VAR
from apollo.agent.logging_utils import LoggingUtils
from apollo.interfaces.agent_response import AgentResponse

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging_utils = LoggingUtils()
agent = Agent(logging_utils)


def _get_response_headers(response: AgentResponse) -> Dict:
    headers = {}
    if response.trace_id:
        headers[TRACE_ID_HEADER] = response.trace_id
    result = response.result
    if isinstance(result, bytes) or isinstance(result, io.IOBase):
        headers["Content-Type"] = "application/octet-stream"
    elif isinstance(result, str):
        headers["Content-Type"] = "text/plain"
    return headers


def _get_flask_response(
    response: AgentResponse,
) -> Union[Response, Tuple[Dict, int, Optional[Dict]]]:
    result = response.result
    if isinstance(result, BinaryIO):
        return send_file(result, mimetype="application/octet-stream")
    return response.result, response.status_code, _get_response_headers(response)


@app.route("/api/v1/agent/execute/<connection_type>/<operation_name>", methods=["POST"])  # type: ignore
def agent_execute(
    connection_type: str, operation_name: str
) -> Union[Response, Tuple[Dict, int, Optional[Dict]]]:
    """
    Executes the operation named "operation_name" in a connection of type "connection_type", for example bigquery.
    The body is expected to be a JSON document including a "credentials" attribute with the credentials to use for
    the connection and an "operation" attribute with the definition of the operation, as described in the README file.
    :param connection_type: the connection type to use, for example bigquery.
    :param operation_name: the name of the operation to execute, this is used only for logging purposes as the
        definition of what is executed is included in the "operation" attribute in the body.
    :return: the result of the operation (that is expected to be a Dictionary) and the status code to send in the
        response. If there was an error executing the operation a dictionary containing: __error__ and __stack_trace__
        will be returned, see :class:`AgentUtils` for more information.
    """
    json_request: Dict = request.json  # type: ignore
    credentials = json_request.get("credentials", {})
    operation = json_request.get("operation")

    response = agent.execute_operation(
        connection_type, operation_name, operation, credentials
    )
    return _get_flask_response(response)


@app.route("/api/v1/test/health", methods=["GET", "POST"])
def test_health() -> Tuple[Dict, int]:
    """
    Endpoint that returns health information about the agent, can be used as a "ping" endpoint.
    :return: health information about this agent, includes version number and information about the platform
    """
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id = request_dict.get("trace_id")
    return agent.health_information(trace_id).to_dict(), 200


@app.route("/api/v1/test/network/open", methods=["GET", "POST"])
def test_network_open() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port.
    Supported parameters (both in a JSON body or as query params):
    - host
    - port
    - timeout (in seconds)
    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_tcp_open_connection)


@app.route("/api/v1/test/network/telnet", methods=["GET", "POST"])
def test_network_telnet() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port using a Telnet connection.
    Supported parameters (both in a JSON body or as query params):
    - host
    - port
    - timeout (in seconds)
    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_telnet_connection)


@app.route("/api/v1/update", methods=["POST"])
def update_agent() -> Tuple[Dict, int]:
    """
    Requests the agent to update itself.
    Supported parameters:
    - trace_id
    - timeout (in seconds)
    - **kwargs supported by the updater implementation
    :return: a dictionary from the updater with the result
    """
    request_dict: Dict[str, Any] = {**request.json}  # type: ignore
    trace_id = request_dict.pop("trace_id") if "trace_id" in request_dict else None
    timeout = request_dict.pop("timeout") if "timeout" in request_dict else None

    response = agent.update(trace_id=trace_id, timeout_seconds=timeout, **request_dict)

    return response.result, response.status_code


def _execute_network_validation(method: Callable) -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore

    response = method(
        host=request_dict.get("host"),
        port_str=request_dict.get("port"),
        timeout_str=request_dict.get("timeout"),
        trace_id=request_dict.get("trace_id"),
    )
    return response.result, response.status_code


if __name__ == "__main__":
    is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
    logging.basicConfig(level=logging.DEBUG if is_debug else logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=is_debug)
