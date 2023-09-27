import logging
from typing import Dict, Tuple, Callable

from flask import Flask, request

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.validators.validate_network import ValidateNetwork

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging_utils = LoggingUtils()
agent = Agent(logging_utils)


@app.route("/api/v1/agent/execute/<connection_type>/<operation_name>", methods=["POST"])
def agent_execute(connection_type: str, operation_name: str) -> Tuple[Dict, int]:
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
    json_request = request.json
    credentials = json_request.get("credentials", {})
    operation = json_request.get("operation")

    response = agent.execute_operation(
        connection_type, operation_name, operation, credentials
    )
    return response.result, response.status_code


@app.route("/api/v1/test/health")
def test_health() -> Tuple[Dict, int]:
    """
    Endpoint that returns health information about the agent, can be used as a "ping" endpoint.
    :return: health information about this agent, includes version number and information about the platform
    """
    return agent.health_information().to_dict(), 200


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
    return _execute_network_validation(ValidateNetwork.validate_tcp_open_connection)


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
    return _execute_network_validation(ValidateNetwork.validate_telnet_connection)


def _execute_network_validation(method: Callable) -> Tuple[Dict, int]:
    request_dict = request.json if request.method == "POST" else request.args

    response = method(
        host=request_dict.get("host"),
        port_str=request_dict.get("port"),
        timeout_str=request_dict.get("timeout"),
    )
    return response.result, response.status_code


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=True)
