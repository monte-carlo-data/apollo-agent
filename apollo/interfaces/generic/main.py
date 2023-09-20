import logging
import os
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
    json_request = request.json
    credentials = json_request.get("credentials", {})
    operation = json_request.get("operation")

    response = agent.execute_operation(
        connection_type, operation_name, operation, credentials
    )
    return response.result, response.status_code


@app.route("/api/v1/test/health")
def test_health() -> Tuple[Dict, int]:
    return agent.health_information(), 200


@app.route("/api/v1/test/network/open", methods=["GET", "POST"])
def test_network_open() -> Tuple[Dict, int]:
    return _execute_network_validation(ValidateNetwork.validate_tcp_open_connection)


@app.route("/api/v1/test/network/telnet", methods=["GET", "POST"])
def test_network_telnet() -> Tuple[Dict, int]:
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
    logging.basicConfig(level=logging.DEBUG if os.getenv("DEBUG") else logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=True)
