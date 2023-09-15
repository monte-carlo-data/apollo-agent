import logging
from typing import Optional, Dict, Tuple

from flask import Flask, request

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils

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
    return Agent.health_information(), 200


@app.route("/api/v1/test/network")
def test_network() -> Tuple[Dict, int]:
    return {}, 200


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=True)
