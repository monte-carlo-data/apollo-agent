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
def agent_execute(
    connection_type: str, operation_name: str
) -> Tuple[Optional[Dict], int]:
    json_request = request.json
    credentials = json_request.get("credentials", {})
    operation = json_request.get("operation")

    response = agent.execute_operation(
        connection_type, operation_name, operation, credentials
    )
    return response.result, response.status_code


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
