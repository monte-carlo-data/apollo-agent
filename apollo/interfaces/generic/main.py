import logging
from typing import Optional, Dict, Tuple, List, Union, Any
from uuid import uuid4

from flask import Flask, request

from apollo.agent.agent import Agent
from apollo.agent.models import AgentOperation
from apollo.interfaces.generic.logging_utils import LoggingUtils

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging_utils = LoggingUtils()


@app.route("/api/v1/agent/execute/<connection_type>", methods=["POST"])
def agent_execute(connection_type: str) -> Tuple[Optional[Dict], int]:
    json_request = request.json
    credentials = json_request.get("credentials", {})
    operation_dict = json_request.get("operation")
    if not operation_dict:
        return {"__error__": "operation is a required parameter"}, 400
    try:
        operation = AgentOperation.from_dict(operation_dict)
    except Exception as ex:
        return {"__error__": f"Failed to read operation: {ex}"}, 400

    client = _get_proxy_client(connection_type, credentials)

    return _execute_commands(connection_type, client, operation)


def _get_proxy_client(connection_type: str, credentials: Dict) -> Any:
    if connection_type == "bigquery":
        return _get_proxy_client_bigquery(credentials)
    else:
        raise Exception(
            f"Connection type not supported by this agent: {connection_type}"
        )


def _get_proxy_client_bigquery(credentials: Dict) -> Any:
    from apollo.integrations.bigquery.bq_proxy_client import BqProxyClient

    return BqProxyClient(credentials=credentials)


def _execute_commands(
    connection_type: str,
    client: Any,
    operation: AgentOperation,
) -> Tuple[Optional[Dict], int]:
    logger.info(
        f"Executing {connection_type} commands",
        extra=logging_utils.build_extra(
            operation.trace_id,
            operation.to_dict(),
        ),
    )
    agent = Agent()
    result = agent.execute(client, operation)
    return result, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
