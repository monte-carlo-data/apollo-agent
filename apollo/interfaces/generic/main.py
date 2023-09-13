import logging
from typing import Optional, Dict, Tuple, List, Union, Any
from uuid import uuid4

from flask import Flask, request

from apollo.agent.agent import Agent
from apollo.interfaces.generic.logging_utils import LoggingUtils

app = Flask(__name__)
logger = logging.getLogger(__name__)
logging_utils = LoggingUtils()


@app.route("/api/v1/agent/execute/<connection_type>", methods=["POST"])
def agent_execute(connection_type: str) -> Tuple[Optional[Dict], int]:
    json_request = request.json
    client = _get_proxy_client(connection_type, json_request.get("credentials", {}))
    trace_id = json_request.get("trace_id", str(uuid4()))
    commands = json_request["commands"]

    return _execute_commands(connection_type, trace_id, client, commands)


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
    connection_type: str, trace_id: str, client: Any, commands: List[Union[Dict, List]]
) -> Tuple[Optional[Dict], int]:
    logger.info(
        f"Executing {connection_type} commands",
        extra=logging_utils.build_extra(
            trace_id,
            {
                "connection_type": connection_type,
                "commands": commands,
            },
        ),
    )
    agent = Agent()
    result = agent.execute(client, commands)
    return result, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
