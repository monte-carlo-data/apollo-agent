from typing import Optional, Dict, Tuple, List, Union, Any
from flask import Flask, request

from apollo.agent.agent import Agent

app = Flask(__name__)


@app.route("/api/v1/agent/execute/<connection_type>", methods=["POST"])
def agent_execute(connection_type: str) -> Tuple[Optional[Dict], int]:
    json_request = request.json
    client = _get_proxy_client(connection_type, json_request.get("credentials", {}))
    commands = json_request["commands"]

    return _execute_commands(client, commands)


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
    client: Any, commands: List[Union[Dict, List]]
) -> Tuple[Optional[Dict], int]:
    agent = Agent()
    result = agent.execute(client, commands)
    return result, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081, debug=True)
