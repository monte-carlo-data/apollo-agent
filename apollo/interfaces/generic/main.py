import logging
import os
import sys
from typing import Optional, Dict, Tuple

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
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


@app.route("/hello/<name>", methods=["GET"])
def hello(name: str):
    return f"hello {name} from docker 19 {os.getenv('PYTHON_VERSION')} {sys.version}\n"


@app.route("/list", methods=["GET"])
def list_files():
    # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    # blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    account_url = "https://mcddevagenttest.blob.core.windows.net"
    default_credential = DefaultAzureCredential()

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=default_credential)
    container_name = "test"
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs()
    return {"blobs": [blob.name for blob in blobs]}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=True)
