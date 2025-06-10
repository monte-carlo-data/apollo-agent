import io
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Callable, Optional, Union, Any, BinaryIO

from flask import Flask, request, Response, send_file, jsonify, render_template
from flask_compress import Compress

from apollo.agent.agent import Agent
from apollo.agent.constants import TRACE_ID_HEADER
from apollo.agent.env_vars import DEBUG_ENV_VAR, MCD_AGENT_CLOUD_PLATFORM_ENV_VAR
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.settings import VERSION
from apollo.credentials.factory import CredentialsFactory
from apollo.interfaces.agent_response import AgentResponse

app = Flask(__name__)
Compress(app)
logger = logging.getLogger(__name__)
logging_utils = LoggingUtils()
agent = Agent(logging_utils)

_DEFAULT_UPDATE_EVENTS_LIMIT = 100


def _get_response_headers(response: AgentResponse) -> Dict:
    headers = {}
    if response.trace_id:
        headers[TRACE_ID_HEADER] = response.trace_id
    result = response.result
    if isinstance(result, bytes) or isinstance(result, io.IOBase):
        headers["Content-Type"] = (
            "application/gzip" if response.compressed else "application/octet-stream"
        )
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
    Executes an agent operation for a given integration.
    Executes the operation named "operation_name" in a connection of type "connection_type", for example `bigquery`.
    The body is expected to be a JSON document including a `credentials` attribute with the credentials to use for
    the connection and an `operation` attribute with the definition of the operation, as described in the [README](https://github.com/monte-carlo-data/apollo-agent/blob/main/examples/README.md)
    file in the examples directory.
    ---
    tags:
        - Agent Operations
    produces:
        - application/json
    parameters:
        - in: path
          name: connection_type
          required: true
          description: the connection type to use, one of bigquery, databricks, http, storage, looker, git,
            redshift, postgres, sql-server, snowflake, mysql, oracle, teradata, azure-dedicated-sql-pool,
            azure-sql-database, tableau, sap-hana, power-bi.
          schema:
              type: string
              example: snowflake
        - in: path
          name: operation_name
          required: true
          description: the name of the operation to execute, this is used only for logging purposes as the
            definition of what is executed is included in the "operation" attribute in the body.
            Check the [README](https://github.com/monte-carlo-data/apollo-agent/blob/main/examples/README.md) file
            for more information about how operations and commands work.
          schema:
              type: string
              example: execute_query
        - in: body
          name: body
          schema:
            id: ExecuteOperationRequest
            properties:
                credentials:
                    type: object
                    description: authentication information for establishing the connection.
                    example:
                        user: user_name
                        password: password
                operation:
                    type: object
                    description: The operation to execute, defined by a list of commands.
                    properties:
                        trace_id:
                            type: string
                            description: An optional trace id
                            example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                        commands:
                            type: array
                            description: The list of commands to execute.
                            items:
                                properties:
                                    target:
                                        type: string
                                        default: _client
                                        example: _cursor
                                        description: Name of the context variable to use as the target for the `method`
                                            to invoke.
                                    method:
                                        type: string
                                        required: true
                                        description: The method to invoke in `target`.
                                        example: execute
                                    store:
                                        type: string
                                        example: _cursor
                                        description: Optional name of a variable to store the result of the invocation.
                                    args:
                                        type: array
                                        description: Positional arguments for the method to invoke.
                                        items:
                                            type: object
                                    kwargs:
                                        type: object
                                        description: Keyword arguments for the method to invoke.
            example:
                credentials:
                    connect_args:
                        user: user_name
                        password: password
                        account: account
                        warehouse: warehouse
                operation:
                    trace_id: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                    commands:
                        - method: cursor
                          store: _cursor
                        - target: _cursor
                          method: execute
                          args: [
                            "select database_name from snowflake.information_schema.databases"
                          ]
                        - target: _cursor
                          method: fetchall
    responses:
        200:
            description: Returns the result of the operation (that is expected to be a Dictionary).
                If there was an error executing the operation a dictionary containing __mcd_error__ and
                __mcd_stack_trace__ will be returned.
            schema:
                properties:
                    __mcd_result__:
                        type: object
                        description: The operation result if the execution was successful.
                            If there was an error executing the operation this
                            element won't be present, see `__mcd_error__` for more information.
                    __mcd_error__:
                        type: string
                        description: The error message occurred (if any).
                    __mcd_exception__:
                        type: string
                        description: Additional information about the error occurred (if any).
                    __mcd_stack_trace__:
                        type: array
                        description: The stack trace for the error occurred (if any).
                        items:
                            type: string
                example:
                    __mcd_result__: [
                        [
                            "database_1"
                        ],
                        [
                            "database_2"
                        ],
                    ]

    :param connection_type: the connection type to use, for example bigquery.
    :param operation_name: the name of the operation to execute, this is used only for logging purposes as the
        definition of what is executed is included in the "operation" attribute in the body.
    :return: the result of the operation (that is expected to be a Dictionary) and the status code to sent in the
        response. If there was an error executing the operation a dictionary containing __error__ and __stack_trace__
        will be returned, see :class:`AgentUtils` for more information.
    """
    json_request: Dict = request.json  # type: ignore
    response = execute_agent_operation(
        connection_type=connection_type,
        operation_name=operation_name,
        json_request=json_request,
    )
    return _get_flask_response(response)


def execute_agent_operation(
    connection_type: str, operation_name: str, json_request: Dict
) -> AgentResponse:
    credentials = _extract_credentials_in_request(json_request.get("credentials", {}))
    operation = json_request.get("operation")

    return agent.execute_operation(
        connection_type, operation_name, operation, credentials
    )


@app.route("/api/v1/agent/execute_script/<connection_type>", methods=["POST"])  # type: ignore
def agent_execute_script(
    connection_type: str,
) -> Union[Response, Tuple[Dict, int, Optional[Dict]]]:
    """
    Executes an agent script for a given integration.
    The script must include a function called `run` that receives the connection client as an
    argument, and a context object with information about the agent and service APIs (storage, etc.).
    ---
    tags:
        - Agent Operations
    produces:
        - application/json
    parameters:
        - in: path
          name: connection_type
          required: true
          description: the connection type to use, one of bigquery, databricks, http, storage, looker, git,
            redshift, postgres, sql-server, snowflake, mysql, oracle, teradata, azure-dedicated-sql-pool,
            azure-sql-database, tableau, sap-hana, power-bi.
          schema:
              type: string
              example: snowflake
        - in: body
          name: body
          schema:
            id: ExecuteScriptRequest
            properties:
                credentials:
                    type: object
                    description: authentication information for establishing the connection.
                    example:
                        user: user_name
                        password: password
                operation:
                    type: object
                    description: The python script to execute.
                    properties:
                        trace_id:
                            type: string
                            description: An optional trace id
                            example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                        script:
                            type: str
                            description: The source of the script to execute.
                        args:
                            type: object
                            description: Keyword arguments for the method to invoke.
            example:
                credentials:
                    connect_args:
                        user: user_name
                        password: password
                        account: account
                        warehouse: warehouse
                operation:
                    trace_id: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                    script: |
                         def run(connection, context, sql_query):
                            cursor = connection.cursor()
                            cursor.execute(sql_query)
                            return cursor.fetchall()

                    kwargs:
                        sql_query: "select database_name from snowflake.information_schema.databases"
    responses:
        200:
            description: Returns the result of the operation (that is expected to be a Dictionary).
                If there was an error executing the operation a dictionary containing __mcd_error__ and
                __mcd_stack_trace__ will be returned.
            schema:
                properties:
                    __mcd_result__:
                        type: object
                        description: The operation result if the execution was successful.
                            If there was an error executing the operation this
                            element won't be present, see `__mcd_error__` for more information.
                    __mcd_error__:
                        type: string
                        description: The error message occurred (if any).
                    __mcd_exception__:
                        type: string
                        description: Additional information about the error occurred (if any).
                    __mcd_stack_trace__:
                        type: array
                        description: The stack trace for the error occurred (if any).
                        items:
                            type: string
                example:
                    __mcd_result__: [
                        [
                            "database_1"
                        ],
                        [
                            "database_2"
                        ],
                    ]

    :param connection_type: the connection type to use, for example bigquery.
    :return: the result of the operation (that is expected to be a Dictionary) and the status code to sent in the
        response. If there was an error executing the operation a dictionary containing __error__ and __stack_trace__
        will be returned, see :class:`AgentUtils` for more information.
    """
    json_request: Dict = request.json  # type: ignore
    response = execute_agent_script(
        connection_type=connection_type,
        json_request=json_request,
    )
    return _get_flask_response(response)


def execute_agent_script(connection_type: str, json_request: Dict) -> AgentResponse:
    script = json_request.get("operation")
    credentials = _extract_credentials_in_request(json_request.get("credentials", {}))
    return agent.execute_script(connection_type, script, credentials)


@app.route("/api/v1/test/health", methods=["GET"])
def test_health_get() -> Tuple[Dict, int]:
    """
    Returns health information about the agent.
    Endpoint that returns health information about the agent, can be used as a "ping" endpoint.
    Receives an optional parameter: "full", which when "true" includes extra information like outbound IP address.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
        - in: query
          name: full
          description: Include extra information like "outbound_ip_address".
          type: boolean
          default: false
    definitions:
        - schema:
            id: HealthInformationResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        build:
                            type: string
                            description: Current build number, for example '345'
                        version:
                            type: string
                            description: Current version, for example '1.0.1'
                        trace_id:
                            type: string
                            description: The same trace_id passed as an input parameter.
                        platform:
                            type: string
                            description: Platform name, for example AWS, GCP or Azure.
                        env:
                            type: object
                            description: Environment variables, depends on the platform.
                        platform_info:
                            type: object
                            description: Platform information, depends on the platform.
                    example:
                        build: "698"
                        version: 1.0.1
                        platform: AWS
                        env:
                            AWS_REGION: us-east-1
                            MCD_STACK_ID: cf_stack_arn
                        platform_info:
                            image: account.dkr.ecr.*.amazonaws.com/repo:1.0.1
                        trace_id: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns health information for this agent.
            schema:
                $ref: "#/definitions/HealthInformationResponse"
    :return: health information about this agent, includes version number and information about the platform
    """
    return _test_health()


@app.route("/api/v1/test/health", methods=["POST"])
def test_health_post() -> Tuple[Dict, int]:
    """
    Returns health information about the agent.
    Endpoint that returns health information about the agent, can be used as a "ping" endpoint.
    Receives an optional parameter: "full", which when "true" includes extra information like outbound IP address.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: TestHealthRequest
            properties:
                trace_id:
                    type: string
                    description: An optional trace_id
                    example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                full:
                    type: boolean
                    default: false
                    description: Include extra information like "outbound_ip_address".
    responses:
        200:
            description: Returns health information for this agent.
            schema:
                $ref: "#/definitions/HealthInformationResponse"
    :return: health information about this agent, includes version number and information about the platform
    """
    return _test_health()


def _test_health() -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id = request_dict.get("trace_id")
    full = str(request_dict.get("full", "false")).lower() == "true"
    if os.getenv("AGENT_PING_SLEEP"):
        time.sleep(int(os.getenv("AGENT_PING_SLEEP")))  # type: ignore
    return agent.health_information(trace_id, full).to_dict(), 200


@app.route("/api/v1/test/network/open", methods=["GET"])
def test_network_open_get() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: host
          description: The host name to test
          required: true
          schema:
              type: string
              example: getmontecarlo.com
        - in: query
          name: port
          description: The port number to test
          required: true
          schema:
              type: integer
              example: 80
        - in: query
          name: timeout
          type: integer
          default: 5
          description: Optional timeout in seconds
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    definitions:
        - schema:
            id: TestNetworkOpenResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        message:
                            type: string
                            description: A message indicating if the connection was successful or not.
                __mcd_trace_id__:
                    type: string
                    description: The trace_id passed as an input parameter.
            example:
                __mcd_result__:
                    message: Port 80 is open on getmontecarlo.com
                __mcd_trace_id__: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message indicating if the connection was successful or not.
            schema:
                $ref: "#/definitions/TestNetworkOpenResponse"

    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_tcp_open_connection)


@app.route("/api/v1/test/network/open", methods=["POST"])
def test_network_open_post() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: TestNetworkOpenRequest
            properties:
                host:
                  type: string
                  description: The host name to test
                  required: true
                  example: getmontecarlo.com
                port:
                  type: integer
                  description: The port number to test
                  required: true
                  example: 80
                timeout:
                  type: integer
                  default: 5
                  description: Optional timeout in seconds
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message indicating if the connection was successful or not.
            schema:
                $ref: "#/definitions/TestNetworkOpenResponse"

    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_tcp_open_connection)


@app.route("/api/v1/test/network/telnet", methods=["GET"])
def test_network_telnet_get() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port using a Telnet connection.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: host
          description: The host name to test
          required: true
          schema:
              type: string
              example: getmontecarlo.com
        - in: query
          name: port
          description: The port number to test
          required: true
          schema:
              type: integer
              example: 80
        - in: query
          name: timeout
          type: integer
          default: 5
          description: Optional timeout in seconds
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    definitions:
        - schema:
            id: TestNetworkTelnetResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        message:
                            type: string
                            description: A message indicating if the connection was successful or not.
                __mcd_trace_id__:
                    type: string
                    description: The trace_id passed as an input parameter.
            example:
                __mcd_result__:
                    message: Telnet connection for getmontecarlo.com:80 is usable.
                __mcd_trace_id__: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message indicating if the connection was successful or not.
            schema:
                $ref: "#/definitions/TestNetworkTelnetResponse"
    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_telnet_connection)


@app.route("/api/v1/test/network/telnet", methods=["POST"])
def test_network_telnet_post() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port using a Telnet connection.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: TestNetworkTelnetRequest
            properties:
                host:
                  type: string
                  description: The host name to test
                  required: true
                  example: getmontecarlo.com
                port:
                  type: integer
                  description: The port number to test
                  required: true
                  example: 80
                timeout:
                  type: integer
                  default: 5
                  description: Optional timeout in seconds
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message indicating if the connection was successful or not.
            schema:
                $ref: "#/definitions/TestNetworkTelnetResponse"
    :return: a message indicating if the connection was successful or not
    """
    return _execute_network_validation(agent.validate_telnet_connection)


@app.route("/api/v1/test/network/dns", methods=["GET"])
def perform_dns_lookup_get() -> Tuple[Dict, int]:
    """
    Performs a DNS lookup for the specified host name.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: host
          description: The host name to lookup for
          required: true
          schema:
              type: string
              example: getmontecarlo.com
        - in: query
          name: port
          description: The port number to use
          required: false
          schema:
              type: integer
              example: 80
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    definitions:
        - schema:
            id: DnsLookupResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        message:
                            type: string
                            description: A message including the response of the lookup operation.
                __mcd_trace_id__:
                    type: string
                    description: The trace_id passed as an input parameter.
            example:
                __mcd_result__:
                    message: Host getmontecarlo.com resolves to 1.2.3.4
                __mcd_trace_id__: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message including the response of the lookup operation.
            schema:
                $ref: "#/definitions/DnsLookupResponse"
    :return: a message including the response of the lookup operation
    """
    return _execute_network_validation(agent.perform_dns_lookup, include_timeout=False)


@app.route("/api/v1/test/network/dns", methods=["POST"])
def perform_dns_lookup_post() -> Tuple[Dict, int]:
    """
    Tests network connectivity to the given host in the specified port using a Telnet connection.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: DnsLookupRequest
            properties:
                host:
                  type: string
                  description: The host name to lookup for
                  required: true
                  example: getmontecarlo.com
                port:
                  type: integer
                  description: The port number to use
                  required: false
                  example: 80
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message including the response of the lookup operation.
            schema:
                $ref: "#/definitions/DnsLookupResponse"
    :return: a message including the response of the lookup operation
    """
    return _execute_network_validation(agent.perform_dns_lookup, include_timeout=False)


@app.route("/api/v1/test/network/http", methods=["GET"])
def test_network_http_connection_get() -> Tuple[Dict, int]:
    """
    Performs a GET request to test HTTP connectivity to the provided URL.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: url
          description: The URL to test
          required: true
          schema:
              type: string
              example: https://getmontecarlo.com
        - in: query
          name: include_response
          description: Whether to include the response from the URL or not
          required: false
          default: false
          schema:
              type: boolean
              example: false
        - in: query
          name: timeout
          type: integer
          default: 10
          description: Optional timeout in seconds, defaults to 10
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
              type: string
              example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    definitions:
        - schema:
            id: HttpConnectionTestResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        message:
                            type: string
                            description: A message including the response of the lookup operation.
                __mcd_trace_id__:
                    type: string
                    description: The trace_id passed as an input parameter.
            example:
                __mcd_result__:
                    message: "URL https://getmontecarlo.com responded with status code: 200."
                __mcd_trace_id__: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message including the status code from executing a GET request.
            schema:
                $ref: "#/definitions/HttpConnectionTestResponse"
    :return: a message including the status code from executing a GET request.
    """
    return _execute_http_connection_test()


@app.route("/api/v1/test/network/http", methods=["POST"])
def test_network_http_connection_post() -> Tuple[Dict, int]:
    """
    Performs a GET request to test HTTP connectivity to the provided URL.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: HttpConnectionTestRequest
            properties:
                url:
                  type: string
                  description: The URL to test
                  required: true
                  example: https://getmontecarlo.com
                include_response:
                  type: boolean
                  description: Whether to include the response from the URL or not
                  required: false
                  example: false
                timeout:
                  type: integer
                  description: Optional timeout in seconds, defaults to 10.
                  example: 10
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns a message including the status code from executing a GET request.
            schema:
                $ref: "#/definitions/HttpConnectionTestResponse"
    :return: a message including the status code from executing a GET request.
    """
    return _execute_http_connection_test()


@app.route("/api/v1/upgrade", methods=["POST"])
def upgrade_agent() -> Tuple[Dict, int]:
    """
    Requests the agent to upgrade to a given image.
    ---
    tags:
        - Upgrading
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: UpgradeRequest
            properties:
                trace_id:
                    type: string
                    description: Optional trace id
                    example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                image:
                    type: string
                    description: Image URI, for example montecarlodata/agent:1.0.1-cloudrun
                    required: true
                    example: montecarlodata/agent:1.0.1-cloudrun
                timeout:
                    type: integer
                    description: Optional timeout in seconds, used only by GCP updater.
                    example: 30
                parameters:
                    type: object
                    description: Optional parameters to update, like ConcurrentExecutions or MemorySize, supported only
                        by AWS platform.
                    example:
                        ConcurrentExecutions: 20
                        MemorySize: 1024
                wait_for_completion:
                    type: boolean
                    default: false
                    description: Optional flag indicating if the endpoint should wait for the update to complete, supported
                        only by AWS platform.
    responses:
        200:
            description: Returns a dictionary from the updater with the result of the upgrade request.
            schema:
                properties:
                    __mcd_result__:
                        type: object
                        description: Properties returned by the platform-specific updater.
                    __mcd_trace_id__:
                        type: string
                        description: The trace_id passed as an input parameter.
                example:
                    __mcd_result__:
                        revision: GCP revision id
                        service-name: GCP service name
                    __mcd_trace_id__: 324986b4-b185-4187-b4af-b0c2cd60f7a0

    :return: a dictionary from the updater with the result of the upgrade request.
    """
    request_dict: Dict[str, Any] = {**request.json}  # type: ignore
    trace_id = request_dict.pop("trace_id") if "trace_id" in request_dict else None
    image = request_dict.pop("image") if "image" in request_dict else None
    timeout = request_dict.pop("timeout") if "timeout" in request_dict else None

    response = agent.update(
        trace_id=trace_id, image=image, timeout_seconds=timeout, **request_dict
    )

    return response.result, response.status_code


@app.route("/api/v1/upgrade/logs", methods=["GET"])
def get_upgrade_logs_get() -> Tuple[Dict, int]:
    """
    Requests the agent to return a list of upgrade log events after the given datetime.
    ---
    tags:
        - Upgrading
    produces:
        - application/json
    parameters:
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
            type: string
            example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
        - in: query
          name: start_time
          description: The start time for the log events, a datetime in ISO format. Defaults to 10 minutes ago.
          schema:
              type: string
              example: "2023-12-25T12:31:45+00:00"
        - in: query
          name: limit
          type: integer
          description: Maximum number of events to return.
          default: 100
    definitions:
        - schema:
            id: GetUpgradeLogsResponse
            properties:
                __mcd_result__:
                    type: object
                    properties:
                        events:
                            type: array
                            items:
                                type: object
                                properties:
                                    timestamp:
                                        type: string
                                    logicalResourceId:
                                        type: string
                                    resourceType:
                                        type: string
                                    resourceStatus:
                                        type: string
                                    resourceStatusReason:
                                        type: string
                    example:
                        events:
                            - timestamp: "2023-12-28T14:13:48.445000+00:00"
                              logicalResourceId: Storage
                              resourceType: AWS::S3::Bucket
                              resourceStatus: UPDATE_COMPLETE
                              resourceStatusReason: null
                            - timestamp: "2023-12-28T14:13:47.445000+00:00"
                              logicalResourceId: Function
                              resourceType: AWS::Lambda::Function
                              resourceStatus: UPDATE_COMPLETE
                              resourceStatusReason: null
    responses:
        200:
            description: Returns a list of upgrade log events after the given datetime.
            schema:
                $ref: "#/definitions/GetUpgradeLogsResponse"
    :return: a dictionary with an "events" attribute containing the list of events returned
        from the updater implementation.
    """
    return _get_upgrade_logs()


@app.route("/api/v1/upgrade/logs", methods=["POST"])
def get_upgrade_logs_post() -> Tuple[Dict, int]:
    """
    Requests the agent to return a list of upgrade log events after the given datetime.
    ---
    tags:
        - Upgrading
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: UpgradeLogsRequest
            properties:
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
                start_time:
                  type: string
                  description: The start time for the log events, a datetime in ISO format. Defaults to 10 minutes ago.
                  example: "2023-12-25T12:31:45+00:00"
                limit:
                  type: integer
                  description: Maximum number of events to return.
                  default: 100
    responses:
        200:
            description: Returns a list of upgrade log events after the given datetime.
            schema:
                $ref: "#/definitions/GetUpgradeLogsResponse"
    :return: a dictionary with an "events" attribute containing the list of events returned
        from the updater implementation.
    """
    return _get_upgrade_logs()


def _get_upgrade_logs() -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    start_time_str: Optional[str] = request_dict.get("start_time")
    limit_value: Optional[Union[int, str]] = request_dict.get("limit")
    start_time = (
        datetime.fromisoformat(start_time_str)
        if start_time_str
        else datetime.now(timezone.utc) - timedelta(minutes=10)
    )
    if not start_time.tzinfo:
        start_time = start_time.astimezone(timezone.utc)  # make it offset-aware
    limit = int(limit_value) if limit_value else _DEFAULT_UPDATE_EVENTS_LIMIT

    response = agent.get_update_logs(
        trace_id=trace_id, start_time=start_time, limit=limit
    )

    return response.result, response.status_code


@app.route("/api/v1/infra/details", methods=["GET"])
def get_infra_details_get() -> Tuple[Dict, int]:
    """
    Get Infrastructure Details
    Requests the infrastructure details to the agent that will forward the request to the "infra_provider"
    previously set.
    Returns a dictionary with the infrastructure details returned by the infra_provider implementation
    set in the agent.
    ---
    tags:
        - Infrastructure
    produces:
        - application/json
    parameters:
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
            type: string
            example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    definitions:
        - schema:
            id: InfraDetailsResponse
            properties:
                __mcd_result__:
                    type: object
                    example:
                        template: CF Template in YAML Format
                        parameters:
                            - ParameterKey: MemorySize
                              ParameterValue: "512"
                            - ParameterKey: ConcurrentExecutions
                              ParameterValue: "20"
    responses:
        200:
            description: Returns infrastructure information for this agent, the attributes returned depend on the
                platform.
            schema:
                $ref: "#/definitions/InfraDetailsResponse"
    """
    return _get_infra_details()


@app.route("/api/v1/infra/details", methods=["POST"])
def get_infra_details_post() -> Tuple[Dict, int]:
    """
    Get Infrastructure Details
    Requests the infrastructure details to the agent that will forward the request to the "infra_provider"
    previously set.
    Returns a dictionary with the infrastructure details returned by the infra_provider implementation
    set in the agent.
    ---
    tags:
        - Infrastructure
    produces:
        - application/json
    parameters:
        - in: body
          name: body
          schema:
            id: InfraDetailsRequest
            properties:
                trace_id:
                  type: string
                  description: An optional trace_id
                  example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns infrastructure information for this agent, the attributes returned depend on the
                platform.
            schema:
                $ref: "#/definitions/InfraDetailsResponse"
    """
    return _get_infra_details()


def _get_infra_details() -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    trace_id: Optional[str] = request_dict.get("trace_id")
    response = agent.get_infra_details(trace_id=trace_id)

    return response.result, response.status_code


@app.route("/api/v1/test/network/outbound_ip_address", methods=["GET"])
def get_outbound_ip_address() -> Tuple[Dict, int]:
    """
    Get outbound IP Address.
    Returns the public IP address used by the agent for outbound connections.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    parameters:
        - in: query
          name: trace_id
          description: An optional trace_id
          schema:
            type: string
            example: 324986b4-b185-4187-b4af-b0c2cd60f7a0
    responses:
        200:
            description: Returns the outbound IP address for this agent.
            schema:
                properties:
                    __mcd_result__:
                        type: object
                        properties:
                            outbound_ip_address:
                                type: string
                        example:
                            outbound_ip_address: 12.34.5.255

    """
    response = agent.get_outbound_ip_address(request.args.get("trace_id"))
    return response.result, response.status_code


# the root route and '/health' are common defaults for health check endpoints
@app.route("/")
@app.route("/health")
def health() -> dict:
    """
    Basic health check endpoint used by container orchestration platforms to verify the container is running.
    ---
    tags:
        - Troubleshooting
    produces:
        - application/json
    responses:
        200:
            description: Agent API is up
            schema:
                properties:
                    status:
                        type: string
                        example: up
    """
    return {"status": "up"}


@app.route("/swagger/openapi.json")
def open_api():
    # imported here so we don't need to add flask-swagger to requirements.in (it's only in requirements-dev.in)
    from flask_swagger import swagger

    swag = swagger(app)
    swag["info"]["title"] = "Monte Carlo - Apollo Agent API"
    swag["info"]["version"] = VERSION
    swag["info"]["license"] = {
        "name": "Monte Carlo Data, Inc. License",
        "url": "https://github.com/monte-carlo-data/apollo-agent/blob/main/LICENSE.md",
    }
    swag["externalDocs"] = {
        "url": "https://docs.getmontecarlo.com",
    }
    swag["host"] = request.host
    swag["schemes"] = ["http"] if VERSION == "local" else ["https"]

    return jsonify(swag)


@app.route("/swagger/")
def swagger_index():
    return render_template("swagger.html")


def _execute_network_validation(
    method: Callable, include_timeout: bool = True
) -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    args = dict(
        host=request_dict.get("host"),
        port_str=request_dict.get("port"),
        trace_id=request_dict.get("trace_id"),
    )
    if include_timeout:
        args["timeout_str"] = request_dict.get("timeout")
    response = method(**args)
    return response.result, response.status_code


def _execute_http_connection_test() -> Tuple[Dict, int]:
    request_dict: Dict = request.json if request.method == "POST" else request.args  # type: ignore
    response = agent.validate_http_connection(
        url=request_dict.get("url"),
        include_response_str=request_dict.get("include_response"),
        timeout_str=request_dict.get("timeout"),
        trace_id=request_dict.get("trace_id"),
    )
    return response.result, response.status_code


def _extract_credentials_in_request(credentials: Dict) -> Dict:
    credential_service = CredentialsFactory.get_credentials_service(credentials)
    return credential_service.get_credentials(credentials)


if __name__ == "__main__":
    is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
    logging.basicConfig(level=logging.DEBUG if is_debug else logging.INFO)
    app.run(host="0.0.0.0", port=8081, debug=is_debug)
