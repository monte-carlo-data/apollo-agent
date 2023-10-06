import logging
import os
import sys
from typing import Any, Dict, Optional

from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.constants import (
    CONTEXT_VAR_UTILS,
    CONTEXT_VAR_CLIENT,
    PLATFORM_GENERIC,
)
from apollo.agent.operation_utils import OperationUtils
from apollo.agent.models import AgentOperation, AgentHealthInformation
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.agent.settings import VERSION, BUILD_NUMBER
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.interfaces.agent_response import AgentResponse
from apollo.validators.validate_network import ValidateNetwork

logger = logging.getLogger(__name__)

_ENV_VARS = [
    "PYTHON_VERSION",
    "SERVER_SOFTWARE",
    "MCD_AGENT_IMAGE_TAG",
    "MCD_AGENT_CLOUD_PLATFORM",
    "MCD_AGENT_WRAPPER_TYPE",
    "MCD_AGENT_WRAPPER_VERSION",
    "MCD_AGENT_IS_REMOTE_UPGRADABLE",
]


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils
        self._platform = PLATFORM_GENERIC
        self._platform_info = {}

    @property
    def platform(self) -> str:
        """
        The name of the platform running this agent, for example: Generic, AWS, GCP, etc.
        """
        return self._platform

    @platform.setter
    def platform(self, platform: str):
        self._platform = platform

    @property
    def platform_info(self) -> Optional[Dict]:
        """
        Dictionary containing platform specific information, it could be container information like versions or
        some other settings relevant to the container.
        """
        return self._platform_info

    @platform_info.setter
    def platform_info(self, platform_info: Optional[Dict]):
        self._platform_info = platform_info

    def health_information(self, trace_id: Optional[str]) -> AgentHealthInformation:
        """
        Returns platform and environment information about the agent:
        - version
        - build
        - platform
        - env (some relevant env information like sys.version or vars like PYTHON_VERSION and MCD_*)
        - specific platform information set using `platform_info` setter
        - the received value for `trace_id` if any
        :return: an `AgentHealthInformation` object that can be converted to JSON.
        """
        logger.info(
            "Health information request received",
            extra=self._logging_utils.build_extra(
                trace_id=trace_id,
                operation_name="health_information",
            ),
        )
        return AgentHealthInformation(
            version=VERSION,
            build=BUILD_NUMBER,
            platform=self._platform,
            env=self._env_dictionary(),
            platform_info=self._platform_info,
            trace_id=trace_id,
        )

    def validate_tcp_open_connection(
        self,
        host: Optional[str],
        port_str: Optional[str],
        timeout_str: Optional[str],
        trace_id: Optional[str] = None,
    ):
        """
        Tests if a destination is reachable and accepts requests. Opens a TCP Socket to the specified host and port.
        :param host: Host to check, will raise `BadRequestError` if None.
        :param port_str: Port to check as a string containing the numeric port value, will raise `BadRequestError`
            if None or non-numeric.
        :param timeout_str: Timeout in seconds as a string containing the numeric value, will raise `BadRequestError`
            if non-numeric. Defaults to 5 seconds.
        :param trace_id: Optional trace ID received from the client that will be included in the response, if present.
        """
        logger.info(
            "Validate TCP Open request received",
            extra=self._logging_utils.build_extra(
                trace_id=trace_id,
                operation_name="test_network_open",
                extra=dict(
                    host=host,
                    port=port_str,
                    timeout=timeout_str,
                ),
            ),
        )
        return ValidateNetwork.validate_tcp_open_connection(
            host, port_str, timeout_str, trace_id
        )

    def validate_telnet_connection(
        self,
        host: Optional[str],
        port_str: Optional[str],
        timeout_str: Optional[str],
        trace_id: Optional[str] = None,
    ):
        """
        Checks if telnet connection is usable.
        :param host: Host to check, will raise `BadRequestError` if None.
        :param port_str: Port to check as a string containing the numeric port value, will raise `BadRequestError`
            if None or non-numeric.
        :param timeout_str: Timeout in seconds as a string containing the numeric value, will raise `BadRequestError`
            if non-numeric. Defaults to 5 seconds.
        :param trace_id: Optional trace ID received from the client that will be included in the response, if present.
        """
        logger.info(
            "Validate Telnet connection request received",
            extra=self._logging_utils.build_extra(
                trace_id=trace_id,
                operation_name="test_network_telnet",
                extra=dict(
                    host=host,
                    port=port_str,
                    timeout=timeout_str,
                ),
            ),
        )
        return ValidateNetwork.validate_telnet_connection(
            host, port_str, timeout_str, trace_id
        )

    @staticmethod
    def _env_dictionary() -> Dict:
        env: Dict[str, Optional[str]] = {
            "sys_version": sys.version,
        }
        env.update(
            {env_var: os.getenv(env_var) for env_var in _ENV_VARS if os.getenv(env_var)}
        )
        return env

    def execute_operation(
        self,
        connection_type: str,
        operation_name: str,
        operation_dict: Optional[Dict],
        credentials: Optional[Dict],
    ) -> AgentResponse:
        """
        Executes an operation for the given connection type using the provided credentials.
        The proxy client factory is used to get a proxy client for the given connection type
        and then the list of commands in the operation are executed on the client object.
        :param connection_type: for example "bigquery"
        :param operation_name: operation name, just for logging purposes
        :param operation_dict: the required dictionary containing the definition of the operation to run, if None an error will be raised
        :param credentials: the optional credentials dictionary
        :return: the result of executing the given operation
        """
        if not operation_dict:
            return AgentUtils.agent_response_for_error(
                "operation is a required parameter", status_code=400
            )
        try:
            operation = AgentOperation.from_dict(operation_dict)
        except Exception:
            logger.exception("Failed to read operation")
            return AgentUtils.agent_response_for_last_exception(
                "Failed to read operation:", 400
            )

        client: Optional[BaseProxyClient] = None
        try:
            client = ProxyClientFactory.get_proxy_client(
                connection_type, credentials, operation.skip_cache, self._platform
            )
            return self._execute_client_operation(
                connection_type, client, operation_name, operation
            )
        except Exception:
            return AgentUtils.agent_response_for_last_exception(
                status_code=500, client=client
            )

    def _execute_client_operation(
        self,
        connection_type: str,
        client: Any,
        operation_name: str,
        operation: AgentOperation,
    ) -> AgentResponse:
        logger.info(
            f"Executing commands: {connection_type}/{operation_name}",
            extra=self._logging_utils.build_extra(
                operation.trace_id,
                operation_name,
                operation.to_dict(),
            ),
        )
        result = self._execute(client, operation)
        return AgentResponse(result or {}, 200, operation.trace_id)

    @staticmethod
    def _execute(client: Any, operation: AgentOperation) -> Optional[Any]:
        context = {
            CONTEXT_VAR_CLIENT: client,
        }
        context[CONTEXT_VAR_UTILS] = OperationUtils(context)

        return AgentEvaluationUtils.execute(context, operation.commands)
