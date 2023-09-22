import logging
import os
from typing import Any, Dict, Optional

from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentOperation, CONTEXT_VAR_UTILS, CONTEXT_VAR_CLIENT
from apollo.agent.operation_utils import OperationUtils
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.agent.settings import VERSION, BUILD_NUMBER
from apollo.agent.utils import AgentUtils
from apollo.interfaces.agent_response import AgentResponse

logger = logging.getLogger(__name__)


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils
        self._platform = "Generic"
        self._platform_info = {}

    def set_platform_info(self, platform: str, info: Optional[Dict] = None):
        self._platform = platform
        if info:
            self._platform_info = {
                "platform_info": {**info},
            }

    def health_information(self) -> Dict:
        return {
            "version": VERSION,
            "build": BUILD_NUMBER,
            "platform": self._platform,
            "env": {
                "python_version": os.getenv("PYTHON_VERSION", "unknown"),
                "server": os.getenv("SERVER_SOFTWARE", "unknown"),
            },
            **self._platform_info,
        }

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
            return AgentResponse(
                AgentUtils.response_for_error("operation is a required parameter"),
                400,
            )
        try:
            operation = AgentOperation.from_dict(operation_dict)
        except Exception:
            logger.exception("Failed to read operation")
            return AgentResponse(
                AgentUtils.response_for_last_exception("Failed to read operation:"),
                400,
            )

        try:
            client = ProxyClientFactory.get_proxy_client(connection_type, credentials)
            return self._execute_client_operation(
                connection_type, client, operation_name, operation
            )
        except Exception:
            return AgentResponse(AgentUtils.response_for_last_exception(), 500)

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
        return AgentResponse(result or {}, 200)

    @staticmethod
    def _execute(client: Any, operation: AgentOperation) -> Optional[Any]:
        context = {
            CONTEXT_VAR_CLIENT: client,
        }
        context[CONTEXT_VAR_UTILS] = OperationUtils(context)

        return AgentEvaluationUtils.execute(context, operation.commands)
