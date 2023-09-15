import logging
from typing import Any, Dict, Optional

from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentOperation, AgentOperationResponse
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.agent.settings import VERSION, BUILD_NUMBER
from apollo.agent.utils import AgentUtils

logger = logging.getLogger(__name__)


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils

    @staticmethod
    def health_information() -> Dict:
        return {
            "version": VERSION,
            "build": BUILD_NUMBER,
        }

    def execute_operation(
        self,
        connection_type: str,
        operation_name: str,
        operation_dict: Optional[Dict],
        credentials: Optional[Dict],
    ) -> AgentOperationResponse:
        if not operation_dict:
            return AgentOperationResponse(
                AgentUtils.response_for_error("operation is a required parameter"),
                400,
            )
        try:
            operation_dict["operation_name"] = operation_name
            operation = AgentOperation.from_dict(operation_dict)
        except Exception:
            logger.exception("Failed to read operation")
            return AgentOperationResponse(
                AgentUtils.response_for_last_exception("Failed to read operation:"),
                400,
            )

        try:
            client = ProxyClientFactory.get_proxy_client(connection_type, credentials)
            return self._execute_commands(connection_type, client, operation)
        except Exception:
            return AgentOperationResponse(AgentUtils.response_for_last_exception(), 500)

    def _execute_commands(
        self,
        connection_type: str,
        client: Any,
        operation: AgentOperation,
    ) -> AgentOperationResponse:
        logger.info(
            f"Executing {connection_type} commands",
            extra=self._logging_utils.build_extra(
                operation.trace_id,
                operation.to_dict(),
            ),
        )
        result = self._execute(client, operation)
        return AgentOperationResponse(result, 200)

    @staticmethod
    def _execute(client: Any, operation: AgentOperation) -> Optional[Any]:
        context = {
            "_client": client,
        }
        return AgentEvaluationUtils.execute(context, operation.commands)
