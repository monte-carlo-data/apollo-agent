import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dataclasses_json import dataclass_json, config

from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.models import AgentOperation, CONTEXT_VAR_UTILS, CONTEXT_VAR_CLIENT
from apollo.agent.operation_utils import OperationUtils
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.agent.settings import VERSION, BUILD_NUMBER
from apollo.agent.utils import AgentUtils
from apollo.interfaces.agent_response import AgentResponse

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


# used so we don't include an empty platform info
def _exclude_empty_values(value: Any) -> bool:
    return not bool(value)


@dataclass_json
@dataclass
class AgentHealthInformation:
    platform: str
    version: str
    build: str
    env: Dict
    platform_info: Optional[Dict] = field(
        metadata=config(exclude=_exclude_empty_values), default=None
    )

    def to_dict(self) -> Dict:
        pass


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils
        self._platform = "Generic"
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

    def health_information(self) -> AgentHealthInformation:
        """
        Returns platform and environment information about the agent:
        - version
        - build
        - platform
        - env (some relevant env information like sys.version or vars like PYTHON_VERSION and MCD_*)
        - specific platform information set using `platform_info` setter
        :return: an `AgentHealthInformation` object that can be converted to JSON.
        """
        return AgentHealthInformation(
            version=VERSION,
            build=BUILD_NUMBER,
            platform=self._platform,
            env=self._env_dictionary(),
            platform_info=self._platform_info,
        )

    @staticmethod
    def _env_dictionary() -> Dict:
        env = {
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
            client = ProxyClientFactory.get_proxy_client(
                connection_type, credentials, operation.skip_cache
            )
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
