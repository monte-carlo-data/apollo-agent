import logging
import os
import sys
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional

from apollo.agent.env_vars import HEALTH_ENV_VARS, IS_REMOTE_UPGRADABLE_ENV_VAR
from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.log_context import AgentLogContext
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.constants import (
    CONTEXT_VAR_UTILS,
    CONTEXT_VAR_CLIENT,
    PLATFORM_GENERIC,
    LOG_ATTRIBUTE_TRACE_ID,
    LOG_ATTRIBUTE_OPERATION_NAME,
)
from apollo.agent.operation_utils import OperationUtils
from apollo.agent.models import (
    AgentOperation,
    AgentHealthInformation,
    AgentConfigurationError,
)
from apollo.agent.proxy_client_factory import ProxyClientFactory
from apollo.agent.settings import VERSION, BUILD_NUMBER
from apollo.agent.updater import AgentUpdater
from apollo.agent.utils import AgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.interfaces.agent_response import AgentResponse
from apollo.interfaces.cloudrun.metadata_service import GCP_PLATFORM_INFO_KEY_IMAGE
from apollo.validators.validate_network import ValidateNetwork

logger = logging.getLogger(__name__)


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils
        self._platform = PLATFORM_GENERIC
        self._platform_info = {}
        self._updater: Optional[AgentUpdater] = None
        self._log_context: Optional[AgentLogContext] = None

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

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return self._updater

    @updater.setter
    def updater(self, updater: Optional[AgentUpdater]):
        self._updater = updater

    @property
    def log_context(self) -> Optional[AgentLogContext]:
        return self._log_context

    @log_context.setter
    def log_context(self, log_context: Optional[AgentLogContext]):
        self._log_context = log_context

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
        with self._inject_log_context("health_information", trace_id):
            logger.info(
                "Health information request received",
                extra=self._logging_utils.build_extra(
                    trace_id=trace_id,
                    operation_name="health_information",
                ),
            )
            if self._updater:
                if self._platform_info is None:
                    self._platform_info = {}
                self._platform_info[
                    GCP_PLATFORM_INFO_KEY_IMAGE
                ] = self._updater.get_current_image(self._platform_info)

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
        with self._inject_log_context("test_network_open", trace_id):
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
        with self._inject_log_context("test_network_telnet", trace_id):
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

    def update(
        self,
        trace_id: Optional[str],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> AgentResponse:
        """
        Updates the agent to the specified image, see AgentUpdater for more information about the supported
        parameters.
        This method checks if there's an agent updater installed in `agent.updater` property and that
        the env var `MCD_AGENT_IS_REMOTE_UPGRADABLE` is set to `true`.
        The returned response is a dictionary returned by the agent updater implementation.
        """
        with self._inject_log_context("update", trace_id):
            try:
                result = self._perform_update(
                    trace_id=trace_id,
                    image=image,
                    timeout_seconds=timeout_seconds,
                    **kwargs,
                )
                return AgentUtils.agent_ok_response(result, trace_id)
            except Exception:
                return AgentUtils.agent_response_for_last_exception("Update failed:")

    def _perform_update(
        self,
        trace_id: Optional[str],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        if not self._updater:
            raise AgentConfigurationError("No updater configured")

        upgradable = os.getenv(IS_REMOTE_UPGRADABLE_ENV_VAR, "false").lower() == "true"
        if not upgradable:
            raise AgentConfigurationError("Remote upgrades are disabled for this agent")

        log_payload = self._logging_utils.build_extra(
            trace_id=trace_id,
            operation_name="update",
            extra={"timeout": timeout_seconds, **kwargs},
        )
        logger.info(
            "Update requested",
            extra=log_payload,
        )

        update_result: Dict
        try:
            update_result = self._updater.update(
                platform_info=self._platform_info,
                image=image,
                timeout_seconds=timeout_seconds,
                **kwargs,
            )
        except Exception:
            logger.exception("Update failed", extra=log_payload)
            raise

        logger.info("Update complete", extra=log_payload)
        return update_result

    @staticmethod
    def _env_dictionary() -> Dict:
        env: Dict[str, Optional[str]] = {
            "sys_version": sys.version,
        }
        env.update(
            {
                env_var: os.getenv(env_var)
                for env_var in HEALTH_ENV_VARS
                if os.getenv(env_var)
            }
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
                prefix="Failed to read operation:", status_code=400
            )

        with self._inject_log_context(
            f"{connection_type}/{operation_name}", operation.trace_id
        ):
            response: Optional[AgentResponse] = None
            client: Optional[BaseProxyClient] = None
            try:
                client = ProxyClientFactory.get_proxy_client(
                    connection_type, credentials, operation.skip_cache, self._platform
                )
                response = self._execute_client_operation(
                    connection_type, client, operation_name, operation
                )
                return response
            except Exception:  # noqa
                return AgentUtils.agent_response_for_last_exception(client=client)
            finally:
                # discard clients that raised exceptions, clients like Redshift keep failing after an error
                if (response is None or response.is_error) and not operation.skip_cache:
                    ProxyClientFactory.dispose_proxy_client(
                        connection_type, credentials, operation.skip_cache
                    )

    def _execute_client_operation(
        self,
        connection_type: str,
        client: BaseProxyClient,
        operation_name: str,
        operation: AgentOperation,
    ) -> AgentResponse:
        start_time = time.time()
        logger.info(
            f"Executing operation: {connection_type}/{operation_name}",
            extra=self._logging_utils.build_extra(
                operation.trace_id,
                operation_name,
                client.log_payload(operation),
            ),
        )

        result = self._execute(client, self._logging_utils, operation_name, operation)
        logger.debug(
            f"Operation executed: {connection_type}/{operation_name}",
            extra=self._logging_utils.build_extra(
                operation.trace_id,
                operation_name,
                dict(elapsed_time=time.time() - start_time),
            ),
        )
        return AgentResponse(result or {}, 200, operation.trace_id)

    @staticmethod
    def _execute(
        client: BaseProxyClient,
        logging_utils: LoggingUtils,
        operation_name: str,
        operation: AgentOperation,
    ) -> Optional[Any]:
        context: Dict[str, Any] = {
            CONTEXT_VAR_CLIENT: client,
        }
        context[CONTEXT_VAR_UTILS] = OperationUtils(context)

        return AgentEvaluationUtils.execute(
            context,
            logging_utils,
            operation_name,
            operation.commands,
            operation.trace_id,
        )

    @contextmanager
    def _inject_log_context(self, operation_name: str, trace_id: Optional[str]):
        if self._log_context:
            context = {
                LOG_ATTRIBUTE_OPERATION_NAME: operation_name,
            }
            if trace_id:
                context[LOG_ATTRIBUTE_TRACE_ID] = trace_id

            self._log_context.set_agent_context(context)
        try:
            yield None
        finally:
            if self._log_context:
                self._log_context.set_agent_context({})
