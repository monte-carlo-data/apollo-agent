import logging
import os
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from apollo.agent.env_vars import (
    HEALTH_ENV_VARS,
    IS_REMOTE_UPGRADABLE_ENV_VAR,
    PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_DEFAULT_VALUE,
    PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR,
)
from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.platform import AgentPlatformProvider
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
from apollo.integrations.storage.storage_proxy_client import StorageProxyClient
from apollo.interfaces.agent_response import AgentResponse
from apollo.interfaces.cloudrun.metadata_service import GCP_PLATFORM_INFO_KEY_IMAGE
from apollo.validators.validate_network import ValidateNetwork

logger = logging.getLogger(__name__)


class Agent:
    def __init__(self, logging_utils: LoggingUtils):
        self._logging_utils = logging_utils
        self._platform_provider: Optional[AgentPlatformProvider] = None
        self._log_context: Optional[AgentLogContext] = None

    @property
    def platform(self) -> str:
        """
        The name of the platform running this agent, for example: Generic, AWS, GCP, etc.
        """
        return (
            self._platform_provider.platform
            if self._platform_provider
            else PLATFORM_GENERIC
        )

    @property
    def platform_info(self) -> Optional[Dict]:
        """
        Dictionary containing platform specific information, it could be container information like versions or
        some other settings relevant to the container.
        """
        return (
            self._platform_provider.platform_info if self._platform_provider else None
        )

    @property
    def updater(self) -> Optional[AgentUpdater]:
        return self._platform_provider.updater if self._platform_provider else None

    @property
    def platform_provider(self) -> Optional[AgentPlatformProvider]:
        return self._platform_provider

    @platform_provider.setter
    def platform_provider(self, value: Optional[AgentPlatformProvider]):
        self._platform_provider = value

    @property
    def log_context(self) -> Optional[AgentLogContext]:
        return self._log_context

    @log_context.setter
    def log_context(self, log_context: Optional[AgentLogContext]):
        self._log_context = log_context

    def health_information(
        self, trace_id: Optional[str], full: bool = False
    ) -> AgentHealthInformation:
        """
        Returns platform and environment information about the agent:
        - version
        - build
        - platform
        - env (some relevant env information like `sys.version` or vars like PYTHON_VERSION and MCD_*)
        - specific platform information set using `platform_info` setter
        - the received value for `trace_id` if any
        :param trace_id: The optional trace id to include back in the response.
        :param full: If true extra information like outbound IP address will be included, defaults to false.
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
            platform_info = {**(self.platform_info or {})}
            if self.updater:
                platform_info[
                    GCP_PLATFORM_INFO_KEY_IMAGE
                ] = self.updater.get_current_image()

        return AgentHealthInformation(
            version=VERSION,
            build=BUILD_NUMBER,
            platform=self.platform,
            env=self._env_dictionary(),
            platform_info=platform_info,
            trace_id=trace_id,
            extra=self._extra_health_information() if full else None,
        )

    @staticmethod
    def _extra_health_information():
        return {
            "outbound_ip_address": AgentUtils.get_outbound_ip_address(),
        }

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
    ) -> AgentResponse:
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

    def get_outbound_ip_address(
        self,
        trace_id: Optional[str] = None,
    ) -> AgentResponse:
        """
        Returns the public IP address used by the agent for outbound connections.
        :param trace_id: Optional trace ID received from the client that will be included in the response, if present.
        """
        with self._inject_log_context("get_outbound_ip_address", trace_id):
            logger.info(
                "Get Outbound IP Address request received",
                extra=self._logging_utils.build_extra(
                    trace_id=trace_id,
                    operation_name="get_outbound_ip_address",
                ),
            )
            return AgentResponse(
                {
                    "outbound_ip_address": AgentUtils.get_outbound_ip_address(),
                },
                200,
                trace_id,
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
            except Exception:  # noqa
                return AgentUtils.agent_response_for_last_exception("Update failed:")

    def get_update_logs(
        self,
        trace_id: Optional[str],
        start_time: datetime,
        limit: int,
    ) -> AgentResponse:
        """
        Returns up to `limit` log events from the updater after the given datetime.
        This method checks if there's an agent updater installed in `agent.updater` property and that
        the env var `MCD_AGENT_IS_REMOTE_UPGRADABLE` is set to `true`.
        The returned response is dictionary with an "events" attribute containing the
        list of dictionaries returned by the agent updater implementation.
        """
        with self._inject_log_context("get_update_logs", trace_id):
            try:
                logger.info(
                    "update logs requested",
                    extra=self._logging_utils.build_extra(
                        trace_id=trace_id,
                        operation_name="get_update_logs",
                        extra={
                            "start_time": start_time.isoformat(),
                            "limit": limit,
                        },
                    ),
                )
                updater = self._check_updater()
                events = updater.get_update_logs(
                    start_time=start_time,
                    limit=limit,
                )
                return AgentUtils.agent_ok_response(
                    {
                        "events": events,
                    },
                    trace_id,
                )
            except Exception:  # noqa
                return AgentUtils.agent_response_for_last_exception(
                    "get_update_logs failed:"
                )

    def get_infra_details(self, trace_id: Optional[str]) -> AgentResponse:
        """
        Returns the infrastructure details returned by the `infra_provider` set on this agent.
        An error is returned if no infra_provider is set.
        """
        with self._inject_log_context("get_infra_details", trace_id):
            try:
                logger.info("infra_details requested")
                if not self._platform_provider:
                    raise AgentConfigurationError("No platform_provider set")
                details = self._platform_provider.get_infra_details()
                return AgentUtils.agent_ok_response(details, trace_id)
            except Exception:  # noqa
                return AgentUtils.agent_response_for_last_exception(
                    "get_infra_details failed:"
                )

    def _check_updater(self) -> AgentUpdater:
        if not self.updater:
            raise AgentConfigurationError("No updater configured")
        return self.updater

    def _perform_update(
        self,
        trace_id: Optional[str],
        image: Optional[str],
        timeout_seconds: Optional[int],
        **kwargs,  # type: ignore
    ) -> Dict:
        updater = self._check_updater()
        upgradable = os.getenv(IS_REMOTE_UPGRADABLE_ENV_VAR, "false").lower() == "true"
        if not upgradable:
            raise AgentConfigurationError("Remote upgrades are disabled for this agent")

        log_payload = self._logging_utils.build_extra(
            trace_id=trace_id,
            operation_name="update",
            extra={"timeout": timeout_seconds, "image": image, **kwargs},
        )
        logger.info(
            "Update requested",
            extra=log_payload,
        )

        update_result: Dict
        try:
            update_result = updater.update(
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
        :param operation_dict: the required dictionary containing the definition of the operation to run,
            if None an error will be raised.
        :param credentials: the optional credentials dictionary
        :return: the result of executing the given operation
        """
        if not operation_dict:
            return AgentUtils.agent_response_for_error(
                "operation is a required parameter", status_code=400
            )
        try:
            operation = AgentOperation.from_dict(operation_dict)
        except Exception:  # noqa
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
                    connection_type, credentials, operation.skip_cache, self.platform
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
        response = AgentResponse(result or {}, 200, operation.trace_id)
        if operation.can_use_pre_signed_url():
            size = response.calculate_result_size()
            if operation.must_use_pre_signed_url(size):
                key = f"responses/{operation.trace_id}"
                storage_client = StorageProxyClient(self.platform)
                storage_client.write(
                    key=key,
                    obj_to_write=response.serialize_result(
                        unwrap_result=operation.must_unwrap_result()
                    ),
                )
                expiration_seconds = int(
                    os.getenv(
                        PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_ENV_VAR,
                        PRE_SIGNED_URL_RESPONSE_EXPIRATION_SECONDS_DEFAULT_VALUE,
                    )
                )
                url = storage_client.generate_presigned_url(key, expiration_seconds)
                response.use_location(url)
        return response

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
