import logging
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Tuple, Optional, Any, Callable

from apollo.egress.agent.backend.backend_client import BackendClient
from apollo.egress.agent.events.ack_sender import (
    AckSender,
    DEFAULT_ACK_INTERVAL_SECONDS,
)
from apollo.egress.agent.events.events_client import EventsClient
from apollo.egress.agent.events.sse_client_receiver import SSEClientReceiver
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.config.config_keys import (
    CONFIG_OPS_RUNNER_THREAD_COUNT,
    CONFIG_PUBLISHER_THREAD_COUNT,
    CONFIG_IS_REMOTE_UPGRADABLE,
    CONFIG_ACK_INTERVAL_SECONDS,
    CONFIG_PUSH_LOGS_INTERVAL_SECONDS,
)
from apollo.egress.agent.service.logs_service import LogsService
from apollo.egress.agent.service.metrics_service import MetricsService
from apollo.egress.agent.service.operation_result import (
    AgentOperationResult,
    OperationAttributes,
)
from apollo.egress.agent.service.operations_runner import Operation, OperationsRunner
from apollo.egress.agent.service.results_processor import ResultsProcessor
from apollo.egress.agent.service.results_publisher import ResultsPublisher
from apollo.egress.agent.service.timer_service import TimerService
from apollo.egress.agent.service.storage_service import StorageService
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_TRACE_ID,
)
from apollo.common.agent.serde import (
    decode_dictionary,
)
from apollo.common.agent.settings import VERSION, BUILD_NUMBER
from apollo.egress.agent.utils import utils
from apollo.egress.agent.utils.result_utils import ResultUtils
from apollo.egress.agent.utils.utils import (
    BACKEND_SERVICE_URL,
    get_mc_login_token,
    X_MCD_ID,
)

logger = logging.getLogger(__name__)

_ATTR_NAME_OPERATION = "operation"
_ATTR_NAME_OPERATION_ID = "operation_id"
_ATTR_NAME_OPERATION_TYPE = "type"
_ATTR_NAME_PATH = "path"
_ATTR_NAME_TRACE_ID = "trace_id"
_ATTR_NAME_LIMIT = "limit"
_ATTR_NAME_QUERY = "query"
_ATTR_NAME_TIMEOUT = "timeout"
_ATTR_NAME_COMPRESS_RESPONSE_FILE = "compress_response_file"
_ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES = "response_size_limit_bytes"
_ATTR_NAME_EVENTS = "events"
_ATTR_NAME_PARAMETERS = "parameters"
_ATTR_NAME_CONFIG = "config"
_ATTR_NAME_ENV = "env"
_ATTR_NAME_KEY_ID = "authentication_key_id"
_ATTR_NAME_JOB_TYPE = "job_type"

_ATTR_NAME_SIZE_EXCEEDED = "__mcd_size_exceeded__"

_ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY = "snowflake_query"
_ATTR_OPERATION_TYPE_SNOWFLAKE_TEST = "snowflake_connection_test"
_ATTR_OPERATION_TYPE_PUSH_METRICS = "push_metrics"
_PATH_PUSH_METRICS = "push_metrics"
_PATH_PUSH_LOGS = "push_logs"

_DEFAULT_COMPRESS_RESPONSE_FILE = True
_DEFAULT_RESPONSE_SIZE_LIMIT_BYTES = (
    20000000  # 20Mb, the same default value we have on the DC side for Snowflake agents
)

_ENV_NAME_IS_REMOTE_UPGRADABLE = "MCD_AGENT_IS_REMOTE_UPGRADABLE"


class OperationMatchingType(Enum):
    EQUALS = "equals"
    STARTS_WITH = "starts_with"


@dataclass
class OperationMapping:
    path: str
    method: Callable[[str, Dict[str, Any]], None]
    schedule: bool = False
    matching_type: OperationMatchingType = OperationMatchingType.EQUALS


class EgressAgentError(Exception):
    pass


class BaseEgressAgentService:
    """
    Base Egress Agent Service, it opens a connection to the Monte Carlo backend
    (using the token provided through configuration) and waits for events including
    agent operations to execute.
    By default, operations are received from the MC backend using a SSE (Server-sent events)
    connection, but new implementations (polling, gRPC, websockets, etc.) can be implemented by
    adding new "receivers" (see ReceiverFactory and BaseReceiver).
    Operations are processed by a pool of background threads (see OperationsRunner) and executed
    asynchronously.
    When the result is ready we send it to the MC backend using another background thread (see
    ResultsPublisher).
    """

    def __init__(
        self,
        config_manager: ConfigurationManager,
        logs_service: LogsService,
        storage_service: StorageService,
        ops_runner: Optional[OperationsRunner] = None,
        results_publisher: Optional[ResultsPublisher] = None,
        events_client: Optional[EventsClient] = None,
        ack_sender: Optional[AckSender] = None,
        logs_sender: Optional[TimerService] = None,
    ):
        self._config_manager = config_manager
        self._ops_runner = ops_runner or OperationsRunner(
            handler=self._execute_scheduled_operation,
            thread_count=config_manager.get_int_value(
                CONFIG_OPS_RUNNER_THREAD_COUNT, 1
            ),
        )
        self._results_publisher = results_publisher or ResultsPublisher(
            handler=self._push_results,
            thread_count=config_manager.get_int_value(CONFIG_PUBLISHER_THREAD_COUNT, 1),
        )
        self._ack_sender = ack_sender or AckSender(
            interval_seconds=config_manager.get_int_value(
                CONFIG_ACK_INTERVAL_SECONDS, DEFAULT_ACK_INTERVAL_SECONDS
            )
        )
        self._logs_service = logs_service
        self._logs_sender = logs_sender or TimerService(
            name="Logs sender",
            interval_seconds=config_manager.get_int_value(
                CONFIG_PUSH_LOGS_INTERVAL_SECONDS, 300
            ),
        )
        self._storage = storage_service
        self._results_processor = ResultsProcessor(
            config_manager=self._config_manager,
            storage=self._storage,
        )

        self._events_client = events_client or EventsClient(
            receiver=SSEClientReceiver(base_url=BACKEND_SERVICE_URL),
        )
        self._operations_mapping = [
            OperationMapping(
                path="/api/v1/agent/execute/storage",
                matching_type=OperationMatchingType.STARTS_WITH,
                method=self._execute_storage_operation,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/test/health",
                method=self._execute_health,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/logs",
                method=self._execute_get_logs,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/snowflake/metrics",
                method=self._execute_get_metrics,
                schedule=True,
            ),
            OperationMapping(
                path=_PATH_PUSH_METRICS,
                method=self._execute_push_metrics,
                schedule=True,
            ),
            OperationMapping(
                path=_PATH_PUSH_LOGS,
                method=self._execute_push_logs,
                schedule=True,
            ),
            OperationMapping(
                path="/api/v1/upgrade",
                method=self._execute_upgrade,
                schedule=True,
            ),
        ]

    def start(self):
        self._ops_runner.start()
        self._results_publisher.start()
        self._events_client.start(handler=self._event_handler)
        self._ack_sender.start(handler=self._send_ack)
        self._logs_sender.start(handler=self._push_logs)

        logger.info(f"SNA Service Started: v{VERSION} (build #{BUILD_NUMBER})")

    def stop(self):
        self._ops_runner.stop()
        self._results_publisher.stop()
        self._events_client.stop()
        self._ack_sender.stop()
        self._logs_sender.stop()

    def health_information(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        health_info = utils.health_information(trace_id)
        health_info[_ATTR_NAME_PARAMETERS] = self._config_manager.get_all_values()
        # update env to include the same env var other agent platforms use to report if they are remote upgradable
        health_info[_ATTR_NAME_ENV][_ENV_NAME_IS_REMOTE_UPGRADABLE] = (
            "true"
            if self._config_manager.get_bool_value(CONFIG_IS_REMOTE_UPGRADABLE, True)
            else "false"
        )
        health_info[_ATTR_NAME_KEY_ID] = get_mc_login_token().get(X_MCD_ID)
        return health_info

    def run_reachability_test(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        trace_id = trace_id or str(uuid.uuid4())
        logger.info(f"Running reachability test, trace_id: {trace_id}")
        return BackendClient.execute_operation(f"/api/v1/test/ping?trace_id={trace_id}")

    def query_completed(self, operation_json: str, query_id: str):
        """
        Invoked by the Snowflake stored procedure when a query execution is completed
        """
        operation_attributes = OperationAttributes.from_json(operation_json)
        operation_id = operation_attributes.operation_id
        logger.info(f"Query completed: {operation_id}, query_id: {query_id}")
        self._schedule_push_results_for_query(
            operation_id, query_id, operation_attributes
        )

    def _event_handler(self, event: Dict[str, Any]):
        """
        Invoked by events client when an event is received with an agent operation to run
        """
        operation_id = event.get(_ATTR_NAME_OPERATION_ID)
        if operation_id:
            path: str = event.get(_ATTR_NAME_PATH, "")
            if path:
                logger.info(
                    f"Received agent operation: {path}, operation_id: {operation_id}"
                )
                self._ack_sender.schedule_ack(operation_id)
                self._execute_operation(path, operation_id, event)
        elif op_type := (event.get(_ATTR_NAME_OPERATION_TYPE)):
            if op_type == _ATTR_OPERATION_TYPE_PUSH_METRICS:
                self._push_metrics()

    def _execute_operation(self, path: str, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        if operation.get(_ATTR_NAME_SIZE_EXCEEDED, False):
            logger.info("Downloading operation from orchestrator")
            event[_ATTR_NAME_OPERATION] = BackendClient.download_operation(operation_id)

        method, schedule = self._resolve_operation_method(path)
        if schedule:
            self._schedule_operation(operation_id, event)
        elif method:
            method(operation_id, event)
        else:
            logger.error(f"Invalid path received: {path}, operation_id: {operation_id}")

    def _resolve_operation_method(
        self,
        path: str,
    ) -> Tuple[Optional[Callable[[str, Dict[str, Any]], None]], bool]:
        for op in self._operations_mapping:
            if op.matching_type == OperationMatchingType.EQUALS:
                if path == op.path:
                    return op.method, op.schedule
            elif op.matching_type == OperationMatchingType.STARTS_WITH:
                if path.startswith(op.path):
                    return op.method, op.schedule
            else:
                raise ValueError(f"Invalid matching type: {op.matching_type}")
        return None, False

    def _execute_storage_operation(self, operation_id: str, event: Dict[str, Any]):
        result = self._storage.execute_operation(decode_dictionary(event))
        self._schedule_push_results(operation_id, result)

    def _execute_health(self, operation_id: str, event: Dict[str, Any]):
        try:
            trace_id = event.get(_ATTR_NAME_OPERATION, {}).get(
                _ATTR_NAME_TRACE_ID, operation_id
            )
            health_information = self.health_information(trace_id=trace_id)
            self._schedule_push_results(operation_id, health_information)
        except Exception as ex:
            self._schedule_push_results(
                operation_id, ResultUtils.result_for_exception(ex)
            )

    def _schedule_operation(self, operation_id: str, event: Dict[str, Any]):
        self._ops_runner.schedule(Operation(operation_id, event))

    def _execute_scheduled_operation(self, op: Operation):
        method, _ = self._resolve_operation_method(op.event.get(_ATTR_NAME_PATH, ""))
        if method:
            method(op.operation_id, op.event)
        else:
            logger.error(
                f"No method mapped to operation path: {op.event.get(_ATTR_NAME_PATH)}"
            )
            self._schedule_push_results(
                op.operation_id,
                ResultUtils.result_for_error_message(
                    f"Unsupported operation path: {op.event.get(_ATTR_NAME_PATH)}"
                ),
            )

    def _execute_get_logs(self, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
        limit = operation.get(_ATTR_NAME_LIMIT) or 1000
        try:
            self._schedule_push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        _ATTR_NAME_EVENTS: self._logs_service.get_logs(limit),
                    },
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, ResultUtils.result_for_exception(ex)
            )

    def _execute_get_metrics(self, operation_id: str, event: Dict[str, Any]):
        operation = event.get(_ATTR_NAME_OPERATION, {})
        trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
        try:
            self._schedule_push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: MetricsService.fetch_metrics(),
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, ResultUtils.result_for_exception(ex)
            )

    def _push_metrics(self):
        self._schedule_operation(
            _PATH_PUSH_METRICS, {_ATTR_NAME_PATH: _PATH_PUSH_METRICS}
        )

    def _execute_push_metrics(self, operation_id: str, event: Dict[str, Any]):
        payload = {
            "format": "prometheus",
            "metrics": MetricsService.fetch_metrics(),
        }
        BackendClient.execute_operation("/api/v1/agent/metrics", "POST", payload)

    def _push_logs(self):
        self._schedule_operation(_PATH_PUSH_LOGS, {_ATTR_NAME_PATH: _PATH_PUSH_LOGS})

    def _execute_push_logs(self, operation_id: str, event: Dict[str, Any]):
        payload = {
            "logs": self._logs_service.get_logs(int(event.get(_ATTR_NAME_LIMIT, 1000))),
        }
        logger.info(f"Pushing {len(payload['logs'])} logs")
        BackendClient.execute_operation("/api/v1/agent/logs", "POST", payload)

    def _execute_upgrade(self, operation_id: str, event: Dict[str, Any]):
        """
        Compatible with /api/v1/upgrade operation from other platforms.
        It updates the configuration if there are parameters under operation and restarts the
        service.
        """
        try:
            if not self._config_manager.get_bool_value(
                CONFIG_IS_REMOTE_UPGRADABLE, True
            ):
                raise EgressAgentError("Remote upgrades are disabled")
            operation = event.get(_ATTR_NAME_OPERATION, {})
            updates = operation.get(_ATTR_NAME_PARAMETERS, {})
            trace_id = operation.get(_ATTR_NAME_TRACE_ID, operation_id)
            if updates:
                self._config_manager.set_values(updates)
            self._restart_service()
            BackendClient.push_results(
                operation_id,
                {
                    ATTRIBUTE_NAME_RESULT: {
                        "updated": True,
                    },
                    ATTRIBUTE_NAME_TRACE_ID: trace_id,
                },
            )
        except Exception as ex:
            self._schedule_push_results(
                operation_id, ResultUtils.result_for_exception(ex)
            )

    @classmethod
    def _get_query_from_event(
        cls,
        event: Dict,
    ) -> Tuple[Optional[str], Optional[int], Optional[OperationAttributes]]:
        operation = event.get(_ATTR_NAME_OPERATION, {})
        operation_type = operation.get(_ATTR_NAME_OPERATION_TYPE)
        operation_id = event.get(_ATTR_NAME_OPERATION_ID)
        if operation_id and operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_QUERY:
            return (
                operation.get(_ATTR_NAME_QUERY),
                operation.get(_ATTR_NAME_TIMEOUT),
                OperationAttributes(
                    operation_id=operation_id,
                    compress_response_file=operation.get(
                        _ATTR_NAME_COMPRESS_RESPONSE_FILE,
                        _DEFAULT_COMPRESS_RESPONSE_FILE,
                    ),
                    response_size_limit_bytes=operation.get(
                        _ATTR_NAME_RESPONSE_SIZE_LIMIT_BYTES,
                        _DEFAULT_RESPONSE_SIZE_LIMIT_BYTES,
                    ),
                    job_type=operation.get(_ATTR_NAME_JOB_TYPE),
                    trace_id=operation.get(_ATTR_NAME_TRACE_ID) or str(uuid.uuid4()),
                ),
            )
        elif operation_type == _ATTR_OPERATION_TYPE_SNOWFLAKE_TEST:
            return None, None, None
        else:
            raise ValueError(f"Invalid operation type: {operation_type}")

    def _send_ack(self, operation_id: str):
        logger.info(f"Sending ACK for operation={operation_id}")
        BackendClient.execute_operation(
            f"/api/v1/agent/operations/{operation_id}/ack", "POST"
        )

    def _schedule_push_results_for_query(
        self,
        operation_id: str,
        query_id: str,
        operation_attrs: OperationAttributes,
    ):
        self._results_publisher.schedule_push_query_results(
            operation_id, query_id, operation_attrs
        )

    def _schedule_push_results(
        self,
        operation_id: str,
        result: Dict[str, Any],
        operation_attrs: Optional[OperationAttributes] = None,
    ):
        self._results_publisher.schedule_push_results(
            operation_id=operation_id,
            result=result,
            operation_attrs=operation_attrs,
        )

    def _push_results(self, result: AgentOperationResult):
        self._ack_sender.operation_completed(result.operation_id)
        if result.query_id and result.operation_attrs is not None:
            logger.error("Push results for query not implemented")
        elif result.result is not None:
            self._push_backend_results(
                result.operation_id, result.result, result.operation_attrs
            )
        else:
            logger.error(f"Invalid result for operation: {result.operation_id}")

    def _restart_service(self):
        logger.error("Restart service not implemented")

    def _push_backend_results(
        self,
        operation_id: str,
        result: Dict[str, Any],
        operation_attrs: Optional[OperationAttributes],
    ):
        if operation_attrs:
            if not _ATTR_NAME_TRACE_ID in result:
                result[ATTRIBUTE_NAME_TRACE_ID] = operation_attrs.trace_id
            result = self._results_processor.process_result(result, operation_attrs)
        BackendClient.push_results(operation_id, result)
