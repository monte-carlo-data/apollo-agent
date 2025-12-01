import logging
from typing import Callable, Any, Dict, Optional

from apollo.egress.agent.service.operation_result import (
    AgentOperationResult,
    OperationAttributes,
)
from apollo.egress.agent.utils.queue_async_processor import QueueAsyncProcessor

logger = logging.getLogger(__name__)


class ResultsPublisher(QueueAsyncProcessor[AgentOperationResult]):
    """
    This class is responsible for processing results to be sent to the backend.
    Currently, it uses a queue and a the given number of threads to publish them, the handler
    is used to send the results.
    """

    def __init__(
        self, handler: Callable[[AgentOperationResult], None], thread_count: int = 1
    ):
        self._results_handler = handler
        super().__init__(
            name="ResultsPublisher",
            handler=self._handler_wrapper,
            thread_count=thread_count,
        )

    def schedule_push_query_results(
        self, operation_id: str, query_id: str, operation_attrs: OperationAttributes
    ):
        self.schedule(
            AgentOperationResult(
                operation_id=operation_id,
                query_id=query_id,
                operation_attrs=operation_attrs,
            )
        )

    def schedule_push_results(
        self,
        operation_id: str,
        result: Dict[str, Any],
        operation_attrs: Optional[OperationAttributes] = None,
    ):
        self.schedule(
            AgentOperationResult(
                operation_id=operation_id,
                result=result,
                operation_attrs=operation_attrs,
            )
        )

    def _handler_wrapper(self, result: AgentOperationResult):
        if result.query_id:
            logger.info(
                f"Running results push, operation: {result.operation_id}, query_id: {result.query_id}"
            )
        else:
            logger.info(f"Running results push, operation: {result.operation_id}")
        self._results_handler(result)
