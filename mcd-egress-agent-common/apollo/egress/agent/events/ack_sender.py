import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from heapq import heappush
from threading import Condition, Thread
from typing import Optional, List, Dict

_CHECK_INTERVAL = 10  # check every 10 seconds if there's an ACK to send
DEFAULT_ACK_INTERVAL_SECONDS = (
    45  # send the ack message if the task was not completed after this time
)
logger = logging.getLogger(__name__)


@dataclass(order=True)
class PendingAckOperation:
    scheduled_time: float
    operation_id: str
    completed: bool = field(init=False, default=False)


class AckSender:
    def __init__(self, interval_seconds: int = DEFAULT_ACK_INTERVAL_SECONDS):
        """
        Sends ACK messages to the backend for received operations, after `interval_seconds`
        seconds, if the task was not completed before that time.
        It starts a thread that wakes up every 10 seconds to check if there are ACKs to send.
        """
        self._interval = interval_seconds
        self._handler: Optional[Callable[[str], None]] = None
        self._condition = Condition()
        self._running = False
        self._queue: List[PendingAckOperation] = []
        self._mapping: Dict[str, PendingAckOperation] = {}

    def start(self, handler: Callable[[str], None]):
        self._handler = handler
        self._running = True
        th = Thread(target=self._run)
        th.start()

    def stop(self):
        with self._condition:
            self._running = False
            self._condition.notify_all()

    def schedule_ack(self, operation_id: str):
        scheduled_time = time.time() + self._interval
        pending_op = PendingAckOperation(
            scheduled_time=scheduled_time, operation_id=operation_id
        )
        with self._condition:
            heappush(self._queue, pending_op)
            self._mapping[operation_id] = pending_op

    def operation_completed(self, operation_id: str):
        with self._condition:
            pending_op = self._mapping.pop(operation_id, None)
            if pending_op:
                pending_op.completed = True

    def _run(self):
        logger.info("ACK sender started")
        while self._running:
            self._run_once()
        logger.info("ACK sender stopped")

    def _run_once(self):
        to_ack_operations = []
        with self._condition:
            while not self._queue and self._running:
                self._condition.wait(_CHECK_INTERVAL)
            if not self._running:
                return
            now = time.time()
            while self._queue and self._queue[0].scheduled_time <= now:
                operation = self._queue.pop(0)
                if self._mapping.pop(operation.operation_id, None):
                    to_ack_operations.append(operation)

        if not self._handler:
            logger.error("No handler defined for ACK sender")
            return
        for operation in to_ack_operations:
            if operation.completed:
                continue
            try:
                logger.info(f"Sending ACK for operation: {operation.operation_id}")
                self._handler(operation.operation_id)
            except Exception as ex:
                logger.exception(
                    f"Failed to send ACK, operation: {operation.operation_id}: {ex}"
                )
