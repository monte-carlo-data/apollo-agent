import logging
from threading import Condition, Thread
from typing import Callable, List, TypeVar, Generic, Any

logger = logging.getLogger(__name__)

T = TypeVar("T")


class QueueAsyncProcessor(Generic[T]):
    """
    Base class used to process operations in a queue asynchronously.
    It is configured with the handler that will be called for each operation in the queue,
    and the number of threads used to execute them in parallel.
    """

    def __init__(self, name: str, handler: Callable[[T], None], thread_count: int):
        self._name = name
        self._handler = handler

        self._condition = Condition()
        self._queue: List[T] = []
        self._running = False
        self._thread_count = max(1, thread_count)

    def start(self):
        self._running = True
        for thread_number in range(self._thread_count):
            th = Thread(target=self._run, args=(thread_number,))
            th.start()

    def stop(self):
        with self._condition:
            self._running = False
            self._condition.notify_all()

    def schedule(self, o: T):
        with self._condition:
            self._queue.append(o)
            self._condition.notify_all()

    def _run(self, thread_number: int):
        thread_name = (
            f"{self._name}"
            if self._thread_count == 1
            else f"{self._name} #{thread_number}"
        )
        logger.info(f"{thread_name} started")
        while self._running:
            with self._condition:
                while not self._queue and self._running:
                    self._condition.wait()
                if not self._running:
                    break
                to_execute = self._queue.pop(0)

            self._invoke_handler(thread_name, to_execute)
        logger.info(f"{thread_name}: stopped")

    def _invoke_handler(self, thread_name: str, param: Any):
        try:
            logger.info(f"{thread_name}: running operation")
            self._handler(param)
            logger.info(f"{thread_name}: completed operation")
        except Exception as ex:
            logger.exception(f"{thread_name}: Failed to run operation: {ex}")
