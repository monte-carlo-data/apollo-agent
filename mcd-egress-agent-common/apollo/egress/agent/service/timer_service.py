import logging
from threading import Thread, Condition
from typing import Optional, Callable

logger = logging.getLogger(__name__)


class TimerService:
    def __init__(self, name: str, interval_seconds: int):
        """
        Invokes the handler passed in `start` every `interval_seconds` seconds
        """
        self._name = name
        self._interval = interval_seconds
        self._handler: Optional[Callable[[], None]] = None
        self._condition = Condition()
        self._running = False

    def start(self, handler: Callable[[], None]):
        self._handler = handler
        self._running = True
        th = Thread(target=self._run)
        th.start()

    def stop(self):
        with self._condition:
            self._running = False
            self._condition.notify_all()

    def _run(self):
        logger.info("%s started", self._name)
        while self._running:
            with self._condition:
                self._condition.wait(self._interval)
            if not self._running:
                break
            if not self._handler:
                logger.error("No handler defined for %s", self._name)
                return
            try:
                self._handler()
            except Exception as ex:
                logger.exception("Failed to run %s operation: %s", self._name, ex)
        logger.info("%s stopped", self._name)
