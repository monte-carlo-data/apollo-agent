import datetime
import logging
from threading import Thread, Condition
from typing import Callable, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)


class HeartbeatChecker:
    """
    This class is responsible for monitoring the heartbeat from the server and call the handler
    when the heartbeat is missing after the configured inactivity timeout.
    """

    def __init__(
        self,
        heartbeat_missing_handler: Callable[[], None],
        inactivity_timeout_seconds: int = 120,
    ):
        self._inactivity_timeout_seconds = inactivity_timeout_seconds
        self._handler = heartbeat_missing_handler
        self._condition = Condition()
        self._last_heartbeat = datetime.datetime.now()
        self._current_loop_id: Optional[str] = None

    def start(self):
        self._last_heartbeat = datetime.datetime.now()

        # current_loop_id is used to stop the current loop when a new one is started
        # it might take some time to stop the current loop, so a single "running" flag is not
        # enough
        loop_id = str(uuid4())
        self._current_loop_id = loop_id

        th = Thread(target=self._run_heartbeat_checker, args=(loop_id,))
        th.start()

    def stop(self):
        self._current_loop_id = None
        self.heartbeat_received()  # wake up the thread to stop running

    def heartbeat_received(self):
        with self._condition:
            self._last_heartbeat = datetime.datetime.now()
            self._condition.notify_all()

    def _is_current_loop(self, loop_id: str):
        return self._current_loop_id == loop_id

    def _run_heartbeat_checker(self, loop_id: str):
        logger.info("Heartbeat monitor started %s", loop_id)
        while self._is_current_loop(loop_id):
            with self._condition:
                self._condition.wait(timeout=self._inactivity_timeout_seconds / 2)
                elapsed_time = datetime.datetime.now() - self._last_heartbeat
            if not self._is_current_loop(loop_id):
                break
            if elapsed_time.total_seconds() > self._inactivity_timeout_seconds:
                logger.error("Heartbeat timeout %s", loop_id)
                self._handler()
        logger.info("Heartbeat monitor stopped, %s", loop_id)
