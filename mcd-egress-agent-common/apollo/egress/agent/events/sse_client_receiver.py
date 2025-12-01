import json
import logging
from threading import Thread
from typing import Callable, Dict, Optional
from urllib.parse import urljoin
from uuid import uuid4

import sseclient
from retry import retry

from apollo.egress.agent.events.base_receiver import BaseReceiver
from apollo.egress.agent.utils.utils import get_mc_login_token, X_MCD_ID

logger = logging.getLogger(__name__)


class SSEConnectionFailed(Exception):
    pass


class SSEClientReceiver(BaseReceiver):
    """
    Receiver that uses SSE (Server-sent events) to listen for events from the server and
    call the handler function when an event is received.
    """

    def __init__(
        self,
        base_url: str,
    ):
        self._current_loop_id: Optional[str] = None
        self._base_url = base_url
        self._sse_client: Optional[sseclient.SSEClient] = None
        self._event_handler: Optional[Callable[[Dict], None]] = None
        self._connected_handler: Optional[Callable[[], None]] = None
        self._disconnected_handler: Optional[Callable[[], None]] = None

    def start(
        self,
        handler: Callable[[Dict], None],
        connected_handler: Callable[[], None],
        disconnected_handler: Callable[[], None],
    ):
        self._event_handler = handler
        self._connected_handler = connected_handler
        self._disconnected_handler = disconnected_handler

        self._start_receiver_thread()

    def _start_receiver_thread(self):
        # current_loop_id is used to stop the current loop when a new one is started
        # it might take some time to stop the current loop, so a single "running" flag is not
        # enough
        loop_id = str(uuid4())
        self._current_loop_id = loop_id
        th = Thread(target=self._run_receiver, args=(loop_id,))
        th.start()

    def stop(self):
        self._current_loop_id = None

    def restart(self):
        self.stop()
        self._start_receiver_thread()

    def _run_receiver(self, loop_id: str):
        while self._is_current_loop(loop_id):
            self._connect_and_consume_events(loop_id)

        logger.info("SSE Client stopped")

    def _is_current_loop(self, loop_id: str):
        return self._current_loop_id == loop_id

    @retry(
        SSEConnectionFailed, delay=2, tries=-1, max_delay=240, backoff=2, logger=logger
    )
    def _connect_and_consume_events(self, loop_id: str):
        try:
            mc_login_token = get_mc_login_token()
            token_id = mc_login_token.get(X_MCD_ID)
            logger.info(f"Connecting SSE Client, using token ID={token_id} ...")
            url = urljoin(self._base_url, f"/stream")
            headers = {
                "Accept": "text/event-stream",
                **mc_login_token,
            }
            self._sse_client = sseclient.SSEClient(url, headers=headers)
            if self._connected_handler:
                self._connected_handler()
            for event in self._sse_client:
                if not self._is_current_loop(loop_id):
                    break
                event_data = event.data
                try:
                    event = json.loads(event_data)
                except Exception as parse_ex:
                    logger.exception(
                        f"Failed to parse event: {parse_ex}, text: {event.data}"
                    )
                    continue
                try:
                    if self._event_handler:
                        self._event_handler(event)
                except Exception as handle_ex:
                    logger.exception(
                        f"Failed to process event: {handle_ex}, text: {event_data}"
                    )
        except Exception as ex:
            if self._is_current_loop(loop_id):
                raise SSEConnectionFailed(str(ex)) from ex
        finally:
            if self._disconnected_handler:
                self._disconnected_handler()
