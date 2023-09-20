import logging
from typing import Dict, Optional, List, Tuple

import requests
from requests import HTTPError
from retry.api import retry_call

from apollo.integrations.base_proxy_client import BaseProxyClient


_logger = logging.getLogger(__name__)

_DEFAULT_RETRY_STATUS_CODE_RANGES = [
    (429, 430),
    (500, 600),
]

_RRI = dict(
    tries=2,
    delay=2,
    backoff=2,
    max_delay=10,
)


class HttpClientError(Exception):
    pass


class HttpRetryableError(Exception):
    pass


class HttpProxyClient(BaseProxyClient):
    def __init__(self, **kwargs):
        pass

    @property
    def wrapped_client(self):
        return None

    @staticmethod
    def is_client_error_status_code(status_code: int) -> bool:
        return 400 <= status_code < 500

    def do_request(
        self,
        url: str,
        http_method: str = "POST",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        token: Optional[str] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
    ) -> Dict:
        """
        Throws HTTPError by calling response.raise_for_status internally.
        """
        # used for testing, we configure "INVALID_host_name" in the connection
        # so we're sure there's no connection established from the data collector
        # and it connects only through the agent
        url = url.replace("INVALID_", "")

        request_args = {}
        if payload:
            request_args["json"] = payload
        if timeout:
            request_args["timeout"] = timeout

        headers = {**additional_headers} if additional_headers else {}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if content_type:
            headers["Content-Type"] = content_type
        if user_agent:
            headers["User-Agent"] = user_agent
        request_args["headers"] = headers

        response = requests.request(http_method, url, **request_args)
        try:
            response.raise_for_status()
        except HTTPError as err:
            _logger.exception(
                f"Request failed with {err.response.status_code}",
                extra=dict(error_text=err.response.text),
            )
            if retry_status_code_ranges is not None and self._is_retry_status_code(
                retry_status_code_ranges, err.response.status_code
            ):
                # retry for this status code
                raise HttpRetryableError(err.response.text) from err
            if self.is_client_error_status_code(err.response.status_code):
                raise HttpClientError(err.response.text) from err
            raise type(err)(err.response.text) from err

        return response.json()

    def do_request_with_retry(
        self,
        url: str,
        http_method: str = "POST",
        payload: Optional[Dict] = None,
        content_type: Optional[str] = None,
        timeout: Optional[int] = None,
        token: Optional[str] = None,
        user_agent: Optional[str] = None,
        additional_headers: Optional[Dict] = None,
        retry_status_code_ranges: Optional[List[Tuple]] = None,
        retry_args: Optional[Dict] = None,
    ) -> Dict:
        retry_status_code_ranges = (
            retry_status_code_ranges or _DEFAULT_RETRY_STATUS_CODE_RANGES
        )
        retry_params = retry_args or _RRI
        return retry_call(
            self.do_request,
            fkwargs={
                "url": url,
                "http_method": http_method,
                "payload": payload,
                "content_type": content_type,
                "timeout": timeout,
                "token": token,
                "user_agent": user_agent,
                "additional_headers": additional_headers,
                "retry_status_code_ranges": retry_status_code_ranges,
            },
            exceptions=HttpRetryableError,
            **retry_params,
        )

    @staticmethod
    def _is_retry_status_code(ranges: List[Tuple], status_code) -> bool:
        return any(r for r in ranges if r[0] <= status_code < r[1])
