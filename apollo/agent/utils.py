import os
import random
import string
import sys
import traceback
import uuid
from typing import Optional, Dict, List, BinaryIO, Any, Tuple

import requests

from apollo.agent.constants import (
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_EXCEPTION,
    ATTRIBUTE_NAME_STACK_TRACE,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_VALUE_REDACTED,
    ATTRIBUTE_NAME_ERROR_ATTRS,
)
from apollo.agent.env_vars import (
    TEMP_PATH_ENV_VAR,
    DEFAULT_TEMP_PATH,
    CHECK_OUTBOUND_IP_ADDRESS_URL_ENV_VAR,
    CHECK_OUTBOUND_IP_ADDRESS_URL_DEFAULT_VALUE,
)
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.interfaces.agent_response import AgentResponse


class AgentUtils:
    """
    Helper methods to create success/failure responses.
    """

    @staticmethod
    def agent_ok_response(
        result: Dict, trace_id: Optional[str] = None
    ) -> AgentResponse:
        return AgentResponse(result, 200, trace_id)

    @classmethod
    def agent_response_for_last_exception(
        cls,
        prefix: Optional[str] = None,
        status_code: int = 200,
        trace_id: Optional[str] = None,
        client: Optional[BaseProxyClient] = None,
    ) -> AgentResponse:
        return AgentResponse(
            cls.response_for_last_exception(prefix=prefix, client=client),
            status_code,
            trace_id,
        )

    @classmethod
    def agent_response_for_error(
        cls,
        message: str,
        stack_trace: Optional[List] = None,
        status_code: int = 200,
        trace_id: Optional[str] = None,
    ) -> AgentResponse:
        return AgentResponse(
            cls._response_for_error(message, stack_trace=stack_trace),
            status_code,
            trace_id,
        )

    @classmethod
    def response_for_last_exception(
        cls, client: Optional[BaseProxyClient] = None, prefix: Optional[str] = None
    ) -> Dict:
        last_type, last_value, _ = sys.exc_info()
        error = str(last_value)
        exception_message = " ".join(
            traceback.format_exception_only(last_type, last_value)
        )
        if prefix:
            error = f"{prefix} {error}"
        stack_trace = traceback.format_tb(last_value.__traceback__)  # type: ignore
        error_type, error_attrs = cls._get_error_details(last_value, client)  # type: ignore
        return cls._response_for_error(
            error,
            exception_message=exception_message,
            stack_trace=stack_trace,
            error_type=error_type,
            error_attrs=error_attrs,
        )

    @staticmethod
    def temp_path():
        return os.getenv(TEMP_PATH_ENV_VAR, DEFAULT_TEMP_PATH)

    @classmethod
    def temp_file_path(
        cls, sub_folder: Optional[str] = None, extension: Optional[str] = None
    ) -> str:
        temp_path = cls.ensure_temp_path(sub_folder)
        file_name = str(uuid.uuid4())
        if extension:
            file_name = f"{file_name}.{extension}"
        return os.path.join(temp_path, file_name)

    @classmethod
    def ensure_temp_path(cls, sub_folder: Optional[str] = None) -> str:
        temp_path = cls.temp_path()
        if sub_folder:
            temp_path = os.path.join(temp_path, sub_folder)
        if not os.path.exists(temp_path):
            os.makedirs(temp_path)
        return temp_path

    @staticmethod
    def open_file(path: str) -> BinaryIO:
        return open(path, "rb")

    @classmethod
    def redact_attributes(cls, value: Any, attributes: List[str]) -> Any:
        if isinstance(value, Dict):
            return {
                k: (
                    ATTRIBUTE_VALUE_REDACTED
                    if k in attributes
                    else cls.redact_attributes(v, attributes)
                )
                for k, v in value.items()
            }
        elif isinstance(value, List):
            return [cls.redact_attributes(v, attributes) for v in value]
        else:
            return value

    @staticmethod
    def get_outbound_ip_address() -> str:
        url = os.getenv(
            CHECK_OUTBOUND_IP_ADDRESS_URL_ENV_VAR,
            CHECK_OUTBOUND_IP_ADDRESS_URL_DEFAULT_VALUE,
        )
        response = requests.get(url)
        # truncate the response, we don't want to return a full webpage if the url is wrong or not working
        return response.content.decode("utf-8")[:20].strip() if response.content else ""

    @staticmethod
    def generate_random_str(rand_len: int) -> str:
        return "".join(random.sample(string.ascii_letters, rand_len))

    @staticmethod
    def _get_error_details(
        error: Exception, client: Optional[BaseProxyClient] = None
    ) -> Tuple[Optional[str], Optional[Dict]]:
        if client:
            return client.get_error_type(error), client.get_error_extra_attributes(
                error
            )
        return None, None

    @staticmethod
    def _response_for_error(
        message: str,
        exception_message: Optional[str] = None,
        stack_trace: Optional[List] = None,
        error_type: Optional[str] = None,
        error_attrs: Optional[Dict] = None,
    ) -> Dict:
        response: Dict[str, Any] = {
            ATTRIBUTE_NAME_ERROR: message,
        }
        if exception_message:
            response[ATTRIBUTE_NAME_EXCEPTION] = exception_message
        if stack_trace:
            response[ATTRIBUTE_NAME_STACK_TRACE] = stack_trace
        if error_type:
            response[ATTRIBUTE_NAME_ERROR_TYPE] = error_type
        if error_attrs:
            response[ATTRIBUTE_NAME_ERROR_ATTRS] = error_attrs
        return response
