import sys
import traceback
from typing import Optional, Dict, Tuple

from apollo.common.agent.utils import AgentUtils as BaseAgentUtils
from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.common.interfaces.agent_response import AgentResponse


class AgentUtils(BaseAgentUtils):
    """
    Helper methods to create success/failure responses.
    """

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
    def _get_error_details(
        error: Exception, client: Optional[BaseProxyClient] = None
    ) -> Tuple[Optional[str], Optional[Dict]]:
        if client:
            return client.get_error_type(error), client.get_error_extra_attributes(
                error
            )
        return None, None
