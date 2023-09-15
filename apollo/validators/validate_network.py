import socket
from typing import Optional, Callable, Dict

from apollo.agent.utils import AgentUtils
from apollo.interfaces.agent_response import AgentResponse

_DEFAULT_TIMEOUT_SECS = 5


class ConnectionFailedError(Exception):
    pass


class BadRequestError(Exception):
    pass


class ValidateNetwork:
    @classmethod
    def validate_tcp_open_connection(
        cls, host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ):
        return cls._call_validation_method(
            cls._internal_validate_tcp_open_connection,
            host=host,
            port_str=port_str,
            timeout_str=timeout_str,
        )

    @staticmethod
    def _call_validation_method(method: Callable, **kwargs) -> AgentResponse:
        try:
            result = method(**kwargs)
            return AgentUtils.agent_ok_response(result)
        except BadRequestError as ex:
            return AgentUtils.agent_response_for_error(message=str(ex), status_code=400)
        except ConnectionFailedError as ex:
            return AgentUtils.agent_response_for_error(message=str(ex))
        except Exception:
            return AgentUtils.agent_response_for_last_exception(status_code=500)

    @staticmethod
    def _internal_validate_tcp_open_connection(
        host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ) -> Dict:
        if not host or not port_str:
            raise BadRequestError("host and port are required parameters")
        try:
            port = int(port_str)
        except ValueError:
            raise BadRequestError(f"Invalid value for port parameter: {port_str}")
        try:
            timeout_in_seconds = (
                int(timeout_str) if timeout_str else _DEFAULT_TIMEOUT_SECS
            )
        except ValueError:
            raise BadRequestError(f"Invalid value for timeout parameter: {timeout_str}")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_in_seconds)

        if sock.connect_ex((host, port)) == 0:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            return {
                "message": f"Port {port} is open on {host}",
            }
        raise ConnectionFailedError(f"Port {port} is closed on {host}.")
