import socket
from telnetlib import Telnet
from typing import Optional, Callable, Dict, Tuple

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

    @classmethod
    def validate_telnet_connection(
        cls, host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ):
        return cls._call_validation_method(
            cls._internal_validate_telnet_connection,
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

    @classmethod
    def _internal_validate_tcp_open_connection(
        cls, host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ) -> Dict:
        port, timeout_in_seconds = cls._internal_validate_network_parameters(
            host, port_str, timeout_str
        )

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout_in_seconds)

        if sock.connect_ex((host, port)) == 0:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()
            return {
                "message": f"Port {port} is open on {host}",
            }
        raise ConnectionFailedError(f"Port {port} is closed on {host}.")

    @classmethod
    def _internal_validate_telnet_connection(
        cls, host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ) -> Dict:
        port, timeout_in_seconds = cls._internal_validate_network_parameters(
            host, port_str, timeout_str
        )
        friendly_name = f"{host}:{port}"

        try:
            with Telnet(host, port, timeout_in_seconds) as session:
                try:
                    session.read_very_eager()
                    return {
                        "message": f"Telnet connection for {friendly_name} is usable."
                    }
                except EOFError as err:
                    raise ConnectionFailedError(
                        f"Telnet connection for {friendly_name} is unusable."
                    ) from err
        except socket.timeout as err:
            raise ConnectionFailedError(
                f"Socket timeout for {friendly_name}. Connection unusable."
            ) from err
        except socket.gaierror as err:
            raise ConnectionFailedError(
                f"Invalid hostname {host} ({err}). Connection unusable."
            ) from err

    @staticmethod
    def _internal_validate_network_parameters(
        host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ) -> Tuple[int, int]:
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
        return port, timeout_in_seconds
