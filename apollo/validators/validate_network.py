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
    """
    Network test utilities, based on the same code in data-collector project
    """

    @classmethod
    def validate_tcp_open_connection(
        cls,
        host: Optional[str],
        port_str: Optional[str],
        timeout_str: Optional[str],
        trace_id: Optional[str],
    ):
        """
        Tests if a destination is reachable and accepts requests. Opens a TCP Socket to the specified host and port.
        :param host: Host to check, will raise `BadRequestError` if None.
        :param port_str: Port to check as a string containing the numeric port value, will raise `BadRequestError`
            if None or non-numeric.
        :param timeout_str: Timeout in seconds as a string containing the numeric value, will raise `BadRequestError`
            if non-numeric. Defaults to 5 seconds.
        :param trace_id: Optional trace ID received from the client that will be included in the response, if present.
        """
        return cls._call_validation_method(
            cls._internal_validate_tcp_open_connection,
            host=host,
            port_str=port_str,
            timeout_str=timeout_str,
            trace_id=trace_id,
        )

    @classmethod
    def validate_telnet_connection(
        cls,
        host: Optional[str],
        port_str: Optional[str],
        timeout_str: Optional[str],
        trace_id: Optional[str] = None,
    ):
        """
        Checks if telnet connection is usable.
        :param host: Host to check, will raise `BadRequestError` if None.
        :param port_str: Port to check as a string containing the numeric port value, will raise `BadRequestError`
            if None or non-numeric.
        :param timeout_str: Timeout in seconds as a string containing the numeric value, will raise `BadRequestError`
            if non-numeric. Defaults to 5 seconds.
        :param trace_id: Optional trace ID received from the client that will be included in the response, if present.
        """
        return cls._call_validation_method(
            cls._internal_validate_telnet_connection,
            host=host,
            port_str=port_str,
            timeout_str=timeout_str,
            trace_id=trace_id,
        )

    @staticmethod
    def _call_validation_method(
        method: Callable, trace_id: Optional[str], **kwargs  # type: ignore
    ) -> AgentResponse:
        """
        Internal method to call one of the network validation methods: `internal_validate_tcp_connection` or
        `_internal_validate_telnet_connection`.
        Converts the different exceptions and the successful result to an `AgentResponse` object.
        """
        try:
            result = method(**kwargs)
            return AgentUtils.agent_ok_response(result, trace_id=trace_id)
        except BadRequestError as ex:
            return AgentUtils.agent_response_for_error(
                message=str(ex), status_code=400, trace_id=trace_id
            )
        except ConnectionFailedError as ex:
            return AgentUtils.agent_response_for_error(
                message=str(ex), trace_id=trace_id
            )
        except Exception:
            return AgentUtils.agent_response_for_last_exception(
                status_code=500, trace_id=trace_id
            )

    @classmethod
    def _internal_validate_tcp_open_connection(
        cls, host: Optional[str], port_str: Optional[str], timeout_str: Optional[str]
    ) -> Dict:
        """
        Implementation for the TCP Open validation, first validates the parameters and convert port and timeout to
        int values and then tries to open a TCP socket to the given destination.
        """
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
        """
        Implementation for the Telnet access validation, first validates the parameters and convert port and timeout to
        int values and then tries to open a Telnet connection to the given destination.
        """
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
        """
        Internal method to validate the input parameters (host, port and timeout) and to convert port and timeout
        to int values.
        """
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
