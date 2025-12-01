import socket
from telnetlib import Telnet
from typing import Optional, Callable, Dict, Tuple, Union

import requests

from apollo.common.agent.utils import AgentUtils
from apollo.common.interfaces.agent_response import AgentResponse

_DEFAULT_TIMEOUT_SECS = 5
_DEFAULT_HTTP_TIMEOUT_SECS = 10


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

    @classmethod
    def perform_dns_lookup(
        cls,
        host: Optional[str],
        port: Optional[Union[int, str]],
        trace_id: Optional[str] = None,
    ):
        """
        Performs a DNS lookup for the specified host name.
        :param host: Host to check, will raise `BadRequestError` if None.
        :param port: Optional port to pass to `getaddrinfo` API, both int and
        string are supported.
        :param trace_id: Optional trace ID received from the client that will be included in
        the response, if present.
        """
        return cls._call_validation_method(
            cls._internal_perform_dns_lookup,
            host=host,
            port=port,
            trace_id=trace_id,
        )

    @classmethod
    def validate_http_connection(
        cls,
        url: Optional[str],
        include_response_str: Optional[Union[bool, str]],
        timeout_str: Optional[Union[int, str]],
        trace_id: Optional[str] = None,
    ):
        """
        Performs a GET request to test connectivity with the provided URL.
        :param url: The URL to test, will raise `BadRequestError` if None.
        :param include_response_str: Optional boolean indicating if the response should be sent back.
        :param timeout_str: Timeout in seconds as a string containing the numeric value,
            will raise `BadRequestError` if non-numeric. Defaults to 10 seconds.
        :param trace_id: Optional trace ID received from the client that will be included in
            the response, if present.
        """
        return cls._call_validation_method(
            cls._internal_validate_http_connection,
            url=url,
            include_response_str=include_response_str,
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
            return AgentUtils.agent_response_for_last_exception(trace_id=trace_id)

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

    @classmethod
    def _internal_perform_dns_lookup(
        cls, host: Optional[str], port: Optional[Union[int, str]]
    ) -> Dict:
        """
        Implementation for the DNS lookup operation, first validates the parameters and then
        uses getaddrinfo to resolve the name.
        """
        if not host:
            raise BadRequestError("host is a required parameter")

        try:
            lookup_result = socket.getaddrinfo(host, port)
            addresses = sorted(set([str(addr[4][0]) for addr in lookup_result]))
            return {"message": f"Host {host} resolves to: {', '.join(addresses)}"}
        except Exception as err:
            raise ConnectionFailedError(
                f"DNS lookup failed for {host}: {err}."
            ) from err

    @classmethod
    def _internal_validate_http_connection(
        cls,
        url: Optional[str],
        include_response_str: Optional[Union[bool, str]],
        timeout_str: Optional[Union[int, str]],
    ) -> Dict:
        """
        Implementation for the HTTP connection validation, first validates the parameters and then
        uses requests.get to connect to the URL.
        """
        if not url:
            raise BadRequestError("url is a required parameter")
        include_response = (
            include_response_str == "true" or include_response_str is True
        )
        timeout_in_seconds = (
            int(timeout_str) if timeout_str else _DEFAULT_HTTP_TIMEOUT_SECS
        )

        try:
            response = requests.get(url, timeout=timeout_in_seconds)
            message = f"URL {url} responded with status: {response.status_code} ({response.reason})"
            if include_response:
                content_str = response.content.decode("utf-8")
                message += f" and content: {content_str}"
            return {"message": message}
        except Exception as err:
            raise ConnectionFailedError(
                f"HTTP request failed for {url}: {err}."
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
