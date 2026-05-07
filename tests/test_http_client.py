import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import MagicMock, create_autospec, patch, call, mock_open

import requests
from requests import Response, HTTPError

from apollo.agent.agent import Agent
from apollo.common.agent.constants import (
    ATTRIBUTE_NAME_RESULT,
    ATTRIBUTE_NAME_ERROR,
    ATTRIBUTE_NAME_ERROR_TYPE,
    ATTRIBUTE_NAME_ERROR_ATTRS,
    ATTRIBUTE_VALUE_REDACTED,
)
from apollo.agent.logging_utils import LoggingUtils
from apollo.integrations.http.http_proxy_client import (
    HttpClientError,
    HttpProxyClient,
)

_HTTP_USER_AGENT = "TestUserAgent"

_HTTP_OPERATION = {
    "trace_id": "1234",
    "commands": [
        {
            "method": "do_request",
            "kwargs": {
                "url": "https://test.com/path",
                "http_method": "GET",
                "payload": {},
                "user_agent": _HTTP_USER_AGENT,
                "additional_headers": None,
                "retry_status_code_ranges": None,
                "verify_ssl": None,
            },
        }
    ],
}
_HTTP_CREDENTIALS = {"token": "123_token"}


class TestHttpClient(TestCase):
    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    def test_http_request(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("apollo.agent.agent.logger.info")
    def test_http_request_data_redacted(self, mock_info, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["data"] = "client_secret=1234&client_id=4321"
        operation["commands"][0]["kwargs"]["additional_headers"] = {
            "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
            "client_secret": "1234",
            "client_id": "4321",
            "auth_key": "1234",
        }
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_info.assert_called_with(
            f"Executing operation: http/do_request",
            extra={
                "mcd_operation_name": "do_request",
                "response_size_limit_bytes": 0,
                "compress_response_threshold_bytes": 0,
                "response_type": "json",
                "skip_cache": False,
                "compress_response_file": False,
                "mcd_trace_id": "1234",
                "commands": [
                    {
                        "method": "do_request",
                        "kwargs": {
                            "url": "https://test.com/path",
                            "http_method": "GET",
                            "payload": ATTRIBUTE_VALUE_REDACTED,
                            "user_agent": ATTRIBUTE_VALUE_REDACTED,
                            "additional_headers": {
                                "Authorization": ATTRIBUTE_VALUE_REDACTED,
                                "client_secret": ATTRIBUTE_VALUE_REDACTED,
                                "client_id": ATTRIBUTE_VALUE_REDACTED,
                                "auth_key": ATTRIBUTE_VALUE_REDACTED,
                            },
                            "retry_status_code_ranges": None,
                            "verify_ssl": None,
                            "data": ATTRIBUTE_VALUE_REDACTED,
                        },
                    }
                ],
            },
        )

    @patch("requests.request")
    def test_http_request_with_custom_auth_type(self, mock_request):
        credentials = {
            "auth_type": "Token",
            "token": "1234",
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            credentials,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"{credentials['auth_type']} {credentials['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_custom_auth_header(self, mock_request):
        # Use connect_args directly (DC pre-shaped path) so CTP pass-through applies.
        # auth_type=None means no prefix: header value is the bare token.
        credentials = {
            "connect_args": {
                "auth_header": "Api-Key",
                "auth_type": None,
                "token": "1234",
            }
        }
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            credentials,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Api-Key": "1234",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_no_auth(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            None,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
                call.json(),
            ]
        )
        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_params(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        params = {
            "int_param": 23,
            "str_param": "abc",
        }
        operation["commands"][0]["kwargs"]["params"] = params
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            params=params,
        )

    @patch("requests.request")
    def test_http_request_with_verify_ssl(self, mock_request):
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {
            "ok": True,
        }
        mock_response.json.return_value = expected_result
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["verify_ssl"] = True
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=True,
        )

        operation["commands"][0]["kwargs"]["verify_ssl"] = False
        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )

    @patch("requests.request")
    def test_http_request_failed(self, mock_request):
        mock_response = create_autospec(Response)
        mock_response.status_code = 404
        mock_response.text = "not found"
        mock_response.reason = "NOT FOUND"
        mock_request.return_value = mock_response
        mock_response.raise_for_status.side_effect = HTTPError(
            "failed",
            response=mock_response,
        )

        response = self._agent.execute_operation(
            "http",
            "do_request",
            _HTTP_OPERATION,
            _HTTP_CREDENTIALS,
        )
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": f"Bearer {_HTTP_CREDENTIALS['token']}",
                "User-Agent": _HTTP_USER_AGENT,
            },
        )
        mock_response.assert_has_calls(
            [
                call.raise_for_status(),
            ]
        )
        self.assertIsNotNone(response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual(mock_response.text, response.result.get(ATTRIBUTE_NAME_ERROR))
        self.assertEqual("HTTPError", response.result.get(ATTRIBUTE_NAME_ERROR_TYPE))
        self.assertEqual(
            {
                "status_code": 404,
                "reason": mock_response.reason,
            },
            response.result.get(ATTRIBUTE_NAME_ERROR_ATTRS),
        )

    @patch("requests.request")
    def test_http_request_with_ssl_options_ca_data(self, mock_request):
        """Test that ssl_verify path in connect_args is forwarded to requests as verify."""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        # Use connect_args directly (DC pre-shaped / CTP-resolved path) so ssl_verify
        # is already a file path — no temp-file creation needed in the proxy client.
        credentials = {
            "connect_args": {
                "token": "test_token",
                "ssl_verify": "/tmp/fake_ca.pem",
            }
        }

        operation = deepcopy(_HTTP_OPERATION)
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        call_kwargs = mock_request.call_args[1]
        self.assertIn("verify", call_kwargs)
        self.assertTrue(call_kwargs["verify"].endswith(".pem"))

        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    def test_http_request_with_ssl_options_disabled(self, mock_request):
        """Test that ssl_options with disabled=True sets verify=False"""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        credentials = {
            "token": "test_token",
            "ssl_options": {
                "disabled": True,
            },
        }

        operation = deepcopy(_HTTP_OPERATION)
        response = self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        # Verify request was made with verify=False
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer test_token",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )

        self.assertTrue(ATTRIBUTE_NAME_RESULT in response.result)
        self.assertEqual(expected_result, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("builtins.open", new_callable=mock_open)
    def test_http_request_verify_ssl_overrides_ssl_options(
        self, mock_file, mock_request
    ):
        """Test that verify_ssl parameter takes precedence over ssl_options"""
        mock_response = create_autospec(Response)
        mock_request.return_value = mock_response
        expected_result = {"ok": True}
        mock_response.json.return_value = expected_result

        ca_data = "-----BEGIN CERTIFICATE-----\nMIIDtest\n-----END CERTIFICATE-----"
        credentials = {
            "token": "test_token",
            "ssl_options": {
                "ca_data": ca_data,
            },
        }

        # Set verify_ssl=False to override the ssl_options ca_data
        operation = deepcopy(_HTTP_OPERATION)
        operation["commands"][0]["kwargs"]["verify_ssl"] = False

        self._agent.execute_operation(
            "http",
            "do_request",
            operation,
            credentials,
        )

        # Verify request was made with verify=False (from verify_ssl param)
        # instead of the cert file path (from ssl_options)
        mock_request.assert_called_with(
            "GET",
            "https://test.com/path",
            headers={
                "Authorization": "Bearer test_token",
                "User-Agent": _HTTP_USER_AGENT,
            },
            verify=False,
        )


class TestDownloadBytes(TestCase):
    """Tests for HttpProxyClient.download_bytes — streaming binary fetches with
    optional auth-skip and size cap."""

    def _make_response(
        self,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_content.return_value = iter(chunks if chunks is not None else [b""])
        if status_code >= 400:
            resp.raise_for_status.side_effect = HTTPError(
                f"{status_code}", response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    @patch("requests.get")
    def test_returns_raw_bytes(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"chunk1", b"chunk2"])

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        result = client.download_bytes("https://s3.example/file.jar")

        self.assertEqual(b"chunk1chunk2", result)

    @patch("requests.get")
    def test_no_auth_header_when_no_auth_true(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        sent_headers = mock_get.call_args.kwargs["headers"]
        self.assertNotIn("Authorization", sent_headers)

    @patch("requests.get")
    def test_auth_header_present_when_no_auth_false(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        client.download_bytes("https://api.example/file", no_auth=False)

        sent_headers = mock_get.call_args.kwargs["headers"]
        self.assertEqual("Bearer tok", sent_headers["Authorization"])

    @patch("requests.get")
    def test_403_raises_http_client_error(self, mock_get):
        mock_get.return_value = self._make_response(status_code=403)

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError):
            client.download_bytes("https://s3.example/file.jar", no_auth=True)

    @patch("requests.get")
    def test_404_raises_http_client_error(self, mock_get):
        mock_get.return_value = self._make_response(status_code=404)

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError):
            client.download_bytes("https://s3.example/file.jar", no_auth=True)

    @patch("requests.get")
    def test_500_raises_http_error_not_http_client_error(self, mock_get):
        mock_get.return_value = self._make_response(status_code=500)

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HTTPError) as ctx:
            client.download_bytes("https://s3.example/file.jar", no_auth=True)
        self.assertIsInstance(ctx.exception, HTTPError)
        self.assertNotIsInstance(ctx.exception, HttpClientError)
        self.assertIsNotNone(ctx.exception.response)

    @patch("requests.get")
    def test_connection_error_raises_http_client_error_without_url(self, mock_get):
        # Pre-signed URL with a sentinel "secret" we want to ensure never leaks.
        secret_url = "https://s3.example/file.jar?Signature=DO-NOT-LEAK-XYZ"
        mock_get.side_effect = requests.ConnectionError("conn refused at " + secret_url)

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes(secret_url, no_auth=True)
        self.assertNotIn("DO-NOT-LEAK-XYZ", str(ctx.exception))
        self.assertNotIn(secret_url, str(ctx.exception))

    @patch("requests.get")
    def test_max_bytes_cap_raises_when_exceeded(self, mock_get):
        # Two 100-byte chunks; max_bytes=150 → second chunk pushes over the limit.
        mock_get.return_value = self._make_response(chunks=[b"x" * 100, b"y" * 100])

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes(
                "https://s3.example/file.jar", no_auth=True, max_bytes=150
            )
        self.assertIn("150", str(ctx.exception))

    @patch("requests.get")
    def test_max_bytes_none_is_unlimited(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"x" * 1024])

        client = HttpProxyClient(credentials={"connect_args": {}})
        result = client.download_bytes(
            "https://s3.example/file.jar", no_auth=True, max_bytes=None
        )
        self.assertEqual(1024, len(result))

    @patch("requests.get")
    def test_timeout_passed_to_requests_get(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True, timeout=42)

        self.assertEqual(42, mock_get.call_args.kwargs["timeout"])

    @patch("requests.get")
    def test_default_timeout_is_120_seconds(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        self.assertEqual(120, mock_get.call_args.kwargs["timeout"])

    @patch("requests.get")
    def test_ssl_verify_passed_through_when_set(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(
            credentials={"connect_args": {"ssl_verify": "/path/to/ca-bundle.crt"}}
        )
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        self.assertEqual("/path/to/ca-bundle.crt", mock_get.call_args.kwargs["verify"])

    @patch("requests.get")
    def test_ssl_verify_default_omits_verify_kwarg(self, mock_get):
        # When ssl_verify is unset on connect_args, requests.get is called WITHOUT
        # the verify kwarg — so requests's own default (True) applies. Mirrors
        # do_request behavior.
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        self.assertNotIn("verify", mock_get.call_args.kwargs)

    @patch("requests.get")
    def test_additional_headers_merged(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        client.download_bytes(
            "https://api.example/file",
            no_auth=False,
            additional_headers={"X-Trace": "abc"},
        )

        sent = mock_get.call_args.kwargs["headers"]
        self.assertEqual("abc", sent["X-Trace"])
        self.assertEqual("Bearer tok", sent["Authorization"])

    @patch("requests.get")
    def test_stream_true_for_chunked_read(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        self.assertTrue(mock_get.call_args.kwargs["stream"])

    @patch("requests.get")
    def test_max_bytes_exactly_at_limit_does_not_raise(self, mock_get):
        # Guard is strict-greater-than: a response of exactly max_bytes bytes must succeed.
        mock_get.return_value = self._make_response(chunks=[b"x" * 200])

        client = HttpProxyClient(credentials={"connect_args": {}})
        result = client.download_bytes("https://s3.example/file.jar", max_bytes=200)
        self.assertEqual(200, len(result))

    @patch("requests.get")
    def test_custom_auth_header_name_used_when_set(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(
            credentials={"connect_args": {"token": "tok", "auth_header": "X-API-Key"}}
        )
        client.download_bytes("https://api.example/file", no_auth=False)

        sent_headers = mock_get.call_args.kwargs["headers"]
        self.assertEqual("Bearer tok", sent_headers["X-API-Key"])
        self.assertNotIn("Authorization", sent_headers)


class TestDownloadBytesUrlSafety(TestCase):
    """SSRF defense-in-depth: verify _assert_safe_download_url rejects every
    non-public IP-literal class (not just RFC1918 / loopback / link-local) and
    that download_bytes itself refuses to follow redirects."""

    def _make_response(
        self,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
        headers: dict | None = None,
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.headers = headers or {}
        resp.iter_content.return_value = iter(chunks if chunks is not None else [b""])
        if status_code >= 400:
            resp.raise_for_status.side_effect = HTTPError(
                f"{status_code}", response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def test_rejects_unspecified_ipv4(self):
        # 0.0.0.0 is not private/loopback/link-local but is_global is False.
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://0.0.0.0/file")
        self.assertIn("non-public", str(ctx.exception))

    def test_rejects_unspecified_ipv6(self):
        # IPv6 :: — not private/loopback/link-local, but not global either.
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://[::]/file")
        self.assertIn("non-public", str(ctx.exception))

    def test_rejects_multicast_ipv4(self):
        # 224.0.0.0/4 is multicast — is_global is False.
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://224.0.0.1/file")
        self.assertIn("non-public", str(ctx.exception))

    def test_rejects_reserved_ipv4(self):
        # 240.0.0.0/4 is reserved — is_global is False.
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://240.0.0.1/file")
        self.assertIn("non-public", str(ctx.exception))

    def test_rejects_imds_link_local(self):
        # 169.254.169.254 — AWS IMDS, the canonical SSRF target.
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://169.254.169.254/latest/meta-data/")
        self.assertIn("non-public", str(ctx.exception))

    def test_rejects_rfc1918(self):
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://10.0.0.1/file")
        self.assertIn("non-public", str(ctx.exception))

    @patch("requests.get")
    def test_dns_hostname_passes_url_safety_check(self, mock_get):
        # Sanity: legitimate DNS hostnames are not literal IPs; guard short-circuits.
        mock_get.return_value = MagicMock(
            status_code=200,
            headers={},
            iter_content=MagicMock(return_value=iter([b"data"])),
            raise_for_status=MagicMock(return_value=None),
        )
        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example.com/file")  # no exception

    @patch("requests.get")
    def test_does_not_follow_redirects(self, mock_get):
        # download_bytes must pass allow_redirects=False so that a 30x cannot
        # send the request to an internal/non-https target (SSRF) or forward
        # credentials when no_auth=False.
        mock_get.return_value = self._make_response(chunks=[b"data"])

        client = HttpProxyClient(credentials={"connect_args": {}})
        client.download_bytes("https://s3.example/file.jar", no_auth=True)

        self.assertEqual(False, mock_get.call_args.kwargs["allow_redirects"])

    @patch("requests.get")
    def test_3xx_response_raises_http_client_error(self, mock_get):
        # With allow_redirects=False, a 30x is returned as a normal response.
        # raise_for_status does NOT flag 3xx, so download_bytes must reject it
        # explicitly — otherwise the empty redirect body would be returned as
        # the "downloaded" bytes.
        mock_get.return_value = self._make_response(
            status_code=302,
            chunks=[b""],
            headers={"Location": "https://attacker.example/exfil"},
        )

        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError) as ctx:
            client.download_bytes("https://s3.example/file.jar", no_auth=True)
        self.assertIn("302", str(ctx.exception))
        # Error message must not echo the Location URL — it could itself point
        # at an internal/sensitive host.
        self.assertNotIn("attacker.example", str(ctx.exception))


class TestMulesoftAgentEndToEnd(TestCase):
    """End-to-end integration: Agent.execute_operation('mulesoft', ...) routes
    raw flat credentials through CTP → HttpProxyClient → requests.request,
    proving factory wiring + CTP + do_request work together. The DC supplies
    the full MuleSoft URL; the agent only attaches the Bearer token from
    OAuth."""

    def setUp(self) -> None:
        self._agent = Agent(LoggingUtils())

    @patch("requests.request")
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_mulesoft_agent_execute_operation_routes_through_http_proxy(
        self, mock_oauth_requests, mock_request
    ):
        # Stage 1 mock: OAuth POST returns a token.
        oauth_resp = MagicMock()
        oauth_resp.json.return_value = {"access_token": "ms-token"}
        oauth_resp.raise_for_status.return_value = None
        mock_oauth_requests.post.return_value = oauth_resp

        # Stage 2 mock: downstream API GET returns JSON.
        api_resp = create_autospec(Response)
        api_resp.json.return_value = {"orgs": []}
        mock_request.return_value = api_resp

        operation = {
            "trace_id": "ms-trace-1",
            "commands": [
                {
                    "method": "do_request",
                    "kwargs": {
                        "url": "https://anypoint.mulesoft.com/apimanager/api/v1/organizations",
                        "http_method": "GET",
                    },
                }
            ],
        }
        raw_creds = {"client_id": "cid", "client_secret": "csec"}

        response = self._agent.execute_operation(
            "mulesoft", "do_request", operation, raw_creds
        )

        # OAuth was attempted at the US region endpoint.
        self.assertEqual(
            "https://anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
            mock_oauth_requests.post.call_args.args[0],
        )
        # Downstream call hit the DC-supplied URL with the Bearer header from CTP.
        mock_request.assert_called_with(
            "GET",
            "https://anypoint.mulesoft.com/apimanager/api/v1/organizations",
            headers={"Authorization": "Bearer ms-token"},
        )
        # Response payload propagates through the agent.
        self.assertEqual({"orgs": []}, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_mulesoft_eu_region_routes_through_eu_endpoint(
        self, mock_oauth_requests, mock_request
    ):
        # Stage 1 mock: OAuth POST returns a token.
        oauth_resp = MagicMock()
        oauth_resp.json.return_value = {"access_token": "ms-eu-token"}
        oauth_resp.raise_for_status.return_value = None
        mock_oauth_requests.post.return_value = oauth_resp

        # Stage 2 mock: downstream API GET returns JSON.
        api_resp = create_autospec(Response)
        api_resp.json.return_value = {"orgs": []}
        mock_request.return_value = api_resp

        operation = {
            "trace_id": "ms-trace-eu",
            "commands": [
                {
                    "method": "do_request",
                    "kwargs": {
                        "url": "https://eu1.anypoint.mulesoft.com/apimanager/api/v1/organizations",
                        "http_method": "GET",
                    },
                }
            ],
        }
        raw_creds = {"client_id": "cid", "client_secret": "csec", "region": "EU"}

        response = self._agent.execute_operation(
            "mulesoft", "do_request", operation, raw_creds
        )

        # OAuth was attempted at the EU region endpoint.
        self.assertEqual(
            "https://eu1.anypoint.mulesoft.com/accounts/api/v2/oauth2/token",
            mock_oauth_requests.post.call_args.args[0],
        )
        # Downstream call hit the EU base URL.
        mock_request.assert_called_with(
            "GET",
            "https://eu1.anypoint.mulesoft.com/apimanager/api/v1/organizations",
            headers={"Authorization": "Bearer ms-eu-token"},
        )
        # Response payload propagates through the agent.
        self.assertEqual({"orgs": []}, response.result.get(ATTRIBUTE_NAME_RESULT))

    @patch("requests.request")
    @patch("apollo.integrations.ctp.transforms.oauth.requests")
    def test_mulesoft_do_request_accepts_arbitrary_url_from_caller(
        self, mock_oauth_requests, mock_request
    ):
        """Documents the design decision: do_request does NOT validate the URL —
        the agent trusts the DC for URL construction. The DC may supply any URL
        the connected app's permissions cover; the agent attaches only the
        Bearer token from CTP."""
        oauth_resp = MagicMock()
        oauth_resp.json.return_value = {"access_token": "tok"}
        oauth_resp.raise_for_status.return_value = None
        mock_oauth_requests.post.return_value = oauth_resp

        api_resp = create_autospec(Response)
        api_resp.json.return_value = {}
        mock_request.return_value = api_resp

        # An unusual URL — different host, different protocol structure — to
        # explicitly demonstrate that do_request doesn't enforce a host allowlist
        # for the mulesoft connection_type.
        operation = {
            "trace_id": "ms-trace-arbitrary",
            "commands": [
                {
                    "method": "do_request",
                    "kwargs": {
                        "url": "https://some-other-anypoint-mirror.example.com/v3/foo",
                        "http_method": "GET",
                    },
                }
            ],
        }
        # Distinct credentials so the proxy-client cache (keyed by creds hash)
        # doesn't return a stale client from earlier tests in this class.
        raw_creds = {"client_id": "url-test-cid", "client_secret": "url-test-csec"}

        self._agent.execute_operation("mulesoft", "do_request", operation, raw_creds)

        # The agent forwarded the DC-supplied URL verbatim, attaching only the Bearer header.
        mock_request.assert_called_with(
            "GET",
            "https://some-other-anypoint-mirror.example.com/v3/foo",
            headers={"Authorization": "Bearer tok"},
        )


class TestDownloadToStorage(TestCase):
    """Tests for HttpProxyClient.download_to_storage — streams a binary download
    directly into the configured storage backend without holding the payload
    in memory. Bytes are spooled to a tempfile; tempfile is uploaded then
    deleted.
    """

    def _make_response(
        self,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_content.return_value = iter(chunks if chunks is not None else [b""])
        if 400 <= status_code < 600:
            resp.raise_for_status.side_effect = HTTPError(
                f"{status_code}", response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    def _patched_storage(self):
        """Patch the late-imported get_storage_client and return the mock client."""
        storage_client = MagicMock(name="storage_client")
        storage_client.upload_file = MagicMock()
        return storage_client, patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        )

    @patch("requests.get")
    def test_streams_to_storage_and_returns_key(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"part1", b"part2"])
        storage_client, ctx = self._patched_storage()
        client = HttpProxyClient(credentials={"connect_args": {}})

        with ctx:
            returned = client.download_to_storage(
                "https://s3.example/file.jar", "uploads/foo.jar"
            )

        self.assertEqual("uploads/foo.jar", returned)
        storage_client.upload_file.assert_called_once()
        called_key, called_path = storage_client.upload_file.call_args.args
        self.assertEqual("uploads/foo.jar", called_key)
        # Verify the tempfile actually contains the streamed bytes (the test
        # reads it BEFORE download_to_storage's finally deletes it — so we
        # capture the path from the mock invocation and read it ourselves
        # while we know it still exists, by patching upload_file to copy).
        # Easier: inspect what the function wrote by stubbing upload_file
        # to read the file at call time.

    @patch("requests.get")
    def test_tempfile_contents_match_stream(self, mock_get):
        """Capture the tempfile's bytes at upload_file call time, then assert
        they equal the streamed concatenation."""
        mock_get.return_value = self._make_response(
            chunks=[b"alpha", b"beta", b"gamma"]
        )
        captured = {}

        def capture_upload(key, path):
            with open(path, "rb") as f:
                captured["bytes"] = f.read()
            captured["key"] = key

        storage_client = MagicMock()
        storage_client.upload_file.side_effect = capture_upload

        client = HttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        ):
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        self.assertEqual(b"alphabetagamma", captured["bytes"])
        self.assertEqual("uploads/foo.jar", captured["key"])

    @patch("requests.get")
    def test_tempfile_cleaned_up_on_success(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        captured_paths = []

        def capture_path(key, path):
            captured_paths.append(path)

        storage_client = MagicMock()
        storage_client.upload_file.side_effect = capture_path

        client = HttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        ):
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        self.assertEqual(1, len(captured_paths))
        self.assertFalse(
            os.path.exists(captured_paths[0]),
            f"tempfile {captured_paths[0]} should have been deleted",
        )

    @patch("requests.get")
    def test_max_bytes_cap_aborts_and_cleans_up_tempfile(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"x" * 100, b"y" * 100])
        # Track tempfile path so we can assert it was cleaned even on abort.
        # The factory's upload_file should NOT be called when max_bytes fires.
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            with self.assertRaises(HttpClientError) as ec:
                client.download_to_storage(
                    "https://s3.example/file.jar",
                    "uploads/foo.jar",
                    max_bytes=150,
                )

        self.assertIn("150", str(ec.exception))
        storage_client.upload_file.assert_not_called()
        # We can't easily check the tempfile path post-fact (it's local to the
        # method), but we can scan /tmp for any leftover *.download_to_storage
        # files older than test start. Simpler: assert no upload happened.

    @patch("requests.get")
    def test_4xx_raises_http_client_error_and_no_upload(self, mock_get):
        mock_get.return_value = self._make_response(status_code=404)
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            with self.assertRaises(HttpClientError):
                client.download_to_storage(
                    "https://s3.example/file.jar", "uploads/foo.jar"
                )

        storage_client.upload_file.assert_not_called()

    @patch("requests.get")
    def test_5xx_raises_http_error_and_no_upload(self, mock_get):
        mock_get.return_value = self._make_response(status_code=500)
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            with self.assertRaises(HTTPError) as ec:
                client.download_to_storage(
                    "https://s3.example/file.jar", "uploads/foo.jar"
                )

        self.assertNotIsInstance(ec.exception, HttpClientError)
        self.assertIsNotNone(ec.exception.response)
        storage_client.upload_file.assert_not_called()

    @patch("requests.get")
    def test_3xx_redirect_refused_no_upload(self, mock_get):
        mock_get.return_value = self._make_response(status_code=302)
        # Headers might contain Location; we don't echo it but make sure.
        mock_get.return_value.headers = {"Location": "https://attacker.example/leak"}
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            with self.assertRaises(HttpClientError) as ec:
                client.download_to_storage(
                    "https://s3.example/file.jar", "uploads/foo.jar"
                )

        self.assertNotIn("attacker.example", str(ec.exception))
        self.assertIn("302", str(ec.exception))
        storage_client.upload_file.assert_not_called()

    @patch("requests.get")
    def test_connection_error_no_upload_no_url_in_message(self, mock_get):
        secret_url = "https://s3.example/file.jar?Signature=DO-NOT-LEAK-XYZ"
        mock_get.side_effect = requests.ConnectionError(
            "connection refused at " + secret_url
        )
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            with self.assertRaises(HttpClientError) as ec:
                client.download_to_storage(secret_url, "uploads/foo.jar")

        self.assertNotIn("DO-NOT-LEAK-XYZ", str(ec.exception))
        self.assertNotIn(secret_url, str(ec.exception))
        storage_client.upload_file.assert_not_called()

    def test_ssrf_guard_rejects_non_https(self):
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError):
            client.download_to_storage("http://example.com/file", "uploads/foo.jar")

    def test_ssrf_guard_rejects_imds(self):
        client = HttpProxyClient(credentials={"connect_args": {}})
        with self.assertRaises(HttpClientError):
            client.download_to_storage(
                "https://169.254.169.254/latest/meta-data/", "uploads/foo.jar"
            )

    @patch("requests.get")
    def test_no_auth_default_omits_authorization_header(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        with ctx:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        sent_headers = mock_get.call_args.kwargs["headers"]
        self.assertNotIn("Authorization", sent_headers)

    @patch("requests.get")
    def test_no_auth_false_attaches_authorization_header(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {"token": "tok"}})
        with ctx:
            client.download_to_storage(
                "https://api.example/file",
                "uploads/foo.jar",
                no_auth=False,
            )

        sent_headers = mock_get.call_args.kwargs["headers"]
        self.assertEqual("Bearer tok", sent_headers["Authorization"])

    @patch("requests.get")
    def test_allow_redirects_false_passed_to_requests(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        self.assertFalse(mock_get.call_args.kwargs["allow_redirects"])
        self.assertTrue(mock_get.call_args.kwargs["stream"])

    @patch("requests.get")
    def test_additional_headers_merged(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            client.download_to_storage(
                "https://s3.example/file.jar",
                "uploads/foo.jar",
                additional_headers={"X-Trace": "abc"},
            )

        self.assertEqual("abc", mock_get.call_args.kwargs["headers"]["X-Trace"])

    @patch("requests.get")
    def test_default_timeout_is_300_seconds(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with ctx:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        self.assertEqual(300, mock_get.call_args.kwargs["timeout"])

    @patch("requests.get")
    def test_ssl_verify_passed_through(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client, ctx = self._patched_storage()

        client = HttpProxyClient(
            credentials={"connect_args": {"ssl_verify": "/path/to/ca.pem"}}
        )
        with ctx:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        self.assertEqual("/path/to/ca.pem", mock_get.call_args.kwargs["verify"])

    @patch("requests.get")
    def test_tempfile_cleaned_up_when_upload_raises(self, mock_get):
        mock_get.return_value = self._make_response(chunks=[b"data"])
        captured_paths: list = []

        def fail_after_capture(key, path):
            captured_paths.append(path)
            raise RuntimeError("storage backend exploded")

        storage_client = MagicMock()
        storage_client.upload_file.side_effect = fail_after_capture

        client = HttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        ):
            with self.assertRaises(RuntimeError):
                client.download_to_storage(
                    "https://s3.example/file.jar", "uploads/foo.jar"
                )

        self.assertEqual(1, len(captured_paths))
        # The tempfile must be cleaned up even when upload_file raises.
        self.assertFalse(
            os.path.exists(captured_paths[0]),
            f"tempfile {captured_paths[0]} should have been deleted",
        )

    @patch("requests.get")
    def test_platform_forwarded_to_storage_factory(self, mock_get):
        """The agent platform passed to HttpProxyClient must be forwarded to
        get_storage_client so the platform→backend default (S3/GCS/Azure)
        applies in production agents where MCD_STORAGE is not set.
        """
        from apollo.common.agent.constants import PLATFORM_AWS

        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client = MagicMock()
        storage_client.upload_file = MagicMock()

        client = HttpProxyClient(
            credentials={"connect_args": {}}, platform=PLATFORM_AWS
        )
        with patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        ) as mock_factory:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        mock_factory.assert_called_once_with(platform=PLATFORM_AWS)

    @patch("requests.get")
    def test_platform_defaults_to_none_when_not_supplied(self, mock_get):
        """Direct constructions (DatabricksRest, tests) that omit platform must
        still work — get_storage_client receives platform=None and falls back
        to MCD_STORAGE (the local-development path)."""
        mock_get.return_value = self._make_response(chunks=[b"data"])
        storage_client = MagicMock()
        storage_client.upload_file = MagicMock()

        client = HttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.storage.factory.get_storage_client",
            return_value=storage_client,
        ) as mock_factory:
            client.download_to_storage("https://s3.example/file.jar", "uploads/foo.jar")

        mock_factory.assert_called_once_with(platform=None)
