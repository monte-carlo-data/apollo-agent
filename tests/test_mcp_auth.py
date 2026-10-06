import base64
import json
from typing import Any
from unittest import TestCase
from unittest.mock import patch

import httpx
import requests
from botocore.credentials import Credentials
from botocore.exceptions import ClientError, NoCredentialsError

from apollo.integrations.aws.aws_utils import AwsSession
from apollo.integrations.http.url_safety import HttpClientError
from apollo.integrations.mcp.auth import (
    SigV4HttpxAuth,
    aws_role_session_name,
    resolve_auth,
)
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode

_AWS_HOST = "aws-mcp.us-east-1.api.aws"
_ROLE = "arn:aws:iam::123456789012:role/narrow"
_SIGV4 = {"type": "aws_sigv4", "assumable_role": _ROLE}


class TestResolveAuth(TestCase):
    def test_none(self):
        resolved = resolve_auth({"type": "none"}, {}, "mcp.example.com")
        self.assertIsNone(resolved.httpx_auth)
        self.assertEqual({}, resolved.headers)
        self.assertEqual({}, resolved.meta)

    def test_missing_auth_is_none(self):
        self.assertIsNone(resolve_auth({}, {}, "mcp.example.com").httpx_auth)

    def test_unsupported_type(self):
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth({"type": "magic"}, {}, "mcp.example.com")
        self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)


class TestSecretHeader(TestCase):
    def test_header_from_connect_args(self):
        resolved = resolve_auth(
            {"type": "secret_header", "header_name": "X-Api-Key"},
            {"header_value": "s3cret"},
            "mcp.example.com",
        )
        self.assertEqual({"X-Api-Key": "s3cret"}, resolved.headers)

    def test_defaults_to_authorization(self):
        resolved = resolve_auth(
            {"type": "secret_header"}, {"header_value": "Bearer t"}, "mcp.example.com"
        )
        self.assertEqual({"Authorization": "Bearer t"}, resolved.headers)

    def test_reserved_header_refused(self):
        for name in ("Host", "Mcp-Session-Id", "X-Amz-Security-Token", "Content-Type"):
            with self.assertRaises(McpClientError, msg=name) as ctx:
                resolve_auth(
                    {"type": "secret_header", "header_name": name},
                    {"header_value": "v"},
                    "mcp.example.com",
                )
            self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)

    def test_missing_value(self):
        with self.assertRaises(McpClientError):
            resolve_auth({"type": "secret_header"}, {}, "mcp.example.com")


@patch("apollo.integrations.mcp.auth.assume_role")
class TestAwsSigV4(TestCase):
    def test_assumes_the_role_with_a_stable_session_name(self, mock_assume):
        mock_assume.return_value = AwsSession("AKIA_TEST", "secret", "token")

        resolved = resolve_auth(
            {**_SIGV4, "external_id": "ext"}, {}, "AWS-MCP.us-east-1.api.aws"
        )

        mock_assume.assert_called_once_with(
            _ROLE, external_id="ext", session_name=aws_role_session_name(_ROLE)
        )
        self.assertIsInstance(resolved.httpx_auth, SigV4HttpxAuth)
        self.assertEqual({"AWS_REGION": "us-east-1"}, resolved.meta)
        self.assertIsNotNone(resolved.assume_role_ms)

    def test_external_id_from_connect_args_wins(self, mock_assume):
        mock_assume.return_value = AwsSession("AKIA_TEST", "secret", "token")
        resolve_auth({**_SIGV4, "external_id": "reg"}, {"external_id": "ss"}, _AWS_HOST)
        self.assertEqual("ss", mock_assume.call_args.kwargs["external_id"])

    def test_requires_assumable_role_and_never_uses_ambient_credentials(
        self, mock_assume
    ):
        for auth in (
            {"type": "aws_sigv4"},
            {"type": "aws_sigv4", "assumable_role": ""},
        ):
            with self.assertRaises(McpClientError) as ctx:
                resolve_auth(auth, {}, _AWS_HOST)
            self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)
        mock_assume.assert_not_called()

    def test_only_for_aws_mcp_hosts(self, mock_assume):
        for host in ("mcp.example.com", "aws-mcp.us-east-1.api.aws.evil.com"):
            with self.assertRaises(McpClientError, msg=host) as ctx:
                resolve_auth(_SIGV4, {}, host)
            self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)
        mock_assume.assert_not_called()

    def test_region_must_match_host(self, mock_assume):
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth({**_SIGV4, "region": "eu-west-1"}, {}, _AWS_HOST)
        self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)
        mock_assume.return_value = AwsSession("AKIA_TEST", "secret", "token")
        resolve_auth({**_SIGV4, "region": "us-east-1"}, {}, _AWS_HOST)

    def test_access_denied(self, mock_assume):
        mock_assume.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "AssumeRole"
        )
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth(_SIGV4, {}, _AWS_HOST)
        self.assertEqual(McpErrorCode.AWS_ACCESS_DENIED, ctx.exception.code)

    def test_agent_without_aws_credentials(self, mock_assume):
        # e.g. a GCP or Azure agent: nothing to assume the role with
        mock_assume.side_effect = NoCredentialsError()
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth(_SIGV4, {}, _AWS_HOST)
        self.assertEqual(McpErrorCode.AUTH_CONFIG, ctx.exception.code)
        self.assertIn("no AWS credentials", str(ctx.exception))

    def test_session_name(self, _):
        name = aws_role_session_name(_ROLE)
        self.assertEqual(name, aws_role_session_name(_ROLE))
        self.assertNotEqual(name, aws_role_session_name(_ROLE + "-other"))
        self.assertLessEqual(len(name), 64)
        self.assertRegex(name, r"^[\w+=,.@-]+$")


class TestSigV4HttpxAuth(TestCase):
    def test_signs_for_aws_mcp_without_the_connection_header(self):
        auth = SigV4HttpxAuth(
            Credentials("AKIA_TEST", "secret", "token"), region="us-east-1"
        )
        request = httpx.Request(
            "POST",
            f"https://{_AWS_HOST}/mcp",
            headers={"connection": "keep-alive", "content-type": "application/json"},
            content=b'{"jsonrpc": "2.0"}',
        )

        signed = next(auth.auth_flow(request))

        authorization = signed.headers["authorization"]
        self.assertIn("Credential=AKIA_TEST/", authorization)
        self.assertIn("/us-east-1/aws-mcp/aws4_request", authorization)
        signed_headers = authorization.split("SignedHeaders=")[1].split(",")[0]
        self.assertNotIn("connection", signed_headers.split(";"))
        self.assertIn("content-type", signed_headers.split(";"))
        self.assertEqual("token", signed.headers["x-amz-security-token"])
        self.assertIn("x-amz-date", signed.headers)


def _token_response(status: int, body: Any = None, headers=None) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode() if body is not None else b""
    response.headers.update(headers or {})
    return response


_OAUTH = {
    "type": "oauth_client_credentials",
    "token_url": "https://idp.example.com/oauth/token",
    "client_id": "client-1",
    "scope": "mcp.read",
}


@patch.dict("os.environ", {"MCD_MCP_ALLOWED_HOSTS": "idp.example.com"})
@patch("apollo.integrations.mcp.auth.safe_request")
class TestOAuthClientCredentials(TestCase):
    def test_fetches_a_bearer_token(self, mock_request):
        mock_request.return_value = _token_response(
            200, {"access_token": "tok-1", "token_type": "Bearer", "expires_in": 3600}
        )

        resolved = resolve_auth(
            {**_OAUTH, "audience": "https://mcp.example.com"},
            {"client_secret": "s3cret"},
            "mcp.example.com",
        )

        self.assertEqual({"Authorization": "Bearer tok-1"}, resolved.headers)
        mock_request.assert_called_once()
        args, kwargs = mock_request.call_args
        self.assertEqual(("POST", "https://idp.example.com/oauth/token"), args)
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(
            {
                "grant_type": "client_credentials",
                "scope": "mcp.read",
                "audience": "https://mcp.example.com",
            },
            kwargs["data"],
        )
        expected = base64.b64encode(b"client-1:s3cret").decode()
        self.assertEqual(f"Basic {expected}", kwargs["headers"]["Authorization"])

    def test_client_id_from_connect_args_wins(self, mock_request):
        mock_request.return_value = _token_response(200, {"access_token": "t"})
        resolve_auth(
            _OAUTH,
            {"client_id": "self-hosted", "client_secret": "s"},
            "mcp.example.com",
        )
        header = mock_request.call_args.kwargs["headers"]["Authorization"]
        self.assertEqual(
            base64.b64encode(b"self-hosted:s").decode(), header.split(" ")[1]
        )

    def test_token_errors(self, mock_request):
        for response in (
            _token_response(401, {"error": "invalid_client"}),
            _token_response(307, headers={"location": "https://evil.example.com/t"}),
            _token_response(200, {"token_type": "Bearer"}),
            _token_response(200, {"access_token": "t", "token_type": "mac"}),
        ):
            mock_request.return_value = response
            with self.assertRaises(McpClientError, msg=response.status_code) as ctx:
                resolve_auth(_OAUTH, {"client_secret": "s"}, "mcp.example.com")
            self.assertEqual(McpErrorCode.AUTH_FAILED, ctx.exception.code)
            self.assertNotIn("s3cret", str(ctx.exception))

    def test_request_failure(self, mock_request):
        mock_request.side_effect = requests.ConnectionError("down")
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth(_OAUTH, {"client_secret": "s"}, "mcp.example.com")
        self.assertEqual(McpErrorCode.AUTH_FAILED, ctx.exception.code)

    def test_ssrf_block(self, mock_request):
        mock_request.side_effect = HttpClientError(
            "Destination resolves to 169.254.169.254"
        )
        with self.assertRaises(McpClientError) as ctx:
            resolve_auth(_OAUTH, {"client_secret": "s"}, "mcp.example.com")
        self.assertEqual(McpErrorCode.SERVER_NOT_ALLOWED, ctx.exception.code)

    def test_config_errors(self, mock_request):
        for auth, connect_args in (
            (
                {**_OAUTH, "token_url": "https://other.example.com/token"},
                {"client_secret": "s"},
            ),
            (
                {**_OAUTH, "token_url": "http://idp.example.com/token"},
                {"client_secret": "s"},
            ),
            (_OAUTH, {}),
            (
                {k: v for k, v in _OAUTH.items() if k != "client_id"},
                {"client_secret": "s"},
            ),
        ):
            with self.assertRaises(McpClientError) as ctx:
                resolve_auth(auth, connect_args, "mcp.example.com")
            self.assertIn(
                ctx.exception.code,
                (McpErrorCode.AUTH_CONFIG, McpErrorCode.SERVER_NOT_ALLOWED),
            )
        mock_request.assert_not_called()
