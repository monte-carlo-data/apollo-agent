"""
Auth providers for the agent-to-MCP-server hop. The auth config comes from the
server registration; secrets come from `connect_args` after the agent resolves
self-hosted credentials, never from the model.
"""

import base64
import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Optional

import httpx
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError

from apollo.integrations.aws.aws_utils import assume_role
from apollo.integrations.http.url_safety import HttpClientError, safe_request
from apollo.integrations.mcp.allowlist import (
    check_server_url,
    get_allowed_host_patterns,
)
from apollo.integrations.mcp.errors import McpClientError, McpErrorCode

AUTH_NONE = "none"
AUTH_SECRET_HEADER = "secret_header"
AUTH_AWS_SIGV4 = "aws_sigv4"
AUTH_OAUTH_CLIENT_CREDENTIALS = "oauth_client_credentials"

_TOKEN_REQUEST_TIMEOUT_SECONDS = 10

AWS_MCP_SIGNING_SERVICE = "aws-mcp"
_AWS_MCP_HOST = re.compile(r"^aws-mcp\.([a-z0-9-]+)\.api\.aws$")

# Headers the MCP transport or SigV4 own; a registration can't set them.
_RESERVED_HEADERS = frozenset(
    {
        "accept",
        "content-length",
        "content-type",
        "host",
        "mcp-protocol-version",
        "mcp-session-id",
        "x-amz-date",
        "x-amz-security-token",
    }
)


@dataclass
class ResolvedAuth:
    httpx_auth: Optional[httpx.Auth] = None
    headers: Dict[str, str] = field(default_factory=dict)
    # `_meta` sent with every tools/call
    meta: Dict[str, Any] = field(default_factory=dict)
    assume_role_ms: Optional[int] = None


def resolve_auth(
    auth_config: Dict[str, Any], connect_args: Dict[str, Any], host: str
) -> ResolvedAuth:
    auth_type = auth_config.get("type") or AUTH_NONE
    if auth_type == AUTH_NONE:
        return ResolvedAuth()
    if auth_type == AUTH_SECRET_HEADER:
        return _secret_header(auth_config, connect_args)
    if auth_type == AUTH_AWS_SIGV4:
        return _aws_sigv4(auth_config, connect_args, host)
    if auth_type == AUTH_OAUTH_CLIENT_CREDENTIALS:
        return _oauth_client_credentials(auth_config, connect_args)
    raise McpClientError(
        McpErrorCode.AUTH_CONFIG, f"Unsupported MCP auth type: {auth_type}"
    )


def _secret_header(
    auth_config: Dict[str, Any], connect_args: Dict[str, Any]
) -> ResolvedAuth:
    name = auth_config.get("header_name") or "Authorization"
    if name.lower() in _RESERVED_HEADERS:
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG, f"Header can't be set by secret_header: {name}"
        )
    value = connect_args.get("header_value")
    if not value:
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG, "secret_header requires a header value"
        )
    return ResolvedAuth(headers={name: value})


def _aws_sigv4(
    auth_config: Dict[str, Any], connect_args: Dict[str, Any], host: str
) -> ResolvedAuth:
    match = _AWS_MCP_HOST.match(host.lower().rstrip("."))
    if not match:
        # SigV4 signatures for the assumed role only ever go to the AWS MCP Server
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG,
            "aws_sigv4 is only supported for aws-mcp.<region>.api.aws",
        )
    region = match.group(1)
    if auth_config.get("region") and auth_config["region"] != region:
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG,
            f"aws_sigv4 region {auth_config['region']} doesn't match the server URL ({region})",
        )
    role = auth_config.get("assumable_role")
    if not role:
        # Never fall back to the agent's own credentials: its execution role can
        # assume every MonteCarloData-tagged role and read the agent bucket, and
        # aws___run_script runs model-written code with the signing credentials.
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG, "aws_sigv4 requires assumable_role"
        )

    start = time.perf_counter()
    try:
        session = assume_role(
            role,
            external_id=connect_args.get("external_id")
            or auth_config.get("external_id"),
            session_name=aws_role_session_name(role),
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "ClientError")
        raise McpClientError(
            (
                McpErrorCode.AWS_ACCESS_DENIED
                if error_code == "AccessDenied"
                else McpErrorCode.AUTH_CONFIG
            ),
            f"Could not assume {role}: {error_code}",
        ) from exc
    except BotoCoreError as exc:
        # e.g. NoCredentialsError on a GCP or Azure agent
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG,
            f"Could not assume {role}: no AWS credentials available to the agent "
            f"({type(exc).__name__})",
        ) from exc
    credentials = Credentials(
        session.access_key_id, session.secret_key, session.session_token
    )
    return ResolvedAuth(
        httpx_auth=SigV4HttpxAuth(credentials, region=region),
        # the AWS MCP Server uses it as the default region for call_boto3
        meta={"AWS_REGION": region},
        assume_role_ms=int((time.perf_counter() - start) * 1000),
    )


def _oauth_client_credentials(
    auth_config: Dict[str, Any], connect_args: Dict[str, Any]
) -> ResolvedAuth:
    """
    OAuth 2.0 client credentials grant, fetched per call (no token cache). The
    client secret comes from the customer's secret store; the client
    authenticates with HTTP Basic, like the CTP OAuth transform.
    """
    token_url = auth_config.get("token_url") or ""
    client_id = connect_args.get("client_id") or auth_config.get("client_id")
    client_secret = connect_args.get("client_secret")
    if not client_id or not client_secret:
        raise McpClientError(
            McpErrorCode.AUTH_CONFIG,
            "oauth_client_credentials requires client_id and client_secret",
        )
    # the token endpoint receives the client secret, so it must be allowlisted too
    check_server_url(token_url, get_allowed_host_patterns())

    data = {"grant_type": "client_credentials"}
    for key in ("scope", "audience"):
        if auth_config.get(key):
            data[key] = auth_config[key]
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode()
    try:
        response = safe_request(
            "POST",
            token_url,
            data=data,
            headers={"Authorization": f"Basic {basic}", "Accept": "application/json"},
            timeout=_TOKEN_REQUEST_TIMEOUT_SECONDS,
            # a redirect would carry the client secret past the allowlist
            allow_redirects=False,
        )
    except HttpClientError as exc:
        raise McpClientError(McpErrorCode.SERVER_NOT_ALLOWED, str(exc)) from exc
    except requests.RequestException as exc:
        raise McpClientError(
            McpErrorCode.AUTH_FAILED, f"Token request failed: {type(exc).__name__}"
        ) from exc

    if response.status_code != 200:
        raise McpClientError(
            McpErrorCode.AUTH_FAILED,
            f"Token endpoint returned HTTP {response.status_code}",
        )
    try:
        body = response.json()
    except ValueError as exc:
        raise McpClientError(
            McpErrorCode.AUTH_FAILED, "Token endpoint returned invalid JSON"
        ) from exc
    token = body.get("access_token") if isinstance(body, dict) else None
    token_type = (
        body.get("token_type") if isinstance(body, dict) else None
    ) or "Bearer"
    if not token or token_type.lower() != "bearer":
        raise McpClientError(
            McpErrorCode.AUTH_FAILED, "Token endpoint returned no bearer token"
        )
    return ResolvedAuth(headers={"Authorization": f"Bearer {token}"})


def aws_role_session_name(role_arn: str) -> str:
    """
    The AWS MCP Server binds a session to the assumed-role session (role ARN +
    RoleSessionName), so a stable name per role lets a later request, with freshly
    assumed credentials, resume the session.
    """
    return "mcd_mcp_" + hashlib.sha256(role_arn.encode("utf-8")).hexdigest()[:16]


class SigV4HttpxAuth(httpx.Auth):
    """Signs every MCP HTTP request with SigV4, like mcp-proxy-for-aws."""

    requires_request_body = True

    def __init__(
        self,
        credentials: Credentials,
        region: str,
        service: str = AWS_MCP_SIGNING_SERVICE,
    ):
        self._signer = SigV4Auth(credentials, service, region)

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        headers = dict(request.headers)
        # hop-by-hop, not part of the server's canonical request: signing it
        # breaks the signature
        headers.pop("connection", None)
        aws_request = AWSRequest(
            method=request.method,
            url=str(request.url),
            data=request.content,
            headers=headers,
        )
        self._signer.add_auth(aws_request)
        request.headers.update(dict(aws_request.headers))
        yield request
