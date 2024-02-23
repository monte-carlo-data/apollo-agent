import logging
import uuid
from datetime import datetime, timedelta
from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
)

import jwt
import requests
from tableauserverclient.models.tableau_auth import Credentials
from tableauserverclient.server.server import Server

from apollo.integrations.base_proxy_client import BaseProxyClient

logger = logging.getLogger(__name__)

_DEFAULT_TOKEN_EXPIRATION_SECONDS = 60 * 5  # 5 minutes


def generate_jwt(
    user_name: str,
    client_id: str,
    secret_id: str,
    secret_value: str,
    expiration_seconds: int,
) -> str:
    """
    Generates a new JWT that can be used to authenticate with Tableau Server. See more here:
    https://help.tableau.com/current/online/en-us/connected_apps_direct.htm#step-3-configure-the-jwt
    """
    token_id = str(uuid.uuid4())
    expires_at = datetime.utcnow() + timedelta(seconds=expiration_seconds)

    headers = {"iss": client_id, "kid": secret_id}
    payload = {
        "iss": client_id,
        "exp": expires_at,
        "jti": token_id,
        "aud": "tableau",
        "sub": user_name,
        "scp": ["tableau:content:read", "tableau:users:read"],
    }
    return jwt.encode(
        payload=payload, key=secret_value, algorithm="HS256", headers=headers
    )


class JwtAuth(Credentials):
    """
    Adapted from:
    https://github.com/tableau/server-client-python/blob/3ec49bccdb5cc2fb038476ddd77bcb0e1e32df56/tableauserverclient/models/tableau_auth.py#L91-L107
    """

    def __init__(self, token: str, site_id: Optional[str] = None):
        super().__init__(site_id)
        self.token = token

    @property
    def credentials(self):
        return {"jwt": self.token}

    def __repr__(self):
        return f"<{self.__class__.__qualname__} jwt={self.token[:5]}... (site={self.site_id})>"


class TableauProxyClient(BaseProxyClient):
    """
    Tableau Proxy Client, simple class that uses the received credentials to create a Tableau
    connection. This connection is returned as the `wrapped_client` attribute and the agent will
    take care of executing methods there.
    """

    def __init__(self, credentials: Optional[Dict], **kwargs: Any):  # noqa
        """
        initializing the tableau client. The credentials dictionary should include the following:
        username (string)
        client_id (string)
        secret_id (string)
        secret_value (string)
        server_name (string)
        site_name (string) (default is "")
        verify_ssl (boolean) (default is true)
        expiration_seconds (int) (default is 300)
        """
        if not credentials:
            raise ValueError("Credentials are required for Tableau")

        self._user_name = credentials["username"]
        self._client_id = credentials["client_id"]
        self._secret_id = credentials["secret_id"]
        self._secret_value = credentials["secret_value"]
        self._site_name = credentials.get("site_name", "")
        self._token_expiration_seconds = credentials.get(
            "token_expiration_seconds", _DEFAULT_TOKEN_EXPIRATION_SECONDS
        )
        server_name = credentials["server_name"]
        verify_ssl = credentials.get("verify_ssl", True)
        self._server = Server(server_name)
        self._server.add_http_options({"verify": verify_ssl})
        self._server.use_server_version()

    @property
    def wrapped_client(self):
        return self

    def _sign_in(self, expiration_seconds: int):
        token = generate_jwt(
            user_name=self._user_name,
            client_id=self._client_id,
            secret_id=self._secret_id,
            secret_value=self._secret_value,
            expiration_seconds=expiration_seconds,
        )
        auth = JwtAuth(token=token, site_id=self._site_name)
        self._server.auth.sign_in(auth)

    def metadata_query(
        self, query: str, variables: Optional[Dict] = None, abort_on_error: bool = False
    ) -> Dict:
        self._sign_in(self._token_expiration_seconds)
        return self._server.metadata.query(
            query=query, variables=variables, abort_on_error=abort_on_error
        )

    def api_request(
        self,
        path: str,
        request_method: str,
        content_type: str,
        data: Optional[str] = None,
        params: Optional[str] = None,
    ) -> Tuple[str, int]:
        self._sign_in(self._token_expiration_seconds)
        headers = {
            "X-Tableau-Auth": self._server.auth_token,
            "Content-Type": content_type,
        }
        url = f"{self._server.baseurl}/sites/{self._server.site_id}/{path}"
        response = requests.request(
            method=request_method, url=url, data=data, headers=headers, params=params
        )
        return response.text, response.status_code
