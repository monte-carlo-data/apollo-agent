"""JWT Bearer auth for the Salesforce Data Cloud proxy client (YET-3269).

The auth method is chosen per credential: ``private_key`` + ``username`` mint the
core token with the JWT Bearer grant, ``client_secret`` alone keeps the Client
Credentials grant, and a credential carrying both uses JWT (the precedence of
``salesforce-cdp-connector``'s ``_get_token``).
"""

import json
import logging
import os
import tempfile
import time
import urllib.parse
from typing import Any
from unittest import TestCase
from unittest.mock import MagicMock, Mock, patch

import jwt
import responses
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from apollo.agent.agent import Agent
from apollo.agent.logging_utils import LoggingUtils
from apollo.agent.proxy_client_factory import _get_proxy_client_salesforce_data_cloud
from apollo.credentials.external_credentials_cache import (
    clear_external_credentials_cache as clear_cache,
)
from apollo.credentials.factory import CredentialsFactory
from apollo.credentials.schema.validator import validate
from apollo.integrations.db import salesforce_data_cloud_proxy_client as sfdc_module
from apollo.integrations.db.salesforce_data_cloud_proxy_client import (
    _CORE_TOKEN_CACHE,
    _JWT_SIGNING_KEY_CACHE,
    _JWT_BEARER_GRANT_TYPE,
    _JWT_BEARER_LIFETIME_SECONDS,
    SALESFORCE_DATA_CLOUD_CREDENTIALS_SCHEMA,
    SalesforceDataCloudCredentials,
    SalesforceDataCloudProxyClient,
)

_DOMAIN = "test.salesforce.com"
_TOKEN_URL = f"https://{_DOMAIN}/services/oauth2/token"
_SSOT_PATH = "/services/data/v62.0/ssot/data-streams"
_SSOT_URL = f"https://{_DOMAIN}{_SSOT_PATH}"
_USERNAME = "integration.user@example.com"


def _rsa_pem() -> tuple[str, Any]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    return pem, key.public_key()


_PEM, _PUBLIC_KEY = _rsa_pem()
_OTHER_PEM, _ = _rsa_pem()


def _jwt_credentials(**overrides: Any) -> SalesforceDataCloudCredentials:
    fields: dict[str, Any] = {
        "domain": _DOMAIN,
        "client_id": "test_client_id",
        "client_secret": None,
        "core_token": None,
        "refresh_token": None,
        "private_key": _PEM,
        "username": _USERNAME,
    }
    fields.update(overrides)
    return SalesforceDataCloudCredentials(**fields)


def _client_credentials(**overrides: Any) -> SalesforceDataCloudCredentials:
    fields: dict[str, Any] = {
        "domain": _DOMAIN,
        "client_id": "test_client_id",
        "client_secret": "test_client_secret",
        "core_token": None,
        "refresh_token": None,
    }
    fields.update(overrides)
    return SalesforceDataCloudCredentials(**fields)


class _MockedSalesforceTestCase(TestCase):
    def setUp(self):
        _CORE_TOKEN_CACHE.clear()
        self.addCleanup(_CORE_TOKEN_CACHE.clear)

        self.mock_responses = responses.RequestsMock(
            assert_all_requests_are_fired=False
        )
        self.mock_responses.start()
        self.addCleanup(self.mock_responses.stop)
        self.addCleanup(self.mock_responses.reset)

        self.minted: list[str] = []

        def _mint(request):
            token = f"core-token-{len(self.minted)}"
            self.minted.append(token)
            body = {"access_token": token, "instance_url": f"https://{_DOMAIN}"}
            return 200, {}, json.dumps(body)

        self.token_endpoint = Mock(side_effect=_mint)
        self.mock_responses.add_callback(
            responses.POST, _TOKEN_URL, callback=self.token_endpoint
        )
        self.mock_responses.add(
            responses.GET, _SSOT_URL, json={"dataStreams": []}, status=200
        )

    def _token_requests(self) -> list[dict[str, str]]:
        """Form fields of every POST to the token endpoint (body or query string)."""
        forms = []
        for call in self.mock_responses.calls:
            if not call.request.url.startswith(_TOKEN_URL):
                continue
            body = call.request.body or ""
            if isinstance(body, bytes):
                body = body.decode()
            query = urllib.parse.urlsplit(call.request.url).query
            forms.append(dict(urllib.parse.parse_qsl(body or query)))
        return forms

    def _replace_token_endpoint(self, status: int, body: str) -> None:
        self.mock_responses.remove(responses.POST, _TOKEN_URL)
        self.mock_responses.add(responses.POST, _TOKEN_URL, status=status, body=body)


class JwtBearerMintTests(_MockedSalesforceTestCase):
    def test_jwt_credentials_mint_with_the_jwt_bearer_grant(self):
        client = SalesforceDataCloudProxyClient(_jwt_credentials())

        before = int(time.time())
        result = client.ssot_get(_SSOT_PATH)
        after = int(time.time())

        self.assertEqual(result, {"dataStreams": []})
        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)
        self.assertEqual(
            set(form), {"grant_type", "assertion"}, "no client_id/secret on JWT"
        )
        claims = jwt.decode(
            form["assertion"],
            _PUBLIC_KEY,
            algorithms=["RS256"],
            audience=f"https://{_DOMAIN}",
        )
        self.assertEqual(claims["iss"], "test_client_id")
        self.assertEqual(claims["sub"], _USERNAME)
        self.assertEqual(claims["aud"], f"https://{_DOMAIN}")
        self.assertGreaterEqual(claims["exp"], before + _JWT_BEARER_LIFETIME_SECONDS)
        self.assertLessEqual(claims["exp"], after + _JWT_BEARER_LIFETIME_SECONDS)
        self.assertEqual(_JWT_BEARER_LIFETIME_SECONDS, 180)
        ssot_auth = [
            c.request.headers["Authorization"]
            for c in self.mock_responses.calls
            if c.request.url.startswith(_SSOT_URL)
        ]
        self.assertEqual(ssot_auth, ["Bearer core-token-0"])

    def test_list_dataspaces_mints_with_the_jwt_bearer_grant(self):
        self.mock_responses.add(
            responses.GET,
            f"https://{_DOMAIN}/services/data/v62.0/query",
            json={"records": [{"DataSpaceApiName": "default"}], "done": True},
        )
        client = SalesforceDataCloudProxyClient(_jwt_credentials())

        self.assertEqual(client.list_dataspaces(), ["default"])
        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)

    def test_jwt_mint_non_200_raises_code_and_never_leaks_key_or_assertion(self):
        self._replace_token_endpoint(
            400,
            json.dumps(
                {"error": "invalid_grant", "error_description": "invalid assertion"}
            ),
        )
        client = SalesforceDataCloudProxyClient(_jwt_credentials())

        with self.assertLogs(sfdc_module.logger, level=logging.DEBUG) as logs:
            with self.assertRaises(RuntimeError) as ctx:
                client.ssot_get(_SSOT_PATH)

        message = str(ctx.exception)
        self.assertIn("code 400", message)
        self.assertIn("invalid assertion", message)
        (form,) = self._token_requests()
        everything_logged = "\n".join(logs.output) + "".join(
            json.dumps(getattr(r, "__dict__", {}), default=str) for r in logs.records
        )
        for secret in (form["assertion"], "PRIVATE KEY", _PEM.splitlines()[1]):
            self.assertNotIn(secret, message)
            self.assertNotIn(secret, everything_logged)
        warning = next(r for r in logs.records if r.levelno == logging.WARNING)
        self.assertEqual(getattr(warning, "auth_method"), "jwt_bearer")

    def test_jwt_mint_200_without_access_token_raises(self):
        self._replace_token_endpoint(200, json.dumps({"instance_url": "x"}))
        client = SalesforceDataCloudProxyClient(_jwt_credentials())

        with self.assertRaises(RuntimeError) as ctx:
            client.ssot_get(_SSOT_PATH)

        self.assertIn("missing access_token", str(ctx.exception))

    def test_credentials_with_both_secret_and_private_key_use_jwt(self):
        client = SalesforceDataCloudProxyClient(
            _jwt_credentials(client_secret="test_client_secret")
        )

        client.ssot_get(_SSOT_PATH)

        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)
        self.assertNotIn("client_secret", form)

    def test_client_credentials_are_unchanged(self):
        client = SalesforceDataCloudProxyClient(_client_credentials())

        client.ssot_get(_SSOT_PATH)

        (form,) = self._token_requests()
        self.assertEqual(
            form,
            {
                "grant_type": "client_credentials",
                "client_id": "test_client_id",
                "client_secret": "test_client_secret",
            },
        )

    def test_auth_method_is_selected_per_credential_in_one_process(self):
        jwt_client = SalesforceDataCloudProxyClient(_jwt_credentials())
        cc_client = SalesforceDataCloudProxyClient(_client_credentials())

        jwt_client.ssot_get(_SSOT_PATH)
        cc_client.ssot_get(_SSOT_PATH)
        jwt_client.ssot_get(_SSOT_PATH)  # served from the JWT client's cache entry

        grants = [f["grant_type"] for f in self._token_requests()]
        self.assertEqual(grants, [_JWT_BEARER_GRANT_TYPE, "client_credentials"])
        ssot_auth = [
            c.request.headers["Authorization"]
            for c in self.mock_responses.calls
            if c.request.url.startswith(_SSOT_URL)
        ]
        self.assertEqual(
            ssot_auth,
            ["Bearer core-token-0", "Bearer core-token-1", "Bearer core-token-0"],
        )


class JwtSigningKeyCacheTests(TestCase):
    def setUp(self):
        _JWT_SIGNING_KEY_CACHE.clear()
        self.addCleanup(_JWT_SIGNING_KEY_CACHE.clear)

    def test_key_is_parsed_once_per_process_not_per_client(self):
        """The DC's skip_cache=true builds a client per op; RSA key loading is
        ~40ms, so it must not run on every construction."""
        with patch.object(
            sfdc_module,
            "_load_jwt_signing_key",
            wraps=sfdc_module._load_jwt_signing_key,
        ) as load:
            first = SalesforceDataCloudProxyClient(_jwt_credentials())
            second = SalesforceDataCloudProxyClient(_jwt_credentials())

        self.assertEqual(load.call_count, 1)
        self.assertIs(first._jwt_signing_key, second._jwt_signing_key)
        self.assertNotIn(_PEM, _JWT_SIGNING_KEY_CACHE)

    def test_invalid_key_is_not_cached(self):
        bogus = "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----"
        for _ in range(2):
            with self.assertRaises(ValueError):
                SalesforceDataCloudProxyClient(_jwt_credentials(private_key=bogus))
        self.assertEqual(_JWT_SIGNING_KEY_CACHE, {})


class JwtBearerCacheKeyTests(TestCase):
    def _key(self, credentials: SalesforceDataCloudCredentials) -> str:
        return SalesforceDataCloudProxyClient(credentials)._core_token_cache_key

    def test_same_jwt_credential_shares_a_cache_key(self):
        self.assertEqual(self._key(_jwt_credentials()), self._key(_jwt_credentials()))

    def test_jwt_cache_key_varies_with_key_user_app_and_domain(self):
        base = self._key(_jwt_credentials())
        for overrides in (
            {"private_key": _OTHER_PEM},
            {"username": "someone.else@example.com"},
            {"client_id": "other_client_id"},
            {"domain": "other.my.salesforce.com"},
        ):
            with self.subTest(overrides=list(overrides)):
                self.assertNotEqual(self._key(_jwt_credentials(**overrides)), base)

    def test_jwt_and_client_credentials_never_share_a_cache_key(self):
        self.assertNotEqual(
            self._key(_jwt_credentials(client_secret="test_client_secret")),
            self._key(_client_credentials()),
        )

    def test_jwt_cache_key_is_not_derived_from_the_private_key_bytes(self):
        """The key is a fingerprint of the PUBLIC key: re-encoding the same private
        key (PKCS#1 vs PKCS#8 PEM) must not change it, which a hash of the PEM
        text would."""
        key = serialization.load_pem_private_key(_PEM.encode(), password=None)
        pkcs1 = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()
        self.assertNotEqual(pkcs1, _PEM)
        self.assertEqual(
            self._key(_jwt_credentials(private_key=pkcs1)),
            self._key(_jwt_credentials()),
        )


class JwtBearerValidationTests(TestCase):
    def test_private_key_without_username_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "username"):
            _jwt_credentials(username=None)

    def test_neither_secret_nor_private_key_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "client_secret.*private_key"):
            _client_credentials(client_secret=None)

    def test_invalid_pem_is_rejected_without_echoing_it(self):
        bogus = "-----BEGIN PRIVATE KEY-----\nnot-a-key\n-----END PRIVATE KEY-----"
        with self.assertRaises(ValueError) as ctx:
            SalesforceDataCloudProxyClient(_jwt_credentials(private_key=bogus))
        self.assertIn("private_key", str(ctx.exception))
        self.assertNotIn("not-a-key", str(ctx.exception))

    def test_non_rsa_key_is_rejected(self):
        ec_pem = (
            ec.generate_private_key(ec.SECP256R1())
            .private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            .decode()
        )
        with self.assertRaisesRegex(ValueError, "RSA"):
            SalesforceDataCloudProxyClient(_jwt_credentials(private_key=ec_pem))

    def test_factory_rejects_jwt_credentials_missing_username(self):
        with self.assertRaisesRegex(ValueError, "username"):
            _get_proxy_client_salesforce_data_cloud(
                {
                    "connect_args": {
                        "domain": _DOMAIN,
                        "client_id": "test_client_id",
                        "private_key": _PEM,
                    }
                }
            )

    def test_factory_keeps_its_error_for_a_payload_with_no_secret_or_key(self):
        with self.assertRaisesRegex(
            ValueError, "Missing required connection parameters"
        ):
            _get_proxy_client_salesforce_data_cloud(
                {"connect_args": {"domain": _DOMAIN, "client_id": "test_client_id"}}
            )

    def test_mint_fails_locally_if_neither_secret_nor_key_reaches_it(self):
        client = SalesforceDataCloudProxyClient(_client_credentials())
        client._credentials.client_secret = None

        with self.assertRaisesRegex(ValueError, "no client_secret or private_key"):
            client._core_token_grant()

    def test_factory_still_requires_domain_and_client_id(self):
        with self.assertRaisesRegex(
            ValueError, "Missing required connection parameters"
        ):
            _get_proxy_client_salesforce_data_cloud(
                {
                    "connect_args": {
                        "domain": _DOMAIN,
                        "private_key": _PEM,
                        "username": "u",
                    }
                }
            )


class JwtBearerLibraryPassthroughTests(_MockedSalesforceTestCase):
    """The ``salesforce-cdp-connector`` connection must get private_key/username
    so the library picks ``_token_by_jwt_bearer_flow`` for query/metadata traffic."""

    def setUp(self):
        super().setUp()
        self.mock_responses.add(
            responses.POST,
            f"https://{_DOMAIN}/services/a360/token",
            json={
                "access_token": "a360-token",
                "expires_in": 3600,
                "instance_url": _DOMAIN,
            },
        )
        self.mock_responses.add(
            responses.GET,
            f"https://{_DOMAIN}/api/v1/metadata",
            json={"metadata": []},
        )

    def test_forwarded_connect_args_reach_the_library_connection(self):
        client = _get_proxy_client_salesforce_data_cloud(
            {
                "connect_args": {
                    "domain": _DOMAIN,
                    "client_id": "test_client_id",
                    "private_key": _PEM,
                    "username": _USERNAME,
                }
            }
        )
        connection = client.wrapped_client
        self.assertEqual(connection.private_key, _PEM)
        self.assertEqual(connection.username, _USERNAME)
        self.assertIsNone(connection.client_secret)

    def test_library_query_path_authenticates_with_jwt(self):
        agent = Agent(LoggingUtils())
        response = agent.execute_operation(
            connection_type="salesforce-data-cloud",
            operation_name="test_list_tables_jwt",
            operation_dict={
                "trace_id": "t",
                "skip_cache": True,
                "commands": [{"method": "list_tables"}],
            },
            credentials={
                "connect_args": {
                    "domain": _DOMAIN,
                    "client_id": "test_client_id",
                    "private_key": _PEM,
                    "username": _USERNAME,
                }
            },
        )

        self.assertFalse(response.is_error, msg=str(response.result))
        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)

    def test_jwt_credential_with_a_forwarded_core_token_takes_the_exchange_path(
        self,
    ):
        """A DC that prefetches a core token sends it alongside the JWT fields.
        The connection then exchanges that token (no JWT mint, private_key not
        given to the library) and a failed exchange raises rather than re-mints."""
        client = _get_proxy_client_salesforce_data_cloud(
            {
                "connect_args": {
                    "domain": _DOMAIN,
                    "client_id": "test_client_id",
                    "private_key": _PEM,
                    "username": _USERNAME,
                    "core_token": "forwarded-core-token",
                }
            }
        )
        connection = client.wrapped_client
        self.assertIsNone(connection.private_key)
        self.assertEqual(connection.core_token, "forwarded-core-token")

        client.list_tables()

        self.assertEqual(self._token_requests(), [])
        (exchange,) = [
            c
            for c in self.mock_responses.calls
            if c.request.url.startswith(f"https://{_DOMAIN}/services/a360/token")
        ]
        self.assertIn("forwarded-core-token", exchange.request.url)

    def test_dataspace_scoped_list_tables_authenticates_with_jwt(self):
        """list_tables(dataspace=...) builds its own temporary connection; it must
        carry the JWT fields too, or a JWT-only credential cannot authenticate."""
        client = SalesforceDataCloudProxyClient(_jwt_credentials())

        client.list_tables(dataspace="CSG")

        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)

    def test_dataspace_scoped_list_tables_with_both_modes_uses_jwt(self):
        client = SalesforceDataCloudProxyClient(
            _jwt_credentials(client_secret="test_client_secret")
        )

        client.list_tables(dataspace="CSG")

        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)

    def test_unscoped_list_tables_on_a_dataspace_client_authenticates_with_jwt(
        self,
    ):
        """A client configured with a dataspace uses a fresh unscoped connection
        for list_tables(None); with no core token it must mint via JWT."""
        client = SalesforceDataCloudProxyClient(_jwt_credentials(dataspace="CSG"))

        client.list_tables()

        (form,) = self._token_requests()
        self.assertEqual(form["grant_type"], _JWT_BEARER_GRANT_TYPE)

    def test_client_credentials_library_path_is_unchanged(self):
        client = _get_proxy_client_salesforce_data_cloud(
            {
                "connect_args": {
                    "domain": _DOMAIN,
                    "client_id": "test_client_id",
                    "client_secret": "test_client_secret",
                }
            }
        )
        connection = client.wrapped_client
        self.assertIsNone(connection.private_key)
        self.assertEqual(connection.client_secret, "test_client_secret")


class JwtBearerSelfHostedTests(TestCase):
    """Self-hosted secrets are JSON; a multi-line PEM must survive every store."""

    def setUp(self):
        clear_cache()
        self.addCleanup(clear_cache)
        self.secret = {
            "connect_args": {
                "domain": _DOMAIN,
                "client_id": "test_client_id",
                "username": _USERNAME,
                "private_key": _PEM,
            }
        }

    def _assert_resolves_to_jwt(self, raw_credentials: dict) -> None:
        resolved = CredentialsFactory.get_credentials_service(
            raw_credentials
        ).get_credentials(raw_credentials)
        client = _get_proxy_client_salesforce_data_cloud(resolved)
        self.assertEqual(client.wrapped_client.private_key, _PEM)
        self.assertEqual(client.wrapped_client.username, _USERNAME)

    def test_env_var(self):
        with patch.dict(os.environ, {"SFDC_DC_JWT_CREDS": json.dumps(self.secret)}):
            self._assert_resolves_to_jwt(
                {
                    "self_hosted_credentials_type": "env_var",
                    "env_var_name": "SFDC_DC_JWT_CREDS",
                }
            )

    def test_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            json.dump(self.secret, f)
        self.addCleanup(os.unlink, f.name)
        self._assert_resolves_to_jwt(
            {"self_hosted_credentials_type": "file", "file_path": f.name}
        )

    @patch("apollo.credentials.asm.SecretsManagerProxyClient")
    def test_aws_secrets_manager(self, mock_client_class: Mock):
        mock_client = MagicMock()
        mock_client.get_secret_string.return_value = json.dumps(self.secret)
        mock_client_class.return_value = mock_client
        self._assert_resolves_to_jwt(
            {
                "self_hosted_credentials_type": "aws_secrets_manager",
                "aws_secret": "sfdc-dc-jwt",
            }
        )

    def test_dc_forwarded_connect_args_merge_with_a_self_hosted_jwt_secret(self):
        """The DC may still forward client-credentials fields; the customer's
        secret wins on overlap and the private key selects JWT."""
        with patch.dict(os.environ, {"SFDC_DC_JWT_CREDS": json.dumps(self.secret)}):
            self._assert_resolves_to_jwt(
                {
                    "self_hosted_credentials_type": "env_var",
                    "env_var_name": "SFDC_DC_JWT_CREDS",
                    "connect_args": {"client_secret": "forwarded_secret"},
                }
            )

    def test_escaped_newlines_in_a_single_line_pem_are_restored(self):
        """A PEM pasted into a one-line env var often arrives with literal ``\\n``
        sequences instead of newlines; normalize it rather than fail to parse."""
        flattened = _PEM.strip().replace("\n", "\\n")
        credentials = _jwt_credentials(private_key=flattened)
        self.assertEqual(credentials.private_key, _PEM.strip())
        SalesforceDataCloudProxyClient(credentials)  # parses


class JwtBearerSchemaTests(TestCase):
    def _errors(self, connect_args: dict) -> dict:
        return validate(
            SALESFORCE_DATA_CLOUD_CREDENTIALS_SCHEMA, {"connect_args": connect_args}
        )

    def test_client_credentials_shape_is_valid(self):
        self.assertEqual(
            self._errors(
                {"domain": _DOMAIN, "client_id": "id", "client_secret": "secret"}
            ),
            {},
        )

    def test_jwt_shape_is_valid(self):
        self.assertEqual(
            self._errors(
                {
                    "domain": _DOMAIN,
                    "client_id": "id",
                    "username": _USERNAME,
                    "private_key": _PEM,
                }
            ),
            {},
        )

    def test_both_shapes_together_are_valid(self):
        self.assertEqual(
            self._errors(
                {
                    "domain": _DOMAIN,
                    "client_id": "id",
                    "client_secret": "secret",
                    "username": _USERNAME,
                    "private_key": _PEM,
                }
            ),
            {},
        )

    def test_private_key_without_username_is_invalid(self):
        self.assertNotEqual(
            self._errors({"domain": _DOMAIN, "client_id": "id", "private_key": _PEM}),
            {},
        )

    def test_secret_and_private_key_without_username_is_invalid(self):
        """Any private_key selects JWT at runtime, so the schema must not let the
        Client Credentials variant accept it without a username."""
        self.assertNotEqual(
            self._errors(
                {
                    "domain": _DOMAIN,
                    "client_id": "id",
                    "client_secret": "secret",
                    "private_key": _PEM,
                }
            ),
            {},
        )

    def test_secret_and_private_key_with_empty_username_is_invalid(self):
        self.assertNotEqual(
            self._errors(
                {
                    "domain": _DOMAIN,
                    "client_id": "id",
                    "client_secret": "secret",
                    "private_key": _PEM,
                    "username": "",
                }
            ),
            {},
        )

    def test_no_auth_fields_is_invalid(self):
        self.assertNotEqual(self._errors({"domain": _DOMAIN, "client_id": "id"}), {})
