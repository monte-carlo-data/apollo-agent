# tests/ctp/test_transforms.py
from unittest import TestCase

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.transforms.registry import TransformRegistry


class TestTransformRegistry(TestCase):
    def test_unknown_type_raises(self):
        with self.assertRaises(CtpPipelineError) as ctx:
            TransformRegistry.get("not_a_real_type")
        self.assertIn("not_a_real_type", str(ctx.exception))

    def test_tmp_file_write_registered(self):
        transform = TransformRegistry.get("tmp_file_write")
        self.assertIsNotNone(transform)


import os
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.transforms.tmp_file_write import TmpFileWriteTransform


class TestTmpFileWriteTransform(TestCase):
    def _make_step(self, contents_template, when=None, file_suffix=".pem"):
        return TransformStep(
            type="tmp_file_write",
            input={
                "contents": contents_template,
                "file_suffix": file_suffix,
                "mode": "0600",
            },
            output={"path": "ssl_ca_path"},
            when=when,
        )

    def test_writes_content_to_temp_file(self):
        state = PipelineState(raw={"ca_pem": "CERT_CONTENT"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)

        path = state.derived.get("ssl_ca_path")
        self.assertIsNotNone(path)
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertEqual("CERT_CONTENT", f.read())
        os.unlink(path)

    def test_output_key_written_to_derived(self):
        state = PipelineState(raw={"ca_pem": "DATA"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)
        self.assertIn("ssl_ca_path", state.derived)
        os.unlink(state.derived["ssl_ca_path"])

    def test_does_not_overwrite_raw(self):
        state = PipelineState(raw={"ca_pem": "DATA"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)
        self.assertNotIn("ssl_ca_path", state.raw)
        os.unlink(state.derived["ssl_ca_path"])

    def test_writes_bytes_content_to_temp_file(self):
        pem_bytes = b"-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
        state = PipelineState(raw={"ca_data": pem_bytes})
        step = self._make_step("{{ raw.ca_data }}")
        TmpFileWriteTransform().execute(step, state)

        path = state.derived.get("ssl_ca_path")
        self.assertIsNotNone(path)
        with open(path, "rb") as f:
            self.assertEqual(pem_bytes, f.read())
        os.unlink(path)

    def test_missing_contents_raises(self):
        from apollo.integrations.ctp.errors import CtpPipelineError

        state = PipelineState(raw={})
        step = TransformStep(
            type="tmp_file_write",
            input={"file_suffix": ".pem"},
            output={"path": "ssl_ca_path"},
        )
        with self.assertRaises(CtpPipelineError):
            TmpFileWriteTransform().execute(step, state)


from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    BestAvailableEncryption,
    Encoding,
    NoEncryption,
    PrivateFormat,
)

from apollo.integrations.ctp.transforms.load_private_key import LoadPrivateKeyTransform


def _generate_pem(passphrase: bytes | None = None) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption = BestAvailableEncryption(passphrase) if passphrase else NoEncryption()
    return key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    )


def _make_load_step(pem_template, password_template=None):
    inp = {"pem": pem_template}
    if password_template is not None:
        inp["password"] = password_template
    return TransformStep(
        type="load_private_key",
        input=inp,
        output={"private_key": "private_key_der"},
    )


class TestLoadPrivateKeyTransform(TestCase):
    def test_loads_unencrypted_pem_string(self):
        pem = _generate_pem()
        state = PipelineState(raw={"private_key_pem": pem.decode()})
        step = _make_load_step("{{ raw.private_key_pem }}")
        LoadPrivateKeyTransform().execute(step, state)
        self.assertIn("private_key_der", state.derived)
        self.assertIsInstance(state.derived["private_key_der"], bytes)

    def test_loads_unencrypted_pem_bytes(self):
        pem = _generate_pem()
        state = PipelineState(raw={"private_key_pem": pem})
        step = _make_load_step("{{ raw.private_key_pem }}")
        LoadPrivateKeyTransform().execute(step, state)
        self.assertIsInstance(state.derived["private_key_der"], bytes)

    def test_loads_encrypted_pem_with_string_passphrase(self):
        passphrase = b"s3cr3t"
        pem = _generate_pem(passphrase=passphrase)
        state = PipelineState(
            raw={"private_key_pem": pem.decode(), "private_key_passphrase": "s3cr3t"}
        )
        step = _make_load_step(
            "{{ raw.private_key_pem }}", "{{ raw.private_key_passphrase }}"
        )
        LoadPrivateKeyTransform().execute(step, state)
        self.assertIsInstance(state.derived["private_key_der"], bytes)

    def test_wrong_passphrase_raises_ctp_error(self):
        pem = _generate_pem(passphrase=b"correct")
        state = PipelineState(
            raw={"private_key_pem": pem.decode(), "private_key_passphrase": "wrong"}
        )
        step = _make_load_step(
            "{{ raw.private_key_pem }}", "{{ raw.private_key_passphrase }}"
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            LoadPrivateKeyTransform().execute(step, state)
        self.assertIn("load_private_key", str(ctx.exception))

    def test_invalid_pem_raises_ctp_error(self):
        state = PipelineState(raw={"private_key_pem": "not-a-pem"})
        step = _make_load_step("{{ raw.private_key_pem }}")
        with self.assertRaises(CtpPipelineError):
            LoadPrivateKeyTransform().execute(step, state)

    def test_missing_pem_input_raises(self):
        state = PipelineState(raw={})
        step = TransformStep(
            type="load_private_key", input={}, output={"private_key": "private_key_der"}
        )
        with self.assertRaises(CtpPipelineError):
            LoadPrivateKeyTransform().execute(step, state)

    def test_missing_output_key_raises(self):
        pem = _generate_pem()
        state = PipelineState(raw={"private_key_pem": pem.decode()})
        step = TransformStep(
            type="load_private_key",
            input={"pem": "{{ raw.private_key_pem }}"},
            output={},
        )
        with self.assertRaises(CtpPipelineError):
            LoadPrivateKeyTransform().execute(step, state)

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("load_private_key"))


import base64
from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.transforms.oauth import OAuthTransform

_CC_CONFIG = {
    "client_id": "my-client",
    "client_secret": "my-secret",
    "access_token_endpoint": "https://auth.example.com/token",
    "grant_type": "client_credentials",
}

_PW_CONFIG = {
    **_CC_CONFIG,
    "grant_type": "password",
    "username": "alice",
    "password": "hunter2",
}


def _make_oauth_step(oauth_template="{{ raw.oauth }}", token_key="oauth_token"):
    return TransformStep(
        type="oauth",
        input={"oauth": oauth_template},
        output={"token": token_key},
    )


def _mock_token_response(token="tok_abc123"):
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"access_token": token}
    mock_resp.raise_for_status.return_value = None
    return mock_resp


class TestOAuthTransform(TestCase):
    # ── Client credentials grant ──────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_client_credentials_stores_token(self, mock_post):
        mock_post.return_value = _mock_token_response("tok_cc")
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        OAuthTransform().execute(_make_oauth_step(), state)
        self.assertEqual("tok_cc", state.derived["oauth_token"])

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_client_credentials_sends_basic_auth_header(self, mock_post):
        mock_post.return_value = _mock_token_response()
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        OAuthTransform().execute(_make_oauth_step(), state)

        _, kwargs = mock_post.call_args
        expected = base64.b64encode(b"my-client:my-secret").decode()
        self.assertEqual(f"Basic {expected}", kwargs["headers"]["Authorization"])

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_client_credentials_sends_grant_type_in_body(self, mock_post):
        mock_post.return_value = _mock_token_response()
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        OAuthTransform().execute(_make_oauth_step(), state)

        _, kwargs = mock_post.call_args
        self.assertEqual("client_credentials", kwargs["data"]["grant_type"])

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_client_credentials_with_scope(self, mock_post):
        mock_post.return_value = _mock_token_response()
        config = {**_CC_CONFIG, "scope": "read:data"}
        state = PipelineState(raw={"oauth": config})
        OAuthTransform().execute(_make_oauth_step(), state)

        _, kwargs = mock_post.call_args
        self.assertEqual("read:data", kwargs["data"]["scope"])

    # ── Password grant ────────────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_password_grant_stores_token(self, mock_post):
        mock_post.return_value = _mock_token_response("tok_pw")
        state = PipelineState(raw={"oauth": _PW_CONFIG})
        OAuthTransform().execute(_make_oauth_step(), state)
        self.assertEqual("tok_pw", state.derived["oauth_token"])

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_password_grant_sends_username_and_password(self, mock_post):
        mock_post.return_value = _mock_token_response()
        state = PipelineState(raw={"oauth": _PW_CONFIG})
        OAuthTransform().execute(_make_oauth_step(), state)

        _, kwargs = mock_post.call_args
        self.assertEqual("alice", kwargs["data"]["username"])
        self.assertEqual("hunter2", kwargs["data"]["password"])

    # ── Error handling ────────────────────────────────────────────────

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_http_error_raises_ctp_error(self, mock_post):
        import requests as req

        error_resp = MagicMock()
        error_resp.status_code = 401
        mock_post.return_value = error_resp
        mock_post.return_value.raise_for_status.side_effect = req.HTTPError(
            response=error_resp
        )
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        with self.assertRaises(CtpPipelineError) as ctx:
            OAuthTransform().execute(_make_oauth_step(), state)
        self.assertIn("401", str(ctx.exception))

    @patch("apollo.integrations.ctp.transforms.oauth.requests.post")
    def test_missing_access_token_in_response_raises(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"error": "invalid_client"}
        mock_resp.raise_for_status.return_value = None
        mock_post.return_value = mock_resp
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        with self.assertRaises(CtpPipelineError) as ctx:
            OAuthTransform().execute(_make_oauth_step(), state)
        self.assertIn("access_token", str(ctx.exception))

    def test_missing_oauth_input_raises(self):
        step = TransformStep(type="oauth", input={}, output={"token": "t"})
        with self.assertRaises(CtpPipelineError):
            OAuthTransform().execute(step, PipelineState(raw={}))

    def test_missing_token_output_raises(self):
        step = TransformStep(
            type="oauth", input={"oauth": "{{ raw.oauth }}"}, output={}
        )
        state = PipelineState(raw={"oauth": _CC_CONFIG})
        with self.assertRaises(CtpPipelineError):
            OAuthTransform().execute(step, state)

    def test_missing_required_oauth_key_raises(self):
        config = {k: v for k, v in _CC_CONFIG.items() if k != "client_secret"}
        state = PipelineState(raw={"oauth": config})
        with self.assertRaises(CtpPipelineError) as ctx:
            OAuthTransform().execute(_make_oauth_step(), state)
        self.assertIn("client_secret", str(ctx.exception))

    def test_unsupported_grant_type_raises(self):
        config = {**_CC_CONFIG, "grant_type": "authorization_code"}
        state = PipelineState(raw={"oauth": config})
        with self.assertRaises(CtpPipelineError) as ctx:
            OAuthTransform().execute(_make_oauth_step(), state)
        self.assertIn("authorization_code", str(ctx.exception))

    def test_password_grant_missing_username_raises(self):
        config = {**_PW_CONFIG}
        del config["username"]
        state = PipelineState(raw={"oauth": config})
        with self.assertRaises(CtpPipelineError) as ctx:
            OAuthTransform().execute(_make_oauth_step(), state)
        self.assertIn("username", str(ctx.exception))

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("oauth"))


import prestodb

from apollo.integrations.ctp.transforms.resolve_presto_auth import (
    ResolvePrestoAuthTransform,
)


def _make_presto_auth_step(
    auth_template="{{ raw.auth }}", output_key="presto_auth_obj"
):
    return TransformStep(
        type="resolve_presto_auth",
        input={"auth": auth_template},
        output={"auth": output_key},
    )


class TestResolvePrestoAuthTransform(TestCase):
    def test_produces_basic_authentication_object(self):
        state = PipelineState(raw={"auth": {"username": "alice", "password": "secret"}})
        ResolvePrestoAuthTransform().execute(_make_presto_auth_step(), state)
        self.assertIsInstance(
            state.derived["presto_auth_obj"], prestodb.auth.BasicAuthentication
        )

    def test_missing_username_raises(self):
        state = PipelineState(raw={"auth": {"password": "secret"}})
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolvePrestoAuthTransform().execute(_make_presto_auth_step(), state)
        self.assertIn("username", str(ctx.exception))

    def test_missing_password_raises(self):
        state = PipelineState(raw={"auth": {"username": "alice"}})
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolvePrestoAuthTransform().execute(_make_presto_auth_step(), state)
        self.assertIn("password", str(ctx.exception))

    def test_missing_auth_input_raises(self):
        step = TransformStep(type="resolve_presto_auth", input={}, output={"auth": "k"})
        with self.assertRaises(CtpPipelineError):
            ResolvePrestoAuthTransform().execute(step, PipelineState(raw={}))

    def test_missing_auth_output_raises(self):
        step = TransformStep(
            type="resolve_presto_auth",
            input={"auth": "{{ raw.auth }}"},
            output={},
        )
        state = PipelineState(raw={"auth": {"username": "u", "password": "p"}})
        with self.assertRaises(CtpPipelineError):
            ResolvePrestoAuthTransform().execute(step, state)

    def test_non_dict_auth_raises(self):
        state = PipelineState(raw={"auth": "not-a-dict"})
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolvePrestoAuthTransform().execute(_make_presto_auth_step(), state)
        self.assertIn("dict", str(ctx.exception))

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("resolve_presto_auth"))


# ── WriteIniFileTransform ─────────────────────────────────────────────────────

import configparser

from apollo.integrations.ctp.transforms.write_ini_file import WriteIniFileTransform


def _make_ini_step(section="Looker", extra_input=None, output_key="ini_path"):
    inp = {"section": section, **(extra_input or {})}
    return TransformStep(
        type="write_ini_file",
        input=inp,
        output={"path": output_key},
    )


class TestWriteIniFileTransform(TestCase):
    def test_writes_ini_file(self):
        state = PipelineState(
            raw={
                "base_url": "https://looker.example.com",
                "client_id": "cid",
                "client_secret": "csec",
            }
        )
        step = _make_ini_step(
            extra_input={
                "base_url": "{{ raw.base_url }}",
                "client_id": "{{ raw.client_id }}",
                "client_secret": "{{ raw.client_secret }}",
            }
        )
        WriteIniFileTransform().execute(step, state)

        path = state.derived["ini_path"]
        self.assertTrue(os.path.exists(path))
        config = configparser.ConfigParser()
        config.read(path)
        self.assertIn("Looker", config)
        self.assertEqual("https://looker.example.com", config["Looker"]["base_url"])
        self.assertEqual("cid", config["Looker"]["client_id"])
        os.unlink(path)

    def test_output_path_stored_in_derived(self):
        state = PipelineState(raw={})
        step = _make_ini_step(extra_input={"key": "value"})
        WriteIniFileTransform().execute(step, state)
        self.assertIn("ini_path", state.derived)
        os.unlink(state.derived["ini_path"])

    def test_none_values_omitted(self):
        state = PipelineState(raw={})
        step = _make_ini_step(extra_input={"present": "yes", "absent": "{{ none }}"})
        WriteIniFileTransform().execute(step, state)
        config = configparser.ConfigParser()
        config.read(state.derived["ini_path"])
        self.assertIn("present", config["Looker"])
        self.assertNotIn("absent", config["Looker"])
        os.unlink(state.derived["ini_path"])

    def test_missing_section_raises(self):
        step = TransformStep(
            type="write_ini_file", input={"key": "val"}, output={"path": "p"}
        )
        with self.assertRaises(CtpPipelineError):
            WriteIniFileTransform().execute(step, PipelineState(raw={}))

    def test_missing_path_output_raises(self):
        step = TransformStep(type="write_ini_file", input={"section": "S"}, output={})
        with self.assertRaises(CtpPipelineError):
            WriteIniFileTransform().execute(step, PipelineState(raw={}))

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("write_ini_file"))


# ── GenerateJwtTransform ──────────────────────────────────────────────────────

import jwt as pyjwt

from apollo.integrations.ctp.transforms.generate_jwt import GenerateJwtTransform

_JWT_RAW = {
    "username": "alice",
    "client_id": "client-uuid",
    "secret_id": "secret-uuid",
    "secret_value": "my-secret",
}


def _make_jwt_step(expiration_seconds=None):
    inp = {
        "username": "{{ raw.username }}",
        "client_id": "{{ raw.client_id }}",
        "secret_id": "{{ raw.secret_id }}",
        "secret_value": "{{ raw.secret_value }}",
    }
    if expiration_seconds is not None:
        inp["expiration_seconds"] = str(expiration_seconds)
    return TransformStep(
        type="generate_jwt", input=inp, output={"token": "tableau_jwt"}
    )


class TestGenerateJwtTransform(TestCase):
    def test_produces_string_token(self):
        state = PipelineState(raw=_JWT_RAW)
        GenerateJwtTransform().execute(_make_jwt_step(), state)
        self.assertIsInstance(state.derived["tableau_jwt"], str)

    def test_token_decodes_with_correct_claims(self):
        state = PipelineState(raw=_JWT_RAW)
        GenerateJwtTransform().execute(_make_jwt_step(), state)
        token = state.derived["tableau_jwt"]
        payload = pyjwt.decode(
            token,
            key="my-secret",
            algorithms=["HS256"],
            audience="tableau",
        )
        self.assertEqual("alice", payload["sub"])
        self.assertEqual("client-uuid", payload["iss"])
        self.assertIn("tableau:content:read", payload["scp"])

    def test_token_headers_contain_kid_and_iss(self):
        state = PipelineState(raw=_JWT_RAW)
        GenerateJwtTransform().execute(_make_jwt_step(), state)
        token = state.derived["tableau_jwt"]
        headers = pyjwt.get_unverified_header(token)
        self.assertEqual("client-uuid", headers["iss"])
        self.assertEqual("secret-uuid", headers["kid"])

    def test_expiration_seconds_respected(self):
        import time

        state = PipelineState(raw=_JWT_RAW)
        GenerateJwtTransform().execute(_make_jwt_step(expiration_seconds=10), state)
        token = state.derived["tableau_jwt"]
        payload = pyjwt.decode(
            token, key="my-secret", algorithms=["HS256"], audience="tableau"
        )
        remaining = payload["exp"] - time.time()
        self.assertLessEqual(remaining, 10)
        self.assertGreater(remaining, 0)

    def test_missing_required_input_raises(self):
        for missing_key in ("username", "client_id", "secret_id", "secret_value"):
            inp = {
                k: "{{ raw." + k + " }}"
                for k in ("username", "client_id", "secret_id", "secret_value")
                if k != missing_key
            }
            step = TransformStep(type="generate_jwt", input=inp, output={"token": "t"})
            with self.assertRaises(CtpPipelineError) as ctx:
                GenerateJwtTransform().execute(step, PipelineState(raw=_JWT_RAW))
            self.assertIn(missing_key, str(ctx.exception))

    def test_missing_token_output_raises(self):
        step = TransformStep(
            type="generate_jwt",
            input={
                k: "{{ raw." + k + " }}"
                for k in ("username", "client_id", "secret_id", "secret_value")
            },
            output={},
        )
        with self.assertRaises(CtpPipelineError):
            GenerateJwtTransform().execute(step, PipelineState(raw=_JWT_RAW))

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("generate_jwt"))


# ── ResolveMsalTokenTransform ─────────────────────────────────────────────────

from unittest.mock import MagicMock, patch

from apollo.integrations.ctp.transforms.resolve_msal_token import (
    ResolveMsalTokenTransform,
)

_MSAL_SP_RAW = {
    "auth_mode": "service_principal",
    "client_id": "cid",
    "tenant_id": "tid",
    "client_secret": "csec",
}

_MSAL_PU_RAW = {
    "auth_mode": "primary_user",
    "client_id": "cid",
    "tenant_id": "tid",
    "username": "alice@example.com",
    "password": "hunter2",
}


def _make_msal_step(raw_keys, output_key="msal_token"):
    return TransformStep(
        type="resolve_msal_token",
        input={k: "{{ raw." + k + " }}" for k in raw_keys},
        output={"token": output_key},
    )


class TestResolveMsalTokenTransform(TestCase):
    @patch(
        "apollo.integrations.ctp.transforms.resolve_msal_token.msal.ConfidentialClientApplication"
    )
    def test_service_principal_stores_token(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {"access_token": "tok_sp"}
        mock_app_cls.return_value = mock_app

        state = PipelineState(raw=_MSAL_SP_RAW)
        ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_SP_RAW), state)
        self.assertEqual("tok_sp", state.derived["msal_token"])

    @patch(
        "apollo.integrations.ctp.transforms.resolve_msal_token.msal.PublicClientApplication"
    )
    def test_primary_user_stores_token(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.get_accounts.return_value = []
        mock_app.acquire_token_by_username_password.return_value = {
            "access_token": "tok_pu"
        }
        mock_app_cls.return_value = mock_app

        state = PipelineState(raw=_MSAL_PU_RAW)
        ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_PU_RAW), state)
        self.assertEqual("tok_pu", state.derived["msal_token"])

    @patch(
        "apollo.integrations.ctp.transforms.resolve_msal_token.msal.PublicClientApplication"
    )
    def test_primary_user_uses_cached_token(self, mock_app_cls):
        mock_app = MagicMock()
        mock_account = MagicMock()
        mock_app.get_accounts.return_value = [mock_account]
        mock_app.acquire_token_silent.return_value = {"access_token": "tok_cached"}
        mock_app_cls.return_value = mock_app

        state = PipelineState(raw=_MSAL_PU_RAW)
        ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_PU_RAW), state)
        self.assertEqual("tok_cached", state.derived["msal_token"])
        mock_app.acquire_token_by_username_password.assert_not_called()

    @patch(
        "apollo.integrations.ctp.transforms.resolve_msal_token.msal.ConfidentialClientApplication"
    )
    def test_msal_error_raises_ctp_error(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = {
            "error": "invalid_client",
            "error_description": "bad secret",
        }
        mock_app_cls.return_value = mock_app

        state = PipelineState(raw=_MSAL_SP_RAW)
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_SP_RAW), state)
        self.assertIn("invalid_client", str(ctx.exception))

    @patch(
        "apollo.integrations.ctp.transforms.resolve_msal_token.msal.ConfidentialClientApplication"
    )
    def test_empty_response_raises_ctp_error(self, mock_app_cls):
        mock_app = MagicMock()
        mock_app.acquire_token_for_client.return_value = None
        mock_app_cls.return_value = mock_app

        state = PipelineState(raw=_MSAL_SP_RAW)
        with self.assertRaises(CtpPipelineError):
            ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_SP_RAW), state)

    def test_unsupported_auth_mode_raises(self):
        state = PipelineState(raw={**_MSAL_SP_RAW, "auth_mode": "magic"})
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveMsalTokenTransform().execute(_make_msal_step(_MSAL_SP_RAW), state)
        self.assertIn("magic", str(ctx.exception))

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("resolve_msal_token"))


# ── ResolveDatabricksOauthTransform ───────────────────────────────────────────

from apollo.integrations.ctp.transforms.resolve_databricks_oauth import (
    ResolveDatabricksOauthTransform,
)

_DATABRICKS_RAW = {
    "server_hostname": "myworkspace.azuredatabricks.net",
    "client_id": "cid",
    "client_secret": "csec",
}

_DATABRICKS_AZURE_RAW = {
    **_DATABRICKS_RAW,
    "azure_tenant_id": "tid",
    "azure_workspace_resource_id": "/subscriptions/xxx/resourceGroups/rg/providers/Microsoft.Databricks/workspaces/ws",
}


def _make_databricks_step(raw_keys, output_key="databricks_provider"):
    return TransformStep(
        type="resolve_databricks_oauth",
        input={k: "{{ raw." + k + " }}" for k in raw_keys},
        output={"credentials_provider": output_key},
    )


class TestResolveDatabricksOauthTransform(TestCase):
    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    def test_databricks_managed_oauth_produces_callable(
        self, mock_config, mock_provider
    ):
        state = PipelineState(raw=_DATABRICKS_RAW)
        ResolveDatabricksOauthTransform().execute(
            _make_databricks_step(_DATABRICKS_RAW), state
        )
        provider = state.derived["databricks_provider"]
        self.assertTrue(callable(provider))

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.azure_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    def test_azure_managed_oauth_uses_azure_service_principal(
        self, mock_config, mock_azure_provider
    ):
        state = PipelineState(raw=_DATABRICKS_AZURE_RAW)
        ResolveDatabricksOauthTransform().execute(
            _make_databricks_step(_DATABRICKS_AZURE_RAW), state
        )
        # Config should be called with azure-specific params
        call_kwargs = mock_config.call_args.kwargs
        self.assertEqual("tid", call_kwargs["azure_tenant_id"])

    @patch(
        "apollo.integrations.ctp.transforms.resolve_databricks_oauth.oauth_service_principal"
    )
    @patch("apollo.integrations.ctp.transforms.resolve_databricks_oauth.Config")
    def test_databricks_managed_config_uses_client_id_client_secret(
        self, mock_config, mock_provider
    ):
        state = PipelineState(raw=_DATABRICKS_RAW)
        ResolveDatabricksOauthTransform().execute(
            _make_databricks_step(_DATABRICKS_RAW), state
        )
        call_kwargs = mock_config.call_args.kwargs
        self.assertEqual("cid", call_kwargs["client_id"])
        self.assertEqual("csec", call_kwargs["client_secret"])
        self.assertNotIn("azure_client_id", call_kwargs)

    def test_missing_required_input_raises(self):
        for missing in ("server_hostname", "client_id", "client_secret"):
            keys = [k for k in _DATABRICKS_RAW if k != missing]
            step = TransformStep(
                type="resolve_databricks_oauth",
                input={k: "{{ raw." + k + " }}" for k in keys},
                output={"credentials_provider": "p"},
            )
            with self.assertRaises(CtpPipelineError) as ctx:
                ResolveDatabricksOauthTransform().execute(
                    step, PipelineState(raw=_DATABRICKS_RAW)
                )
            self.assertIn(missing, str(ctx.exception))

    def test_missing_credentials_provider_output_raises(self):
        step = TransformStep(
            type="resolve_databricks_oauth",
            input={k: "{{ raw." + k + " }}" for k in _DATABRICKS_RAW},
            output={},
        )
        with self.assertRaises(CtpPipelineError):
            ResolveDatabricksOauthTransform().execute(
                step, PipelineState(raw=_DATABRICKS_RAW)
            )

    def test_registered(self):
        self.assertIsNotNone(TransformRegistry.get("resolve_databricks_oauth"))


# ── ResolveRedshiftCredentialsTransform ───────────────────────────────────────

from apollo.integrations.ctp.transforms.resolve_redshift_credentials import (
    ResolveRedshiftCredentialsTransform,
)

_REDSHIFT_RAW = {
    "cluster_identifier": "my-cluster",
    "db_user": "iam_alice",
    "db_name": "mydb",
    "aws_region": "us-east-1",
}

_FAKE_CREDENTIALS = {
    "DbUser": "iam:iam_alice:123456",
    "DbPassword": "AmazingPassword123!",
    "Expiration": "2099-01-01T00:00:00Z",
}


def _make_redshift_step(
    extra_input=None, user_key="federated_user", password_key="federated_password"
):
    inp = {k: "{{ raw." + k + " }}" for k in _REDSHIFT_RAW}
    if extra_input:
        inp.update(extra_input)
    return TransformStep(
        type="resolve_redshift_credentials",
        input=inp,
        output={"user": user_key, "password": password_key},
    )


@patch("apollo.integrations.ctp.transforms.resolve_redshift_credentials.boto3")
class TestResolveRedshiftCredentialsTransform(TestCase):
    def _mock_redshift_client(self, mock_boto3):
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.return_value = _FAKE_CREDENTIALS
        mock_boto3.Session.return_value.client.return_value = mock_client
        return mock_client

    def test_stores_db_user_and_password_in_derived(self, mock_boto3):
        mock_client = self._mock_redshift_client(mock_boto3)
        state = PipelineState(raw=_REDSHIFT_RAW)
        ResolveRedshiftCredentialsTransform().execute(_make_redshift_step(), state)
        self.assertEqual("iam:iam_alice:123456", state.derived["federated_user"])
        self.assertEqual("AmazingPassword123!", state.derived["federated_password"])

    def test_calls_get_cluster_credentials_with_required_params(self, mock_boto3):
        mock_client = self._mock_redshift_client(mock_boto3)
        state = PipelineState(raw=_REDSHIFT_RAW)
        ResolveRedshiftCredentialsTransform().execute(_make_redshift_step(), state)
        mock_client.get_cluster_credentials.assert_called_once_with(
            DbUser="iam_alice",
            DbName="mydb",
            ClusterIdentifier="my-cluster",
        )

    def test_duration_seconds_passed_when_provided(self, mock_boto3):
        mock_client = self._mock_redshift_client(mock_boto3)
        state = PipelineState(raw={**_REDSHIFT_RAW, "duration_seconds": 1800})
        step = _make_redshift_step(
            extra_input={"duration_seconds": "{{ raw.duration_seconds }}"}
        )
        ResolveRedshiftCredentialsTransform().execute(step, state)
        call_kwargs = mock_client.get_cluster_credentials.call_args.kwargs
        self.assertEqual(1800, call_kwargs["DurationSeconds"])

    def test_invalid_duration_seconds_raises_ctp_error(self, mock_boto3):
        state = PipelineState(raw={**_REDSHIFT_RAW, "duration_seconds": "abc"})
        step = _make_redshift_step(
            extra_input={"duration_seconds": "{{ raw.duration_seconds }}"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(step, state)
        self.assertIn("duration_seconds", str(ctx.exception))

    def test_creates_session_with_correct_region(self, mock_boto3):
        self._mock_redshift_client(mock_boto3)
        state = PipelineState(raw=_REDSHIFT_RAW)
        ResolveRedshiftCredentialsTransform().execute(_make_redshift_step(), state)
        mock_boto3.Session.assert_called_once_with(region_name="us-east-1")

    def test_assumes_role_when_provided(self, mock_boto3):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        mock_boto3.client.return_value = mock_sts
        self._mock_redshift_client(mock_boto3)

        state = PipelineState(
            raw={**_REDSHIFT_RAW, "assumable_role": "arn:aws:iam::123:role/MyRole"}
        )
        step = _make_redshift_step(
            extra_input={"assumable_role": "{{ raw.assumable_role }}"}
        )
        ResolveRedshiftCredentialsTransform().execute(step, state)

        mock_boto3.client.assert_called_with("sts")
        mock_sts.assume_role.assert_called_once()
        call_kwargs = mock_sts.assume_role.call_args.kwargs
        self.assertEqual("arn:aws:iam::123:role/MyRole", call_kwargs["RoleArn"])

    def test_assumes_role_with_external_id(self, mock_boto3):
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        mock_boto3.client.return_value = mock_sts
        self._mock_redshift_client(mock_boto3)

        state = PipelineState(
            raw={
                **_REDSHIFT_RAW,
                "assumable_role": "arn:aws:iam::123:role/MyRole",
                "external_id": "ext-123",
            }
        )
        step = _make_redshift_step(
            extra_input={
                "assumable_role": "{{ raw.assumable_role }}",
                "external_id": "{{ raw.external_id }}",
            }
        )
        ResolveRedshiftCredentialsTransform().execute(step, state)
        call_kwargs = mock_sts.assume_role.call_args.kwargs
        self.assertEqual("ext-123", call_kwargs["ExternalId"])

    def test_api_error_raises_ctp_error(self, mock_boto3):
        # Simulate a botocore ClientError — its str() echoes back DbUser/DbName/ClusterIdentifier,
        # so the error message must use only the AWS error code, not str(exc).
        client_error = Exception("some error")
        client_error.response = {"Error": {"Code": "AccessDenied", "Message": "User: iam_alice is not authorized"}}  # type: ignore[attr-defined]
        mock_client = MagicMock()
        mock_client.get_cluster_credentials.side_effect = client_error
        mock_boto3.Session.return_value.client.return_value = mock_client

        state = PipelineState(raw=_REDSHIFT_RAW)
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(_make_redshift_step(), state)
        error_str = str(ctx.exception)
        self.assertIn("AccessDenied", error_str)
        # Verify credentials are NOT leaked into the error message
        self.assertNotIn("iam_alice", error_str)
        self.assertNotIn("mydb", error_str)
        self.assertNotIn("my-cluster", error_str)

    def test_custom_output_keys_used(self, mock_boto3):
        self._mock_redshift_client(mock_boto3)
        state = PipelineState(raw=_REDSHIFT_RAW)
        step = _make_redshift_step(user_key="rs_user", password_key="rs_pass")
        ResolveRedshiftCredentialsTransform().execute(step, state)
        self.assertIn("rs_user", state.derived)
        self.assertIn("rs_pass", state.derived)

    def test_missing_required_input_raises(self, mock_boto3):
        for missing in ("cluster_identifier", "db_user", "db_name", "aws_region"):
            inp = {k: "{{ raw." + k + " }}" for k in _REDSHIFT_RAW if k != missing}
            step = TransformStep(
                type="resolve_redshift_credentials",
                input=inp,
                output={"user": "u", "password": "p"},
            )
            with self.assertRaises(CtpPipelineError) as ctx:
                ResolveRedshiftCredentialsTransform().execute(
                    step, PipelineState(raw=_REDSHIFT_RAW)
                )
            self.assertIn(missing, str(ctx.exception))

    def test_missing_user_output_raises(self, mock_boto3):
        step = TransformStep(
            type="resolve_redshift_credentials",
            input={k: "{{ raw." + k + " }}" for k in _REDSHIFT_RAW},
            output={"password": "p"},
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(
                step, PipelineState(raw=_REDSHIFT_RAW)
            )
        self.assertIn("user", str(ctx.exception))

    def test_missing_password_output_raises(self, mock_boto3):
        step = TransformStep(
            type="resolve_redshift_credentials",
            input={k: "{{ raw." + k + " }}" for k in _REDSHIFT_RAW},
            output={"user": "u"},
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(
                step, PipelineState(raw=_REDSHIFT_RAW)
            )
        self.assertIn("password", str(ctx.exception))

    def test_sts_error_raises_ctp_error_without_role_arn(self, mock_boto3):
        import botocore.exceptions

        role_arn = "arn:aws:iam::123456789012:role/SecretRole"
        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = botocore.exceptions.ClientError(
            {
                "Error": {
                    "Code": "AccessDenied",
                    "Message": "Not authorized to assume role",
                }
            },
            "AssumeRole",
        )
        mock_boto3.client.return_value = mock_sts

        state = PipelineState(raw={**_REDSHIFT_RAW, "assumable_role": role_arn})
        step = _make_redshift_step(
            extra_input={"assumable_role": "{{ raw.assumable_role }}"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(step, state)
        error_str = str(ctx.exception)
        # Error code must appear in the message
        self.assertIn("AccessDenied", error_str)
        # Role ARN must NOT be leaked into the error message
        self.assertNotIn(role_arn, error_str)

    def test_duration_seconds_below_minimum_raises(self, mock_boto3):
        state = PipelineState(raw={**_REDSHIFT_RAW, "duration_seconds": 899})
        step = _make_redshift_step(
            extra_input={"duration_seconds": "{{ raw.duration_seconds }}"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(step, state)
        self.assertIn("899", str(ctx.exception))

    def test_duration_seconds_above_maximum_raises(self, mock_boto3):
        state = PipelineState(raw={**_REDSHIFT_RAW, "duration_seconds": 3601})
        step = _make_redshift_step(
            extra_input={"duration_seconds": "{{ raw.duration_seconds }}"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            ResolveRedshiftCredentialsTransform().execute(step, state)
        self.assertIn("3601", str(ctx.exception))

    def test_registered(self, mock_boto3):
        self.assertIsNotNone(TransformRegistry.get("resolve_redshift_credentials"))
