# tests/ctp/test_power_bi_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.power_bi import POWERBI_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(POWERBI_DEFAULT_CTP, credentials)


def _sp_creds(**kwargs) -> dict:
    return {
        "auth_mode": "service_principal",
        "client_id": "app-client-id",
        "client_secret": "app-secret",
        "tenant_id": "tenant-uuid",
        **kwargs,
    }


def _pu_creds(**kwargs) -> dict:
    return {
        "auth_mode": "primary_user",
        "client_id": "app-client-id",
        "tenant_id": "tenant-uuid",
        "username": "user@example.com",
        "password": "userpass",
        **kwargs,
    }


class TestPowerBiCtp(TestCase):
    def test_powerbi_registered(self):
        self.assertIsNotNone(CtpRegistry.get("power-bi"))

    def test_auth_type_always_bearer(self):
        self.assertEqual("Bearer", _resolve(_sp_creds())["auth_type"])

    # ── Raw-creds pass-through (agent mints the token per host, not the CTP) ──

    def test_service_principal_params_passed_through(self):
        args = _resolve(_sp_creds())
        self.assertEqual("service_principal", args["auth_mode"])
        self.assertEqual("app-client-id", args["client_id"])
        self.assertEqual("tenant-uuid", args["tenant_id"])
        self.assertEqual("app-secret", args["client_secret"])
        # The CTP no longer mints a token — that happens per request in the proxy client.
        self.assertNotIn("token", args)

    def test_primary_user_params_passed_through(self):
        args = _resolve(_pu_creds())
        self.assertEqual("primary_user", args["auth_mode"])
        self.assertEqual("app-client-id", args["client_id"])
        self.assertEqual("tenant-uuid", args["tenant_id"])
        self.assertEqual("user@example.com", args["username"])
        self.assertEqual("userpass", args["password"])
        self.assertNotIn("token", args)

    def test_optional_fields_absent_are_dropped(self):
        # A service-principal connection carries no username/password.
        args = _resolve(_sp_creds())
        self.assertNotIn("username", args)
        self.assertNotIn("password", args)

    # ── Legacy pre-shaped token path ─────────────────────────────────────────

    def test_preshaped_token_passed_through(self):
        args = _resolve({"token": "pre-minted-token"})
        self.assertEqual("pre-minted-token", args["token"])
        self.assertEqual("Bearer", args["auth_type"])
        self.assertNotIn("auth_mode", args)
