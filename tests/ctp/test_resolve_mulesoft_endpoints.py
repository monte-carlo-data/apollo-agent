"""
Tests for the resolve_mulesoft_endpoints CTP transform.

Covers region selection, override validation (HTTPS + host allowlist),
required-input validation, output shape, override precedence, and
credential-leak safety. The transform performs no outbound HTTP, so no
network mocking is required.
"""

from unittest import TestCase

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.transforms.registry import TransformRegistry
from apollo.integrations.ctp.transforms.resolve_mulesoft_endpoints import (
    ResolveMulesoftEndpointsTransform,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_US_AUTH_URL = "https://anypoint.mulesoft.com/accounts/api/v2/oauth2/token"
_US_API_BASE = "https://anypoint.mulesoft.com"
_EU_AUTH_URL = "https://eu1.anypoint.mulesoft.com/accounts/api/v2/oauth2/token"
_EU_API_BASE = "https://eu1.anypoint.mulesoft.com"
_GOV_AUTH_URL = "https://mpt.mulesoft.com/accounts/api/v2/oauth2/token"
_GOV_API_BASE = "https://mpt.mulesoft.com"

_DEFAULT_INPUT = {
    "client_id": "{{ raw.client_id }}",
    "client_secret": "{{ raw.client_secret }}",
}


def _make_step(
    input_: dict,
    oauth_config_key: str = "mulesoft_oauth_config",
    api_base_url_key: str = "mulesoft_api_base_url",
) -> TransformStep:
    return TransformStep(
        type="resolve_mulesoft_endpoints",
        input=input_,
        output={
            "oauth_config": oauth_config_key,
            "api_base_url": api_base_url_key,
        },
    )


def _run(step: TransformStep, raw: dict) -> PipelineState:
    state = PipelineState(raw=raw)
    ResolveMulesoftEndpointsTransform().execute(step, state)
    return state


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class TestRegistration(TestCase):
    def test_registered(self):
        transform = TransformRegistry.get("resolve_mulesoft_endpoints")
        self.assertIsInstance(transform, ResolveMulesoftEndpointsTransform)


# ---------------------------------------------------------------------------
# Region selection
# ---------------------------------------------------------------------------


class TestRegionSelection(TestCase):
    def test_default_region_is_us(self):
        step = _make_step(_DEFAULT_INPUT)
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(_US_API_BASE, state.derived["mulesoft_api_base_url"])
        self.assertEqual(
            _US_AUTH_URL,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )

    def test_us_region(self):
        step = _make_step({**_DEFAULT_INPUT, "region": "US"})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(_US_API_BASE, state.derived["mulesoft_api_base_url"])
        self.assertEqual(
            _US_AUTH_URL,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )

    def test_eu_region(self):
        step = _make_step({**_DEFAULT_INPUT, "region": "EU"})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(_EU_API_BASE, state.derived["mulesoft_api_base_url"])
        self.assertEqual(
            _EU_AUTH_URL,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )

    def test_gov_region(self):
        step = _make_step({**_DEFAULT_INPUT, "region": "Gov"})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(_GOV_API_BASE, state.derived["mulesoft_api_base_url"])
        self.assertEqual(
            _GOV_AUTH_URL,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )

    def test_invalid_region_raises_with_offending_value(self):
        step = _make_step({**_DEFAULT_INPUT, "region": "APAC"})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertIn("APAC", str(ctx.exception))


# ---------------------------------------------------------------------------
# Override validation (HTTPS + host allowlist)
# ---------------------------------------------------------------------------


class TestOverrideValidation(TestCase):
    def test_auth_url_override_on_allowlisted_host_accepted(self):
        custom = "https://eu1.anypoint.mulesoft.com/some/other/oauth/path"
        step = _make_step({**_DEFAULT_INPUT, "auth_url": custom})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(
            custom,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )

    def test_api_base_url_override_on_allowlisted_host_accepted(self):
        custom = "https://mpt.mulesoft.com/custom/base"
        step = _make_step({**_DEFAULT_INPUT, "api_base_url": custom})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(custom, state.derived["mulesoft_api_base_url"])

    def test_auth_url_override_with_http_scheme_raises(self):
        step = _make_step(
            {**_DEFAULT_INPUT, "auth_url": "http://anypoint.mulesoft.com/foo"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertIn("https", str(ctx.exception))

    def test_auth_url_override_on_disallowed_host_raises(self):
        step = _make_step(
            {**_DEFAULT_INPUT, "auth_url": "https://attacker.example/oauth"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertIn("attacker.example", str(ctx.exception))

    def test_api_base_url_override_on_disallowed_host_raises(self):
        step = _make_step(
            {**_DEFAULT_INPUT, "api_base_url": "https://attacker.example/api"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertIn("attacker.example", str(ctx.exception))

    def test_both_overrides_take_precedence_over_conflicting_region(self):
        custom_auth = "https://anypoint.mulesoft.com/custom/auth"
        custom_api = "https://eu1.anypoint.mulesoft.com/custom/api"
        step = _make_step(
            {
                **_DEFAULT_INPUT,
                "region": "Gov",
                "auth_url": custom_auth,
                "api_base_url": custom_api,
            }
        )
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(
            custom_auth,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )
        self.assertEqual(custom_api, state.derived["mulesoft_api_base_url"])


# ---------------------------------------------------------------------------
# Required inputs
# ---------------------------------------------------------------------------


class TestRequiredInputs(TestCase):
    def test_missing_client_id_raises(self):
        step = _make_step({"client_secret": "{{ raw.client_secret }}"})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_secret": "sec"})
        self.assertIn("client_id", str(ctx.exception))

    def test_missing_client_secret_raises(self):
        step = _make_step({"client_id": "{{ raw.client_id }}"})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id"})
        self.assertIn("client_secret", str(ctx.exception))

    def test_empty_client_id_raises(self):
        step = _make_step(_DEFAULT_INPUT)
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "", "client_secret": "sec"})
        self.assertIn("client_id", str(ctx.exception))


# ---------------------------------------------------------------------------
# Output shape
# ---------------------------------------------------------------------------


class TestOutputShape(TestCase):
    def test_oauth_config_dict_has_exactly_expected_keys(self):
        step = _make_step(_DEFAULT_INPUT)
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        oauth_config = state.derived["mulesoft_oauth_config"]
        self.assertEqual(
            {"grant_type", "client_id", "client_secret", "access_token_endpoint"},
            set(oauth_config.keys()),
        )

    def test_grant_type_is_client_credentials(self):
        step = _make_step(_DEFAULT_INPUT)
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(
            "client_credentials",
            state.derived["mulesoft_oauth_config"]["grant_type"],
        )

    def test_oauth_config_propagates_credentials(self):
        step = _make_step(_DEFAULT_INPUT)
        state = _run(step, {"client_id": "the-id", "client_secret": "the-sec"})
        oauth_config = state.derived["mulesoft_oauth_config"]
        self.assertEqual("the-id", oauth_config["client_id"])
        self.assertEqual("the-sec", oauth_config["client_secret"])

    def test_us_default_emits_expected_api_base_url(self):
        step = _make_step(_DEFAULT_INPUT)
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(_US_API_BASE, state.derived["mulesoft_api_base_url"])


# ---------------------------------------------------------------------------
# Credential safety (compliance-critical)
# ---------------------------------------------------------------------------


class TestCredentialSafety(TestCase):
    """The client_secret value must never appear in error messages."""

    _SECRET = "SECRET-CONTENT-DO-NOT-LEAK"

    def test_invalid_region_does_not_leak_secret(self):
        step = _make_step({**_DEFAULT_INPUT, "region": "APAC"})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": self._SECRET})
        self.assertNotIn(self._SECRET, str(ctx.exception))

    def test_disallowed_host_override_does_not_leak_secret(self):
        step = _make_step(
            {**_DEFAULT_INPUT, "auth_url": "https://attacker.example/oauth"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": self._SECRET})
        self.assertNotIn(self._SECRET, str(ctx.exception))

    def test_http_scheme_override_does_not_leak_secret(self):
        step = _make_step(
            {**_DEFAULT_INPUT, "auth_url": "http://anypoint.mulesoft.com/oauth"}
        )
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_id": "id", "client_secret": self._SECRET})
        self.assertNotIn(self._SECRET, str(ctx.exception))

    def test_missing_client_id_does_not_leak_secret(self):
        step = _make_step({"client_secret": "{{ raw.client_secret }}"})
        with self.assertRaises(CtpPipelineError) as ctx:
            _run(step, {"client_secret": self._SECRET})
        self.assertNotIn(self._SECRET, str(ctx.exception))


# ---------------------------------------------------------------------------
# Override precedence combos
# ---------------------------------------------------------------------------


class TestOverridePrecedence(TestCase):
    def test_auth_url_override_with_no_region_uses_default_for_api_base(self):
        custom_auth = "https://eu1.anypoint.mulesoft.com/custom/oauth"
        step = _make_step({**_DEFAULT_INPUT, "auth_url": custom_auth})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(
            custom_auth,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )
        self.assertEqual(_US_API_BASE, state.derived["mulesoft_api_base_url"])

    def test_auth_url_override_with_conflicting_region_keeps_override_for_auth(self):
        custom_auth = "https://anypoint.mulesoft.com/custom/oauth"
        step = _make_step({**_DEFAULT_INPUT, "region": "Gov", "auth_url": custom_auth})
        state = _run(step, {"client_id": "id", "client_secret": "sec"})
        self.assertEqual(
            custom_auth,
            state.derived["mulesoft_oauth_config"]["access_token_endpoint"],
        )
        self.assertEqual(_GOV_API_BASE, state.derived["mulesoft_api_base_url"])
