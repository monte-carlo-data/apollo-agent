import ipaddress
import socket
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.http import url_safety
from apollo.integrations.http.url_safety import (
    HttpClientError,
    _policy,
    _safe_create_connection,
    safe_request,
    safety_policy,
)


def _addrinfo(ip: str, *, port: int = 443, family: int = socket.AF_INET):
    return (family, socket.SOCK_STREAM, 0, "", (ip, port))


class TestSafeCreateConnection(TestCase):
    """The urllib3 hook is the live enforcement point. Verify (a) it's
    installed, (b) it passes through when no policy is active, and (c)
    it enforces the active policy with multi-IP fallback."""

    def tearDown(self):
        # Belt-and-suspenders: never leave the policy active across tests.
        _policy.active = False

    def test_hook_is_installed(self):
        from urllib3.util import connection as urllib3_connection

        self.assertIs(urllib3_connection.create_connection, _safe_create_connection)

    def test_passthrough_when_policy_inactive(self):
        # Other urllib3 users (Snowflake driver etc.) must see the original
        # function behavior when no policy is active.
        _policy.active = False
        original = url_safety._original_create_connection
        with patch.object(
            url_safety, "_original_create_connection", wraps=original
        ) as wrapped:
            try:
                _safe_create_connection(("203.0.113.10", 443), timeout=1)
            except OSError:
                pass  # connect itself may fail; we only care it was attempted
            wrapped.assert_called_once()
            args, _ = wrapped.call_args
            self.assertEqual(args[0], ("203.0.113.10", 443))

    def test_active_default_policy_blocks_metadata_ip_literal(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError) as ctx:
                _safe_create_connection(("169.254.169.254", 80), timeout=1)
        self.assertIn("blocked address", str(ctx.exception))
        called.assert_not_called()

    def test_active_default_policy_blocks_loopback_ipv4(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("127.0.0.1", 443), timeout=1)
        called.assert_not_called()

    def test_active_default_policy_blocks_loopback_ipv6(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("::1", 443), timeout=1)
        called.assert_not_called()

    def test_active_default_policy_blocks_link_local_ipv6(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("fe80::1", 443), timeout=1)
        called.assert_not_called()

    def test_active_default_policy_allows_rfc1918(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(
            url_safety, "_original_create_connection", return_value="sock"
        ) as called:
            result = _safe_create_connection(("10.0.0.20", 443), timeout=1)
        self.assertEqual(result, "sock")
        called.assert_called_once()

    def test_active_default_policy_allows_public(self):
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(
            url_safety, "_original_create_connection", return_value="sock"
        ) as called:
            _safe_create_connection(("93.184.216.34", 443), timeout=1)
        called.assert_called_once()

    def test_strict_policy_blocks_rfc1918(self):
        _policy.active = True
        _policy.strict_ip_policy = True
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("10.0.0.5", 443), timeout=1)
        called.assert_not_called()

    def test_strict_policy_blocks_multicast(self):
        _policy.active = True
        _policy.strict_ip_policy = True
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("224.0.0.1", 443), timeout=1)
        called.assert_not_called()

    def test_strict_policy_blocks_unspecified(self):
        _policy.active = True
        _policy.strict_ip_policy = True
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("0.0.0.0", 443), timeout=1)
        called.assert_not_called()

    def test_strict_policy_blocks_reserved(self):
        _policy.active = True
        _policy.strict_ip_policy = True
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("240.0.0.1", 443), timeout=1)
        called.assert_not_called()

    def test_strict_policy_allows_public(self):
        _policy.active = True
        _policy.strict_ip_policy = True
        with patch.object(
            url_safety, "_original_create_connection", return_value="sock"
        ) as called:
            _safe_create_connection(("93.184.216.34", 443), timeout=1)
        called.assert_called_once()

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_blocks_hostname_resolving_to_metadata(self, mock_gai):
        mock_gai.return_value = [_addrinfo("169.254.169.254")]
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_original_create_connection") as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("attacker.example.com", 443), timeout=1)
        called.assert_not_called()

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_mixed_records_succeed_via_unblocked_first(self, mock_gai):
        # Public address first, blocked address second. We connect to the
        # public one and never even look at the blocked one — the security
        # guarantee is "never connect to a blocked IP", not "reject
        # hostnames that advertise any blocked IP".
        mock_gai.return_value = [
            _addrinfo("93.184.216.34"),
            _addrinfo("169.254.169.254"),
        ]
        _policy.active = True
        _policy.strict_ip_policy = False
        sentinel_sock = MagicMock(name="socket")
        with patch.object(
            url_safety, "_original_create_connection", return_value=sentinel_sock
        ) as called:
            result = _safe_create_connection(("mixed.example.com", 443), timeout=1)
        self.assertIs(result, sentinel_sock)
        called.assert_called_once()  # only the public IP was tried
        args, _ = called.call_args
        self.assertEqual(args[0], ("93.184.216.34", 443))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_blocked_record_after_failed_first_raises(self, mock_gai):
        # First address is public but unreachable; second is blocked. We
        # MUST raise HttpClientError rather than connect to the blocked
        # address as a fallback.
        mock_gai.return_value = [
            _addrinfo("93.184.216.34"),
            _addrinfo("169.254.169.254"),
        ]
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(
            url_safety,
            "_original_create_connection",
            side_effect=OSError("first unreachable"),
        ) as called:
            with self.assertRaises(HttpClientError):
                _safe_create_connection(("hop.example.com", 443), timeout=1)
        # Only the first (public, unreachable) connect was attempted; the
        # blocked second IP triggered HttpClientError before any connect.
        called.assert_called_once()
        args, _ = called.call_args
        self.assertEqual(args[0], ("93.184.216.34", 443))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_wraps_dns_failure(self, mock_gai):
        mock_gai.side_effect = socket.gaierror("Name or service not known")
        _policy.active = True
        _policy.strict_ip_policy = False
        with self.assertRaises(HttpClientError) as ctx:
            _safe_create_connection(("nonexistent.invalid", 443), timeout=1)
        self.assertIn("DNS resolution failed", str(ctx.exception))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_iterates_validated_ips_on_failure(self, mock_gai):
        # Multi-IP fallback: if the first connect fails (OSError), the hook
        # falls back to the next validated address, mirroring urllib3's
        # original behavior.
        mock_gai.return_value = [
            _addrinfo("93.184.216.34"),
            _addrinfo("93.184.216.35"),
        ]
        _policy.active = True
        _policy.strict_ip_policy = False
        sentinel_sock = MagicMock(name="socket")
        with patch.object(
            url_safety,
            "_original_create_connection",
            side_effect=[OSError("first try fails"), sentinel_sock],
        ) as called:
            result = _safe_create_connection(("example.com", 443), timeout=1)
        self.assertIs(result, sentinel_sock)
        self.assertEqual(called.call_count, 2)
        first_args, _ = called.call_args_list[0]
        second_args, _ = called.call_args_list[1]
        self.assertEqual(first_args[0], ("93.184.216.34", 443))
        self.assertEqual(second_args[0], ("93.184.216.35", 443))


class TestExtraBlockedCidrs(TestCase):
    """``MCD_HTTP_BLOCKED_CIDRS`` env var feeds the ``_EXTRA_NETWORKS``
    list at import time. We test the parsing function directly and verify
    the hook honors the list via ``patch.object`` — no module reload
    required (reload would create new ``_policy`` / ``_safe_create_connection``
    objects, invalidating any references the tests hold)."""

    def tearDown(self):
        _policy.active = False

    def _assert_hook_blocks(self, ip: str, *, extra_networks):
        """Activate the policy, patch _EXTRA_NETWORKS, and verify the
        hook rejects ``ip`` before any connect."""
        _policy.active = True
        _policy.strict_ip_policy = False
        with patch.object(url_safety, "_EXTRA_NETWORKS", extra_networks):
            with patch.object(url_safety, "_original_create_connection") as called:
                with self.assertRaises(HttpClientError):
                    _safe_create_connection((ip, 443), timeout=1)
            called.assert_not_called()

    def test_extra_cidr_honored_by_hook(self):
        self._assert_hook_blocks(
            "203.0.113.5",
            extra_networks=[ipaddress.ip_network("203.0.113.0/24")],
        )

    def test_multiple_extra_cidrs_honored_by_hook(self):
        extra = [
            ipaddress.ip_network("203.0.113.0/24"),
            ipaddress.ip_network("198.51.100.0/24"),
        ]
        self._assert_hook_blocks("203.0.113.7", extra_networks=extra)
        self._assert_hook_blocks("198.51.100.7", extra_networks=extra)

    def test_env_var_parsing_at_import(self):
        # Verifies the env var actually feeds _EXTRA_NETWORKS — covers the
        # parsing path that runs at module load.
        with patch.dict(
            "os.environ", {"MCD_HTTP_BLOCKED_CIDRS": "203.0.113.0/24,198.51.100.0/24"}
        ):
            networks = url_safety._load_extra_blocked_networks()
        self.assertEqual(
            sorted(str(n) for n in networks),
            ["198.51.100.0/24", "203.0.113.0/24"],
        )

    def test_invalid_cidr_logged_and_skipped(self):
        with self.assertLogs(
            "apollo.integrations.http.url_safety", level="WARNING"
        ) as cm:
            result = url_safety._parse_cidrs(
                ("not-a-cidr", "203.0.113.0/24"), source="test"
            )
        self.assertEqual([str(n) for n in result], ["203.0.113.0/24"])
        self.assertTrue(
            any("invalid CIDR" in msg for msg in cm.output),
            f"expected 'invalid CIDR' warning, got: {cm.output}",
        )


class TestSafetyPolicyContext(TestCase):
    """The context manager is what callers actually invoke. Verify it
    sets and clears the per-thread policy and validates the URL upfront."""

    def tearDown(self):
        _policy.active = False

    def test_activates_and_deactivates(self):
        self.assertFalse(getattr(_policy, "active", False))
        with safety_policy("https://example.com/"):
            self.assertTrue(_policy.active)
        self.assertFalse(_policy.active)

    def test_deactivates_on_exception(self):
        with self.assertRaises(RuntimeError):
            with safety_policy("https://example.com/"):
                raise RuntimeError("boom")
        self.assertFalse(_policy.active)

    def test_url_scheme_check_runs_upfront(self):
        # The scheme check is at the URL layer; failure must happen before
        # the policy is even activated.
        with self.assertRaises(HttpClientError) as ctx:
            with safety_policy("ftp://example.com/"):
                self.fail("body should not run")
        self.assertIn("scheme", str(ctx.exception))
        self.assertFalse(getattr(_policy, "active", False))

    def test_https_only_rejects_http(self):
        with self.assertRaises(HttpClientError) as ctx:
            with safety_policy("http://93.184.216.34/", https_only=True):
                pass
        self.assertIn("scheme", str(ctx.exception))

    def test_rejects_localhost(self):
        with self.assertRaises(HttpClientError) as ctx:
            with safety_policy("https://localhost/foo"):
                pass
        self.assertIn("localhost", str(ctx.exception))

    def test_rejects_empty_host(self):
        with self.assertRaises(HttpClientError):
            with safety_policy("https:///foo"):
                pass

    def test_url_none_skips_upfront_validation(self):
        # Advanced callers that have done their own URL validation can
        # activate the policy without an upfront URL check.
        with safety_policy(url=None, strict_ip_policy=True):
            self.assertTrue(_policy.active)
            self.assertTrue(_policy.strict_ip_policy)
        self.assertFalse(_policy.active)


class TestSafeRequest(TestCase):
    """End-to-end smoke test on the public ``safe_request`` API."""

    def tearDown(self):
        _policy.active = False

    @patch("apollo.integrations.http.url_safety.requests.request")
    def test_passes_method_url_and_kwargs_through(self, mock_request):
        mock_request.return_value = "stub-response"
        result = safe_request(
            "GET", "https://93.184.216.34/path", timeout=5, headers={"X": "y"}
        )
        self.assertEqual(result, "stub-response")
        mock_request.assert_called_once_with(
            "GET", "https://93.184.216.34/path", timeout=5, headers={"X": "y"}
        )

    def test_rejects_unsupported_scheme_before_calling_requests(self):
        with patch(
            "apollo.integrations.http.url_safety.requests.request"
        ) as mock_request:
            with self.assertRaises(HttpClientError):
                safe_request("GET", "ftp://example.com/")
            mock_request.assert_not_called()

    @patch("apollo.integrations.http.url_safety.requests.request")
    def test_clears_policy_on_exception(self, mock_request):
        mock_request.side_effect = RuntimeError("boom")
        with self.assertRaises(RuntimeError):
            safe_request("GET", "https://93.184.216.34/")
        self.assertFalse(getattr(_policy, "active", False))
