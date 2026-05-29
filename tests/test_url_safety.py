import concurrent.futures
import ipaddress
import os
import socket
import threading
from unittest import TestCase
from unittest.mock import MagicMock, patch

from apollo.integrations.http import url_safety
from apollo.integrations.http.url_safety import (
    HttpClientError,
    _policy,
    _safe_create_connection,
    assert_safe_destination,
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
        _policy.strict_ip_policy = False

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

    # --- T-F10: Cross-thread isolation ---

    def test_policy_is_per_thread(self):
        """T-F10: A policy active on the main thread must not be visible on a
        worker thread. threading.local ensures each thread starts with no
        policy."""
        worker_observations = []
        worker_error: list = []

        def worker():
            try:
                active = getattr(_policy, "active", False)
                worker_observations.append(active)
            except Exception as exc:  # pragma: no cover
                worker_error.append(exc)

        url = "https://93.184.216.34/"
        with safety_policy(url, strict_ip_policy=True):
            self.assertTrue(_policy.active)
            self.assertTrue(_policy.strict_ip_policy)
            t = threading.Thread(target=worker)
            t.start()
            t.join()

        # Main thread: policy deactivated after context exits.
        self.assertFalse(_policy.active)
        # Worker thread: never saw the main thread's policy.
        self.assertEqual([], worker_error)
        self.assertEqual([False], worker_observations)

    def test_active_policy_on_one_thread_does_not_block_another(self):
        """T-F10 (additional): When the main thread has strict_ip_policy active,
        a worker thread that calls _safe_create_connection directly sees no
        active policy (passthrough), so RFC1918 addresses are allowed on the
        worker even though they'd be blocked on the main thread."""
        sentinel_sock = MagicMock(name="socket")
        worker_results: list = []
        worker_errors: list = []

        with patch.object(
            url_safety,
            "_original_create_connection",
            return_value=sentinel_sock,
        ) as called:

            def worker_task():
                try:
                    # RFC1918 — would be blocked under strict_ip_policy on main
                    # thread but must be a passthrough on the worker (no policy).
                    result = _safe_create_connection(("10.0.0.5", 443))
                    worker_results.append(result)
                except Exception as exc:  # pragma: no cover
                    worker_errors.append(exc)

            with safety_policy("https://93.184.216.34/", strict_ip_policy=True):
                t = threading.Thread(target=worker_task)
                t.start()
                t.join()
            # Now we can inspect `called` — patch is still active in this with block
            self.assertEqual([], worker_errors, f"worker raised: {worker_errors}")
            self.assertEqual([sentinel_sock], worker_results)
            self.assertEqual(called.call_count, 1)

    # --- T-F6: ThreadPoolExecutor does not propagate policy ---

    def test_threadpool_executor_does_not_propagate_policy(self):
        """T-F6 / T-F10 (docs contract): A policy active on the submitting
        thread does NOT propagate into a ThreadPoolExecutor worker. Callers
        that fan out HTTP work to a thread pool MUST re-enter safety_policy on
        the worker thread — the threading.local semantics make this explicit.

        This test locks the contract by asserting that an RFC1918 address
        (blocked under strict_ip_policy) does NOT raise HttpClientError when
        called from a pool worker — confirming the policy was not inherited."""
        sentinel_sock = MagicMock(name="socket")

        with patch.object(
            url_safety,
            "_original_create_connection",
            return_value=sentinel_sock,
        ):

            def worker_task():
                # Under the main thread's strict policy this would raise. On the
                # worker thread there is no policy — so it must be a passthrough.
                return _safe_create_connection(("10.0.0.5", 443))

            url = "https://93.184.216.34/"
            with safety_policy(url, strict_ip_policy=True):
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(worker_task)
                    result = future.result()  # raises if worker raised

        self.assertIs(sentinel_sock, result)


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

    # --- T-F1: Nested safety_policy save/restore ---

    def test_nested_safety_policy_restores_strict_flag(self):
        """T-F1: After an inner safety_policy (default tier) exits inside an
        outer safety_policy (strict tier), the outer's strict_ip_policy flag
        must be restored on the current thread."""
        url = "https://93.184.216.34/"
        with safety_policy(url, strict_ip_policy=True):
            self.assertTrue(_policy.strict_ip_policy)
            with safety_policy(url):  # inner: default tier (strict=False)
                self.assertFalse(_policy.strict_ip_policy)
            # Inner has exited — outer's strict flag must be back.
            self.assertTrue(
                _policy.strict_ip_policy,
                "outer strict_ip_policy was not restored after inner context exited",
            )

    def test_nested_safety_policy_restores_active_flag(self):
        """T-F1: After an inner safety_policy exits inside an outer
        safety_policy, the outer's active flag must still be True."""
        url = "https://93.184.216.34/"
        with safety_policy(url):
            self.assertTrue(_policy.active)
            with safety_policy(url):
                self.assertTrue(_policy.active)
            # Inner exited — outer is still active.
            self.assertTrue(
                _policy.active,
                "outer active flag was cleared when inner context exited",
            )

    def test_outer_safety_policy_stays_active_after_inner_exits(self):
        """T-F1 (regression): Both active and strict_ip_policy are correctly
        restored when a default-tier inner context exits inside a strict-tier
        outer context."""
        url = "https://93.184.216.34/"
        with safety_policy(url, strict_ip_policy=True):
            with safety_policy(url):  # inner: default tier
                pass
            # After inner exits, outer's full state must be intact.
            self.assertTrue(
                _policy.active,
                "_policy.active was False after inner context exited",
            )
            self.assertTrue(
                _policy.strict_ip_policy,
                "_policy.strict_ip_policy was False after inner context exited",
            )

    def test_nested_inner_exception_restores_outer_state(self):
        """The F1 fix's central guarantee: if an inner safety_policy
        raises mid-body, the outer context's prev_active and
        prev_strict are still restored on inner exit, and the outer
        body continues with its own state intact."""
        with safety_policy("https://93.184.216.34/", strict_ip_policy=False):
            outer_active_before = _policy.active
            outer_strict_before = _policy.strict_ip_policy
            with self.assertRaises(RuntimeError):
                with safety_policy("https://93.184.216.34/", strict_ip_policy=True):
                    # inner is now active with strict=True
                    self.assertTrue(_policy.active)
                    self.assertTrue(_policy.strict_ip_policy)
                    raise RuntimeError("boom")
            # after inner exits via exception, outer state must be intact
            self.assertEqual(_policy.active, outer_active_before)
            self.assertEqual(_policy.strict_ip_policy, outer_strict_before)
        # after outer exits, both back to baseline
        self.assertFalse(getattr(_policy, "active", False))
        self.assertFalse(getattr(_policy, "strict_ip_policy", False))


class TestRedirectGuard(TestCase):
    """T-F3: The create_connection hook guards every hop — including redirected
    connections — because it runs on every TCP connect, not just the first.

    Since requests_mock is not a dependency, these tests exercise the hook
    directly: call _safe_create_connection for an allowed host (simulating the
    initial connect) then for a blocked host (simulating the redirect connect).
    This is a direct proxy for the per-hop guarantee: if the hook runs on every
    create_connection call, it will block the redirect's TCP connect the same
    way it blocks the test's second direct call.
    """

    def tearDown(self):
        _policy.active = False
        _policy.strict_ip_policy = False

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_blocked_redirect_target_raises_on_connect(self, mock_gai):
        """T-F3: Simulates the initial connect succeeding (public IP) then the
        redirect connect being blocked (metadata IP literal). The hook must
        raise HttpClientError on the second call without ever delegating to
        the original create_connection."""
        url = "https://safe-source.example.com/"
        with safety_policy(url):
            # --- hop 1: initial request to a public host (hostname path) ---
            mock_gai.return_value = [_addrinfo("93.184.216.34")]
            sentinel = MagicMock(name="socket")
            with patch.object(
                url_safety, "_original_create_connection", return_value=sentinel
            ) as called_first:
                result = _safe_create_connection(("safe-source.example.com", 443))
            self.assertIs(sentinel, result)
            called_first.assert_called_once()

            # --- hop 2: redirect to cloud metadata (IP literal path) ---
            with patch.object(
                url_safety, "_original_create_connection"
            ) as called_second:
                with self.assertRaises(HttpClientError) as ctx:
                    _safe_create_connection(("169.254.169.254", 80))
            self.assertIn("blocked address", str(ctx.exception))
            called_second.assert_not_called()

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_safe_redirect_target_is_allowed(self, mock_gai):
        """T-F3 (complement): A redirect to a safe public IP must succeed — the
        guard must not over-block legitimate redirects."""
        url = "https://safe-source.example.com/"
        sentinel = MagicMock(name="socket")

        with safety_policy(url):
            # hop 1
            mock_gai.return_value = [_addrinfo("93.184.216.34")]
            with patch.object(
                url_safety, "_original_create_connection", return_value=sentinel
            ):
                _safe_create_connection(("safe-source.example.com", 443))

            # hop 2: redirect to another safe public host
            mock_gai.return_value = [_addrinfo("93.184.216.35")]
            with patch.object(
                url_safety, "_original_create_connection", return_value=sentinel
            ) as called:
                result = _safe_create_connection(("safe-redirect.example.com", 443))
            self.assertIs(sentinel, result)
            called.assert_called_once()


class TestRequireHttpsEnvVar(TestCase):
    """T-F14: MCD_HTTP_REQUIRE_HTTPS env-var feeds _REQUIRE_HTTPS_BY_DEFAULT.
    Tests use patch.object on the module attribute rather than reloading the
    module (which would create new _policy / _safe_create_connection objects
    and break other tests that hold references to the originals)."""

    def tearDown(self):
        _policy.active = False
        _policy.strict_ip_policy = False

    def test_default_tier_allows_http_when_env_var_false(self):
        """T-F14: When _REQUIRE_HTTPS_BY_DEFAULT is False (the default), the
        default policy tier must accept HTTP URLs."""
        with patch.object(url_safety, "_REQUIRE_HTTPS_BY_DEFAULT", False):
            # Must not raise — HTTP is allowed in the default tier.
            with safety_policy("http://93.184.216.34/"):
                self.assertTrue(_policy.active)

    def test_default_tier_rejects_http_when_env_var_true(self):
        """T-F14: When _REQUIRE_HTTPS_BY_DEFAULT is True (operator opt-in),
        the default policy tier must reject HTTP URLs the same way the strict
        tier does."""
        with patch.object(url_safety, "_REQUIRE_HTTPS_BY_DEFAULT", True):
            with self.assertRaises(HttpClientError) as ctx:
                with safety_policy("http://93.184.216.34/"):
                    self.fail("body must not run when env-var HTTPS enforcement is on")
        self.assertIn("scheme", str(ctx.exception))

    def test_strict_tier_rejects_http_regardless_of_env_var(self):
        """T-F14 (OR semantics): even with _REQUIRE_HTTPS_BY_DEFAULT=False, the
        strict tier (https_only=True) still rejects HTTP — the env-var default
        is OR'd into the explicit https_only flag, so both paths lead to
        rejection."""
        with patch.object(url_safety, "_REQUIRE_HTTPS_BY_DEFAULT", False):
            with self.assertRaises(HttpClientError) as ctx:
                with safety_policy("http://93.184.216.34/", https_only=True):
                    self.fail("strict tier must reject HTTP regardless of env-var")
        self.assertIn("scheme", str(ctx.exception))


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


class TestLoadBoolEnv(TestCase):
    """Direct unit tests for the env-var → bool parser used by
    _REQUIRE_HTTPS_BY_DEFAULT. The integration tests patch
    _REQUIRE_HTTPS_BY_DEFAULT directly so this parsing logic was
    previously untested."""

    def test_truthy_aliases_return_true(self):
        for value in ("true", "1", "yes", "on", "TRUE", "YeS", "  true  "):
            with patch.dict(os.environ, {"MCD_TEST": value}):
                self.assertTrue(
                    url_safety._load_bool_env("MCD_TEST", default=False),
                    f"expected True for {value!r}",
                )

    def test_falsy_aliases_return_false(self):
        for value in ("false", "0", "no", "off", "FALSE", "Off"):
            with patch.dict(os.environ, {"MCD_TEST": value}):
                self.assertFalse(
                    url_safety._load_bool_env("MCD_TEST", default=True),
                    f"expected False for {value!r}",
                )

    def test_empty_string_returns_default(self):
        with patch.dict(os.environ, {"MCD_TEST": ""}):
            self.assertTrue(url_safety._load_bool_env("MCD_TEST", default=True))
            self.assertFalse(url_safety._load_bool_env("MCD_TEST", default=False))

    def test_whitespace_only_returns_default(self):
        with patch.dict(os.environ, {"MCD_TEST": "   "}):
            self.assertTrue(url_safety._load_bool_env("MCD_TEST", default=True))
            self.assertFalse(url_safety._load_bool_env("MCD_TEST", default=False))

    def test_missing_env_var_returns_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("MCD_TEST", None)
            self.assertTrue(url_safety._load_bool_env("MCD_TEST", default=True))
            self.assertFalse(url_safety._load_bool_env("MCD_TEST", default=False))

    def test_invalid_value_logs_warning_and_returns_default(self):
        with patch.dict(os.environ, {"MCD_TEST": "maybe"}):
            with self.assertLogs(
                "apollo.integrations.http.url_safety", level="WARNING"
            ) as cm:
                result = url_safety._load_bool_env("MCD_TEST", default=False)
            self.assertFalse(result)
            self.assertTrue(any("invalid value" in msg for msg in cm.output))


class TestAssertSafeDestination(TestCase):
    """`assert_safe_destination(host, port)` is a no-connection variant of
    the SSRF guard for callers that own their own socket / telnet layer
    (the troubleshooting validators). It must reject empty / localhost,
    blocked IP literals, and hostnames that resolve to a blocked IP."""

    def test_rejects_empty_host(self):
        with self.assertRaises(HttpClientError):
            assert_safe_destination("", 80)

    def test_rejects_localhost(self):
        with self.assertRaises(HttpClientError) as ctx:
            assert_safe_destination("localhost", 80)
        self.assertIn("localhost", str(ctx.exception))

    def test_rejects_metadata_ip_literal(self):
        with self.assertRaises(HttpClientError) as ctx:
            assert_safe_destination("169.254.169.254", 80)
        self.assertIn("blocked address", str(ctx.exception))

    def test_rejects_loopback_ipv4(self):
        with self.assertRaises(HttpClientError):
            assert_safe_destination("127.0.0.1", 80)

    def test_rejects_loopback_ipv6(self):
        with self.assertRaises(HttpClientError):
            assert_safe_destination("::1", 80)

    def test_strict_policy_rejects_rfc1918(self):
        with self.assertRaises(HttpClientError):
            assert_safe_destination("10.0.0.5", 80, strict_ip_policy=True)

    def test_default_policy_allows_rfc1918(self):
        # Returns None on success (no exception).
        self.assertIsNone(assert_safe_destination("10.0.0.5", 80))

    def test_allows_public_ip_literal(self):
        self.assertIsNone(assert_safe_destination("93.184.216.34", 80))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_rejects_hostname_resolving_to_blocked_ip(self, mock_gai):
        mock_gai.return_value = [_addrinfo("169.254.169.254", port=80)]
        with self.assertRaises(HttpClientError) as ctx:
            assert_safe_destination("attacker.example.com", 80)
        self.assertIn("blocked address resolved from hostname", str(ctx.exception))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_allows_hostname_resolving_to_public_ip(self, mock_gai):
        mock_gai.return_value = [_addrinfo("93.184.216.34", port=80)]
        self.assertIsNone(assert_safe_destination("example.com", 80))

    @patch("apollo.integrations.http.url_safety.socket.getaddrinfo")
    def test_wraps_dns_failure(self, mock_gai):
        mock_gai.side_effect = socket.gaierror("Name or service not known")
        with self.assertRaises(HttpClientError) as ctx:
            assert_safe_destination("nonexistent.invalid", 80)
        self.assertIn("DNS resolution failed", str(ctx.exception))
