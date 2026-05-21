from unittest import TestCase
from unittest.mock import patch

from apollo.credentials import external_credentials_cache as cache_module
from apollo.credentials.base import BaseCredentialsService
from apollo.credentials.external_credentials_cache import (
    _cache_key,
    clear_external_credentials_cache,
    load_cached,
)


class _RecordingCredentialsService(BaseCredentialsService):
    """Subclass that counts calls and returns a fresh dict each time."""

    def __init__(self):
        super().__init__(provider_name="recording")
        self.calls = 0

    def _load_external_credentials(self, credentials: dict) -> dict:
        self.calls += 1
        return {"resolved": True, "call_number": self.calls}


class _FailingCredentialsService(BaseCredentialsService):
    def __init__(self):
        super().__init__(provider_name="failing")
        self.calls = 0

    def _load_external_credentials(self, credentials: dict) -> dict:
        self.calls += 1
        raise ValueError("boom")


def _noop_loader(credentials: dict) -> dict:
    return {}


class TestCredentialsCacheKey(TestCase):
    def test_connect_args_do_not_affect_key(self):
        a = {"aws_secret": "s1", "aws_region": "us-east-1", "connect_args": {"x": 1}}
        b = {"aws_secret": "s1", "aws_region": "us-east-1", "connect_args": {"x": 2}}
        c = {"aws_secret": "s1", "aws_region": "us-east-1"}
        self.assertEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))
        self.assertEqual(_cache_key(a, _noop_loader), _cache_key(c, _noop_loader))

    def test_loader_does_not_affect_key(self):
        creds = {"aws_secret": "s1"}

        def loader_a(c):
            return {}

        def loader_b(c):
            return {}

        self.assertEqual(_cache_key(creds, loader_a), _cache_key(creds, loader_b))

    def test_different_secret_name_yields_different_key(self):
        a = {"aws_secret": "s1", "aws_region": "us-east-1"}
        b = {"aws_secret": "s2", "aws_region": "us-east-1"}
        self.assertNotEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))

    def test_different_region_yields_different_key(self):
        a = {"aws_secret": "s1", "aws_region": "us-east-1"}
        b = {"aws_secret": "s1", "aws_region": "us-west-2"}
        self.assertNotEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))

    def test_different_assumable_role_yields_different_key(self):
        a = {"aws_secret": "s1", "assumable_role": "arn:aws:iam::111:role/a"}
        b = {"aws_secret": "s1", "assumable_role": "arn:aws:iam::222:role/b"}
        self.assertNotEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))

    def test_different_provider_yields_different_key(self):
        a = {"self_hosted_credentials_type": "aws_secrets_manager", "secret": "s"}
        b = {"self_hosted_credentials_type": "azure_key_vault", "secret": "s"}
        self.assertNotEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))

    def test_key_is_stable_across_key_order(self):
        a = {"aws_region": "us-east-1", "aws_secret": "s1"}
        b = {"aws_secret": "s1", "aws_region": "us-east-1"}
        self.assertEqual(_cache_key(a, _noop_loader), _cache_key(b, _noop_loader))


class TestCacheBehavior(TestCase):
    def setUp(self):
        clear_external_credentials_cache()

    def tearDown(self):
        clear_external_credentials_cache()

    def test_cache_hit_skips_load(self):
        svc = _RecordingCredentialsService()
        creds = {"aws_secret": "s1"}
        first = svc.get_credentials(creds)
        second = svc.get_credentials(creds)
        self.assertEqual(1, svc.calls)
        self.assertEqual({"resolved": True, "call_number": 1}, first)
        # second hit also returns call_number=1 (the cached value), not a new fetch
        self.assertEqual({"resolved": True, "call_number": 1}, second)

    def test_different_keys_do_not_share_cache(self):
        svc = _RecordingCredentialsService()
        svc.get_credentials({"aws_secret": "s1"})
        svc.get_credentials({"aws_secret": "s2"})
        self.assertEqual(2, svc.calls)

    def test_connect_args_only_difference_is_a_cache_hit(self):
        svc = _RecordingCredentialsService()
        svc.get_credentials({"aws_secret": "s1", "connect_args": {"warehouse": "a"}})
        svc.get_credentials({"aws_secret": "s1", "connect_args": {"warehouse": "b"}})
        self.assertEqual(1, svc.calls)

    def test_connect_args_still_merge_on_cache_hit(self):
        svc = _RecordingCredentialsService()
        svc._load_external_credentials = (  # type: ignore[method-assign]
            lambda credentials: {"connect_args": {"password": "secret"}}
        )
        # First call populates the cache
        first = svc.get_credentials(
            {"aws_secret": "s1", "connect_args": {"username": "alice"}}
        )
        self.assertEqual(
            {"connect_args": {"username": "alice", "password": "secret"}}, first
        )
        # Second call uses different incoming connect_args; merge must still apply
        second = svc.get_credentials(
            {"aws_secret": "s1", "connect_args": {"username": "bob"}}
        )
        self.assertEqual(
            {"connect_args": {"username": "bob", "password": "secret"}}, second
        )

    def test_failed_load_is_not_cached(self):
        svc = _FailingCredentialsService()
        creds = {"aws_secret": "s1"}
        with self.assertRaises(ValueError):
            svc.get_credentials(creds)
        with self.assertRaises(ValueError):
            svc.get_credentials(creds)
        self.assertEqual(2, svc.calls)

    def test_passthrough_base_service_bypasses_cache(self):
        # Default BaseCredentialsService is a passthrough — caching it would
        # pin request-specific dicts. Confirm we don't.
        svc = BaseCredentialsService(provider_name="passthrough")
        svc.get_credentials({"aws_secret": "s1", "connect_args": {"x": 1}})
        # Nothing should have been written to the shared cache
        self.assertEqual(0, len(cache_module._CACHE))

    def test_returned_dict_can_be_mutated_without_corrupting_cache(self):
        svc = _RecordingCredentialsService()
        creds = {"aws_secret": "s1"}
        first = svc.get_credentials(creds)
        first["resolved"] = "MUTATED"  # type: ignore[assignment]
        second = svc.get_credentials(creds)
        self.assertEqual(True, second["resolved"])  # cached value unchanged
        self.assertEqual(1, svc.calls)

    def test_nested_mutation_does_not_corrupt_cached_value(self):
        """A shallow copy would also pass the top-level mutation test above.
        This one specifically pins the deep-copy semantics: mutating a
        nested dict in the returned value must not propagate to subsequent
        cache reads."""

        class _NestedReturningService(BaseCredentialsService):
            def __init__(self):
                super().__init__(provider_name="nested")
                self.calls = 0

            def _load_external_credentials(self, credentials: dict) -> dict:
                self.calls += 1
                return {"outer": {"inner": {"value": "original"}}}

        svc = _NestedReturningService()
        creds = {"aws_secret": "s1"}
        first = svc.get_credentials(creds)
        first["outer"]["inner"]["value"] = "MUTATED"
        second = svc.get_credentials(creds)
        self.assertEqual("original", second["outer"]["inner"]["value"])
        self.assertEqual(1, svc.calls)

    def test_disabling_via_ttl_zero_calls_loader_every_time(self):
        with patch.object(cache_module, "_CACHE_TTL_SECONDS", 0):
            svc = _RecordingCredentialsService()
            creds = {"aws_secret": "s1"}
            svc.get_credentials(creds)
            svc.get_credentials(creds)
        self.assertEqual(2, svc.calls)

    def test_load_cached_with_ttl_zero_bypasses_cache_directly(self):
        with patch.object(cache_module, "_CACHE_TTL_SECONDS", 0):
            calls = []

            def loader(c):
                calls.append(c)
                return {"resolved": True}

            load_cached({"aws_secret": "s1"}, loader, "test_provider")
            load_cached({"aws_secret": "s1"}, loader, "test_provider")
        self.assertEqual(2, len(calls))


class TestLoadCachedLogging(TestCase):
    """The duration logs are how we'll verify the cache is doing its job in
    production. Pin the shape of the message so we don't accidentally break
    downstream parsing."""

    def setUp(self):
        clear_external_credentials_cache()

    def tearDown(self):
        clear_external_credentials_cache()

    def test_cache_miss_log_includes_provider_and_duration(self):
        svc = _RecordingCredentialsService()
        with self.assertLogs(
            "apollo.credentials.external_credentials_cache", level="INFO"
        ) as cm:
            svc.get_credentials({"aws_secret": "s1"})
        miss_logs = [m for m in cm.output if "cache=miss" in m]
        self.assertEqual(1, len(miss_logs), cm.output)
        self.assertIn("provider=recording", miss_logs[0])
        self.assertRegex(miss_logs[0], r"duration_s=\d+\.\d{3}")

    def test_cache_hit_log_emitted_on_second_call(self):
        svc = _RecordingCredentialsService()
        creds = {"aws_secret": "s1"}
        svc.get_credentials(creds)  # populate cache
        with self.assertLogs(
            "apollo.credentials.external_credentials_cache", level="INFO"
        ) as cm:
            svc.get_credentials(creds)
        hit_logs = [m for m in cm.output if "cache=hit" in m]
        self.assertEqual(1, len(hit_logs), cm.output)
        self.assertIn("provider=recording", hit_logs[0])
        self.assertRegex(hit_logs[0], r"duration_s=\d+\.\d{3}")

    def test_cache_disabled_log_emitted_when_ttl_zero(self):
        svc = _RecordingCredentialsService()
        with patch.object(cache_module, "_CACHE_TTL_SECONDS", 0):
            with self.assertLogs(
                "apollo.credentials.external_credentials_cache", level="INFO"
            ) as cm:
                svc.get_credentials({"aws_secret": "s1"})
        disabled_logs = [m for m in cm.output if "cache=disabled" in m]
        self.assertEqual(1, len(disabled_logs), cm.output)
        self.assertIn("provider=recording", disabled_logs[0])
        self.assertRegex(disabled_logs[0], r"duration_s=\d+\.\d{3}")

    def test_cache_miss_failed_log_emitted_when_loader_raises(self):
        svc = _FailingCredentialsService()
        with self.assertLogs(
            "apollo.credentials.external_credentials_cache", level="INFO"
        ) as cm:
            with self.assertRaises(ValueError):
                svc.get_credentials({"aws_secret": "s1"})
        failed_logs = [m for m in cm.output if "cache=miss-failed" in m]
        self.assertEqual(1, len(failed_logs), cm.output)
        self.assertIn("provider=failing", failed_logs[0])
        self.assertRegex(failed_logs[0], r"duration_s=\d+\.\d{3}")

    def test_cache_error_log_emitted_when_machinery_raises_before_loader(self):
        """Exceptions raised before the loader is invoked — e.g. inside the
        cache key hash, the lock acquisition, or the @cached decorator
        itself — must still produce a log line so a silent failure can't
        sneak past the diagnostics."""
        svc = _RecordingCredentialsService()
        # Force the cached wrapper to raise before the inner timed_loader
        # has a chance to run.
        with patch.object(
            cache_module, "_load_and_cache", side_effect=RuntimeError("machinery boom")
        ):
            with self.assertLogs(
                "apollo.credentials.external_credentials_cache", level="INFO"
            ) as cm:
                with self.assertRaises(RuntimeError):
                    svc.get_credentials({"aws_secret": "s1"})
        error_logs = [m for m in cm.output if "cache=error" in m]
        self.assertEqual(1, len(error_logs), cm.output)
        self.assertIn("provider=recording", error_logs[0])
        self.assertRegex(error_logs[0], r"duration_s=\d+\.\d{3}")
        # Loader was never called
        self.assertEqual(0, svc.calls)
