import os
import subprocess
import threading
import time
from contextlib import suppress
from unittest import TestCase
from unittest.mock import Mock, patch

from apollo.integrations.db import sql_server_kerberos_env as kerberos_env
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.db.sql_server_kerberos_env import (
    KerberosConnectionParams,
    KerberosEnvironmentError,
    kerberos_environment,
    pop_kerberos_params,
)

_MANAGED = ("KRB5_CONFIG", "KRB5_CLIENT_KTNAME", "KRB5CCNAME")
_PRINCIPAL = "svc-montecarlo@MCLAB.INTERNAL"
_PASSWORD = "p@ss-with-$dollar-and-'quote"


class _KerberosEnvTestBase(TestCase):
    def setUp(self):
        self._saved = {key: os.environ.get(key) for key in _MANAGED}
        for key in _MANAGED:
            os.environ.pop(key, None)
        self.addCleanup(self._restore)
        self.assertFalse(
            kerberos_env._KERBEROS_ENV_LOCK.locked(),
            "the kerberos env lock leaked from a prior test",
        )

    def _restore(self):
        for key, value in self._saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @staticmethod
    def _keytab_params(**overrides) -> dict:
        params = {
            "krb5_config_path": "/tmp/krb5-test.conf",
            "ccache": "MEMORY:abc123",
            "client_keytab_path": "/tmp/test.keytab",
        }
        params.update(overrides)
        return params

    @staticmethod
    def _password_params(**overrides) -> dict:
        params = {
            "krb5_config_path": "/tmp/krb5-test.conf",
            "ccache": "FILE:/tmp/test.ccache",
            "principal": _PRINCIPAL,
            "password": _PASSWORD,
        }
        params.update(overrides)
        return params


class TestKerberosEnvironmentScoping(_KerberosEnvTestBase):
    """The variables are visible inside the scope and gone after it.

    This is the reason the environment moved out of the CTP transform: libkrb5's settings
    are process-global, and Hive/Impala with auth_mechanism=GSSAPI read the same three
    variables. A SQL Server connection that left them set would point an unrelated
    integration at a single-realm config that gets deleted when the client closes.
    """

    def test_variables_are_set_inside_the_scope(self):
        params = self._keytab_params()
        with kerberos_environment(params):
            self.assertEqual(params["krb5_config_path"], os.environ["KRB5_CONFIG"])
            self.assertEqual(params["ccache"], os.environ["KRB5CCNAME"])
            self.assertEqual(
                params["client_keytab_path"], os.environ["KRB5_CLIENT_KTNAME"]
            )

    def test_variables_absent_before_are_absent_after(self):
        with kerberos_environment(self._keytab_params()):
            pass
        for key in _MANAGED:
            self.assertNotIn(key, os.environ, f"{key} outlived the connection")

    def test_pre_existing_values_are_restored(self):
        """The Hive case: another integration's configuration must survive intact."""
        os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"
        os.environ["KRB5CCNAME"] = "FILE:/tmp/hive.ccache"

        with kerberos_environment(self._keytab_params()):
            self.assertEqual("/tmp/krb5-test.conf", os.environ["KRB5_CONFIG"])

        self.assertEqual("/etc/krb5.conf", os.environ["KRB5_CONFIG"])
        self.assertEqual("FILE:/tmp/hive.ccache", os.environ["KRB5CCNAME"])
        self.assertNotIn("KRB5_CLIENT_KTNAME", os.environ)

    def test_restoration_happens_even_when_the_connect_raises(self):
        """A failed connect is the common case in the field, so restoring only on the
        happy path would leak the config almost every time it mattered."""
        os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"

        with self.assertRaises(RuntimeError):
            with kerberos_environment(self._keytab_params()):
                raise RuntimeError("pyodbc.connect failed")

        self.assertEqual("/etc/krb5.conf", os.environ["KRB5_CONFIG"])
        self.assertNotIn("KRB5_CLIENT_KTNAME", os.environ)

    def test_the_lock_is_released_after_the_scope(self):
        with kerberos_environment(self._keytab_params()):
            self.assertTrue(kerberos_env._KERBEROS_ENV_LOCK.locked())
        self.assertFalse(kerberos_env._KERBEROS_ENV_LOCK.locked())

    def test_incomplete_params_are_rejected_before_touching_the_environment(self):
        for missing in ("krb5_config_path", "ccache"):
            params = self._keytab_params()
            del params[missing]
            with self.assertRaises(KerberosEnvironmentError) as ctx:
                with kerberos_environment(params):
                    pass
            self.assertIn(missing, str(ctx.exception))
            self.assertNotIn("KRB5_CONFIG", os.environ)


class TestKerberosTicketAcquisition(_KerberosEnvTestBase):
    """The password form's TGT acquisition — SDD §5.3.

    The keytab form needs nothing: MIT Kerberos' default client keytab makes GSSAPI
    acquire and refresh the TGT itself. There is no equivalent for a stored password, so
    kinit runs in a subprocess.

    Verified against a live KDC on 2026-08-21 that a subprocess kinit populating a FILE
    ccache does satisfy msodbcsql's GSSAPI at connect time.
    """

    @patch.object(kerberos_env.subprocess, "run")
    def test_keytab_form_never_invokes_kinit(self, mock_run):
        """GSSAPI auto-acquires from the client keytab; a kinit here would be redundant
        work on every connection and would defeat the design's whole point."""
        with kerberos_environment(self._keytab_params()):
            pass
        mock_run.assert_not_called()

    @patch.object(kerberos_env.subprocess, "run")
    def test_password_form_acquires_a_tgt(self, mock_run):
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        with kerberos_environment(self._password_params()):
            pass

        self.assertEqual(1, mock_run.call_count)
        kinit_argv = mock_run.call_args_list[0].args[0]
        # Exact argv, not assertIn: the "--" is a security control (a principal beginning
        # with a dash would otherwise be parsed as an option), and assertIn passes with or
        # without it.
        self.assertEqual(["kinit", "--", _PRINCIPAL], kinit_argv)

    @patch.object(kerberos_env.subprocess, "run")
    def test_password_is_passed_on_stdin_never_in_argv(self, mock_run):
        """argv is world-readable via /proc; a password there leaks to any local process."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        with kerberos_environment(self._password_params()):
            pass

        kinit_call = mock_run.call_args_list[0]
        self.assertNotIn(_PASSWORD, kinit_call.args[0])
        self.assertIn(_PASSWORD, kinit_call.kwargs.get("input", ""))

    @patch.object(kerberos_env.subprocess, "run")
    def test_kinit_runs_against_this_connections_cache(self, mock_run):
        """kinit reads KRB5CCNAME, so it must already be set when the subprocess starts."""
        observed = {}

        def _capture(argv, **kwargs):
            observed["ccache"] = os.environ.get("KRB5CCNAME")
            observed["keytab"] = os.environ.get("KRB5_CLIENT_KTNAME")
            return Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = _capture
        with kerberos_environment(self._password_params()):
            pass

        self.assertEqual("FILE:/tmp/test.ccache", observed["ccache"])
        # An inherited keytab would let GSSAPI satisfy the connection from someone else's
        # credential under a different principal.
        self.assertIsNone(observed["keytab"])

    @patch.object(kerberos_env.subprocess, "run")
    def test_an_inherited_client_keytab_is_cleared_for_the_password_form(
        self, mock_run
    ):
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        os.environ["KRB5_CLIENT_KTNAME"] = "/tmp/someone-elses.keytab"

        with kerberos_environment(self._password_params()):
            self.assertNotIn("KRB5_CLIENT_KTNAME", os.environ)

        # Still restored afterwards -- clearing it is scoped, not destructive.
        self.assertEqual("/tmp/someone-elses.keytab", os.environ["KRB5_CLIENT_KTNAME"])

    @patch.object(kerberos_env.subprocess, "run")
    def test_a_ticket_is_always_acquired_rather_than_reused(self, mock_run):
        """No reuse probe. Each connection gets its own ccache, so a probe could only ever
        hit a concurrent connection's cache and reuse its ticket under another
        principal."""
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        for _ in range(2):
            with kerberos_environment(self._password_params()):
                pass

        self.assertEqual(2, mock_run.call_count)


class TestKerberosAcquisitionFailures(_KerberosEnvTestBase):
    @patch.object(kerberos_env.subprocess, "run")
    def test_kinit_failure_raises_without_echoing_the_password(self, mock_run):
        mock_run.return_value = Mock(
            returncode=1, stdout="", stderr="kinit: Password incorrect"
        )
        with self.assertRaises(KerberosEnvironmentError) as ctx:
            with kerberos_environment(self._password_params()):
                pass

        message = str(ctx.exception)
        self.assertNotIn(_PASSWORD, message)
        self.assertIn("Password incorrect", message)  # actionable

    @patch.object(kerberos_env.subprocess, "run")
    def test_kinit_timeout_is_reported_as_a_kdc_reachability_problem(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="kinit", timeout=30)
        with self.assertRaises(KerberosEnvironmentError) as ctx:
            with kerberos_environment(self._password_params()):
                pass

        message = str(ctx.exception).lower()
        self.assertIn("kdc", message)
        self.assertNotIn(_PASSWORD.lower(), message)

    @patch.object(kerberos_env.subprocess, "run")
    def test_missing_kinit_binary_is_actionable(self, mock_run):
        """krb5-user is a deployment prerequisite; say so rather than surfacing ENOENT."""
        mock_run.side_effect = FileNotFoundError("kinit")
        with self.assertRaises(KerberosEnvironmentError) as ctx:
            with kerberos_environment(self._password_params()):
                pass
        self.assertIn("krb5", str(ctx.exception).lower())

    @patch.object(kerberos_env.subprocess, "run")
    def test_a_failed_acquisition_still_restores_the_environment(self, mock_run):
        """Otherwise a bad password would leave every later Kerberos consumer in this
        process pointed at a config that is about to be deleted."""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="kinit: failed")
        os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"

        with suppress(KerberosEnvironmentError):
            with kerberos_environment(self._password_params()):
                pass

        self.assertEqual("/etc/krb5.conf", os.environ["KRB5_CONFIG"])
        self.assertFalse(kerberos_env._KERBEROS_ENV_LOCK.locked())


class TestKerberosEnvironmentConcurrency(_KerberosEnvTestBase):
    @patch.object(kerberos_env.subprocess, "run")
    def test_scopes_do_not_interleave(self, mock_run):
        """The lock spans the whole scope, not just the kinit.

        Holding it only over the acquisition -- as an earlier version did -- left the
        later pyodbc.connect reading variables another thread could already have
        repointed, so two connections under different principals could cross.
        """
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        observed_stable = []
        errors: list[BaseException] = []

        def _worker(index: int):
            params = self._password_params(ccache=f"FILE:/tmp/ccache-{index}")
            try:
                with kerberos_environment(params):
                    # Stand in for pyodbc.connect: read, yield the GIL, read again. Under
                    # a scope-wide lock the value cannot move; under a kinit-only lock it
                    # can.
                    before = os.environ.get("KRB5CCNAME")
                    time.sleep(0.05)
                    observed_stable.append(before == os.environ.get("KRB5CCNAME"))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual([], errors)
        self.assertEqual([True] * 8, observed_stable)


class TestPopKerberosParams(TestCase):
    def test_removes_the_block_from_connect_args(self):
        """It must not reach odbc_string_from_dict, which stringifies every value it is
        given -- so a dict left here would serialize the keytab path, and on the password
        form the password itself, into the connection string."""
        connect_args = {"SERVER": "tcp:host,1433", "kerberos": {"ccache": "MEMORY:x"}}
        params = pop_kerberos_params(connect_args)

        self.assertEqual({"ccache": "MEMORY:x"}, params)
        self.assertNotIn("kerberos", connect_args)

    def test_returns_none_on_the_sql_login_path(self):
        connect_args = {"SERVER": "tcp:host,1433", "UID": "u", "PWD": "p"}
        self.assertIsNone(pop_kerberos_params(connect_args))
        self.assertEqual(
            {"SERVER": "tcp:host,1433", "UID": "u", "PWD": "p"}, connect_args
        )

    def test_a_non_dict_block_is_rejected(self):
        with self.assertRaises(KerberosEnvironmentError):
            pop_kerberos_params({"kerberos": "MEMORY:x"})


class TestKerberosParamsContract(TestCase):
    """The transform and this module agree on the key names (review F4 on #379).

    They span two modules with no call edge, so a rename on one side fails at runtime
    rather than at type-check. These pin the names in one place.
    """

    def test_the_env_module_reads_exactly_the_keys_the_type_declares(self):
        declared = set(KerberosConnectionParams.__required_keys__) | set(
            KerberosConnectionParams.__optional_keys__
        )
        consumed = {
            kerberos_env._ATTR_KRB5_CONFIG_PATH,
            kerberos_env._ATTR_CCACHE,
            kerberos_env._ATTR_CLIENT_KEYTAB_PATH,
            kerberos_env._ATTR_PRINCIPAL,
            kerberos_env._ATTR_PASSWORD,
        }
        self.assertEqual(declared, consumed)

    def test_the_transform_emits_only_declared_keys(self):
        """Catches a key added to the transform that this module would silently ignore."""
        declared = set(KerberosConnectionParams.__required_keys__) | set(
            KerberosConnectionParams.__optional_keys__
        )
        for creds in (
            {"keytab_base64": "a2V5dGFiLWJ5dGVz"},
            {"password": "pw"},
        ):
            with self.subTest(form=next(iter(creds))):
                temp_files: list[str] = []
                resolved = CtpRegistry.resolve(
                    "sql-server",
                    {
                        "auth_type": "kerberos",
                        "host": "labsql.mclab.internal",
                        "port": 1433,
                        "realm": "MCLAB.INTERNAL",
                        "kdc": "labdc.mclab.internal",
                        "principal": _PRINCIPAL,
                        **creds,
                    },
                    temp_files=temp_files,
                )
                for path in temp_files:
                    with suppress(OSError):
                        os.unlink(path)
                emitted = set(resolved["connect_args"][kerberos_env.ATTR_KERBEROS])
                self.assertTrue(
                    emitted <= declared,
                    f"undeclared keys emitted: {emitted - declared}",
                )

    def test_the_password_key_is_the_one_the_redactor_matches(self):
        """The password stays out of logs only because the key contains "pass". Renaming it
        to something like "secret" would keep every other test green while exposing it.
        """
        self.assertIn("pass", kerberos_env._ATTR_PASSWORD)


class TestOtherKerberosConsumersDuringTheWindow(_KerberosEnvTestBase):
    """What a non-SQL-Server Kerberos consumer sees while a connect is in progress.

    Answers review F1 on #379. The lock is private to sql_server_kerberos_env, so it
    serializes SQL Server Windows-auth connects against each other and not against other
    libkrb5 consumers -- Hive and Impala with auth_mechanism=GSSAPI read the same three
    variables and nothing else in the agent manages them.

    These tests pin the actual behaviour rather than leaving it as a hypothesis: the
    exposure is real, and it is bounded to the connect window.
    """

    @staticmethod
    def _other_consumer_view() -> dict:
        """What libkrb5 reads at connect time for any other integration."""
        return {k: os.environ.get(k) for k in _MANAGED}

    @patch.object(kerberos_env.subprocess, "run")
    def test_another_consumer_reads_our_config_during_the_connect(self, mock_run):
        """CONFIRMED EXPOSURE, not a regression guard.

        If this ever starts failing because another consumer no longer sees our values,
        that is the shared-lock fix landing -- update the module docstring rather than
        "fixing" the test.
        """
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        inside, release = threading.Event(), threading.Event()
        observed: dict = {}

        def sql_server_connect():
            with kerberos_environment(self._keytab_params()):
                inside.set()
                release.wait(timeout=5)

        def other_consumer():
            inside.wait(timeout=5)
            observed.update(self._other_consumer_view())
            release.set()

        threads = [
            threading.Thread(target=sql_server_connect),
            threading.Thread(target=other_consumer),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        self.assertEqual("/tmp/krb5-test.conf", observed["KRB5_CONFIG"])
        self.assertEqual("MEMORY:abc123", observed["KRB5CCNAME"])
        self.assertEqual("/tmp/test.keytab", observed["KRB5_CLIENT_KTNAME"])

    @patch.object(kerberos_env.subprocess, "run")
    def test_the_exposure_is_bounded_to_the_connect(self, mock_run):
        """The other half, and the reason this is a narrow window rather than a broken
        agent: once the connect returns, another consumer sees the environment it had.
        """
        mock_run.return_value = Mock(returncode=0, stdout="", stderr="")
        os.environ["KRB5_CONFIG"] = "/etc/krb5.conf"

        with kerberos_environment(self._keytab_params()):
            pass

        self.assertEqual("/etc/krb5.conf", self._other_consumer_view()["KRB5_CONFIG"])
        self.assertIsNone(self._other_consumer_view()["KRB5_CLIENT_KTNAME"])
