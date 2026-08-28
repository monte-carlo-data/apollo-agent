"""The Data Collector's SQL Server payload, checked against this repo's real schemas.

Every SQL Server Windows-auth failure found by testing PRO-3016 end to end was the same
shape: a contract mismatch between the collector and the agent, invisible to unit tests on
either side because each builds its own fixtures. The collector's tests assert the dict it
sends; this repo's tests assert what the pipeline does with a dict it wrote itself. Nothing
checked one against the other.

Three of those four failures would have been caught here:

  - the mapper rejecting login_timeout / query_timeout_in_seconds, which the collector
    always sends and SqlServerOdbcArgs did not declare
  - the UID template raising on a credential with neither user nor username
  - port typed as a string by the collector and an integer by the schema

The fixtures below mirror `_create_kerberos_connection` in the data-collector repo,
`lambdas/clients/plugins/sql_server_plugins/plugin_sql_server.py`. They are a copy, not an
import -- the repos have no shared package -- so they must be updated alongside that
function. The mirror test on the collector side asserts it produces this shape.
"""

from contextlib import suppress
from unittest import TestCase

from cerberus import Validator

from apollo.integrations.ctp.defaults.sql_server import (
    SQL_SERVER_CREDENTIALS_SCHEMA,
)
from apollo.integrations.ctp.registry import CtpRegistry

# PluginConnectionSchema.port is Optional[str], so the collector sends a STRING. Written
# out here rather than normalised, because the point is to test what it actually sends.
_DC_PORT = "1433"

# Non-default for the same reason as the query timeout below: 15 is the shared
# fallback on both sides, so it could not distinguish survival from a default.
_DC_LOGIN_TIMEOUT = 22
# Deliberately NOT the 840s default both sides fall back to -- a default here would
# pass whether or not the value actually survived the hop.
_DC_QUERY_TIMEOUT = 600


def _dc_payload(**connect_args_overrides) -> dict:
    """The credentials dict the collector sends for a kerberos SQL Server connection."""
    connect_args = {
        "auth_type": "kerberos",
        "host": "winauth-lab-sql.mclab.internal",
        "port": _DC_PORT,
        "realm": "MCLAB.INTERNAL",
        "kdc": "mclab.internal",
        "principal": "svc-montecarlo@MCLAB.INTERNAL",
    }
    connect_args.update(connect_args_overrides)
    connect_args = {k: v for k, v in connect_args.items() if v is not None}
    connect_args.setdefault("database", "mcdemo")
    return {
        "connect_args": connect_args,
        "login_timeout": _DC_LOGIN_TIMEOUT,
        "query_timeout": _DC_QUERY_TIMEOUT,
    }


class SqlServerDcContractTest(TestCase):
    """Resolve the collector's payload through the real pipeline entry point."""

    def setUp(self):
        self._temp_files: list[str] = []

    def tearDown(self):
        import os

        for path in self._temp_files:
            with suppress(OSError):
                os.unlink(path)

    def _resolve(self, payload: dict) -> dict:
        """The resolved connect_args, as the proxy client receives them.

        CtpRegistry.resolve is what the agent itself calls -- not CtpPipeline.execute,
        which the other tests use and which skips both the connect_args unwrapping and
        the sibling merge. It returns the mapper output nested back under connect_args,
        so unwrap it here.

        The sibling merge matters: the collector sends login_timeout alongside
        connect_args, not inside it, and _build_pipeline_input folds outer siblings into
        the pipeline input. Tests that call CtpPipeline.execute directly never see those
        fields, which is why the mapper rejecting them reached production.
        """
        resolved = CtpRegistry.resolve(
            "sql-server", payload, temp_files=self._temp_files
        )
        return resolved["connect_args"]

    def test_password_form_resolves_to_valid_connect_args(self):
        # No kinit stub needed: the pipeline only materializes files now. Acquisition
        # happens in sql_server_kerberos_env, around the connect.
        args = self._resolve(_dc_payload(password="s3cret"))

        self.assertEqual("yes", args["Trusted_Connection"])
        self.assertNotIn("UID", args)
        self.assertNotIn("PWD", args)
        self.assertEqual("tcp:winauth-lab-sql.mclab.internal,1433", args["SERVER"])

    def test_keytab_form_resolves_to_valid_connect_args(self):
        args = self._resolve(_dc_payload(keytab_base64="a2V5dGFiLWJ5dGVz"))

        self.assertEqual("yes", args["Trusted_Connection"])
        self.assertNotIn("UID", args)
        self.assertNotIn("PWD", args)

    def test_the_timeouts_the_collector_always_sends_survive_the_mapper(self):
        """The regression that reached the UI: the mapper rejected its own output because
        SqlServerOdbcArgs did not declare fields the base field map emits."""
        args = self._resolve(_dc_payload(keytab_base64="a2V5dGFi"))
        self.assertEqual(_DC_LOGIN_TIMEOUT, args["login_timeout"])
        # The collector sends `database` inside connect_args; the mapper has to emit it or
        # the session silently lands in master.
        self.assertEqual("mcdemo", args["DATABASE"])
        # The collector spells this `query_timeout` while the mapper reads
        # `query_timeout_in_seconds`. Asserted with a NON-default value: both sides default
        # to 840, so a default would pass whether or not the value actually survived.
        self.assertEqual(_DC_QUERY_TIMEOUT, args["query_timeout_in_seconds"])

    def test_a_string_port_is_accepted(self):
        """The collector types port as a string. The mapper interpolates it into SERVER,
        so a string is fine there -- this pins that, since the credentials schema below
        disagreed for a while."""
        args = self._resolve(_dc_payload(keytab_base64="a2V5dGFi", port="14330"))
        self.assertEqual("tcp:winauth-lab-sql.mclab.internal,14330", args["SERVER"])

    def test_the_collector_payload_satisfies_the_self_hosted_credentials_schema(self):
        """SQL_SERVER_CREDENTIALS_SCHEMA documents the self-hosted credentials JSON, and
        is enforced only on the validate-credentials operation -- not on the connection
        path the collector takes.

        Asserting the collector's payload against it anyway keeps the two producers of the
        same shape honest: a customer hand-writing JSON and the collector building a dict
        should not disagree about types.
        """
        for label, extra in (
            ("password", {"password": "s3cret"}),
            ("keytab", {"keytab_base64": "a2V5dGFi"}),
        ):
            with self.subTest(form=label):
                payload = _dc_payload(**extra)
                v = Validator(SQL_SERVER_CREDENTIALS_SCHEMA)
                self.assertTrue(
                    v.validate(payload),
                    f"collector payload rejected by the schema: {v.errors}",
                )
