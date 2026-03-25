# tests/ctp/test_motherduck_ctp.py
#
# The proxy client expects connect_args to be the pre-built connection string
# "md:{db_name}?motherduck_token={token}" (a string, not a dict). Not registered
# until Phase 2 updates MotherDuckProxyClient to build the string from the dict.
# Tests use CtpPipeline().execute() directly.
from unittest import TestCase

from apollo.integrations.ctp.defaults.motherduck import MOTHERDUCK_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(MOTHERDUCK_DEFAULT_CTP, credentials)


class TestMotherDuckCtp(TestCase):
    def test_motherduck_not_registered(self):
        self.assertIsNone(CtpRegistry.get("motherduck"))

    def test_basic_connection(self):
        args = _resolve({"db_name": "mydb", "token": "md_token_abc123"})
        self.assertEqual("mydb", args["db_name"])
        self.assertEqual("md_token_abc123", args["token"])
