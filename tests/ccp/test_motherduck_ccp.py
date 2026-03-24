# tests/ccp/test_motherduck_ccp.py
#
# The proxy client expects connect_args to be the pre-built connection string
# "md:{db_name}?motherduck_token={token}" (a string, not a dict). Not registered
# until Phase 2 updates MotherDuckProxyClient to build the string from the dict.
# Tests use CcpPipeline().execute() directly.
from unittest import TestCase

from apollo.integrations.ccp.defaults.motherduck import MOTHERDUCK_DEFAULT_CCP
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(credentials: dict) -> dict:
    return CcpPipeline().execute(MOTHERDUCK_DEFAULT_CCP, credentials)


class TestMotherDuckCcp(TestCase):
    def test_motherduck_not_registered(self):
        self.assertIsNone(CcpRegistry.get("motherduck"))

    def test_basic_connection(self):
        args = _resolve({"db_name": "mydb", "token": "md_token_abc123"})
        self.assertEqual("mydb", args["db_name"])
        self.assertEqual("md_token_abc123", args["token"])
