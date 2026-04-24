# tests/ctp/test_motherduck_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.motherduck import MOTHERDUCK_DEFAULT_CTP
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(credentials: dict) -> dict:
    return CtpPipeline().execute(MOTHERDUCK_DEFAULT_CTP, credentials)


class TestMotherDuckCtp(TestCase):
    def test_registered(self):
        self.assertIsNotNone(CtpRegistry.get("motherduck"))

    def test_basic_connection(self):
        args = _resolve({"db_name": "mydb", "token": "md_token_abc123"})
        self.assertEqual("mydb", args["db_name"])
        self.assertEqual("md_token_abc123", args["token"])
