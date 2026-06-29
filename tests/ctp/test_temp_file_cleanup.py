"""End-to-end: CTP resolve surfaces materialized temp-file paths so the proxy
client can delete them on close.
"""

import os
from unittest import TestCase

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.ctp.registry import CtpRegistry


class _StubProxyClient(BaseProxyClient):
    @property
    def wrapped_client(self):
        return None


class TestCtpResolveSurfacesTempFiles(TestCase):
    def test_http_resolve_records_ca_temp_file(self):
        creds = {
            "connect_args": {"token": "t"},
            "ssl_options": {"ca_data": "-----BEGIN CERTIFICATE-----\nX\n"},
        }
        temp_files: list[str] = []
        resolved = CtpRegistry.resolve("http", creds, temp_files=temp_files)

        self.assertEqual(1, len(temp_files))
        ca_path = temp_files[0]
        self.assertTrue(os.path.exists(ca_path))
        # The resolved connect_args points ssl_verify at the same file.
        self.assertEqual(ca_path, resolved["connect_args"]["ssl_verify"])

        # A client that registers them cleans them up on close.
        client = _StubProxyClient()
        client.register_temp_files(temp_files)
        client.close()
        self.assertFalse(os.path.exists(ca_path))

    def test_resolve_without_temp_files_arg_still_works(self):
        # Backwards compatibility: the out-param is optional.
        resolved = CtpRegistry.resolve("http", {"connect_args": {"token": "t"}})
        self.assertIn("connect_args", resolved)
