"""Tests for owner-only (0o600), per-client CA temp-file writing.

Security context: CA bundles written for db2/teradata/aws (and the CTP
resolve_ssl_options transform) used to be created world-readable (0o644) at a
deterministic path. They are now written to a unique 0o600 temp file per call,
so (a) the file is never world-readable and (b) one client closing can never
delete a CA file still in use by another client with the same CA data.
"""

import os
import stat
from unittest import TestCase

from apollo.integrations.db.ssl_options import SslOptions

_CA_DATA = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----"


def _mode(path: str) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


class TestWriteCaDataToTempFile(TestCase):
    def test_writes_owner_only_with_content_and_suffix(self):
        path = SslOptions(ca_data=_CA_DATA).write_ca_data_to_temp_file(
            suffix="_db2_ca.pem"
        )
        try:
            self.assertTrue(path.endswith("_db2_ca.pem"))
            self.assertEqual(0o600, _mode(path))
            with open(path) as f:
                self.assertEqual(_CA_DATA, f.read())
        finally:
            os.unlink(path)

    def test_each_call_returns_a_unique_path(self):
        # The fix for the shared-deterministic-path bug: two clients with the
        # SAME CA data must get DIFFERENT files, so closing one cannot delete
        # the other's CA file.
        opts = SslOptions(ca_data=_CA_DATA)
        p1 = opts.write_ca_data_to_temp_file()
        p2 = opts.write_ca_data_to_temp_file()
        try:
            self.assertNotEqual(p1, p2)
            self.assertTrue(os.path.exists(p1))
            self.assertTrue(os.path.exists(p2))
        finally:
            os.unlink(p1)
            os.unlink(p2)

    def test_raises_when_no_ca_data(self):
        with self.assertRaises(ValueError):
            SslOptions().write_ca_data_to_temp_file()
