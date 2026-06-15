"""Tests for owner-only (0o600) writing of CA temp files.

Security context: CA bundles written for db2/teradata/aws (and the CTP
resolve_ssl_options transform) used to be created world-readable (0o644) via
plain open(). They are now written atomically with owner-only permissions.
"""

import os
import stat
import tempfile
from unittest import TestCase

from apollo.integrations.db.ssl_options import SslOptions, write_owner_only

_CA_DATA = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----"


def _mode(path: str) -> int:
    return stat.S_IMODE(os.stat(path).st_mode)


class TestWriteOwnerOnly(TestCase):
    def test_new_file_is_owner_only_with_content(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            write_owner_only(path, "DATA")
            self.assertEqual(0o600, _mode(path))
            with open(path) as f:
                self.assertEqual("DATA", f.read())

    def test_existing_world_readable_file_becomes_owner_only(self):
        # The case that motivated the atomic write: an existing 0o644 file must
        # end up 0o600 with the new contents, never exposing them at 0o644.
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            with open(path, "w") as f:
                f.write("OLD")
            os.chmod(path, 0o644)

            write_owner_only(path, "NEW")

            self.assertEqual(0o600, _mode(path))
            with open(path) as f:
                self.assertEqual("NEW", f.read())

    def test_no_temp_file_left_behind(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            write_owner_only(path, "DATA")
            # Only the target file should remain in the directory.
            self.assertEqual(["ca.pem"], os.listdir(d))


class TestWriteCaDataToTempFile(TestCase):
    def test_writes_owner_only(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            returned = SslOptions(ca_data=_CA_DATA).write_ca_data_to_temp_file(
                path, upsert=True
            )
            self.assertEqual(path, returned)
            self.assertEqual(0o600, _mode(path))
            with open(path) as f:
                self.assertEqual(_CA_DATA, f.read())

    def test_raises_when_file_exists_and_not_upsert(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            open(path, "w").close()
            with self.assertRaises(ValueError):
                SslOptions(ca_data=_CA_DATA).write_ca_data_to_temp_file(
                    path, upsert=False
                )

    def test_raises_when_no_ca_data(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "ca.pem")
            with self.assertRaises(ValueError):
                SslOptions().write_ca_data_to_temp_file(path, upsert=True)
