"""Tests for CTP temp-file cleanup tied to the proxy client lifecycle.

Security context: TLS cert/key (and ini) files materialized by the CTP
pipeline used to persist for the lifetime of the container. They are now
registered on the proxy client and deleted when the client is closed.
"""

import os
import tempfile
from unittest import TestCase
from unittest.mock import patch

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.db.base_db_proxy_client import BaseDbProxyClient


class _StubProxyClient(BaseProxyClient):
    """Minimal concrete client — exercises the base close()/register path."""

    @property
    def wrapped_client(self):
        return None


def _write_temp(content: str = "SECRET_KEY_DATA") -> str:
    fd, path = tempfile.mkstemp(suffix=".pem")
    with os.fdopen(fd, "w") as f:
        f.write(content)
    return path


class TestBaseProxyClientTempFileCleanup(TestCase):
    def test_close_removes_registered_files(self):
        path = _write_temp()
        client = _StubProxyClient()
        client.register_temp_files([path])

        self.assertTrue(os.path.exists(path))
        client.close()
        self.assertFalse(
            os.path.exists(path), "temp credential file should be deleted on close"
        )

    def test_close_logs_removal_count(self):
        # A positive log signal so cleanup can be confirmed in production logs.
        p1, p2 = _write_temp(), _write_temp()
        client = _StubProxyClient()
        client.register_temp_files([p1, p2])

        with self.assertLogs(
            "apollo.integrations.base_proxy_client", level="INFO"
        ) as cm:
            client.close()
        self.assertTrue(
            any(
                "Removed 2 temporary file(s) on connection close" in m
                for m in cm.output
            ),
            cm.output,
        )

    def test_close_logs_only_actually_removed_count(self):
        # A registered path that's already gone must not inflate the count —
        # the log reports real unlinks, not registrations.
        real = _write_temp()
        client = _StubProxyClient()
        client.register_temp_files([real, real + ".never-created"])

        with self.assertLogs(
            "apollo.integrations.base_proxy_client", level="INFO"
        ) as cm:
            client.close()
        self.assertTrue(
            any(
                "Removed 1 temporary file(s) on connection close" in m
                for m in cm.output
            ),
            cm.output,
        )

    def test_cleanup_log_message_survives_redaction(self):
        # The agent's log redaction replaces an entire message with "__redacted__"
        # if it contains credential/key/auth/secret/token/password. The cleanup
        # log must not trip it, or the confirmation signal is unusable in prod.
        from apollo.common.agent.redact import AgentRedactUtilities
        from apollo.common.agent.constants import ATTRIBUTE_VALUE_REDACTED

        message = "Removed 3 temporary file(s) on connection close"
        self.assertEqual(message, AgentRedactUtilities.standard_redact(message))
        self.assertNotEqual(
            ATTRIBUTE_VALUE_REDACTED, AgentRedactUtilities.standard_redact(message)
        )

    def test_close_with_no_temp_files_logs_nothing(self):
        # No noise on the common path (clients with no registered temp files).
        client = _StubProxyClient()
        with self.assertNoLogs("apollo.integrations.base_proxy_client", level="INFO"):
            client.close()

    def test_close_with_no_registered_files_is_noop(self):
        # Clients constructed outside the factory never call register_temp_files;
        # close() must not blow up on the missing attribute.
        _StubProxyClient().close()

    def test_register_accumulates_and_skips_falsy(self):
        p1, p2 = _write_temp(), _write_temp()
        client = _StubProxyClient()
        client.register_temp_files([p1, None, ""])
        client.register_temp_files([p2])

        client.close()
        self.assertFalse(os.path.exists(p1))
        self.assertFalse(os.path.exists(p2))

    def test_close_is_idempotent_and_tolerates_missing_file(self):
        path = _write_temp()
        client = _StubProxyClient()
        client.register_temp_files([path])

        client.close()
        # Second close (e.g. via __del__) must not raise on the already-gone file.
        client.close()
        self.assertFalse(os.path.exists(path))


class _StubDbProxyClient(BaseDbProxyClient):
    """DB client whose connection close is a no-op."""

    def __init__(self):
        super().__init__(connection_type="stub")

    @property
    def wrapped_client(self):
        return None


class _RaisingProxyClient(BaseProxyClient):
    """Client whose teardown raises — exercises the finally cleanup path."""

    @property
    def wrapped_client(self):
        return None

    def _close_client(self):
        raise RuntimeError("connection teardown failed")


class TestGetCertPathRegistration(TestCase):
    @patch("apollo.integrations.db.base_db_proxy_client.urlretrieve")
    @patch("apollo.integrations.db.base_db_proxy_client.AgentUtils.temp_file_path")
    def test_get_cert_path_registers_downloaded_cert(
        self, mock_temp_path, _mock_urlretrieve
    ):
        # get_cert_path (used by Presto and MySQL) must register the cert it
        # downloads so it is deleted on close — otherwise those connectors leak
        # the cert for the container lifetime.
        mock_temp_path.return_value = "/tmp/_stub_downloaded_cert.pem"
        client = _StubDbProxyClient()

        path = client.get_cert_path(
            platform="p", remote_location="https://host/cert.pem"
        )

        self.assertEqual("/tmp/_stub_downloaded_cert.pem", path)
        self.assertEqual([path], client._temp_files)


class TestCloseTemplate(TestCase):
    def test_db_client_close_removes_temp_files(self):
        path = _write_temp()
        client = _StubDbProxyClient()
        client.register_temp_files([path])

        client.close()
        self.assertFalse(os.path.exists(path))

    def test_temp_files_removed_even_when_close_client_raises(self):
        # The whole point of the template method: a failing connection teardown
        # must not leave the credential file lingering.
        path = _write_temp()
        client = _RaisingProxyClient()
        client.register_temp_files([path])

        with self.assertRaises(RuntimeError):
            client.close()
        self.assertFalse(
            os.path.exists(path),
            "temp file must be removed even when _close_client raises",
        )
