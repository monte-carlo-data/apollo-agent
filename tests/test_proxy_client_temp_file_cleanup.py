"""Tests for CTP temp-file cleanup tied to the proxy client lifecycle.

Security context: TLS cert/key (and ini) files materialized by the CTP
pipeline used to persist for the lifetime of the container. They are now
registered on the proxy client and deleted when the client is closed.
"""

import os
import tempfile
from unittest import TestCase

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
