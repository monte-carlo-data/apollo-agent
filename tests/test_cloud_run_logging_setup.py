import logging
from unittest import TestCase
from unittest.mock import patch, create_autospec

from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler, StructuredLogHandler

from apollo.interfaces.cloudrun.logging_setup import setup_cloud_run_logging


class CloudRunLoggingSetupTests(TestCase):
    """Guards the Cloud Run log path against silently switching to the buffered,
    API-based ``CloudLoggingHandler``. A security review raised that an in-process
    gRPC log buffer would let an attacker with RCE drop log records before
    ingestion; we pin the stdout-based ``StructuredLogHandler`` so that risk does
    not apply. These tests fail loudly if that pin is ever lost.
    """

    def _run_setup(self, is_debug_log: bool):
        client = create_autospec(Client, instance=True)
        client.project = "test-project-id"
        # patch the module-level setup_logging so we capture the handler without
        # mutating the global root logger state.
        with patch(
            "apollo.interfaces.cloudrun.logging_setup.setup_logging"
        ) as setup_logging_mock:
            setup_cloud_run_logging(client, is_debug_log)
        setup_logging_mock.assert_called_once()
        return setup_logging_mock.call_args

    def test_installs_stdout_structured_log_handler(self):
        handler = self._run_setup(is_debug_log=False).args[0]

        # the handler must be the stdout/stream-based StructuredLogHandler ...
        self.assertIsInstance(handler, StructuredLogHandler)
        self.assertIsInstance(handler, logging.StreamHandler)
        # ... and explicitly NOT the buffered, API-based CloudLoggingHandler.
        self.assertNotIsInstance(handler, CloudLoggingHandler)
        # the buffered handler is the one that carries an in-process transport.
        self.assertFalse(hasattr(handler, "transport"))

    def test_passes_project_id_from_client(self):
        handler = self._run_setup(is_debug_log=False).args[0]
        self.assertEqual(handler.project_id, "test-project-id")

    def test_log_level_defaults_to_info(self):
        call_args = self._run_setup(is_debug_log=False)
        self.assertEqual(call_args.kwargs["log_level"], logging.INFO)

    def test_log_level_debug_when_enabled(self):
        call_args = self._run_setup(is_debug_log=True)
        self.assertEqual(call_args.kwargs["log_level"], logging.DEBUG)
