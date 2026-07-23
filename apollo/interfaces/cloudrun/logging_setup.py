import logging

from google.cloud.logging import Client
from google.cloud.logging.handlers import StructuredLogHandler, setup_logging


def setup_cloud_run_logging(client: Client, is_debug_log: bool) -> None:
    """Install the stdout-based ``StructuredLogHandler`` for Cloud Run.

    We pin ``StructuredLogHandler`` explicitly rather than relying on
    ``Client.setup_logging()``'s environment auto-detection. For the
    ``cloud_run_revision`` resource type the library already selects this same
    handler (see ``get_default_handler()`` in
    ``google/cloud/logging_v2/client.py``), so this is behavior-preserving.

    Pinning it makes the log path unambiguous: records are written to stdout and
    ingested out-of-process by Cloud Run, never held in an in-process gRPC buffer
    (as the API-based ``CloudLoggingHandler`` would). This avoids any chance of
    falling back to the buffered handler if resource detection fails, and
    documents for future reviewers that we do not buffer logs in-process.

    :param client: the GCP logging client, used to resolve the project id for
        the structured-log trace fields.
    :param is_debug_log: when True, the root logger is set to ``DEBUG`` level;
        otherwise ``INFO``.
    """
    setup_logging(
        StructuredLogHandler(project_id=client.project),
        log_level=logging.DEBUG if is_debug_log else logging.INFO,
    )
