"""Shared relative-path validation for REST-passthrough proxy clients.

Several proxy clients (Snowflake's ``execute_rest_request``, Salesforce Data
Cloud's ``ssot_get``) accept a caller-supplied ``path`` that they append to a
connection-owned host and call with a connection-owned credential (a session
token or a minted OAuth token). If ``path`` were allowed to carry its own
scheme or host, the credential would be sent to an attacker-controlled
destination instead of the connection's own host. ``validate_relative_rest_path``
is the single place that check lives, so both callers enforce the same rules.
"""

import urllib.parse
from typing import Any


def validate_relative_rest_path(path: Any) -> None:
    """Raise ``ValueError`` unless ``path`` is a plain relative URL path.

    A valid path starts with a single ``/``, carries no scheme or host (so
    the request can't be redirected to a different origin), and has no
    embedded ``\\r``/``\\n`` (so it can't smuggle extra header/request lines
    into the outgoing request line).
    """
    try:
        split = urllib.parse.urlsplit(path) if isinstance(path, str) else None
    except ValueError:
        split = None
    if (
        split is None
        or split.scheme
        or split.netloc
        or not path.startswith("/")
        or path.startswith("//")
        or "\r" in path
        or "\n" in path
    ):
        raise ValueError(
            f"path must be a relative path beginning with '/' (no scheme or "
            f"host, no CR/LF), got: {path!r}"
        )
