"""httplib2 client factory for Google API discovery clients.

``googleapiclient.discovery.build()`` builds its own ``httplib2.Http`` through
``googleapiclient.http.build_http()``, which takes no configuration. Two settings
need overriding, so the client is built here and passed in as ``http=``:

- **Decompression ratio.** httplib2 0.32.0 (CVE-2026-59939) rejects a gzip'd
  response whose decompressed size exceeds 100x the compressed size, once the
  output passes its 10 MiB ``safe_limit``. Highly repetitive JSON responses
  legitimately exceed that. ``HTTPLIB2_DECODE_LIMIT_RATIO`` stays the operator
  override.
- **Timeout.** ``build_http()`` derives the timeout from
  ``socket.getdefaulttimeout()``, so the only way to influence it was to mutate
  that process global before calling ``build()`` — a race under the agent's
  threaded workers, where an overlapping build can restore another request's
  value as the process default. The same default is read here, but passed
  explicitly instead of written.

``build()`` rejects ``credentials=`` and ``http=`` together, so this factory also
takes over the credential handling ``build()`` would otherwise do: resolving
Application Default Credentials when none are supplied, and applying scopes.
Without the scoping step, service-account credentials fail to refresh.

Nothing here is specific to one API: callers supply their own scopes, and any
discovery-based Google API client can use this factory. BigQuery is currently the
only caller, and is where the ratio limit was first hit.

Note: the SSRF guard in ``url_safety`` hooks ``urllib3``, and httplib2 does not
use urllib3 — requests made through this client are not covered by it.
"""

import logging
import os
import socket
from typing import Optional, Sequence, cast

import google.auth
import httplib2
from google.auth.credentials import Credentials, with_scopes_if_required
from google_auth_httplib2 import AuthorizedHttp

logger = logging.getLogger(__name__)

# httplib2 reads the lowercase name first and the uppercase one second; mirrored
# here so the limit resolves identically whether it is applied by us or by httplib2.
_DECODE_LIMIT_RATIO_ENV_VARS = (
    "httplib2_decode_limit_ratio",
    "HTTPLIB2_DECODE_LIMIT_RATIO",
)

# Highest ratio observed on legitimate BigQuery responses is ~255x, measured as a
# running ratio over 64 KiB input chunks. httplib2's own default is 100.
DEFAULT_DECODE_LIMIT_RATIO = 500.0

# googleapiclient.http.DEFAULT_HTTP_TIMEOUT_SEC
DEFAULT_TIMEOUT_SECONDS = 60


def resolve_decode_limit_ratio() -> float:
    """Resolve the gzip amplification limit, honoring httplib2's own env vars.

    Resolved here rather than deferred to httplib2 (by leaving the kwarg unset)
    because httplib2 silently discards an unparseable value and falls back to its
    100x default — which would quietly re-break collection.
    """
    for name in _DECODE_LIMIT_RATIO_ENV_VARS:
        raw = os.getenv(name)
        if not raw:
            continue
        try:
            return float(raw)
        except ValueError:
            logger.warning(
                "Ignoring invalid %s=%r, using %s",
                name,
                raw,
                DEFAULT_DECODE_LIMIT_RATIO,
            )
    return DEFAULT_DECODE_LIMIT_RATIO


def build_authorized_http(
    credentials: Optional[Credentials],
    scopes: Sequence[str],
    timeout: Optional[float] = None,
) -> AuthorizedHttp:
    """Build an authorized httplib2 client to pass to ``discovery.build(http=...)``.

    :param credentials: credentials to authorize requests with, or ``None`` to use
        Application Default Credentials.
    :param scopes: OAuth scopes to apply to credentials that require them.
    :param timeout: socket timeout in seconds. Defaults to the process socket
        default, then to ``DEFAULT_TIMEOUT_SECONDS``, matching ``build_http()``.
    """
    resolved_credentials: Credentials
    if credentials is None:
        adc_credentials, _ = google.auth.default(scopes=list(scopes))
        resolved_credentials = cast(Credentials, adc_credentials)
    else:
        resolved_credentials = cast(
            Credentials, with_scopes_if_required(credentials, list(scopes))
        )

    if timeout is None:
        timeout = socket.getdefaulttimeout()
    if timeout is None:
        timeout = DEFAULT_TIMEOUT_SECONDS

    http = httplib2.Http(
        timeout=timeout,
        # httplib2 ships no py.typed, so this resolves against typeshed stubs that
        # predate 0.32.0. The kwarg exists at runtime and is covered by tests.
        decode_limit_ratio=resolve_decode_limit_ratio(),  # type: ignore[reportCallIssue]
    )
    # As build_http() does: Google APIs use 308 for resumable uploads rather than
    # as a permanent redirect.
    http.redirect_codes = http.redirect_codes - {308}

    return AuthorizedHttp(resolved_credentials, http=http)
