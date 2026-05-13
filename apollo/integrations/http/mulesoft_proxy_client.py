"""Proxy client for MuleSoft Anypoint Platform connections.

Subclasses ``HttpProxyClient`` rather than composing it because every operation
the MuleSoft integration needs on the agent side is fundamentally an HTTP
request — Anypoint REST calls (``do_request`` / ``download_bytes``), JAR
HEAD'ing for change detection (``head_external_url``-equivalent via
``do_request``), and the MuleSoft-specific flow-source extraction added here.
The subclass exists so MuleSoft-aware logic (the Mule artifact manifest layout,
``configs[]`` semantics, ``properties/`` conventions) doesn't leak into the
generic ``HttpProxyClient`` that other connectors share.

Contrast with ``InformaticaProxyClient`` (which composes ``HttpProxyClient``):
Informatica has session management + V2/V3 API dispatch logic that's
conceptually distinct from "HTTP". MuleSoft's agent-side concern is just
"HTTP plus a MuleSoft-aware post-step on one specific download", so inheritance
keeps the right amount of code-reuse without introducing delegation boilerplate.

See YET-1229 for the wire contract this client implements + YET-1215 for the
end-to-end architectural rationale (parse where the bytes live).
"""

from __future__ import annotations

import base64
import io
import json
import logging
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from apollo.integrations.http.http_proxy_client import HttpProxyClient

_logger = logging.getLogger(__name__)

# Structural locations inside a Mule application JAR that
# ``extract_mulesoft_sources`` reads. The manifest names the XML configs to
# extract; everything under ``properties/`` is included verbatim so the
# downstream parser can resolve ``${...}`` placeholders.
_MULESOFT_ARTIFACT_MANIFEST = "META-INF/mule-artifact/mule-artifact.json"
_MULESOFT_PROPERTIES_PREFIX = "properties/"


class MulesoftHttpProxyClient(HttpProxyClient):
    """HTTP proxy client for the ``mulesoft`` connection type.

    Inherits the full HTTP surface from ``HttpProxyClient`` (so existing
    Anypoint REST calls — ``do_request`` / ``download_bytes`` / etc. —
    keep working unchanged) and adds the MuleSoft-specific
    ``extract_mulesoft_sources`` operation that data-collector's
    ``MulesoftExtractor`` invokes via the agent transport.

    No MuleSoft-specific construction is needed in ``__init__`` — the
    ``mulesoft`` connection-type's ``connect_args`` shape (auth token,
    SSL verification, region-resolved URLs) is already what
    ``HttpProxyClient`` consumes.
    """

    def extract_mulesoft_sources(
        self,
        url: str,
        timeout: int = 300,
        max_bytes: Optional[int] = None,
        no_auth: bool = True,
        additional_headers: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Stream a Mule application JAR from ``url`` to a transient tempfile,
        extract just the flow source files (the XML configs listed in
        ``META-INF/mule-artifact/mule-artifact.json::configs[]`` plus every
        entry under ``properties/``), repackage them into a small in-memory
        deflate zip, and return the result inline. **No agent-storage write.**
        The JAR exists only as a transient tempfile and is deleted on every
        exit path.

        This is the server-side companion to data-collector's YET-1215
        architecture: DC parses MuleSoft flow XML in-process, so all the
        agent needs to ship back is the small sources zip (~25 KB typical
        for a 100 MB JAR). DC then unzips locally, parses, and ships a
        ``parsed_app`` dict on its Kinesis event — the 100 MB JAR never
        crosses the JSON-base64 agent transport.

        Same streaming-download safety surface as ``download_to_storage``:
        HTTPS-only, non-public IP literals rejected, redirects refused,
        ``max_bytes`` cap fires mid-stream, error messages strip the URL,
        connection released on every exit path.

        Return shape (the DC contract — see YET-1229):

        * ``sources_zip_b64``: ``str | None`` — base64-encoded deflate zip
          of the extracted XML configs + ``properties/*`` files. ``None`` on
          extraction failure.
        * ``sources_size_bytes``: ``int | None`` — pre-base64 zip size, for
          ops visibility. ``None`` on extraction failure.
        * ``sources_extraction_status``: ``"ok"`` (sources extracted and
          inlined) or ``"extraction_failed"`` (missing / corrupt manifest,
          unreadable JAR). A specific ``configs[]`` entry that's missing
          from the JAR is *not* a failure — it's logged and skipped, and
          the op still returns ``"ok"`` with the entries it could read.

        Raises:
            HttpClientError: SSRF guard rejection, transport error, 3xx/4xx
                response, or ``max_bytes`` exceeded.
            HTTPError: 5xx response.
        """
        with self._stream_to_tempfile(
            url,
            timeout=timeout,
            max_bytes=max_bytes,
            no_auth=no_auth,
            additional_headers=additional_headers,
            op_label="extract_mulesoft_sources",
        ) as tmp_path:
            zip_bytes, status = _extract_mulesoft_sources_from_jar(tmp_path)

        return {
            "sources_zip_b64": (
                base64.b64encode(zip_bytes).decode("ascii")
                if zip_bytes is not None
                else None
            ),
            "sources_size_bytes": len(zip_bytes) if zip_bytes is not None else None,
            "sources_extraction_status": status,
        }


def _extract_mulesoft_sources_from_jar(tmp_path: str) -> Tuple[Optional[bytes], str]:
    """Open a downloaded Mule application JAR + repackage its flow sources.

    Returns ``(zip_bytes, status)`` where ``status`` is one of:

    * ``"ok"`` — extraction succeeded; ``zip_bytes`` is the in-memory
      deflate zip containing the listed config XMLs + every entry under
      ``properties/``.
    * ``"extraction_failed"`` — JAR was unreadable, the manifest was missing
      or corrupt, or some other unrecoverable error. ``zip_bytes`` is
      ``None`` in this case; the caller emits ``sources_zip_b64=None``.

    Individual ``configs[]`` entries that are listed in the manifest but
    missing from the JAR are logged as warnings and skipped — the op still
    returns ``"ok"`` with whatever entries it could read. The downstream
    parser is tolerant of partial config sets; failing the whole op for
    one stale manifest reference would lose more lineage than it preserves.
    """
    try:
        with zipfile.ZipFile(tmp_path) as jar:
            try:
                manifest_bytes = jar.read(_MULESOFT_ARTIFACT_MANIFEST)
            except KeyError:
                _logger.warning(
                    "mulesoft_sources_missing_manifest: %s not found in JAR",
                    _MULESOFT_ARTIFACT_MANIFEST,
                )
                return None, "extraction_failed"
            try:
                manifest = json.loads(manifest_bytes)
            except json.JSONDecodeError:
                _logger.exception(
                    "mulesoft_sources_corrupt_manifest: %s was not valid JSON",
                    _MULESOFT_ARTIFACT_MANIFEST,
                )
                return None, "extraction_failed"

            extracted: List[Tuple[str, bytes]] = []

            for name in manifest.get("configs") or []:
                try:
                    extracted.append((name, jar.read(name)))
                except KeyError:
                    _logger.warning(
                        "mulesoft_sources_missing_config_entry: configs[] referenced %s "
                        "but the entry is not in the JAR; skipping",
                        name,
                    )

            for info in jar.infolist():
                if info.is_dir():
                    continue
                if info.filename.startswith(_MULESOFT_PROPERTIES_PREFIX):
                    extracted.append((info.filename, jar.read(info.filename)))
    except zipfile.BadZipFile:
        _logger.exception(
            "mulesoft_sources_bad_zip: extract_mulesoft_sources received a non-ZIP body",
        )
        return None, "extraction_failed"

    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as repack:
        for name, content in extracted:
            repack.writestr(name, content)
    return out.getvalue(), "ok"
