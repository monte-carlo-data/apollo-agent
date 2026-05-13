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

# Defense-in-depth cap on total uncompressed bytes the helper will extract
# from a single JAR. Real Mule applications produce well under 100 KiB of
# flow sources + properties combined (the smoke-test fixture's 93 MiB JAR
# yields 19.5 KiB); 10 MiB is generous headroom. A crafted or buggy JAR
# that exceeds this terminates extraction with ``"extraction_failed"``
# rather than risk OOM from a zip-bomb expansion.
_MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES = 10 * 1024 * 1024  # 10 MiB


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


def _is_safe_zip_entry_name(name: str) -> bool:
    """Reject zip entry names that could escape an intended directory on
    downstream unzip (defense-in-depth against ZipSlip / path traversal).

    Returns ``True`` if ``name`` is a relative POSIX path with no
    parent-traversal segments, no leading slash, no backslashes, and no
    drive-letter prefix. Zip names should be forward-slash-separated per
    the spec (APPNOTE.TXT §4.4.17); anything else is a hand-crafted
    adversarial entry name.

    Notes:
        ``extract_mulesoft_sources`` doesn't write extracted entries to
        the filesystem (it repackages them into an in-memory zip),
        so ZipSlip here is *primarily* a downstream-consumer concern —
        the data-collector reads our zip back into memory only, but a
        future consumer that calls ``ZipFile.extract`` on the result
        would be vulnerable. Sanitising at the source closes that
        avenue regardless of what the consumer does.
    """
    if not name:
        return False
    if name.startswith("/"):
        return False
    if "\\" in name:
        # Backslash is never a directory separator in the zip spec.
        # Reject any name containing one — both the literal-backslash
        # case and the Windows-style path case.
        return False
    if ":" in name:
        # Drive-letter prefix (``C:`` etc.) — POSIX zip paths shouldn't
        # contain colons in any position.
        return False
    if any(part == ".." for part in name.split("/")):
        return False
    return True


def _extract_mulesoft_sources_from_jar(tmp_path: str) -> Tuple[Optional[bytes], str]:
    """Open a downloaded Mule application JAR + repackage its flow sources.

    Returns ``(zip_bytes, status)`` where ``status`` is one of:

    * ``"ok"`` — extraction succeeded; ``zip_bytes`` is the in-memory
      deflate zip containing the listed config XMLs + every entry under
      ``properties/``.
    * ``"extraction_failed"`` — JAR was unreadable, the manifest was missing
      or corrupt, the manifest had an unexpected shape, or the cumulative
      uncompressed extraction would exceed
      ``_MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES`` (zip-bomb defense).
      ``zip_bytes`` is ``None`` in this case; the caller emits
      ``sources_zip_b64=None``.

    Individual ``configs[]`` entries that are skipped (missing from the
    JAR, unsafe path, non-string manifest value) are logged as warnings;
    the op still returns ``"ok"`` with whatever entries it could read.
    The downstream parser is tolerant of partial config sets; failing the
    whole op for one stale manifest reference would lose more lineage
    than it preserves.

    Hardening (YET-1229 review feedback):

    * **Manifest shape** — the JSON root must be a dict, ``configs``
      must be a list, and each ``configs[]`` entry must be a string.
      Non-conforming manifests return ``"extraction_failed"``.
    * **ZipSlip / path traversal** — every entry name is validated by
      ``_is_safe_zip_entry_name`` before it's read from the JAR or
      written into the output zip. Unsafe names are skipped with a
      warning; the op continues.
    * **Zip bomb** — extraction tracks total uncompressed bytes via
      ``ZipInfo.file_size`` and aborts with ``"extraction_failed"`` if
      the cumulative size would exceed
      ``_MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES`` (10 MiB). Generous
      headroom over real-world Mule apps but bounded against runaway
      expansion from a crafted JAR.
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

            if not isinstance(manifest, dict):
                _logger.warning(
                    "mulesoft_sources_malformed_manifest: expected dict at JSON root, got %s",
                    type(manifest).__name__,
                )
                return None, "extraction_failed"

            configs = manifest.get("configs", [])
            if not isinstance(configs, list):
                _logger.warning(
                    "mulesoft_sources_malformed_manifest: 'configs' must be a list, got %s",
                    type(configs).__name__,
                )
                return None, "extraction_failed"

            extracted: List[Tuple[str, bytes]] = []
            total_uncompressed_bytes = 0
            # Build a name → ZipInfo lookup once so the per-entry size
            # check is O(1) and so we can validate ``configs[]`` entries
            # against the actual archive contents.
            entries_by_name: Dict[str, zipfile.ZipInfo] = {
                info.filename: info for info in jar.infolist()
            }

            def _try_extract(name: str, source: str) -> bool:
                """Read ``name`` from the JAR if it's safe + fits the cap.

                Returns ``True`` on success (entry was appended to
                ``extracted`` and ``total_uncompressed_bytes`` advanced),
                ``False`` if the entry was skipped (with a warning
                already logged). Raises nothing — the caller continues
                regardless. ``source`` is "manifest" or "properties" for
                log-key disambiguation.
                """
                nonlocal total_uncompressed_bytes
                if not _is_safe_zip_entry_name(name):
                    # f-string the log-key portion so the per-source key
                    # (``mulesoft_sources_unsafe_manifest_entry`` vs
                    # ``mulesoft_sources_unsafe_properties_entry``) shows up
                    # verbatim in the format string — makes log-search +
                    # test-assertion straightforward.
                    _logger.warning(
                        f"mulesoft_sources_unsafe_{source}_entry: %r is not a safe relative path; skipping",
                        name,
                    )
                    return False
                info = entries_by_name.get(name)
                if info is None:
                    _logger.warning(
                        f"mulesoft_sources_missing_{source}_entry: %r referenced by the manifest but not present in the JAR; skipping",
                        name,
                    )
                    return False
                if (
                    total_uncompressed_bytes + info.file_size
                    > _MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES
                ):
                    _logger.warning(
                        "mulesoft_sources_uncompressed_cap_exceeded: extracting %s (%d bytes) "
                        "would push the cumulative uncompressed total past the "
                        "%d-byte cap; aborting",
                        name,
                        info.file_size,
                        _MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES,
                    )
                    raise _UncompressedCapExceeded
                extracted.append((name, jar.read(name)))
                total_uncompressed_bytes += info.file_size
                return True

            try:
                for raw_name in configs:
                    if not isinstance(raw_name, str):
                        _logger.warning(
                            "mulesoft_sources_malformed_manifest: configs[] entry must be a string, got %s",
                            type(raw_name).__name__,
                        )
                        continue
                    _try_extract(raw_name, source="manifest")

                for info in jar.infolist():
                    if info.is_dir():
                        continue
                    if not info.filename.startswith(_MULESOFT_PROPERTIES_PREFIX):
                        continue
                    _try_extract(info.filename, source="properties")
            except _UncompressedCapExceeded:
                return None, "extraction_failed"
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


class _UncompressedCapExceeded(Exception):
    """Internal control-flow exception raised by ``_try_extract`` when the
    cumulative uncompressed extraction would exceed
    ``_MULESOFT_SOURCES_MAX_UNCOMPRESSED_BYTES``. Caught at the helper's
    outer scope to convert into the public ``"extraction_failed"``
    status; never propagates beyond ``_extract_mulesoft_sources_from_jar``.
    """
