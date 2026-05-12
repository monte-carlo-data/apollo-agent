"""Tests for ``MulesoftHttpProxyClient.extract_mulesoft_sources`` (YET-1229).

The op streams a Mule application JAR to a transient tempfile, extracts the
config XMLs listed in ``META-INF/mule-artifact/mule-artifact.json::configs[]``
plus every entry under ``properties/``, repackages them into an in-memory
deflate zip, and returns the result inline. **No agent-storage write
anywhere on this path** — a regression test in this file patches
``get_storage_client`` and asserts it's never invoked.

Sibling test cases:

* ``test_http_client.py::TestDownloadToStorage`` — covers the underlying
  streaming-download + tempfile pattern that ``extract_mulesoft_sources``
  reuses verbatim. The SSRF / max_bytes / redirect / 4xx-5xx guards on
  ``_open_download_response`` are exhaustively tested there; we only
  pin one regression of each surface here (HTTPS-only + max_bytes).
"""

import base64
import io
import json
import os
import tempfile as tempfile_mod
import zipfile
from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import MagicMock, patch

from requests import HTTPError

from apollo.integrations.http.http_proxy_client import HttpClientError
from apollo.integrations.http.mulesoft_proxy_client import MulesoftHttpProxyClient


def _build_mule_jar(
    configs_in_manifest: list[str],
    config_xmls: dict[str, bytes],
    properties: dict[str, bytes] | None = None,
    manifest_override: bytes | None = None,
    include_manifest: bool = True,
) -> bytes:
    """Construct an in-memory Mule application JAR for the tests.

    ``configs_in_manifest`` is the list that lands in
    ``META-INF/mule-artifact/mule-artifact.json::configs[]``; ``config_xmls``
    is the actual XML payloads written at JAR root (keyed by filename — may
    be a subset of ``configs_in_manifest`` to exercise the missing-config
    skip path).
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as jar:
        if include_manifest:
            if manifest_override is not None:
                jar.writestr("META-INF/mule-artifact/mule-artifact.json", manifest_override)
            else:
                jar.writestr(
                    "META-INF/mule-artifact/mule-artifact.json",
                    json.dumps({"configs": configs_in_manifest}).encode("ascii"),
                )
        for name, content in config_xmls.items():
            jar.writestr(name, content)
        for name, content in (properties or {}).items():
            jar.writestr(name, content)
    return buf.getvalue()


def _chunked(payload: bytes, size: int = 8192) -> list[bytes]:
    """Split a bytestring into ``iter_content``-style chunks so the production
    streaming loop sees realistic data — the helper itself reads via
    ``response.iter_content(chunk_size=8192)``."""
    return [payload[i : i + size] for i in range(0, len(payload), size)] or [b""]


class TestExtractMulesoftSources(TestCase):
    """Tests for ``MulesoftHttpProxyClient.extract_mulesoft_sources``."""

    def _make_response(
        self,
        status_code: int = 200,
        body: bytes = b"",
    ) -> MagicMock:
        resp = MagicMock()
        resp.status_code = status_code
        resp.iter_content.return_value = iter(_chunked(body))
        resp.__enter__.return_value = resp
        resp.__exit__.return_value = None
        if 400 <= status_code < 600:
            resp.raise_for_status.side_effect = HTTPError(
                f"{status_code}", response=resp
            )
        else:
            resp.raise_for_status.return_value = None
        return resp

    @contextmanager
    def _no_storage_should_be_called(self):
        """Patch ``get_storage_client`` (consumer-side binding in
        ``http_proxy_client.py`` since the storage factory is hoisted there)
        and assert it's never invoked during the test body.
        ``extract_mulesoft_sources`` is meant to be storage-free; a
        regression that accidentally re-introduced an upload path would be
        caught here."""
        storage_client = MagicMock(name="storage_client_should_not_be_used")
        with patch(
            "apollo.integrations.http.http_proxy_client.get_storage_client",
            return_value=storage_client,
        ) as factory:
            yield factory
        factory.assert_not_called()
        storage_client.upload_file.assert_not_called()

    @patch("requests.get")
    def test_happy_path_extracts_configs_and_properties(self, mock_get):
        # 3 XML configs + 2 properties files. Verify the returned zip
        # contains exactly those 5 entries, status="ok",
        # sources_size_bytes populated, and no storage write.
        configs = ["a.xml", "b.xml", "c.xml"]
        config_xmls = {
            "a.xml": b"<mule xmlns='http://www.mulesoft.org/schema/mule/core'/>",
            "b.xml": b"<mule xmlns='http://www.mulesoft.org/schema/mule/core'/>",
            "c.xml": b"<mule xmlns='http://www.mulesoft.org/schema/mule/core'/>",
        }
        properties = {
            "properties/dev.yaml": b"db_host: dev.example\n",
            "properties/prod.yaml": b"db_host: prod.example\n",
        }
        jar = _build_mule_jar(configs, config_xmls, properties=properties)
        mock_get.return_value = self._make_response(body=jar)

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("ok", result["sources_extraction_status"])
        self.assertIsNotNone(result["sources_zip_b64"])
        self.assertIsNotNone(result["sources_size_bytes"])
        decoded = base64.b64decode(result["sources_zip_b64"])
        self.assertEqual(result["sources_size_bytes"], len(decoded))
        with zipfile.ZipFile(io.BytesIO(decoded)) as out:
            names = set(out.namelist())
        self.assertEqual(set(configs) | set(properties.keys()), names)

    @patch("requests.get")
    def test_missing_config_entry_skipped_op_still_returns_ok(self, mock_get):
        # Manifest lists 3 configs but the JAR only contains 2. Verify the
        # op still returns "ok" with the 2 present entries, and a warning
        # is logged on the missing one (per ticket: don't fail the whole
        # op for one stale manifest reference).
        configs = ["a.xml", "b.xml", "c.xml"]
        config_xmls = {
            "a.xml": b"<mule xmlns='http://www.mulesoft.org/schema/mule/core'/>",
            "b.xml": b"<mule xmlns='http://www.mulesoft.org/schema/mule/core'/>",
            # c.xml deliberately missing from the JAR
        }
        jar = _build_mule_jar(configs, config_xmls)
        mock_get.return_value = self._make_response(body=jar)

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.http.mulesoft_proxy_client._logger"
        ) as mock_logger:
            with self._no_storage_should_be_called():
                result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("ok", result["sources_extraction_status"])
        decoded = base64.b64decode(result["sources_zip_b64"])
        with zipfile.ZipFile(io.BytesIO(decoded)) as out:
            names = set(out.namelist())
        self.assertEqual({"a.xml", "b.xml"}, names)
        # A WARNING log was emitted for the skipped entry.
        warn_calls = [
            args
            for args, _ in mock_logger.warning.call_args_list
            if "mulesoft_sources_missing_config_entry" in args[0]
        ]
        self.assertEqual(1, len(warn_calls))

    @patch("requests.get")
    def test_missing_manifest_returns_extraction_failed(self, mock_get):
        # JAR has no META-INF/mule-artifact/mule-artifact.json.
        # status="extraction_failed", both sources_* fields None.
        jar = _build_mule_jar(
            configs_in_manifest=[],
            config_xmls={"a.xml": b"<mule/>"},
            include_manifest=False,
        )
        mock_get.return_value = self._make_response(body=jar)

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("extraction_failed", result["sources_extraction_status"])
        self.assertIsNone(result["sources_zip_b64"])
        self.assertIsNone(result["sources_size_bytes"])

    @patch("requests.get")
    def test_corrupt_manifest_returns_extraction_failed(self, mock_get):
        # The manifest file exists but isn't valid JSON. Same failure shape
        # as the missing-manifest case.
        jar = _build_mule_jar(
            configs_in_manifest=[],
            config_xmls={"a.xml": b"<mule/>"},
            manifest_override=b"{ not valid json",
        )
        mock_get.return_value = self._make_response(body=jar)

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("extraction_failed", result["sources_extraction_status"])
        self.assertIsNone(result["sources_zip_b64"])
        self.assertIsNone(result["sources_size_bytes"])

    @patch("requests.get")
    def test_not_a_zip_returns_extraction_failed(self, mock_get):
        # The response body is not a valid ZIP archive. zipfile.BadZipFile
        # raised internally → status="extraction_failed", no exception
        # propagates to the caller (the catalog walk should continue).
        mock_get.return_value = self._make_response(body=b"definitely not a zip")

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("extraction_failed", result["sources_extraction_status"])
        self.assertIsNone(result["sources_zip_b64"])
        self.assertIsNone(result["sources_size_bytes"])

    @patch("requests.get")
    def test_no_properties_directory_returns_ok_with_only_configs(self, mock_get):
        # JAR has only configs[] XMLs and nothing under properties/. The op
        # should succeed; the returned zip contains only the XMLs.
        configs = ["a.xml", "b.xml"]
        config_xmls = {name: b"<mule/>" for name in configs}
        jar = _build_mule_jar(configs, config_xmls, properties=None)
        mock_get.return_value = self._make_response(body=jar)

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            result = client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual("ok", result["sources_extraction_status"])
        decoded = base64.b64decode(result["sources_zip_b64"])
        with zipfile.ZipFile(io.BytesIO(decoded)) as out:
            names = set(out.namelist())
        self.assertEqual(set(configs), names)

    @patch("requests.get")
    def test_max_bytes_cap_aborts_and_cleans_up_tempfile(self, mock_get):
        # max_bytes enforced mid-stream. HttpClientError raised; tempfile
        # cleaned up; storage never consulted.
        mock_get.return_value = self._make_response(body=b"x" * 200)
        real_named_tempfile = tempfile_mod.NamedTemporaryFile
        captured_paths: list = []

        def capture(*args, **kwargs):
            f = real_named_tempfile(*args, **kwargs)
            captured_paths.append(f.name)
            return f

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.http.mulesoft_proxy_client.tempfile.NamedTemporaryFile",
            side_effect=capture,
        ):
            with self._no_storage_should_be_called():
                with self.assertRaises(HttpClientError) as ec:
                    client.extract_mulesoft_sources(
                        "https://s3.example/app.jar", max_bytes=50
                    )

        self.assertIn("50", str(ec.exception))
        self.assertEqual(1, len(captured_paths))
        self.assertFalse(
            os.path.exists(captured_paths[0]),
            f"tempfile {captured_paths[0]} should have been deleted",
        )

    def test_ssrf_guard_rejects_non_https(self):
        # The shared _open_download_response guards reject non-https URLs
        # before any I/O happens. Same surface as download_to_storage.
        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with self._no_storage_should_be_called():
            with self.assertRaises(HttpClientError):
                client.extract_mulesoft_sources("http://example.com/app.jar")

    @patch("requests.get")
    def test_tempfile_cleaned_up_on_extraction_failure(self, mock_get):
        # The tempfile must be deleted on every exit path — including the
        # extraction-failed branch where the body wasn't a valid ZIP.
        # Mirrors download_to_storage's "tempfile cleaned up when upload
        # raises" coverage, scoped to this op's failure mode.
        mock_get.return_value = self._make_response(body=b"not a zip at all")
        real_named_tempfile = tempfile_mod.NamedTemporaryFile
        captured_paths: list = []

        def capture(*args, **kwargs):
            f = real_named_tempfile(*args, **kwargs)
            captured_paths.append(f.name)
            return f

        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        with patch(
            "apollo.integrations.http.mulesoft_proxy_client.tempfile.NamedTemporaryFile",
            side_effect=capture,
        ):
            with self._no_storage_should_be_called():
                client.extract_mulesoft_sources("https://s3.example/app.jar")

        self.assertEqual(1, len(captured_paths))
        self.assertFalse(
            os.path.exists(captured_paths[0]),
            f"tempfile {captured_paths[0]} should have been deleted",
        )

    @patch("requests.get")
    def test_inherits_http_proxy_client_surface(self, mock_get):
        # Sanity: the subclass inherits the parent's HTTP surface so the
        # MuleSoft REST client (DC side) can call do_request /
        # download_bytes / etc. through the same client instance. A test
        # here pins the inheritance contract so a future refactor that
        # accidentally hides parent methods on the subclass would be
        # caught without needing the full agent-dispatch integration test.
        client = MulesoftHttpProxyClient(credentials={"connect_args": {}})
        for method in (
            "do_request",
            "do_request_with_retry",
            "download_bytes",
            "download_to_storage",
            "extract_mulesoft_sources",
        ):
            self.assertTrue(
                hasattr(client, method),
                f"MulesoftHttpProxyClient must expose {method}() — inherited or own",
            )
