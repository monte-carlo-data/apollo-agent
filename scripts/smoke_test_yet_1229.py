"""YET-1229 local smoke test — exercises ``MulesoftHttpProxyClient.extract_mulesoft_sources``
against a real Mule application JAR on disk.

This is the agent-side smoke test (sibling to data-collector's
``scripts/mulesoft_poc/smoke_test_yet_1215.py`` which simulated this op's
behaviour Python-side). Here we run the **real production agent code**:

1. Read a Mule application JAR from disk (default:
   ``worker-sf-integ-1.3.0.jar`` in the data-collector repo root, which is
   the same fixture the DC smoke test uses).
2. Mock ``requests.get`` to return a streaming response that yields the JAR
   bytes — so the URL string can be fictional, no network or auth required.
3. Patch the SSRF guard so the fictional URL passes the HTTPS-only / non-
   public-IP checks. (The SSRF surface is exhaustively tested in
   ``tests/test_http_client.py::TestDownloadBytesUrlSafety``; here we just
   want to drive the extraction path without setting up a real local
   HTTPS server.)
4. Patch ``get_storage_client`` to assert the agent never touches the
   storage backend during this operation — YET-1229's central invariant.
5. Construct ``MulesoftHttpProxyClient``, call ``extract_mulesoft_sources``,
   decode the returned base64 zip, and print a summary of the result:
   status, sizes, wire-size ratio vs the JAR, listing of entries.

The smoke test is intentionally self-contained: no Anypoint credentials,
no MuleSoft sandbox, no running Flask app. Just point at a JAR on disk
and run.

Usage
-----

    cd ~/Montecarlo/apollo-agent
    source .venv/bin/activate

    # Default: ~/Montecarlo/data-collector/worker-sf-integ-1.3.0.jar
    python scripts/smoke_test_yet_1229.py

    # Or point at a different JAR:
    python scripts/smoke_test_yet_1229.py /path/to/other.jar
"""

from __future__ import annotations

import base64
import io
import json
import sys
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Run from the apollo-agent repo root (the parent of scripts/) so
# ``apollo.integrations.http.mulesoft_proxy_client`` is importable. The
# repo's pyproject.toml sets ``pythonpath = "."`` for pytest, but a
# standalone script run from anywhere needs the path nudge manually.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT))


def _heading(text: str) -> None:
    print(f"\n{'=' * 72}\n {text}\n{'=' * 72}")


def _bullet(label: str, value) -> None:
    print(f"  • {label}: {value}")


def _chunked(payload: bytes, size: int = 8192):
    """Mirror what ``requests.iter_content(chunk_size=8192)`` yields."""
    for i in range(0, len(payload), size):
        yield payload[i : i + size]


def _make_streaming_response(jar_bytes: bytes) -> MagicMock:
    """Build a ``MagicMock`` that imitates the ``requests.Response`` shape
    the production ``_open_download_response`` context manager expects:
    a 200 status with a chunked-iterable body, a no-op
    ``raise_for_status``, and ``close()`` available."""
    resp = MagicMock()
    resp.status_code = 200
    resp.iter_content.return_value = _chunked(jar_bytes)
    resp.raise_for_status.return_value = None
    resp.headers = {}
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = None
    return resp


def main() -> int:
    if len(sys.argv) > 1:
        jar_path = Path(sys.argv[1])
    else:
        jar_path = Path.home() / "Montecarlo/data-collector/worker-sf-integ-1.3.0.jar"

    if not jar_path.is_file():
        print(f"❌ JAR not found: {jar_path}", file=sys.stderr)
        print(
            f"   Pass an explicit path: python {Path(__file__).name} /path/to/app.jar",
            file=sys.stderr,
        )
        return 2

    _heading("YET-1229 local smoke test")
    print(f"JAR: {jar_path}")
    jar_bytes = jar_path.read_bytes()
    _bullet("size", f"{len(jar_bytes) / (1024 * 1024):.1f} MiB")

    # Late imports so the sys.path nudge above takes effect first.
    from apollo.integrations.http.mulesoft_proxy_client import MulesoftHttpProxyClient

    _heading("Step 1 — Inspect the JAR's manifest")
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as jar:
        manifest = json.loads(jar.read("META-INF/mule-artifact/mule-artifact.json"))
        configs = manifest.get("configs") or []
        properties_entries = [
            n
            for n in jar.namelist()
            if n.startswith("properties/") and not n.endswith("/")
        ]
    _bullet("configs[] in manifest", configs)
    _bullet("properties/ entries", properties_entries)

    _heading("Step 2 — Invoke MulesoftHttpProxyClient.extract_mulesoft_sources")
    client = MulesoftHttpProxyClient(credentials={"connect_args": {}})

    # Patch surfaces:
    # - requests.get → returns a streaming response with the JAR bytes
    # - _assert_safe_download_url → no-op (lets the fictional URL through; SSRF
    #   surface tested elsewhere)
    # - get_storage_client → MagicMock that we assert is never called (YET-1229's
    #   "no agent-storage write" invariant)
    fictional_url = "https://exchange.mulesoft.example/worker-sf-integ-1.3.0.jar"
    with patch(
        "requests.get", return_value=_make_streaming_response(jar_bytes)
    ), patch.object(
        MulesoftHttpProxyClient, "_assert_safe_download_url", return_value=None
    ), patch(
        "apollo.integrations.http.http_proxy_client.get_storage_client"
    ) as mock_storage_factory:
        result = client.extract_mulesoft_sources(fictional_url)

    if mock_storage_factory.called:
        print(
            "❌ FAIL: extract_mulesoft_sources called get_storage_client — "
            "YET-1229's no-agent-storage-write invariant violated.",
            file=sys.stderr,
        )
        return 1

    _heading("Step 3 — Inspect the agent response")
    _bullet("sources_extraction_status", result["sources_extraction_status"])
    _bullet("sources_size_bytes", f"{result['sources_size_bytes']:,}")
    b64_len = len(result["sources_zip_b64"]) if result["sources_zip_b64"] else 0
    _bullet("base64 wire size", f"{b64_len:,} bytes")
    _bullet(
        "compression ratio vs JAR",
        f"{result['sources_size_bytes'] / len(jar_bytes):.4%}",
    )

    if result["sources_extraction_status"] != "ok":
        print(
            f"\n❌ Expected status='ok'; got '{result['sources_extraction_status']}'",
            file=sys.stderr,
        )
        return 1

    _heading("Step 4 — Verify the extracted zip's contents")
    extracted_zip = base64.b64decode(result["sources_zip_b64"])
    with zipfile.ZipFile(io.BytesIO(extracted_zip)) as out:
        out_names = sorted(out.namelist())
    _bullet("entries in extracted zip", len(out_names))
    for name in out_names:
        kind = "XML config" if name.endswith(".xml") else "property"
        print(f"    [{kind:11s}] {name}")

    expected_xmls = set(configs)
    expected_properties = set(properties_entries)
    expected_all = expected_xmls | expected_properties
    actual = set(out_names)

    missing = expected_all - actual
    unexpected = actual - expected_all

    if missing:
        print(f"\n❌ Missing expected entries: {sorted(missing)}", file=sys.stderr)
        return 1
    if unexpected:
        print(f"\n❌ Unexpected extra entries: {sorted(unexpected)}", file=sys.stderr)
        return 1

    print(
        f"\n✅ Smoke test passed — extracted {len(expected_xmls)} XML configs + "
        f"{len(expected_properties)} properties files from a {len(jar_bytes) / (1024 * 1024):.1f} MiB JAR "
        f"into a {result['sources_size_bytes'] / 1024:.1f} KiB sources zip "
        f"({result['sources_size_bytes'] / len(jar_bytes):.4%} of the JAR)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
