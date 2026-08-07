"""Streaming OTLP-JSON file writer (AO-914 contract C4).

Turns a stream of OTLP-JSON fragments — each a self-contained
``{"resourceSpans": [...]}`` document, one per source row — into uncompressed
OTLP-JSON files in object storage. Each output object is exactly one
``ExportTraceServiceRequest``-shaped document whose ``resourceSpans`` list is
the concatenation of the buffered fragments' lists.

This is a pure library module: it has no knowledge of jobs, output streams, or
dispatch modes. Callers (the data collector in-process; later a remote agent
operation) supply an already-authenticated storage client and a key template.

Contract notes (see the AO-914 master plan, contracts C1/C4):

- **Batching:** the file boundary is the pre-add check: when adding the next
  fragment would cross ``flush_bytes``, the buffer is flushed first and the
  new fragment starts the next file. A post-add check also flushes once the
  buffer reaches ``flush_bytes``; it fires only on exact-boundary equality or
  a single oversized fragment on an empty buffer, releasing the buffer one
  fragment earlier without ever changing file contents. Together these keep
  each output file close to ``max(flush_bytes, largest_single_fragment)`` — an
  approximate bound, not an exact one: the written payload is a re-serialized
  compact merge, and JSON number round-tripping (e.g. ``1e5`` becoming
  ``100000.0``) can inflate it by a few bytes per scientific-notation numeric
  field. A single fragment larger than the threshold ships alone, never
  truncated. The residual buffer is flushed when the iterator is exhausted; an
  empty buffer never produces a file.
- **Keys:** ``key_template`` is filled via ``key_template.format(seq=N)`` with
  ``seq`` 0-based and incremented once per file; the canonical template uses
  ``{seq:04d}`` (pinned in C1) but the module is format-agnostic. The template
  is validated before any fragment is consumed: one that fails to format or
  lacks an effective ``{seq}`` placeholder raises ``ValueError``. The template
  is relative to the storage client's namespace — ``BaseStorageClient``
  prepends its configured prefix — and ``Manifest.files`` reports keys as
  passed (pre-prefix); aligning client prefix + template with the ingest path
  is the caller's responsibility.
- **Failure semantics:** any fragment-level failure (transform error, JSON
  parse error, missing/invalid ``resourceSpans``) raises
  :class:`OtlpFragmentError` and aborts the run. A storage-write failure
  raises :class:`OtlpStorageError`, deliberately chained to the backend
  exception: storage errors carry operation/error-code details, never
  fragment content, and the chain preserves the backend's
  retryable-vs-permanent signal. Semantics are at-least-once: files flushed
  before the failure remain in storage (``keys_written`` on either error
  lists them, same as-passed namespace as ``Manifest.files``); a retry with
  the same deterministic key template overwrites them. Fragment errors never
  carry fragment content — fragments may hold prompt/completion text and
  exceptions flow to logs and error reporting. Transform exceptions are
  referenced by type only, and the underlying exception is dropped inside the
  ``except`` handler so :class:`OtlpFragmentError` is raised outside it: this
  leaves both ``__cause__`` and the implicit ``__context__`` as ``None``, so
  a transform's own exception message — which may embed fragment content — is
  unreachable through the exception chain. The JSON-parse path applies the
  same treatment for the same reason (``JSONDecodeError.doc`` holds the full
  raw fragment).
- **Memory:** buffered fragments are held as parsed object trees, whose
  resident size is a shape-dependent multiple of the raw ``flush_bytes``
  accounting — roughly 6-11x for attribute-dense OTLP-JSON (small dicts and
  keys dominate, and ``json.loads`` does not share key strings across
  fragments), down to ~1.2-3x when large prompt/completion strings dominate.
  At the default 16 MiB threshold, plan for on the order of 100-200 MB
  resident in the attribute-dense case, plus the merged serialized payload at
  flush time. Peak usage is unbounded by a single oversized fragment.
  Fragments are consumed lazily from the iterator; the full stream is never
  materialized.
- **PII posture:** fragment content is written as-is. If PII filtering is ever
  required on this path, the ``transform`` hook is the application point.
"""

import json
from dataclasses import dataclass, field
from typing import Callable, Iterable, List, Optional, Tuple

from apollo.integrations.storage.base_storage_client import BaseStorageClient

DEFAULT_FLUSH_BYTES = 16 * 1024 * 1024

_COLLECTED_TS_ATTRIBUTE = "montecarlo.collected_ts"
_RESOURCE_SPANS_KEY = "resourceSpans"


@dataclass(frozen=True)
class Manifest:
    """Result of a completed :func:`write_otlp_files` run.

    ``files`` holds the object keys as passed to the storage client (pre-prefix),
    in flush order. ``max_collected_ts`` is the lexicographic max of the
    ``montecarlo.collected_ts`` span attribute seen across all spans (fixed-width
    ISO-8601 UTC per contract C2, so string comparison is chronological), or
    ``None`` if no span carried the attribute.
    """

    files: List[str] = field(default_factory=list)
    span_count: int = 0
    fragment_count: int = 0
    max_collected_ts: Optional[str] = None
    bytes_written: int = 0


class OtlpFragmentError(Exception):
    """A fragment failed to transform, parse, or validate; the run is aborted.

    Carries the 0-based ``fragment_index`` of the failing fragment, a
    content-free ``detail`` (error type/position only — never fragment text),
    and ``keys_written``: the keys of files already flushed this run (as-passed
    namespace, same as ``Manifest.files``) so the caller can report or clean up
    orphans. Files already written stay in storage; a retry with the same
    deterministic key template overwrites them.
    """

    def __init__(self, fragment_index: int, detail: str, keys_written: List[str]):
        self.fragment_index = fragment_index
        self.detail = detail
        self.keys_written = keys_written
        super().__init__(f"OTLP fragment {fragment_index}: {detail}")


class OtlpStorageError(Exception):
    """A flush failed to write its file to storage; the run is aborted.

    Carries ``key`` — the key whose write failed (the object may be partially
    written) — and ``keys_written``: the keys of files successfully flushed
    before the failure (as-passed namespace, same as ``Manifest.files``).
    Deliberately chained to the backend exception (storage errors carry
    operation/error-code details, never fragment content), preserving the
    backend's retryable-vs-permanent signal.
    """

    def __init__(self, key: str, keys_written: List[str]):
        self.key = key
        self.keys_written = keys_written
        super().__init__(f"storage write failed for '{key}'")


def write_otlp_files(
    fragments: Iterable[str],
    storage: BaseStorageClient,
    key_template: str,
    flush_bytes: int = DEFAULT_FLUSH_BYTES,
    transform: Optional[Callable[[str], str]] = None,
) -> Manifest:
    """Merge OTLP-JSON fragments into files in object storage and return a manifest.

    :param fragments: stream of self-contained ``{"resourceSpans": [...]}`` JSON
        documents, consumed lazily.
    :param storage: destination client; only ``write(key, payload)`` is used.
    :param key_template: object-key template with a ``{seq}`` placeholder
        (canonically ``{seq:04d}``), relative to the client's namespace.
    :param flush_bytes: buffered-bytes threshold; a flush triggers before
        adding a fragment that would cross it (the file-boundary trigger). A
        post-add flush on exact-boundary equality or a single oversized
        fragment releases the buffer early without changing file contents;
        see module docstring for the file-size bound.
    :param transform: optional per-fragment rewrite applied to the raw string
        before parsing; ``None`` means identity.
    :return: a :class:`Manifest` of files written and stream statistics.
    :raises ValueError: if ``key_template`` fails to format or lacks an
        effective ``{seq}`` placeholder; validated before the iterator is
        touched.
    :raises OtlpFragmentError: on the first fragment that fails to transform,
        parse, or validate (fail-fast; see module docstring for semantics).
    :raises OtlpStorageError: when a flush's storage write fails; chained to
        the backend exception.
    """
    _validate_key_template(key_template)

    files: List[str] = []
    buffer: List[list] = []  # each entry is one fragment's resourceSpans list
    buffered_bytes = 0
    span_count = 0
    fragment_count = 0
    max_collected_ts: Optional[str] = None
    bytes_written = 0

    def flush() -> None:
        nonlocal buffer, buffered_bytes, bytes_written
        if not buffer:
            return
        merged = {
            _RESOURCE_SPANS_KEY: [
                resource_span for fragment in buffer for resource_span in fragment
            ]
        }
        payload = json.dumps(merged, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        key = key_template.format(seq=len(files))
        try:
            storage.write(key, payload)
        except Exception as exc:
            # Chained deliberately: storage exceptions carry operation and
            # error-code details, never fragment content, and the chain keeps
            # the backend's retryable-vs-permanent signal.
            raise OtlpStorageError(key, list(files)) from exc
        files.append(key)
        bytes_written += len(payload)
        buffer = []
        buffered_bytes = 0

    for index, raw_fragment in enumerate(fragments):
        if transform is not None:
            transform_error: Optional[str] = None
            try:
                raw_fragment = transform(raw_fragment)
            except Exception as exc:
                # Capture a content-free detail only. A transform's own
                # exception message may embed fragment content (e.g.
                # prompt/completion text), so the exception object itself must
                # not survive this handler.
                transform_error = f"transform raised {type(exc).__name__}"
            if transform_error is not None:
                # Raised outside the ``except`` block so Python leaves
                # ``__context__`` as ``None``. ``from None`` alone would clear
                # ``__cause__`` and set ``__suppress_context__``, but the
                # implicit ``__context__`` chain would still reference the
                # transform's exception — and thus its fragment-bearing message.
                raise OtlpFragmentError(index, transform_error, list(files))

        if not isinstance(raw_fragment, str):
            raise OtlpFragmentError(
                index,
                f"fragment is {type(raw_fragment).__name__}, expected str",
                list(files),
            )

        parse_error: Optional[str] = None
        try:
            document = json.loads(raw_fragment)
        except json.JSONDecodeError as exc:
            # Capture position/message only. JSONDecodeError retains the full
            # document on its ``.doc`` attribute (may hold prompt content), so
            # the exception object itself must not survive this handler.
            parse_error = (
                f"invalid JSON: {exc.msg} at line {exc.lineno} column {exc.colno}"
            )
        if parse_error is not None:
            # Raised outside the ``except`` block so ``__context__`` stays
            # ``None`` and the fragment-bearing ``JSONDecodeError`` is not
            # reachable through the implicit exception chain.
            raise OtlpFragmentError(index, parse_error, list(files))

        resource_spans = (
            document.get(_RESOURCE_SPANS_KEY) if isinstance(document, dict) else None
        )
        if not isinstance(resource_spans, list):
            raise OtlpFragmentError(
                index,
                f"document has no '{_RESOURCE_SPANS_KEY}' list",
                list(files),
            )

        fragment_spans, fragment_max_ts = _collect_span_stats(resource_spans)
        span_count += fragment_spans
        if fragment_max_ts is not None and (
            max_collected_ts is None or fragment_max_ts > max_collected_ts
        ):
            max_collected_ts = fragment_max_ts
        fragment_count += 1

        fragment_bytes = len(raw_fragment.encode("utf-8"))
        if buffer and buffered_bytes + fragment_bytes > flush_bytes:
            flush()
        buffer.append(resource_spans)
        buffered_bytes += fragment_bytes
        if buffered_bytes >= flush_bytes:
            flush()

    flush()

    return Manifest(
        files=files,
        span_count=span_count,
        fragment_count=fragment_count,
        max_collected_ts=max_collected_ts,
        bytes_written=bytes_written,
    )


def _validate_key_template(key_template: str) -> None:
    """Reject templates that cannot produce one distinct key per file.

    Probe-formats with two ``seq`` values: a template that fails to format is
    malformed, and one that formats both probes to the same key has no
    effective ``{seq}`` placeholder — every flush would overwrite the same
    object. The template is caller-supplied configuration and never carries
    fragment content, so it may appear in the error message.
    """
    try:
        first = key_template.format(seq=0)
        second = key_template.format(seq=1)
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(
            f"key_template {key_template!r} is not formattable: {exc}"
        ) from exc
    if first == second:
        raise ValueError(
            f"key_template {key_template!r} has no effective {{seq}} placeholder; "
            "every flush would overwrite the same key"
        )


def _collect_span_stats(resource_spans: list) -> Tuple[int, Optional[str]]:
    """Count spans and find the max ``montecarlo.collected_ts`` in one fragment.

    Walks the OTLP-JSON shape ``resourceSpans[].scopeSpans[].spans[]`` with span
    ``attributes`` as a list of ``{"key": ..., "value": {"stringValue": ...}}``
    entries. Non-conforming levels are skipped rather than failing the run —
    envelope validation is the caller's (``resourceSpans`` list) concern; stats
    are best-effort over well-formed spans.

    :return: ``(span_count, max_collected_ts_or_none)``
    """
    span_count = 0
    max_ts: Optional[str] = None
    for resource_span in resource_spans:
        if not isinstance(resource_span, dict):
            continue
        for scope_span in _list_of_dicts(resource_span.get("scopeSpans")):
            for span in _list_of_dicts(scope_span.get("spans")):
                span_count += 1
                for attribute in _list_of_dicts(span.get("attributes")):
                    if attribute.get("key") != _COLLECTED_TS_ATTRIBUTE:
                        continue
                    value = attribute.get("value")
                    if not isinstance(value, dict):
                        continue
                    ts = value.get("stringValue")
                    if isinstance(ts, str) and (max_ts is None or ts > max_ts):
                        max_ts = ts
    return span_count, max_ts


def _list_of_dicts(value: object) -> List[dict]:
    if not isinstance(value, list):
        return []
    return [entry for entry in value if isinstance(entry, dict)]
