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

- **Batching:** a flush can trigger two ways. Pre-add: adding the next
  fragment would cross ``flush_bytes``, so the buffer is flushed first and the
  new fragment starts the next file. Post-add: the fragment just appended
  brings the buffer to ``flush_bytes`` or beyond, so the file is flushed
  immediately rather than waiting for another fragment. Together these keep
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
  is relative to the storage client's namespace — ``BaseStorageClient``
  prepends its configured prefix — and ``Manifest.files`` reports keys as
  passed (pre-prefix); aligning client prefix + template with the ingest path
  is the caller's responsibility.
- **Failure semantics:** any fragment-level failure (transform error, JSON
  parse error, missing/invalid ``resourceSpans``) raises
  :class:`OtlpFragmentError` and aborts the run. Semantics are at-least-once:
  files flushed before the failure remain in storage (``keys_written`` on the
  error lists them, same as-passed namespace as ``Manifest.files``); a retry
  with the same deterministic key template overwrites them. Errors never carry
  fragment content — fragments may hold prompt/completion text and exceptions
  flow to logs and error reporting. Transform exceptions are referenced by
  type only and are not chained (``raise ... from None``), so a transform's
  own exception message — which may embed fragment content — never reaches
  ``__cause__``; the JSON-parse path applies the same treatment for the same
  reason.
- **Memory:** peak usage is a small multiple of ``flush_bytes`` (parsed object
  trees plus the merged output string at flush time) and is unbounded by a
  single oversized fragment. Fragments are consumed lazily from the iterator;
  the full stream is never materialized.
- **PII posture:** fragment content is written as-is. If PII filtering is ever
  required on this path, the ``transform`` hook is the application point.
"""

import json
from dataclasses import dataclass, field
from typing import Callable, Iterator, List, Optional, Tuple

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


def write_otlp_files(
    fragments: Iterator[str],
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
    :param flush_bytes: buffered-bytes threshold that triggers a flush, both
        pre-add (adding the next fragment would cross it) and post-add (the
        fragment just appended reaches or exceeds it); see module docstring
        for the file-size bound.
    :param transform: optional per-fragment rewrite applied to the raw string
        before parsing; ``None`` means identity.
    :return: a :class:`Manifest` of files written and stream statistics.
    :raises OtlpFragmentError: on the first fragment that fails to transform,
        parse, or validate (fail-fast; see module docstring for semantics).
    """
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
        payload = json.dumps(merged, separators=(",", ":"), ensure_ascii=False)
        key = key_template.format(seq=len(files))
        storage.write(key, payload)
        files.append(key)
        bytes_written += len(payload.encode("utf-8"))
        buffer = []
        buffered_bytes = 0

    for index, raw_fragment in enumerate(fragments):
        if transform is not None:
            try:
                raw_fragment = transform(raw_fragment)
            except Exception as exc:
                # Deliberately not chained: a transform's own exception
                # message may embed fragment content (e.g. prompt/completion
                # text). The exception type is already captured in the detail
                # string, so no diagnostic value is lost by breaking the
                # chain.
                raise OtlpFragmentError(
                    index,
                    f"transform raised {type(exc).__name__}",
                    list(files),
                ) from None

        if not isinstance(raw_fragment, str):
            raise OtlpFragmentError(
                index,
                f"fragment is {type(raw_fragment).__name__}, expected str",
                list(files),
            )

        try:
            document = json.loads(raw_fragment)
        except json.JSONDecodeError as exc:
            # Deliberately not chained: JSONDecodeError retains the full
            # document on its .doc attribute, which may hold prompt content.
            raise OtlpFragmentError(
                index,
                f"invalid JSON: {exc.msg} at line {exc.lineno} column {exc.colno}",
                list(files),
            ) from None

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
