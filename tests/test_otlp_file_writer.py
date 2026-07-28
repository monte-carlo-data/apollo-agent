"""Tests for the streaming OTLP-JSON file writer (AO-929, AO-914 contract C4).

Fixtures use the real OTLP-JSON schema: spans live at
``resourceSpans[].scopeSpans[].spans[]`` and span attributes are a list of
``{"key": ..., "value": {"stringValue": ...}}`` entries. The storage client is
mocked; no cloud SDKs are involved.
"""

import json
import traceback
from unittest import TestCase
from unittest.mock import Mock

from apollo.integrations.otlp.otlp_file_writer import (
    DEFAULT_FLUSH_BYTES,
    Manifest,
    OtlpFragmentError,
    write_otlp_files,
)
from apollo.integrations.storage.base_storage_client import BaseStorageClient

_KEY_TEMPLATE = (
    "exports/acc-1/year=2026/month=07/day=27/traces-agent-job-{seq:04d}.json"
)


def _make_fragment(
    span_names,
    collected_ts=None,
    padding=0,
    padding_char="x",
):
    """Build one real-shaped OTLP-JSON fragment.

    :param span_names: list of span names; one span per name, all in one scope.
    :param collected_ts: value for the ``montecarlo.collected_ts`` attribute on
        every span, omitted entirely when ``None``.
    :param padding: adds an attribute whose value is ``padding`` repetitions of
        ``padding_char``, to control the fragment's serialized size.
    """
    spans = []
    for name in span_names:
        attributes = [
            {"key": "montecarlo.agent_name", "value": {"stringValue": "test-agent"}},
        ]
        if collected_ts is not None:
            attributes.append(
                {
                    "key": "montecarlo.collected_ts",
                    "value": {"stringValue": collected_ts},
                }
            )
        if padding:
            attributes.append(
                {
                    "key": "gen_ai.prompt.0.content",
                    "value": {"stringValue": padding_char * padding},
                }
            )
        spans.append(
            {
                "traceId": "0123456789abcdef0123456789abcdef",
                "spanId": "0123456789abcdef",
                "name": name,
                "attributes": attributes,
            }
        )
    return json.dumps(
        {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": [
                            {
                                "key": "service.name",
                                "value": {"stringValue": "test-service"},
                            }
                        ]
                    },
                    "scopeSpans": [{"scope": {"name": "test-scope"}, "spans": spans}],
                }
            ]
        },
        # Raw UTF-8 (no \uXXXX escaping) so multi-byte fixtures genuinely
        # diverge in char vs byte length, as real warehouse output does.
        ensure_ascii=False,
    )


class TestWriteOtlpFiles(TestCase):
    def setUp(self):
        self.storage = Mock(spec=BaseStorageClient)

    def _written_payloads(self):
        return [call.args[1] for call in self.storage.write.call_args_list]

    def _written_keys(self):
        return [call.args[0] for call in self.storage.write.call_args_list]

    def test_happy_path_merges_fragments_into_one_export_request(self):
        fragments = [
            _make_fragment(["span-a"], collected_ts="2026-07-27T00:00:01Z"),
            _make_fragment(["span-b", "span-c"], collected_ts="2026-07-27T00:00:02Z"),
            _make_fragment(["span-d"]),
        ]

        manifest = write_otlp_files(iter(fragments), self.storage, _KEY_TEMPLATE)

        self.assertEqual(1, self.storage.write.call_count)
        document = json.loads(self._written_payloads()[0])
        # Exactly one ExportTraceServiceRequest-shaped object.
        self.assertEqual(["resourceSpans"], list(document.keys()))
        # resourceSpans lists merged in input order.
        self.assertEqual(3, len(document["resourceSpans"]))
        names = [
            span["name"]
            for resource_span in document["resourceSpans"]
            for scope_span in resource_span["scopeSpans"]
            for span in scope_span["spans"]
        ]
        self.assertEqual(["span-a", "span-b", "span-c", "span-d"], names)
        self.assertEqual(
            Manifest(
                files=self._written_keys(),
                span_count=4,
                fragment_count=3,
                max_collected_ts="2026-07-27T00:00:02Z",
                bytes_written=len(self._written_payloads()[0].encode("utf-8")),
            ),
            manifest,
        )

    def test_pre_add_flush_bounds_file_size(self):
        fragments = [
            _make_fragment(["s1"], padding=600),
            _make_fragment(["s2"], padding=600),
            _make_fragment(["s3"], padding=600),
        ]
        fragment_sizes = [len(f.encode("utf-8")) for f in fragments]
        # Threshold fits two fragments but not three: adding the third
        # triggers a pre-add flush of the first two.
        flush_bytes = sum(fragment_sizes[:2]) + 1

        manifest = write_otlp_files(
            iter(fragments), self.storage, _KEY_TEMPLATE, flush_bytes=flush_bytes
        )

        self.assertEqual(2, self.storage.write.call_count)
        first, second = (json.loads(p) for p in self._written_payloads())
        self.assertEqual(2, len(first["resourceSpans"]))
        self.assertEqual(1, len(second["resourceSpans"]))
        # No written payload exceeds max(flush_bytes, largest single fragment).
        bound = max(flush_bytes, max(fragment_sizes))
        for payload in self._written_payloads():
            self.assertLessEqual(len(payload.encode("utf-8")), bound)
        self.assertEqual(3, manifest.fragment_count)

    def test_byte_length_not_char_length_drives_flush(self):
        # Multi-byte padding: char length stays under the threshold while the
        # UTF-8 byte length crosses it ('é' is 2 bytes).
        ascii_fragment = _make_fragment(["s1"], padding=200)
        non_ascii_fragment = _make_fragment(["s2"], padding=200, padding_char="é")
        self.assertEqual(len(ascii_fragment), len(non_ascii_fragment))
        char_total = len(ascii_fragment) + len(non_ascii_fragment)
        byte_total = sum(
            len(f.encode("utf-8")) for f in (ascii_fragment, non_ascii_fragment)
        )
        flush_bytes = char_total + 100  # over the char sum, under the byte sum
        self.assertGreater(byte_total, flush_bytes)

        write_otlp_files(
            iter([ascii_fragment, non_ascii_fragment]),
            self.storage,
            _KEY_TEMPLATE,
            flush_bytes=flush_bytes,
        )

        # Counting chars instead of bytes would merge both into one file.
        self.assertEqual(2, self.storage.write.call_count)

    def test_oversized_fragment_ships_alone_untruncated(self):
        small = _make_fragment(["small"], padding=10)
        oversized = _make_fragment(["huge"], padding=5_000)
        flush_bytes = 1_000
        self.assertGreater(len(oversized.encode("utf-8")), flush_bytes)

        manifest = write_otlp_files(
            iter([small, oversized]),
            self.storage,
            _KEY_TEMPLATE,
            flush_bytes=flush_bytes,
        )

        self.assertEqual(2, self.storage.write.call_count)
        first, second = (json.loads(p) for p in self._written_payloads())
        self.assertEqual(
            "small", first["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"]
        )
        huge_span = second["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        self.assertEqual("huge", huge_span["name"])
        # Untruncated: the full padded attribute survives the round-trip.
        self.assertEqual(
            "x" * 5_000, huge_span["attributes"][-1]["value"]["stringValue"]
        )
        self.assertEqual(2, len(manifest.files))

    def test_final_flush_writes_residual_buffer(self):
        fragments = [_make_fragment(["s1"]), _make_fragment(["s2"])]

        manifest = write_otlp_files(
            iter(fragments),
            self.storage,
            _KEY_TEMPLATE,
            flush_bytes=DEFAULT_FLUSH_BYTES,
        )

        self.assertEqual(1, self.storage.write.call_count)
        self.assertEqual(1, len(manifest.files))

    def test_no_empty_trailing_file_after_exact_boundary_flush(self):
        fragment = _make_fragment(["s1"], padding=100)
        # Threshold exactly equal to the fragment size: the post-add check
        # flushes immediately, leaving an empty buffer at end of stream.
        flush_bytes = len(fragment.encode("utf-8"))

        manifest = write_otlp_files(
            iter([fragment]), self.storage, _KEY_TEMPLATE, flush_bytes=flush_bytes
        )

        self.assertEqual(1, self.storage.write.call_count)
        self.assertEqual(1, len(manifest.files))

    def test_empty_iterator_writes_nothing(self):
        manifest = write_otlp_files(iter([]), self.storage, _KEY_TEMPLATE)

        self.storage.write.assert_not_called()
        self.assertEqual(Manifest(), manifest)

    def test_malformed_fragment_fails_fast(self):
        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(iter(["{not-json"]), self.storage, _KEY_TEMPLATE)

        self.storage.write.assert_not_called()
        self.assertEqual(0, ctx.exception.fragment_index)
        self.assertEqual([], ctx.exception.keys_written)

    def test_failure_after_flush_reports_keys_written(self):
        good = _make_fragment(["s1"], padding=100)
        flush_bytes = len(good.encode("utf-8"))  # good fragment flushes alone

        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(
                iter([good, "{not-json"]),
                self.storage,
                _KEY_TEMPLATE,
                flush_bytes=flush_bytes,
            )

        self.assertEqual(1, self.storage.write.call_count)
        # keys_written holds the as-passed keys of files flushed pre-failure.
        self.assertEqual(self._written_keys(), ctx.exception.keys_written)
        self.assertEqual(1, ctx.exception.fragment_index)

    def test_missing_resource_spans_fails_fast(self):
        valid_json_wrong_envelope = json.dumps({"spans": []})

        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(
                iter([valid_json_wrong_envelope]), self.storage, _KEY_TEMPLATE
            )

        self.storage.write.assert_not_called()
        self.assertEqual(0, ctx.exception.fragment_index)
        self.assertEqual([], ctx.exception.keys_written)
        self.assertIn("resourceSpans", str(ctx.exception))

    def test_parse_error_carries_no_fragment_content(self):
        secret = "SUPER-SECRET-PROMPT-CONTENT"
        broken = '{"resourceSpans": [' + secret

        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(iter([broken]), self.storage, _KEY_TEMPLATE)

        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(vars(ctx.exception)))
        # Not chained: JSONDecodeError retains the document on .doc, so neither
        # __cause__ nor __context__ may reference it — a reporter walking the
        # implicit context chain must not reach the raw fragment.
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        full_chain = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__
            )
        )
        self.assertNotIn(secret, full_chain)

    def test_transform_error_is_wrapped_without_its_message(self):
        secret = "SECRET-FRAGMENT-TEXT"

        def failing_transform(fragment: str) -> str:
            raise ValueError(f"could not rewrite: {secret}")

        good = _make_fragment(["s1"])
        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(
                iter([good]), self.storage, _KEY_TEMPLATE, transform=failing_transform
            )

        self.assertEqual(0, ctx.exception.fragment_index)
        # The wrapper names the exception type but never its str()/args.
        self.assertIn("ValueError", str(ctx.exception))
        self.assertNotIn(secret, str(ctx.exception))
        self.assertNotIn(secret, repr(vars(ctx.exception)))
        # Deliberately not chained: the transform's own exception may embed
        # fragment content, so it must reach neither __cause__ nor __context__,
        # and must not surface anywhere in the formatted traceback chain.
        self.assertIsNone(ctx.exception.__cause__)
        self.assertIsNone(ctx.exception.__context__)
        full_chain = "".join(
            traceback.format_exception(
                type(ctx.exception), ctx.exception, ctx.exception.__traceback__
            )
        )
        self.assertNotIn(secret, full_chain)

    def test_transform_returning_non_string_fails_fast(self):
        def returns_none(fragment: str) -> str:
            return None

        good = _make_fragment(["s1"])
        with self.assertRaises(OtlpFragmentError) as ctx:
            write_otlp_files(
                iter([good]), self.storage, _KEY_TEMPLATE, transform=returns_none
            )

        self.assertEqual(0, ctx.exception.fragment_index)
        self.storage.write.assert_not_called()
        self.assertEqual([], ctx.exception.keys_written)
        # Content-free: names the wrong type, never fragment content.
        self.assertIn("NoneType", ctx.exception.detail)

    def test_transform_is_applied_per_fragment(self):
        def rename_spans(fragment: str) -> str:
            return fragment.replace("span-original", "span-rewritten")

        fragments = [_make_fragment(["span-original"])]
        write_otlp_files(
            iter(fragments), self.storage, _KEY_TEMPLATE, transform=rename_spans
        )

        document = json.loads(self._written_payloads()[0])
        self.assertEqual(
            "span-rewritten",
            document["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["name"],
        )

    def test_identity_when_transform_is_none(self):
        fragment = _make_fragment(["s1"], collected_ts="2026-07-27T00:00:03Z")

        write_otlp_files(iter([fragment]), self.storage, _KEY_TEMPLATE, transform=None)

        document = json.loads(self._written_payloads()[0])
        self.assertEqual(
            json.loads(fragment)["resourceSpans"], document["resourceSpans"]
        )

    def test_manifest_fields_match_actual_writes(self):
        fragments = [
            _make_fragment(
                ["s1", "s2"], collected_ts="2026-07-27T00:00:05Z", padding=600
            ),
            # Non-ASCII content: bytes_written must count UTF-8 bytes.
            _make_fragment(["s3"], padding=100, padding_char="日"),
            _make_fragment(["s4"]),  # no collected_ts attribute at all
        ]
        flush_bytes = len(fragments[0].encode("utf-8"))  # first flushes alone

        manifest = write_otlp_files(
            iter(fragments), self.storage, _KEY_TEMPLATE, flush_bytes=flush_bytes
        )

        payloads = self._written_payloads()
        self.assertEqual(2, len(payloads))
        # Computed from what was actually passed to the mock, not mirrored
        # from the implementation's arithmetic.
        self.assertEqual(
            sum(len(p.encode("utf-8")) for p in payloads), manifest.bytes_written
        )
        self.assertGreater(
            manifest.bytes_written, sum(len(p) for p in payloads)
        )  # non-ASCII made bytes > chars
        self.assertEqual(4, manifest.span_count)
        self.assertEqual(3, manifest.fragment_count)
        self.assertEqual("2026-07-27T00:00:05Z", manifest.max_collected_ts)
        # files == the exact keys passed to storage.write, in flush order,
        # final flush included.
        self.assertEqual(self._written_keys(), manifest.files)

    def test_max_collected_ts_is_true_max_across_fragments(self):
        fragments = [
            _make_fragment(["s1"], collected_ts="2026-07-27T00:00:09Z"),
            _make_fragment(["s2"], collected_ts="2026-07-27T00:00:01Z"),
            _make_fragment(["s3"]),  # no collected_ts attribute at all
        ]

        manifest = write_otlp_files(iter(fragments), self.storage, _KEY_TEMPLATE)

        # A "last non-null wins" regression would report 00:00:01Z (fragment 2)
        # since it's the last fragment carrying an actual value.
        self.assertEqual("2026-07-27T00:00:09Z", manifest.max_collected_ts)

    def test_max_collected_ts_across_spans_within_a_fragment(self):
        # Built directly (bypassing _make_fragment, which pins one ts for all
        # spans) so two spans in the same fragment carry different timestamps.
        fragment = json.dumps(
            {
                "resourceSpans": [
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "name": "span-low",
                                        "attributes": [
                                            {
                                                "key": "montecarlo.collected_ts",
                                                "value": {
                                                    "stringValue": "2026-07-27T00:00:01Z"
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "name": "span-high",
                                        "attributes": [
                                            {
                                                "key": "montecarlo.collected_ts",
                                                "value": {
                                                    "stringValue": "2026-07-27T00:00:09Z"
                                                },
                                            }
                                        ],
                                    },
                                ]
                            }
                        ]
                    }
                ]
            }
        )

        manifest = write_otlp_files(iter([fragment]), self.storage, _KEY_TEMPLATE)

        self.assertEqual(2, manifest.span_count)
        self.assertEqual("2026-07-27T00:00:09Z", manifest.max_collected_ts)

    def test_malformed_nested_levels_are_skipped_not_fatal(self):
        # Top-level resourceSpans is a well-formed list (so the envelope check
        # in write_otlp_files passes); nested levels below it are malformed in
        # several distinct ways that _collect_span_stats/_list_of_dicts must
        # tolerate rather than crash on.
        fragment = json.dumps(
            {
                "resourceSpans": [
                    # scopeSpans is not a list at all: skipped, 0 spans.
                    {"scopeSpans": "not-a-list"},
                    # scopeSpan present but its spans is null: skipped, 0 spans.
                    {"scopeSpans": [{"spans": None}]},
                    # Span itself is well-formed (counted), but its attributes
                    # is a dict instead of a list: attributes are unreachable,
                    # not fatal.
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "name": "malformed-attributes",
                                        "attributes": {"key": "not-a-list-entry"},
                                    }
                                ]
                            }
                        ]
                    },
                    # Span is well-formed (counted), but its collected_ts
                    # attribute's value is a bare string instead of the
                    # expected {"stringValue": ...} dict: that attribute is
                    # skipped, so this high timestamp must NOT win.
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "name": "malformed-value",
                                        "attributes": [
                                            {
                                                "key": "montecarlo.collected_ts",
                                                "value": "2026-07-27T00:00:09Z",
                                            }
                                        ],
                                    }
                                ]
                            }
                        ]
                    },
                    # One fully well-formed span, to prove counting still works.
                    {
                        "scopeSpans": [
                            {
                                "spans": [
                                    {
                                        "name": "well-formed",
                                        "attributes": [
                                            {
                                                "key": "montecarlo.collected_ts",
                                                "value": {
                                                    "stringValue": "2026-07-27T00:00:05Z"
                                                },
                                            }
                                        ],
                                    }
                                ]
                            }
                        ]
                    },
                ]
            }
        )

        manifest = write_otlp_files(iter([fragment]), self.storage, _KEY_TEMPLATE)

        self.storage.write.assert_called_once()
        self.assertEqual(1, len(manifest.files))
        # Only the three dict-shaped spans count; the two resourceSpans
        # entries with malformed nesting above the span level (non-list
        # scopeSpans, null spans) contribute nothing rather than raising.
        self.assertEqual(3, manifest.span_count)
        # The malformed-value attribute's timestamp is ignored; only the
        # well-formed span's timestamp is reflected.
        self.assertEqual("2026-07-27T00:00:05Z", manifest.max_collected_ts)

    def test_fragments_are_consumed_lazily(self):
        fragment = _make_fragment(["s1"], padding=100)
        flush_bytes = len(fragment.encode("utf-8"))
        pulled = 0
        pulled_at_first_write = None

        def fragment_generator():
            nonlocal pulled
            for _ in range(10):
                pulled += 1
                yield fragment

        def record_pull_count(key, payload):
            nonlocal pulled_at_first_write
            if pulled_at_first_write is None:
                pulled_at_first_write = pulled

        self.storage.write.side_effect = record_pull_count

        write_otlp_files(
            fragment_generator(), self.storage, _KEY_TEMPLATE, flush_bytes=flush_bytes
        )

        # Flush-as-you-go: the first write happened while most of the stream
        # was still unconsumed (a list(fragments) would make this 10).
        self.assertEqual(1, pulled_at_first_write)

    def test_seq_uses_canonical_zero_padded_template(self):
        fragment = _make_fragment(["s1"], padding=100)
        flush_bytes = len(fragment.encode("utf-8"))  # one file per fragment

        manifest = write_otlp_files(
            iter([fragment] * 3), self.storage, _KEY_TEMPLATE, flush_bytes=flush_bytes
        )

        expected = [
            "exports/acc-1/year=2026/month=07/day=27/traces-agent-job-0000.json",
            "exports/acc-1/year=2026/month=07/day=27/traces-agent-job-0001.json",
            "exports/acc-1/year=2026/month=07/day=27/traces-agent-job-0002.json",
        ]
        self.assertEqual(expected, manifest.files)
        self.assertEqual(expected, self._written_keys())

    def test_seq_is_template_format_agnostic(self):
        fragment = _make_fragment(["s1"], padding=100)
        flush_bytes = len(fragment.encode("utf-8"))

        manifest = write_otlp_files(
            iter([fragment] * 2),
            self.storage,
            "traces-{seq}.json",
            flush_bytes=flush_bytes,
        )

        self.assertEqual(["traces-0.json", "traces-1.json"], manifest.files)
