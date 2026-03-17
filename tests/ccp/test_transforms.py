# tests/ccp/test_transforms.py
from unittest import TestCase

from apollo.integrations.ccp.errors import CcpPipelineError
from apollo.integrations.ccp.transforms.registry import TransformRegistry


class TestTransformRegistry(TestCase):
    def test_unknown_type_raises(self):
        with self.assertRaises(CcpPipelineError) as ctx:
            TransformRegistry.get("not_a_real_type")
        self.assertIn("not_a_real_type", str(ctx.exception))

    def test_tmp_file_write_registered(self):
        transform = TransformRegistry.get("tmp_file_write")
        self.assertIsNotNone(transform)


import os
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.transforms.tmp_file_write import TmpFileWriteTransform


class TestTmpFileWriteTransform(TestCase):
    def _make_step(self, contents_template, when=None, file_suffix=".pem"):
        return TransformStep(
            type="tmp_file_write",
            input={"contents": contents_template, "file_suffix": file_suffix, "mode": "0600"},
            output={"path": "ssl_ca_path"},
            when=when,
        )

    def test_writes_content_to_temp_file(self):
        state = PipelineState(raw={"ca_pem": "CERT_CONTENT"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)

        path = state.derived.get("ssl_ca_path")
        self.assertIsNotNone(path)
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertEqual("CERT_CONTENT", f.read())
        os.unlink(path)

    def test_output_key_written_to_derived(self):
        state = PipelineState(raw={"ca_pem": "DATA"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)
        self.assertIn("ssl_ca_path", state.derived)
        os.unlink(state.derived["ssl_ca_path"])

    def test_does_not_overwrite_raw(self):
        state = PipelineState(raw={"ca_pem": "DATA"})
        step = self._make_step("{{ raw.ca_pem }}")
        TmpFileWriteTransform().execute(step, state)
        self.assertNotIn("ssl_ca_path", state.raw)
        os.unlink(state.derived["ssl_ca_path"])

    def test_missing_contents_raises(self):
        from apollo.integrations.ccp.errors import CcpPipelineError
        state = PipelineState(raw={})
        step = TransformStep(
            type="tmp_file_write",
            input={"file_suffix": ".pem"},
            output={"path": "ssl_ca_path"},
        )
        with self.assertRaises(CcpPipelineError):
            TmpFileWriteTransform().execute(step, state)


import base64
import apollo.integrations.ccp.transforms.decode_bytes  # noqa: F401 — triggers registration
from apollo.integrations.ccp.transforms.decode_bytes import DecodeBytesTransform


class TestDecodeBytesTransform(TestCase):
    def _make_step(self):
        return TransformStep(type="decode_bytes", input={}, output={})

    def test_plain_values_unchanged(self):
        state = PipelineState(raw={"host": "h", "port": 5432})
        DecodeBytesTransform().execute(self._make_step(), state)
        self.assertEqual({"host": "h", "port": 5432}, state.raw)

    def test_encoded_bytes_decoded(self):
        encoded = {"__type__": "bytes", "__data__": base64.b64encode(b"cert-data").decode()}
        state = PipelineState(raw={"ssl_cert": encoded})
        DecodeBytesTransform().execute(self._make_step(), state)
        self.assertEqual(b"cert-data", state.raw["ssl_cert"])

    def test_nested_encoded_bytes_decoded(self):
        encoded = {"__type__": "bytes", "__data__": base64.b64encode(b"nested").decode()}
        state = PipelineState(raw={"config": {"key": encoded}})
        DecodeBytesTransform().execute(self._make_step(), state)
        self.assertEqual(b"nested", state.raw["config"]["key"])

    def test_registered_in_registry(self):
        transform = TransformRegistry.get("decode_bytes")
        self.assertIsNotNone(transform)
