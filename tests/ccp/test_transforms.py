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
