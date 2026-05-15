"""Unit tests for the encode_basic_auth transform."""

import base64
from unittest import TestCase

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.transforms.encode_basic_auth import (
    EncodeBasicAuthTransform,
)


def _make_step(
    input_map: dict | None = None,
    output_map: dict | None = None,
) -> TransformStep:
    return TransformStep(
        type="encode_basic_auth",
        input=input_map or {"username": "{{ raw.user }}", "password": "{{ raw.pass }}"},
        output=output_map or {"token": "auth_token"},
    )


class TestEncodeBasicAuth(TestCase):
    def test_encodes_username_password(self):
        state = PipelineState(raw={"user": "alice", "pass": "secret"})
        step = _make_step()

        EncodeBasicAuthTransform().execute(step, state)

        expected = base64.b64encode(b"alice:secret").decode()
        self.assertEqual(expected, state.derived["auth_token"])

    def test_special_characters(self):
        state = PipelineState(raw={"user": "u:s@r", "pass": "p/a+ss="})
        step = _make_step()

        EncodeBasicAuthTransform().execute(step, state)

        expected = base64.b64encode(b"u:s@r:p/a+ss=").decode()
        self.assertEqual(expected, state.derived["auth_token"])

    def test_missing_username_raises(self):
        state = PipelineState(raw={"pass": "secret"})
        step = _make_step()

        with self.assertRaises((CtpPipelineError, Exception)):
            EncodeBasicAuthTransform().execute(step, state)

    def test_missing_password_raises(self):
        state = PipelineState(raw={"user": "alice"})
        step = _make_step()

        with self.assertRaises((CtpPipelineError, Exception)):
            EncodeBasicAuthTransform().execute(step, state)

    def test_empty_username_raises(self):
        state = PipelineState(raw={"user": "", "pass": "secret"})
        step = _make_step()

        with self.assertRaises(CtpPipelineError):
            EncodeBasicAuthTransform().execute(step, state)

    def test_empty_password_raises(self):
        state = PipelineState(raw={"user": "alice", "pass": ""})
        step = _make_step()

        with self.assertRaises(CtpPipelineError):
            EncodeBasicAuthTransform().execute(step, state)

    def test_required_input_keys_enforced(self):
        state = PipelineState(raw={"user": "alice", "pass": "secret"})
        step = TransformStep(
            type="encode_basic_auth",
            input={"username": "{{ raw.user }}"},  # missing password
            output={"token": "auth_token"},
        )

        with self.assertRaises(CtpPipelineError):
            EncodeBasicAuthTransform().execute(step, state)

    def test_required_output_keys_enforced(self):
        state = PipelineState(raw={"user": "alice", "pass": "secret"})
        step = TransformStep(
            type="encode_basic_auth",
            input={"username": "{{ raw.user }}", "password": "{{ raw.pass }}"},
            output={},  # missing token
        )

        with self.assertRaises(CtpPipelineError):
            EncodeBasicAuthTransform().execute(step, state)
