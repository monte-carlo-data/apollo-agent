from unittest import TestCase

from apollo.agent.agent import Agent
from apollo.agent.models import (
    AgentCommand,
    AgentOperation,
)


class AgentOperationTests(TestCase):
    def setUp(self):
        self._trace_id = "b60be73d-db84-4157-afb0-a2e1f51b8dff"
        self._commands = [AgentCommand(method="foo")]

    def test_use_pre_signed_url_empty_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = None
        size = Agent._calculate_byte_size(result)
        self.assertEqual(0, size)
        self.assertFalse(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_string_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = "fizz"
        size = Agent._calculate_byte_size(result)
        self.assertEqual(4, size)
        self.assertFalse(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_bytes_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = b"fizz"
        size = Agent._calculate_byte_size(result)
        self.assertEqual(4, size)
        self.assertFalse(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_int_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=0,
        )
        result = 2100
        size = Agent._calculate_byte_size(result)
        self.assertEqual(2, size)
        self.assertTrue(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_float_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = 1234.567
        size = Agent._calculate_byte_size(result)
        self.assertEqual(8, size)
        self.assertTrue(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_dict_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = {"fizz": "buzz"}
        size = Agent._calculate_byte_size(result)
        self.assertEqual(16, size)
        self.assertTrue(operation.use_pre_signed_url(size))

    def test_use_pre_signed_url_list_result(self):
        operation = AgentOperation(
            trace_id=self._trace_id,
            commands=self._commands,
            pre_signed_url_result_limit_bytes=5,
        )
        result = ["fizz", "buzz"]
        size = Agent._calculate_byte_size(result)
        self.assertEqual(16, size)
        self.assertTrue(operation.use_pre_signed_url(size))
