import asyncio
import logging
import types
from unittest import TestCase
from unittest.mock import create_autospec, call

from apollo.agent.agent import Agent
from apollo.agent.evaluation_utils import AgentEvaluationUtils
from apollo.agent.log_context import AgentLogContext
from apollo.agent.logging_utils import LoggingUtils
from apollo.common.agent.constants import CONTEXT_VAR_CLIENT
from apollo.common.agent.models import AgentCommand, AgentCommands
from tests.sample_proxy_client import SampleProxyClient

_EVALUATION_LOGGER = "apollo.agent.evaluation_utils"


class AgentCommandsTests(TestCase):
    def setUp(self) -> None:
        self._query = "SELECT * FROM table"
        self._expected_result = SampleProxyClient().execute_and_fetch(self._query)

        self._client = SampleProxyClient()

    def test_single_call_wrapper_method(self):
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "execute_and_fetch",
                            "args": [self._query],
                        }
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_commands_list_client_methods(self):
        # _client.execute_query(query)
        # _client.fetch_results()

        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                        },
                        {
                            "method": "fetch_results",
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_store_and_commands_list_cursor_methods(self):
        # _cursor = _client.cursor()
        # _cursor.cursor_execute_query(query)
        # _cursor.cursor_fetch_results()
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "cursor",
                            "store": "_cursor",
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_fetch_results",
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_store_and_chained_cursor_methods(self):
        # _cursor = _client.cursor()
        # _cursor.cursor_execute_query(query).cursor_fetch_results()
        result = Agent(LoggingUtils())._execute(
            self._client,
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {
                            "method": "cursor",
                            "store": "_cursor",
                        },
                        {
                            "target": "_cursor",
                            "method": "cursor_execute_query",
                            "kwargs": {
                                "query": self._query,
                            },
                            "next": {
                                "method": "query_results",
                            },
                        },
                    ],
                }
            ),
        )
        self.assertEqual(self._expected_result, result)

    def test_log_context(self):
        agent = Agent(LoggingUtils())
        log_context = create_autospec(AgentLogContext)
        agent.log_context = log_context

        trace_id = "135"
        agent.health_information(trace_id)

        log_context.set_agent_context.assert_has_calls(
            [
                call(
                    {
                        "mcd_operation_name": "health_information",
                        "mcd_trace_id": trace_id,
                    }
                ),
                call({}),
            ]
        )


class AgentCommandDunderHardeningTests(TestCase):
    """
    Pins the dunder guard in ``_resolve_method``: double-underscore method names
    are rejected except the allow-list the client dispatches (``__getitem__`` /
    ``__setitem__`` / ``__str__`` / ``__repr__``); single-underscore names pass
    through. See the guard comment in ``apollo/agent/evaluation_utils.py`` for
    the full rationale.
    """

    # Dunder names that DO resolve on a plain instance. Deleting the guard would
    # let these through the getattr, so they discriminate the guard from the
    # not-found fallback (names like ``__globals__``/``__subclasses__`` live on
    # functions/classes, not instances, and would raise via the fallback with or
    # without the guard — so they prove nothing here).
    _INSTANCE_DUNDER_GADGETS = (
        "__init__",
        "__class__",
        "__dict__",
        "__getattribute__",
        "__reduce__",
    )

    def test_resolve_method_rejects_dunder_names_at_the_guard(self):
        client = SampleProxyClient()
        for name in self._INSTANCE_DUNDER_GADGETS:
            with self.subTest(method=name):
                with self.assertLogs(
                    _EVALUATION_LOGGER, level=logging.ERROR
                ) as captured:
                    with self.assertRaises(AttributeError):
                        AgentEvaluationUtils._resolve_method(client, name)
                # The guard rejected it, not the not-found fallback (which would
                # log "Failed to resolve method"). This is what proves the guard.
                self.assertTrue(
                    any(
                        "Refusing to resolve dunder method" in r.getMessage()
                        and name in r.getMessage()
                        for r in captured.records
                    )
                )

    def test_resolve_method_allows_single_underscore_private_names(self):
        # Single-underscore names cannot start an escape chain (that always
        # needs a dunder hop) and existing operations dispatch them, e.g.
        # salesforce-data-cloud calls "_connection_type". They must resolve.
        client = SampleProxyClient()
        resolved = AgentEvaluationUtils._resolve_method(client, "_client")
        self.assertIs(resolved, client._client)

    def test_resolve_method_allows_safe_dunders(self):
        # data-collector legitimately dispatches these (dict subscript writes,
        # subscript reads, and stringifying a call result).
        target = {"key": "value"}
        for name in ("__getitem__", "__setitem__", "__str__", "__repr__"):
            with self.subTest(method=name):
                method = AgentEvaluationUtils._resolve_method(target, name)
                self.assertTrue(callable(method))

    def test_allowlisted_dunder_dispatches_end_to_end(self):
        # __setitem__/__getitem__ are load-bearing for aggregating query results;
        # assert they dispatch through the command path, not just resolve.
        context = {CONTEXT_VAR_CLIENT: SampleProxyClient()}
        target = {"seed": 0}  # non-empty so it is used as the explicit target

        set_command = AgentCommand.from_dict(
            {"method": "__setitem__", "args": ["k", "v"]}
        )
        AgentEvaluationUtils._execute_single_command(
            set_command, context, target=target
        )
        self.assertEqual("v", target["k"])

        get_command = AgentCommand.from_dict({"method": "__getitem__", "args": ["k"]})
        result = AgentEvaluationUtils._execute_single_command(
            get_command, context, target=target
        )
        self.assertEqual("v", result)

    def test_exploit_chain_first_hop_rejected(self):
        # The gadget's first hop (`__init__`) must fail before anything runs.
        context = {CONTEXT_VAR_CLIENT: SampleProxyClient()}
        command = AgentCommand.from_dict({"method": "__init__"})
        with self.assertRaises(AttributeError):
            AgentEvaluationUtils._execute_single_command(command, context)

    def test_underscore_target_and_store_names_still_resolve(self):
        # The guard is method-only: dunder-named target/store variables (context
        # variables like "_cursor"/"__utils") must keep working end to end.
        query = "SELECT * FROM table"
        expected = SampleProxyClient().execute_and_fetch(query)

        result = Agent(LoggingUtils())._execute(
            SampleProxyClient(),
            "test",
            AgentCommands.from_dict(
                {
                    "operation_name": "test",
                    "trace_id": "1",
                    "commands": [
                        {"method": "cursor", "store": "__cursor__"},
                        {
                            "target": "__cursor__",
                            "method": "cursor_execute_query",
                            "kwargs": {"query": query},
                        },
                        {
                            "target": "__cursor__",
                            "method": "cursor_fetch_results",
                        },
                    ],
                }
            ),
        )
        self.assertEqual(expected, result)


def _unstarted_generator():
    # Mirrors GitCloneClient.get_files: calling it returns an unstarted
    # generator whose body never runs, so no side effect is needed to obtain it.
    yield "x"


class _GeneratorReturningInternalClient:
    def get_files(self, _paths):
        return _unstarted_generator()


class _GeneratorProxyClient(SampleProxyClient):
    # wrapped_client exposes a generator-returning driver method, like the
    # Git/Looker-Git proxy clients in VULN-1227's reachable chain.
    def __init__(self):
        super().__init__()
        self._client = _GeneratorReturningInternalClient()


class _ReflectiveWrappedProxyClient(SampleProxyClient):
    # A hypothetical future proxy whose wrapped_client is itself reflective. The
    # fallback resolves the method off the wrapped_client, so the gate must apply
    # there too — otherwise `os.system` resolves straight off the module.
    @property
    def wrapped_client(self):
        import os

        return os


class AgentCommandReflectionHardeningTests(TestCase):
    """
    Pins the target type gate in ``_resolve_method``: resolving a method off a
    reflective/escape-hatch object (generator, frame, module, ...) is refused,
    cutting the ``gi_frame.f_globals.__getitem__("os")`` walk that bypasses the
    dunder block (YET-3286 / VULN-1227).
    """

    def test_resolve_method_refuses_generator_target(self):
        gen = _unstarted_generator()
        try:
            with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
                with self.assertRaises(AttributeError):
                    AgentEvaluationUtils._resolve_method(gen, "gi_frame")
            self.assertTrue(
                any(
                    "Refusing to resolve method on reflective target" in r.getMessage()
                    for r in captured.records
                )
            )
        finally:
            gen.close()

    def test_resolve_method_refuses_frame_target(self):
        gen = _unstarted_generator()
        frame = gen.gi_frame
        try:
            with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
                with self.assertRaises(AttributeError):
                    AgentEvaluationUtils._resolve_method(frame, "f_globals")
            self.assertTrue(
                any(
                    "Refusing to resolve method on reflective target" in r.getMessage()
                    for r in captured.records
                )
            )
        finally:
            gen.close()

    def test_resolve_method_refuses_module_target(self):
        import os

        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(AttributeError):
                AgentEvaluationUtils._resolve_method(os, "system")
        self.assertTrue(
            any(
                "Refusing to resolve method on reflective target" in r.getMessage()
                for r in captured.records
            )
        )

    def test_resolve_method_refuses_other_reflective_targets(self):
        # The remaining gated types beyond generator/frame/module. A regression
        # dropping any from _REFLECTIVE_TARGET_TYPES would otherwise pass silently;
        # tb_frame/co_consts/cr_frame/ag_frame are real gadget hops.
        try:
            raise ValueError("boom")
        except ValueError as ex:
            traceback = ex.__traceback__

        async def _coro():
            return None

        async def _agen():
            yield None

        coro = _coro()
        agen = _agen()
        cases = {
            "code": ((lambda: 0).__code__, "co_consts"),
            "traceback": (traceback, "tb_frame"),
            "coroutine": (coro, "cr_frame"),
            "async_generator": (agen, "ag_frame"),
        }
        try:
            for label, (target, attr) in cases.items():
                with self.subTest(target=label):
                    with self.assertLogs(
                        _EVALUATION_LOGGER, level=logging.ERROR
                    ) as captured:
                        with self.assertRaises(AttributeError):
                            AgentEvaluationUtils._resolve_method(target, attr)
                    self.assertTrue(
                        any(
                            "Refusing to resolve method on reflective target"
                            in r.getMessage()
                            for r in captured.records
                        )
                    )
        finally:
            coro.close()
            asyncio.run(agen.aclose())

    def test_resolve_method_refuses_reflective_wrapped_client(self):
        # The wrapped_client fallback resolves the method off the wrapped_client;
        # the gate must apply there too. Without it, "system" resolves off the os
        # module (`system` is not on the proxy, so the fallback is taken).
        client = _ReflectiveWrappedProxyClient()
        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(AttributeError):
                AgentEvaluationUtils._resolve_method(client, "system")
        self.assertTrue(
            any(
                "Refusing to resolve method on reflective target" in r.getMessage()
                for r in captured.records
            )
        )

    def test_reflection_chain_blocked_end_to_end(self):
        # client.wrapped_client.get_files(["x"]) -> unstarted generator;
        # the next hop (.gi_frame) must be refused before the walk reaches
        # f_globals / os. Reproduces the ticket chain benignly (no os.system).
        context = {CONTEXT_VAR_CLIENT: _GeneratorProxyClient()}
        commands = AgentCommands.from_dict(
            {
                "operation_name": "exploit",
                "trace_id": "1",
                "commands": [
                    {"method": "get_files", "args": [["x"]], "store": "_gen"},
                    {"target": "_gen", "method": "gi_frame"},
                ],
            }
        )
        get_files_cmd, gi_frame_cmd = commands.commands
        try:
            # First hop resolves the unstarted generator via the wrapped_client
            # fallback; assert that so the refusal below is pinned to .gi_frame.
            AgentEvaluationUtils._execute_command(get_files_cmd, context)
            self.assertIsInstance(context["_gen"], types.GeneratorType)
            with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
                with self.assertRaises(AttributeError):
                    AgentEvaluationUtils._execute_command(gi_frame_cmd, context)
            self.assertTrue(
                any(
                    "Refusing to resolve method on reflective target" in r.getMessage()
                    for r in captured.records
                )
            )
        finally:
            if isinstance(context.get("_gen"), types.GeneratorType):
                context["_gen"].close()

    def test_dict_getitem_still_resolves_on_query_results(self):
        # The gate must not touch ordinary query-result dicts: __getitem__ on a
        # plain dict stays dispatchable (load-bearing for result aggregation).
        method = AgentEvaluationUtils._resolve_method({"k": "v"}, "__getitem__")
        self.assertEqual("v", method("k"))


class _SecretRaisingClient(SampleProxyClient):
    _SECRET = "presigned-url-token-super-secret"

    def download_bytes(self, *args, **kwargs):
        raise ValueError(f"oversized payload {self._SECRET}")


class AgentCommandFailureLoggingTests(TestCase):
    def test_method_resolution_failure_logs_error_with_stable_message(self):
        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(AttributeError):
                AgentEvaluationUtils._resolve_method(
                    SampleProxyClient(), "does_not_exist"
                )

        self.assertEqual(1, len(captured.records))
        record = captured.records[0]
        self.assertEqual(logging.ERROR, record.levelno)
        self.assertIn("Failed to resolve method", record.getMessage())
        self.assertIn("does_not_exist", record.getMessage())

    def test_method_invocation_failure_logs_error_without_leaking_exception(self):
        client = _SecretRaisingClient()
        context = {CONTEXT_VAR_CLIENT: client}
        command = AgentCommand.from_dict({"method": "download_bytes"})

        with self.assertLogs(_EVALUATION_LOGGER, level=logging.ERROR) as captured:
            with self.assertRaises(ValueError):
                AgentEvaluationUtils._execute_single_command(command, context)

        self.assertEqual(1, len(captured.records))
        record = captured.records[0]
        self.assertEqual(logging.ERROR, record.levelno)
        self.assertIsNotNone(record.exc_info)
        message = record.getMessage()
        self.assertIn("download_bytes", message)
        self.assertIn("ValueError", message)
        self.assertNotIn(_SecretRaisingClient._SECRET, message)

    def test_method_invocation_expected_failure_logs_info_without_traceback(self):
        client = _ExpectedErrorRaisingClient()
        context = {CONTEXT_VAR_CLIENT: client}
        command = AgentCommand.from_dict({"method": "download_bytes"})

        with self.assertLogs(_EVALUATION_LOGGER, level=logging.INFO) as captured:
            with self.assertRaises(ValueError):
                AgentEvaluationUtils._execute_single_command(command, context)

        self.assertEqual(1, len(captured.records))
        record = captured.records[0]
        self.assertEqual(logging.INFO, record.levelno)
        self.assertIsNone(record.exc_info)
        message = record.getMessage()
        self.assertIn("Command failed", message)
        self.assertIn("download_bytes", message)
        self.assertNotIn(_SecretRaisingClient._SECRET, message)


class _ExpectedErrorRaisingClient(_SecretRaisingClient):
    def should_log_exception(self, ex: Exception) -> bool:
        return False


class _LongExpectedErrorRaisingClient(SampleProxyClient):
    _BODY = "<html>" + "x" * 5000 + "</html>"

    def download_bytes(self, *args, **kwargs):
        raise ValueError(self._BODY)

    def should_log_exception(self, ex: Exception) -> bool:
        return False


class AgentOperationExpectedFailureLoggingTests(TestCase):
    def test_expected_failure_log_truncates_long_error_message(self):
        client = _LongExpectedErrorRaisingClient()
        context = {CONTEXT_VAR_CLIENT: client}
        commands = [AgentCommand.from_dict({"method": "download_bytes"})]

        with self.assertLogs(_EVALUATION_LOGGER, level=logging.INFO) as captured:
            AgentEvaluationUtils.execute(
                context, LoggingUtils(), "op", commands, "trace-id"
            )

        messages = [
            r.getMessage()
            for r in captured.records
            if r.getMessage().startswith("Exception occurred executing operation")
        ]
        self.assertEqual(1, len(messages))
        self.assertLess(len(messages[0]), 1000)
        self.assertIn("<html>xxx", messages[0])
        self.assertNotIn("</html>", messages[0])
