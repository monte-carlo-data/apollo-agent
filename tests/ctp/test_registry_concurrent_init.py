"""Concurrency regression: registry init must not expose a partial registry.

YET-1420: on Azure agents (WSGI multi-threaded), the original
``_ensure_initialized()`` set ``_initialized = True`` BEFORE calling
``_discover()``, creating a race window where Thread B could observe
``_initialized == True`` while Thread A was still mid-discover, then look up
a not-yet-registered transform — producing the
``Unknown transform type: 'resolve_databricks_oauth'`` error seen on Haleon,
Ensemble HP, Wesco, and Mercedes Benz.

Two threads exercise the registry from a cold-start state. Thread A's
``_discover()`` is wrapped to block partway through. Thread B's lookup must
either (a) wait for Thread A's discover to complete or (b) run discover
itself — but must never see a partial registry.

Implementation note: when other tests in the suite have already imported the
transform/default modules, calling ``_discover()`` again is a no-op (modules
are in ``sys.modules`` so ``register()`` calls don't re-run). The test
captures a snapshot of the real registry contents BEFORE clearing, and the
patched ``_discover`` re-applies that snapshot — simulating registration
without relying on module re-import.
"""

from __future__ import annotations

import threading
from types import ModuleType
from typing import Any, Callable, Iterator

import pytest

from apollo.integrations.ctp import registry as ctp_registry_module
from apollo.integrations.ctp.registry import CtpRegistry
from apollo.integrations.ctp.transforms import registry as transform_registry_module
from apollo.integrations.ctp.transforms.registry import TransformRegistry


@pytest.fixture
def cold_transform_registry() -> Iterator[dict[str, Any]]:
    """Clear TransformRegistry to a cold-start state and yield a snapshot of
    its real contents (so the test's patched _discover can repopulate it).
    Restores on exit.

    Warms up the registry first to guarantee the snapshot is populated, even
    when this test file runs in isolation (no other tests have triggered
    transform module imports yet)."""
    transform_registry_module._discover()  # ensure modules imported + registered
    saved_initialized = transform_registry_module._initialized
    snapshot = dict(TransformRegistry._registry)
    assert snapshot, "TransformRegistry warm-up failed — snapshot is empty"
    transform_registry_module._initialized = False
    TransformRegistry._registry.clear()
    try:
        yield snapshot
    finally:
        transform_registry_module._initialized = saved_initialized
        TransformRegistry._registry.clear()
        TransformRegistry._registry.update(snapshot)


@pytest.fixture
def cold_ctp_registry() -> Iterator[dict[str, Any]]:
    ctp_registry_module._discover()
    saved_initialized = ctp_registry_module._initialized
    snapshot = dict(CtpRegistry._registry)
    assert snapshot, "CtpRegistry warm-up failed — snapshot is empty"
    ctp_registry_module._initialized = False
    CtpRegistry._registry.clear()
    try:
        yield snapshot
    finally:
        ctp_registry_module._initialized = saved_initialized
        CtpRegistry._registry.clear()
        CtpRegistry._registry.update(snapshot)


def _run_concurrent_init(
    registry_module: ModuleType,
    registry_dict: dict[str, Any],
    snapshot: dict[str, Any],
    trigger_lookup: Callable[[], None],
    late_lookup: Callable[[], None],
) -> BaseException | None:
    """Drive the cold-start race for a given registry.

    ``trigger_lookup`` runs on Thread A and triggers ``_ensure_initialized()``.
    ``late_lookup`` runs on Thread B after Thread A has entered ``_discover()``
    but before registrations complete. Returns the exception (if any) Thread
    B observed during its lookup.
    """
    in_discover = threading.Event()
    release_discover = threading.Event()

    def slow_discover() -> None:
        # Signal that the init thread has entered _discover() (and, in the
        # buggy version, has already flipped _initialized=True), then block
        # before repopulating so Thread B can race in.
        in_discover.set()
        assert release_discover.wait(timeout=5), "release_discover never set"
        registry_dict.update(snapshot)

    thread_a_error: list[BaseException] = []
    thread_b_error: list[BaseException] = []

    def thread_a_target() -> None:
        try:
            trigger_lookup()
        except BaseException as exc:  # noqa: BLE001 — surface for assertion
            thread_a_error.append(exc)

    def thread_b_target() -> None:
        # Wait until Thread A has entered slow_discover, then attempt the
        # lookup. In the buggy version _initialized is already True here,
        # so _ensure_initialized() returns immediately and the lookup hits
        # an empty registry.
        assert in_discover.wait(timeout=5), "Thread A never entered _discover"
        try:
            late_lookup()
        except BaseException as exc:  # noqa: BLE001 — surface for assertion
            thread_b_error.append(exc)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(registry_module, "_discover", slow_discover)
        thread_a = threading.Thread(target=thread_a_target, name="init-thread")
        thread_b = threading.Thread(target=thread_b_target, name="lookup-thread")
        thread_a.start()
        thread_b.start()
        # Thread B should complete (either by failing immediately on partial
        # state, or by waiting for Thread A and succeeding). Give it a moment,
        # then release Thread A so the test always cleans up.
        thread_b.join(timeout=3)
        release_discover.set()
        thread_a.join(timeout=5)

    # Surface Thread A errors immediately — they indicate a test setup issue
    # rather than the bug under test.
    if thread_a_error:
        raise AssertionError(
            f"Thread A (init) failed unexpectedly — test setup issue, not "
            f"the YET-1420 race: {thread_a_error[0]!r}"
        )
    return thread_b_error[0] if thread_b_error else None


def test_transform_registry_does_not_expose_partial_state(
    cold_transform_registry: dict[str, Any],
) -> None:
    """Thread B's lookup of a late-registered transform must not raise
    'Unknown transform type' while Thread A is mid-discover."""

    def init_thread() -> None:
        TransformRegistry.get("oauth")

    def late_lookup() -> None:
        # encode_basic_auth is the LAST entry in _discover(); maximally
        # exposed to the partial-registration race.
        TransformRegistry.get("encode_basic_auth")

    error = _run_concurrent_init(
        transform_registry_module,
        TransformRegistry._registry,
        cold_transform_registry,
        trigger_lookup=init_thread,
        late_lookup=late_lookup,
    )

    assert error is None, (
        f"Thread B observed a partial TransformRegistry while Thread A was "
        f"mid-discover. This is the YET-1420 race. Error: {error!r}"
    )


def test_ctp_registry_does_not_expose_partial_state(
    cold_ctp_registry: dict[str, Any],
) -> None:
    """Same race exists in CtpRegistry — Thread B's lookup of a late-registered
    connector must not return None while Thread A is mid-discover."""

    def init_thread() -> None:
        CtpRegistry.get("mysql")

    def late_lookup() -> None:
        # fivetran is the LAST default imported in _discover(); maximally
        # exposed to the partial-registration race.
        result = CtpRegistry.get("fivetran")
        if result is None:
            raise AssertionError(
                "CtpRegistry.get('fivetran') returned None — the "
                "registry was partially initialized when Thread B looked up"
            )

    error = _run_concurrent_init(
        ctp_registry_module,
        CtpRegistry._registry,
        cold_ctp_registry,
        trigger_lookup=init_thread,
        late_lookup=late_lookup,
    )

    assert error is None, (
        f"Thread B observed a partial CtpRegistry while Thread A was "
        f"mid-discover. This is the YET-1420 race in the connector registry. "
        f"Error: {error!r}"
    )
