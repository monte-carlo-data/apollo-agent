"""In-memory TTL cache for externally fetched self-hosted credentials.

Network-backed credentials providers (ASM, AKV, GSM) make a fresh API call on
every operation, which has been observed to add seconds of latency per op when
the provider is intermittently slow. Caching the resolved external credentials
collapses those calls to roughly one per TTL window per distinct identifier.

The cache key is a sha256 of the credentials request with ``connect_args``
removed — ``connect_args`` carries request-specific values (warehouse id,
http_path, etc.) that should not affect which secret is fetched, while
everything else (provider type, secret name, region, vault url, assumable
role, …) is what identifies the secret source.

TTL is configurable via ``MCD_EXTERNAL_CREDENTIALS_CACHE_TTL_SECONDS``.
Set to ``0`` to disable the cache entirely. The value is read once at module
import — changing the env var on a running process has no effect; restart the
process for the new TTL to take effect.

Caveat on concurrency: ``cachetools.cached`` releases the lock between the
cache lookup and the loader call, so N threads that miss the cache for the
same key simultaneously will all invoke the loader (the last write wins).
In practice the egress agent's traffic pattern is "one first call, then
concurrent calls hit the warm cache", so the herd is small and the
correctness cost is zero (all loaders return the same value). The fix would
be per-key locking, but the added complexity is not justified by observed
load.

Each call to :func:`load_cached` emits an INFO-level log line with the
provider class name, whether the call was a cache hit or miss, and the
elapsed time. This lets us verify in production whether the cache is doing
its job and, when ops are still slow, whether the time is spent inside the
loader or somewhere else in the request path.
"""

import copy
import hashlib
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List

from cachetools import TTLCache, cached

logger = logging.getLogger(__name__)

_CACHE_TTL_ENV_VAR = "MCD_EXTERNAL_CREDENTIALS_CACHE_TTL_SECONDS"
_DEFAULT_CACHE_TTL_SECONDS = 300
# Sized for the number of distinct credential identifiers a single agent
# process might hold — one entry per (provider type, secret name, region,
# assumable role) tuple. A typical customer has a handful of integrations;
# 128 leaves plenty of headroom and the per-entry memory cost is negligible.
_CACHE_MAX_SIZE = 128
_CONNECT_ARGS_KEY = "connect_args"

Loader = Callable[[Dict[str, Any]], Dict[str, Any]]


def _resolve_ttl_seconds() -> int:
    raw = os.getenv(_CACHE_TTL_ENV_VAR)
    if raw is None or raw == "":
        return _DEFAULT_CACHE_TTL_SECONDS
    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            f"Invalid {_CACHE_TTL_ENV_VAR}={raw!r}, falling back to "
            f"{_DEFAULT_CACHE_TTL_SECONDS}s"
        )
        return _DEFAULT_CACHE_TTL_SECONDS
    if value < 0:
        logger.warning(
            f"Negative {_CACHE_TTL_ENV_VAR}={value}, falling back to "
            f"{_DEFAULT_CACHE_TTL_SECONDS}s"
        )
        return _DEFAULT_CACHE_TTL_SECONDS
    return value


_CACHE_TTL_SECONDS: int = _resolve_ttl_seconds()
# TTLCache requires ttl > 0; the load_cached() short-circuit handles ttl=0.
_CACHE: TTLCache = TTLCache(maxsize=_CACHE_MAX_SIZE, ttl=max(_CACHE_TTL_SECONDS, 1))
_CACHE_LOCK = threading.RLock()


def _cache_key(credentials: Dict[str, Any], _loader: Loader) -> str:
    """Stable sha256 of the secret-identifying portion of the credentials dict.

    ``connect_args`` is excluded because it is request-specific and does not
    influence which secret should be fetched. The ``loader`` argument is
    ignored — the same credentials should always resolve to the same cached
    value regardless of which bound method produced it. ``default=str``
    protects against non-JSON-serialisable values appearing in the dict by
    stringifying them rather than raising.
    """
    cacheable = {k: v for k, v in credentials.items() if k != _CONNECT_ARGS_KEY}
    serialized = json.dumps(cacheable, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


@cached(_CACHE, key=_cache_key, lock=_CACHE_LOCK)
def _load_and_cache(credentials: Dict[str, Any], loader: Loader) -> Dict[str, Any]:
    """Cached load. The ``@cached`` decorator stores the loader's return value
    keyed by ``_cache_key(credentials, loader)`` — which by design ignores the
    ``loader`` argument, so the same credentials always map to the same cache
    entry regardless of which (potentially wrapped) loader was supplied."""
    return loader(credentials)


def load_cached(
    credentials: Dict[str, Any], loader: Loader, provider_name: str
) -> Dict[str, Any]:
    """Return the loader's result for ``credentials``, using the process-wide
    TTL cache.

    A deep copy is returned so callers can mutate the result (the existing
    ``_merge_connect_args`` implementation mutates the external credentials
    dict in place) without corrupting the cached value for subsequent callers.

    When the TTL is configured to ``0`` the cache is bypassed entirely and the
    loader is invoked on every call.

    Emits one INFO log per call describing ``provider_name``, hit/miss, and
    duration. ``provider_name`` is supplied by the caller (typically the
    ``BaseCredentialsService._provider_name`` instance attribute) rather than
    inferred from the loader, so the log label stays stable regardless of
    how the loader is plumbed.

    Five log states are possible: ``cache=hit``, ``cache=miss``,
    ``cache=disabled``, ``cache=miss-failed`` (loader was invoked and
    raised), ``cache=error`` (the cache machinery itself — key hash, lock,
    deepcopy — raised before/after the loader ran).
    """
    if _CACHE_TTL_SECONDS == 0:
        t0 = time.monotonic()
        try:
            return loader(credentials)
        finally:
            logger.info(
                f"Loaded external credentials, provider={provider_name}, "
                f"cache=disabled, duration_s={time.monotonic() - t0:.3f}"
            )

    # Wrap the loader so we know whether it actually ran (cache miss) without
    # having to peek into the cache ourselves and racing against other threads.
    miss_started_at: List[float] = []

    def timed_loader(c: Dict[str, Any]) -> Dict[str, Any]:
        miss_started_at.append(time.monotonic())
        return loader(c)

    t_total = time.monotonic()
    state = "error"
    try:
        cached_value = _load_and_cache(credentials, timed_loader)
        result = copy.deepcopy(cached_value)
        state = "miss" if miss_started_at else "hit"
        return result
    except Exception:
        state = "miss-failed" if miss_started_at else "error"
        raise
    finally:
        if miss_started_at:
            duration_s = time.monotonic() - miss_started_at[0]
        else:
            duration_s = time.monotonic() - t_total
        logger.info(
            f"Loaded external credentials, provider={provider_name}, "
            f"cache={state}, duration_s={duration_s:.3f}"
        )


def clear_external_credentials_cache() -> None:
    """Drop every cached entry. Intended for tests."""
    with _CACHE_LOCK:
        _CACHE.clear()
