"""
Process-wide cache for Databricks OAuth HeaderFactory objects.

Shared by the ``resolve_databricks_token`` transform (REST API path) and the
``resolve_databricks_oauth`` transform (SQL warehouse path). Both build a fresh
``HeaderFactory`` from a fresh ``Config`` per invocation by default; without
caching, that defeats the Databricks SDK's per-Config token caching and forces
an IdP round-trip per operation.

The cache is keyed by a sha256 digest of the credential tuple, so the raw
``client_secret`` never appears in the cache key. (The secret is still held in
the ``Config`` object the cached HeaderFactory owns — that's unavoidable.)

Thread-safety and LRU eviction are delegated to ``cachetools.cached``.
``info=True`` adds ``cache_info()`` / ``cache_clear()`` methods on the wrapped
function for diagnostic logging and tests.
"""

import logging
import time
from hashlib import sha256
from threading import Lock
from typing import Callable, Optional

from cachetools import LRUCache, cached
from databricks.sdk.core import (
    Config,
    azure_service_principal,
    oauth_service_principal,
)

logger = logging.getLogger(__name__)


_OAUTH_CACHE: LRUCache = LRUCache(maxsize=128)
_OAUTH_CACHE_LOCK = Lock()


def _oauth_cache_key(
    workspace_url: str,
    client_id: str,
    client_secret: str,
    azure_tenant_id: Optional[str],
    azure_workspace_resource_id: Optional[str],
) -> str:
    """sha256-digest cache key derived from the credential tuple."""
    h = sha256()
    for part in (
        workspace_url,
        client_id,
        client_secret,
        azure_tenant_id,
        azure_workspace_resource_id,
    ):
        h.update((part or "").encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


@cached(_OAUTH_CACHE, key=_oauth_cache_key, lock=_OAUTH_CACHE_LOCK, info=True)
def cached_header_factory(
    workspace_url: str,
    client_id: str,
    client_secret: str,
    azure_tenant_id: Optional[str],
    azure_workspace_resource_id: Optional[str],
) -> Callable[[], dict]:
    """
    Return a cached Databricks SDK ``HeaderFactory`` for the given credentials.

    The HeaderFactory owns the SDK's internal TokenSource, which caches access
    tokens and refreshes them before expiry. Reusing the same factory across
    operations is what lets the SDK's token cache work; rebuilding it per call
    (the previous behaviour) defeated the cache and forced an IdP round-trip
    per operation.
    """
    t0 = time.monotonic()
    is_azure = bool(azure_tenant_id and azure_workspace_resource_id)
    if is_azure:
        config = Config(
            host=workspace_url,
            azure_client_id=client_id,
            azure_client_secret=client_secret,
            azure_tenant_id=azure_tenant_id,
            azure_workspace_resource_id=azure_workspace_resource_id,
        )
        factory = azure_service_principal(config)
    else:
        config = Config(
            host=workspace_url,
            client_id=client_id,
            client_secret=client_secret,
        )
        factory = oauth_service_principal(config)
    logger.info(
        f"Built Databricks OAuth header factory (cache miss), "
        f"duration_s={time.monotonic() - t0:.3f}, is_azure={is_azure}"
    )
    return factory


def cache_stats() -> dict:
    """Return a snapshot of cache stats for diagnostic logging."""
    info = cached_header_factory.cache_info()
    return {
        "hits": info.hits,
        "misses": info.misses,
        "size": info.currsize,
        "max_size": info.maxsize,
    }


def _reset_for_tests() -> None:
    """Clear cache state — for use in unit tests only."""
    cached_header_factory.cache_clear()
