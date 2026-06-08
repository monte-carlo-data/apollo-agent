import dataclasses
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.custom_etl.custom_etl_connector_loader import (
    get_custom_etl_connector_registry,
    load_connector_module,
    load_manifest,
)

logger = logging.getLogger(__name__)

_ATTR_CONNECT_ARGS = "connect_args"


def _serialize(obj: Any) -> Any:
    """Serialize connector model objects to JSON-compatible dicts.

    Handles dataclasses (via ``dataclasses.asdict``), objects with ``__dict__``,
    and primitive/collection types.  ``None`` values are stripped from the
    top-level dict so the response stays compact — downstream consumers treat
    absent keys as null anyway.
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items() if v is not None}
    try:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return {
                k: _serialize(v)
                for k, v in dataclasses.asdict(obj).items()
                if v is not None
            }
    except Exception:
        pass
    if hasattr(obj, "__dict__"):
        return {
            k: _serialize(v)
            for k, v in obj.__dict__.items()
            if not k.startswith("_") and v is not None
        }
    return str(obj)


class CustomEtlProxyClient(BaseProxyClient):
    """
    Proxy client for custom ETL connectors loaded from
    /opt/custom-etl-connectors/{name}/.

    The connector module is expected to define a Connector class
    (inheriting from BaseEtlConnector) with methods: setup_connection,
    close_connection, fetch_metadata, and fetch_run_details.
    """

    def __init__(
        self,
        credentials: Optional[Dict],
        connector_dir: str,
        **kwargs: Any,
    ):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Custom ETL connector agent client requires "
                f"{_ATTR_CONNECT_ARGS} in credentials"
            )

        module = load_connector_module(connector_dir)
        self._connector = module.Connector()

        self._connector.credentials = credentials[_ATTR_CONNECT_ARGS]
        self._connector.setup_connection()
        self._manifest = load_manifest(connector_dir)

        logger.info("Opened custom ETL connector from %s", connector_dir)

    @property
    def wrapped_client(self):
        return self._connector

    def test_connection(self) -> Dict[str, bool]:
        """Connection is established in __init__; if we got here it succeeded."""
        return {"success": True}

    def fetch_etl_assets(
        self,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Fetch ETL asset metadata from the connector.

        Delegates to the connector's ``fetch_metadata`` and serializes the
        returned model objects into the unified ETL asset shape expected by
        the data-collector.
        """
        assets = self._connector.fetch_metadata(limit=limit, offset=offset)
        return {"all_results": [_serialize(a) for a in assets]}

    def fetch_etl_runs(
        self,
        lookback_min: int,
        job_ids: Optional[List[str]] = None,
        job_run_ids: Optional[List[str]] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Fetch ETL run events from the connector.

        Translates the DC-facing parameters into the connector's
        ``fetch_run_details`` interface: ``lookback_min`` becomes a
        ``timedelta``, ``job_run_ids`` maps to ``run_ids``.  When
        ``job_ids`` is provided, results are post-filtered to include
        only runs whose ``job_source_id`` is in the set.
        """
        lookback = timedelta(minutes=lookback_min)
        runs = self._connector.fetch_run_details(
            run_ids=job_run_ids,
            lookback=lookback,
            limit=limit,
            offset=offset,
        )
        if job_ids:
            allowed = set(job_ids)
            runs = [r for r in runs if getattr(r, "job_source_id", None) in allowed]
        return {"all_results": [_serialize(r) for r in runs]}

    def get_manifest(self) -> Dict:
        """Return the full manifest from manifest.json."""
        return self._manifest

    @staticmethod
    def get_custom_etl_connector_types() -> List[Dict[str, str]]:
        """
        Return a lightweight list of supported custom ETL connector types.

        Each entry contains:
          - type: the connection_type identifier from the manifest
          - name: the human-readable name (falls back to type)

        Returns an empty list when no custom ETL connectors are installed.
        """
        registry = get_custom_etl_connector_registry()
        result: List[Dict[str, str]] = []
        for connection_type, connector_dir in registry.items():
            manifest = load_manifest(connector_dir)
            result.append(
                {
                    "type": connection_type,
                    "name": manifest.get("name", connection_type),
                }
            )
        return result

    @staticmethod
    def get_connection_manifests() -> Dict[str, Dict[str, Any]]:
        """
        Discover all custom ETL connectors and return their manifests.

        Returns a dict keyed by connection_type, e.g.:
            {
                "custom-etl-connector-de8d7c2": {
                    "manifest": {
                        "connection_type": "custom-etl-connector-de8d7c2",
                        "name": "adf",
                        "terminology": {...},
                        "icon_url": "..."
                    }
                }
            }
        """
        registry = get_custom_etl_connector_registry()
        result: Dict[str, Dict[str, Any]] = {}
        for connection_type, connector_dir in registry.items():
            result[connection_type] = {
                "manifest": load_manifest(connector_dir),
            }
        return result

    def close(self):
        try:
            self._connector.close_connection()
            logger.info("Closed custom ETL connector connection")
        except Exception:
            logger.exception("Error closing custom ETL connector connection")
