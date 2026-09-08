import dataclasses
import logging
from datetime import date, datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from apollo.integrations.base_proxy_client import BaseProxyClient
from apollo.integrations.custom_bi.custom_bi_connector_loader import (
    get_custom_bi_connector_registry,
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

    Serialization is fully recursive and field-name-agnostic: it preserves the
    nested structure of whatever the connector returns.  Because it never
    references model field names, new fields added by a connector are carried
    through automatically rather than dropped.

    ``Enum`` members serialize to their ``.value`` and ``datetime``/``date`` to
    ISO-8601 strings.
    """
    if obj is None:
        return None
    if isinstance(obj, Enum):
        return _serialize(obj.value)
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
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


class CustomBiProxyClient(BaseProxyClient):
    """
    Proxy client for custom BI connectors loaded from
    /opt/custom-bi-connectors/{name}/.

    The connector module is expected to define a Connector class with methods:
    setup_connection, close_connection, and fetch_metadata.  BI assets have no
    run pipeline, so there is no fetch_run_details.
    """

    def __init__(
        self,
        credentials: Optional[Dict],
        connector_dir: str,
        **kwargs: Any,
    ):
        if not credentials or _ATTR_CONNECT_ARGS not in credentials:
            raise ValueError(
                f"Custom BI connector agent client requires "
                f"{_ATTR_CONNECT_ARGS} in credentials"
            )

        module = load_connector_module(connector_dir)
        self._connector = module.Connector()

        self._connector.credentials = credentials[_ATTR_CONNECT_ARGS]
        self._connector.setup_connection()
        self._manifest = load_manifest(connector_dir)

        logger.info("Opened custom BI connector from %s", connector_dir)

    @property
    def wrapped_client(self):
        return self._connector

    def test_connection(self) -> Dict[str, bool]:
        """Connection is established in __init__; if we got here it succeeded."""
        return {"success": True}

    def fetch_bi_metadata(
        self,
        limit: int = 1000,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Fetch BI asset metadata from the connector.

        Delegates to the connector's ``fetch_metadata`` and serializes the
        returned model objects into dicts for the data-collector.
        """
        assets = self._connector.fetch_metadata(limit=limit, offset=offset)
        return {"all_results": [_serialize(a) for a in assets]}

    def get_manifest(self) -> Dict:
        """Return the manifest from manifest.json (excluding credentials_schema)."""
        result = dict(self._manifest)
        result.pop("credentials_schema", None)
        return result

    @staticmethod
    def get_custom_bi_connector_types() -> List[Dict[str, str]]:
        """
        Return a lightweight list of supported custom BI connector types.

        Each entry contains:
          - type: the connection_type identifier from the manifest
          - name: the human-readable name (falls back to type)

        Returns an empty list when no custom BI connectors are installed.
        """
        registry = get_custom_bi_connector_registry()
        result: List[Dict[str, str]] = []
        for connection_type, connector_dir in registry.items():
            manifest = load_manifest(connector_dir)
            result.append(
                {
                    "type": connection_type,
                    "name": manifest.get("connection_name", connection_type),
                }
            )
        return result

    @staticmethod
    def get_connection_manifests() -> Dict[str, Dict[str, Any]]:
        """
        Discover all custom BI connectors and return their manifests.

        Returns a dict keyed by connection_type, e.g.:
            {
                "custom-bi-connector-de8d7c2": {
                    "manifest": {
                        "connection_type": "custom-bi-connector-de8d7c2",
                        "name": "tableau",
                        "terminology": {...},
                        "icon_url": "..."
                    }
                }
            }
        """
        registry = get_custom_bi_connector_registry()
        result: Dict[str, Dict[str, Any]] = {}
        for connection_type, connector_dir in registry.items():
            manifest = load_manifest(connector_dir)
            manifest.pop("credentials_schema", None)
            result[connection_type] = {
                "manifest": manifest,
            }
        return result

    def _close_client(self):
        try:
            self._connector.close_connection()
            logger.info("Closed custom BI connector connection")
        except Exception:
            logger.exception("Error closing custom BI connector connection")
