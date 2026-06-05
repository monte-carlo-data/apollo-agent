import importlib.util
import json
import logging
import os
import types
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_CUSTOM_ETL_CONNECTORS_BASE_PATH = "/opt/custom-etl-connectors"

# Module-level cache: {connection_type: connector_dir_path}
_custom_etl_connector_registry: Optional[Dict[str, str]] = None

# Module-level cache: {module_name: loaded module}
_loaded_modules: Dict[str, types.ModuleType] = {}


def _discover_custom_etl_connectors() -> Dict[str, str]:
    """
    Scan the custom ETL connectors directory and build a mapping of
    connection_type -> directory path by reading each manifest.json.
    """
    registry: Dict[str, str] = {}
    base_path = _CUSTOM_ETL_CONNECTORS_BASE_PATH

    if not os.path.isdir(base_path):
        logger.info("Custom ETL connectors directory not found: %s", base_path)
        return registry

    for name in sorted(os.listdir(base_path)):
        connector_dir = os.path.join(base_path, name)
        if not os.path.isdir(connector_dir):
            continue

        manifest_path = os.path.join(connector_dir, "manifest.json")
        if not os.path.isfile(manifest_path):
            logger.warning("Skipping custom ETL connector %s: no manifest.json", name)
            continue

        try:
            with open(manifest_path) as f:
                manifest = json.load(f)
            connection_type = manifest.get("connection_type")
            if not connection_type:
                logger.warning(
                    "Skipping custom ETL connector %s: no connection_type in manifest",
                    name,
                )
                continue
            registry[connection_type] = connector_dir
            logger.info(
                "Discovered custom ETL connector: %s -> %s",
                connection_type,
                connector_dir,
            )
        except Exception:
            logger.exception(
                "Failed to read manifest for custom ETL connector %s", name
            )

    return registry


def get_custom_etl_connector_registry() -> Dict[str, str]:
    """
    Return the cached custom ETL connector registry, discovering on first access.
    Contents are baked into the Docker image so they never change at runtime.
    """
    global _custom_etl_connector_registry
    if _custom_etl_connector_registry is None:
        _custom_etl_connector_registry = _discover_custom_etl_connectors()
    return _custom_etl_connector_registry


def load_connector_module(connector_dir: str) -> types.ModuleType:
    """
    Dynamically load connector.py from the given directory.
    Uses a unique module name per connector to avoid namespace collisions.
    Returns the loaded module.
    """
    module_path = os.path.join(connector_dir, "connector.py")
    if not os.path.isfile(module_path):
        raise FileNotFoundError(f"connector.py not found in {connector_dir}")

    # Use directory name as part of module name for uniqueness
    dir_name = os.path.basename(connector_dir)
    module_name = f"custom_etl_connector_{dir_name}"

    if module_name in _loaded_modules:
        return _loaded_modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Failed to create module spec for {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _loaded_modules[module_name] = module
    return module


def load_manifest(connector_dir: str) -> Dict:
    """
    Read manifest.json from the connector directory.
    Returns the parsed dict, or empty dict if not found.
    """
    manifest_path = os.path.join(connector_dir, "manifest.json")
    if not os.path.isfile(manifest_path):
        return {}

    with open(manifest_path) as f:
        return json.load(f)
