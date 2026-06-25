# apollo/integrations/ctp/registry.py
import threading
from typing import Any

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import CtpConfig
from apollo.integrations.ctp.pipeline import CtpPipeline

_ATTR_CONNECT_ARGS = "connect_args"
_initialized: bool = False
_init_lock = threading.Lock()


def _discover() -> None:
    """Import all CTP default modules to trigger registration.

    Called once on first registry access. Add new connector imports here as
    their proxy clients are updated in Phase 2 to read from connect_args.
    """
    import apollo.integrations.ctp.defaults.aws  # noqa: F401
    import apollo.integrations.ctp.defaults.bigquery  # noqa: F401
    import apollo.integrations.ctp.defaults.databricks  # noqa: F401
    import apollo.integrations.ctp.defaults.db2  # noqa: F401
    import apollo.integrations.ctp.defaults.dremio  # noqa: F401
    import apollo.integrations.ctp.defaults.gcp_dataform  # noqa: F401
    import apollo.integrations.ctp.defaults.git  # noqa: F401
    import apollo.integrations.ctp.defaults.hive  # noqa: F401
    import apollo.integrations.ctp.defaults.http  # noqa: F401
    import apollo.integrations.ctp.defaults.informatica  # noqa: F401
    import apollo.integrations.ctp.defaults.informatica_v2  # noqa: F401
    import apollo.integrations.ctp.defaults.motherduck  # noqa: F401
    import apollo.integrations.ctp.defaults.presto  # noqa: F401
    import apollo.integrations.ctp.defaults.snowflake  # noqa: F401
    import apollo.integrations.ctp.defaults.teradata  # noqa: F401
    import apollo.integrations.ctp.defaults.starburst_galaxy  # noqa: F401
    import apollo.integrations.ctp.defaults.redshift  # noqa: F401
    import apollo.integrations.ctp.defaults.sap_hana  # noqa: F401
    import apollo.integrations.ctp.defaults.salesforce_crm  # noqa: F401
    import apollo.integrations.ctp.defaults.starburst_enterprise  # noqa: F401
    import apollo.integrations.ctp.defaults.postgres  # noqa: F401
    import apollo.integrations.ctp.defaults.sql_server  # noqa: F401
    import apollo.integrations.ctp.defaults.tableau  # noqa: F401
    import apollo.integrations.ctp.defaults.power_bi  # noqa: F401
    import apollo.integrations.ctp.defaults.looker  # noqa: F401
    import apollo.integrations.ctp.defaults.mysql  # noqa: F401
    import apollo.integrations.ctp.defaults.oracle  # noqa: F401
    import apollo.integrations.ctp.defaults.mulesoft  # noqa: F401
    import apollo.integrations.ctp.defaults.fivetran  # noqa: F401


def _ensure_initialized() -> None:
    # Double-checked locking: keep the post-init path lock-free, but guard the
    # cold-start window so a second thread cannot observe a partial registry
    # while another thread is mid-discover. The flag flip MUST happen after
    # _discover() completes — see YET-1420 (apollo/integrations/ctp/transforms/
    # registry.py has the same pattern and was the original symptom site).
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            _discover()
            _initialized = True


class CtpRegistry:
    _registry: dict[str, CtpConfig] = {}

    @classmethod
    def register(cls, connection_type: str, config: CtpConfig) -> None:
        cls._registry[connection_type] = config

    @classmethod
    def get(cls, connection_type: str) -> CtpConfig | None:
        _ensure_initialized()
        return cls._registry.get(connection_type)

    @classmethod
    def resolve_custom(
        cls,
        connection_type: str,
        credentials: dict[str, Any],
        ctp_config: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Resolve credentials using a caller-supplied CTP config dict.

        The TypedDict schema from the registered default for connection_type is
        injected into the custom config's mapper so the output contract is preserved.
        Follows the same connect_args unwrap-and-run path as resolve().
        """
        _ensure_initialized()
        config = CtpConfig.from_dict(ctp_config)
        registered = cls._registry.get(connection_type)
        if registered is not None:
            config.mapper.schema = registered.mapper.schema
            # Inherit connector-level defaults from the registered config so custom
            # mappers automatically get static constants (e.g. http_scheme, keepalives).
            # Custom config's own connect_args_defaults take precedence over registered ones.
            config.connect_args_defaults = {
                **registered.connect_args_defaults,
                **config.connect_args_defaults,
            }
        pipeline_input = _build_pipeline_input(credentials)
        if pipeline_input is None:
            return credentials
        return {
            _ATTR_CONNECT_ARGS: CtpPipeline().execute(
                config, pipeline_input, context=context or {}
            )
        }

    @classmethod
    def resolve(
        cls,
        connection_type: str,
        credentials: dict[str, Any],
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Run the registered CTP pipeline for connection_type and return
        {"connect_args": <pipeline output>}.
        If credentials contain connect_args (DC pre-shaped path), the inner dict
        is unwrapped and run through the pipeline — both flat and pre-shaped
        credentials follow the same transform path.
        Raises CtpPipelineError if connection_type is not registered.
        """
        _ensure_initialized()
        config = cls.get(connection_type)
        if config is None:
            raise CtpPipelineError(
                stage="registry",
                message=f"No CTP config registered for '{connection_type}'. Call CtpRegistry.get() before resolve().",
            )
        pipeline_input = _build_pipeline_input(credentials)
        if pipeline_input is None:
            return credentials
        return {
            _ATTR_CONNECT_ARGS: CtpPipeline().execute(
                config, pipeline_input, context=context or {}
            )
        }


def _build_pipeline_input(credentials: dict[str, Any]) -> dict[str, Any] | None:
    """Return the dict to feed the CTP pipeline, or None if credentials cannot
    be processed (e.g. pre-built ODBC string).

    For DC pre-shaped credentials (a ``connect_args`` dict alongside other
    credential fields like ``ssl_options``), merge the outer fields with the
    inner ``connect_args`` so the pipeline sees the full credential picture.
    Inner ``connect_args`` keys take precedence — they're driver-direct and
    have been explicitly named.

    SUP-373: without this merge, the data-collector's mysql agent path
    (clients/plugins/plugin_mysql.py:48-51) sends ``ssl_options`` as a
    sibling of ``connect_args``; the previous unwrap discarded it, no SSL
    context was built, and pymysql connected without TLS — MySQL with
    ``require_secure_transport=ON`` rejected with error 3159.
    """
    inner = credentials.get(_ATTR_CONNECT_ARGS)
    if inner is None:
        return credentials
    if not isinstance(inner, dict):
        return None
    outer_siblings = {k: v for k, v in credentials.items() if k != _ATTR_CONNECT_ARGS}
    return {**outer_siblings, **inner}
