import threading

from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.transforms.base import Transform

_initialized: bool = False
_init_lock = threading.Lock()


def _discover() -> None:
    """Import all transform modules to trigger registration.

    Called once on first registry access. Add new transform imports here.
    """
    import apollo.integrations.ctp.transforms.tmp_file_write  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_ssl_options  # noqa: F401
    import apollo.integrations.ctp.transforms.fetch_remote_file  # noqa: F401
    import apollo.integrations.ctp.transforms.load_private_key  # noqa: F401
    import apollo.integrations.ctp.transforms.oauth  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_presto_auth  # noqa: F401
    import apollo.integrations.ctp.transforms.write_ini_file  # noqa: F401
    import apollo.integrations.ctp.transforms.generate_jwt  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_msal_token  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_databricks_oauth  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_databricks_token  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_informatica_session  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_redshift_credentials  # noqa: F401
    import apollo.integrations.ctp.transforms.resolve_mulesoft_endpoints  # noqa: F401
    import apollo.integrations.ctp.transforms.encode_basic_auth  # noqa: F401


def _ensure_initialized() -> None:
    # Double-checked locking: keep the post-init path lock-free, but guard the
    # cold-start window so a second thread cannot observe a partial registry
    # while another thread is mid-discover. The flag flip MUST happen after
    # _discover() completes — flipping it first (the prior implementation)
    # caused YET-1420: under Azure WSGI threading, late-registered transforms
    # appeared "unknown" while another thread's imports were still in flight.
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if not _initialized:
            _discover()
            _initialized = True


class TransformRegistry:
    _registry: dict[str, type[Transform]] = {}

    @classmethod
    def register(cls, type_name: str, transform_class: type[Transform]) -> None:
        cls._registry[type_name] = transform_class

    @classmethod
    def get(cls, type_name: str) -> Transform:
        _ensure_initialized()
        if type_name not in cls._registry:
            raise CtpPipelineError(
                stage="transform_lookup",
                message=f"Unknown transform type: '{type_name}'. Registered types: {sorted(cls._registry.keys())}",
            )
        return cls._registry[type_name]()
