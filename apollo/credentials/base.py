from apollo.credentials.external_credentials_cache import load_cached


class BaseCredentialsService:
    """
    Base class for credentials services, provides default behavior of
    expecting warehouse credentials to be included in the request.

    Subclasses pass a short ``provider_name`` identifier to ``__init__``
    (e.g. ``aws_secrets_manager``, ``azure_key_vault``). The name appears in
    the cache log lines emitted by :func:`load_cached` and is purely for
    observability — it doesn't affect routing. Making it a required
    constructor argument means a new subclass that forgets to supply it
    fails loudly at instantiation rather than silently logging a generic
    label. The attribute is stored as ``_provider_name`` because it is an
    implementation detail of the cache logging path, not part of the
    service's public contract.
    """

    def __init__(self, provider_name: str):
        self._provider_name = provider_name

    def get_credentials(self, credentials: dict) -> dict:
        external_credentials = self._load_external_credentials_cached(credentials)
        return self._merge_connect_args(
            incoming_credentials=credentials,
            external_credentials=external_credentials,
        )

    def _load_external_credentials_cached(self, credentials: dict) -> dict:
        """Cache the result of ``_load_external_credentials`` across operations.

        The default ``BaseCredentialsService`` implementation is a passthrough
        that does no network I/O — caching it adds no value and would pin
        request-specific dicts in memory, so we short-circuit it. Subclasses
        that talk to ASM / AKV / GSM benefit from the cache.

        ``type(self) is BaseCredentialsService`` (not ``isinstance``) is
        deliberate: it bypasses the cache only for *direct* uses of the
        passthrough base class. Every existing subclass overrides
        ``_load_external_credentials`` to actually fetch a secret, so they
        all benefit from caching. A future subclass that forgets to override
        ``_load_external_credentials`` would inherit the passthrough and
        cache request-specific dicts; that is a subclass authoring bug, not
        a defect in this gate.
        """
        if type(self) is BaseCredentialsService:
            return self._load_external_credentials(credentials)
        return load_cached(
            credentials, self._load_external_credentials, self._provider_name
        )

    def _load_external_credentials(self, credentials: dict) -> dict:
        return credentials

    def _merge_connect_args(
        self, incoming_credentials: dict, external_credentials: dict
    ) -> dict:
        """
        Merges the 'connect_args' objects from the incoming credentials (from the service node)
        and the external credentials (provided by the customer).
        Returns the external credentials containing the merged 'connect_args' object.
        """
        incoming_connect_args = incoming_credentials.get("connect_args")
        external_connect_args = external_credentials.get("connect_args")
        if not incoming_connect_args:
            # when incoming credentials don't have connect args, return external credentials as is
            return external_credentials
        if not external_connect_args:
            # when external credentials don't have connect args, return incoming connect args
            external_credentials["connect_args"] = incoming_connect_args
            return external_credentials
        if not isinstance(external_connect_args, dict) or not isinstance(
            incoming_connect_args, dict
        ):
            # when connect args is not a dict (could be a connection string), return
            # external credentials as is
            return external_credentials
        # merge the connect args dicts, external credentials take precedence
        merged_connect_args = {
            **incoming_connect_args,
            **external_connect_args,
        }
        external_credentials["connect_args"] = merged_connect_args
        return external_credentials
