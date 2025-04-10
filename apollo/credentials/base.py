class BaseCredentialsService:
    """
    Base class for credentials services, provides default behavior of
    expecting warehouse credentials to be included in the request.
    """

    def get_credentials(self, credentials: dict) -> dict:
        return credentials

    def _merge_connect_args(
        self, incoming_credentials: dict, external_credentials: dict
    ) -> dict:
        """
        Merges the 'connect_args' objects from the incoming credentials (from the service node)
        and the external credentials (provided by the customer).
        Returns the external credentials containing the merged 'connect_args' object.
        """
        incoming_connect_args = incoming_credentials.get("connect_args", {})
        external_connect_args = external_credentials.get("connect_args", {})
        merged_connect_args = {
            **incoming_connect_args,
            **external_connect_args,
        }
        if merged_connect_args:
            external_credentials["connect_args"] = merged_connect_args
        return external_credentials
