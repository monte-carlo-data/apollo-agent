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
