class BaseCredentialsService:
    """
    Base class for credentials services, provides default behavior of
    expecting warehouse credentials to be included in the request.
    """

    def get_credentials(self, credentials: dict) -> dict:
        return credentials
