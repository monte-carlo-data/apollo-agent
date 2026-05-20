import pytest

from apollo.credentials.external_credentials_cache import (
    clear_external_credentials_cache,
)


@pytest.fixture(autouse=True)
def _isolate_external_credentials_cache():
    """Drop the process-wide cache before and after every credentials test.

    The cache lives at module scope and persists across tests; without this
    fixture, provider tests that ``assert_called_once_with`` on a mocked
    client would flake whenever a prior test populated the cache for the
    same key.
    """
    clear_external_credentials_cache()
    yield
    clear_external_credentials_cache()
