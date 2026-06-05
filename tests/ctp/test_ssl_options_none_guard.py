"""Regression suite: every CTP default that consumes ``ssl_options`` must
skip its ssl resolution step(s) when the field arrives as ``None``.

Background: the data-collector serializes ``PluginConnectionSchema.ssl_options:
Optional[dict] = None`` straight into the credential payload. For customers
without SSL configured, the field arrives at the agent as the literal Python
``None``. Jinja2 considers ``None`` to be ``is defined`` (the value exists in
the namespace, even if it is None), and attribute access on ``None`` returns
``Undefined``, so guard expressions of the form
``raw.ssl_options is defined and raw.ssl_options.X is not defined`` evaluate
True on ``None`` — the step fires, the ``{{ raw.ssl_options }}`` template
renders to None, and the downstream transform rejects the non-dict input.

Each ``when`` clause that references ``raw.ssl_options`` must explicitly check
mapping-ness (e.g. ``raw.ssl_options is mapping``), so missing / None / non-dict
values short-circuit before any field traversal.
"""

from __future__ import annotations

from typing import Any

import pytest

from apollo.integrations.ctp.defaults.db2 import DB2_DEFAULT_CTP
from apollo.integrations.ctp.defaults.git import GIT_DEFAULT_CTP
from apollo.integrations.ctp.defaults.http import HTTP_DEFAULT_CTP
from apollo.integrations.ctp.defaults.mysql import MYSQL_DEFAULT_CTP
from apollo.integrations.ctp.defaults.postgres import POSTGRES_DEFAULT_CTP
from apollo.integrations.ctp.defaults.redshift import REDSHIFT_DEFAULT_CTP
from apollo.integrations.ctp.defaults.starburst_enterprise import (
    STARBURST_ENTERPRISE_DEFAULT_CTP,
)
from apollo.integrations.ctp.defaults.starburst_galaxy import (
    STARBURST_GALAXY_DEFAULT_CTP,
)
from apollo.integrations.ctp.defaults.teradata import TERADATA_DEFAULT_CTP
from apollo.integrations.ctp.models import CtpConfig
from apollo.integrations.ctp.pipeline import CtpPipeline


_MYSQL_FLAT = {
    "host": "db.example.com",
    "port": "3306",
    "user": "admin",
    "password": "secret",
}
_POSTGRES_FLAT = {
    "host": "db.example.com",
    "port": "5432",
    "user": "admin",
    "password": "secret",
    "database": "appdb",
}
_REDSHIFT_FLAT = {
    "host": "rs.example.com",
    "port": "5439",
    "user": "admin",
    "password": "secret",
    "database": "dev",
}
_GIT_FLAT = {
    "repo_url": "git@github.com:foo/bar.git",
    "ssh_key": "ZmFrZQ==",
}
_HTTP_FLAT: dict[str, str] = {}
_STARBURST_FLAT = {
    "host": "trino.example.com",
    "port": "443",
    "user": "admin",
    "password": "secret",
    "catalog": "hive",
}
_DB2_FLAT = {
    "host": "db2.example.com",
    "port": "50000",
    "user": "admin",
    "password": "secret",
    "database": "appdb",
}
_TERADATA_FLAT = {
    "host": "td.example.com",
    "port": "1025",
    "user": "admin",
    "password": "secret",
}


# Each tuple: (label, ctp_config, base_flat_credentials).
# Tests run the pipeline with ssl_options=None added to base credentials and
# assert the pipeline completes — i.e. the ssl-resolution step(s) skipped.
_DEFAULTS_WITH_SSL_OPTIONS: list[tuple[str, CtpConfig, dict[str, Any]]] = [
    ("mysql", MYSQL_DEFAULT_CTP, _MYSQL_FLAT),
    ("postgres", POSTGRES_DEFAULT_CTP, _POSTGRES_FLAT),
    ("redshift", REDSHIFT_DEFAULT_CTP, _REDSHIFT_FLAT),
    ("git", GIT_DEFAULT_CTP, _GIT_FLAT),
    ("http", HTTP_DEFAULT_CTP, _HTTP_FLAT),
    ("starburst_enterprise", STARBURST_ENTERPRISE_DEFAULT_CTP, _STARBURST_FLAT),
    ("starburst_galaxy", STARBURST_GALAXY_DEFAULT_CTP, _STARBURST_FLAT),
    ("db2", DB2_DEFAULT_CTP, _DB2_FLAT),
    ("teradata", TERADATA_DEFAULT_CTP, _TERADATA_FLAT),
]


@pytest.mark.parametrize(
    "ctp,base",
    [(ctp, base) for _, ctp, base in _DEFAULTS_WITH_SSL_OPTIONS],
    ids=[label for label, _, _ in _DEFAULTS_WITH_SSL_OPTIONS],
)
def test_ssl_options_none_is_skipped(ctp: CtpConfig, base: dict[str, Any]) -> None:
    """Pipeline must tolerate ``ssl_options: None`` (DC's serialized default
    for customers without SSL) without raising."""
    creds = {**base, "ssl_options": None}
    CtpPipeline().execute(ctp, creds)
