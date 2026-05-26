"""Shared cerberus schema fragments used by multiple connectors.

Putting these here (rather than redeclaring in every CTP default file) keeps
the per-connector schemas focused on what makes that connector different.
"""

from __future__ import annotations

from typing import Any

# Customer-facing shape of the top-level `ssl_options` field. Consumed by
# `resolve_ssl_options` and related transforms; subsets apply to different
# connectors (Postgres/Redshift/Oracle use `ca_data`; Teradata adds `disabled`;
# Starburst adds `skip_cert_verification`/`verify_cert`/`verify_identity`).
# `allow_unknown=True` keeps the block forgiving for forward compat — the
# resolve transforms ignore irrelevant keys.
SSL_OPTIONS_FIELD: dict[str, Any] = {
    "type": "dict",
    "allow_unknown": True,
    "schema": {
        "ca_data": {"type": "string"},
        "cert_data": {"type": "string"},
        "key_data": {"type": "string"},
        "key_password": {"type": "string"},
        "disabled": {"type": "boolean"},
        "skip_cert_verification": {"type": "boolean"},
        "verify_cert": {"type": "boolean"},
        "verify_identity": {"type": "boolean"},
    },
}
