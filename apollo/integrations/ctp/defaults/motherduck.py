from typing import Required, TypedDict

from apollo.integrations.ctp.models import CtpConfig, MapperConfig
from apollo.integrations.ctp.registry import CtpRegistry


class MotherDuckClientArgs(TypedDict):
    db_name: Required[str]
    token: Required[str]


# Docs document connect_args as a STRING DuckDB connection string
# ("md:<db>?motherduck_token=<token>"). CTP also accepts a dict with db_name +
# token. Validator accepts both forms.
MOTHERDUCK_CREDENTIALS_SCHEMA = {
    "connect_args": {
        "required": True,
        "anyof": [
            {"type": "string", "empty": False},
            {
                "type": "dict",
                "schema": {
                    "db_name": {"type": "string", "required": True, "empty": False},
                    "token": {"type": "string", "required": True, "empty": False},
                },
            },
        ],
    },
}

MOTHERDUCK_DEFAULT_CTP = CtpConfig(
    name="motherduck-default",
    raw_credentials_schema=MOTHERDUCK_CREDENTIALS_SCHEMA,
    steps=[],
    mapper=MapperConfig(
        name="motherduck_client_args",
        schema=MotherDuckClientArgs,
        field_map={
            "db_name": "{{ raw.db_name }}",
            "token": "{{ raw.token }}",
        },
    ),
)

CtpRegistry.register("motherduck", MOTHERDUCK_DEFAULT_CTP)
