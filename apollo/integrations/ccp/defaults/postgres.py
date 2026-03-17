from apollo.integrations.ccp.models import CcpConfig, MapperConfig, TransformStep

POSTGRES_DEFAULT_CCP = CcpConfig(
    name="postgres-default",
    steps=[
        TransformStep(
            type="tmp_file_write",
            when="raw.ssl_ca_pem is defined",
            input={
                "contents": "{{ raw.ssl_ca_pem }}",
                "file_suffix": ".pem",
                "mode": "0400",
            },
            output={"path": "ssl_ca_path"},
            field_map={"sslrootcert": "{{ derived.ssl_ca_path }}"},
        )
    ],
    mapper=MapperConfig(
        name="postgres_client_args",
        output_schema="PostgresClientArgs",
        field_map={
            "host": "{{ raw.host }}",
            "port": "{{ raw.port }}",
            "dbname": "{{ raw.database }}",
            "user": "{{ raw.user }}",
            "password": "{{ raw.password }}",
            "sslmode": "{{ raw.ssl_mode | default('require') }}",
        },
    ),
)
