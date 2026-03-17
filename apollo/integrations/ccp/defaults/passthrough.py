from apollo.integrations.ccp.models import CcpConfig, MapperConfig

PASSTHROUGH_CCP = CcpConfig(
    name="passthrough",
    steps=[],
    mapper=MapperConfig(
        name="passthrough",
        field_map={},
        passthrough=True,
    ),
)
