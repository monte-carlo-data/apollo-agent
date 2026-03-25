from apollo.integrations.ctp.models import CtpConfig, MapperConfig

PASSTHROUGH_CTP = CtpConfig(
    name="passthrough",
    steps=[],
    mapper=MapperConfig(
        name="passthrough",
        field_map={},
        passthrough=True,
    ),
)
