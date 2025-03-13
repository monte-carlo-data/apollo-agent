from apollo.agent.platform import AgentPlatformProvider
from apollo.interfaces.generic.platforms.aws_metal.platform import (
    AwsMetalPlatformProvider,
)
from apollo.agent.constants import PLATFORM_AWS_METAL


def get_generic_platform_provider(platform: str, **kwargs) -> AgentPlatformProvider | None:  # type: ignore
    if platform == PLATFORM_AWS_METAL:
        return AwsMetalPlatformProvider(**kwargs)
    return None
