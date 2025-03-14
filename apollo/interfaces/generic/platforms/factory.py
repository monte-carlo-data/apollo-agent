from apollo.agent.platform import AgentPlatformProvider
from apollo.interfaces.generic.platforms.aws_generic.platform import (
    AwsGenericPlatformProvider,
)
from apollo.agent.constants import PLATFORM_AWS_GENERIC


def get_generic_platform_provider(platform: str, **kwargs) -> AgentPlatformProvider | None:  # type: ignore
    if platform == PLATFORM_AWS_GENERIC:
        return AwsGenericPlatformProvider(**kwargs)
    return None
