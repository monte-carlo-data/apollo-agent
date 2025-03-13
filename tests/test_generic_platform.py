import os
from unittest import TestCase

from apollo.agent.constants import PLATFORM_AWS_METAL
from apollo.interfaces.generic.platforms.aws_metal.platform import (
    AwsMetalPlatformProvider,
)
from apollo.interfaces.generic.platforms.factory import get_generic_platform_provider


class TestGenericPlatform(TestCase):
    def test_get_generic_platform_provider(self):
        self.assertIsInstance(
            get_generic_platform_provider(PLATFORM_AWS_METAL), AwsMetalPlatformProvider
        )
        self.assertIsNone(get_generic_platform_provider("BLAH"))
