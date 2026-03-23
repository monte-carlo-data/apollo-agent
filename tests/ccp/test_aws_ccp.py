# tests/ccp/test_aws_ccp.py
#
# BaseAwsProxyClient currently reads credentials flat, so no AWS CCP config is
# registered in CcpRegistry._discover(). Tests import configs directly and
# call CcpPipeline().execute() rather than going through CcpRegistry.resolve().
from unittest import TestCase

from apollo.integrations.ccp.defaults.aws import (
    ATHENA_DEFAULT_CCP,
    GLUE_DEFAULT_CCP,
    MSK_CONNECT_DEFAULT_CCP,
    MSK_KAFKA_DEFAULT_CCP,
    S3_DEFAULT_CCP,
)
from apollo.integrations.ccp.pipeline import CcpPipeline
from apollo.integrations.ccp.registry import CcpRegistry


def _resolve(config, credentials: dict) -> dict:
    return CcpPipeline().execute(config, credentials)


_ALL_CONFIGS = [
    ("athena", ATHENA_DEFAULT_CCP),
    ("glue", GLUE_DEFAULT_CCP),
    ("s3", S3_DEFAULT_CCP),
    ("msk-connect", MSK_CONNECT_DEFAULT_CCP),
    ("msk-kafka", MSK_KAFKA_DEFAULT_CCP),
]


class TestAwsCcp(TestCase):
    def test_aws_services_not_registered(self):
        for connection_type, _ in _ALL_CONFIGS:
            with self.subTest(connection_type=connection_type):
                self.assertIsNone(CcpRegistry.get(connection_type))

    def test_resolve_with_assumable_role(self):
        for _, config in _ALL_CONFIGS:
            with self.subTest(config=config.name):
                result = _resolve(
                    config,
                    {
                        "assumable_role": "arn:aws:iam::123456789012:role/MyRole",
                        "aws_region": "us-east-1",
                        "external_id": "my-external-id",
                    },
                )
                self.assertEqual(
                    "arn:aws:iam::123456789012:role/MyRole", result["assumable_role"]
                )
                self.assertEqual("us-east-1", result["aws_region"])
                self.assertEqual("my-external-id", result["external_id"])

    def test_resolve_region_only(self):
        result = _resolve(ATHENA_DEFAULT_CCP, {"aws_region": "eu-west-1"})
        self.assertEqual("eu-west-1", result["aws_region"])
        self.assertNotIn("assumable_role", result)
        self.assertNotIn("external_id", result)

    def test_omits_absent_optional_fields(self):
        result = _resolve(GLUE_DEFAULT_CCP, {"aws_region": "us-west-2"})
        self.assertNotIn("assumable_role", result)
        self.assertNotIn("external_id", result)
        self.assertNotIn("ssl_options", result)

    def test_ssl_options_passed_through(self):
        ssl = {
            "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
        }
        result = _resolve(
            S3_DEFAULT_CCP, {"aws_region": "us-east-1", "ssl_options": ssl}
        )
        self.assertEqual(ssl, result["ssl_options"])

    def test_empty_credentials_produces_empty_result(self):
        result = _resolve(MSK_CONNECT_DEFAULT_CCP, {})
        self.assertEqual({}, result)

    def test_msk_kafka_passes_through_same_fields(self):
        result = _resolve(
            MSK_KAFKA_DEFAULT_CCP,
            {
                "assumable_role": "arn:aws:iam::999:role/KafkaRole",
                "aws_region": "ap-southeast-1",
            },
        )
        self.assertEqual("arn:aws:iam::999:role/KafkaRole", result["assumable_role"])
        self.assertEqual("ap-southeast-1", result["aws_region"])
