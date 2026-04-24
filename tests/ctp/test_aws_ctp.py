# tests/ctp/test_aws_ctp.py
from unittest import TestCase

from apollo.integrations.ctp.defaults.aws import (
    ATHENA_DEFAULT_CTP,
    GLUE_DEFAULT_CTP,
    MSK_CONNECT_DEFAULT_CTP,
    MSK_KAFKA_DEFAULT_CTP,
    S3_DEFAULT_CTP,
)
from apollo.integrations.ctp.pipeline import CtpPipeline
from apollo.integrations.ctp.registry import CtpRegistry


def _resolve(config, credentials: dict) -> dict:
    return CtpPipeline().execute(config, credentials)


_ALL_CONFIGS = [
    ("athena", ATHENA_DEFAULT_CTP),
    ("glue", GLUE_DEFAULT_CTP),
    ("s3", S3_DEFAULT_CTP),
    ("msk-connect", MSK_CONNECT_DEFAULT_CTP),
    ("msk-kafka", MSK_KAFKA_DEFAULT_CTP),
]


class TestAwsCtp(TestCase):
    def test_aws_services_registered(self):
        for connection_type, _ in _ALL_CONFIGS:
            with self.subTest(connection_type=connection_type):
                self.assertIsNotNone(CtpRegistry.get(connection_type))

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
        result = _resolve(ATHENA_DEFAULT_CTP, {"aws_region": "eu-west-1"})
        self.assertEqual("eu-west-1", result["aws_region"])
        self.assertNotIn("assumable_role", result)
        self.assertNotIn("external_id", result)

    def test_omits_absent_optional_fields(self):
        result = _resolve(GLUE_DEFAULT_CTP, {"aws_region": "us-west-2"})
        self.assertNotIn("assumable_role", result)
        self.assertNotIn("external_id", result)
        self.assertNotIn("ssl_options", result)

    def test_ssl_options_passed_through(self):
        ssl = {
            "ca_data": "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n"
        }
        result = _resolve(
            S3_DEFAULT_CTP, {"aws_region": "us-east-1", "ssl_options": ssl}
        )
        self.assertEqual(ssl, result["ssl_options"])

    def test_empty_credentials_produces_empty_result(self):
        result = _resolve(MSK_CONNECT_DEFAULT_CTP, {})
        self.assertEqual({}, result)

    def test_msk_kafka_passes_through_same_fields(self):
        result = _resolve(
            MSK_KAFKA_DEFAULT_CTP,
            {
                "assumable_role": "arn:aws:iam::999:role/KafkaRole",
                "aws_region": "ap-southeast-1",
            },
        )
        self.assertEqual("arn:aws:iam::999:role/KafkaRole", result["assumable_role"])
        self.assertEqual("ap-southeast-1", result["aws_region"])
