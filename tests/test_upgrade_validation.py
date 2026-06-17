from unittest import TestCase
from unittest.mock import patch

from apollo.agent.upgrade_validation import (
    UPGRADE_ALLOWED_REPOS_ENV_VAR,
    ImageRef,
    get_configured_allowed_repos,
    parse_image_ref,
    validate_upgrade_image,
)
from apollo.common.agent.models import AgentConfigurationError

# CloudRun/Azure carry a platform suffix on the docker.io tag; Lambda/ECR tags
# are plain semver (no suffix), as stored in the ECR repository.
_DOCKER_CURRENT = "montecarlodata/agent:1.8.6-cloudrun"
_ECR_CURRENT = "111111111111.dkr.ecr.us-east-1.amazonaws.com/mcd-agent:1.8.6"


class ParseImageRefTests(TestCase):
    def test_docker_bare_reference(self):
        ref = parse_image_ref("montecarlodata/agent:1.8.6-cloudrun")
        self.assertEqual("docker.io", ref.registry)
        self.assertEqual("montecarlodata/agent", ref.repository)
        self.assertEqual("montecarlodata", ref.namespace)
        self.assertEqual("1.8.6-cloudrun", ref.tag)
        self.assertIsNone(ref.digest)
        self.assertTrue(ref.is_public_registry)

    def test_explicit_registry_with_port_is_not_a_repo_segment(self):
        ref = parse_image_ref("localhost:5000/team/agent:tag")
        self.assertEqual("localhost:5000", ref.registry)
        self.assertEqual("team/agent", ref.repository)
        self.assertEqual("tag", ref.tag)
        self.assertFalse(ref.is_public_registry)

    def test_digest_reference(self):
        ref = parse_image_ref("montecarlodata/agent@sha256:abc123")
        self.assertEqual("montecarlodata/agent", ref.repository)
        self.assertIsNone(ref.tag)
        self.assertEqual("sha256:abc123", ref.digest)

    def test_docker_io_aliases_normalized(self):
        self.assertEqual(
            "docker.io",
            parse_image_ref("index.docker.io/montecarlodata/agent:x").registry,
        )

    def test_ecr_region_collapsed_to_wildcard(self):
        # a concrete region and the Lambda "*" wildcard must normalize equal
        concrete = parse_image_ref(_ECR_CURRENT)
        wildcard = parse_image_ref(
            "111111111111.dkr.ecr.*.amazonaws.com/mcd-agent:1.9.0"
        )
        self.assertEqual(concrete.registry, wildcard.registry)
        self.assertEqual("111111111111.dkr.ecr.*.amazonaws.com", concrete.registry)

    def test_azure_linuxfxversion_prefix_stripped(self):
        # Azure get_current_image() returns "DOCKER|<ref>"; it must parse like the
        # bare candidate ref so a same-namespace upgrade is allowed.
        ref = parse_image_ref(
            "DOCKER|docker.io/montecarlodata/pre-release-agent:0.2.4rc674-azure"
        )
        self.assertEqual("docker.io", ref.registry)
        self.assertEqual("montecarlodata/pre-release-agent", ref.repository)
        self.assertEqual("montecarlodata", ref.namespace)

    def test_azure_current_image_allows_same_namespace_upgrade(self):
        validate_upgrade_image(
            image="docker.io/montecarlodata/pre-release-agent:0.2.4rc675-azure",
            current_image="DOCKER|docker.io/montecarlodata/pre-release-agent:0.2.4rc674-azure",
            current_version="0.2.4rc674",
        )

    def test_empty_reference_rejected(self):
        with self.assertRaises(AgentConfigurationError):
            parse_image_ref("   ")


class AllowlistTests(TestCase):
    def test_same_namespace_allowed(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.9.0-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
        )

    def test_sibling_repo_same_namespace_allowed(self):
        # pre-release repo lives under the same docker.io namespace
        validate_upgrade_image(
            image="montecarlodata/pre-release-agent:1.9.0rc5-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
        )

    def test_different_namespace_rejected(self):
        with self.assertRaisesRegex(AgentConfigurationError, "not from an allowed"):
            validate_upgrade_image(
                image="evil/agent:9.9.9-cloudrun",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_registry_confusion_rejected(self):
        # attacker-controlled host with a look-alike namespace path
        with self.assertRaisesRegex(AgentConfigurationError, "not from an allowed"):
            validate_upgrade_image(
                image="registry.evil.com/montecarlodata/agent:9.9.9-cloudrun",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_ecr_same_account_with_wildcard_allowed(self):
        # ECR/Lambda tags are plain semver with no platform suffix
        validate_upgrade_image(
            image="111111111111.dkr.ecr.*.amazonaws.com/mcd-agent:1.9.0",
            current_image=_ECR_CURRENT,
            current_version="1.8.6",
        )

    def test_ecr_different_account_rejected(self):
        with self.assertRaisesRegex(AgentConfigurationError, "not from an allowed"):
            validate_upgrade_image(
                image="222222222222.dkr.ecr.*.amazonaws.com/mcd-agent:1.9.0",
                current_image=_ECR_CURRENT,
                current_version="1.8.6",
            )

    def test_no_current_image_rejected_without_override(self):
        with self.assertRaisesRegex(AgentConfigurationError, "not from an allowed"):
            validate_upgrade_image(
                image="montecarlodata/agent:1.9.0-cloudrun",
                current_image=None,
                current_version="1.8.6",
            )

    def test_extra_allowed_repos_widens(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.9.0-cloudrun",
            current_image=None,
            current_version="1.8.6",
            extra_allowed_repos=["docker.io/montecarlodata"],
        )


class PlatformMatchTests(TestCase):
    def test_cross_platform_swap_rejected(self):
        # cloudrun agent must not be sent an azure image (would not start)
        with self.assertRaisesRegex(
            AgentConfigurationError, "is a 'azure' image but .* 'cloudrun'"
        ):
            validate_upgrade_image(
                image="montecarlodata/agent:1.9.0-azure",
                current_image=_DOCKER_CURRENT,  # ...-cloudrun
                current_version="1.8.6",
            )

    def test_same_platform_allowed(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.9.0-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
        )

    def test_ecr_plain_tags_match(self):
        # both sides have no suffix -> same platform
        validate_upgrade_image(
            image="111111111111.dkr.ecr.*.amazonaws.com/mcd-agent:1.9.0",
            current_image=_ECR_CURRENT,
            current_version="1.8.6",
        )


class VersionFloorTests(TestCase):
    def test_newer_allowed(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.9.0-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
        )

    def test_older_rejected(self):
        with self.assertRaisesRegex(AgentConfigurationError, "downgrade"):
            validate_upgrade_image(
                image="montecarlodata/agent:1.8.5-cloudrun",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_prerelease_is_older_than_final(self):
        # 1.8.6rc1234 < 1.8.6 per PEP 440
        with self.assertRaisesRegex(AgentConfigurationError, "downgrade"):
            validate_upgrade_image(
                image="montecarlodata/agent:1.8.6rc1234-cloudrun",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_final_newer_than_running_prerelease_allowed(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.8.6-cloudrun",
            current_image="montecarlodata/agent:1.8.6rc1234-cloudrun",
            current_version="1.8.6rc1234",
        )

    def test_newer_prerelease_allowed(self):
        validate_upgrade_image(
            image="montecarlodata/agent:1.8.6rc1235-cloudrun",
            current_image="montecarlodata/agent:1.8.6rc1234-cloudrun",
            current_version="1.8.6rc1234",
        )

    def test_same_version_allowed(self):
        # floor is strictly-less-than; re-deploying the same version is permitted
        validate_upgrade_image(
            image="montecarlodata/agent:1.8.6-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
        )

    def test_unversioned_tag_rejected_by_default(self):
        with self.assertRaisesRegex(AgentConfigurationError, "no parseable version"):
            validate_upgrade_image(
                image="montecarlodata/agent:latest",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_unversioned_tag_allowed_when_opted_in(self):
        validate_upgrade_image(
            image="montecarlodata/agent:latest",
            current_image=_DOCKER_CURRENT,
            current_version="1.8.6",
            allow_unversioned=True,
        )

    def test_digest_only_treated_as_unversioned(self):
        with self.assertRaisesRegex(AgentConfigurationError, "no parseable version"):
            validate_upgrade_image(
                image="montecarlodata/agent@sha256:abc123",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )

    def test_unsupported_platform_suffix_rejected(self):
        # only "-cloudrun"/"-azure" suffixes are supported; "-lambda" (a docker.io
        # reference-only tag) and "-fargate"/"-generic" are not upgrade sources and
        # do not parse as a version, so they are rejected
        for tag in ("1.9.0-lambda", "1.9.0-fargate", "1.9.0-generic"):
            with self.subTest(tag=tag):
                with self.assertRaisesRegex(
                    AgentConfigurationError, "no parseable version"
                ):
                    validate_upgrade_image(
                        image=f"montecarlodata/agent:{tag}",
                        current_image=_DOCKER_CURRENT,
                        current_version="1.8.6",
                    )

    def test_ecr_plain_semver_floor_enforced(self):
        # plain ECR semver tags compare directly
        with self.assertRaisesRegex(AgentConfigurationError, "downgrade"):
            validate_upgrade_image(
                image="111111111111.dkr.ecr.*.amazonaws.com/mcd-agent:1.8.5",
                current_image=_ECR_CURRENT,
                current_version="1.8.6",
            )

    def test_local_running_version_skips_floor(self):
        # dev build: downgrade check cannot be meaningfully applied
        validate_upgrade_image(
            image="montecarlodata/agent:0.0.1-cloudrun",
            current_image=_DOCKER_CURRENT,
            current_version="local",
        )

    def test_hyphenated_prerelease_suffix_not_truncated(self):
        # guards the rpartition-on-known-suffix behavior: "1.8.6-rc1" must not be
        # read as "1.8.6"
        with self.assertRaisesRegex(AgentConfigurationError, "downgrade"):
            validate_upgrade_image(
                image="montecarlodata/agent:1.8.6-rc1-cloudrun",
                current_image=_DOCKER_CURRENT,
                current_version="1.8.6",
            )


class ConfiguredAllowedReposTests(TestCase):
    def test_empty_when_unset(self):
        with patch.dict("os.environ", {}, clear=False):
            import os

            os.environ.pop(UPGRADE_ALLOWED_REPOS_ENV_VAR, None)
            self.assertEqual([], get_configured_allowed_repos())

    def test_parses_comma_separated(self):
        with patch.dict(
            "os.environ",
            {
                UPGRADE_ALLOWED_REPOS_ENV_VAR: "docker.io/montecarlodata, docker.io/other "
            },
        ):
            self.assertEqual(
                ["docker.io/montecarlodata", "docker.io/other"],
                get_configured_allowed_repos(),
            )
