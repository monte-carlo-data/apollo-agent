"""
Validation for agent self-upgrade requests.

The `/api/v1/upgrade` endpoint lets the Monte Carlo backend ask the agent to
redeploy itself with a different container image. Without constraints, any
caller authorized to invoke the agent can point it at an arbitrary image and
obtain code execution with the agent's cloud identity. Two cheap, defense-in-
depth controls are enforced here, at the single choke point in
`Agent._perform_update`, so all platforms (CloudRun, Azure, Lambda) are covered:

1. Registry/namespace allow-list  - the candidate image must come from the same
   registry (and, for multi-tenant public registries, the same namespace) as the
   image the agent is *currently* running. This turns "run any image" into "run
   another image from the place we already trust", and removes the arbitrary-code
   property that drives the High severity rating.

2. Platform match                 - the candidate must target the same runtime as
   the running image (Cloud Run / Azure / ECR-Lambda). Deploying an image built
   for another platform would brick the service, so a cross-platform swap is
   rejected.

3. Version floor                  - the candidate image's version may not be
   older than the running version. This blocks downgrade-to-known-vulnerable
   attacks that the allow-list alone leaves open.
"""

import os
from dataclasses import dataclass
from typing import List, Optional, Sequence

from packaging.version import InvalidVersion, Version

from apollo.common.agent.models import AgentConfigurationError

# Optional operator override: comma-separated "registry/namespace" or
# "registry/repository" prefixes to permit in addition to the running image's
# own registry+namespace. Normally empty - the running image is the trust anchor.
UPGRADE_ALLOWED_REPOS_ENV_VAR = "MCD_AGENT_UPGRADE_ALLOWED_REPOS"

# Public, multi-tenant registries where the namespace (first path segment) is the
# tenant boundary and therefore must be pinned in addition to the host. For a
# private registry (e.g. an ECR account host) the host itself is the boundary.
_PUBLIC_MULTI_TENANT_REGISTRIES = frozenset({"docker.io"})

_DEFAULT_REGISTRY = "docker.io"

# Platform suffix appended to the version in the docker.io agent image tags that
# this upgrade path supports, e.g. the "azure" in "1.8.6rc1234-azure". Only these
# two deployments upgrade via a suffixed docker.io tag; it is stripped before PEP
# 440 parsing. AWS Lambda pulls from ECR with plain "<semver>" tags (no suffix),
# which parse directly. Other markers (e.g. the docker.io "-lambda" reference
# image, or "-generic"/"-fargate") are not valid upgrade sources and are rejected
# by the version floor since they don't parse as a version.
# Splitting on a known suffix (rather than the first hyphen) also preserves a
# hyphen that is part of the version itself (e.g. "1.8.6-rc1").
_SUPPORTED_PLATFORM_TAG_SUFFIXES = frozenset({"cloudrun", "azure"})

# AWS Lambda image URIs may contain "*" in the region segment, which the Lambda
# updater later replaces with the current region. We normalize that segment away
# before comparing hosts so the wildcard can't be used to dodge the host match.
_ECR_HOST_SUFFIX = ".amazonaws.com"


@dataclass(frozen=True)
class ImageRef:
    """A parsed OCI image reference."""

    registry: str  # normalized host, e.g. "docker.io" or "123.dkr.ecr.*.amazonaws.com"
    repository: str  # full path, e.g. "montecarlodata/agent" or "mcd-agent"
    tag: Optional[str]
    digest: Optional[str]

    @property
    def namespace(self) -> Optional[str]:
        """First path segment, the tenant namespace on public registries."""
        return self.repository.split("/")[0] if "/" in self.repository else None

    @property
    def is_public_registry(self) -> bool:
        return self.registry in _PUBLIC_MULTI_TENANT_REGISTRIES


def get_configured_allowed_repos() -> List[str]:
    """Read the optional operator-configured allow-list of extra repo prefixes."""
    raw = os.getenv(UPGRADE_ALLOWED_REPOS_ENV_VAR, "")
    return [prefix.strip() for prefix in raw.split(",") if prefix.strip()]


def parse_image_ref(image: str) -> ImageRef:
    """
    Parse an image reference into (registry, repository, tag, digest), applying
    Docker's default-registry normalization.

    Deliberately strict: a component is only treated as a registry host if it
    looks like one (contains "." or ":", or is "localhost"). This prevents
    registry-confusion bypasses such as "montecarlodata/evil" being read as
    host="montecarlodata".
    """
    ref = image.strip()
    if not ref:
        raise AgentConfigurationError("Upgrade rejected: empty image reference")

    # Azure stores the image as a linuxFxVersion string ("DOCKER|<ref>"); strip
    # that prefix so the current and candidate images parse to the same registry.
    if ref.upper().startswith("DOCKER|"):
        ref = ref[len("DOCKER|") :]

    digest: Optional[str] = None
    if "@" in ref:
        ref, digest = ref.rsplit("@", 1)

    first, _, rest = ref.partition("/")
    if rest and ("." in first or ":" in first or first == "localhost"):
        registry, remainder = first, rest
    else:
        registry, remainder = _DEFAULT_REGISTRY, ref

    tag: Optional[str] = None
    # A ":" after the last "/" is a tag separator (": " inside the host was already
    # consumed as the port above).
    repo_part, sep, maybe_tag = remainder.rpartition(":")
    if sep and "/" not in maybe_tag:
        repository, tag = repo_part, maybe_tag
    else:
        repository = remainder

    if not repository:
        raise AgentConfigurationError(
            f"Upgrade rejected: unparseable image reference '{image}'"
        )

    return ImageRef(
        registry=_normalize_registry(registry),
        repository=repository,
        tag=tag,
        digest=digest or None,
    )


def _normalize_registry(registry: str) -> str:
    """Collapse the ECR region segment to '*' so the Lambda wildcard and a
    concrete region compare equal, and normalize the docker.io aliases."""
    if registry in ("index.docker.io", "registry-1.docker.io"):
        return "docker.io"
    if registry.endswith(_ECR_HOST_SUFFIX) and ".dkr.ecr." in registry:
        account, _, tail = registry.partition(".dkr.ecr.")
        # tail looks like "<region>.amazonaws.com"; replace region with "*"
        _, _, suffix = tail.partition(".")
        return f"{account}.dkr.ecr.*.{suffix}"
    return registry


def _matches_allowed_source(candidate: ImageRef, current: ImageRef) -> bool:
    if candidate.registry != current.registry:
        return False
    # On a public multi-tenant registry the namespace is the trust boundary and
    # must match too; on a private registry the host already scopes it.
    if current.is_public_registry:
        return candidate.namespace == current.namespace
    return True


def _platform_tag_suffix(ref: ImageRef) -> Optional[str]:
    """
    The supported platform suffix on the tag ("cloudrun" or "azure"), or None for
    a plain tag (e.g. ECR/Lambda "1.8.6", or "latest"). Splits on the LAST hyphen
    so a hyphen inside the version (e.g. "1.8.6-rc1") is never mistaken for a
    suffix, and an unsupported marker (e.g. "-lambda") reports as None.
    """
    if not ref.tag:
        return None
    base, sep, suffix = ref.tag.rpartition("-")
    return suffix if sep and suffix in _SUPPORTED_PLATFORM_TAG_SUFFIXES else None


def _extract_version(ref: ImageRef) -> Optional[Version]:
    """
    Best-effort PEP 440 version from a tag like "1.0.1-cloudrun" or
    "0.0.1rc202-azure". Returns None if no version can be parsed (e.g. "latest",
    a digest-only reference, or an unsupported suffix like "-lambda").
    """
    if not ref.tag:
        return None
    suffix = _platform_tag_suffix(ref)
    candidate = ref.tag[: -(len(suffix) + 1)] if suffix else ref.tag
    try:
        return Version(candidate)
    except InvalidVersion:
        return None


def validate_upgrade_image(
    image: str,
    current_image: Optional[str],
    current_version: str,
    *,
    extra_allowed_repos: Optional[Sequence[str]] = None,
    allow_unversioned: bool = False,
) -> None:
    """
    Raise AgentConfigurationError if `image` is not a permitted upgrade target.

    :param image: the requested image reference from the upgrade request.
    :param current_image: the image the agent is currently running
        (updater.get_current_image()); the trust anchor for the allow-list.
    :param current_version: the running agent version (settings.VERSION);
        "local" disables the version floor for dev images.
    :param extra_allowed_repos: optional additional "registry/namespace" or
        "registry/repository" prefixes to permit, from operator config.
    :param allow_unversioned: if False (default), reject candidate images whose
        tag carries no parseable version (e.g. "latest").
    """
    candidate = parse_image_ref(image)
    current = parse_image_ref(current_image) if current_image else None

    # --- Control 1: registry / namespace allow-list ---------------------------
    allowed = False
    if current:
        allowed = _matches_allowed_source(candidate, current)
    if not allowed and extra_allowed_repos:
        cand_full = f"{candidate.registry}/{candidate.repository}"
        allowed = any(cand_full.startswith(prefix) for prefix in extra_allowed_repos)
    if not allowed:
        raise AgentConfigurationError(
            f"Upgrade rejected: image '{image}' is not from an allowed registry/namespace"
        )

    # --- Control 2: platform match -------------------------------------------
    # Block a recognized cross-platform swap: an Azure image deployed onto a Cloud
    # Run service (or vice versa) would not start. Only fires when both images
    # carry a recognized suffix and they differ; unsuffixed candidates (ECR/Lambda
    # semver, "latest", digests, unsupported suffixes) fall through to the version
    # floor, which rejects the ones that aren't a real versioned image.
    candidate_platform = _platform_tag_suffix(candidate)
    current_platform = _platform_tag_suffix(current) if current else None
    if (
        candidate_platform
        and current_platform
        and candidate_platform != current_platform
    ):
        raise AgentConfigurationError(
            f"Upgrade rejected: image '{image}' is a '{candidate_platform}' image but "
            f"the agent is running a '{current_platform}' image"
        )

    # --- Control 3: version floor --------------------------------------------
    if current_version == "local":
        return  # dev build: no meaningful floor to enforce

    candidate_version = _extract_version(candidate)
    if candidate_version is None:
        if allow_unversioned:
            return
        raise AgentConfigurationError(
            f"Upgrade rejected: image '{image}' has no parseable version "
            f"(expected '<version>', '<version>-cloudrun' or '<version>-azure')"
        )

    try:
        running_version = Version(current_version)
    except InvalidVersion:
        # Unparseable running version: fail open on the floor only, the
        # allow-list above still applies.
        return

    if candidate_version < running_version:
        raise AgentConfigurationError(
            f"Upgrade rejected: image version {candidate_version} is older than "
            f"the running version {running_version} (downgrade blocked)"
        )
