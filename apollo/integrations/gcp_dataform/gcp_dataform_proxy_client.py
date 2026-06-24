"""Thin proxy client for GCP Dataform.

Instantiates the ``google-cloud-dataform`` SDK and exposes individual API
operations as methods that return mechanically serialized dicts via
proto-plus ``to_dict()``. No business logic, no filtering, no status
mapping — all interpretation lives in the data-collector client.

Used in two modes:
- **Agent-side**: instantiated by the proxy client factory when the DC
  delegates via ``@agent_operation``.
- **DC-side (direct)**: instantiated directly by the DC client when no
  remote agent is configured, since DC depends on apollo-agent.
"""

from __future__ import annotations

import logging
from typing import Any, cast

from google.cloud import dataform_v1
from google.oauth2 import service_account

from apollo.integrations.base_proxy_client import BaseProxyClient

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


class GcpDataformProxyClient(BaseProxyClient):
    """Thin proxy around the ``google-cloud-dataform`` SDK.

    Each public method maps to one SDK call and returns the results
    serialized via proto-plus ``to_dict()``. The DC interprets the
    raw fields, maps enum values, and builds the unified wire format.

    Required connect_args:
        - ``project_id`` — GCP project ID
        - ``service_account_info`` — service account JSON key dict
    """

    def __init__(
        self,
        credentials: dict | None = None,
        **kwargs: Any,
    ):
        connect_args = (credentials or {}).get("connect_args", credentials or {})

        self._project_id: str = connect_args.get("project_id", "")
        self._locations: list[str] = connect_args.get("locations", [])

        sa_info = connect_args.get("service_account_info")
        if not sa_info:
            raise ValueError(
                "GCP Dataform requires 'service_account_info' in credentials"
            )
        creds = service_account.Credentials.from_service_account_info(
            sa_info,
            scopes=_SCOPES,
        )
        self._client = dataform_v1.DataformClient(credentials=creds)

    @property
    def wrapped_client(self) -> Any:
        return self._client

    def get_connection_metadata(self) -> dict[str, Any]:
        """Return non-secret metadata extracted from the resolved credentials."""
        return {
            "project_id": self._project_id,
            "locations": self._locations,
        }

    # -- List operations ------------------------------------------------------

    def list_repositories(self, parent: str) -> list[dict]:
        return [
            cast(dict, type(r).to_dict(r))
            for r in self._client.list_repositories(parent=parent)
        ]

    def list_workflow_configs(self, parent: str) -> list[dict]:
        return [
            cast(dict, type(c).to_dict(c))
            for c in self._client.list_workflow_configs(parent=parent)
        ]

    def list_workflow_invocations(self, parent: str) -> list[dict]:
        return [
            cast(dict, type(i).to_dict(i))
            for i in self._client.list_workflow_invocations(parent=parent)
        ]

    # -- Get operations -------------------------------------------------------

    def get_release_config(self, name: str) -> dict:
        """Get a single release config by resource name."""
        result = self._client.get_release_config(name=name)
        return cast(dict, type(result).to_dict(result))

    # -- Query operations -----------------------------------------------------

    def query_compilation_result_actions(self, name: str) -> list[dict]:
        """Query actions from a compilation result."""
        request = dataform_v1.QueryCompilationResultActionsRequest(name=name)
        return [
            cast(dict, type(a).to_dict(a))
            for a in self._client.query_compilation_result_actions(request=request)
        ]

    def query_workflow_invocation_actions(self, name: str) -> list[dict]:
        """Query action-level run details for a workflow invocation."""
        request = dataform_v1.QueryWorkflowInvocationActionsRequest(name=name)
        return [
            cast(dict, type(a).to_dict(a))
            for a in self._client.query_workflow_invocation_actions(request=request)
        ]

    # -- Misc -----------------------------------------------------------------

    def test_connection(self, project_id: str, locations: list[str]) -> dict:
        for location in locations:
            parent = f"projects/{project_id}/locations/{location}"
            repos = list(self._client.list_repositories(parent=parent))
            logger.info(
                "Dataform test_connection: found %d repos in %s",
                len(repos),
                location,
            )
        return {"success": True}

    def _close_client(self) -> None:
        pass
