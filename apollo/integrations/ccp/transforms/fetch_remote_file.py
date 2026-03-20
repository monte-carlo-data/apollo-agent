from urllib.request import urlretrieve

from apollo.agent.utils import AgentUtils
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.template import TemplateEngine
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry


class FetchRemoteFileTransform(Transform):
    """Download a file from a remote URL or storage bucket to a local temp path.

    Input keys:
        url:        template resolving to a URL or storage key
        sub_folder: (optional) subdirectory hint for temp file placement
        mechanism:  (optional) "url" (default) or a storage retrieval mechanism

    Output keys:
        path: key in derived where the local file path is stored

    Context:
        platform: agent platform string — required when mechanism != "url"
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        url = TemplateEngine.render(step.input["url"], state)
        sub_folder = step.input.get("sub_folder")
        raw_mechanism = step.input.get("mechanism", "url")
        mechanism = (
            TemplateEngine.render(raw_mechanism, state)
            if "{{" in str(raw_mechanism)
            else raw_mechanism
        )

        download_path = AgentUtils.temp_file_path(sub_folder)

        if mechanism == "url":
            urlretrieve(url=url, filename=download_path)
        else:
            from apollo.integrations.storage.base_storage_client import (
                BaseStorageClient,
            )
            from apollo.integrations.storage.storage_proxy_client import (
                StorageProxyClient,
            )

            platform = state.context.get("platform")
            storage_client = StorageProxyClient(platform).wrapped_client
            try:
                storage_client.download_file(key=url, download_path=download_path)
            except BaseStorageClient.NotFoundError:
                return  # derived.path not set; downstream field_map renders to none

        state.derived[step.output["path"]] = download_path


TransformRegistry.register("fetch_remote_file", FetchRemoteFileTransform)
