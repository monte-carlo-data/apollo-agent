from urllib.request import urlretrieve

from apollo.agent.utils import AgentUtils
from apollo.integrations.ctp.errors import CtpPipelineError
from apollo.integrations.ctp.models import PipelineState, TransformStep
from apollo.integrations.ctp.template import TemplateEngine
from apollo.integrations.ctp.transforms.base import Transform
from apollo.integrations.ctp.transforms.registry import TransformRegistry


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

    required_input_keys = ("url",)
    optional_input_keys = ("sub_folder", "mechanism")
    required_output_keys = ("path",)
    optional_output_keys = ()

    def _execute(self, step: TransformStep, state: PipelineState) -> None:
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
            platform = state.context.get("platform")
            if not platform:
                raise CtpPipelineError(
                    stage="transform_execute",
                    step_name=step.type,
                    message="'platform' is required in context for non-URL fetch mechanisms",
                )

            from apollo.integrations.storage.base_storage_client import (
                BaseStorageClient,
            )
            from apollo.integrations.storage.storage_proxy_client import (
                StorageProxyClient,
            )

            storage_client = StorageProxyClient(platform).wrapped_client
            try:
                storage_client.download_file(key=url, download_path=download_path)
            except BaseStorageClient.NotFoundError:
                return  # derived.path not set; downstream field_map renders to none

        state.derived[step.output["path"]] = download_path


TransformRegistry.register("fetch_remote_file", FetchRemoteFileTransform)
