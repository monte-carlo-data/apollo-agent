from apollo.common.agent.serde import decode_dictionary
from apollo.integrations.ccp.models import PipelineState, TransformStep
from apollo.integrations.ccp.transforms.base import Transform
from apollo.integrations.ccp.transforms.registry import TransformRegistry


class DecodeBytesTransform(Transform):
    """Decodes wire-encoded bytes values in raw credentials.

    Walks state.raw and converts any {"__type__": "bytes", "__data__": "<base64>"}
    sentinel dicts to Python bytes objects. A no-op on credentials that contain
    no encoded values.
    """

    def execute(self, step: TransformStep, state: PipelineState) -> None:
        state.raw = decode_dictionary(state.raw)


TransformRegistry.register("decode_bytes", DecodeBytesTransform)
