import json
from abc import ABC
from datetime import timedelta
from typing import Optional, Union, Dict, Tuple, List


class BaseStorageClient(ABC):
    _GZIP_MAGIC_NUMBER = (
        b"\x1f\x8b"  # Hex signature used to identify a gzip compressed files
    )

    class GenericError(Exception):
        pass

    class PermissionsError(GenericError):
        pass

    class NotFoundError(GenericError):
        pass

    def write(self, key: str, obj_to_write) -> None:
        raise NotImplementedError()

    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        raise NotImplementedError()

    def delete(self, key: str) -> None:
        raise NotImplementedError()

    def download_file(self, key: str, download_path: str) -> None:
        raise NotImplementedError()

    def read_json(self, key: str) -> Dict:
        data = self.read(key)
        return json.loads(data.decode("utf-8"))

    def read_many_json(self, prefix: str) -> Dict:
        raise NotImplementedError()

    def managed_download(self, key: str, download_path: str):
        raise NotImplementedError()

    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,
        **kwargs,
    ) -> Tuple[Union[List, None], Union[str, None]]:
        raise NotImplementedError()

    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        raise NotImplementedError()

    def is_bucket_private(self) -> bool:
        raise NotImplementedError()

    def _is_gzip(self, content: bytes) -> bool:
        return content[:2] == self._GZIP_MAGIC_NUMBER
