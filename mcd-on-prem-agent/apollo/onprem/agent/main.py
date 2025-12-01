import json
import logging
import os
import signal
import sys
from datetime import timedelta
from typing import Any, Optional, Tuple, Union, List, Dict

from flask import Flask
from flask import make_response
from flask import request

from apollo.agent.logging_utils import LoggingUtils
from apollo.common.integrations.storage.base_storage_client import BaseStorageClient
from apollo.egress.agent.config.config_manager import ConfigurationManager
from apollo.egress.agent.config.local_config import LocalConfig
from apollo.egress.agent.utils.utils import enable_tcp_keep_alive, init_logging, LOCAL
from apollo.integrations.s3.s3_reader_writer import S3ReaderWriter

init_logging()
logger = logging.getLogger(__name__)

from apollo.onprem.agent.service.on_prem_service import OnPremService

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVICE_PORT = os.getenv("SERVER_PORT") or "8081"


class EmptyStorageClient(BaseStorageClient):
    @property
    def bucket_name(self) -> str:
        return "dummy"

    def write(self, key: str, obj_to_write: Union[bytes, str]) -> None:
        pass

    def read(
        self,
        key: str,
        decompress: Optional[bool] = False,
        encoding: Optional[str] = None,
    ) -> Union[bytes, str]:
        return ""

    def delete(self, key: str) -> None:
        pass

    def download_file(self, key: str, download_path: str) -> None:
        pass

    def upload_file(self, key: str, local_file_path: str) -> None:
        pass

    def read_many_json(self, prefix: str) -> Dict:
        return {}

    def managed_download(self, key: str, download_path: str):
        pass

    def list_objects(
        self,
        prefix: Optional[str] = None,
        batch_size: Optional[int] = None,
        continuation_token: Optional[str] = None,
        delimiter: Optional[str] = None,
        *args,  # type: ignore
        **kwargs  # type: ignore
    ) -> Tuple[Union[List, None], Union[str, None]]:
        return [], None

    def generate_presigned_url(self, key: str, expiration: timedelta) -> str:
        return ""

    def is_bucket_private(self) -> bool:
        return False

    def __init__(self):
        super().__init__()


"""
This is the main entry point for the Agent service, it starts a Flask application
and the `OnPremService` that will handle the communication with the MC backend.
It defines a few HTTP endpoints that provides information about the service and its health.
"""

app = Flask(__name__)
logging_utils = LoggingUtils()
service = OnPremService(
    config_manager=ConfigurationManager(persistence=LocalConfig()),
    storage_client=EmptyStorageClient(),
    logging_utils=logging_utils,
)


def handler(signum: int, frame: Any):
    print("Signal handler called with signal", signum)
    service.stop()
    print("Signal handler completed")
    sys.exit(0)


signal.signal(signal.SIGINT, handler)


@app.get("/api/v1/test/healthcheck")
def health_check():
    """
    Used for readiness probe from the Snowflake platform.
    """
    return "OK"


@app.post("/api/v1/test/health")
def api_health():
    """
    Intended to be used from the Streamlit application, this gets called through a
    Snowflake function.
    """
    health_response = service.health_information()
    output_rows = [[0, json.dumps(health_response)]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


@app.get("/api/v1/test/health")
def health():
    """
    Intended to be used for local troubleshooting, not from the Streamlit application.
    """
    health_response = service.health_information(trace_id=request.args.get("trace_id"))
    response = make_response(health_response)
    response.headers["Content-type"] = "application/json"
    return response


@app.post("/api/v1/test/reachability")
def run_reachability_test():
    """
    Intended to be used from the Streamlit application, this gets called through a
    Snowflake function.
    """
    reachability_response = service.run_reachability_test()
    output_rows = [[0, json.dumps(reachability_response)]]
    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    return response


enable_tcp_keep_alive()
service.start()

if __name__ == "__main__":
    # only used for local development, when gunicorn is not used
    app.run(host=SERVICE_HOST, port=int(SERVICE_PORT))
