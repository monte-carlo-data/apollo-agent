import logging
import os

from apollo.interfaces.lambda_function.json_log_formatter import (
    ExtraLogger,
    JsonLogFormatter,
)
from apollo.interfaces.generic.log_context import BaseLogContext

# set the logger class before any other apollo code
logging.setLoggerClass(ExtraLogger)

from apig_wsgi import make_lambda_handler

from apollo.agent.env_vars import (
    DEBUG_ENV_VAR,
)
from apollo.interfaces.lambda_function.main import main
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider

log_context = BaseLogContext()
log_context.install()

formatter = JsonLogFormatter()
root_logger = logging.getLogger()
for h in root_logger.handlers:
    h.setFormatter(formatter)

is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
root_logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

app = main.app
main.agent.log_context = log_context
main.agent.platform_provider = AwsPlatformProvider()

lambda_handler = make_lambda_handler(app.wsgi_app, binary_support=True)
