import logging
import os

from apollo.interfaces.lambda_function.cf_platform import CFPlatformProvider
from apollo.interfaces.lambda_function.json_log_formatter import (
    ExtraLogger,
    JsonLogFormatter,
)
from apollo.interfaces.lambda_function.log_context import LambdaLogContext

# set the logger class before any other apollo code
logging.setLoggerClass(ExtraLogger)

from apig_wsgi import make_lambda_handler

from apollo.agent.constants import PLATFORM_AWS
from apollo.agent.env_vars import (
    AGENT_WRAPPER_TYPE_ENV_VAR,
    WRAPPER_TYPE_CLOUDFORMATION,
    DEBUG_ENV_VAR,
)
from apollo.interfaces.generic import main
from apollo.interfaces.lambda_function.cf_updater import LambdaCFUpdater

log_context = LambdaLogContext()
formatter = JsonLogFormatter()
root_logger = logging.getLogger()
for h in root_logger.handlers:
    h.setFormatter(formatter)
    h.addFilter(lambda record: log_context.filter(record))

is_debug = os.getenv(DEBUG_ENV_VAR, "false").lower() == "true"
root_logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

app = main.app
main.agent.log_context = log_context

wrapper_type = os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR)
if wrapper_type == WRAPPER_TYPE_CLOUDFORMATION:
    main.agent.platform_provider = CFPlatformProvider()

lambda_handler = make_lambda_handler(app.wsgi_app, binary_support=True)
