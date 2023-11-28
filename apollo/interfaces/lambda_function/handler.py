import os

from apig_wsgi import make_lambda_handler

from apollo.agent.constants import PLATFORM_AWS
from apollo.agent.env_vars import (
    AGENT_WRAPPER_TYPE_ENV_VAR,
    WRAPPER_TYPE_CLOUDFORMATION,
)
from apollo.interfaces.generic import main
from apollo.interfaces.lambda_function.lambda_cf_updater import LambdaCFUpdater

app = main.app
main.agent.platform = PLATFORM_AWS

wrapper_type = os.getenv(AGENT_WRAPPER_TYPE_ENV_VAR)
if wrapper_type == WRAPPER_TYPE_CLOUDFORMATION:
    main.agent.updater = LambdaCFUpdater()

lambda_handler = make_lambda_handler(app.wsgi_app, binary_support=True)
