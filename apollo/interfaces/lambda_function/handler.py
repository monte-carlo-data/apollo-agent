import logging
import os

from apollo.interfaces.lambda_function.json_log_formatter import (
    ExtraLogger,
)

# set the logger class before any other apollo code
logging.setLoggerClass(ExtraLogger)

from apig_wsgi import make_lambda_handler

from apollo.interfaces.aws.main import main
from apollo.interfaces.lambda_function.platform import AwsPlatformProvider

app = main.app
main.agent.platform_provider = AwsPlatformProvider()

lambda_handler = make_lambda_handler(app.wsgi_app, binary_support=True)
