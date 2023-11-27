from apig_wsgi import make_lambda_handler

from apollo.agent.constants import PLATFORM_AWS
from apollo.interfaces.generic import main
from apollo.interfaces.lambda_function.lambda_cf_updater import LambdaCFUpdater

app = main.app
main.agent.platform = PLATFORM_AWS
main.agent.updater = LambdaCFUpdater()
lambda_handler = make_lambda_handler(app.wsgi_app, binary_support=True)
