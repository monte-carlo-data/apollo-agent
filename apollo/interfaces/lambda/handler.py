from apig_wsgi import make_lambda_handler

from apollo.interfaces.generic import main

app = main.app
main.agent.platform = "AWS"

lambda_handler = make_lambda_handler(app.wsgi_app)
