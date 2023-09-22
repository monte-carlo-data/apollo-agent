import azure.functions as func

from apollo.interfaces.generic import main

app = func.WsgiFunctionApp(
    app=main.app.wsgi_app, http_auth_level=func.AuthLevel.FUNCTION
)
