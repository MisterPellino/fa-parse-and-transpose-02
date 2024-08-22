import azure.functions as func
from blueprints import http_parse_to_csv_fb

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(http_parse_to_csv_fb.bp)