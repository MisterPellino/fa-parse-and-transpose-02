import azure.functions as func
from blueprints import (
    http_parse_to_csv_fb,
    http_parse_to_csv_ff,
    http_parse_to_csv_analytical
)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(http_parse_to_csv_fb.bp)
app.register_functions(http_parse_to_csv_ff.bp)
app.register_functions(http_parse_to_csv_analytical.bp)
