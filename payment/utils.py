from sanic import response


def json_response(resp: dict, http_code: int) -> response.HTTPResponse:
    """Easier way to get a json response"""
    # escape_forward_slashes: https://github.com/huge-success/sanic/issues/1019
    return response.json(resp, status=http_code,
                         escape_forward_slashes=False)
