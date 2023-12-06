import base64
import json
import urllib
import urllib.request
import urllib.error

REST_API_REQUEST_TIMEOUT = 10

REST_API_AUTH_TYPE = "type"
REST_API_AUTH_BASIC = "basic"
REST_API_AUTH_BASIC_USERNAME = "username"
REST_API_AUTH_BASIC_PASSWORD = "password"
REST_API_AUTH_BEARER = "bearer"
REST_API_AUTH_BEARER_TOKEN = "token"
REST_API_AUTH_API_KEY = "api-key"


def rest_api_open(url_or_req, timeout=None):
    if timeout is None:
        timeout = REST_API_REQUEST_TIMEOUT
    response = urllib.request.urlopen(
        url_or_req, timeout=timeout)
    return response.read()


def _add_auth_header(req, auth):
    if auth:
        auth_type = auth[REST_API_AUTH_TYPE]
        if auth_type == REST_API_AUTH_BASIC:
            _add_basic_auth_header(
                req, auth[REST_API_AUTH_BASIC_USERNAME],
                auth[REST_API_AUTH_BASIC_PASSWORD])
        elif auth_type == REST_API_AUTH_BEARER:
            _add_bearer_auth_header(
                req, auth[REST_API_AUTH_BEARER_TOKEN])
        elif auth_type == REST_API_AUTH_API_KEY:
            _add_api_key_auth_header(
                req, auth[REST_API_AUTH_API_KEY])


def _add_basic_auth_header(req, username, password):
    basic_auth_string = base64.b64encode(
        f'{username}:{password}'.encode('utf-8')).decode('utf-8')
    req.add_header(
        "Authorization", f'Basic {basic_auth_string}')


def _add_bearer_auth_header(req, token):
    req.add_header(
        "Authorization", f'Bearer {token}')


def _add_api_key_auth_header(req, api_key):
    req.add_header(
        "X-API-KEY", f'{api_key}')


def _add_content_type_header(req, data_format):
    if data_format:
        req.add_header('Content-Type', 'application/{}; charset=utf-8'.format(
            data_format))


def rest_api_get(
        endpoint_url, auth=None, timeout=None):
    # disable all proxy on 127.0.0.1
    proxy_support = urllib.request.ProxyHandler({"no": "127.0.0.1"})
    opener = urllib.request.build_opener(proxy_support)
    urllib.request.install_opener(opener)

    req = urllib.request.Request(endpoint_url)
    _add_auth_header(req, auth)
    return rest_api_open(req, timeout=timeout)


def rest_api_method(
        endpoint_url, data, data_format=None,
        method=None, auth=None, timeout=None):
    data_in_bytes = data.encode(
        'utf-8') if data is not None else None  # needs to be bytes
    req = urllib.request.Request(
        endpoint_url, data=data_in_bytes, method=method)
    _add_content_type_header(req, data_format)
    _add_auth_header(req, auth)
    return rest_api_open(req, timeout=timeout)


def rest_api_post(
        endpoint_url, data, data_format=None,
        auth=None, timeout=None):
    return rest_api_method(
        endpoint_url, data, data_format,
        auth=auth, timeout=timeout)


def rest_api_put(
        endpoint_url, data, data_format=None,
        auth=None, timeout=None):
    return rest_api_method(
        endpoint_url, data, data_format,
        method="PUT", auth=auth, timeout=timeout)


def rest_api_delete(
        endpoint_url, auth=None, timeout=None):
    req = urllib.request.Request(url=endpoint_url, method="DELETE")
    _add_auth_header(req, auth)
    return rest_api_open(req, timeout=timeout)


def rest_api_get_json(
        endpoint_url, auth=None, timeout=None):
    response = rest_api_get(
        endpoint_url, auth=auth, timeout=timeout)
    return json.loads(response)


def rest_api_post_json(
        endpoint_url, body, auth=None, timeout=None):
    data = json.dumps(body)
    response = rest_api_post(
        endpoint_url, data, "json", auth=auth, timeout=timeout)
    return json.loads(response)


def rest_api_put_json(
        endpoint_url, body, auth=None, timeout=None):
    data = json.dumps(body)
    response = rest_api_put(
        endpoint_url, data, "json", auth=auth, timeout=timeout)
    return json.loads(response)


def rest_api_method_json(
        endpoint_url, body, method=None, auth=None, timeout=None):
    data = json.dumps(body)
    response = rest_api_method(
        endpoint_url, data, "json", method=method, auth=auth, timeout=timeout)
    return json.loads(response)


def rest_api_delete_json(
        endpoint_url, auth=None, timeout=None):
    response = rest_api_delete(
        endpoint_url, auth=auth, timeout=timeout)
    return json.loads(response)
