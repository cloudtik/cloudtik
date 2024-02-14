import base64
import contextlib
import json
import threading
import urllib
import urllib.request
import urllib.error
from typing import Union, List, Tuple

from cloudtik.core._private.util.core_utils import split_list, http_address_string, \
    service_address_from_string

REST_API_REQUEST_TIMEOUT = 10

REST_API_AUTH_TYPE = "type"
REST_API_AUTH_BASIC = "basic"
REST_API_AUTH_BASIC_USERNAME = "username"
REST_API_AUTH_BASIC_PASSWORD = "password"
REST_API_AUTH_BEARER = "bearer"
REST_API_AUTH_BEARER_TOKEN = "token"
REST_API_AUTH_API_KEY = "api-key"

REST_ENDPOINT_URL_FORMAT = "{}{}"


def rest_api_open(url_or_req, timeout=None):
    if timeout is None:
        timeout = REST_API_REQUEST_TIMEOUT
    return urllib.request.urlopen(
        url_or_req, timeout=timeout)


def rest_api_read(
        url_or_req, timeout=None, with_headers=False):
    if timeout is None:
        timeout = REST_API_REQUEST_TIMEOUT
    with contextlib.closing(
            urllib.request.urlopen(
                url_or_req, timeout=timeout)) as response:
        if with_headers:
            body = response.read()
            headers = response.info()
            return body, headers
        else:
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
        req.add_header(
            'Content-Type', 'application/{}; charset=utf-8'.format(
                data_format))


def rest_api_get(
        endpoint_url, auth=None, timeout=None, with_headers=False):
    # disable all proxy on 127.0.0.1
    proxy_support = urllib.request.ProxyHandler({"no": "127.0.0.1"})
    opener = urllib.request.build_opener(proxy_support)
    urllib.request.install_opener(opener)

    req = urllib.request.Request(endpoint_url)
    _add_auth_header(req, auth)
    return rest_api_read(
        req, timeout=timeout, with_headers=with_headers)


def rest_api_method(
        endpoint_url, data, data_format=None,
        method=None, auth=None, timeout=None, with_headers=False):
    data_in_bytes = data.encode(
        'utf-8') if data is not None else None  # needs to be bytes
    req = urllib.request.Request(
        endpoint_url, data=data_in_bytes, method=method)
    _add_content_type_header(req, data_format)
    _add_auth_header(req, auth)
    return rest_api_read(
        req, timeout=timeout, with_headers=with_headers)


def rest_api_method_open(
        endpoint_url, data, data_format=None,
        method=None, auth=None, timeout=None):
    data_in_bytes = data.encode(
        'utf-8') if data is not None else None  # needs to be bytes
    req = urllib.request.Request(
        endpoint_url, data=data_in_bytes, method=method)
    _add_content_type_header(req, data_format)
    _add_auth_header(req, auth)
    return rest_api_open(
        req, timeout=timeout)


def rest_api_post(
        endpoint_url, data, data_format=None,
        auth=None, timeout=None, with_headers=False):
    return rest_api_method(
        endpoint_url, data, data_format,
        auth=auth, timeout=timeout, with_headers=with_headers)


def rest_api_put(
        endpoint_url, data, data_format=None,
        auth=None, timeout=None, with_headers=False):
    return rest_api_method(
        endpoint_url, data, data_format,
        method="PUT", auth=auth, timeout=timeout, with_headers=with_headers)


def rest_api_delete(
        endpoint_url, auth=None, timeout=None, with_headers=False):
    req = urllib.request.Request(url=endpoint_url, method="DELETE")
    _add_auth_header(req, auth)
    return rest_api_read(
        req, timeout=timeout, with_headers=with_headers)


def _load_json(response, with_headers=False):
    if with_headers:
        body, headers = response
        return json.loads(body), headers
    else:
        return json.loads(response)


def rest_api_get_json(
        endpoint_url, auth=None, timeout=None, with_headers=False):
    response = rest_api_get(
        endpoint_url,
        auth=auth, timeout=timeout, with_headers=with_headers)
    return _load_json(response, with_headers=with_headers)


def rest_api_post_json(
        endpoint_url, body, auth=None, timeout=None, with_headers=False):
    data = json.dumps(body)
    response = rest_api_post(
        endpoint_url, data, "json",
        auth=auth, timeout=timeout, with_headers=with_headers)
    return _load_json(response, with_headers=with_headers)


def rest_api_put_json(
        endpoint_url, body, auth=None, timeout=None,
        with_headers=False):
    data = json.dumps(body)
    response = rest_api_put(
        endpoint_url, data, "json", auth=auth, timeout=timeout,
        with_headers=with_headers)
    return _load_json(response, with_headers=with_headers)


def rest_api_method_json(
        endpoint_url, body, method=None, auth=None, timeout=None,
        with_headers=False):
    data = json.dumps(body)
    response = rest_api_method(
        endpoint_url, data, "json", method=method, auth=auth,
        timeout=timeout, with_headers=with_headers)
    return _load_json(response, with_headers=with_headers)


def rest_api_delete_json(
        endpoint_url, auth=None, timeout=None, with_headers=False):
    response = rest_api_delete(
        endpoint_url, auth=auth, timeout=timeout,
        with_headers=with_headers)
    return _load_json(response, with_headers=with_headers)


def get_rest_endpoint_url(
        endpoint: str,
        address: Union[str, Tuple[str, int]] = None,
        default_port=None):
    if address:
        if isinstance(address, str):
            host, port = service_address_from_string(
                address, default_port=default_port)
        else:
            host, port = address
        http_address = http_address_string(host, port)
    else:
        http_address = http_address_string(
            "127.0.0.1", default_port)
    endpoint_url = REST_ENDPOINT_URL_FORMAT.format(
        http_address, endpoint)
    return endpoint_url


EndPointAddress = Union[str, List[str], Tuple[str, int], List[Tuple[str, int]]]


class MultiEndpointClient:
    def __init__(
            self,
            endpoints: EndPointAddress, default_port=None):
        # endpoints is a list or str
        if isinstance(endpoints, str):
            self.endpoints = split_list(endpoints)
        elif isinstance(endpoints, tuple):
            # single host and port tuple
            self.endpoints = [endpoints]
        else:
            # list string or tuple
            self.endpoints = endpoints
        self.num_endpoints = len(self.endpoints)
        self.default_port = default_port

        # make a shallow copy of the origin endpoints
        self.health_endpoints = self.endpoints.copy()
        self.failed_endpoints = set()
        self._lock = threading.Lock()

    def request(self, endpoint, func):
        # This method must be thread safe
        if self.num_endpoints == 1:
            endpoint_address = self.endpoints[0]
            endpoint_url = self._get_endpoint_url(endpoint, endpoint_address)
            return func(endpoint_url)
        else:
            return self._request_retry(endpoint, func)

    def _request_retry(self, endpoint, func):
        health_endpoints = self._get_health_endpoints()
        # iterate the health endpoints
        failed_endpoints = set()
        for endpoint_address in health_endpoints:
            endpoint_url = self._get_endpoint_url(endpoint, endpoint_address)
            try:
                return func(endpoint_url)
            except urllib.error.URLError:
                # the endpoint is not reachable
                failed_endpoints.add(endpoint_address)
                continue

        if failed_endpoints:
            self._update_health_endpoints(failed_endpoints)

        raise RuntimeError(
            "Connection failed with all the endpoints: {}.".format(
                self.endpoints))

    def _get_health_endpoints(self):
        with self._lock:
            return self.health_endpoints.copy()

    def _update_health_endpoints(self, failed_endpoints):
        with self._lock:
            health_endpoints = [
                endpoint_address for endpoint_address in self.health_endpoints
                if endpoint_address not in failed_endpoints]
            if not health_endpoints:
                # all endpoints failed, reset the list
                self.health_endpoints = self.endpoints.copy()
            else:
                self.health_endpoints = health_endpoints
            self.failed_endpoints.update(failed_endpoints)

    def _get_endpoint_url(
            self,
            endpoint: str,
            address: Union[str, Tuple[str, int]] = None):
        return get_rest_endpoint_url(
            endpoint, address, self.default_port)
