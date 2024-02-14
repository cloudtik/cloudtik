import contextlib
import json
import urllib.error

from cloudtik.core._private.util.core_utils import base64_encode_string
from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_post_json, rest_api_method_open, \
    MultiEndpointClient, EndPointAddress

ETCD_HTTP_PORT = 2379
ETCD_REQUEST_TIMEOUT = 5
ETCD_BLOCKING_QUERY_TIMEOUT = 60 * 60 * 24

ETCD_REST_ENDPOINT_SESSION = "/v3/lease"
ETCD_REST_ENDPOINT_SESSION_CREATE = ETCD_REST_ENDPOINT_SESSION + "/grant"
ETCD_REST_ENDPOINT_SESSION_DESTROY = ETCD_REST_ENDPOINT_SESSION + "/revoke"
ETCD_REST_ENDPOINT_SESSION_RENEW = ETCD_REST_ENDPOINT_SESSION + "/keepalive"

ETCD_REST_ENDPOINT_KV = "/v3/kv"
ETCD_REST_ENDPOINT_KV_GET = ETCD_REST_ENDPOINT_KV + "/range"
ETCD_REST_ENDPOINT_KV_PUT = ETCD_REST_ENDPOINT_KV + "/put"
ETCD_REST_ENDPOINT_KV_DELETE = ETCD_REST_ENDPOINT_KV + "/deleterange"
ETCD_REST_ENDPOINT_KV_TXN = ETCD_REST_ENDPOINT_KV + "/txn"

ETCD_REST_ENDPOINT_WATCH = "/v3/watch"


class EtcdClient(MultiEndpointClient):
    def __init__(
            self,
            endpoints: EndPointAddress):
        super().__init__(endpoints, default_port=ETCD_HTTP_PORT)


def etcd_api_get(
        client, endpoint: str,
        timeout=ETCD_REQUEST_TIMEOUT):
    def func(endpoint_url):
        return rest_api_get_json(endpoint_url, timeout=timeout)

    return client.request(endpoint, func)


def etcd_api_post(
        client, endpoint: str, body):
    def func(endpoint_url):
        return rest_api_post_json(
            endpoint_url, body, timeout=ETCD_REQUEST_TIMEOUT)

    return client.request(endpoint, func)


def etcd_api_watch(
        client, endpoint: str, body, key, revision):
    data = json.dumps(body)

    def func(endpoint_url):
        return etcd_watch(endpoint_url, data, key, revision)

    return client.request(endpoint, func)


def etcd_watch(endpoint_url, data, key, revision):
    response = rest_api_method_open(
        endpoint_url, data, "json",
        timeout=ETCD_BLOCKING_QUERY_TIMEOUT)

    with contextlib.closing(response):
        # read the first message of creation
        block = response.readline()
        if not block:
            return

        created_event = json.loads(block)
        if not created_event:
            return
        result = created_event.get("result", {})
        if not result.get("created"):
            return
        if _key_changed(result, key, revision):
            return

        while True:
            block = response.readline()
            if not block:
                break

            # we got the event
            events_stream = json.loads(block)
            if not events_stream:
                break
            result = events_stream.get("result", {})
            if _key_changed(result, key, revision):
                break
    # key changed or problem


def _get_event_kv_mod_revision(event, key):
    kv = event.get("kv")
    if not kv:
        return None
    if kv.get("key") != key:
        return None
    mod_revision_str = kv.get("mod_revision")
    if not mod_revision_str:
        return None
    return int(mod_revision_str)


def _key_changed(result, key, revision):
    if not result:
        return False
    events = result.get("events")
    if not events:
        return False
    for event in events:
        mod_revision = _get_event_kv_mod_revision(event, key)
        if mod_revision >= revision:
            return True
    return False


def create_session(client, ttl):
    endpoint_url = ETCD_REST_ENDPOINT_SESSION_CREATE
    data = {
        "TTL": f"{ttl}",
    }
    return etcd_api_post(client, endpoint_url, data)


def destroy_session(client, session_id):
    endpoint_url = ETCD_REST_ENDPOINT_SESSION_DESTROY
    data = {
        "ID": f"{session_id}",
    }
    try:
        return etcd_api_post(client, endpoint_url, data)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        else:
            raise e


def renew_session(client, session_id):
    endpoint_url = ETCD_REST_ENDPOINT_SESSION_RENEW
    data = {
        "ID": f"{session_id}",
    }
    return etcd_api_post(client, endpoint_url, data)


"""
The acquire operation acts like a Check-And-Set operation except
it can only succeed if I am the first one create the key with revision = 0
"""


def acquire_key(client, session_id, key, value):
    endpoint_url = ETCD_REST_ENDPOINT_KV_TXN
    base64_key = base64_encode_string(key)
    base64_value = base64_encode_string(value)
    req = {
        "compare": [
            {"result": "EQUAL", "target": "CREATE", "key": base64_key, "createRevision": "0"}
        ],
        "success": [
            {"requestPut": {"key": base64_key, "value": base64_value, "lease": session_id}}
        ],
        "failure": [
            {"requestRange": {"key": base64_key}}
        ]
    }
    resp = etcd_api_post(client, endpoint_url, body=req)
    if not resp:
        raise RuntimeError(
            "Error happened in requesting.")
    if resp.get("succeeded", False):
        # acquired the key
        return True
    return False


def release_key(client, session_id, key):
    endpoint_url = ETCD_REST_ENDPOINT_KV_TXN
    final_key = "{}{}".format(key, session_id)
    base64_key = base64_encode_string(final_key)
    # release when the lease is mine
    req = {
        "compare": [
            {"result": "EQUAL", "target": "LEASE", "key": base64_key, "lease": session_id}
        ],
        "success": [
            {"requestDeleteRange": {"key": base64_key}}
        ],
        "failure": [
        ]
    }
    resp = etcd_api_post(client, endpoint_url, body=req)
    if not resp:
        raise RuntimeError(
            "Error happened in requesting.")
    if resp.get("succeeded", False):
        # deleted the key
        return True
    return False


def get_key(client, key):
    endpoint_url = ETCD_REST_ENDPOINT_KV_GET
    base64_key = base64_encode_string(key)
    data = {
        "key": base64_key,
    }
    return etcd_api_post(client, endpoint_url, body=data)


def query_key_blocking(client, key, revision):
    endpoint_url = ETCD_REST_ENDPOINT_WATCH
    base64_key = base64_encode_string(key)
    start_revision = int(revision) + 1
    data = {
        "create_request": {
            "key": base64_key,
            "start_revision": f"{start_revision}"
        }
    }
    etcd_api_watch(
        client, endpoint_url, data, base64_key, start_revision)
