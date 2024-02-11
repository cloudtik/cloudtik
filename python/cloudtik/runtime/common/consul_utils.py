from typing import Optional, Tuple
import urllib.error

from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_put_json

CONSUL_CLIENT_ADDRESS = "127.0.0.1"
CONSUL_HTTP_PORT = 8500
CONSUL_REQUEST_TIMEOUT = 5

REST_ENDPOINT_URL_FORMAT = "http://{}:{}{}"

REST_ENDPOINT_SESSION = "/v1/session"
REST_ENDPOINT_SESSION_CREATE = REST_ENDPOINT_SESSION + "/create"
REST_ENDPOINT_SESSION_DESTROY = REST_ENDPOINT_SESSION + "/destroy"
REST_ENDPOINT_SESSION_RENEW = REST_ENDPOINT_SESSION + "/renew"

REST_ENDPOINT_KV = "/v1/kv"

def consul_api_get(
        endpoint: str, address: Optional[Tuple[str, int]] = None):
    if address:
        host, _ = address
        endpoint_url = REST_ENDPOINT_URL_FORMAT.format(
            host, CONSUL_HTTP_PORT, endpoint)
    else:
        endpoint_url = REST_ENDPOINT_URL_FORMAT.format(
            CONSUL_CLIENT_ADDRESS, CONSUL_HTTP_PORT, endpoint)
    return rest_api_get_json(endpoint_url, timeout=CONSUL_REQUEST_TIMEOUT)


def consul_api_put(
        endpoint: str, body, address: Optional[Tuple[str, int]] = None):
    if address:
        host, _ = address
        endpoint_url = REST_ENDPOINT_URL_FORMAT.format(
            host, CONSUL_HTTP_PORT, endpoint)
    else:
        endpoint_url = REST_ENDPOINT_URL_FORMAT.format(
            CONSUL_CLIENT_ADDRESS, CONSUL_HTTP_PORT, endpoint)
    return rest_api_put_json(
        endpoint_url, body, timeout=CONSUL_REQUEST_TIMEOUT)


"""
The contract that Consul provides is that under any of the following situations,
the session will be invalidated:

Node is deregistered
Any of the health checks are deregistered
Any of the health checks go to the critical state
Session is explicitly destroyed
TTL expires, if applicable
When a session is invalidated, it is destroyed and can no longer be used.
What happens to the associated locks depends on the behavior specified at
creation time. Consul supports a release and delete behavior. The release
behavior is the default if none is specified.

If the delete behavior is used, the key corresponding to any of the held
locks is simply deleted. This can be used to create ephemeral entries that
are automatically deleted by Consul.
"""


def create_session(lock_delay, ttl, behavior="release"):
    endpoint_url = REST_ENDPOINT_SESSION_CREATE
    data = {
        "LockDelay": f"{lock_delay}s",
        "Behavior": behavior,
    }
    if ttl:
        data["TTL"] = f"{ttl}s"
    return consul_api_put(endpoint_url, data)


def destroy_session(session_id):
    endpoint_url = "{}/{}".format(
        REST_ENDPOINT_SESSION_DESTROY, session_id)
    return consul_api_put(endpoint_url, body=None)


def renew_session(session_id):
    endpoint_url = "{}/{}".format(
        REST_ENDPOINT_SESSION_RENEW, session_id)
    return consul_api_put(endpoint_url, body=None)


"""
The acquire operation acts like a Check-And-Set operation except
 it can only succeed if there is no existing lock holder.
"""


def acquire_key(session_id, key, data):
    endpoint_url = "{}/{}?acquire={}".format(
        REST_ENDPOINT_KV, key, session_id)
    return consul_api_put(endpoint_url, body=data)


"""
Once held, the lock can be released using a corresponding release operation,
providing the same session. Again, this acts like a Check-And-Set operation
since the request will fail if given an invalid session. A critical note is
that the lock can be released without being the creator of the session. This
is by design as it allows operators to intervene and force-terminate a session
if necessary.
"""


def release_key(session_id, key):
    endpoint_url = "{}/{}?release={}".format(
        REST_ENDPOINT_KV, key, session_id)
    return consul_api_put(endpoint_url, body=None)


def get_key_meta(key):
    try:
        endpoint_url = "{}/{}".format(
            REST_ENDPOINT_KV, key)
        return consul_api_get(endpoint_url)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        else:
            raise e


def query_key_blocking(key, index):
    try:
        endpoint_url = "{}/{}?index=".format(
            REST_ENDPOINT_KV, key, index)
        return consul_api_get(endpoint_url)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        else:
            raise e
