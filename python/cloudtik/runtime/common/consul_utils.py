from typing import Optional, Tuple
import urllib.error

from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_put_json, get_rest_endpoint_url

CONSUL_HTTP_PORT = 8500
CONSUL_REQUEST_TIMEOUT = 5
CONSUL_BLOCKING_QUERY_TIMEOUT = 10 * 60

CONSUL_REST_ENDPOINT_SESSION = "/v1/session"
CONSUL_REST_ENDPOINT_SESSION_CREATE = CONSUL_REST_ENDPOINT_SESSION + "/create"
CONSUL_REST_ENDPOINT_SESSION_DESTROY = CONSUL_REST_ENDPOINT_SESSION + "/destroy"
CONSUL_REST_ENDPOINT_SESSION_RENEW = CONSUL_REST_ENDPOINT_SESSION + "/renew"

CONSUL_REST_ENDPOINT_KV = "/v1/kv"


def consul_api_get(
        endpoint: str, address: Optional[Tuple[str, int]] = None,
        timeout=CONSUL_REQUEST_TIMEOUT):
    endpoint_url = get_rest_endpoint_url(
        endpoint, address, default_port=CONSUL_HTTP_PORT)
    return rest_api_get_json(endpoint_url, timeout=timeout)


def consul_api_put(
        endpoint: str, body, address: Optional[Tuple[str, int]] = None):
    endpoint_url = get_rest_endpoint_url(
        endpoint, address, default_port=CONSUL_HTTP_PORT)
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
    endpoint_url = CONSUL_REST_ENDPOINT_SESSION_CREATE
    data = {
        "LockDelay": f"{lock_delay}s",
        "Behavior": behavior,
    }
    if ttl:
        data["TTL"] = f"{ttl}s"
    return consul_api_put(endpoint_url, data)


def destroy_session(session_id):
    endpoint_url = "{}/{}".format(
        CONSUL_REST_ENDPOINT_SESSION_DESTROY, session_id)
    return consul_api_put(endpoint_url, body=None)


def renew_session(session_id):
    endpoint_url = "{}/{}".format(
        CONSUL_REST_ENDPOINT_SESSION_RENEW, session_id)
    return consul_api_put(endpoint_url, body=None)


"""
The acquire operation acts like a Check-And-Set operation except
 it can only succeed if there is no existing lock holder.
"""


def acquire_key(session_id, key, data):
    endpoint_url = "{}/{}?acquire={}".format(
        CONSUL_REST_ENDPOINT_KV, key, session_id)
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
        CONSUL_REST_ENDPOINT_KV, key, session_id)
    return consul_api_put(endpoint_url, body=None)


def get_key(key):
    try:
        endpoint_url = "{}/{}".format(
            CONSUL_REST_ENDPOINT_KV, key)
        return consul_api_get(endpoint_url)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        else:
            raise e


def query_key_blocking(key, index):
    try:
        # wait parameter specifying a maximum duration for the blocking request.
        # This is limited to 10 minutes. If not set, the wait time defaults to 5 minutes.
        endpoint_url = "{}/{}?index={}&wait=10m".format(
            CONSUL_REST_ENDPOINT_KV, key, index)
        return consul_api_get(
            endpoint_url, timeout=CONSUL_BLOCKING_QUERY_TIMEOUT)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        else:
            raise e
