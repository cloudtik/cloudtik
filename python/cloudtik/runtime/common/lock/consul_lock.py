import time
from datetime import datetime
from typing import Optional, Tuple

from cloudtik.core._private.util.rest_api import rest_api_put_json
from cloudtik.runtime.common.lock.lock_base import Lock, LOCK_MAX_ATTEMPTS, LockAcquisitionException
from cloudtik.runtime.common.service_discovery.consul import CONSUL_HTTP_PORT, REST_ENDPOINT_URL_FORMAT, \
    CONSUL_CLIENT_ADDRESS, CONSUL_REQUEST_TIMEOUT

REST_ENDPOINT_SESSION = "/v1/session"
REST_ENDPOINT_SESSION_CREATE = REST_ENDPOINT_SESSION + "/create"
REST_ENDPOINT_SESSION_DESTROY = REST_ENDPOINT_SESSION + "/destroy"

REST_ENDPOINT_KV = "/v1/kv"

# the key will always be substituted into this pattern before locking,
# a good prefix is recommended for organization
FULL_KEY_PATTERN = 'cloudtik/locks/%s'


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


class ConsulLock(Lock):
    def __init__(self,
                 key,
                 lock_timeout_seconds=None,
                 acquire_timeout_ms=None):
        """
        :param key: the unique key to lock
        :param acquire_timeout_ms: how long the caller is willing to wait to acquire the lock
        :param lock_timeout_seconds: how long the lock will stay alive if it is never released,
            this is controlled by Consul's Session TTL and may stay alive a bit longer according
            to their docs. As of the current version of Consul, this must be between 10s and 86400s
        """
        super().__init__(key, acquire_timeout_ms, lock_timeout_seconds)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = None
        self._started_acquiring = False
        assert 10 <= self.lock_timeout_seconds <= 86400, \
            'lock_timeout_seconds must be between 10 and 86400 to due to Consul session ttl settings'

    def acquire(self, fail_hard=True):
        """
        Attempt to acquire the lock.

        :param fail_hard: when true, this method will only return gracefully
            if the lock has been acquired and will throw an exception if
            it cannot acquire the lock.

        :return: True if the lock was successfully acquired,
            false if it was not (unreachable if failing hard)
        """
        assert not self._started_acquiring, 'Can only lock once'
        start_time = time.time()

        # how long to hold locks after session times out.
        # we don't want to hold on to them since this is a temporary session just for this lock
        session_lock_delay = 0

        # how long to keep the session alive without a renew (heartbeat/keepalive) sent.
        # we are using this to time out the individual lock
        session_ttl = self.lock_timeout_seconds

        # delete locks when session is invalidated/destroyed
        session_invalidate_behavior = 'delete'

        session = create_session(
            lock_delay=session_lock_delay,
            ttl=session_ttl,
            behavior=session_invalidate_behavior
        )
        self.session_id = session["ID"]

        self._started_acquiring = True

        acquired = False
        max_attempts = LOCK_MAX_ATTEMPTS  # don't loop forever
        for attempt in range(0, max_attempts):
            acquired = self._acquire_key()

            # exponential backoff yo
            sleep_ms = 50 * pow(attempt, 2)
            elapsed_time_ms = int(round(1000 * (time.time() - start_time)))
            time_left_ms = self.acquire_timeout_ms - elapsed_time_ms
            sleep_ms = min(time_left_ms, sleep_ms)

            retry_acquire = (not acquired) and (time_left_ms > 0)
            if retry_acquire:
                sleep_seconds_float = sleep_ms / 1000.0
                time.sleep(sleep_seconds_float)
            else:
                break

        if not acquired and fail_hard:
            raise LockAcquisitionException("Failed to acquire %s" % self.full_key)
        else:
            return acquired

    def release(self):
        """
        Release the lock immediately. Does nothing if never locked.
        """
        if not self._started_acquiring:
            return False
        return self._release_key()

    def _acquire_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = dict(locked_at=str(datetime.now()))
        return acquire_key(
            session_id=self.session_id,
            key=self.full_key,
            data=data,
        )

    def _release_key(self):
        assert self.session_id, 'Must have a session id to release key'
        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Consul.
        return destroy_session(session_id=self.session_id)
