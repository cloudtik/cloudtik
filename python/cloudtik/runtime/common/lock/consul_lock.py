import contextlib
import time
from datetime import datetime
from typing import Optional, Tuple

from cloudtik.core._private.util.rest_api import rest_api_put_json, rest_api_put
from cloudtik.runtime.common.service_discovery.consul import CONSUL_HTTP_PORT, REST_ENDPOINT_URL_FORMAT, \
    CONSUL_CLIENT_ADDRESS, CONSUL_REQUEST_TIMEOUT

REST_ENDPOINT_SESSION = "/v1/session"
REST_ENDPOINT_SESSION_CREATE = REST_ENDPOINT_SESSION + "/create"
REST_ENDPOINT_SESSION_DESTROY = REST_ENDPOINT_SESSION + "/destroy"

REST_ENDPOINT_KV = "/v1/kv"

# default to wait for maximum 60 seconds
DEFAULT_ACQUIRE_TIMEOUT_MS = 1000 * 60
DEFAULT_LOCK_TIMEOUT_SECONDS = 60 * 3
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


def create_session(lock_delay, ttl, behavior="release"):
    endpoint_url = REST_ENDPOINT_SESSION_CREATE
    data = {
        "LockDelay": f"{lock_delay}s",
        "TTL": f"{ttl}s",
        "Behavior": behavior,
    }
    return consul_api_put(endpoint_url, data)


def destroy_session(session_id):
    endpoint_url = "{}/{}".format(
        REST_ENDPOINT_SESSION_DESTROY, session_id)
    return consul_api_put(endpoint_url, body=None)


def acquire_key(session_id, key, data):
    endpoint_url = "{}/{}?acquire={}".format(
        REST_ENDPOINT_KV, key, session_id)
    return consul_api_put(endpoint_url, body=data)


def release_key(session_id, key):
    endpoint_url = "{}/{}?release={}".format(
        REST_ENDPOINT_KV, key, session_id)
    return consul_api_put(endpoint_url, body=None)


class ConsulLockException(RuntimeError):
    pass


class LockAcquisitionException(ConsulLockException):
    pass


def _get_required(value, default):
    if value is not None:
        return value
    return default


class ConsulLock(object):
    def __init__(self,
                 key,
                 acquire_timeout_ms=None,
                 lock_timeout_seconds=None):
        """
        :param key: the unique key to lock
        :param acquire_timeout_ms: how long the caller is willing to wait to acquire the lock
        :param lock_timeout_seconds: how long the lock will stay alive if it is never released,
            this is controlled by Consul's Session TTL and may stay alive a bit longer according
            to their docs. As of the current version of Consul, this must be between 10s and 86400s
        """
        assert key, 'Key is required for locking.'
        self.key = key
        self.full_key = FULL_KEY_PATTERN % key
        self.lock_timeout_seconds = _get_required(
            lock_timeout_seconds, DEFAULT_LOCK_TIMEOUT_SECONDS)
        self.acquire_timeout_ms = _get_required(
            acquire_timeout_ms, DEFAULT_ACQUIRE_TIMEOUT_MS)
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
        max_attempts = 1000  # don't loop forever
        for attempt in range(0, max_attempts):
            acquired = self._acquire_consul_key()

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

    def _acquire_consul_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = dict(locked_at=str(datetime.now()))
        return acquire_key(
            session_id=self.session_id,
            key=self.full_key,
            data=data,
        )

    def release(self):
        """
        Release the lock immediately. Does nothing if never locked.
        """
        if not self._started_acquiring:
            return False

        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Consul.
        return destroy_session(session_id=self.session_id)

    @contextlib.contextmanager
    def hold(self):
        """
        Context manager for holding the lock
        """
        try:
            self.acquire(fail_hard=True)
            yield
        finally:
            self.release()
