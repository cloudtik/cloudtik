import time
from datetime import datetime

from cloudtik.runtime.common.consul_utils import destroy_session, acquire_key, create_session, ConsulClient
from cloudtik.runtime.common.lock.lock_base import Lock, LOCK_MAX_ATTEMPTS, LockAcquisitionException

# the key will always be substituted into this pattern before locking,
# a good prefix is recommended for organization
FULL_KEY_PATTERN = 'cloudtik/locks/%s'


class ConsulLock(Lock):
    def __init__(
            self,
            key,
            lock_timeout_seconds=None,
            acquire_timeout_ms=None,
            endpoints=None):
        """
        Args:
        key: the unique key to lock
        acquire_timeout_ms: how long the caller is willing to wait to acquire the lock
        lock_timeout_seconds: how long the lock will stay alive if it is never released,
            this is controlled by Consul's Session TTL and may stay alive a bit longer according
            to their docs. As of the current version of Consul, this must be between 10s and 86400s
        endpoints: The endpoints for the Consul cluster. None for connecting with local.
        """
        super().__init__(key, acquire_timeout_ms, lock_timeout_seconds)
        self.client = ConsulClient(endpoints)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = None
        self._started_acquiring = False
        assert 10 <= self.lock_timeout_seconds <= 86400, \
            'lock_timeout_seconds must be between 10 and 86400 to due to Consul session ttl settings'

    def acquire(self, fail_hard=True):
        """
        Attempt to acquire the lock.

        Args:
        fail_hard: when true, this method will only return gracefully
            if the lock has been acquired and will throw an exception if
            it cannot acquire the lock.

        return: True if the lock was successfully acquired,
            false if it was not (unreachable if failing hard)
        """
        assert not self._started_acquiring, 'Can only lock once'
        start_time = time.time()

        session = self._create_session()
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

    def _create_session(self):
        # how long to hold locks after session times out.
        # we don't want to hold on to them since this is a temporary session just for this lock
        session_lock_delay = 0

        # how long to keep the session alive without a renew (heartbeat/keepalive) sent.
        # we are using this to time out the individual lock
        session_ttl = self.lock_timeout_seconds

        # delete locks when session is invalidated/destroyed
        session_invalidate_behavior = 'delete'

        session = create_session(
            self.client,
            lock_delay=session_lock_delay,
            ttl=session_ttl,
            behavior=session_invalidate_behavior
        )
        return session

    def _destroy_session(self):
        assert self.session_id, 'Must have a session id to destroy'
        return destroy_session(
            self.client, session_id=self.session_id)

    def _acquire_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = dict(locked_at=str(datetime.now()))
        return acquire_key(
            self.client,
            session_id=self.session_id,
            key=self.full_key,
            data=data)

    def _release_key(self):
        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Consul.
        return self._destroy_session()
