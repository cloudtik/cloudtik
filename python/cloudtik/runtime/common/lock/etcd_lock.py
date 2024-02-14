import time
from datetime import datetime

from cloudtik.runtime.common.etcd_utils import destroy_session, acquire_key, create_session, EtcdClient
from cloudtik.runtime.common.lock.lock_base import Lock, LOCK_MAX_ATTEMPTS, LockAcquisitionException

# the key will always be substituted into this pattern before locking,
# a good prefix is recommended for organization
FULL_KEY_PATTERN = 'cloudtik/locks/%s'

"""
Each lease has a minimum time-to-live (TTL) value specified by the application at grant time.
The lease’s actual TTL value is at least the minimum TTL and is chosen by the etcd cluster.
Once a lease’s TTL elapses, the lease expires and all attached keys are deleted.
"""


class EtcdLock(Lock):
    def __init__(
            self,
            endpoints,
            key,
            lock_timeout_seconds=None,
            acquire_timeout_ms=None):
        """
        Args:
        endpoints: The endpoints for the Etcd cluster
        key: the unique key to lock
        acquire_timeout_ms: how long the caller is willing to wait to acquire the lock
        lock_timeout_seconds: how long the lock will stay alive if it is never released,
            this is controlled by Etcd lease TTL and may stay alive a bit longer according
            to their docs.
        """
        super().__init__(key, acquire_timeout_ms, lock_timeout_seconds)
        self.client = EtcdClient(endpoints)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = None
        self._started_acquiring = False

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
        # how long to keep the session alive without a renew (heartbeat/keepalive) sent.
        # we are using this to time out the individual lock
        session_ttl = self.lock_timeout_seconds
        session = create_session(
            self.client,
            ttl=session_ttl)
        return session

    def _destroy_session(self):
        assert self.session_id, 'Must have a session id to destroy'
        return destroy_session(
            self.client, session_id=self.session_id)

    def _acquire_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = str(datetime.now())
        return acquire_key(
            self.client,
            session_id=self.session_id,
            key=self.full_key,
            value=data,
        )

    def _release_key(self):
        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Etcd.
        return self._destroy_session()
