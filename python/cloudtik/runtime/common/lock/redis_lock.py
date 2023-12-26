import time
import uuid

from cloudtik.core._private.util.runtime_utils import get_redis_client
from cloudtik.runtime.common.lock.lock_base import Lock, LOCK_MAX_ATTEMPTS, LockAcquisitionException

# the key will always be substituted into this pattern before locking,
# a good prefix is recommended for organization
FULL_KEY_PATTERN = '@cloudtik:locks:%s'


"""
To acquire the lock, the way to go is the following:
SET resource_name my_random_value NX PX 30000
"""


def acquire_lock_key(redis_client, key, session_id, ttl):
    try:
        return redis_client.set(
            key.encode(), session_id.encode(), nx=True, ex=ttl)
    except Exception:
        return False


"""
Basically the random value is used in order to release the lock in a safe way,
with a script that tells Redis: remove the key only if it exists and the value
stored at the key is exactly the one I expect to be. 

if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return 0
end

This is important in order to avoid removing a lock that was created by another
client. For example a client may acquire the lock, get blocked performing some
operation for longer than the lock validity time (the time at which the key will
expire), and later remove the lock, that was already acquired by some other client.
Using just DEL is not safe as a client may remove another client's lock.
"""


def release_lock_key(redis_client, key, session_id):
    key = key.encode()
    try:
        value = redis_client.get(key)
        if value is None:
            return False
        old_session_id = value.decode()
        if old_session_id == session_id:
            # still hold the lock, release it
            if redis_client.delete(key) == 1:
                return True
        return False
    except Exception:
        return False


class RedisLock(Lock):
    def __init__(self,
                 key,
                 lock_timeout_seconds=None,
                 acquire_timeout_ms=None):
        """
        Args:
        key: the unique key to lock
        acquire_timeout_ms: how long the caller is willing to wait to acquire the lock
        lock_timeout_seconds: how long the lock will stay alive if it is never released,
            this is controlled by redis SET EX/PX
        """
        super().__init__(key, lock_timeout_seconds, acquire_timeout_ms)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = str(uuid.uuid4())
        self._started_acquiring = False
        self.redis_client = get_redis_client()

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
        return acquire_lock_key(
            self.redis_client,
            key=self.full_key,
            session_id=self.session_id,
            ttl=self.lock_timeout_seconds,
        )

    def _release_key(self):
        assert self.session_id, 'Must have a session id to release key'
        return release_lock_key(
            self.redis_client,
            key=self.full_key,
            session_id=self.session_id,
        )
