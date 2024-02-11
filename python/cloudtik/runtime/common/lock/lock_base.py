import contextlib

# Lock time to live default to 10 minutes
DEFAULT_LOCK_TIMEOUT_SECONDS = 60 * 10

# Lock acquire wait default to lock time to live
DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS = 1000 * DEFAULT_LOCK_TIMEOUT_SECONDS

LOCK_MAX_ATTEMPTS = 1000


def _get_required(value, default):
    if value is not None:
        return value
    return default


class LockException(RuntimeError):
    pass


class LockAcquisitionException(LockException):
    pass


class Lock(object):
    def __init__(self,
                 key,
                 lock_timeout_seconds=None,
                 acquire_timeout_ms=None):
        assert key, 'Key is required for locking.'

        self.key = key
        self.lock_timeout_seconds = _get_required(
            lock_timeout_seconds, DEFAULT_LOCK_TIMEOUT_SECONDS)
        self.acquire_timeout_ms = _get_required(
            acquire_timeout_ms, DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS)

    def acquire(self, fail_hard=True):
        """
        Attempt to acquire the lock.

        :param fail_hard: when true, this method will only return gracefully
            if the lock has been acquired and will throw an exception if
            it cannot acquire the lock.

        :return: True if the lock was successfully acquired,
            false if it was not (unreachable if failing hard)
        """
        raise RuntimeError(
            "A lock implementation needs to override this.")

    def release(self):
        """
        Release the lock immediately. Does nothing if never locked.
        """
        raise RuntimeError(
            "A lock implementation needs to override this.")

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
