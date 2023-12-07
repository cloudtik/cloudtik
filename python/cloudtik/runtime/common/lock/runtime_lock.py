from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_CONSUL
from cloudtik.core._private.utils import is_runtime_enabled
from cloudtik.runtime.common.lock.consul_lock import ConsulLock
from cloudtik.runtime.common.lock.redis_lock import RedisLock


# A factory method for creating a Lock based on runtime configuration
# If consul is configured, we create a consul lock.
# If consul is not configured, we use redis lock on the head redis instance.
def get_runtime_lock(
        runtime_config, lock_name,
        lock_timeout_seconds=None,
        acquire_timeout_ms=None):
    if is_runtime_enabled(
            runtime_config, BUILT_IN_RUNTIME_CONSUL):
        return ConsulLock(
            lock_name,
            lock_timeout_seconds=lock_timeout_seconds,
            acquire_timeout_ms=acquire_timeout_ms)

    return RedisLock(
        lock_name,
        lock_timeout_seconds=lock_timeout_seconds,
        acquire_timeout_ms=acquire_timeout_ms)
