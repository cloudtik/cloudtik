import urllib.parse

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_CONSUL
from cloudtik.core._private.utils import is_runtime_enabled
from cloudtik.runtime.common.lock.consul_lock import ConsulLock
from cloudtik.runtime.common.lock.etcd_lock import EtcdLock
from cloudtik.runtime.common.lock.lock_base import Lock
from cloudtik.runtime.common.lock.redis_lock import RedisLock

DISTRIBUTED_SCHEMA_CONSUL = "consul"
DISTRIBUTED_SCHEMA_ETCD = "etcd"
DISTRIBUTED_SCHEMA_REDIS = "redis"

# This is the same as runtime service discovery settings for each runtime
ETCD_URI_KEY = "etcd_uri"


def _get_distributed_system_url(schema, endpoints=None):
    if not endpoints:
        return "{}:://".format(schema)
    else:
        return "{}:://{}".format(schema, endpoints)


# A factory method for creating a Lock based on runtime configuration
# If consul is configured, we create a consul lock.
# If consul is not configured, we use redis lock on the head redis instance.
def get_runtime_lock(
        url, lock_name,
        lock_timeout_seconds=None,
        acquire_timeout_ms=None) -> Lock:
    addr = urllib.parse.urlparse(url)
    schema = addr.scheme
    if schema == DISTRIBUTED_SCHEMA_CONSUL:
        return ConsulLock(
            lock_name,
            lock_timeout_seconds=lock_timeout_seconds,
            acquire_timeout_ms=acquire_timeout_ms)
    elif schema == DISTRIBUTED_SCHEMA_ETCD:
        endpoints = addr.netloc
        if not endpoints:
            raise ValueError(
                "Invalid distributed system URL. No endpoints specified.")
        return EtcdLock(
            endpoints,
            lock_name,
            lock_timeout_seconds=lock_timeout_seconds,
            acquire_timeout_ms=acquire_timeout_ms)
    elif schema == DISTRIBUTED_SCHEMA_REDIS:
        return RedisLock(
            lock_name,
            lock_timeout_seconds=lock_timeout_seconds,
            acquire_timeout_ms=acquire_timeout_ms)
    else:
        raise ValueError(
            "Unknown distributed system schema: {}".format(schema))


def get_runtime_lock_url(
        runtime_config, runtime_type):
    if is_runtime_enabled(
            runtime_config, BUILT_IN_RUNTIME_CONSUL):
        return _get_distributed_system_url(DISTRIBUTED_SCHEMA_CONSUL)

    if runtime_type:
        runtime_type_config = runtime_config.get(runtime_type, {})
        etcd_uri = runtime_type_config.get(ETCD_URI_KEY)
        if etcd_uri:
            return _get_distributed_system_url(
                DISTRIBUTED_SCHEMA_ETCD, etcd_uri)

    return _get_distributed_system_url(DISTRIBUTED_SCHEMA_REDIS)
