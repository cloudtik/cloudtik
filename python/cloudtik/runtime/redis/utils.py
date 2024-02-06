import os
from typing import Any, Dict, Optional

from cloudtik.core._private.constants import CLOUDTIK_NODE_TYPE_WORKER_DEFAULT
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_REDIS
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_KEY_VALUE, define_runtime_service_on_head, \
    define_runtime_service_on_worker
from cloudtik.core._private.util.core_utils import get_config_for_update, address_string
from cloudtik.core._private.utils import _sum_min_workers, get_runtime_config_for_update, get_cluster_name
from cloudtik.runtime.common.health_check import HEALTH_CHECK_NODE_KIND_HEAD, HEALTH_CHECK_NODE_KIND_NODE, \
    HEALTH_CHECK_PORT, HEALTH_CHECK_NODE_KIND

REDIS_SERVICE_PORT_CONFIG_KEY = "port"

REDIS_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
REDIS_CLUSTER_MODE_NONE = "none"
# simple cluster
REDIS_CLUSTER_MODE_SIMPLE = "simple"
# replication
REDIS_CLUSTER_MODE_REPLICATION = "replication"
# sharding cluster
REDIS_CLUSTER_MODE_SHARDING = "sharding"

REDIS_PASSWORD_CONFIG_KEY = "password"

REDIS_REPLICATION_CONFIG_KEY = "replication"
REDIS_SENTINEL_CONFIG_KEY = "sentinel"
REDIS_SENTINEL_PORT_CONFIG_KEY = "sentinel_port"
REDIS_SENTINEL_QUORUM_CONFIG_KEY = "sentinel_quorum"

REDIS_SHARDING_CONFIG_KEY = "sharding"
REDIS_CLUSTER_PORT_CONFIG_KEY = "cluster_port"
REDIS_MASTER_SIZE_CONFIG_KEY = "master_size"
REDIS_ROLE_BY_NODE_TYPE_CONFIG_KEY = "role_by_node_type"
REDIS_RESHARD_DELAY_CONFIG_KEY = "reshard_delay"
REDIS_MASTER_NODE_TYPE_CONFIG_KEY = "master_node_type"

REDIS_HEALTH_CHECK_PORT_CONFIG_KEY = "health_check_port"

REDIS_SERVICE_TYPE = BUILT_IN_RUNTIME_REDIS
REDIS_SECONDARY_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-secondary"
REDIS_NODE_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-node"
REDIS_SENTINEL_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-sentinel"

REDIS_SERVICE_PORT_DEFAULT = 6379

REDIS_PASSWORD_DEFAULT = "cloudtik"
REDIS_RESHARD_DELAY_DEFAULT = 5


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_REDIS, {})


def _get_service_port(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_SERVICE_PORT_CONFIG_KEY, REDIS_SERVICE_PORT_DEFAULT)


def _get_cluster_mode(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_MODE_CONFIG_KEY, REDIS_CLUSTER_MODE_REPLICATION)


def _get_replication_config(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_REPLICATION_CONFIG_KEY, {})


def _is_sentinel_enabled(replication_config: Dict[str, Any]):
    return replication_config.get(
        REDIS_SENTINEL_CONFIG_KEY, True)


def _get_sentinel_port(replication_config: Dict[str, Any], service_port):
    return replication_config.get(
        REDIS_SENTINEL_PORT_CONFIG_KEY, service_port + 20000)


def _get_sentinel_quorum(replication_config: Dict[str, Any]):
    return replication_config.get(
        REDIS_SENTINEL_QUORUM_CONFIG_KEY, 2)


def _get_sharding_config(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_SHARDING_CONFIG_KEY, {})


def _get_cluster_port(sharding_config: Dict[str, Any], service_port):
    return sharding_config.get(
        REDIS_CLUSTER_PORT_CONFIG_KEY, service_port + 10000)


def _get_master_size(sharding_config: Dict[str, Any]):
    return sharding_config.get(
        REDIS_MASTER_SIZE_CONFIG_KEY)


def _is_role_by_node_type(sharding_config: Dict[str, Any]):
    return sharding_config.get(
        REDIS_ROLE_BY_NODE_TYPE_CONFIG_KEY, True)


def _get_reshard_delay(sharding_config: Dict[str, Any]):
    return sharding_config.get(
        REDIS_RESHARD_DELAY_CONFIG_KEY, REDIS_RESHARD_DELAY_DEFAULT)


def _get_master_node_type(sharding_config):
    return sharding_config.get(REDIS_MASTER_NODE_TYPE_CONFIG_KEY)


def _get_health_check_port(redis_config: Dict[str, Any]):
    default_port = _get_service_port(redis_config) + 30000
    return redis_config.get(
        REDIS_HEALTH_CHECK_PORT_CONFIG_KEY, default_port)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_REDIS)


def _get_runtime_processes():
    home_dir = _get_home_dir()
    redis_server_pidfile = os.path.join(
        home_dir, "redis-server.pid")
    redis_sentinel_pidfile = os.path.join(
        home_dir, "redis-sentinel.pid")
    runtime_processes = [
        [redis_server_pidfile, None, "Redis Server", "node"],
        [redis_sentinel_pidfile, None, "Redis Sentinel", "node"],
    ]
    return runtime_processes


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_REDIS: logs_dir}


def update_master_size(cluster_config, master_size):
    runtime_config_to_update = get_runtime_config_for_update(cluster_config)
    redis_config_to_update = get_config_for_update(
        runtime_config_to_update, BUILT_IN_RUNTIME_REDIS)
    sharding_config_to_update = get_config_for_update(
        redis_config_to_update, REDIS_SHARDING_CONFIG_KEY)
    sharding_config_to_update[REDIS_MASTER_SIZE_CONFIG_KEY] = master_size


def update_master_node_type(cluster_config, master_node_type):
    runtime_config_to_update = get_runtime_config_for_update(cluster_config)
    redis_config_to_update = get_config_for_update(
        runtime_config_to_update, BUILT_IN_RUNTIME_REDIS)
    sharding_config_to_update = get_config_for_update(
        redis_config_to_update, REDIS_SHARDING_CONFIG_KEY)
    sharding_config_to_update[REDIS_MASTER_NODE_TYPE_CONFIG_KEY] = master_node_type


def _check_master_node_type(cluster_config, master_node_type):
    available_node_types = cluster_config["available_node_types"]
    head_node_type = cluster_config["head_node_type"]
    worker_node_types = set()
    for node_type in available_node_types:
        if node_type != head_node_type:
            worker_node_types.add(node_type)

    if master_node_type:
        if master_node_type in worker_node_types:
            return master_node_type
        else:
            raise RuntimeError(
                "Node type {} is not defined.".format(master_node_type))

    # this configuration need extra worker node types
    if len(worker_node_types) <= 1:
        return None
    master_node_type = CLOUDTIK_NODE_TYPE_WORKER_DEFAULT
    if master_node_type not in worker_node_types:
        raise RuntimeError(
            "Default worker type {} is not defined for role by node type.".format(
                master_node_type))
    return master_node_type


def _configure_master_size(sharding_config, cluster_config):
    num_static_nodes = _sum_min_workers(cluster_config) + 1

    # WARNING: the static nodes when starting the cluster will
    # limit the number of masters.
    user_master_size = _get_master_size(sharding_config)
    master_size = user_master_size
    if not master_size:
        # for sharding, decide the number of masters if not specified
        if num_static_nodes <= 5:
            master_size = num_static_nodes
        else:
            master_size = num_static_nodes // 2
    else:
        if master_size > num_static_nodes:
            master_size = num_static_nodes
    if master_size != user_master_size:
        update_master_size(cluster_config, master_size)


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    redis_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode != REDIS_CLUSTER_MODE_NONE:
        if cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
            sharding_config = _get_sharding_config(redis_config)
            # check whether we need to use role by node type or based on master size
            if _is_role_by_node_type(sharding_config):
                master_node_type = _get_master_node_type(sharding_config)
                master_node_type = _check_master_node_type(
                    cluster_config, master_node_type)
                if master_node_type:
                    update_master_node_type(cluster_config, master_node_type)
                else:
                    _configure_master_size(sharding_config, cluster_config)
            else:
                _configure_master_size(sharding_config, cluster_config)
    return cluster_config


def _validate_config(config: Dict[str, Any]):
    pass


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    redis_config = _get_config(runtime_config)
    service_port = _get_service_port(redis_config)
    runtime_envs["REDIS_SERVICE_PORT"] = service_port
    cluster_mode = _get_cluster_mode(redis_config)
    runtime_envs["REDIS_CLUSTER_MODE"] = cluster_mode

    if cluster_mode == REDIS_CLUSTER_MODE_REPLICATION:
        replication_config = _get_replication_config(redis_config)
        sentinel_enabled = _is_sentinel_enabled(replication_config)
        runtime_envs["REDIS_SENTINEL_ENABLED"] = sentinel_enabled
        if sentinel_enabled:
            sentinel_port = _get_sentinel_port(replication_config, service_port)
            runtime_envs["REDIS_SENTINEL_PORT"] = sentinel_port
            sentinel_quorum = _get_sentinel_quorum(replication_config)
            runtime_envs["REDIS_SENTINEL_QUORUM"] = sentinel_quorum
    elif cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
        sharding_config = _get_sharding_config(redis_config)
        cluster_port = _get_cluster_port(sharding_config, service_port)
        runtime_envs["REDIS_CLUSTER_PORT"] = cluster_port

        master_node_type = _get_master_node_type(sharding_config)
        if master_node_type:
            runtime_envs["REDIS_MASTER_NODE_TYPE"] = master_node_type
        else:
            master_size = _get_master_size(sharding_config)
            if not master_size:
                # This just for safety, master size will be checked at bootstrap
                master_size = 1
            runtime_envs["REDIS_MASTER_SIZE"] = master_size

    password = redis_config.get(
        REDIS_PASSWORD_CONFIG_KEY, REDIS_PASSWORD_DEFAULT)
    runtime_envs["REDIS_PASSWORD"] = password

    return runtime_envs


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "redis": {
            "name": "Redis",
            "url": address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "redis": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    redis_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(redis_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, REDIS_SERVICE_TYPE)
    service_port = _get_service_port(redis_config)

    def define_redis_service(define_fn, service_type=None):
        if not service_type:
            service_type = REDIS_SERVICE_TYPE
        return define_fn(
            service_type,
            service_discovery_config, service_port,
            features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE])

    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode == REDIS_CLUSTER_MODE_REPLICATION:
        replication_config = _get_replication_config(redis_config)
        sentinel_enabled = _is_sentinel_enabled(replication_config)
        if sentinel_enabled:
            # every node is possible be primary and secondary
            sentinel_service_name = get_canonical_service_name(
                service_discovery_config, cluster_name, REDIS_SENTINEL_SERVICE_TYPE)
            sentinel_port = _get_sentinel_port(replication_config, service_port)
            services = {
                service_name: define_redis_service(
                    define_runtime_service, REDIS_NODE_SERVICE_TYPE),
                sentinel_service_name: define_runtime_service(
                    REDIS_SENTINEL_SERVICE_TYPE, service_discovery_config,
                    sentinel_port),
            }
        else:
            # primary service on head and secondary service on workers
            secondary_service_name = get_canonical_service_name(
                service_discovery_config, cluster_name, REDIS_SECONDARY_SERVICE_TYPE)
            services = {
                service_name: define_redis_service(define_runtime_service_on_head),
                secondary_service_name: define_redis_service(
                    define_runtime_service_on_worker, REDIS_SECONDARY_SERVICE_TYPE),
            }
    elif cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
        # Service register for each node but don't give key-value feature to avoid
        # these service been discovered.
        # TODO: Ideally a middle layer needs to expose a client discoverable service.
        services = {
            service_name: define_redis_service(define_runtime_service),
        }
    elif cluster_mode == REDIS_CLUSTER_MODE_SIMPLE:
        services = {
            service_name: define_redis_service(define_runtime_service),
        }
    else:
        # single standalone on head
        services = {
            service_name: define_redis_service(define_runtime_service_on_head),
        }
    return services


def _get_health_check(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    mysql_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(mysql_config)
    health_check_port = _get_health_check_port(mysql_config)

    node_kind = HEALTH_CHECK_NODE_KIND_HEAD
    if cluster_mode != REDIS_CLUSTER_MODE_NONE:
        node_kind = HEALTH_CHECK_NODE_KIND_NODE

    health_check = {
        HEALTH_CHECK_PORT: health_check_port,
        HEALTH_CHECK_NODE_KIND: node_kind,
    }
    return health_check
