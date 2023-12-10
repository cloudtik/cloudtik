import os
from typing import Any, Dict

from cloudtik.core._private.core_utils import get_config_for_update
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_REDIS
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_KEY_VALUE, define_runtime_service_on_head, \
    define_runtime_service_on_worker
from cloudtik.core._private.utils import is_node_seq_id_enabled, enable_node_seq_id, \
    _sum_min_workers, get_runtime_config_for_update

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["redis-server", True, "Redis Server", "node"],
    ]

REDIS_SERVICE_PORT_CONFIG_KEY = "port"
REDIS_CLUSTER_PORT_CONFIG_KEY = "cluster_port"
REDIS_MASTER_SIZE_CONFIG_KEY = "master_size"
REDIS_RESHARD_DELAY_CONFIG_KEY = "reshard_delay"

REDIS_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
REDIS_CLUSTER_MODE_NONE = "none"
# simple cluster
REDIS_CLUSTER_MODE_SIMPLE = "simple"
# replication
REDIS_CLUSTER_MODE_REPLICATION = "replication"
# sharding cluster
REDIS_CLUSTER_MODE_SHARDING = "sharding"

REDIS_PASSWORD_CONFIG_KEY = "password"

REDIS_SERVICE_TYPE = BUILT_IN_RUNTIME_REDIS
REDIS_REPLICA_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-replica"
REDIS_SERVICE_PORT_DEFAULT = 6379

REDIS_PASSWORD_DEFAULT = "cloudtik"
REDIS_RESHARD_DELAY_DEFAULT = 5


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_REDIS, {})


def _get_service_port(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_SERVICE_PORT_CONFIG_KEY, REDIS_SERVICE_PORT_DEFAULT)


def _get_cluster_port(redis_config: Dict[str, Any]):
    service_port = _get_service_port(redis_config)
    return redis_config.get(
        REDIS_CLUSTER_PORT_CONFIG_KEY, service_port + 10000)


def _get_cluster_mode(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_MODE_CONFIG_KEY, REDIS_CLUSTER_MODE_REPLICATION)


def _get_master_size(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_MASTER_SIZE_CONFIG_KEY)


def _get_reshard_delay(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_RESHARD_DELAY_CONFIG_KEY, REDIS_RESHARD_DELAY_DEFAULT)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_REDIS)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"redis": logs_dir}


def update_config_master_size(cluster_config, master_size):
    runtime_config_to_update = get_runtime_config_for_update(cluster_config)
    redis_config_to_update = get_config_for_update(
        runtime_config_to_update, BUILT_IN_RUNTIME_REDIS)
    redis_config_to_update[REDIS_MASTER_SIZE_CONFIG_KEY] = master_size


def _configure_master_size(redis_config, cluster_config):
    num_static_nodes = _sum_min_workers(cluster_config) + 1
    if num_static_nodes < 3:
        raise RuntimeError("Redis Cluster for sharding requires at least 3 master nodes.")

    # WARNING: the static nodes when starting the cluster will
    # limit the number of masters.
    user_master_size = _get_master_size(redis_config)
    master_size = user_master_size
    if not master_size:
        # for sharding, decide the number of masters if not specified
        if num_static_nodes <= 5:
            master_size = num_static_nodes
        else:
            master_size = num_static_nodes // 2
    else:
        if master_size < 3:
            master_size = 3
        elif master_size > num_static_nodes:
            master_size = num_static_nodes
    if master_size != user_master_size:
        update_config_master_size(cluster_config, master_size)


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    redis_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode != REDIS_CLUSTER_MODE_NONE:
        # We must enable the node seq id (stable seq id is preferred)
        # But we don't enforce it.
        if not is_node_seq_id_enabled(cluster_config):
            enable_node_seq_id(cluster_config)

        if cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
            _configure_master_size(redis_config, cluster_config)

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

    if cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
        cluster_port = _get_cluster_port(redis_config)
        runtime_envs["REDIS_CLUSTER_PORT"] = cluster_port

        master_size = _get_master_size(redis_config)
        if not master_size:
            # This just for safety, master size will be checked at bootstrap
            master_size = 1
        runtime_envs["REDIS_MASTER_SIZE"] = master_size

    password = redis_config.get(
        REDIS_PASSWORD_CONFIG_KEY, REDIS_PASSWORD_DEFAULT)
    runtime_envs["REDIS_PASSWORD"] = password

    return runtime_envs


def _get_runtime_endpoints(runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "redis": {
            "name": "Redis",
            "url": "{}:{}".format(cluster_head_ip, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "redis": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    redis_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(redis_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, REDIS_SERVICE_TYPE)
    service_port = _get_service_port(redis_config)

    def define_redis_service(define_fn, service_type=None):
        if not service_type:
            service_type = REDIS_SERVICE_TYPE
        define_fn(
            service_type,
            service_discovery_config, service_port,
            features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE])

    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode == REDIS_CLUSTER_MODE_REPLICATION:
        # primary service on head and replica service on workers
        replica_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, REDIS_REPLICA_SERVICE_TYPE)
        services = {
            service_name: define_redis_service(define_runtime_service_on_head),
            replica_service_name: define_redis_service(
                define_runtime_service_on_worker, REDIS_REPLICA_SERVICE_TYPE),
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
