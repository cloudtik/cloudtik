import os
import uuid
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_REDIS
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_KEY_VALUE, define_runtime_service_on_head, \
    define_runtime_service_on_worker
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, is_node_seq_id_enabled, enable_node_seq_id

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["redis-server", True, "Redis Server", "node"],
    ]

REDIS_SERVICE_PORT_CONFIG_KEY = "port"
REDIS_CLUSTER_PORT_CONFIG_KEY = "cluster_port"

REDIS_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
REDIS_CLUSTER_MODE_NONE = "none"
# replication
REDIS_CLUSTER_MODE_REPLICATION = "replication"
# cluster
REDIS_CLUSTER_MODE_CLUSTER = "cluster"

REDIS_CLUSTER_NAME_CONFIG_KEY = "cluster_name"

REDIS_PASSWORD_CONFIG_KEY = "password"

REDIS_SERVICE_TYPE = BUILT_IN_RUNTIME_REDIS
REDIS_REPLICA_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-replica"
REDIS_SERVICE_PORT_DEFAULT = 6379
REDIS_CLUSTER_PORT_DEFAULT = 33061

REDIS_PASSWORD_DEFAULT = "cloudtik"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_REDIS, {})


def _get_service_port(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_SERVICE_PORT_CONFIG_KEY, REDIS_SERVICE_PORT_DEFAULT)


def _get_cluster_port(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_PORT_CONFIG_KEY, REDIS_CLUSTER_PORT_DEFAULT)


def _get_cluster_mode(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_MODE_CONFIG_KEY, REDIS_CLUSTER_MODE_CLUSTER)


def _get_cluster_name(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_NAME_CONFIG_KEY)


def _generate_cluster_name(config: Dict[str, Any]):
    workspace_name = config["workspace_name"]
    cluster_name = config["cluster_name"]
    return str(uuid.uuid3(uuid.NAMESPACE_OID, workspace_name + cluster_name))


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_REDIS)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"redis": logs_dir}


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

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    redis_config = _get_config(runtime_config)
    # TODO


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    redis_config = _get_config(runtime_config)

    service_port = _get_service_port(redis_config)
    runtime_envs["REDIS_SERVICE_PORT"] = service_port

    cluster_mode = _get_cluster_mode(redis_config)
    runtime_envs["REDIS_CLUSTER_MODE"] = cluster_mode

    if cluster_mode == REDIS_CLUSTER_MODE_CLUSTER:
        # configure the cluster GUID
        cluster_name = _get_cluster_name(redis_config)
        if not cluster_name:
            cluster_name = _generate_cluster_name(config)
        runtime_envs["REDIS_CLUSTER_NAME"] = cluster_name

        cluster_port = _get_cluster_port(redis_config)
        runtime_envs["REDIS_CLUSTER_PORT"] = cluster_port

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

    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode == REDIS_CLUSTER_MODE_REPLICATION:
        # primary service on head and replica service on workers
        replica_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, REDIS_REPLICA_SERVICE_TYPE)
        services = {
            service_name: define_runtime_service_on_head(
                REDIS_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
            replica_service_name: define_runtime_service_on_worker(
                REDIS_REPLICA_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
        }
    elif cluster_mode == REDIS_CLUSTER_MODE_CLUSTER:
        # Service register for each node but don't give key-value feature to avoid
        # these service been discovered.
        # TODO: Ideally a middle layer needs to expose a client discoverable service.
        services = {
            service_name: define_runtime_service(
                REDIS_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
        }
    else:
        # single standalone on head
        services = {
            service_name: define_runtime_service_on_head(
                REDIS_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
        }
    return services
