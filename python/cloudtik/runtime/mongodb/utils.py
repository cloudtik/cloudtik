import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MONGODB
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, define_runtime_service_on_head
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, is_node_seq_id_enabled, enable_node_seq_id

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["mongod", True, "MongoDB", "node"],
    ]

MONGODB_SERVICE_PORT_CONFIG_KEY = "port"

MONGODB_REPLICATION_SET_NAME_CONFIG_KEY = "replication_set_name"

MONGODB_SHARDING_CONFIG_KEY = "sharding"


MONGODB_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
MONGODB_CLUSTER_MODE_NONE = "none"
# replication
MONGODB_CLUSTER_MODE_REPLICATION = "replication"
# sharding
MONGODB_CLUSTER_MODE_SHARDING = "sharding"

MONGODB_ROOT_USER_CONFIG_KEY = "root_user"
MONGODB_ROOT_PASSWORD_CONFIG_KEY = "root_password"

MONGODB_DATABASE_CONFIG_KEY = "database"
MONGODB_DATABASE_NAME_CONFIG_KEY = "name"
MONGODB_DATABASE_USER_CONFIG_KEY = "user"
MONGODB_DATABASE_PASSWORD_CONFIG_KEY = "password"

MONGODB_SERVICE_TYPE = BUILT_IN_RUNTIME_MONGODB

MONGODB_DYNAMIC_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-dynamic"
MONGODB_REPLICA_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-replica"


MONGODB_SERVICE_PORT_DEFAULT = 27017

MONGODB_ROOT_USER_DEFAULT = "root"
MONGODB_ROOT_PASSWORD_DEFAULT = ""


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_MONGODB, {})


def _get_service_port(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_SERVICE_PORT_CONFIG_KEY, MONGODB_SERVICE_PORT_DEFAULT)


def _get_cluster_mode(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_CLUSTER_MODE_CONFIG_KEY, MONGODB_CLUSTER_MODE_REPLICATION)


def _get_replication_set_name(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_REPLICATION_SET_NAME_CONFIG_KEY)


def _get_sharding_config(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_SHARDING_CONFIG_KEY, {})


def _generate_replication_set_name(config: Dict[str, Any]):
    workspace_name = config["workspace_name"]
    cluster_name = config["cluster_name"]
    return f"{workspace_name}-{cluster_name}"


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_MONGODB)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"mongodb": logs_dir}


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    mongodb_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode != MONGODB_CLUSTER_MODE_NONE:
        # We must enable the node seq id (stable seq id is preferred)
        # But we don't enforce it.
        if not is_node_seq_id_enabled(cluster_config):
            enable_node_seq_id(cluster_config)

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    mongodb_config = _get_config(runtime_config)

    database = mongodb_config.get(MONGODB_DATABASE_CONFIG_KEY, {})
    user = database.get(MONGODB_DATABASE_USER_CONFIG_KEY)
    password = database.get(MONGODB_DATABASE_PASSWORD_CONFIG_KEY)
    if (user and not password) or (not user and password):
        raise ValueError("User and password must be both specified or not specified.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    mongodb_config = _get_config(runtime_config)

    service_port = _get_service_port(mongodb_config)
    runtime_envs["MONGODB_SERVICE_PORT"] = service_port

    cluster_mode = _get_cluster_mode(mongodb_config)
    runtime_envs["MONGODB_CLUSTER_MODE"] = cluster_mode

    if (cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION
            or cluster_mode == MONGODB_CLUSTER_MODE_SHARDING):
        # default to workspace name + cluster name
        replication_set_name = _get_replication_set_name(
            mongodb_config)
        if not replication_set_name:
            replication_set_name = _generate_replication_set_name(config)
        runtime_envs["MONGODB_REPLICATION_SET_NAME"] = replication_set_name

    root_user = mongodb_config.get(
        MONGODB_ROOT_USER_CONFIG_KEY, MONGODB_ROOT_USER_DEFAULT)
    runtime_envs["MONGODB_ROOT_USER"] = root_user

    root_password = mongodb_config.get(
        MONGODB_ROOT_PASSWORD_CONFIG_KEY, MONGODB_ROOT_PASSWORD_DEFAULT)
    runtime_envs["MONGODB_ROOT_PASSWORD"] = root_password

    database = mongodb_config.get(MONGODB_DATABASE_CONFIG_KEY, {})
    database_name = database.get(MONGODB_DATABASE_NAME_CONFIG_KEY)
    if database_name:
        runtime_envs["MONGODB_DATABASE"] = database_name
    user = database.get(MONGODB_DATABASE_USER_CONFIG_KEY)
    if user:
        runtime_envs["MONGODB_USER"] = user
    password = database.get(MONGODB_DATABASE_PASSWORD_CONFIG_KEY)
    if password:
        runtime_envs["MONGODB_PASSWORD"] = password

    return runtime_envs


def _get_runtime_endpoints(runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "mongodb": {
            "name": "MongoDB",
            "url": "{}:{}".format(cluster_head_ip, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "mongodb": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    mongodb_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(mongodb_config)
    service_port = _get_service_port(mongodb_config)

    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION:
        # all nodes are possible primary
        dynamic_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_DYNAMIC_SERVICE_TYPE)
        services = {
            dynamic_service_name: define_runtime_service(
                MONGODB_DYNAMIC_SERVICE_TYPE,
                service_discovery_config, service_port),
        }
    elif cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        # TODO: Ideally a middle layer needs to expose a client discoverable service.
        dynamic_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_DYNAMIC_SERVICE_TYPE)
        services = {
            dynamic_service_name: define_runtime_service(
                MONGODB_DYNAMIC_SERVICE_TYPE,
                service_discovery_config, service_port),
        }
    else:
        # single standalone on head
        service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_SERVICE_TYPE)
        services = {
            service_name: define_runtime_service_on_head(
                MONGODB_SERVICE_TYPE,
                service_discovery_config, service_port),
        }
    return services
