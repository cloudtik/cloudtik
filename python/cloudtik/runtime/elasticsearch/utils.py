import os
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_NODE_TYPE_WORKER_DEFAULT
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ELASTICSEARCH, BUILT_IN_RUNTIME_MOUNT
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, define_runtime_service_on_head
from cloudtik.core._private.util.core_utils import get_config_for_update, http_address_string
from cloudtik.core._private.utils import get_runtime_config_for_update, \
    get_runtime_config, get_cluster_name
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["elasticsearch", True, "ElasticSearch Server", "node"],
    ]

ELASTICSEARCH_SERVICE_PORT_CONFIG_KEY = "port"
ELASTICSEARCH_TRANSPORT_PORT_CONFIG_KEY = "transport_port"

ELASTICSEARCH_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
ELASTICSEARCH_CLUSTER_MODE_NONE = "none"
# cluster
ELASTICSEARCH_CLUSTER_MODE_CLUSTER = "cluster"

ELASTICSEARCH_PASSWORD_CONFIG_KEY = "password"
ELASTICSEARCH_SECURITY_CONFIG_KEY = "security"
ELASTICSEARCH_SNAPSHOT_REPOSITORY_CONFIG_KEY = "snapshot_repository"

ELASTICSEARCH_CLUSTERING_CONFIG_KEY = "clustering"
ELASTICSEARCH_ROLE_BY_NODE_TYPE_CONFIG_KEY = "role_by_node_type"
ELASTICSEARCH_NODE_TYPE_OF_ROLES_CONFIG_KEY = "node_type_of_roles"

ELASTICSEARCH_SERVICE_TYPE = BUILT_IN_RUNTIME_ELASTICSEARCH
ELASTICSEARCH_SERVICE_PORT_DEFAULT = 9200
ELASTICSEARCH_TRANSPORT_PORT_DEFAULT = 9300

ELASTICSEARCH_PASSWORD_DEFAULT = "cloudtik"

ELASTICSEARCH_ROLE_MASTER = "master"
ELASTICSEARCH_ROLE_DATA = "data"
ELASTICSEARCH_ROLE_DATA_CONTENT = "data_content"
ELASTICSEARCH_ROLE_DATA_HOT = "data_hot"
ELASTICSEARCH_ROLE_DATA_WARM = "data_warm"
ELASTICSEARCH_ROLE_DATA_COLD = "data_cold"
ELASTICSEARCH_ROLE_DATA_FROZEN = "data_frozen"
ELASTICSEARCH_ROLE_INGEST = "ingest"
ELASTICSEARCH_ROLE_ML = "ml"
ELASTICSEARCH_ROLE_TRANSFORM = "transform"
ELASTICSEARCH_ROLE_REMOTE_CLUSTER_CLIENT = "remote_cluster_client"

ELASTICSEARCH_ALL_ROLES = [
    ELASTICSEARCH_ROLE_MASTER,
    ELASTICSEARCH_ROLE_DATA,
    ELASTICSEARCH_ROLE_DATA_CONTENT,
    ELASTICSEARCH_ROLE_DATA_HOT,
    ELASTICSEARCH_ROLE_DATA_WARM,
    ELASTICSEARCH_ROLE_DATA_COLD,
    ELASTICSEARCH_ROLE_DATA_FROZEN,
    ELASTICSEARCH_ROLE_INGEST,
    ELASTICSEARCH_ROLE_ML,
    ELASTICSEARCH_ROLE_TRANSFORM,
    ELASTICSEARCH_ROLE_REMOTE_CLUSTER_CLIENT,
]


def _is_role_support_snapshot_repository(role):
    if (role == ELASTICSEARCH_ROLE_MASTER
            or role.startswith(ELASTICSEARCH_ROLE_DATA)):
        return True
    return False


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_ELASTICSEARCH, {})


def _get_service_port(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_SERVICE_PORT_CONFIG_KEY, ELASTICSEARCH_SERVICE_PORT_DEFAULT)


def _get_transport_port(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_TRANSPORT_PORT_CONFIG_KEY, ELASTICSEARCH_TRANSPORT_PORT_DEFAULT)


def _get_cluster_mode(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_CLUSTER_MODE_CONFIG_KEY, ELASTICSEARCH_CLUSTER_MODE_CLUSTER)


def _is_security(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_SECURITY_CONFIG_KEY, False)


def _is_snapshot_repository_enabled(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_SNAPSHOT_REPOSITORY_CONFIG_KEY, False)


def _get_clustering_config(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_CLUSTERING_CONFIG_KEY, {})


def _is_role_by_node_type(clustering_config: Dict[str, Any]):
    return clustering_config.get(
        ELASTICSEARCH_ROLE_BY_NODE_TYPE_CONFIG_KEY, True)


def _get_node_type_of_roles(clustering_config: Dict[str, Any]):
    return clustering_config.get(
        ELASTICSEARCH_NODE_TYPE_OF_ROLES_CONFIG_KEY)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_ELASTICSEARCH)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_ELASTICSEARCH: logs_dir}


def _match_roles_for_node_type(node_type, roles_matched):
    if node_type == CLOUDTIK_NODE_TYPE_WORKER_DEFAULT:
        roles = [ELASTICSEARCH_ROLE_MASTER]
        roles_matched.update(roles)
        return roles

    # smart match node type with a role
    # if matched, use the node for a specific role
    meaningful_parts = node_type.split(".")
    name_to_match = meaningful_parts[-1]
    if name_to_match in ELASTICSEARCH_ALL_ROLES:
        roles = [name_to_match]
        if (name_to_match == ELASTICSEARCH_ROLE_ML
                or name_to_match == ELASTICSEARCH_ROLE_TRANSFORM):
            roles += [ELASTICSEARCH_ROLE_REMOTE_CLUSTER_CLIENT]
        roles_matched.update(roles)
        return roles
    return None


def _check_node_type_of_roles(cluster_config, node_type_of_roles):
    available_node_types = cluster_config["available_node_types"]
    head_node_type = cluster_config["head_node_type"]
    worker_node_types = set()
    for node_type in available_node_types:
        if node_type != head_node_type:
            worker_node_types.add(node_type)

    if node_type_of_roles:
        # user specified the node type of roles
        for node_type in node_type_of_roles:
            if (node_type != head_node_type
                    and node_type not in worker_node_types):
                raise RuntimeError(
                    "Node type {} is not defined.".format(node_type))
        return node_type_of_roles

    # This configuration need extra worker node types
    if len(worker_node_types) <= 1:
        return None
    master_node_type = CLOUDTIK_NODE_TYPE_WORKER_DEFAULT
    if master_node_type not in worker_node_types:
        raise RuntimeError(
            "Default worker type {} is not defined for role by node type.".format(
                master_node_type))

    # first try to match with names
    node_type_of_roles = {}
    roles_matched = set()
    for node_type in worker_node_types:
        roles = _match_roles_for_node_type(node_type, roles_matched)
        if roles is not None:
            node_type_of_roles[node_type] = roles

    # finally assign all the remaining
    roles_all = set(ELASTICSEARCH_ALL_ROLES)
    roles_remaining = list(roles_all.difference(roles_matched))
    for node_type in worker_node_types:
        if node_type not in node_type_of_roles:
            # not assign any roles, assign remaining roles
            # coordinating role if empty
            node_type_of_roles[node_type] = roles_remaining

    # for this case head node is master only
    node_type_of_roles[head_node_type] = [ELASTICSEARCH_ROLE_MASTER]
    return node_type_of_roles


def _update_node_type_of_roles(cluster_config, node_type_of_roles):
    runtime_config_to_update = get_runtime_config_for_update(cluster_config)
    elasticsearch_config_to_update = get_config_for_update(
        runtime_config_to_update, BUILT_IN_RUNTIME_ELASTICSEARCH)
    clustering_config_to_update = get_config_for_update(
        elasticsearch_config_to_update, ELASTICSEARCH_CLUSTERING_CONFIG_KEY)
    clustering_config_to_update[ELASTICSEARCH_NODE_TYPE_OF_ROLES_CONFIG_KEY] = node_type_of_roles


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    elasticsearch_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(elasticsearch_config)
    if cluster_mode != ELASTICSEARCH_CLUSTER_MODE_NONE:
        if cluster_mode == ELASTICSEARCH_CLUSTER_MODE_CLUSTER:
            clustering_config = _get_clustering_config(elasticsearch_config)
            # check whether we need to use role by node type
            if _is_role_by_node_type(clustering_config):
                node_type_of_roles = _get_node_type_of_roles(clustering_config)
                node_type_of_roles = _check_node_type_of_roles(
                    cluster_config, node_type_of_roles)
                if node_type_of_roles:
                    _update_node_type_of_roles(cluster_config, node_type_of_roles)

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    elasticsearch_config = _get_config(runtime_config)
    if (_is_snapshot_repository_enabled(elasticsearch_config)
            and not has_runtime_in_cluster(
            runtime_config, BUILT_IN_RUNTIME_MOUNT)):
        raise ValueError(
            "Enabling snapshot repository needs {} runtime to be configured.".format(
                BUILT_IN_RUNTIME_MOUNT))


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    elasticsearch_config = _get_config(runtime_config)

    service_port = _get_service_port(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_SERVICE_PORT"] = service_port

    transport_port = _get_transport_port(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_TRANSPORT_PORT"] = transport_port

    cluster_mode = _get_cluster_mode(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_CLUSTER_MODE"] = cluster_mode

    password = elasticsearch_config.get(
        ELASTICSEARCH_PASSWORD_CONFIG_KEY, ELASTICSEARCH_PASSWORD_DEFAULT)
    runtime_envs["ELASTICSEARCH_PASSWORD"] = password
    runtime_envs["ELASTICSEARCH_SECURITY"] = _is_security(
        elasticsearch_config)

    return runtime_envs


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "elasticsearch": {
            "name": "ElasticSearch",
            "url": http_address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "elasticsearch": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    elasticsearch_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(elasticsearch_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, ELASTICSEARCH_SERVICE_TYPE)
    service_port = _get_service_port(elasticsearch_config)

    def define_elasticsearch_service(define_fn, service_type=None):
        if not service_type:
            service_type = ELASTICSEARCH_SERVICE_TYPE
        return define_fn(
            service_type,
            service_discovery_config, service_port)

    cluster_mode = _get_cluster_mode(elasticsearch_config)
    if cluster_mode == ELASTICSEARCH_CLUSTER_MODE_CLUSTER:
        services = {
            service_name: define_elasticsearch_service(define_runtime_service),
        }
    else:
        # single standalone on head
        services = {
            service_name: define_elasticsearch_service(
                define_runtime_service_on_head),
        }
    return services
