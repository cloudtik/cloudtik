import copy
import logging
import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_CONSUL
from cloudtik.core._private.service_discovery.naming import CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME
from cloudtik.core._private.service_discovery.runtime_services import get_runtime_services_by_node_type, \
    CONSUL_HTTP_PORT_DEFAULT, CONSUL_DNS_PORT_DEFAULT, CONSUL_CONFIG_HTTP_PORT, CONSUL_CONFIG_DNS_PORT, \
    CONSUL_CONFIG_RPC_PORT, CONSUL_RPC_PORT_DEFAULT
from cloudtik.core._private.service_discovery.utils import SERVICE_DISCOVERY_TAGS, SERVICE_DISCOVERY_LABELS, \
    SERVICE_DISCOVERY_LABEL_RUNTIME, \
    SERVICE_DISCOVERY_LABEL_CLUSTER, \
    SERVICE_DISCOVERY_TAG_CLUSTER_PREFIX, ServiceRegisterException, \
    get_runtime_service_features, SERVICE_DISCOVERY_TAG_FEATURE_PREFIX, SERVICE_DISCOVERY_SERVICE_TYPE, \
    SERVICE_DISCOVERY_LABEL_SERVICE, SERVICE_DISCOVERY_LABEL_PROTOCOL, SERVICE_DISCOVERY_PROTOCOL
from cloudtik.core._private.util.core_utils import get_list_for_update, get_config_for_update, http_address_string, \
    address_string
from cloudtik.core._private.util.runtime_utils import RUNTIME_NODE_IP, sort_nodes_by_seq_id
from cloudtik.core._private.utils import \
    _get_node_type_specific_runtime_config, \
    RUNTIME_CONFIG_KEY, get_cluster_name, get_available_node_types, \
    get_head_node_type, get_runtime_types
from cloudtik.runtime.common.service_discovery.cluster import register_service_to_cluster
from cloudtik.runtime.common.service_discovery.discovery import DiscoveryType
from cloudtik.runtime.common.service_discovery.runtime_discovery import discover_consul
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

logger = logging.getLogger(__name__)

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["consul", True, "Consul", "node"],
]

CONFIG_KEY_JOIN_LIST = "join_list"
CONFIG_KEY_RPC_PORT = "rpc_port"
CONFIG_KEY_SERVICES = "services"
CONFIG_KEY_DATA_CENTER = "data_center"

CONSUL_SERVER_SERVICE_SELECTOR_KEY = "server_service_selector"

CONSUL_SERVER_RPC_PORT = CONSUL_RPC_PORT_DEFAULT
CONSUL_SERVER_HTTP_PORT = CONSUL_HTTP_PORT_DEFAULT
CONSUL_SERVER_DNS_PORT = CONSUL_DNS_PORT_DEFAULT

CONSUL_TAG_CLUSTER_FORMAT = SERVICE_DISCOVERY_TAG_CLUSTER_PREFIX + "{}"
CONSUL_TAG_FEATURE_FORMAT = SERVICE_DISCOVERY_TAG_FEATURE_PREFIX + "{}"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_CONSUL, {})


def _get_service_port(consul_config: Dict[str, Any]):
    return consul_config.get(
        CONSUL_CONFIG_RPC_PORT, CONSUL_SERVER_RPC_PORT)


def _get_client_port(consul_config: Dict[str, Any]):
    return consul_config.get(
        CONSUL_CONFIG_HTTP_PORT, CONSUL_SERVER_HTTP_PORT)


def _get_dns_port(consul_config: Dict[str, Any]):
    return consul_config.get(
        CONSUL_CONFIG_DNS_PORT, CONSUL_SERVER_DNS_PORT)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _is_agent_server_mode(runtime_config):
    # Whether this is a consul server cluster or deploy at client
    consul_config = _get_config(runtime_config)
    return consul_config.get("server", False)


def _is_disable_cluster_node_name(consul_config):
    return consul_config.get(CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME, False)


def _get_consul_config_for_update(cluster_config):
    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_CONSUL)


def _get_cluster_name_tag(cluster_name):
    return CONSUL_TAG_CLUSTER_FORMAT.format(cluster_name)


def _get_feature_tag(cluster_name):
    return CONSUL_TAG_FEATURE_FORMAT.format(cluster_name)


def _bootstrap_runtime_config(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    return cluster_config


def _bootstrap_join_list(cluster_config: Dict[str, Any]):
    consul_config = _get_consul_config_for_update(cluster_config)
    # The consul server cluster must be running and registered
    # discovered with bootstrap methods (through workspace)

    server_addresses = discover_consul(
        consul_config, CONSUL_SERVER_SERVICE_SELECTOR_KEY,
        cluster_config=cluster_config,
        discovery_type=DiscoveryType.WORKSPACE)
    if not server_addresses:
        raise RuntimeError(
            "No running consul server cluster is detected.")

    join_list = ",".join([server_address[0] for server_address in server_addresses])
    consul_config[CONFIG_KEY_JOIN_LIST] = join_list
    # current we don't use it
    rpc_port = server_addresses[0][1]
    if not rpc_port:
        rpc_port = CONSUL_SERVER_RPC_PORT
    consul_config[CONFIG_KEY_RPC_PORT] = rpc_port
    return cluster_config


def _bootstrap_runtime_services(config: Dict[str, Any]):
    # for all the runtimes, query its services per node type
    service_configs = {}
    cluster_name = get_cluster_name(config)
    services_map = get_runtime_services_by_node_type(config)
    for node_type, services_for_node_type in services_map.items():
        service_config_for_node_type = {}
        for service_name, service in services_for_node_type.items():
            runtime_type, runtime_service = service
            service_config = _generate_service_config(
                cluster_name, runtime_type, runtime_service)
            service_config_for_node_type[service_name] = service_config
        if service_config_for_node_type:
            service_configs[node_type] = service_config_for_node_type

    if service_configs:
        consul_config = _get_consul_config_for_update(config)
        consul_config[CONFIG_KEY_SERVICES] = service_configs

    return config


def _generate_service_config(cluster_name, runtime_type, runtime_service):
    # We utilize all the standard service discovery properties
    service_config = copy.deepcopy(runtime_service)

    # tags cluster name as tags
    tags = get_list_for_update(
        service_config, SERVICE_DISCOVERY_TAGS)

    # cluster name tag
    cluster_name_tag = _get_cluster_name_tag(cluster_name)
    tags.append(cluster_name_tag)

    # features tag
    features = get_runtime_service_features(service_config)
    if features:
        for feature in features:
            feature_tag = _get_feature_tag(feature)
            tags.append(feature_tag)

    labels = get_config_for_update(
        service_config, SERVICE_DISCOVERY_LABELS)

    # protocol as label
    protocol = service_config.get(SERVICE_DISCOVERY_PROTOCOL)
    if protocol:
        labels[SERVICE_DISCOVERY_LABEL_PROTOCOL] = protocol

    labels[SERVICE_DISCOVERY_LABEL_CLUSTER] = cluster_name
    labels[SERVICE_DISCOVERY_LABEL_RUNTIME] = runtime_type
    service_type = runtime_service.get(SERVICE_DISCOVERY_SERVICE_TYPE)
    if service_type:
        labels[SERVICE_DISCOVERY_LABEL_SERVICE] = service_type
    return service_config


def _with_runtime_environment_variables(
        server_mode, runtime_config, config):
    runtime_envs = {}
    consul_config = _get_config(runtime_config)
    data_center = consul_config.get(CONFIG_KEY_DATA_CENTER)
    if not data_center:
        # default to use workspace name as datacenter unless override
        data_center = config["workspace_name"]
    runtime_envs["CONSUL_DATA_CENTER"] = data_center

    if server_mode:
        runtime_envs["CONSUL_SERVER"] = True

        # get the number of the workers plus head
        minimal_workers = _get_consul_minimal_workers(config)
        runtime_envs["CONSUL_NUM_SERVERS"] = minimal_workers + 1
    else:
        runtime_envs["CONSUL_CLIENT"] = True
        join_list = consul_config.get(CONFIG_KEY_JOIN_LIST)
        if not join_list:
            raise RuntimeError(
                "Invalid join list. No running consul server cluster is detected.")
        runtime_envs["CONSUL_JOIN_LIST"] = join_list

    runtime_envs["CONSUL_SERVICE_PORT"] = _get_service_port(consul_config)
    runtime_envs["CONSUL_CLIENT_PORT"] = _get_client_port(consul_config)
    runtime_envs["CONSUL_DNS_PORT"] = _get_dns_port(consul_config)
    return runtime_envs


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_CONSUL)


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_CONSUL: logs_dir}


def _get_runtime_endpoints(
        server_mode, runtime_config: Dict[str, Any],
        cluster_config, cluster_head_ip):
    consul_config = _get_config(runtime_config)
    endpoints = {
        "consul": {
            "name": "Consul",
            "url": address_string(
                cluster_head_ip, _get_service_port(consul_config))
        },
    }
    if server_mode:
        endpoints["consul_http"] = {
            "name": "Consul HTTP",
            "url": http_address_string(
                cluster_head_ip, _get_client_port(consul_config))
        }
        endpoints["consul_dns"] = {
            "name": "Consul DNS",
            "url": address_string(
                cluster_head_ip, _get_dns_port(consul_config))
        }

    return endpoints


def _get_head_service_ports(
        server_mode, runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    consul_config = _get_config(runtime_config)
    service_ports = {
        "consul": {
            "protocol": "TCP",
            "port": _get_service_port(consul_config),
        },
    }

    if server_mode:
        service_ports["consul-http"] = {
            "protocol": "TCP",
            "port": _get_client_port(consul_config),
        }
        service_ports["consul-dns"] = {
            "protocol": "TCP",
            "port": _get_dns_port(consul_config),
        }
    return service_ports


def _handle_node_constraints_reached(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any],
        node_type: str, head_info: Dict[str, Any], nodes_info: Dict[str, Any]):
    # We know this is called in the cluster scaler context
    consul_config = _get_config(runtime_config)
    rpc_port = _get_service_port(consul_config)
    server_ensemble = sort_nodes_by_seq_id(nodes_info)
    endpoints = [(head_info[RUNTIME_NODE_IP], rpc_port)]
    worker_nodes = [(node_info[RUNTIME_NODE_IP], rpc_port
                     ) for node_info in server_ensemble]
    endpoints += worker_nodes

    try:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_CONSUL,
            service_addresses=endpoints)
    except ServiceRegisterException as e:
        logger.warning("Error happened: {}", str(e))

    register_service_to_cluster(
        BUILT_IN_RUNTIME_CONSUL,
        service_addresses=endpoints)


def _get_consul_minimal_workers(config: Dict[str, Any]):
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    for node_type in available_node_types:
        if node_type == head_node_type:
            # Exclude the head
            continue
        # Check the runtimes of the node type whether it needs to wait minimal before update
        runtime_config = _get_node_type_specific_runtime_config(
            config, node_type)
        if not runtime_config:
            continue
        runtime_types = get_runtime_types(runtime_config)
        if BUILT_IN_RUNTIME_CONSUL not in runtime_types:
            continue
        node_type_config = available_node_types[node_type]
        min_workers = node_type_config.get("min_workers", 0)
        return min_workers
    return 0


def _get_services_of_node_type(runtime_config, node_type):
    consul_config = _get_config(runtime_config)
    services_map = consul_config.get(CONFIG_KEY_SERVICES)
    if not services_map:
        return None
    return services_map.get(node_type)
