from typing import Dict, Any

from cloudtik.core._private.constants import CLOUDTIK_DEFAULT_PORT, CLOUDTIK_METRIC_PORT, CLOUDTIK_RUNTIME_NAME
from cloudtik.core._private.runtime_factory import _get_runtime, BUILT_IN_RUNTIME_CONSUL
from cloudtik.core._private.service_discovery.utils import match_service_node, get_canonical_service_name, \
    define_runtime_service_on_head, get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_METRICS, \
    SERVICE_DISCOVERY_PROTOCOL_HTTP
from cloudtik.core._private.utils import _get_node_type_specific_runtime_config, \
    is_runtime_enabled, get_cluster_name, get_available_node_types, get_head_node_type, \
    get_runtime_config, get_runtime_types

CLOUDTIK_REDIS_SERVICE_TYPE = "cloudtik-redis"
CLOUDTIK_CLUSTER_CONTROLLER_METRICS_SERVICE_TYPE = "cloudtik-metrics"

CLOUDTIK_CLUSTER_CONTROLLER_METRICS_PORT = CLOUDTIK_METRIC_PORT
CLOUDTIK_REDIS_SERVICE_PORT = CLOUDTIK_DEFAULT_PORT

SERVICE_DISCOVERY_RUNTIMES = [BUILT_IN_RUNTIME_CONSUL]

CONSUL_CONFIG_JOIN_LIST = "join_list"
CONSUL_CONFIG_RPC_PORT = "rpc_port"

CONSUL_CONFIG_HTTP_PORT = "http_port"
CONSUL_CONFIG_DNS_PORT = "dns_port"

CONSUL_RPC_PORT_DEFAULT = 8300
CONSUL_HTTP_PORT_DEFAULT = 8500
CONSUL_DNS_PORT_DEFAULT = 8600


def get_runtime_services_by_node_type(config: Dict[str, Any]):
    # for all the runtimes, query its services per node type
    cluster_name = get_cluster_name(config)
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    built_in_services = _get_built_in_services(config, cluster_name)

    services_map = {}
    for node_type in available_node_types:
        head = True if node_type == head_node_type else False
        services_for_node_type = {}

        for service_name, runtime_service in built_in_services.items():
            if match_service_node(runtime_service, head):
                services_for_node_type[service_name] = (
                    CLOUDTIK_RUNTIME_NAME, runtime_service)

        runtime_config = _get_node_type_specific_runtime_config(config, node_type)
        if runtime_config:
            # services runtimes
            runtime_types = get_runtime_types(runtime_config)
            for runtime_type in runtime_types:
                if runtime_type == BUILT_IN_RUNTIME_CONSUL:
                    continue

                runtime = _get_runtime(runtime_type, runtime_config)
                services = runtime.get_runtime_services(config)
                if not services:
                    continue

                for service_name, runtime_service in services.items():
                    if match_service_node(runtime_service, head):
                        services_for_node_type[service_name] = (
                            runtime_type, runtime_service)
        if services_for_node_type:
            services_map[node_type] = services_for_node_type
    return services_map


def get_services_of_runtime(config: Dict[str, Any], runtime_type):
    cluster_name = get_cluster_name(config)
    if runtime_type == CLOUDTIK_RUNTIME_NAME:
        built_in_services = _get_built_in_services(config, cluster_name)
        return built_in_services
    runtime_config = get_runtime_config(config)
    if not is_runtime_enabled(runtime_config, runtime_type):
        return None

    runtime = _get_runtime(runtime_type, runtime_config)
    return runtime.get_runtime_services(config)


def _get_built_in_services(config: Dict[str, Any], cluster_name):
    runtime_config = get_runtime_config(config)
    service_discovery_config = get_service_discovery_config(runtime_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name,
        CLOUDTIK_CLUSTER_CONTROLLER_METRICS_SERVICE_TYPE)
    redis_service_name = get_canonical_service_name(
        service_discovery_config, cluster_name,
        CLOUDTIK_REDIS_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_head(
            CLOUDTIK_CLUSTER_CONTROLLER_METRICS_SERVICE_TYPE,
            service_discovery_config, CLOUDTIK_CLUSTER_CONTROLLER_METRICS_PORT,
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_METRICS]),
        redis_service_name: define_runtime_service_on_head(
            CLOUDTIK_REDIS_SERVICE_TYPE,
            service_discovery_config, CLOUDTIK_REDIS_SERVICE_PORT),
    }
    return services


def get_service_discovery_runtime(runtime_config):
    for runtime_type in SERVICE_DISCOVERY_RUNTIMES:
        if is_runtime_enabled(runtime_config, runtime_type):
            return runtime_type
    return None


def is_service_discovery_runtime(runtime_type):
    return True if runtime_type in SERVICE_DISCOVERY_RUNTIMES else False


def get_consul_server_addresses(runtime_config: Dict[str, Any]):
    consul_config = runtime_config.get(BUILT_IN_RUNTIME_CONSUL, {})
    join_list = consul_config.get(CONSUL_CONFIG_JOIN_LIST)
    if not join_list:
        return None
    hosts = join_list.split(',')
    port = consul_config.get(
        CONSUL_CONFIG_RPC_PORT, CONSUL_RPC_PORT_DEFAULT)
    return [(host, port) for host in hosts]


def get_consul_local_client_port(runtime_config):
    consul_config = runtime_config.get(BUILT_IN_RUNTIME_CONSUL, {})
    return consul_config.get(CONSUL_CONFIG_HTTP_PORT, CONSUL_HTTP_PORT_DEFAULT)


def get_local_dns_server_port(runtime_config):
    consul_config = runtime_config.get(BUILT_IN_RUNTIME_CONSUL, {})
    return consul_config.get(CONSUL_CONFIG_DNS_PORT, CONSUL_DNS_PORT_DEFAULT)
