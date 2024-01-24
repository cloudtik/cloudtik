import os
from typing import Any, Dict

from cloudtik.core._private.util.core_utils import get_list_for_update, get_config_for_update, http_address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PROMETHEUS
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime, \
    get_runtime_services_by_node_type
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, \
    define_runtime_service_on_head_or_all, get_service_discovery_config, SERVICE_DISCOVERY_PORT, \
    SERVICE_DISCOVERY_PROTOCOL_HTTP, \
    has_runtime_service_feature, SERVICE_DISCOVERY_FEATURE_METRICS
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, get_runtime_config, get_cluster_name

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["prometheus", True, "Prometheus", "node"],
    ]

PROMETHEUS_SERVICE_PORT_CONFIG_KEY = "port"
PROMETHEUS_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"
PROMETHEUS_SERVICE_DISCOVERY_CONFIG_KEY = "service_discovery"
PROMETHEUS_SCRAPE_SCOPE_CONFIG_KEY = "scrape_scope"
PROMETHEUS_SCRAPE_SERVICES_CONFIG_KEY = "scrape_services"

# if consul is not used, static federation targets can be used
PROMETHEUS_FEDERATION_TARGETS_CONFIG_KEY = "federation_targets"

# This is used for local pull service targets
PROMETHEUS_PULL_SERVICES_CONFIG_KEY = "pull_services"
PROMETHEUS_PULL_NODE_TYPES_CONFIG_KEY = "node_types"

PROMETHEUS_SERVICE_TYPE = BUILT_IN_RUNTIME_PROMETHEUS
PROMETHEUS_SERVICE_PORT_DEFAULT = 9090

PROMETHEUS_SERVICE_DISCOVERY_FILE = "file"
PROMETHEUS_SERVICE_DISCOVERY_CONSUL = "consul"

PROMETHEUS_SCRAPE_SCOPE_LOCAL = "local"
PROMETHEUS_SCRAPE_SCOPE_WORKSPACE = "workspace"
PROMETHEUS_SCRAPE_SCOPE_FEDERATION = "federation"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_PROMETHEUS, {})


def _get_service_port(prometheus_config: Dict[str, Any]):
    return prometheus_config.get(
        PROMETHEUS_SERVICE_PORT_CONFIG_KEY, PROMETHEUS_SERVICE_PORT_DEFAULT)


def _get_federation_targets(prometheus_config: Dict[str, Any]):
    return prometheus_config.get(
        PROMETHEUS_FEDERATION_TARGETS_CONFIG_KEY)


def _is_high_availability(prometheus_config: Dict[str, Any]):
    return prometheus_config.get(
        PROMETHEUS_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_PROMETHEUS)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_PROMETHEUS: logs_dir}


def _get_config_for_update(cluster_config):
    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_PROMETHEUS)


def _bootstrap_runtime_services(config: Dict[str, Any]):
    # for all the runtimes, query its services per node type
    pull_services = {}
    services_map = get_runtime_services_by_node_type(config)
    for node_type, services_for_node_type in services_map.items():
        for service_name, service in services_for_node_type.items():
            runtime_type, runtime_service = service
            # check whether this service provide metric or not
            if has_runtime_service_feature(
                    runtime_service, SERVICE_DISCOVERY_FEATURE_METRICS):
                if service_name not in pull_services:
                    service_port = runtime_service[SERVICE_DISCOVERY_PORT]
                    pull_service = {
                        SERVICE_DISCOVERY_PORT: service_port,
                        PROMETHEUS_PULL_NODE_TYPES_CONFIG_KEY: [node_type]
                    }
                    pull_services[service_name] = pull_service
                else:
                    pull_service = pull_services[service_name]
                    node_types_of_service = get_list_for_update(
                        pull_service, PROMETHEUS_PULL_NODE_TYPES_CONFIG_KEY)
                    node_types_of_service.append(node_type)

    if pull_services:
        prometheus_config = _get_config_for_update(config)
        prometheus_config[PROMETHEUS_PULL_SERVICES_CONFIG_KEY] = pull_services

    return config


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    prometheus_config = _get_config(runtime_config)
    cluster_runtime_config = get_runtime_config(config)

    service_port = _get_service_port(prometheus_config)
    runtime_envs["PROMETHEUS_SERVICE_PORT"] = service_port

    high_availability = _is_high_availability(prometheus_config)
    if high_availability:
        runtime_envs["PROMETHEUS_HIGH_AVAILABILITY"] = high_availability

    sd = prometheus_config.get(PROMETHEUS_SERVICE_DISCOVERY_CONFIG_KEY)
    if not sd:
        # auto decide
        if get_service_discovery_runtime(cluster_runtime_config):
            sd = PROMETHEUS_SERVICE_DISCOVERY_CONSUL
        else:
            sd = PROMETHEUS_SERVICE_DISCOVERY_FILE

    scrape_scope = prometheus_config.get(PROMETHEUS_SCRAPE_SCOPE_CONFIG_KEY)
    if scrape_scope == PROMETHEUS_SCRAPE_SCOPE_WORKSPACE:
        # make sure
        if not get_service_discovery_runtime(cluster_runtime_config):
            raise RuntimeError(
                "Service discovery service is needed for workspace scoped scrape.")
        sd = PROMETHEUS_SERVICE_DISCOVERY_CONSUL
    elif scrape_scope == PROMETHEUS_SCRAPE_SCOPE_FEDERATION:
        federation_targets = _get_federation_targets(prometheus_config)
        if federation_targets:
            sd = PROMETHEUS_SERVICE_DISCOVERY_FILE
        else:
            if not get_service_discovery_runtime(cluster_runtime_config):
                raise RuntimeError(
                    "Service discovery service is needed for federation scoped scrape.")
            sd = PROMETHEUS_SERVICE_DISCOVERY_CONSUL
    elif not scrape_scope:
        scrape_scope = PROMETHEUS_SCRAPE_SCOPE_LOCAL

    if sd == PROMETHEUS_SERVICE_DISCOVERY_FILE:
        _with_file_sd_environment_variables(
            prometheus_config, config, runtime_envs)
    elif sd == PROMETHEUS_SERVICE_DISCOVERY_CONSUL:
        _with_consul_sd_environment_variables(
            prometheus_config, config, runtime_envs)
    else:
        raise RuntimeError(
            "Unsupported service discovery type: {}. "
            "Valid types are: {}, {}.".format(
                sd,
                PROMETHEUS_SERVICE_DISCOVERY_FILE,
                PROMETHEUS_SERVICE_DISCOVERY_CONSUL))

    runtime_envs["PROMETHEUS_SERVICE_DISCOVERY"] = sd
    runtime_envs["PROMETHEUS_SCRAPE_SCOPE"] = scrape_scope
    return runtime_envs


def _with_file_sd_environment_variables(
        prometheus_config, config, runtime_envs):
    # discovery through file periodically updated by daemon
    pass


def _with_consul_sd_environment_variables(
        prometheus_config, config, runtime_envs):
    # TODO: export variables necessary for Consul service discovery
    pass


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    prometheus_config = _get_config(runtime_config)
    service_port = _get_service_port(prometheus_config)
    endpoints = {
        "prometheus": {
            "name": "Prometheus",
            "url": http_address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    prometheus_config = _get_config(runtime_config)
    service_port = _get_service_port(prometheus_config)
    service_ports = {
        "prometheus": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    prometheus_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(prometheus_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, PROMETHEUS_SERVICE_TYPE)
    service_port = _get_service_port(prometheus_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            PROMETHEUS_SERVICE_TYPE,
            service_discovery_config, service_port,
            _is_high_availability(prometheus_config),
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_METRICS])
    }
    return services
