import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PROMETHEUS, BUILT_IN_RUNTIME_GRAFANA
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime, \
    get_services_of_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_head_or_all, get_service_discovery_config, \
    SERVICE_DISCOVERY_PORT, SERVICE_DISCOVERY_PROTOCOL_HTTP, \
    SERVICE_DISCOVERY_FEATURE_METRICS
from cloudtik.core._private.util.core_utils import http_address_string
from cloudtik.core._private.utils import get_runtime_config, get_cluster_name

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["grafana", True, "Grafana", "node"],
    ]

GRAFANA_SERVICE_PORT_CONFIG_KEY = "port"
GRAFANA_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"
GRAFANA_DATA_SOURCES_SCOPE_CONFIG_KEY = "data_sources_scope"
# statically configured data sources
GRAFANA_DATA_SOURCES_CONFIG_KEY = "data_sources"
GRAFANA_DATA_SOURCES_SERVICES_CONFIG_KEY = "data_sources_services"

GRAFANA_SERVICE_TYPE = BUILT_IN_RUNTIME_GRAFANA
GRAFANA_SERVICE_PORT_DEFAULT = 3000

GRAFANA_DATA_SOURCES_SCOPE_NONE = "none"
GRAFANA_DATA_SOURCES_SCOPE_LOCAL = "local"
GRAFANA_DATA_SOURCES_SCOPE_WORKSPACE = "workspace"

GRAFANA_DATA_SOURCE_AUTO_CREATED = "autoCreated"


def get_data_source_name(service_name, cluster_name):
    # WARNING: if a service has many nodes form a load balancer in a single cluster
    # it should be filtered by service selector using service name ,tags or labels
    # or a load balancer should be exposed with a new service
    return "{}-{}".format(
        service_name, cluster_name) if cluster_name else service_name


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_GRAFANA, {})


def _get_service_port(grafana_config: Dict[str, Any]):
    return grafana_config.get(
        GRAFANA_SERVICE_PORT_CONFIG_KEY, GRAFANA_SERVICE_PORT_DEFAULT)


def _is_high_availability(grafana_config: Dict[str, Any]):
    return grafana_config.get(
        GRAFANA_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_GRAFANA)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_GRAFANA: logs_dir}


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    grafana_config = _get_config(runtime_config)
    cluster_runtime_config = get_runtime_config(config)

    service_port = _get_service_port(grafana_config)
    runtime_envs["GRAFANA_SERVICE_PORT"] = service_port

    high_availability = _is_high_availability(grafana_config)
    if high_availability:
        runtime_envs["GRAFANA_HIGH_AVAILABILITY"] = high_availability

    data_sources_scope = grafana_config.get(GRAFANA_DATA_SOURCES_SCOPE_CONFIG_KEY)
    if data_sources_scope == GRAFANA_DATA_SOURCES_SCOPE_WORKSPACE:
        # we need service discovery service for discover workspace scope data sources
        if not get_service_discovery_runtime(cluster_runtime_config):
            raise RuntimeError(
                "Service discovery service is needed for workspace scoped data sources.")
    elif not data_sources_scope:
        data_sources_scope = GRAFANA_DATA_SOURCES_SCOPE_LOCAL

    runtime_envs["GRAFANA_DATA_SOURCES_SCOPE"] = data_sources_scope

    if data_sources_scope == GRAFANA_DATA_SOURCES_SCOPE_LOCAL:
        with_local_data_sources(grafana_config, config, runtime_envs)
    elif data_sources_scope == GRAFANA_DATA_SOURCES_SCOPE_WORKSPACE:
        with_workspace_data_sources(grafana_config, config, runtime_envs)

    return runtime_envs


def with_local_data_sources(
        grafana_config, config, runtime_envs):
    prometheus_services = get_services_of_runtime(
        config, BUILT_IN_RUNTIME_PROMETHEUS)
    if prometheus_services:
        service = next(iter(prometheus_services.values()))
        runtime_envs["GRAFANA_LOCAL_PROMETHEUS_PORT"] = service[SERVICE_DISCOVERY_PORT]


def with_workspace_data_sources(
        grafana_config, config, runtime_envs):
    # discovery through file periodically updated by daemon
    pass


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    grafana_config = _get_config(runtime_config)
    service_port = _get_service_port(grafana_config)
    endpoints = {
        "grafana": {
            "name": "Grafana",
            "url": http_address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    grafana_config = _get_config(runtime_config)
    service_port = _get_service_port(grafana_config)
    service_ports = {
        "grafana": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    grafana_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(grafana_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, GRAFANA_SERVICE_TYPE)
    service_port = _get_service_port(grafana_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            GRAFANA_SERVICE_TYPE,
            service_discovery_config, service_port,
            _is_high_availability(grafana_config),
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_METRICS]),
    }
    return services


def get_prometheus_data_source(name, url, is_default=False):
    prometheus_data_source = {
        "name": name,
        "type": "prometheus",
        "access": "proxy",
        "url": url,
        "isDefault": is_default,
        "editable": True,
        "jsonData": {
            GRAFANA_DATA_SOURCE_AUTO_CREATED: True
        }
    }
    return prometheus_data_source
