import os
from typing import Any, Dict

from cloudtik.core._private.util.core_utils import http_address_string, export_environment_variables
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_APISIX
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_head_or_all, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_API_GATEWAY, SERVICE_DISCOVERY_PROTOCOL_HTTP
from cloudtik.core._private.utils import get_runtime_config, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import discover_etcd_from_workspace, \
    discover_etcd_on_head, ETCD_URI_KEY, is_etcd_service_discovery

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["/usr/local/apisix", False, "APISIX", "node"],
    ]

APISIX_SERVICE_PORT_CONFIG_KEY = "port"
APISIX_ADMIN_PORT_CONFIG_KEY = "admin_port"
APISIX_ADMIN_KEY_CONFIG_KEY = "admin_key"

APISIX_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

APISIX_BACKEND_CONFIG_KEY = "backend"
APISIX_BACKEND_CONFIG_MODE_CONFIG_KEY = "config_mode"
APISIX_BACKEND_SELECTOR_CONFIG_KEY = "selector"

# roundrobin, chash (consistent-hashing), least_conn, ewma (latency)
APISIX_BACKEND_BALANCE_CONFIG_KEY = "balance"

APISIX_BALANCE_TYPE_ROUND_ROBIN = "roundrobin"
APISIX_BALANCE_TYPE_CONSISTENT_HASH = "chash"
APISIX_BALANCE_TYPE_LEAST_CONN = "least_conn"
APISIX_BALANCE_TYPE_LATENCY = "ewma"

APISIX_ADMIN_KEY_DEFAULT = "edd1c9f035435d136f87ad84b625c8f2"

APISIX_CONFIG_MODE_DNS = "dns"
APISIX_CONFIG_MODE_CONSUL = "consul"
APISIX_CONFIG_MODE_DYNAMIC = "dynamic"

APISIX_SERVICE_TYPE = BUILT_IN_RUNTIME_APISIX
APISIX_SERVICE_PORT_DEFAULT = 9080
APISIX_ADMIN_PORT_DEFAULT = 9180


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_APISIX, {})


def _get_service_port(apisix_config: Dict[str, Any]):
    return apisix_config.get(
        APISIX_SERVICE_PORT_CONFIG_KEY, APISIX_SERVICE_PORT_DEFAULT)


def _get_admin_port(apisix_config: Dict[str, Any]):
    return apisix_config.get(
        APISIX_ADMIN_PORT_CONFIG_KEY, APISIX_ADMIN_PORT_DEFAULT)


def _is_high_availability(apisix_config: Dict[str, Any]):
    return apisix_config.get(
        APISIX_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_backend_config(apisix_config: Dict[str, Any]):
    return apisix_config.get(
        APISIX_BACKEND_CONFIG_KEY, {})


def _get_config_mode(backend_config: Dict[str, Any]):
    return backend_config.get(
        APISIX_BACKEND_CONFIG_MODE_CONFIG_KEY, APISIX_CONFIG_MODE_CONSUL)


def _get_admin_key(apisix_config: Dict[str, Any]):
    return apisix_config.get(
        APISIX_ADMIN_KEY_CONFIG_KEY, APISIX_ADMIN_KEY_DEFAULT)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_APISIX)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_APISIX: logs_dir}


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_etcd_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_APISIX)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_etcd_on_head(
        cluster_config, BUILT_IN_RUNTIME_APISIX)

    _validate_config(cluster_config, final=True)
    return cluster_config


def _validate_config(config: Dict[str, Any], final=False):
    # Check etcd configuration
    runtime_config = get_runtime_config(config)
    apisix_config = _get_config(runtime_config)
    etcd_uri = apisix_config.get(ETCD_URI_KEY)
    if not etcd_uri:
        # if there is service discovery mechanism, assume we can get from service discovery
        if (final or not is_etcd_service_discovery(apisix_config) or
                not get_service_discovery_runtime(runtime_config)):
            raise ValueError(
                "ETCD must be configured for APISIX.")

    cluster_runtime_config = get_runtime_config(config)
    if not get_service_discovery_runtime(cluster_runtime_config):
        raise ValueError(
            "Service discovery runtime is needed for APISIX.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    apisix_config = _get_config(runtime_config)

    service_port = _get_service_port(apisix_config)
    runtime_envs["APISIX_SERVICE_PORT"] = service_port

    admin_port = _get_admin_port(apisix_config)
    runtime_envs["APISIX_ADMIN_PORT"] = admin_port

    high_availability = _is_high_availability(apisix_config)
    if high_availability:
        runtime_envs["APISIX_HIGH_AVAILABILITY"] = high_availability

    backend_config = _get_backend_config(apisix_config)
    config_mode = _get_config_mode(backend_config)
    runtime_envs["APISIX_CONFIG_MODE"] = config_mode

    balance = backend_config.get(
        APISIX_BACKEND_BALANCE_CONFIG_KEY, APISIX_BALANCE_TYPE_ROUND_ROBIN)
    runtime_envs["APISIX_BACKEND_BALANCE"] = balance

    return runtime_envs


def _node_configure(runtime_config, head: bool):
    apisix_config = _get_config(runtime_config)
    admin_key = _get_admin_key(apisix_config)
    envs = {"APISIX_ADMIN_KEY": admin_key}
    export_environment_variables(envs)


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "apisix": {
            "name": "APISIX",
            "url": http_address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "apisix": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    apisix_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(apisix_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, APISIX_SERVICE_TYPE)
    service_port = _get_service_port(apisix_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            APISIX_SERVICE_TYPE,
            service_discovery_config, service_port,
            _is_high_availability(apisix_config),
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_API_GATEWAY]),
    }
    return services
