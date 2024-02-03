import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HAPROXY
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, \
    get_service_discovery_config, define_runtime_service_on_head_or_all, \
    SERVICE_DISCOVERY_FEATURE_LOAD_BALANCER, include_runtime_for_selector, exclude_runtime_of_cluster, \
    include_service_name_for_selector, include_service_type_for_selector, get_service_type_override, \
    get_service_discovery_config_for_update, set_service_type_override, get_service_selector_copy
from cloudtik.core._private.util.core_utils import get_config_for_update, \
    export_environment_variables, http_address_string, address_string
from cloudtik.core._private.utils import get_runtime_config, get_cluster_name
from cloudtik.runtime.common.health_check import HEALTH_CHECK_RUNTIME, get_health_check_port_of_service, \
    get_health_check_service_type_of, get_service_type_from_health_check_path
from cloudtik.runtime.common.service_discovery.consul import get_rfc2782_service_dns_name
from cloudtik.runtime.common.service_discovery.discovery import DiscoveryType, query_services
from cloudtik.runtime.haproxy.admin_api import get_backend_server_name

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["haproxy", True, "HAProxy", "node"],
]

HAPROXY_SERVICE_PORT_CONFIG_KEY = "port"
HAPROXY_SERVICE_PROTOCOL_CONFIG_KEY = "protocol"
HAPROXY_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"
HAPROXY_APP_MODE_CONFIG_KEY = "app_mode"

HAPROXY_BACKEND_CONFIG_KEY = "backend"
HAPROXY_BACKEND_CONFIG_MODE_CONFIG_KEY = "config_mode"
HAPROXY_BACKEND_BALANCE_CONFIG_KEY = "balance"
HAPROXY_BACKEND_MAX_SERVERS_CONFIG_KEY = "max_servers"
HAPROXY_BACKEND_SERVICE_NAME_CONFIG_KEY = "service_name"
HAPROXY_BACKEND_SERVICE_TAG_CONFIG_KEY = "service_tag"
HAPROXY_BACKEND_SERVICE_CLUSTER_CONFIG_KEY = "service_cluster"
HAPROXY_BACKEND_SERVERS_CONFIG_KEY = "servers"
HAPROXY_BACKEND_SELECTOR_CONFIG_KEY = "selector"
HAPROXY_BACKEND_SESSION_PERSISTENCE_CONFIG_KEY = "session_persistence"

HAPROXY_HTTP_CHECK_ENABLED_CONFIG_KEY = "http_check_enabled"
HAPROXY_HTTP_CHECK_PORT_CONFIG_KEY = "http_check_port"
HAPROXY_HTTP_CHECK_PATH_CONFIG_KEY = "http_check_path"
HAPROXY_HTTP_CHECK_DISCOVERY_CONFIG_KEY = "http_check_discovery"
HAPROXY_HTTP_CHECK_SELECTOR_CONFIG_KEY = "http_check_selector"

HAPROXY_SERVICE_TYPE_DISCOVERY_CONFIG_KEY = "service_type_discovery"
HAPROXY_SERVICE_TYPE_KEEP_ORIGINAL_CONFIG_KEY = "service_type_keep_original"

HAPROXY_SERVICE_TYPE = BUILT_IN_RUNTIME_HAPROXY
HAPROXY_SERVICE_PORT_DEFAULT = 80
HAPROXY_SERVICE_PROTOCOL_TCP = "tcp"
HAPROXY_SERVICE_PROTOCOL_HTTP = "http"
HAPROXY_BACKEND_MAX_SERVERS_DEFAULT = 32
HAPROXY_BACKEND_DYNAMIC_FREE_SLOTS = 8

HAPROXY_APP_MODE_LOAD_BALANCER = "load-balancer"
HAPROXY_APP_MODE_API_GATEWAY = "api-gateway"

HAPROXY_CONFIG_MODE_DNS = "dns"
HAPROXY_CONFIG_MODE_STATIC = "static"
HAPROXY_CONFIG_MODE_DYNAMIC = "dynamic"

"""
NOTE:
1. For using dynamic config mode which uses HAProxy Runtime API with add server command:
The backend must be configured to use a dynamic load-balancing algorithm for the balance directive:
roundrobin, leastconn, first, or random.
So for other balance options, need to use DNS config mode to scale.

"""
HAPROXY_BACKEND_BALANCE_ROUNDROBIN = "roundrobin"
HAPROXY_BACKEND_BALANCE_LEASTCONN = "leastconn"
HAPROXY_BACKEND_BALANCE_FIRST = "first"
HAPROXY_BACKEND_BALANCE_RANDOM = "random"
# Takes a regular sample expression in argument.
# The expression is evaluated for each request and hashed according to the configured hash-type.
# The result of the hash is divided by the total weight of the running servers
# to designate which server will receive the request.
# This can be used in place of "source", "uri", "hdr()", "url_param()", "rdp-cookie"
HAPROXY_BACKEND_BALANCE_HASH = "hash"
# The source IP address is hashed and divided by the total weight of
# the running servers to designate which server will receive the request.
HAPROXY_BACKEND_BALANCE_SOURCE = "source"
# This algorithm hashes either the left part of the URI or the whole URI
# and divides the hash value by the total weight of the running servers.
HAPROXY_BACKEND_BALANCE_URI = "uri"
# The URL parameter specified in argument will be looked up
# in the query string of each HTTP GET request.
HAPROXY_BACKEND_BALANCE_URL_PARAM = "url_param"
# hdr(<name>) The HTTP header <name> will be looked up in each HTTP request for hash
HAPROXY_BACKEND_BALANCE_HDR = "hdr"
# rdp-cookie(<name>)
HAPROXY_BACKEND_BALANCE_RDP_COOKIE = "rdp-cookie"

# Persistence based on an HTTP cookie. This option is only available with mode http
HAPROXY_BACKEND_SESSION_PERSISTENCE_COOKIE = "cookie"
# Persistence based on the client's IP address. This option is available with mode http and mode tcp
HAPROXY_BACKEND_SESSION_PERSISTENCE_IP = "ip"

HAPROXY_BACKEND_NAME_DEFAULT = "servers"
HAPROXY_BACKEND_SERVER_BASE_NAME = "server"


def get_default_server_name(server_id):
    return get_backend_server_name(
        HAPROXY_BACKEND_SERVER_BASE_NAME, server_id)


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HAPROXY, {})


def _get_service_port(haproxy_config: Dict[str, Any]):
    return haproxy_config.get(
        HAPROXY_SERVICE_PORT_CONFIG_KEY, HAPROXY_SERVICE_PORT_DEFAULT)


def _get_service_protocol(haproxy_config):
    app_mode = _get_app_mode(haproxy_config)
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        default_protocol = HAPROXY_SERVICE_PROTOCOL_TCP
    else:
        default_protocol = HAPROXY_SERVICE_PROTOCOL_HTTP
    return haproxy_config.get(
        HAPROXY_SERVICE_PROTOCOL_CONFIG_KEY, default_protocol)


def _get_app_mode(haproxy_config):
    return haproxy_config.get(
        HAPROXY_APP_MODE_CONFIG_KEY, HAPROXY_APP_MODE_LOAD_BALANCER)


def _is_high_availability(haproxy_config: Dict[str, Any]):
    return haproxy_config.get(
        HAPROXY_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_backend_config(haproxy_config: Dict[str, Any]):
    return haproxy_config.get(
        HAPROXY_BACKEND_CONFIG_KEY, {})


def _get_backend_config_mode(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_BACKEND_CONFIG_MODE_CONFIG_KEY)


def _get_backend_dns_service_name(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_BACKEND_SERVICE_NAME_CONFIG_KEY)


def _is_backend_http_check_enabled(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_HTTP_CHECK_ENABLED_CONFIG_KEY, False)


def _get_backend_http_check_port(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_HTTP_CHECK_PORT_CONFIG_KEY)


def _get_backend_http_check_path(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_HTTP_CHECK_PATH_CONFIG_KEY)


def _is_http_check_service_discovery(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_HTTP_CHECK_DISCOVERY_CONFIG_KEY, True)


def _is_service_type_discovery(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_SERVICE_TYPE_DISCOVERY_CONFIG_KEY, True)


def _is_service_type_keep_original(backend_config: Dict[str, Any]):
    return backend_config.get(HAPROXY_SERVICE_TYPE_KEEP_ORIGINAL_CONFIG_KEY, False)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_HAPROXY)


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_HAPROXY: logs_dir}


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_backend_service_selector(
        backend_config, cluster_name):
    service_selector = get_service_selector_copy(
        backend_config, HAPROXY_BACKEND_SELECTOR_CONFIG_KEY)
    service_selector = exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_HAPROXY, cluster_name)
    return service_selector


def _discover_backend_service(
        backend_config, config_mode,
        cluster_config: Dict[str, Any]):
    cluster_name = get_cluster_name(cluster_config)
    # 1. for DNS, service_name can be used to match
    # 2. for dynamic service discovery, runtime type can be used to match
    if config_mode == HAPROXY_CONFIG_MODE_DNS:
        service_selector = {}
        service_name = _get_backend_dns_service_name(backend_config)
        service_selector = include_service_name_for_selector(
            service_selector, service_name)
    else:
        service_selector = _get_backend_service_selector(
            backend_config, cluster_name)

    return query_services(
        cluster_config, service_selector,
        discovery_type=DiscoveryType.CLUSTER)


def _discover_health_check(
        backend_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        runtime_type: str):
    health_check_service_type = get_health_check_service_type_of(runtime_type)
    service_selector = get_service_selector_copy(
        backend_config, HAPROXY_HTTP_CHECK_SELECTOR_CONFIG_KEY)
    service_selector = include_runtime_for_selector(
        service_selector, HEALTH_CHECK_RUNTIME)
    service_selector = include_service_type_for_selector(
        service_selector, health_check_service_type)
    return query_services(
        cluster_config, service_selector,
        discovery_type=DiscoveryType.CLUSTER)


def discover_http_check_on_head(
        haproxy_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        config_mode):
    backend_config = _get_backend_config(haproxy_config)
    if not _is_http_check_service_discovery(backend_config):
        return cluster_config

    if _is_backend_http_check_enabled(backend_config):
        # http check already configured by user
        return cluster_config

    # Best effort for service discovery
    backend_services = _discover_backend_service(
        backend_config, config_mode, cluster_config)
    if not backend_services:
        return cluster_config

    # if there are more than one backend services, it's hard to match service type
    if len(backend_services) > 1:
        return cluster_config

    _, backend_service = next(iter(backend_services.items()))
    # use runtime type to match service type
    runtime_type = backend_service.runtime_type
    health_check_services = _discover_health_check(
        backend_config, cluster_config, runtime_type)
    if not health_check_services:
        return cluster_config

    # Cannot decide if there are more than one health check services
    if len(health_check_services) > 1:
        return cluster_config

    _, health_check_service = next(iter(health_check_services.items()))
    cluster_config = _enable_http_check(
        cluster_config, health_check_service)
    return cluster_config


def _enable_http_check(
        cluster_config, health_check_service):
    runtime_config = get_runtime_config(cluster_config)
    haproxy_config = get_config_for_update(
        runtime_config, BUILT_IN_RUNTIME_HAPROXY)
    backend_config = get_config_for_update(
        haproxy_config, HAPROXY_BACKEND_CONFIG_KEY)
    backend_config[HAPROXY_HTTP_CHECK_ENABLED_CONFIG_KEY] = True
    health_check_port = get_health_check_port_of_service(health_check_service)
    if health_check_port:
        backend_config[HAPROXY_HTTP_CHECK_PORT_CONFIG_KEY] = health_check_port
    return cluster_config


def _is_service_type_configured(haproxy_config: Dict[str, Any]):
    service_discovery_config = get_service_discovery_config(haproxy_config)
    service_type = get_service_type_override(service_discovery_config)
    if not service_type:
        return False
    return True


def _update_service_type_config(
        cluster_config: Dict[str, Any], service_type):
    runtime_config = get_runtime_config(cluster_config)
    haproxy_config = get_config_for_update(
        runtime_config, BUILT_IN_RUNTIME_HAPROXY)
    service_discovery_config = get_service_discovery_config_for_update(
        haproxy_config)
    set_service_type_override(service_discovery_config, service_type)
    return cluster_config


def discover_service_type_on_head(
        haproxy_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        config_mode):
    backend_config = _get_backend_config(haproxy_config)
    if not _is_service_type_discovery(backend_config):
        return cluster_config

    if _is_service_type_configured(haproxy_config):
        # service type already configured by user
        return cluster_config

    # Best effort for health check service discovery
    backend_services = _discover_backend_service(
        backend_config, config_mode, cluster_config)
    if not backend_services:
        return cluster_config

    # if there are more than one backend services, it's hard to match service type
    if len(backend_services) > 1:
        return cluster_config

    _, backend_service = next(iter(backend_services.items()))
    # Use runtime type as service type here: unless if there is a need for
    # service type which is not runtime type
    if _is_service_type_keep_original(backend_config):
        service_type = backend_service.service_type
    else:
        # we can be smart here if three is health check and path
        # Convention notice: if the health check explicitly specified path
        # it is the desired service type suffix for the runtime type
        if _is_backend_http_check_enabled(backend_config):
            http_check_path = _get_backend_http_check_path(backend_config)
            service_type = get_service_type_from_health_check_path(
                backend_service.runtime_type, http_check_path)
        else:
            service_type = backend_service.runtime_type
    if service_type:
        cluster_config = _update_service_type_config(
            cluster_config, service_type)
    return cluster_config


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    haproxy_config = _get_config(runtime_config)
    app_mode = _get_app_mode(haproxy_config)
    if app_mode != HAPROXY_APP_MODE_LOAD_BALANCER:
        return cluster_config

    cluster_config = _prepare_http_check_config(cluster_config, haproxy_config)
    cluster_config = _prepare_service_type_config(cluster_config, haproxy_config)
    return cluster_config


def _prepare_http_check_config(
        cluster_config: Dict[str, Any],
        haproxy_config: Dict[str, Any]):
    backend_config = _get_backend_config(haproxy_config)
    config_mode = _get_checked_config_mode(cluster_config, backend_config)
    if config_mode == HAPROXY_CONFIG_MODE_STATIC:
        # for static mode, we will not do auto discovery health check
        return cluster_config

    cluster_config = discover_http_check_on_head(
        haproxy_config, cluster_config, config_mode)
    return cluster_config


def _prepare_service_type_config(
        cluster_config: Dict[str, Any],
        haproxy_config: Dict[str, Any]):
    backend_config = _get_backend_config(haproxy_config)
    config_mode = _get_checked_config_mode(cluster_config, backend_config)
    if config_mode == HAPROXY_CONFIG_MODE_STATIC:
        # for static mode, we will not do auto discovery service type
        return cluster_config

    cluster_config = discover_service_type_on_head(
        haproxy_config, cluster_config, config_mode)
    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    haproxy_config = _get_config(runtime_config)
    backend_config = _get_backend_config(haproxy_config)

    app_mode = _get_app_mode(haproxy_config)
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        config_mode = _get_checked_config_mode(config, backend_config)
        if config_mode == HAPROXY_CONFIG_MODE_STATIC:
            if not backend_config.get(
                    HAPROXY_BACKEND_SERVERS_CONFIG_KEY):
                raise ValueError(
                    "Static servers must be provided with config mode: static.")
        elif config_mode == HAPROXY_CONFIG_MODE_DNS:
            service_name = _get_backend_dns_service_name(backend_config)
            if not service_name:
                raise ValueError(
                    "Service name must be configured for config mode: dns.")
    else:
        config_mode = _get_backend_config_mode(backend_config)
        if config_mode and config_mode != HAPROXY_CONFIG_MODE_DYNAMIC:
            raise ValueError(
                "API Gateway mode support only dynamic config mode.")

        # API gateway should use http protocol
        service_protocol = haproxy_config.get(
            HAPROXY_SERVICE_PROTOCOL_CONFIG_KEY)
        if service_protocol and service_protocol != HAPROXY_SERVICE_PROTOCOL_HTTP:
            raise ValueError(
                "API Gateway mode should use http protocol.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    haproxy_config = _get_config(runtime_config)

    high_availability = _is_high_availability(haproxy_config)
    if high_availability:
        runtime_envs["HAPROXY_HIGH_AVAILABILITY"] = high_availability

    runtime_envs["HAPROXY_FRONTEND_PORT"] = _get_service_port(haproxy_config)
    runtime_envs["HAPROXY_FRONTEND_PROTOCOL"] = _get_service_protocol(haproxy_config)

    app_mode = _get_app_mode(haproxy_config)
    runtime_envs["HAPROXY_APP_MODE"] = app_mode

    # Backend discovery support mode for:
    # 1. DNS (given static service name and optionally service tag)
    # 2. Static: a static list of servers
    # 3. Dynamic: a dynamic discovered service (services)
    backend_config = _get_backend_config(haproxy_config)
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        _with_runtime_envs_for_load_balancer(
            config, backend_config, runtime_envs)
    elif app_mode == HAPROXY_APP_MODE_API_GATEWAY:
        _with_runtime_envs_for_api_gateway(
            config, backend_config, runtime_envs)
    else:
        raise ValueError(
            "Invalid application mode: {}. "
            "Must be load-balancer or api-gateway.".format(app_mode))

    runtime_envs["HAPROXY_BACKEND_MAX_SERVERS"] = backend_config.get(
        HAPROXY_BACKEND_MAX_SERVERS_CONFIG_KEY,
        HAPROXY_BACKEND_MAX_SERVERS_DEFAULT)

    balance = backend_config.get(HAPROXY_BACKEND_BALANCE_CONFIG_KEY)
    if not balance:
        balance = HAPROXY_BACKEND_BALANCE_ROUNDROBIN
    runtime_envs["HAPROXY_BACKEND_BALANCE"] = balance
    return runtime_envs


def _get_default_load_balancer_config_mode(config, backend_config):
    cluster_runtime_config = get_runtime_config(config)
    if backend_config.get(
            HAPROXY_BACKEND_SERVERS_CONFIG_KEY):
        # if there are static servers configured
        config_mode = HAPROXY_CONFIG_MODE_STATIC
    elif get_service_discovery_runtime(cluster_runtime_config):
        # if there is service selector defined
        if backend_config.get(
                HAPROXY_BACKEND_SELECTOR_CONFIG_KEY):
            config_mode = HAPROXY_CONFIG_MODE_DYNAMIC
        elif _get_backend_dns_service_name(backend_config):
            config_mode = HAPROXY_CONFIG_MODE_DNS
        else:
            config_mode = HAPROXY_CONFIG_MODE_DYNAMIC
    else:
        config_mode = HAPROXY_CONFIG_MODE_STATIC
    return config_mode


def _get_checked_config_mode(config, backend_config):
    config_mode = _get_backend_config_mode(backend_config)
    if not config_mode:
        config_mode = _get_default_load_balancer_config_mode(
            config, backend_config)
    return config_mode


def _with_runtime_envs_for_load_balancer(config, backend_config, runtime_envs):
    config_mode = _get_checked_config_mode(config, backend_config)
    if config_mode == HAPROXY_CONFIG_MODE_DNS:
        _with_runtime_envs_for_dns(backend_config, runtime_envs)
    elif config_mode == HAPROXY_CONFIG_MODE_STATIC:
        _with_runtime_envs_for_static(backend_config, runtime_envs)
    else:
        _with_runtime_envs_for_dynamic(backend_config, runtime_envs)
    runtime_envs["HAPROXY_CONFIG_MODE"] = config_mode


def _with_runtime_envs_for_dns(backend_config, runtime_envs):
    service_name = _get_backend_dns_service_name(backend_config)
    if not service_name:
        raise ValueError(
            "Service name must be configured for config mode: dns.")

    service_tag = backend_config.get(
        HAPROXY_BACKEND_SERVICE_TAG_CONFIG_KEY)
    service_cluster = backend_config.get(
        HAPROXY_BACKEND_SERVICE_CLUSTER_CONFIG_KEY)

    service_dns_name = get_rfc2782_service_dns_name(
        service_name, service_tag, service_cluster)
    runtime_envs["HAPROXY_BACKEND_SERVICE_DNS_NAME"] = service_dns_name


def _with_runtime_envs_for_static(backend_config, runtime_envs):
    pass


def _with_runtime_envs_for_dynamic(backend_config, runtime_envs):
    pass


def _with_http_check(backend_config, envs=None):
    if envs is None:
        envs = {}
    # set HAPROXY_HTTP_CHECK, HAPROXY_HTTP_CHECK_PORT, HAPROXY_HTTP_CHECK_PATH
    http_check_enabled = _is_backend_http_check_enabled(backend_config)
    envs["HAPROXY_HTTP_CHECK"] = http_check_enabled
    if http_check_enabled:
        http_check_port = _get_backend_http_check_port(backend_config)
        if http_check_port:
            envs["HAPROXY_HTTP_CHECK_PORT"] = http_check_port
        http_check_path = _get_backend_http_check_path(backend_config)
        if http_check_path:
            if not http_check_path.startswith("/"):
                http_check_path = "/" + http_check_path
            envs["HAPROXY_HTTP_CHECK_PATH"] = http_check_path
    return envs


def _node_configure(runtime_config, head: bool):
    haproxy_config = _get_config(runtime_config)
    backend_config = _get_backend_config(haproxy_config)

    # because http check may be configured in prepare_on_head
    # we need to export in node_configure instead of with_environment_variables
    envs = _with_http_check(backend_config)
    export_environment_variables(envs)


def _get_default_api_gateway_config_mode(config, backend_config):
    cluster_runtime_config = get_runtime_config(config)
    if not get_service_discovery_runtime(cluster_runtime_config):
        raise ValueError(
            "Service discovery runtime is needed for API gateway mode.")

    # for simplicity, the API gateway operates with the service selector
    config_mode = HAPROXY_CONFIG_MODE_DYNAMIC
    return config_mode


def _with_runtime_envs_for_api_gateway(config, backend_config, runtime_envs):
    config_mode = _get_backend_config_mode(backend_config)
    if not config_mode:
        config_mode = _get_default_api_gateway_config_mode(
            config, backend_config)

    runtime_envs["HAPROXY_CONFIG_MODE"] = config_mode


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    haproxy_config = _get_config(runtime_config)
    service_port = _get_service_port(haproxy_config)
    service_protocol = _get_service_protocol(haproxy_config)

    endpoint_url = http_address_string(
        head_host, service_port) if (
            service_protocol == HAPROXY_SERVICE_PROTOCOL_HTTP) else address_string(
        head_host, service_port)
    endpoints = {
        "haproxy": {
            "name": "HAProxy",
            "url": endpoint_url
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    haproxy_config = _get_config(runtime_config)
    service_port = _get_service_port(haproxy_config)
    service_ports = {
        "haproxy": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    haproxy_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(haproxy_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HAPROXY_SERVICE_TYPE)
    service_port = _get_service_port(haproxy_config)
    service_type = get_service_type_override(service_discovery_config)
    if not service_type:
        service_type = HAPROXY_SERVICE_TYPE
    services = {
        service_name: define_runtime_service_on_head_or_all(
            service_type,
            service_discovery_config, service_port,
            _is_high_availability(haproxy_config),
            features=[SERVICE_DISCOVERY_FEATURE_LOAD_BALANCER])
    }
    return services
