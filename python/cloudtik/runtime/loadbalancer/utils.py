import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_LOAD_BALANCER
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import exclude_runtime_of_cluster, \
    get_service_selector_copy
from cloudtik.core._private.util.core_utils import export_environment_variables, get_config_copy, get_config_for_update
from cloudtik.core._private.util.service.service_daemon import get_service_daemon_process_file
from cloudtik.core._private.utils import get_runtime_config, get_runtime_config_for_update
from cloudtik.runtime.loadbalancer.provider_api import bootstrap_provider_config

LOAD_BALANCER_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

LOAD_BALANCER_PROVIDER_CONFIG_KEY = "provider"

LOAD_BALANCER_BACKEND_CONFIG_KEY = "backend"
LOAD_BALANCER_BACKEND_CONFIG_MODE_CONFIG_KEY = "config_mode"
LOAD_BALANCER_BACKEND_SELECTOR_CONFIG_KEY = "selector"
LOAD_BALANCER_BACKEND_SERVICES_CONFIG_KEY = "services"

LOAD_BALANCER_BACKEND_SERVICE_PROTOCOL_CONFIG_KEY = "protocol"
LOAD_BALANCER_BACKEND_SERVICE_PORT_CONFIG_KEY = "port"
LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_NAME_CONFIG_KEY = "load_balancer_name"
LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_SCHEME_CONFIG_KEY = "load_balancer_scheme"
LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PROTOCOL_CONFIG_KEY = "load_balancer_protocol"
LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PORT_CONFIG_KEY = "load_balancer_port"
LOAD_BALANCER_BACKEND_SERVICE_SERVICE_PATH_CONFIG_KEY = "service_path"
LOAD_BALANCER_BACKEND_SERVICE_ROUTE_PATH_CONFIG_KEY = "route_path"
LOAD_BALANCER_BACKEND_SERVICE_DEFAULT_SERVICE_CONFIG_KEY = "default_service"
LOAD_BALANCER_BACKEND_SERVICE_SERVERS_CONFIG_KEY = "servers"

LOAD_BALANCER_HTTP_CHECK_ENABLED_CONFIG_KEY = "http_check_enabled"
LOAD_BALANCER_HTTP_CHECK_PORT_CONFIG_KEY = "http_check_port"
LOAD_BALANCER_HTTP_CHECK_PATH_CONFIG_KEY = "http_check_path"
LOAD_BALANCER_HTTP_CHECK_DISCOVERY_CONFIG_KEY = "http_check_discovery"
LOAD_BALANCER_HTTP_CHECK_SELECTOR_CONFIG_KEY = "http_check_selector"

LOAD_BALANCER_SERVICE_TYPE = BUILT_IN_RUNTIME_LOAD_BALANCER
LOAD_BALANCER_SERVICE_PORT_DEFAULT = 80
LOAD_BALANCER_SERVICE_PROTOCOL_TCP = "tcp"
LOAD_BALANCER_SERVICE_PROTOCOL_TLS = "tls"

LOAD_BALANCER_CONFIG_MODE_STATIC = "static"
LOAD_BALANCER_CONFIG_MODE_DYNAMIC = "dynamic"


def _get_service_identifier():
    return "{}-controller".format(BUILT_IN_RUNTIME_LOAD_BALANCER)


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_LOAD_BALANCER, {})


def _get_config_for_update(cluster_config):
    runtime_config = get_runtime_config_for_update(cluster_config)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_LOAD_BALANCER)


def _is_high_availability(load_balancer_config: Dict[str, Any]):
    return load_balancer_config.get(
        LOAD_BALANCER_HIGH_AVAILABILITY_CONFIG_KEY, True)


def _get_provider_config(load_balancer_config: Dict[str, Any]):
    return load_balancer_config.get(
        LOAD_BALANCER_PROVIDER_CONFIG_KEY, {})


def _get_backend_config(load_balancer_config: Dict[str, Any]):
    return load_balancer_config.get(
        LOAD_BALANCER_BACKEND_CONFIG_KEY, {})


def _get_backend_config_mode(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_BACKEND_CONFIG_MODE_CONFIG_KEY)


def _get_backend_services(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_BACKEND_SERVICES_CONFIG_KEY)


def _is_backend_http_check_enabled(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_HTTP_CHECK_ENABLED_CONFIG_KEY, False)


def _get_backend_http_check_port(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_HTTP_CHECK_PORT_CONFIG_KEY)


def _get_backend_http_check_path(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_HTTP_CHECK_PATH_CONFIG_KEY)


def _is_http_check_service_discovery(backend_config: Dict[str, Any]):
    return backend_config.get(LOAD_BALANCER_HTTP_CHECK_DISCOVERY_CONFIG_KEY, True)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_LOAD_BALANCER)


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_LOAD_BALANCER: logs_dir}


def _get_runtime_processes():
    identifier = _get_service_identifier()
    pid_file = get_service_daemon_process_file(identifier)
    runtime_processes = [
        [pid_file, None, "Load Balancer Controller", "node"],
    ]
    return runtime_processes


def _get_backend_service_selector(
        backend_config, cluster_name):
    service_selector = get_service_selector_copy(
        backend_config, LOAD_BALANCER_BACKEND_SELECTOR_CONFIG_KEY)
    service_selector = exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_LOAD_BALANCER, cluster_name)
    return service_selector


def _prepare_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    load_balancer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(load_balancer_config)
    config_mode = _get_backend_config_mode(backend_config)
    if not config_mode:
        # do update
        load_balancer_config = _get_config_for_update(cluster_config)
        backend_config = get_config_for_update(
            load_balancer_config, LOAD_BALANCER_BACKEND_CONFIG_KEY)
        config_mode = _get_checked_config_mode(
            cluster_config, backend_config)
        backend_config[LOAD_BALANCER_BACKEND_CONFIG_MODE_CONFIG_KEY] = config_mode

    return cluster_config


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    # bootstrap provider config for backend controller
    load_balancer_config = _get_config(runtime_config)
    provider_config = get_config_copy(
        load_balancer_config, LOAD_BALANCER_PROVIDER_CONFIG_KEY)
    provider_config = bootstrap_provider_config(
        cluster_config, provider_config)

    # update the provider config to cluster config
    load_balancer_config = _get_config_for_update(cluster_config)
    load_balancer_config[LOAD_BALANCER_PROVIDER_CONFIG_KEY] = provider_config
    return cluster_config


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    load_balancer_config = _get_config(runtime_config)
    cluster_config = _prepare_http_check_config(cluster_config, load_balancer_config)
    return cluster_config


def _prepare_http_check_config(
        cluster_config: Dict[str, Any],
        load_balancer_config: Dict[str, Any]):
    backend_config = _get_backend_config(load_balancer_config)
    config_mode = _get_checked_config_mode(cluster_config, backend_config)
    if config_mode == LOAD_BALANCER_CONFIG_MODE_STATIC:
        # for static mode, we will not do auto discovery health check
        return cluster_config

    # TODO: integrate with http service check?
    # cluster_config = discover_http_check_on_head(
    #    load_balancer_config, cluster_config, config_mode)
    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    load_balancer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(load_balancer_config)

    config_mode = _get_checked_config_mode(config, backend_config)
    if config_mode == LOAD_BALANCER_CONFIG_MODE_STATIC:
        if not backend_config.get(
                LOAD_BALANCER_BACKEND_SERVICES_CONFIG_KEY):
            raise ValueError(
                "Services must be provided with config mode: static.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    load_balancer_config = _get_config(runtime_config)
    high_availability = _is_high_availability(load_balancer_config)
    if high_availability:
        runtime_envs["LOAD_BALANCER_HIGH_AVAILABILITY"] = high_availability

    # Backend discovery support mode for:
    # 2. Static: a static list of servers
    # 3. Dynamic: a dynamic discovered service (services)
    backend_config = _get_backend_config(load_balancer_config)
    config_mode = _get_checked_config_mode(config, backend_config)
    if config_mode == LOAD_BALANCER_CONFIG_MODE_STATIC:
        _with_runtime_envs_for_static(backend_config, runtime_envs)
    else:
        _with_runtime_envs_for_dynamic(backend_config, runtime_envs)
    runtime_envs["LOAD_BALANCER_CONFIG_MODE"] = config_mode

    return runtime_envs


def _get_default_load_balancer_config_mode(config, backend_config):
    cluster_runtime_config = get_runtime_config(config)
    if backend_config.get(
            LOAD_BALANCER_BACKEND_SERVICES_CONFIG_KEY):
        # if there are static services configured
        config_mode = LOAD_BALANCER_CONFIG_MODE_STATIC
    elif get_service_discovery_runtime(cluster_runtime_config):
        # if there is service selector defined
        config_mode = LOAD_BALANCER_CONFIG_MODE_DYNAMIC
    else:
        config_mode = LOAD_BALANCER_CONFIG_MODE_STATIC
    return config_mode


def _get_checked_config_mode(config, backend_config):
    config_mode = _get_backend_config_mode(backend_config)
    if not config_mode:
        config_mode = _get_default_load_balancer_config_mode(
            config, backend_config)
    return config_mode


def _with_runtime_envs_for_static(backend_config, runtime_envs):
    pass


def _with_runtime_envs_for_dynamic(backend_config, runtime_envs):
    pass


def _with_http_check(backend_config, envs=None):
    if envs is None:
        envs = {}
    # set LOAD_BALANCER_HTTP_CHECK, LOAD_BALANCER_HTTP_CHECK_PORT, LOAD_BALANCER_HTTP_CHECK_PATH
    http_check_enabled = _is_backend_http_check_enabled(backend_config)
    envs["LOAD_BALANCER_HTTP_CHECK"] = http_check_enabled
    if http_check_enabled:
        http_check_port = _get_backend_http_check_port(backend_config)
        if http_check_port:
            envs["LOAD_BALANCER_HTTP_CHECK_PORT"] = http_check_port
        http_check_path = _get_backend_http_check_path(backend_config)
        if http_check_path:
            if not http_check_path.startswith("/"):
                http_check_path = "/" + http_check_path
            envs["LOAD_BALANCER_HTTP_CHECK_PATH"] = http_check_path
    return envs


def _node_configure(runtime_config, head: bool):
    load_balancer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(load_balancer_config)

    # because http check may be configured in prepare_on_head
    # we need to export in node_configure instead of with_environment_variables
    envs = _with_http_check(backend_config)
    export_environment_variables(envs)
