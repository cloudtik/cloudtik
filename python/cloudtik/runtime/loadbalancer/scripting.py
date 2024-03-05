from shlex import quote

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_LOAD_BALANCER
from cloudtik.core._private.service_discovery.utils import serialize_service_selector
from cloudtik.core._private.util.core_utils import exec_with_output, serialize_config, service_address_from_string
from cloudtik.core._private.util.runtime_utils import \
    get_runtime_config_from_node, get_runtime_cluster_name, get_runtime_workspace_name
from cloudtik.runtime.common.leader_election.runtime_leader_election import get_runtime_leader_election_url
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.loadbalancer.provider_api import get_load_balancer_manager, LoadBalancerBackendService
from cloudtik.runtime.loadbalancer.utils import _get_config, _get_backend_config, \
    _get_logs_dir, _get_backend_service_selector, _get_service_identifier, _get_provider_config, \
    _get_backend_config_mode, LOAD_BALANCER_CONFIG_MODE_STATIC, _get_backend_services, \
    LOAD_BALANCER_BACKEND_SERVICE_PORT_CONFIG_KEY, LOAD_BALANCER_BACKEND_SERVICE_PROTOCOL_CONFIG_KEY, \
    LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_NAME_CONFIG_KEY, LOAD_BALANCER_BACKEND_SERVICE_SERVERS_CONFIG_KEY, \
    LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PROTOCOL_CONFIG_KEY, \
    LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PORT_CONFIG_KEY, LOAD_BALANCER_BACKEND_SERVICE_ROUTE_PATH_CONFIG_KEY, \
    LOAD_BALANCER_BACKEND_SERVICE_SERVICE_PATH_CONFIG_KEY, LOAD_BALANCER_BACKEND_SERVICE_DEFAULT_SERVICE_CONFIG_KEY, \
    LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_SCHEME_CONFIG_KEY

LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)

    backend_config = _get_backend_config(load_balancer_config)
    config_mode = _get_backend_config_mode(backend_config)
    if config_mode == LOAD_BALANCER_CONFIG_MODE_STATIC:
        provider_config = _get_provider_config(load_balancer_config)

        # build backend services based on static configuration
        backend_services = _get_backend_services_from_config(backend_config)

        workspace_name = get_runtime_workspace_name()
        load_balancer_manager = get_load_balancer_manager(
            provider_config, workspace_name)
        load_balancer_manager.update(backend_services)


def _get_backend_services_from_config(backend_config):
    backend_services = {}
    backend_services_config = _get_backend_services(backend_config)
    if not backend_services_config:
        return backend_services

    for service_name, backend_service_config in backend_services_config.items():
        backend_service = _get_backend_service_from_config(
            service_name, backend_service_config)
        if backend_service is not None:
            backend_services[service_name] = backend_service
    return backend_services


def _get_backend_service_from_config(service_name, backend_service_config):
    servers = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_SERVERS_CONFIG_KEY)
    if not servers:
        return None
    backend_servers = {}
    for server in servers:
        service_address = service_address_from_string(server, None)
        backend_server = {
            "address": service_address[0],
            "port": service_address[1],
        }
        backend_servers[service_address] = backend_server
    if not backend_servers:
        return None

    protocol = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_PROTOCOL_CONFIG_KEY)
    port = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_PORT_CONFIG_KEY)
    load_balancer_name = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_NAME_CONFIG_KEY)
    load_balancer_scheme = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_SCHEME_CONFIG_KEY)
    load_balancer_protocol = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PROTOCOL_CONFIG_KEY)
    load_balancer_port = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_LOAD_BALANCER_PORT_CONFIG_KEY)

    route_path = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_ROUTE_PATH_CONFIG_KEY)
    service_path = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_SERVICE_PATH_CONFIG_KEY)
    default_service = backend_service_config.get(
        LOAD_BALANCER_BACKEND_SERVICE_DEFAULT_SERVICE_CONFIG_KEY, False)

    return LoadBalancerBackendService(
        service_name, backend_servers,
        protocol=protocol, port=port,
        load_balancer_name=load_balancer_name,
        load_balancer_scheme=load_balancer_scheme,
        load_balancer_protocol=load_balancer_protocol,
        load_balancer_port=load_balancer_port,
        route_path=route_path, service_path=service_path,
        default_service=default_service)


def start_controller(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)

    backend_config = _get_backend_config(load_balancer_config)
    cluster_name = get_runtime_cluster_name()
    workspace_name = get_runtime_workspace_name()
    service_selector = _get_backend_service_selector(
        backend_config, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.loadbalancer.controller.LoadBalancerController"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    coordinator_url = get_runtime_leader_election_url(
        runtime_config, BUILT_IN_RUNTIME_LOAD_BALANCER)
    if coordinator_url:
        cmd += ["coordinator_url={}".format(
            quote(coordinator_url))]
    cmd += ["interval={}".format(
        LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]

    provider_config = _get_provider_config(load_balancer_config)
    provider_config_str = serialize_config(provider_config) if provider_config else None
    if provider_config_str:
        cmd += ["provider_config={}".format(provider_config_str)]
    if workspace_name:
        cmd += ["workspace_name={}".format(workspace_name)]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_controller():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)
