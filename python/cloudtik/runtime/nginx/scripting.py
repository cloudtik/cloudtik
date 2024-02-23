import os
from shlex import quote

from cloudtik.core._private.util.core_utils import exec_with_call, exec_with_output, remove_files, get_address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_NGINX
from cloudtik.core._private.util.runtime_utils import get_runtime_value, get_runtime_config_from_node, \
    get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import exclude_runtime_of_cluster, \
    serialize_service_selector, get_service_selector_copy
from cloudtik.runtime.common.service_discovery.load_balancer import ApplicationBackendService
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.nginx.utils import _get_config, NGINX_APP_MODE_LOAD_BALANCER, NGINX_CONFIG_MODE_STATIC, \
    _get_home_dir, _get_backend_config, NGINX_BACKEND_SERVERS_CONFIG_KEY, NGINX_LOAD_BALANCER_UPSTREAM_NAME, \
    NGINX_BACKEND_BALANCE_ROUND_ROBIN, NGINX_CONFIG_MODE_DNS, NGINX_BACKEND_SELECTOR_CONFIG_KEY, _get_logs_dir

NGINX_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    nginx_config = _get_config(runtime_config)

    app_mode = get_runtime_value("NGINX_APP_MODE")
    config_mode = get_runtime_value("NGINX_CONFIG_MODE")
    if app_mode == NGINX_APP_MODE_LOAD_BALANCER:
        if config_mode == NGINX_CONFIG_MODE_STATIC:
            _configure_static_backend(nginx_config)


def _get_upstreams_config_dir():
    home_dir = _get_home_dir()
    return os.path.join(
        home_dir, "conf", "upstreams")


def _get_routers_config_dir():
    return os.path.join(
        _get_home_dir(), "conf", "routers")


def _configure_static_backend(nginx_config):
    backend_config = _get_backend_config(nginx_config)
    servers = backend_config.get(
        NGINX_BACKEND_SERVERS_CONFIG_KEY)
    if not servers:
        raise RuntimeError(
            "Static servers must be provided with config mode: static.")
    balance_method = get_runtime_value("NGINX_BACKEND_BALANCE")
    _save_load_balancer_upstream(
        servers, balance_method)


def _save_load_balancer_upstream(servers, balance_method):
    upstreams_dir = _get_upstreams_config_dir()
    config_file = os.path.join(
        upstreams_dir, "load-balancer.conf")
    _save_upstream_config(
        config_file, NGINX_LOAD_BALANCER_UPSTREAM_NAME,
        servers, balance_method)


def _save_load_balancer_router():
    routers_dir = _get_routers_config_dir()
    config_file = os.path.join(
        routers_dir, "load-balancer.conf")
    _save_router_config(
        config_file, "/", NGINX_LOAD_BALANCER_UPSTREAM_NAME)


def _save_router_config(router_file, location, backend_name):
    with open(router_file, "w") as f:
        # for each backend, we generate a location block
        f.write("location " + location + " {\n")
        f.write(f"    proxy_pass http://{backend_name};\n")
        f.write("}\n")


def _save_upstream_config(
        upstream_config_file, backend_name,
        servers, balance_method):
    with open(upstream_config_file, "w") as f:
        # upstream block
        f.write("upstream " + backend_name + " {\n")
        if balance_method and balance_method != NGINX_BACKEND_BALANCE_ROUND_ROBIN:
            f.write(f"    {balance_method};\n")
        for server in servers:
            server_line = f"    server {server} max_fails=10 fail_timeout=30s;\n"
            f.write(server_line)
        # end upstream block
        f.write("}\n")


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_NGINX)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    nginx_config = _get_config(runtime_config)

    app_mode = get_runtime_value("NGINX_APP_MODE")
    if app_mode == NGINX_APP_MODE_LOAD_BALANCER:
        discovery_class = "DiscoverBackendService"
    else:
        config_mode = get_runtime_value("NGINX_CONFIG_MODE")
        if config_mode == NGINX_CONFIG_MODE_DNS:
            discovery_class = "DiscoverAPIGatewayDNSBackendService"
        else:
            discovery_class = "DiscoverAPIGatewayBackendService"

    backend_config = _get_backend_config(nginx_config)
    service_selector = get_service_selector_copy(
        backend_config, NGINX_BACKEND_SELECTOR_CONFIG_KEY)
    cluster_name = get_runtime_cluster_name()
    service_selector = exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_NGINX, cluster_name)

    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.nginx.discovery.{}".format(
        discovery_class)]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        NGINX_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]

    balance_method = get_runtime_value("NGINX_BACKEND_BALANCE")
    if balance_method:
        cmd += ["balance_method={}".format(
            quote(balance_method))]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)


def update_load_balancer_configuration(
        backend_servers, balance_method):
    # write load balancer upstream config file
    servers = [get_address_string(
        server_address[0], server_address[1]
    ) for _, server_address in backend_servers.items()]

    _save_load_balancer_upstream(servers, balance_method)
    _save_load_balancer_router()

    # the upstream config is changed, reload the service
    exec_with_call("sudo service nginx reload")


class APIGatewayBackendService(ApplicationBackendService):
    def __init__(
            self, service_name, backend_servers,
            route_path=None, service_path=None, default_service=False):
        super().__init__(
            service_name, route_path, service_path, default_service)
        self.backend_servers = backend_servers


def update_api_gateway_dynamic_backends(
        api_gateway_backends, balance_method):
    # sort to make the order to the backends are always the same
    sorted_api_gateway_backends = sorted(api_gateway_backends.items())

    # write upstreams config
    _update_api_gateway_dynamic_upstreams(
        sorted_api_gateway_backends, balance_method)
    # write api-gateway config
    _update_api_gateway_dynamic_routers(
        sorted_api_gateway_backends)

    # Need reload nginx if there is new backend added
    exec_with_call("sudo service nginx reload")


def _update_api_gateway_dynamic_upstreams(
        sorted_api_gateway_backends, balance_method):
    upstreams_dir = _get_upstreams_config_dir()
    remove_files(upstreams_dir)

    for backend_name, backend_service in sorted_api_gateway_backends:
        upstream_config_file = os.path.join(
            upstreams_dir, "{}.conf".format(backend_name))
        backend_servers = backend_service.backend_servers
        servers = [get_address_string(
            server_address[0], server_address[1]
        ) for _, server_address in backend_servers.items()]
        _save_upstream_config(
            upstream_config_file, backend_name,
            servers, balance_method)


def _update_api_gateway_dynamic_routers(
        sorted_api_gateway_backends):
    routers_dir = _get_routers_config_dir()
    remove_files(routers_dir)

    for backend_name, backend_service in sorted_api_gateway_backends:
        router_file = os.path.join(
            routers_dir, "{}.conf".format(backend_name))
        route_path = backend_service.get_route_path()
        _save_api_gateway_router_config(
            router_file, route_path, backend_name,
            service_path=backend_service.get_service_path())


def _save_api_gateway_router_config(
        router_file, route_path, backend_name, service_path=None):
    with open(router_file, "w") as f:
        # IMPORTANT NOTE:
        # the trailing slash (uri) in proxy_pass http://backend/ is key
        # for striping and replace with the uri
        target_path = "/"
        if service_path:
            target_path = service_path + target_path
        if route_path.endswith('/'):
            # if the route path ends with /, we don't need two location blocks
            # /abc/ will use /abc/ to match (it will not match /abc)
            # / will use / to match
            f.write("location " + route_path + " {\n")
        else:
            # we generate two location blocks
            # one for exact match /abc and redirect to /abc/
            # and one for prefix match with /abc/ and mapping to the target path
            f.write("location = " + route_path + " {\n")
            f.write("    return 302 " + route_path + "/;\n")
            f.write("}\n")
            f.write("location " + route_path + "/ {\n")
        f.write(f"    proxy_pass http://{backend_name}{target_path};\n")
        f.write("}\n")


class APIGatewayDNSBackendService(ApplicationBackendService):
    def __init__(
            self, service_name, service_port, service_dns_name,
            route_path=None, service_path=None, default_service=False):
        super().__init__(
            service_name, route_path, service_path, default_service)
        self.service_port = service_port
        self.service_dns_name = service_dns_name


def update_api_gateway_dns_backends(
        api_gateway_backends):
    routers_dir = _get_routers_config_dir()
    remove_files(routers_dir)

    # sort to make the order to the backends are always the same
    sorted_api_gateway_backends = sorted(api_gateway_backends.items())

    for backend_name, backend_service in sorted_api_gateway_backends:
        router_file = os.path.join(
            routers_dir, "{}.conf".format(backend_name))

        service_port = backend_service.service_port
        service_dns_name = backend_service.service_dns_name
        route_path = backend_service.get_route_path()

        _save_api_gateway_dns_router_config(
            router_file, route_path, backend_name,
            service_dns_name, service_port,
            service_path=backend_service.get_service_path())

    # Need reload nginx if there is new backend added
    exec_with_call("sudo service nginx reload")


def _save_api_gateway_dns_router_config(
        router_file, route_path, backend_name,
        service_dns_name, service_port, service_path=None):
    variable_name = backend_name.replace('-', '_')
    with open(router_file, "w") as f:
        # IMPORTANT NOTE:
        # When the variable is used in proxy pass, it handled specially
        # see nginx.org/r/proxy_pass
        # In some cases, the part of a request URI to be replaced cannot be determined:
        # When variables are used in proxy_pass: In this case, if URI is specified in
        # the directive, it is passed to the server as is, replacing the original request URI.
        target_path = "/$1"
        if service_path:
            target_path = service_path + target_path
        if route_path.endswith('/'):
            # if the route path ends with /, we don't need two location blocks
            # /abc/ will use /abc/ to match (it will not match /abc)
            # / will use / to match
            f.write("location ~ ^" + route_path + "(.*)$ {\n")
        else:
            # we generate two location blocks
            # one for exact match /abc and redirect to /abc/
            # and one for prefix match with /abc/xyz using regex and set the target uri
            f.write("location = " + route_path + " {\n")
            f.write("    return 302 " + route_path + "/;\n")
            f.write("}\n")
            f.write("location ~ ^" + route_path + "/(.*)$ {\n")
        f.write(f"    set ${variable_name}_servers {service_dns_name};\n")
        f.write(f"    proxy_pass http://${variable_name}_servers:{service_port}{target_path};\n")
        f.write("}\n")
