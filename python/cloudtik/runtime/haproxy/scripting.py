import os
import shutil
from shlex import quote

from cloudtik.core._private.util.core_utils import exec_with_output, exec_with_call
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HAPROXY
from cloudtik.core._private.util.runtime_utils import \
    get_runtime_config_from_node, get_runtime_value, get_runtime_node_ip, \
    get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import serialize_service_selector
from cloudtik.runtime.common.service_discovery.load_balancer import ApplicationBackendService
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.haproxy.utils import _get_config, HAPROXY_APP_MODE_LOAD_BALANCER, HAPROXY_CONFIG_MODE_STATIC, \
    HAPROXY_BACKEND_SERVERS_CONFIG_KEY, _get_home_dir, _get_backend_config, get_default_server_name, \
    HAPROXY_SERVICE_PORT_DEFAULT, HAPROXY_SERVICE_PROTOCOL_HTTP, \
    HAPROXY_BACKEND_NAME_DEFAULT, HAPROXY_BACKEND_DYNAMIC_FREE_SLOTS, _get_logs_dir, _get_backend_service_selector

HAPROXY_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    haproxy_config = _get_config(runtime_config)

    app_mode = get_runtime_value("HAPROXY_APP_MODE")
    config_mode = get_runtime_value("HAPROXY_CONFIG_MODE")
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        if config_mode == HAPROXY_CONFIG_MODE_STATIC:
            _configure_static_backend(haproxy_config)


def _configure_static_backend(haproxy_config):
    backend_config = _get_backend_config(haproxy_config)
    servers = backend_config.get(
        HAPROXY_BACKEND_SERVERS_CONFIG_KEY)
    if servers:
        home_dir = _get_home_dir()
        config_file = os.path.join(
            home_dir, "conf", "haproxy.cfg")
        with open(config_file, "a") as f:
            for server_id, server in enumerate(servers, start=1):
                server_name = get_default_server_name(server_id)
                f.write("    server {} {} check\n".format(
                    server_name, server))


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_HAPROXY)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    haproxy_config = _get_config(runtime_config)

    app_mode = get_runtime_value("HAPROXY_APP_MODE")
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        discovery_class = "DiscoverBackendService"
    else:
        discovery_class = "DiscoverAPIGatewayBackendServers"

    backend_config = _get_backend_config(haproxy_config)
    cluster_name = get_runtime_cluster_name()
    service_selector = _get_backend_service_selector(
        backend_config, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.haproxy.discovery.{}".format(
        discovery_class)]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        HAPROXY_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]
    if app_mode == HAPROXY_APP_MODE_LOAD_BALANCER:
        cmd += ["backend_name={}".format(HAPROXY_BACKEND_NAME_DEFAULT)]
    else:
        # the bind_ip, bind_port and balance type
        bind_ip = get_runtime_node_ip()
        bind_port = get_runtime_value("HAPROXY_FRONTEND_PORT")
        balance_method = get_runtime_value("HAPROXY_BACKEND_BALANCE")
        cmd += ["bind_ip={}".format(bind_ip)]
        cmd += ["bind_port={}".format(bind_port)]
        cmd += ["balance_method={}".format(
            quote(balance_method))]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)


def _get_backend_server_block(backend_servers):
    backend_server_block = ""
    i = 0
    for backend_server in backend_servers:
        i += 1
        server_name = get_default_server_name(i)
        backend_server_block += "    server %s %s:%s check\n" % (
            server_name,
            backend_server[0], backend_server[1])
    for disabled_slot in range(0, HAPROXY_BACKEND_DYNAMIC_FREE_SLOTS):
        i += 1
        server_name = get_default_server_name(i)
        backend_server_block += "    server %s 0.0.0.0:80 check disabled\n" % (
            server_name)
    return backend_server_block


def update_configuration(backend_servers):
    # write haproxy config file
    conf_dir = os.path.join(_get_home_dir(), "conf")
    template_file = os.path.join(
        conf_dir, "haproxy-template.cfg")
    working_file = os.path.join(
        conf_dir, "haproxy-working.cfg")
    shutil.copyfile(template_file, working_file)

    backend_server_block = _get_backend_server_block(
        backend_servers)
    with open(working_file, "a") as f:
        f.write(backend_server_block)

    config_file = os.path.join(
        conf_dir, "haproxy.cfg")
    # move overwritten
    shutil.move(working_file, config_file)


class APIGatewayBackendService(ApplicationBackendService):
    def __init__(
            self, service_name, backend_servers,
            route_path=None, service_path=None, default_service=False):
        super().__init__(
            service_name, route_path, service_path, default_service)
        self.service_name = service_name
        self.backend_servers = backend_servers


def _get_sorted_api_gateway_backends(api_gateway_backends):
    def sort_by_route_and_name(api_gateway_backend):
        backend_name, backend_service = api_gateway_backend
        route_path = backend_service.get_route_path()
        return [route_path, backend_name]

    return sorted(
        api_gateway_backends.items(),
        key=sort_by_route_and_name, reverse=True)


def update_api_gateway_configuration(
        api_gateway_backends, new_backends,
        bind_ip, bind_port, balance_method):
    if not bind_port:
        bind_port = HAPROXY_SERVICE_PORT_DEFAULT
    service_protocol = HAPROXY_SERVICE_PROTOCOL_HTTP
    conf_dir = os.path.join(_get_home_dir(), "conf")
    template_file = os.path.join(
        conf_dir, "haproxy-template.cfg")
    working_file = os.path.join(
        conf_dir, "haproxy-working.cfg")
    shutil.copyfile(template_file, working_file)

    # The backends should be reverse sorted by the route paths
    # reversed to make sure the shortest route with the same prefix showing after
    # this make route / to match any path showing at the last as the default backend.
    sorted_api_gateway_backends = _get_sorted_api_gateway_backends(
        api_gateway_backends)

    with open(working_file, "a") as f:
        f.write("frontend api_gateway\n")
        if bind_ip:
            f.write(f"    bind {bind_ip}:{bind_port}\n")
        else:
            f.write(f"    bind :{bind_port}\n")
        f.write(f"    mode {service_protocol}\n")
        f.write(f"    option {service_protocol}log\n")
        # IMPORTANT NOTE:
        # There may be as many "use_backend" rules as desired. All of these rules are
        # evaluated in their declaration order, and the first one which matches will
        # assign the backend.
        for backend_name, backend_service in sorted_api_gateway_backends:
            route_path = backend_service.get_route_path()
            if route_path.endswith('/'):
                # if the route path ends with /, we don't need two conditions
                # /abc/ will use path_beg /abc/ to match (it will not match /abc)
                f.write("    use_backend " + backend_name +
                        " if { path_beg " + route_path +
                        " }\n")
            else:
                # match two cases
                # path /abc match exactly the /abc
                # path_beg /abc/ match paths that prefixed by /abc/
                # route to a backend based on path's prefix
                f.write("    use_backend " + backend_name +
                        " if { path " + route_path +
                        " } || { path_beg " + route_path +
                        "/ }\n")

        f.write("\n")
        # write each backend
        for backend_name, backend_service in sorted_api_gateway_backends:
            backend_servers = backend_service.backend_servers
            route_path = backend_service.get_route_path()
            backend_server_block = _get_backend_server_block(
                backend_servers)
            f.write(f"backend {backend_name}\n")
            f.write(f"    mode {service_protocol}\n")
            if balance_method:
                f.write(f"    balance {balance_method}\n")

            # IMPORTANT NOTE:
            # strip the route path and replace this part with service path if there is one
            target_path = "/\\2"
            service_path = backend_service.get_service_path()
            if service_path:
                target_path = service_path + target_path
            f.write("    http-request replace-path " + route_path +
                    "(/)?(.*) " + target_path + "\n")
            f.write(backend_server_block)

    config_file = os.path.join(
        conf_dir, "haproxy.cfg")
    # move overwritten
    shutil.move(working_file, config_file)

    if new_backends:
        # Need reload haproxy if there is new backend added
        exec_with_call("sudo service haproxy reload")
