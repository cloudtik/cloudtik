import os
from shlex import quote

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PGPOOL, BUILT_IN_RUNTIME_POSTGRES
from cloudtik.core._private.service_discovery.utils import serialize_service_selector, \
    include_runtime_service_for_selector, get_service_selector_copy
from cloudtik.core._private.util.core_utils import exec_with_output, get_address_string, exec_with_call, \
    address_from_string
from cloudtik.core._private.util.database_utils import DATABASE_PORT_POSTGRES_DEFAULT
from cloudtik.core._private.util.runtime_utils import get_runtime_config_from_node, get_runtime_node_address_type
from cloudtik.core._private.utils import load_properties_file, save_properties_file, run_system_command
from cloudtik.runtime.common.service_discovery.runtime_discovery import DATABASE_SERVICE_SELECTOR_KEY
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.pgpool.utils import _get_config, _get_home_dir, _get_backend_config, \
    PGPOOL_BACKEND_SERVERS_CONFIG_KEY, PGPOOL_DISCOVER_POSTGRES_SERVICE_TYPES, _get_logs_dir

PGPOOL_PULL_BACKENDS_INTERVAL = 15
PGPOOL_MAX_SERVERS = 1024

###################################
# Calls from node when configuring
###################################


def _get_config_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "conf")


def _get_config_file():
    return os.path.join(_get_config_dir(), "pgpool.conf")


def _get_pcp_file():
    return os.path.join(_get_config_dir(), "pcp.conf")


def _get_hba_file():
    return os.path.join(_get_config_dir(), "pool_hba.conf")


def _get_initial_backend_servers(pgpool_config):
    backend_config = _get_backend_config(pgpool_config)
    servers = backend_config.get(PGPOOL_BACKEND_SERVERS_CONFIG_KEY, [])
    #  a list of servers with host:port
    backend_servers = {}
    for server in servers:
        host_port = address_from_string(server)
        server_host = host_port[0]
        if len(host_port) > 1:
            server_port = host_port[1]
        else:
            server_port = DATABASE_PORT_POSTGRES_DEFAULT
        backend_servers[server] = (server_host, server_port)
    return backend_servers


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    pgpool_config = _get_config(runtime_config)
    # no matter static or dynamic, we need to the initial backend servers
    backend_servers = _get_initial_backend_servers(pgpool_config)
    _update_backends(backend_servers)


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_PGPOOL)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    pgpool_config = _get_config(runtime_config)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    service_selector = get_service_selector_copy(
        pgpool_config, DATABASE_SERVICE_SELECTOR_KEY)

    service_selector = include_runtime_service_for_selector(
        service_selector,
        runtime_type=BUILT_IN_RUNTIME_POSTGRES,
        service_type=PGPOOL_DISCOVER_POSTGRES_SERVICE_TYPES)

    service_selector_str = serialize_service_selector(service_selector)
    address_type = get_runtime_node_address_type()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.pgpool.discovery.DiscoverBackendService"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        PGPOOL_PULL_BACKENDS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]
    cmd += ["address_type={}".format(str(address_type))]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)


def update_configuration(backend_servers):
    if _update_backends(backend_servers):
        # the conf is changed, reload the service
        config_file = _get_config_file()
        pcp_file = _get_pcp_file()
        hba_file = _get_hba_file()
        cmd = [
            "pgpool",
            "-f", config_file,
            "-F", pcp_file,
            "-a", hba_file,
            "reload"]
        cmd_str = " ".join(cmd)
        exec_with_call(cmd_str)


def _update_backends(backend_servers):
    # load the property file
    config_file = _get_config_file()
    config_object, comments = load_properties_file(config_file)

    existing_backend_servers = _get_backend_servers_from_config(
        config_object)
    new_backend_servers = _get_new_backend_servers(
        existing_backend_servers, backend_servers)
    if not new_backend_servers:
        return False

    # update the conf file with additional servers
    new_server_addresses = [
        server_address for _, server_address in new_backend_servers.items()]
    new_server_addresses.sort()
    _add_backend_servers_to_config(
        config_object, existing_backend_servers, new_server_addresses)

    # Write back the configuration file if updated
    save_properties_file(
        config_file, config_object, comments=comments)
    return True


def _get_backend_servers_from_config(config_object):
    backend_servers = []
    for i in range(0, PGPOOL_MAX_SERVERS):
        backend_hostname_key = f"backend_hostname{i}"
        if backend_hostname_key not in config_object:
            break
        backend_hostname = config_object[backend_hostname_key]
        backend_port_key = f"backend_port{i}"
        backend_port = config_object.get(
            backend_port_key, DATABASE_PORT_POSTGRES_DEFAULT)
        backend_servers.append((backend_hostname, backend_port))
    return backend_servers


def _get_new_backend_servers(
        existing_backend_servers, backend_servers):
    new_backend_servers = {}
    existing_keys = {
        get_address_string(
            backend_server[0], backend_server[1]): backend_server
        for backend_server in existing_backend_servers
    }
    for server_key, server_address in backend_servers.items():
        if server_key not in existing_keys:
            new_backend_servers[server_key] = server_address
    return new_backend_servers


def _add_backend_servers_to_config(
        config_object, existing_backend_servers, new_server_addresses):
    start_index = len(existing_backend_servers)
    for server_address in new_server_addresses:
        _add_backend_server_to_config(
            config_object, start_index, server_address)
        start_index += 1


def _add_backend_server_to_config(
        config_object, index, server_address):
    backend_hostname_key = f"backend_hostname{index}"
    backend_port_key = f"backend_port{index}"
    backend_weight_key = f"backend_weight{index}"
    backend_data_directory_key = f"backend_data_directory{index}"
    backend_flag_key = f"backend_flag{index}"

    data_dir = os.path.join(_get_home_dir(), "data")
    config_object[backend_hostname_key] = server_address[0]
    config_object[backend_port_key] = server_address[1]
    config_object[backend_weight_key] = str(1)
    # manually quote for path
    config_object[backend_data_directory_key] = "'{}'".format(data_dir)
    config_object[backend_flag_key] = "ALLOW_TO_FAILOVER"


def do_node_check():
    this_dir = os.path.dirname(__file__)
    shell_path = os.path.join(
        this_dir, "scripts", "pgpool-node-check.sh")
    cmds = [
        "bash",
        quote(shell_path),
    ]

    final_cmd = " ".join(cmds)
    run_system_command(final_cmd)
