import os
from shlex import quote

from cloudtik.core._private.service_discovery.runtime_services import get_local_dns_server_port, \
    get_consul_local_client_port
from cloudtik.core._private.util.core_utils import get_config_for_update, get_list_for_update, \
    exec_with_output, http_address_string, address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_APISIX
from cloudtik.core._private.util.runtime_utils import \
    get_runtime_config_from_node, load_and_save_yaml, get_runtime_value, \
    get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import \
    exclude_runtime_of_cluster, serialize_service_selector, get_service_selector_copy
from cloudtik.core._private.utils import encrypt_string
from cloudtik.runtime.apisix.utils import _get_config, _get_admin_port, _get_admin_key, _get_backend_config, \
    _get_config_mode, APISIX_BACKEND_SELECTOR_CONFIG_KEY, APISIX_CONFIG_MODE_DNS, APISIX_CONFIG_MODE_CONSUL, \
    _get_home_dir, _get_logs_dir
from cloudtik.runtime.common.service_discovery.runtime_discovery import ETCD_URI_KEY
from cloudtik.runtime.common.service_discovery.utils import get_service_addresses_from_string
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier

APISIX_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def update_configurations(head):
    runtime_config = get_runtime_config_from_node(head)
    apisix_config = _get_config(runtime_config)

    def update_callback(config_object):
        etcd_uri = apisix_config.get(ETCD_URI_KEY)
        if etcd_uri:
            service_addresses = get_service_addresses_from_string(etcd_uri)
            deployment = get_config_for_update(config_object, "deployment")
            etcd = get_config_for_update(deployment, "etcd")
            hosts = get_list_for_update(etcd, "host")
            for service_address in service_addresses:
                hosts.append(
                    http_address_string(service_address[0], service_address[1]))

        # service discovery
        backend_config = _get_backend_config(apisix_config)
        config_mode = _get_config_mode(backend_config)
        if config_mode == APISIX_CONFIG_MODE_DNS:
            discovery = get_config_for_update(config_object, "discovery")
            dsn = get_config_for_update(discovery, "dns")
            servers = get_list_for_update(dsn, "servers")
            # get the address based on configuration
            dns_server_port = get_local_dns_server_port(runtime_config)
            servers.append(
                address_string("127.0.0.1", dns_server_port))
        elif config_mode == APISIX_CONFIG_MODE_CONSUL:
            discovery = get_config_for_update(config_object, "discovery")
            consul = get_config_for_update(discovery, "consul")
            servers = get_list_for_update(consul, "servers")
            # get the address based on configuration
            consul_client_port = get_consul_local_client_port(runtime_config)
            servers.append(
                http_address_string("127.0.0.1", consul_client_port))

    _update_configurations(update_callback)


def _update_configurations(update_callback):
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "conf", "config.yaml")
    load_and_save_yaml(config_file, update_callback)


#######################################
# Calls from node when running services
#######################################

def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_APISIX)


def _get_admin_api_endpoint(node_ip, admin_port):
    return http_address_string(
        node_ip, admin_port)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    apisix_config = _get_config(runtime_config)
    admin_endpoint = _get_admin_api_endpoint(
        "127.0.0.1", _get_admin_port(apisix_config))
    # encrypt the admin key
    admin_key = encrypt_string(
        _get_admin_key(apisix_config))

    backend_config = _get_backend_config(apisix_config)
    config_mode = _get_config_mode(backend_config)
    service_selector = get_service_selector_copy(
        backend_config, APISIX_BACKEND_SELECTOR_CONFIG_KEY)
    cluster_name = get_runtime_cluster_name()
    service_selector = exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_APISIX, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)
    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.apisix.discovery.DiscoverBackendService"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        APISIX_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    cmd += ["admin_endpoint={}".format(quote(admin_endpoint))]
    cmd += ["admin_key={}".format(admin_key)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]
    if config_mode:
        cmd += ["config_mode={}".format(config_mode)]
    balance_method = get_runtime_value("APISIX_BACKEND_BALANCE")
    if balance_method:
        cmd += ["balance_method={}".format(
            quote(balance_method))]
    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)
