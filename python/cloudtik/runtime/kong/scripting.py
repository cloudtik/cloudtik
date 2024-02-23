from shlex import quote

from cloudtik.core._private.util.core_utils import exec_with_output, http_address_string
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_KONG
from cloudtik.core._private.util.runtime_utils import get_runtime_value, get_runtime_config_from_node, \
    get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import \
    exclude_runtime_of_cluster, serialize_service_selector, get_service_selector_copy
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.kong.utils import _get_config, KONG_ADMIN_PORT_DEFAULT, _get_backend_config, _get_config_mode, \
    KONG_BACKEND_SELECTOR_CONFIG_KEY, _get_logs_dir

KONG_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


#######################################
# Calls from node when running services
#######################################


def _get_service_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_KONG)


def _get_admin_api_endpoint(node_ip, admin_port):
    return http_address_string(
        node_ip, admin_port)


def start_pull_service(head):
    runtime_config = get_runtime_config_from_node(head)
    kong_config = _get_config(runtime_config)
    admin_endpoint = _get_admin_api_endpoint(
        "127.0.0.1", KONG_ADMIN_PORT_DEFAULT)

    backend_config = _get_backend_config(kong_config)
    config_mode = _get_config_mode(backend_config)
    service_selector = get_service_selector_copy(
        backend_config, KONG_BACKEND_SELECTOR_CONFIG_KEY)
    cluster_name = get_runtime_cluster_name()
    service_selector = exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_KONG, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)
    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.kong.discovery.DiscoverBackendService"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        KONG_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    cmd += ["admin_endpoint={}".format(quote(admin_endpoint))]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]
    if config_mode:
        cmd += ["config_mode={}".format(config_mode)]
    balance_method = get_runtime_value("KONG_BACKEND_BALANCE")
    if balance_method:
        cmd += ["balance_method={}".format(
            quote(balance_method))]
    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_service():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)
