import os
from shlex import quote
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_CLUSTER
from cloudtik.core._private.core_utils import get_config_for_update, get_list_for_update, get_address_string, \
    exec_with_output
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_APISIX
from cloudtik.core._private.runtime_utils import get_runtime_config_from_node, load_and_save_yaml, get_runtime_value
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_head_or_all, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_API_GATEWAY, SERVICE_DISCOVERY_PROTOCOL_HTTP, \
    exclude_runtime_of_cluster, serialize_service_selector
from cloudtik.core._private.utils import get_runtime_config, RUNTIME_CONFIG_KEY, encrypt_string, string_to_hex_string
from cloudtik.runtime.common.service_discovery.runtime_discovery import discover_etcd_from_workspace, \
    discover_etcd_on_head, ETCD_URI_KEY, is_etcd_service_discovery
from cloudtik.runtime.common.service_discovery.utils import get_service_addresses_from_string
from cloudtik.runtime.common.utils import stop_pull_server_by_identifier

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

APISIX_DISCOVER_BACKEND_SERVERS_INTERVAL = 15

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


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
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
            raise ValueError("ETCD must be configured for APISIX.")

    cluster_runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if not get_service_discovery_runtime(cluster_runtime_config):
        raise ValueError("Service discovery runtime is needed for APISIX.")


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


def _configure(runtime_config, head: bool):
    apisix_config = _get_config(runtime_config)
    admin_key = _get_admin_key(apisix_config)
    os.environ["APISIX_ADMIN_KEY"] = admin_key


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "apisix": {
            "name": "APISIX",
            "url": "http://{}".format(
                get_address_string(cluster_head_ip, service_port))
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
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
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
                hosts.append("http://{}".format(
                    get_address_string(service_address[0], service_address[1])))
            cluster_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_CLUSTER)
            if cluster_name:
                prefix = "apisix" + string_to_hex_string(cluster_name)
                etcd["prefix"] = prefix

        # service discovery
        backend_config = _get_backend_config(apisix_config)
        config_mode = _get_config_mode(backend_config)
        if config_mode == APISIX_CONFIG_MODE_DNS:
            discovery = get_config_for_update(config_object, "discovery")
            dsn = get_config_for_update(discovery, "dns")
            servers = get_list_for_update(dsn, "servers")
            # TODO: get the address based on configuration
            servers.append("127.0.0.1:8600")
        elif config_mode == APISIX_CONFIG_MODE_CONSUL:
            discovery = get_config_for_update(config_object, "discovery")
            consul = get_config_for_update(discovery, "consul")
            servers = get_list_for_update(consul, "servers")
            # TODO: get the address based on configuration
            servers.append("http://127.0.0.1:8500")

    _update_configurations(update_callback)


def _update_configurations(update_callback):
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "conf", "config.yaml")
    load_and_save_yaml(config_file, update_callback)


#######################################
# Calls from node when running services
#######################################

def _get_pull_identifier():
    return "{}-discovery".format(BUILT_IN_RUNTIME_APISIX)


def _get_admin_api_endpoint(node_ip, admin_port):
    return "http://{}:{}".format(
        node_ip, admin_port)


def start_pull_server(head):
    runtime_config = get_runtime_config_from_node(head)
    apisix_config = _get_config(runtime_config)
    admin_endpoint = _get_admin_api_endpoint(
        "127.0.0.1", _get_admin_port(apisix_config))
    # encrypt the admin key
    admin_key = encrypt_string(
        _get_admin_key(apisix_config))

    backend_config = _get_backend_config(apisix_config)
    config_mode = _get_config_mode(backend_config)
    service_selector = backend_config.get(
            APISIX_BACKEND_SELECTOR_CONFIG_KEY, {})
    cluster_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_CLUSTER)
    exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_APISIX, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)
    pull_identifier = _get_pull_identifier()

    cmd = ["cloudtik", "node", "pull", pull_identifier, "start"]
    cmd += ["--pull-class=cloudtik.runtime.apisix.discovery.DiscoverBackendServers"]
    cmd += ["--interval={}".format(
        APISIX_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    # job parameters
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


def stop_pull_server():
    pull_identifier = _get_pull_identifier()
    stop_pull_server_by_identifier(pull_identifier)
