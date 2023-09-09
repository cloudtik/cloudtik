import os
from shlex import quote
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_CLUSTER
from cloudtik.core._private.core_utils import get_address_string, exec_with_output
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_KONG, BUILT_IN_RUNTIME_POSTGRES
from cloudtik.core._private.runtime_utils import get_runtime_bool, \
    get_runtime_value, get_runtime_config_from_node, get_runtime_node_ip
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_head_or_all, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_API_GATEWAY, SERVICE_DISCOVERY_PROTOCOL_HTTP, \
    exclude_runtime_of_cluster, serialize_service_selector
from cloudtik.core._private.util.database_utils import is_database_configured, export_database_environment_variables, \
    DATABASE_ENGINE_POSTGRES, get_database_engine, DATABASE_ENV_ENABLED, DATABASE_ENV_ENGINE
from cloudtik.core._private.utils import get_runtime_config, is_use_managed_cloud_database, PROVIDER_DATABASE_CONFIG_KEY
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    DATABASE_CONNECT_KEY, is_database_service_discovery, discover_database_on_head, \
    discover_database_from_workspace
from cloudtik.runtime.common.utils import stop_pull_server_by_identifier

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["/usr/local/kong", False, "KONG", "node"],
    ]

KONG_SERVICE_PORT_CONFIG_KEY = "port"
KONG_SERVICE_SSL_PORT_CONFIG_KEY = "ssl_port"
KONG_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

KONG_UPSTREAM_CONFIG_MODE_CONFIG_KEY = "config_mode"
KONG_UPSTREAM_SELECTOR_CONFIG_KEY = "upstream_selector"
# "consistent-hashing", "least-connections", "round-robin", "latency"
KONG_UPSTREAM_BALANCE_CONFIG_KEY = "balance"

KONG_CONFIG_MODE_DYNAMIC = "dynamic"

KONG_SERVICE_NAME = BUILT_IN_RUNTIME_KONG

KONG_SERVICE_PORT_DEFAULT = 8000
KONG_SERVICE_SSL_PORT_DEFAULT = 8443

KONG_ADMIN_PORT_DEFAULT = 8001
KONG_ADMIN_SSL_PORT_DEFAULT = 8444
KONG_ADMIN_UI_PORT_DEFAULT = 8002
KONG_ADMIN_UI_SSL_PORT_DEFAULT = 8445

KONG_DISCOVER_UPSTREAM_SERVERS_INTERVAL = 15


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_KONG, {})


def _get_database_config(metastore_config):
    return metastore_config.get(DATABASE_CONNECT_KEY, {})


def _get_service_port(kong_config: Dict[str, Any]):
    return kong_config.get(
        KONG_SERVICE_PORT_CONFIG_KEY, KONG_SERVICE_PORT_DEFAULT)


def _get_service_ssl_port(kong_config: Dict[str, Any]):
    return kong_config.get(
        KONG_SERVICE_SSL_PORT_CONFIG_KEY, KONG_SERVICE_SSL_PORT_DEFAULT)


def _is_high_availability(kong_config: Dict[str, Any]):
    return kong_config.get(
        KONG_HIGH_AVAILABILITY_CONFIG_KEY, True)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_KONG)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_KONG: logs_dir}


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_database_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_KONG,
        database_runtime_type=BUILT_IN_RUNTIME_POSTGRES,
        allow_local=False
    )
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_database_on_head(
        cluster_config, BUILT_IN_RUNTIME_KONG,
        database_runtime_type=BUILT_IN_RUNTIME_POSTGRES,
        allow_local=False)

    _validate_config(cluster_config, final=True)
    return cluster_config


def _is_valid_database_config(config: Dict[str, Any], final=False):
    # Check database configuration
    runtime_config = get_runtime_config(config)
    kong_config = _get_config(runtime_config)
    database_config = _get_database_config(kong_config)
    if is_database_configured(database_config):
        if get_database_engine(database_config) != DATABASE_ENGINE_POSTGRES:
            return False
        return True

    # check whether cloud database is available (must be postgres)
    provider_config = config["provider"]
    if (PROVIDER_DATABASE_CONFIG_KEY in provider_config or
            (not final and is_use_managed_cloud_database(config))):
        return True

    # if there is service discovery mechanism, assume we can get from service discovery
    if (not final and is_database_service_discovery(kong_config)
            and get_service_discovery_runtime(runtime_config)):
        return True

    return False


def _validate_config(config: Dict[str, Any], final=False):
    if not _is_valid_database_config(config, final):
        raise ValueError("Postgres must be configured for Kong.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    kong_config = _get_config(runtime_config)

    service_port = _get_service_port(kong_config)
    runtime_envs["KONG_SERVICE_PORT"] = service_port
    service_ssl_port = _get_service_ssl_port(kong_config)
    runtime_envs["KONG_SERVICE_SSL_PORT"] = service_ssl_port
    runtime_envs["KONG_ADMIN_PORT"] = KONG_ADMIN_PORT_DEFAULT
    runtime_envs["KONG_ADMIN_SSL_PORT"] = KONG_ADMIN_SSL_PORT_DEFAULT
    runtime_envs["KONG_ADMIN_UI_PORT"] = KONG_ADMIN_UI_PORT_DEFAULT
    runtime_envs["KONG_ADMIN_UI_SSL_PORT"] = KONG_ADMIN_UI_SSL_PORT_DEFAULT

    high_availability = _is_high_availability(kong_config)
    if high_availability:
        runtime_envs["KONG_HIGH_AVAILABILITY"] = high_availability

    config_mode = kong_config.get(
        KONG_UPSTREAM_CONFIG_MODE_CONFIG_KEY, KONG_CONFIG_MODE_DYNAMIC)
    runtime_envs["KONG_CONFIG_MODE"] = config_mode

    balance = kong_config.get(
        KONG_UPSTREAM_BALANCE_CONFIG_KEY)
    if balance:
        runtime_envs["KONG_UPSTREAM_BALANCE"] = balance

    return runtime_envs


def _export_database_configurations(runtime_config):
    kong_config = _get_config(runtime_config)
    database_config = _get_database_config(kong_config)
    if is_database_configured(database_config):
        # set the database environments from database config
        # This may override the environments from provider
        export_database_environment_variables(database_config)
    else:
        # check cloud database is configured
        database_enabled = get_runtime_bool(DATABASE_ENV_ENABLED)
        if not database_enabled:
            raise RuntimeError("No Postgres is configured for Kong.")
        database_engine = get_runtime_value(DATABASE_ENV_ENGINE)
        if database_engine != DATABASE_ENGINE_POSTGRES:
            raise RuntimeError("Postgres must be configured for Kong.")


def _configure(runtime_config, head: bool):
    _export_database_configurations(runtime_config)


def _services(runtime_config, head: bool):
    # We put the database schema init right before the start of metastore service
    _export_database_configurations(runtime_config)


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "kong": {
            "name": "KONG",
            "url": "http://{}".format(
                get_address_string(cluster_head_ip, service_port))
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "kong": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    kong_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(kong_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, KONG_SERVICE_NAME)
    service_port = _get_service_port(kong_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            service_discovery_config, service_port,
            _is_high_availability(kong_config),
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_API_GATEWAY]),
    }
    return services


###################################
# Calls from node at running time
###################################


def _get_pull_identifier():
    return "{}-discovery".format(KONG_SERVICE_NAME)


def _get_admin_api_endpoint(node_ip, admin_port):
    return "http://{}:{}".format(
        node_ip, admin_port)


def start_pull_server(head):
    runtime_config = get_runtime_config_from_node(head)
    kong_config = _get_config(runtime_config)
    node_ip = get_runtime_node_ip()
    admin_endpoint = _get_admin_api_endpoint(node_ip, KONG_ADMIN_PORT_DEFAULT)

    service_selector = kong_config.get(
            KONG_UPSTREAM_SELECTOR_CONFIG_KEY, {})
    cluster_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_CLUSTER)
    exclude_runtime_of_cluster(
        service_selector, BUILT_IN_RUNTIME_KONG, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)
    pull_identifier = _get_pull_identifier()

    cmd = ["cloudtik", "node", "pull", pull_identifier, "start"]
    cmd += ["--pull-class=cloudtik.runtime.kong.discovery.DiscoverUpstreamServers"]
    cmd += ["--interval={}".format(
        KONG_DISCOVER_UPSTREAM_SERVERS_INTERVAL)]
    # job parameters
    cmd += ["admin_endpoint={}".format(quote(admin_endpoint))]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]
    balance_method = get_runtime_value("KONG_UPSTREAM_BALANCE")
    if balance_method:
        cmd += ["balance_method={}".format(
            quote(balance_method))]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_pull_server():
    pull_identifier = _get_pull_identifier()
    stop_pull_server_by_identifier(pull_identifier)
