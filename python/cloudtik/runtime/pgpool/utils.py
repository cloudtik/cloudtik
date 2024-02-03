import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PGPOOL, BUILT_IN_RUNTIME_POSTGRES
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, \
    define_runtime_service_on_head_or_all, get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_DATABASE
from cloudtik.core._private.util.core_utils import get_config_for_update, address_string, get_address_string
from cloudtik.core._private.util.database_utils import \
    DATABASE_PASSWORD_POSTGRES_DEFAULT, DATABASE_USERNAME_POSTGRES_DEFAULT, DATABASE_PORT_POSTGRES_DEFAULT
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, get_runtime_config, get_cluster_name
from cloudtik.runtime.common.service_discovery.discovery import DiscoveryType
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    is_database_service_discovery, get_database_runtime_in_cluster, DATABASE_SERVICE_SELECTOR_KEY, discover_database

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["pgpool", True, "Pgpool", "node"],
    ]

PGPOOL_SERVICE_PORT_CONFIG_KEY = "port"
PGPOOL_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

PGPOOL_BACKEND_CONFIG_KEY = "backend"
PGPOOL_BACKEND_CONFIG_MODE_CONFIG_KEY = "config_mode"
PGPOOL_BACKEND_SERVERS_CONFIG_KEY = "servers"

PGPOOL_ADMIN_USER_CONFIG_KEY = "admin_user"
PGPOOL_ADMIN_PASSWORD_CONFIG_KEY = "admin_password"

PGPOOL_POSTGRES_ADMIN_USER_CONFIG_KEY = "postgres_admin_user"
PGPOOL_POSTGRES_ADMIN_PASSWORD_CONFIG_KEY = "postgres_admin_password"

PGPOOL_POSTGRES_REPLICATION_USER_CONFIG_KEY = "replication_user"
PGPOOL_POSTGRES_REPLICATION_PASSWORD_CONFIG_KEY = "replication_password"

PGPOOL_MAX_POOL_CONFIG_KEY = "max_pool"
PGPOOL_PCP_PORT_CONFIG_KEY = "pcp_port"

PGPOOL_SERVICE_NAME = BUILT_IN_RUNTIME_PGPOOL
PGPOOL_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
PGPOOL_SERVICE_PORT_DEFAULT = DATABASE_PORT_POSTGRES_DEFAULT
PGPOOL_PCP_PORT_DEFAULT = 9898

PGPOOL_CONFIG_MODE_STATIC = "static"
PGPOOL_CONFIG_MODE_DYNAMIC = "dynamic"

PGPOOL_ADMIN_USER_DEFAULT = DATABASE_USERNAME_POSTGRES_DEFAULT
PGPOOL_ADMIN_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

PGPOOL_POSTGRES_ADMIN_USER_DEFAULT = DATABASE_USERNAME_POSTGRES_DEFAULT
PGPOOL_POSTGRES_ADMIN_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

PGPOOL_POSTGRES_REPLICATION_USER_DEFAULT = "repl_user"
PGPOOL_POSTGRES_REPLICATION_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

PGPOOL_MAX_POOL_DEFAULT = 15

# share values from Postgres
PGPOOL_POSTGRES_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
PGPOOL_POSTGRES_SECONDARY_SERVICE_TYPE = PGPOOL_POSTGRES_SERVICE_TYPE + "-secondary"
PGPOOL_POSTGRES_NODE_SERVICE_TYPE = PGPOOL_POSTGRES_SERVICE_TYPE + "-node"

PGPOOL_DISCOVER_POSTGRES_SERVICE_TYPES = [
    PGPOOL_POSTGRES_SERVICE_TYPE,
    PGPOOL_POSTGRES_SECONDARY_SERVICE_TYPE,
    PGPOOL_POSTGRES_NODE_SERVICE_TYPE]


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_PGPOOL, {})


def _get_service_port(pgpool_config: Dict[str, Any]):
    return pgpool_config.get(
        PGPOOL_SERVICE_PORT_CONFIG_KEY, PGPOOL_SERVICE_PORT_DEFAULT)


def _is_high_availability(pgpool_config: Dict[str, Any]):
    return pgpool_config.get(
        PGPOOL_HIGH_AVAILABILITY_CONFIG_KEY, True)


def _get_backend_config(pgpool_config: Dict[str, Any]):
    return pgpool_config.get(
        PGPOOL_BACKEND_CONFIG_KEY, {})


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_PGPOOL)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_PGPOOL: logs_dir}


def _get_config_for_update(cluster_config):
    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_PGPOOL)


def _get_default_config_mode(config, backend_config):
    cluster_runtime_config = get_runtime_config(config)
    if backend_config.get(PGPOOL_BACKEND_SERVERS_CONFIG_KEY):
        # if there are static servers configured
        config_mode = PGPOOL_CONFIG_MODE_STATIC
    elif get_service_discovery_runtime(cluster_runtime_config):
        config_mode = PGPOOL_CONFIG_MODE_DYNAMIC
    else:
        raise ValueError(
            "No valid configuration mode could be identified.")
    return config_mode


def _get_config_mode(
        pgpool_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    backend_config = _get_backend_config(pgpool_config)
    config_mode = backend_config.get(
        PGPOOL_BACKEND_CONFIG_MODE_CONFIG_KEY)
    if not config_mode:
        config_mode = _get_default_config_mode(
            cluster_config, backend_config)
    return config_mode


def _set_backend_servers_config(
        pgpool_config: Dict[str, Any], server_addresses):
    backend_config = get_config_for_update(
            pgpool_config, PGPOOL_BACKEND_CONFIG_KEY)
    backend_servers = [
        get_address_string(
            server_address[0], server_address[1]) for server_address in server_addresses]
    backend_config[PGPOOL_BACKEND_SERVERS_CONFIG_KEY] = backend_servers


def discover_postgres_on_head(
        cluster_config: Dict[str, Any], runtime_type):
    runtime_config = get_runtime_config(cluster_config)
    runtime_type_config = runtime_config.get(runtime_type, {})
    if not is_database_service_discovery(runtime_type_config):
        return cluster_config

    # There is service discovery to come here
    database_service = discover_database(
        runtime_type_config, DATABASE_SERVICE_SELECTOR_KEY,
        cluster_config=cluster_config,
        discovery_type=DiscoveryType.CLUSTER,
        database_runtime_type=BUILT_IN_RUNTIME_POSTGRES,
        database_service_type=PGPOOL_DISCOVER_POSTGRES_SERVICE_TYPES)
    if database_service:
        runtime_type_config = get_config_for_update(
            runtime_config, runtime_type)
        _, service_addresses = database_service
        # set the backend servers
        _set_backend_servers_config(
            runtime_type_config, service_addresses)

    return cluster_config


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    pgpool_config = _get_config(runtime_config)
    config_mode = _get_config_mode(pgpool_config, cluster_config)
    if config_mode == PGPOOL_CONFIG_MODE_DYNAMIC:
        cluster_config = discover_postgres_on_head(
            cluster_config, BUILT_IN_RUNTIME_PGPOOL)

    _validate_config(cluster_config, final=True)
    return cluster_config


def _get_database_runtime_in_cluster(runtime_config):
    database_runtime = get_database_runtime_in_cluster(
        runtime_config)
    if (database_runtime
            and database_runtime == BUILT_IN_RUNTIME_POSTGRES):
        return database_runtime
    return None


def _is_valid_postgres_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    pgpool_config = _get_config(runtime_config)
    backend_config = _get_backend_config(pgpool_config)
    backend_servers = backend_config.get(PGPOOL_BACKEND_SERVERS_CONFIG_KEY)
    # check backend servers either static or discovered
    if backend_servers:
        return True

    config_mode = _get_config_mode(pgpool_config, config)
    if config_mode == PGPOOL_CONFIG_MODE_STATIC:
        raise ValueError(
            "Missing backend servers for static configuration.")

    # TODO: check in cluster postgres database
    database_runtime = _get_database_runtime_in_cluster(
        runtime_config)
    if database_runtime:
        return True

    # if there is service discovery mechanism, assume we can get from service discovery
    if (not final and is_database_service_discovery(pgpool_config)
            and get_service_discovery_runtime(runtime_config)):
        return True

    return False


def _validate_config(config: Dict[str, Any], final=False):
    if not _is_valid_postgres_config(config, final):
        raise ValueError(
            "Postgres must be configured for Pgpool.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    pgpool_config = _get_config(runtime_config)

    service_port = _get_service_port(pgpool_config)
    runtime_envs["PGPOOL_SERVICE_PORT"] = service_port

    admin_user = pgpool_config.get(
        PGPOOL_ADMIN_USER_CONFIG_KEY, PGPOOL_ADMIN_USER_DEFAULT)
    runtime_envs["PGPOOL_ADMIN_USER"] = admin_user

    admin_password = pgpool_config.get(
        PGPOOL_ADMIN_PASSWORD_CONFIG_KEY, PGPOOL_ADMIN_PASSWORD_DEFAULT)
    runtime_envs["PGPOOL_ADMIN_PASSWORD"] = admin_password

    postgres_user = pgpool_config.get(
        PGPOOL_POSTGRES_ADMIN_USER_CONFIG_KEY, PGPOOL_POSTGRES_ADMIN_USER_DEFAULT)
    runtime_envs["PGPOOL_POSTGRES_USER"] = postgres_user

    postgres_password = pgpool_config.get(
        PGPOOL_POSTGRES_ADMIN_PASSWORD_CONFIG_KEY, PGPOOL_POSTGRES_ADMIN_PASSWORD_DEFAULT)
    runtime_envs["PGPOOL_POSTGRES_PASSWORD"] = postgres_password

    replication_user = pgpool_config.get(
        PGPOOL_POSTGRES_REPLICATION_USER_CONFIG_KEY, PGPOOL_POSTGRES_REPLICATION_USER_DEFAULT)
    runtime_envs["PGPOOL_REPLICATION_USER"] = replication_user

    replication_password = pgpool_config.get(
        PGPOOL_POSTGRES_REPLICATION_PASSWORD_CONFIG_KEY, PGPOOL_POSTGRES_REPLICATION_PASSWORD_DEFAULT)
    runtime_envs["PGPOOL_REPLICATION_PASSWORD"] = replication_password

    runtime_envs["PGPOOL_MAX_POOL"] = pgpool_config.get(
        PGPOOL_MAX_POOL_CONFIG_KEY, PGPOOL_MAX_POOL_DEFAULT)
    runtime_envs["PGPOOL_PCP_PORT"] = pgpool_config.get(
        PGPOOL_PCP_PORT_CONFIG_KEY, PGPOOL_PCP_PORT_DEFAULT)

    high_availability = _is_high_availability(pgpool_config)
    if high_availability:
        runtime_envs["PGPOOL_HIGH_AVAILABILITY"] = high_availability

    backend_config = _get_backend_config(pgpool_config)
    config_mode = _get_config_mode(pgpool_config, config)
    if config_mode == PGPOOL_CONFIG_MODE_STATIC:
        _with_runtime_envs_for_static(backend_config, runtime_envs)
    else:
        _with_runtime_envs_for_dynamic(backend_config, runtime_envs)
    runtime_envs["PGPOOL_CONFIG_MODE"] = config_mode

    return runtime_envs


def _with_runtime_envs_for_static(backend_config, runtime_envs):
    pass


def _with_runtime_envs_for_dynamic(backend_config, runtime_envs):
    pass


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    pgpool_config = _get_config(runtime_config)
    service_port = _get_service_port(pgpool_config)
    endpoints = {
        "pgpool": {
            "name": "Pgpool",
            "url": address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    pgpool_config = _get_config(runtime_config)
    service_port = _get_service_port(pgpool_config)
    service_ports = {
        "pgpool": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    pgpool_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(pgpool_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, PGPOOL_SERVICE_NAME)
    service_port = _get_service_port(pgpool_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            PGPOOL_SERVICE_TYPE,
            service_discovery_config, service_port,
            _is_high_availability(pgpool_config),
            features=[SERVICE_DISCOVERY_FEATURE_DATABASE])
    }
    return services
