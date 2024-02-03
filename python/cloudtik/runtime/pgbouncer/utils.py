import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PGBOUNCER, BUILT_IN_RUNTIME_POSTGRES
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, \
    define_runtime_service_on_head_or_all, get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_DATABASE
from cloudtik.core._private.util.core_utils import get_config_for_update, address_string
from cloudtik.core._private.util.database_utils import \
    DATABASE_PASSWORD_POSTGRES_DEFAULT, DATABASE_USERNAME_POSTGRES_DEFAULT, DATABASE_CONFIG_ENGINE, \
    DATABASE_ENGINE_POSTGRES
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, get_runtime_config, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    is_database_service_discovery, get_database_runtime_in_cluster

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["pgbouncer", True, "PgBouncer", "node"],
    ]

PGBOUNCER_SERVICE_PORT_CONFIG_KEY = "port"
PGBOUNCER_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

PGBOUNCER_BACKEND_CONFIG_KEY = "backend"
PGBOUNCER_BACKEND_CONFIG_MODE_CONFIG_KEY = "config_mode"
PGBOUNCER_BACKEND_DATABASES_CONFIG_KEY = "databases"
PGBOUNCER_BACKEND_DYNAMIC_CONFIG_KEY = "dynamic"

PGBOUNCER_DATABASE_CONNECT_CONFIG_KEY = "connect"
PGBOUNCER_DATABASE_BIND_USER_CONFIG_KEY = "bind_user"
PGBOUNCER_DATABASE_AUTH_USER_CONFIG_KEY = "auth_user"
PGBOUNCER_DATABASE_AUTH_PASSWORD_CONFIG_KEY = "auth_password"
PGBOUNCER_DATABASE_AUTH_QUERY_CONFIG_KEY = "auth_query"

PGBOUNCER_ADMIN_USER_CONFIG_KEY = "admin_user"
PGBOUNCER_ADMIN_PASSWORD_CONFIG_KEY = "admin_password"

PGBOUNCER_POOL_CONFIG_KEY = "pool"
PGBOUNCER_POOL_MODE_CONFIG_KEY = "pool_mode"
PGBOUNCER_POOL_SIZE_CONFIG_KEY = "pool_size"
PGBOUNCER_MIN_POOL_SIZE_CONFIG_KEY = "min_pool_size"
PGBOUNCER_RESERVE_POOL_SIZE_CONFIG_KEY = "reserve_pool_size"

PGBOUNCER_SERVICE_NAME = BUILT_IN_RUNTIME_PGBOUNCER
PGBOUNCER_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
PGBOUNCER_SERVICE_PORT_DEFAULT = 6432

PGBOUNCER_CONFIG_MODE_STATIC = "static"
PGBOUNCER_CONFIG_MODE_DYNAMIC = "dynamic"
PGBOUNCER_CONFIG_MODE_LOCAL = "local"

PGBOUNCER_ADMIN_USER_DEFAULT = "pgbouncer"
PGBOUNCER_ADMIN_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

PGBOUNCER_POSTGRES_USER_DEFAULT = DATABASE_USERNAME_POSTGRES_DEFAULT
PGBOUNCER_POSTGRES_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

PGBOUNCER_POOL_MODE_SESSION = "session"
PGBOUNCER_POOL_MODE_TRANSACTION = "transaction"
PGBOUNCER_POOL_MODE_STATEMENT = "statement"

PGBOUNCER_POOL_SIZE_DEFAULT = 20
PGBOUNCER_MIN_POOL_SIZE_DEFAULT = 0
PGBOUNCER_RESERVE_POOL_SIZE_DEFAULT = 0

# share values from Postgres
PGBOUNCER_POSTGRES_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
PGBOUNCER_POSTGRES_SECONDARY_SERVICE_TYPE = PGBOUNCER_POSTGRES_SERVICE_TYPE + "-secondary"
PGBOUNCER_POSTGRES_NODE_SERVICE_TYPE = PGBOUNCER_POSTGRES_SERVICE_TYPE + "-node"

PGBOUNCER_DISCOVER_POSTGRES_SERVICE_TYPES = [
    PGBOUNCER_POSTGRES_SERVICE_TYPE,
    PGBOUNCER_POSTGRES_SECONDARY_SERVICE_TYPE,
    PGBOUNCER_POSTGRES_NODE_SERVICE_TYPE]


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_PGBOUNCER, {})


def _get_service_port(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_SERVICE_PORT_CONFIG_KEY, PGBOUNCER_SERVICE_PORT_DEFAULT)


def _is_high_availability(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_HIGH_AVAILABILITY_CONFIG_KEY, True)


def _get_admin_user(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_ADMIN_USER_CONFIG_KEY, PGBOUNCER_ADMIN_USER_DEFAULT)


def _get_admin_password(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_ADMIN_PASSWORD_CONFIG_KEY, PGBOUNCER_ADMIN_PASSWORD_DEFAULT)


def _get_pool_config(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_POOL_CONFIG_KEY, {})


def _get_backend_config(pgbouncer_config: Dict[str, Any]):
    return pgbouncer_config.get(
        PGBOUNCER_BACKEND_CONFIG_KEY, {})


def _get_backend_databases(backend_config):
    return backend_config.get(PGBOUNCER_BACKEND_DATABASES_CONFIG_KEY)


def _get_database_connect(database_config):
    return database_config.get(PGBOUNCER_DATABASE_CONNECT_CONFIG_KEY, {})


def _is_database_bind_user(database_config):
    return database_config.get(PGBOUNCER_DATABASE_BIND_USER_CONFIG_KEY, False)


def _get_database_auth_user(database_config):
    return database_config.get(PGBOUNCER_DATABASE_AUTH_USER_CONFIG_KEY)


def _get_database_auth_password(database_config):
    return database_config.get(PGBOUNCER_DATABASE_AUTH_PASSWORD_CONFIG_KEY)


def _get_database_auth_query(database_config):
    return database_config.get(PGBOUNCER_DATABASE_AUTH_QUERY_CONFIG_KEY)


def _get_dynamic_config(backend_config):
    return backend_config.get(PGBOUNCER_BACKEND_DYNAMIC_CONFIG_KEY)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_PGBOUNCER)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_logs_dir():
    home_dir = _get_home_dir()
    return os.path.join(home_dir, "logs")


def _get_runtime_logs():
    logs_dir = _get_logs_dir()
    return {BUILT_IN_RUNTIME_PGBOUNCER: logs_dir}


def _get_config_for_update(cluster_config):
    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_PGBOUNCER)


def _get_default_config_mode(config, backend_config):
    cluster_runtime_config = get_runtime_config(config)
    if _get_backend_databases(backend_config):
        # if there are static servers configured
        config_mode = PGBOUNCER_CONFIG_MODE_STATIC
    elif get_service_discovery_runtime(cluster_runtime_config):
        config_mode = PGBOUNCER_CONFIG_MODE_DYNAMIC
    else:
        raise ValueError(
            "No valid configuration mode could be identified.")
    return config_mode


def _get_checked_config_mode(
        pgbouncer_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    backend_config = _get_backend_config(pgbouncer_config)
    config_mode = backend_config.get(
        PGBOUNCER_BACKEND_CONFIG_MODE_CONFIG_KEY)
    if not config_mode:
        config_mode = _get_default_config_mode(
            cluster_config, backend_config)
    return config_mode


def _set_backend_databases_engine_type(
        cluster_config: Dict[str, Any]):
    runtime_config = get_runtime_config(cluster_config)
    pgbouncer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(pgbouncer_config)
    backend_databases = _get_backend_databases(backend_config)
    if not backend_databases:
        return cluster_config
    for _, database_config in backend_databases.items():
        database_connect = _get_database_connect(database_config)
        database_connect[DATABASE_CONFIG_ENGINE] = DATABASE_ENGINE_POSTGRES
    return cluster_config


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    pgbouncer_config = _get_config(runtime_config)
    config_mode = _get_checked_config_mode(pgbouncer_config, cluster_config)
    if config_mode == PGBOUNCER_CONFIG_MODE_STATIC:
        # for static, add the postgres engine type
        cluster_config = _set_backend_databases_engine_type(
            cluster_config)

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
    pgbouncer_config = _get_config(runtime_config)
    backend_config = _get_backend_config(pgbouncer_config)

    config_mode = _get_checked_config_mode(pgbouncer_config, config)
    if config_mode == PGBOUNCER_CONFIG_MODE_STATIC:
        backend_databases = _get_backend_databases(backend_config)
        if backend_databases:
            return True
        raise ValueError(
            "Missing backend servers for static configuration.")
    elif config_mode == PGBOUNCER_CONFIG_MODE_LOCAL:
        # TODO: check in cluster postgres database
        database_runtime = _get_database_runtime_in_cluster(
            runtime_config)
        if database_runtime:
            return True
    else:
        # if there is service discovery mechanism, assume we can get from service discovery
        if (is_database_service_discovery(pgbouncer_config)
                and get_service_discovery_runtime(runtime_config)):
            return True

    return False


def _validate_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    pgbouncer_config = _get_config(runtime_config)

    # Check admin password
    admin_user = _get_admin_user(pgbouncer_config)
    admin_password = _get_admin_password(pgbouncer_config)
    if not admin_user or not admin_password:
        raise ValueError(
            "Admin user and password must be both specified.")

    if not _is_valid_postgres_config(config, final):
        raise ValueError(
            "Postgres must be configured for PgBouncer.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    pgbouncer_config = _get_config(runtime_config)

    service_port = _get_service_port(pgbouncer_config)
    runtime_envs["PGBOUNCER_SERVICE_PORT"] = service_port

    admin_user = pgbouncer_config.get(
        PGBOUNCER_ADMIN_USER_CONFIG_KEY, PGBOUNCER_ADMIN_USER_DEFAULT)
    runtime_envs["PGBOUNCER_ADMIN_USER"] = admin_user

    pool_config = _get_pool_config(pgbouncer_config)
    runtime_envs["PGBOUNCER_POOL_MODE"] = pool_config.get(
        PGBOUNCER_POOL_MODE_CONFIG_KEY, PGBOUNCER_POOL_MODE_SESSION)
    runtime_envs["PGBOUNCER_POOL_SIZE"] = pool_config.get(
        PGBOUNCER_POOL_SIZE_CONFIG_KEY, PGBOUNCER_POOL_SIZE_DEFAULT)
    runtime_envs["PGBOUNCER_MIN_POOL_SIZE"] = pool_config.get(
        PGBOUNCER_MIN_POOL_SIZE_CONFIG_KEY, PGBOUNCER_MIN_POOL_SIZE_DEFAULT)
    runtime_envs["PGBOUNCER_RESERVE_POOL_SIZE"] = pool_config.get(
        PGBOUNCER_RESERVE_POOL_SIZE_CONFIG_KEY, PGBOUNCER_RESERVE_POOL_SIZE_DEFAULT)

    high_availability = _is_high_availability(pgbouncer_config)
    if high_availability:
        runtime_envs["PGBOUNCER_HIGH_AVAILABILITY"] = high_availability

    backend_config = _get_backend_config(pgbouncer_config)
    config_mode = _get_checked_config_mode(pgbouncer_config, config)
    if config_mode == PGBOUNCER_CONFIG_MODE_STATIC:
        _with_runtime_envs_for_static(backend_config, runtime_envs)
    else:
        _with_runtime_envs_for_dynamic(backend_config, runtime_envs)
    runtime_envs["PGBOUNCER_CONFIG_MODE"] = config_mode

    return runtime_envs


def _with_runtime_envs_for_static(backend_config, runtime_envs):
    pass


def _with_runtime_envs_for_dynamic(backend_config, runtime_envs):
    pass


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    pgbouncer_config = _get_config(runtime_config)
    service_port = _get_service_port(pgbouncer_config)
    endpoints = {
        "pgbouncer": {
            "name": "PgBouncer",
            "url": address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    pgbouncer_config = _get_config(runtime_config)
    service_port = _get_service_port(pgbouncer_config)
    service_ports = {
        "pgbouncer": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    pgbouncer_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(pgbouncer_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, PGBOUNCER_SERVICE_NAME)
    service_port = _get_service_port(pgbouncer_config)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            PGBOUNCER_SERVICE_TYPE,
            service_discovery_config, service_port,
            _is_high_availability(pgbouncer_config),
            features=[SERVICE_DISCOVERY_FEATURE_DATABASE])
    }
    return services