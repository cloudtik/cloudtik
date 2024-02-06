import os
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_POSTGRES, BUILT_IN_RUNTIME_MOUNT
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, get_service_discovery_config, \
    SERVICE_DISCOVERY_FEATURE_DATABASE, define_runtime_service_on_head, \
    define_runtime_service_on_worker, define_runtime_service
from cloudtik.core._private.util.core_utils import get_config_for_update, address_string
from cloudtik.core._private.util.database_utils import DATABASE_PORT_POSTGRES_DEFAULT, \
    DATABASE_USERNAME_POSTGRES_DEFAULT, DATABASE_PASSWORD_POSTGRES_DEFAULT
from cloudtik.core._private.utils import _sum_min_workers, get_runtime_config_for_update, get_runtime_config, \
    get_cluster_name
from cloudtik.runtime.common.health_check import HEALTH_CHECK_PORT, HEALTH_CHECK_NODE_KIND, \
    HEALTH_CHECK_NODE_KIND_HEAD, HEALTH_CHECK_NODE_KIND_NODE
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["postgres", True, "Postgres", "node"],
        ["repmgrd", True, "Repmgr", "node"],
    ]

POSTGRES_SERVICE_PORT_CONFIG_KEY = "port"

POSTGRES_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
POSTGRES_ADMIN_USER_CONFIG_KEY = "admin_user"
POSTGRES_ADMIN_PASSWORD_CONFIG_KEY = "admin_password"

POSTGRES_REPLICATION_USER_CONFIG_KEY = "replication_user"
POSTGRES_REPLICATION_PASSWORD_CONFIG_KEY = "replication_password"

POSTGRES_ARCHIVE_MODE_CONFIG_KEY = "archive_mode"
POSTGRES_REPLICATION_SLOT_CONFIG_KEY = "replication_slot"
POSTGRES_REPLICATION_SYNCHRONOUS_CONFIG_KEY = "replication_synchronous"

POSTGRES_REPLICATION_SYNCHRONOUS_MODE_CONFIG_KEY = "mode"
POSTGRES_REPLICATION_SYNCHRONOUS_NUM_CONFIG_KEY = "num"
POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_MODE_CONFIG_KEY = "commit_mode"
POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY = "synchronous_size"

POSTGRES_DATABASE_CONFIG_KEY = "database"
POSTGRES_DATABASE_NAME_CONFIG_KEY = "name"
POSTGRES_DATABASE_USER_CONFIG_KEY = "user"
POSTGRES_DATABASE_PASSWORD_CONFIG_KEY = "password"

POSTGRES_HEALTH_CHECK_PORT_CONFIG_KEY = "health_check_port"

POSTGRES_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
POSTGRES_SECONDARY_SERVICE_TYPE = POSTGRES_SERVICE_TYPE + "-secondary"
POSTGRES_NODE_SERVICE_TYPE = POSTGRES_SERVICE_TYPE + "-node"

POSTGRES_SERVICE_PORT_DEFAULT = DATABASE_PORT_POSTGRES_DEFAULT

POSTGRES_ADMIN_USER_DEFAULT = DATABASE_USERNAME_POSTGRES_DEFAULT
POSTGRES_ADMIN_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

POSTGRES_REPLICATION_USER_DEFAULT = "repl_user"

POSTGRES_CLUSTER_MODE_NONE = "none"
# replication with physical replication
POSTGRES_CLUSTER_MODE_REPLICATION = "replication"

POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE = "none"
# FIRST 1
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_DEFAULT = "default"
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_FIRST = "first"
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_ANY = "any"

POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_ON = "on"
POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_OFF = "off"
POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_LOCAL = "local"

# repmgr
POSTGRES_REPMGR_CONFIG_KEY = "repmgr"

POSTGRES_REPMGR_ENABLED = "enabled"
POSTGRES_REPMGR_USER_CONFIG_KEY = "repmgr_user"
POSTGRES_REPMGR_PASSWORD_CONFIG_KEY = "repmgr_password"
POSTGRES_REPMGR_DATABASE_CONFIG_KEY = "repmgr_database"
POSTGRES_REPMGR_USE_PGREWIND_CONFIG_KEY = "use_pgrewind"
POSTGRES_REPMGR_USE_PASSFILE_CONFIG_KEY = "use_passfile"

POSTGRES_REPMGR_USER_DEFAULT = "repmgr"
POSTGRES_REPMGR_PASSWORD_DEFAULT = POSTGRES_ADMIN_PASSWORD_DEFAULT
POSTGRES_REPMGR_DATABASE_DEFAULT = "repmgr"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_POSTGRES, {})


def _get_service_port(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_SERVICE_PORT_CONFIG_KEY, POSTGRES_SERVICE_PORT_DEFAULT)


def _get_cluster_mode(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_CLUSTER_MODE_CONFIG_KEY, POSTGRES_CLUSTER_MODE_REPLICATION)


def _is_archive_mode_enabled(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_ARCHIVE_MODE_CONFIG_KEY, False)


def _is_replication_slot_enabled(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_REPLICATION_SLOT_CONFIG_KEY, False)


def _get_replication_synchronous_config(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_CONFIG_KEY, {})


def _get_replication_synchronous_mode(synchronous_config: Dict[str, Any]):
    return synchronous_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_MODE_CONFIG_KEY,
        POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE)


def _get_replication_synchronous_num(synchronous_config: Dict[str, Any]):
    return synchronous_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_NUM_CONFIG_KEY, 1)


def _get_replication_synchronous_commit_mode(synchronous_config: Dict[str, Any]):
    return synchronous_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_MODE_CONFIG_KEY,
        POSTGRES_REPLICATION_SYNCHRONOUS_COMMIT_ON)


def _get_repmgr_config(postgres_config: Dict[str, Any]):
    return postgres_config.get(
        POSTGRES_REPMGR_CONFIG_KEY, {})


def _is_repmgr_enabled(repmgr_config):
    return repmgr_config.get(POSTGRES_REPMGR_ENABLED, True)


def _get_health_check_port(postgres_config: Dict[str, Any]):
    default_port = 10000 + _get_service_port(postgres_config)
    return postgres_config.get(
        POSTGRES_HEALTH_CHECK_PORT_CONFIG_KEY, default_port)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_POSTGRES)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_POSTGRES: logs_dir}


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    postgres_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(postgres_config)
    if cluster_mode != POSTGRES_CLUSTER_MODE_NONE:
        synchronous_config = _get_replication_synchronous_config(
            postgres_config)
        if _get_replication_synchronous_mode(
                synchronous_config) != POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE:
            runtime_config = get_runtime_config_for_update(cluster_config)
            postgres_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_POSTGRES)
            postgres_workers = _sum_min_workers(cluster_config)
            if postgres_workers < 1:
                raise RuntimeError(
                    "Replication synchronously needs at least one workers.")
            postgres_config[POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY] = postgres_workers

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    postgres_config = _get_config(runtime_config)

    database = postgres_config.get(POSTGRES_DATABASE_CONFIG_KEY, {})
    user = database.get(POSTGRES_DATABASE_USER_CONFIG_KEY)
    password = database.get(POSTGRES_DATABASE_PASSWORD_CONFIG_KEY)
    if (user and not password) or (not user and password):
        raise ValueError(
            "Database user and password must be both specified or not specified.")

    if (_is_archive_mode_enabled(postgres_config) and not has_runtime_in_cluster(
            runtime_config, BUILT_IN_RUNTIME_MOUNT)):
        raise ValueError(
            "Archive mode needs {} runtime to be configured for data sharing.".format(
                BUILT_IN_RUNTIME_MOUNT))


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    postgres_config = _get_config(runtime_config)

    service_port = _get_service_port(postgres_config)
    runtime_envs["POSTGRES_SERVICE_PORT"] = service_port

    admin_user = postgres_config.get(
        POSTGRES_ADMIN_USER_CONFIG_KEY, POSTGRES_ADMIN_USER_DEFAULT)
    runtime_envs["POSTGRES_USER"] = admin_user

    admin_password = postgres_config.get(
        POSTGRES_ADMIN_PASSWORD_CONFIG_KEY, POSTGRES_ADMIN_PASSWORD_DEFAULT)
    runtime_envs["POSTGRES_PASSWORD"] = admin_password

    database = postgres_config.get(POSTGRES_DATABASE_CONFIG_KEY, {})
    database_name = database.get(POSTGRES_DATABASE_NAME_CONFIG_KEY)
    if database_name:
        runtime_envs["POSTGRES_DATABASE_NAME"] = database_name
    user = database.get(POSTGRES_DATABASE_USER_CONFIG_KEY)
    if user:
        runtime_envs["POSTGRES_DATABASE_USER"] = user
    password = database.get(POSTGRES_DATABASE_PASSWORD_CONFIG_KEY)
    if password:
        runtime_envs["POSTGRES_DATABASE_PASSWORD"] = password

    cluster_mode = _get_cluster_mode(postgres_config)
    runtime_envs["POSTGRES_CLUSTER_MODE"] = cluster_mode

    runtime_envs["POSTGRES_ARCHIVE_MODE"] = _is_archive_mode_enabled(
        postgres_config)

    replication_user = postgres_config.get(
        POSTGRES_REPLICATION_USER_CONFIG_KEY, POSTGRES_REPLICATION_USER_DEFAULT)
    runtime_envs["POSTGRES_REPLICATION_USER"] = replication_user

    replication_password = postgres_config.get(
        POSTGRES_REPLICATION_PASSWORD_CONFIG_KEY, POSTGRES_ADMIN_PASSWORD_DEFAULT)
    runtime_envs["POSTGRES_REPLICATION_PASSWORD"] = replication_password

    if cluster_mode == POSTGRES_CLUSTER_MODE_REPLICATION:
        # For enable replication slot, we need seq id (stable seq id is preferred)
        # Also user need to monitor the disk usage for primary if there is any
        # standby dead and cannot consume WAL which make the disk usage keep increasing.
        runtime_envs["POSTGRES_REPLICATION_SLOT"] = _is_replication_slot_enabled(
            postgres_config)

        synchronous_config = _get_replication_synchronous_config(
            postgres_config)
        replication_synchronous_mode = _get_replication_synchronous_mode(
            synchronous_config)
        runtime_envs["POSTGRES_SYNCHRONOUS_MODE"] = replication_synchronous_mode
        if replication_synchronous_mode != POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE:
            synchronous_size = postgres_config.get(
                POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY, 1)
            synchronous_num = _get_replication_synchronous_num(
                synchronous_config)
            if synchronous_num > synchronous_size:
                synchronous_num = synchronous_size
            synchronous_commit_mode = _get_replication_synchronous_commit_mode(
                synchronous_config)
            runtime_envs["POSTGRES_SYNCHRONOUS_NUM"] = synchronous_num
            runtime_envs["POSTGRES_SYNCHRONOUS_SIZE"] = synchronous_size
            runtime_envs["POSTGRES_SYNCHRONOUS_COMMIT_MODE"] = synchronous_commit_mode

        # repmgr
        repmgr_config = _get_repmgr_config(postgres_config)
        repmgr_enabled = _is_repmgr_enabled(repmgr_config)
        runtime_envs["POSTGRES_REPMGR_ENABLED"] = repmgr_enabled
        if repmgr_enabled:
            repmgr_user = repmgr_config.get(
                POSTGRES_REPMGR_USER_CONFIG_KEY, POSTGRES_REPMGR_USER_DEFAULT)
            runtime_envs["POSTGRES_REPMGR_USER"] = repmgr_user

            repmgr_password = repmgr_config.get(
                POSTGRES_REPMGR_PASSWORD_CONFIG_KEY, POSTGRES_REPMGR_PASSWORD_DEFAULT)
            runtime_envs["POSTGRES_REPMGR_PASSWORD"] = repmgr_password

            repmgr_database = repmgr_config.get(
                POSTGRES_REPMGR_DATABASE_CONFIG_KEY, POSTGRES_REPMGR_DATABASE_DEFAULT)
            runtime_envs["POSTGRES_REPMGR_DATABASE"] = repmgr_database

            repmgr_use_rewind = repmgr_config.get(
                POSTGRES_REPMGR_USE_PGREWIND_CONFIG_KEY, True)
            runtime_envs["POSTGRES_REPMGR_USE_PGREWIND"] = repmgr_use_rewind

            repmgr_use_passfile = repmgr_config.get(
                POSTGRES_REPMGR_USE_PASSFILE_CONFIG_KEY, True)
            runtime_envs["POSTGRES_REPMGR_USE_PASSFILE"] = repmgr_use_passfile

    return runtime_envs


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "postgres": {
            "name": "Postgres",
            "url": address_string(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "postgres": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    postgres_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(postgres_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, POSTGRES_SERVICE_TYPE)
    service_port = _get_service_port(postgres_config)
    cluster_mode = _get_cluster_mode(postgres_config)
    if cluster_mode == POSTGRES_CLUSTER_MODE_REPLICATION:
        repmgr_config = _get_repmgr_config(postgres_config)
        repmgr_enabled = _is_repmgr_enabled(repmgr_config)
        if repmgr_enabled:
            # every node is possible be primary and standby
            services = {
                service_name: define_runtime_service(
                    POSTGRES_NODE_SERVICE_TYPE,
                    service_discovery_config, service_port,
                    features=[SERVICE_DISCOVERY_FEATURE_DATABASE]),
            }
        else:
            # primary service on head and secondary service on workers
            secondary_service_name = get_canonical_service_name(
                service_discovery_config, cluster_name, POSTGRES_SECONDARY_SERVICE_TYPE)
            services = {
                service_name: define_runtime_service_on_head(
                    POSTGRES_SERVICE_TYPE,
                    service_discovery_config, service_port,
                    features=[SERVICE_DISCOVERY_FEATURE_DATABASE]),
                secondary_service_name: define_runtime_service_on_worker(
                    POSTGRES_SECONDARY_SERVICE_TYPE,
                    service_discovery_config, service_port,
                    features=[SERVICE_DISCOVERY_FEATURE_DATABASE]),
            }
    else:
        # single standalone on head
        services = {
            service_name: define_runtime_service_on_head(
                POSTGRES_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_DATABASE]),
        }
    return services


def _get_health_check(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    postgres_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(postgres_config)
    health_check_port = _get_health_check_port(postgres_config)

    node_kind = HEALTH_CHECK_NODE_KIND_HEAD
    if cluster_mode == POSTGRES_CLUSTER_MODE_REPLICATION:
        node_kind = HEALTH_CHECK_NODE_KIND_NODE

    health_check = {
        HEALTH_CHECK_PORT: health_check_port,
        HEALTH_CHECK_NODE_KIND: node_kind,
    }
    return health_check
