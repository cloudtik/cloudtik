import os
from typing import Any, Dict

from cloudtik.core._private.core_utils import get_config_for_update
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_POSTGRES, BUILT_IN_RUNTIME_MOUNT
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, get_service_discovery_config, \
    SERVICE_DISCOVERY_FEATURE_DATABASE, define_runtime_service_on_head, \
    define_runtime_service_on_worker
from cloudtik.core._private.util.database_utils import DATABASE_PORT_POSTGRES_DEFAULT, \
    DATABASE_USERNAME_POSTGRES_DEFAULT, DATABASE_PASSWORD_POSTGRES_DEFAULT
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY, is_node_seq_id_enabled, enable_node_seq_id, \
    _sum_min_workers, get_runtime_config_for_update
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["postgres", True, "Postgres", "node"],
    ]

POSTGRES_SERVICE_PORT_CONFIG_KEY = "port"

POSTGRES_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
POSTGRES_ADMIN_USER_CONFIG_KEY = "admin_user"
POSTGRES_ADMIN_PASSWORD_CONFIG_KEY = "admin_password"
POSTGRES_REPLICATION_PASSWORD_CONFIG_KEY = "replication_password"

POSTGRES_ARCHIVE_MODE_CONFIG_KEY = "archive_mode"
POSTGRES_REPLICATION_SLOT_CONFIG_KEY = "replication_slot"
POSTGRES_REPLICATION_SYNCHRONOUS_CONFIG_KEY = "replication_synchronous"

POSTGRES_REPLICATION_SYNCHRONOUS_MODE_CONFIG_KEY = "mode"
POSTGRES_REPLICATION_SYNCHRONOUS_NUM_CONFIG_KEY = "num"

POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY = "synchronous_size"

POSTGRES_DATABASE_CONFIG_KEY = "database"
POSTGRES_DATABASE_NAME_CONFIG_KEY = "name"
POSTGRES_DATABASE_USER_CONFIG_KEY = "user"
POSTGRES_DATABASE_PASSWORD_CONFIG_KEY = "password"

POSTGRES_SERVICE_TYPE = BUILT_IN_RUNTIME_POSTGRES
POSTGRES_REPLICA_SERVICE_TYPE = POSTGRES_SERVICE_TYPE + "-replica"
POSTGRES_SERVICE_PORT_DEFAULT = DATABASE_PORT_POSTGRES_DEFAULT

POSTGRES_ADMIN_USER_DEFAULT = DATABASE_USERNAME_POSTGRES_DEFAULT
POSTGRES_ADMIN_PASSWORD_DEFAULT = DATABASE_PASSWORD_POSTGRES_DEFAULT

POSTGRES_CLUSTER_MODE_NONE = "none"
# replication with physical replication
POSTGRES_CLUSTER_MODE_REPLICATION = "replication"

POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE = "none"
# FIRST 1
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_DEFAULT = "default"
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_FIRST = "first"
POSTGRES_REPLICATION_SYNCHRONOUS_MODE_ANY = "any"


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


def _get_replication_synchronous_mode(postgres_config: Dict[str, Any]):
    replication_synchronous_config = _get_replication_synchronous_config(
        postgres_config)
    return replication_synchronous_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_MODE_CONFIG_KEY,
        POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE)


def _get_replication_synchronous_num(postgres_config: Dict[str, Any]):
    replication_synchronous_config = _get_replication_synchronous_config(
        postgres_config)
    return replication_synchronous_config.get(
        POSTGRES_REPLICATION_SYNCHRONOUS_NUM_CONFIG_KEY, 1)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_POSTGRES)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"postgres": logs_dir}


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    postgres_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(postgres_config)
    if cluster_mode != POSTGRES_CLUSTER_MODE_NONE:
        # We must enable the node seq id (stable seq id is preferred)
        # But we don't enforce it.
        if not is_node_seq_id_enabled(cluster_config):
            enable_node_seq_id(cluster_config)

        if _get_replication_synchronous_mode(
                postgres_config) != POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE:
            runtime_config = get_runtime_config_for_update(cluster_config)
            postgres_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_POSTGRES)
            postgres_workers = _sum_min_workers(cluster_config)
            if postgres_workers < 1:
                raise RuntimeError("Replication synchronously needs at least one workers.")
            postgres_config[POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY] = postgres_workers

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    postgres_config = _get_config(runtime_config)

    database = postgres_config.get(POSTGRES_DATABASE_CONFIG_KEY, {})
    user = database.get(POSTGRES_DATABASE_USER_CONFIG_KEY)
    password = database.get(POSTGRES_DATABASE_PASSWORD_CONFIG_KEY)
    if (user and not password) or (not user and password):
        raise ValueError("Database user and password must be both specified or not specified.")

    if (_is_archive_mode_enabled(postgres_config) and not has_runtime_in_cluster(
            runtime_config, BUILT_IN_RUNTIME_MOUNT)):
        raise ValueError("Archive mode needs {} runtime to be configured for data sharing.".format(
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

    replication_password = postgres_config.get(
        POSTGRES_ADMIN_PASSWORD_CONFIG_KEY, POSTGRES_ADMIN_PASSWORD_DEFAULT)
    runtime_envs["POSTGRES_REPLICATION_PASSWORD"] = replication_password

    if cluster_mode == POSTGRES_CLUSTER_MODE_REPLICATION:
        # For enable replication slot, we need seq id (stable seq id is preferred)
        # Also user need to monitor the disk usage for primary if there is any
        # standby dead and cannot consume WAL which make the disk usage keep increasing.
        runtime_envs["POSTGRES_REPLICATION_SLOT"] = _is_replication_slot_enabled(
            postgres_config)

        replication_synchronous_mode = _get_replication_synchronous_mode(
            postgres_config)
        runtime_envs["POSTGRES_SYNCHRONOUS_MODE"] = replication_synchronous_mode
        if replication_synchronous_mode != POSTGRES_REPLICATION_SYNCHRONOUS_MODE_NONE:
            synchronous_size = postgres_config.get(
                POSTGRES_SYNCHRONOUS_SIZE_CONFIG_KEY, 1)
            synchronous_num = _get_replication_synchronous_num(
                postgres_config)
            if synchronous_num > synchronous_size:
                synchronous_num = synchronous_size
            runtime_envs["POSTGRES_SYNCHRONOUS_NUM"] = synchronous_num
            runtime_envs["POSTGRES_SYNCHRONOUS_SIZE"] = synchronous_size

    return runtime_envs


def _get_runtime_endpoints(runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "postgres": {
            "name": "Postgres",
            "url": "{}:{}".format(cluster_head_ip, service_port)
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
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    postgres_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(postgres_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, POSTGRES_SERVICE_TYPE)
    service_port = _get_service_port(postgres_config)
    cluster_mode = _get_cluster_mode(postgres_config)
    if cluster_mode == POSTGRES_CLUSTER_MODE_REPLICATION:
        # primary service on head and replica service on workers
        replica_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, POSTGRES_REPLICA_SERVICE_TYPE)
        services = {
            service_name: define_runtime_service_on_head(
                POSTGRES_SERVICE_TYPE,
                service_discovery_config, service_port,
                features=[SERVICE_DISCOVERY_FEATURE_DATABASE]),
            replica_service_name: define_runtime_service_on_worker(
                POSTGRES_REPLICA_SERVICE_TYPE,
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
