import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_METASTORE
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, \
    get_service_discovery_config, define_runtime_service_on_head_or_all
from cloudtik.core._private.util.core_utils import export_environment_variables
from cloudtik.core._private.util.database_utils import is_database_configured, \
    with_database_environment_variables
from cloudtik.core._private.utils import export_runtime_flags, get_node_cluster_ip_of, get_runtime_config, \
    PROVIDER_DATABASE_CONFIG_KEY, is_use_managed_cloud_database, get_provider_config, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_database_from_workspace, discover_database_on_head, DATABASE_CONNECT_KEY, \
    get_database_runtime_in_cluster, with_database_runtime_environment_variables, \
    is_database_service_discovery
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["proc_metastore", False, "Metastore", "node"],
]

METASTORE_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

METASTORE_SERVICE_TYPE = BUILT_IN_RUNTIME_METASTORE
METASTORE_SERVICE_PORT = 9083


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_METASTORE, {})


def _get_database_config(metastore_config):
    return metastore_config.get(DATABASE_CONNECT_KEY, {})


def _is_high_availability(metastore_config: Dict[str, Any]):
    return metastore_config.get(
        METASTORE_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_database_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_METASTORE)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_database_on_head(
        cluster_config, BUILT_IN_RUNTIME_METASTORE)
    return cluster_config


def _validate_config(config: Dict[str, Any], final=False):
    if not _is_valid_database_config(config, final):
        raise ValueError(
            "Database must be configured for Metastore.")


def _is_valid_database_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    metastore_config = _get_config(runtime_config)

    # Check database configuration
    database_config = _get_database_config(metastore_config)
    if is_database_configured(database_config):
        return True

    # check in cluster database
    database_runtime = get_database_runtime_in_cluster(
        runtime_config)
    if database_runtime:
        return True

    # check whether cloud database is available
    provider_config = get_provider_config(config)
    if (PROVIDER_DATABASE_CONFIG_KEY in provider_config or
            (not final and is_use_managed_cloud_database(config))):
        return True

    # if there is service discovery mechanism, assume we can get from service discovery
    if (not final and is_database_service_discovery(metastore_config)
            and get_service_discovery_runtime(runtime_config)):
        return True

    return False


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {"METASTORE_ENABLED": True}

    metastore_config = _get_config(runtime_config)

    high_availability = _is_high_availability(metastore_config)
    if high_availability:
        runtime_envs["METASTORE_HIGH_AVAILABILITY"] = high_availability

    export_runtime_flags(
        metastore_config, BUILT_IN_RUNTIME_METASTORE, runtime_envs)
    return runtime_envs


def _with_database_configurations(runtime_config, envs=None):
    metastore_config = _get_config(runtime_config)

    # first the user configured or discovered
    database_config = _get_database_config(metastore_config)
    if is_database_configured(database_config):
        # set the database environments from database config
        # This may override the environments from provider
        envs = with_database_environment_variables(database_config, envs)
        return envs

    # next the in cluster database
    database_runtime = get_database_runtime_in_cluster(
        runtime_config)
    if database_runtime:
        envs = with_database_runtime_environment_variables(
            runtime_config, database_runtime, envs)
        return envs

    # finally cloud database is configured
    # database environment variables already exported
    return envs


def _node_configure(runtime_config, head: bool):
    envs = _with_database_configurations(runtime_config)
    export_environment_variables(envs)


def _node_services(runtime_config, head: bool):
    # We put the database schema init right before the start of metastore service
    envs = _with_database_configurations(runtime_config)
    export_environment_variables(envs)


def register_service(cluster_config: Dict[str, Any], head_node_id: str) -> None:
    head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
    head_host = get_cluster_head_host(cluster_config, head_ip)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_METASTORE,
        service_addresses=[(head_host, METASTORE_SERVICE_PORT)])


def _get_runtime_logs():
    hive_logs_dir = os.path.join(os.getenv("METASTORE_HOME"), "logs")
    all_logs = {"metastore": hive_logs_dir}
    return all_logs


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    endpoints = {
        "metastore": {
            "name": "Metastore Uri",
            "url": "thrift://{}:{}".format(head_host, METASTORE_SERVICE_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "metastore": {
            "protocol": "TCP",
            "port": METASTORE_SERVICE_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    metastore_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(metastore_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, METASTORE_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            METASTORE_SERVICE_TYPE,
            service_discovery_config, METASTORE_SERVICE_PORT,
            _is_high_availability(metastore_config),
        )
    }
    return services
