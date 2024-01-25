import os
from typing import Any, Dict

from cloudtik.core._private.util.core_utils import http_address_string, \
    export_environment_variables
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_AI
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, \
    get_service_discovery_config, SERVICE_DISCOVERY_PROTOCOL_HTTP, define_runtime_service_on_head_or_all
from cloudtik.core._private.util.database_utils import is_database_configured, with_database_environment_variables
from cloudtik.core._private.utils import export_runtime_flags, get_node_cluster_ip_of, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import discover_hdfs_on_head, \
    discover_hdfs_from_workspace, HDFS_URI_KEY, discover_database_from_workspace, discover_database_on_head, \
    DATABASE_CONNECT_KEY, get_database_runtime_in_cluster, with_database_runtime_environment_variables
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace
from cloudtik.runtime.common.utils import get_runtime_endpoints_of

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["mlflow.server:app", False, "MLflow", "head"],
]

MLFLOW_HIGH_AVAILABILITY_CONFIG_KEY = "high_availability"

MLFLOW_SERVICE_TYPE = "mlflow"
MLFLOW_SERVICE_PORT = 5001


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_AI, {})


def _get_database_config(ai_config):
    return ai_config.get(DATABASE_CONNECT_KEY, {})


def _is_high_availability(ai_config: Dict[str, Any]):
    return ai_config.get(
        MLFLOW_HIGH_AVAILABILITY_CONFIG_KEY, False)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_hdfs_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_AI)
    cluster_config = discover_database_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_AI)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_hdfs_on_head(
        cluster_config, BUILT_IN_RUNTIME_AI)
    cluster_config = discover_database_on_head(
        cluster_config, BUILT_IN_RUNTIME_AI)
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {"AI_ENABLED": True}

    ai_config = _get_config(runtime_config)

    high_availability = _is_high_availability(ai_config)
    if high_availability:
        runtime_envs["MLFLOW_HIGH_AVAILABILITY"] = high_availability

    export_runtime_flags(
        ai_config, BUILT_IN_RUNTIME_AI, runtime_envs)

    return runtime_envs


def _with_database_configurations(runtime_config, envs=None):
    ai_config = _get_config(runtime_config)

    # first the user configured or discovered
    database_config = _get_database_config(ai_config)
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

    # final the cloud database configured
    # database environment variables already exported
    return envs


def _node_configure(runtime_config, head: bool):
    ai_config = _get_config(runtime_config)
    envs = {}

    hadoop_default_cluster = ai_config.get(
        "hadoop_default_cluster", False)
    if hadoop_default_cluster:
        envs["HADOOP_DEFAULT_CLUSTER"] = hadoop_default_cluster

    hdfs_uri = ai_config.get(HDFS_URI_KEY)
    if hdfs_uri:
        envs["HDFS_NAMENODE_URI"] = hdfs_uri

    envs = _with_database_configurations(runtime_config, envs)
    export_environment_variables(envs)


def _node_services(runtime_config, head: bool):
    # We put the database schema init right before the start of metastore service
    envs = _with_database_configurations(runtime_config)
    export_environment_variables(envs)


def register_service(cluster_config: Dict[str, Any], head_node_id: str) -> None:
    head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
    head_host = get_cluster_head_host(cluster_config, head_ip)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_AI,
        service_addresses=[(head_host, MLFLOW_SERVICE_PORT)],
        service_name=MLFLOW_SERVICE_TYPE)


def _get_runtime_logs():
    mlflow_logs_dir = os.path.join(
        os.getenv("HOME"), "runtime", "mlflow", "logs")
    all_logs = {"mlflow": mlflow_logs_dir}
    return all_logs


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    endpoints = {
        "mlflow": {
            "name": "MLflow",
            "url": http_address_string(head_host, MLFLOW_SERVICE_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "mlflow": {
            "protocol": "TCP",
            "port": MLFLOW_SERVICE_PORT,
        },
    }
    return service_ports


def get_runtime_endpoints(config: Dict[str, Any]):
    return get_runtime_endpoints_of(config, BUILT_IN_RUNTIME_AI)


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    ai_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(ai_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, MLFLOW_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_head_or_all(
            MLFLOW_SERVICE_TYPE,
            service_discovery_config, MLFLOW_SERVICE_PORT,
            _is_high_availability(ai_config),
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP),
    }
    return services
