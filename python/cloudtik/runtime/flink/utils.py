import os
from typing import Any, Dict

from cloudtik.core._private.cluster.cluster_tunnel_request import _request_rest_to_head
from cloudtik.core._private.util.core_utils import double_quote, http_address_string, export_environment_variables
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_FLINK, BUILT_IN_RUNTIME_YARN, \
    BUILT_IN_RUNTIME_HADOOP
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_PROTOCOL_HTTP
from cloudtik.core._private.utils import round_memory_size_to_gb, RUNTIME_CONFIG_KEY, get_config_for_update, \
    get_runtime_config, \
    get_node_type_resources, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_metastore_from_workspace, discover_metastore_on_head, METASTORE_URI_KEY
from cloudtik.runtime.common.utils import get_runtime_endpoints_of, get_runtime_default_storage_of

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["org.apache.flink.runtime.webmonitor.history.HistoryServer", False, "FlinkHistoryServer", "head"],
]

FLINK_YARN_RESOURCE_MEMORY_RATIO = 0.8

FLINK_TASKMANAGER_MEMORY_RATIO = 1
FLINK_JOBMANAGER_MEMORY_RATIO = 0.02
FLINK_JOBMANAGER_MEMORY_MINIMUM = 1024
FLINK_JOBMANAGER_MEMORY_MAXIMUM = 8192
FLINK_TASKMANAGER_CORES_DEFAULT = 4
FLINK_ADDITIONAL_OVERHEAD = 1024
FLINK_TASKMANAGER_OVERHEAD_MINIMUM = 384
FLINK_TASKMANAGER_OVERHEAD_RATIO = 0.1

FLINK_HISTORY_SERVER_API_PORT = 8082
FLINK_JUPYTER_WEB_PORT = 8888

FLINK_HISTORY_SERVER_SERVICE_TYPE = "flink-history"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_FLINK, {})


def get_yarn_resource_memory_ratio(cluster_config: Dict[str, Any]):
    yarn_resource_memory_ratio = FLINK_YARN_RESOURCE_MEMORY_RATIO
    runtime_config = get_runtime_config(cluster_config)
    yarn_config = runtime_config.get(BUILT_IN_RUNTIME_YARN, {})
    memory_ratio = yarn_config.get("yarn_resource_memory_ratio")
    if memory_ratio:
        yarn_resource_memory_ratio = memory_ratio
    return yarn_resource_memory_ratio


def get_flink_jobmanager_memory(worker_memory_for_flink: int) -> int:
    flink_jobmanager_memory = round_memory_size_to_gb(
        int(worker_memory_for_flink * FLINK_JOBMANAGER_MEMORY_RATIO))
    return max(min(flink_jobmanager_memory,
               FLINK_JOBMANAGER_MEMORY_MAXIMUM), FLINK_JOBMANAGER_MEMORY_MINIMUM)


def get_flink_overhead(worker_memory_for_flink: int) -> int:
    # Calculate the flink overhead including one jobmanager based on worker_memory_for_flink
    flink_jobmanager = get_flink_jobmanager_memory(worker_memory_for_flink)
    return flink_jobmanager + FLINK_ADDITIONAL_OVERHEAD


def get_flink_taskmanager_overhead(flink_taskmanager_memory_all: int) -> int:
    return max(
        int(flink_taskmanager_memory_all * FLINK_TASKMANAGER_OVERHEAD_RATIO),
        FLINK_TASKMANAGER_OVERHEAD_MINIMUM)


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = _configure_runtime_resources(cluster_config)
    cluster_config = discover_metastore_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_FLINK)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_metastore_on_head(
        cluster_config, BUILT_IN_RUNTIME_FLINK)

    # call validate config to fail earlier
    _validate_config(cluster_config, final=True)

    return cluster_config


def _configure_runtime_resources(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_resource = get_node_type_resources(cluster_config)

    yarn_resource_memory_ratio = get_yarn_resource_memory_ratio(cluster_config)
    worker_memory_for_yarn = round_memory_size_to_gb(
        int(cluster_resource["worker_memory"] * yarn_resource_memory_ratio))

    # Calculate Flink taskmanager cores
    flink_taskmanager_cores = FLINK_TASKMANAGER_CORES_DEFAULT
    if flink_taskmanager_cores > cluster_resource["worker_cpu"]:
        flink_taskmanager_cores = cluster_resource["worker_cpu"]

    runtime_resource = {"flink_taskmanager_cores": flink_taskmanager_cores}

    # For Flink taskmanager memory, we use the following formula:
    # x = worker_memory_for_yarn
    # n = number_of_taskmanagers
    # m = flink_taskmanager_memory
    # a = flink_overhead (jobmanager_memory + others)
    # x = n * m + a

    number_of_taskmanagers = int(cluster_resource["worker_cpu"] / flink_taskmanager_cores)
    worker_memory_for_flink = round_memory_size_to_gb(
        int(worker_memory_for_yarn * FLINK_TASKMANAGER_MEMORY_RATIO))
    flink_overhead = round_memory_size_to_gb(
        get_flink_overhead(worker_memory_for_flink))
    worker_memory_for_taskmanagers = worker_memory_for_flink - flink_overhead
    flink_taskmanager_memory_all = round_memory_size_to_gb(
        int(worker_memory_for_taskmanagers / number_of_taskmanagers))
    runtime_resource["flink_taskmanager_memory"] =\
        flink_taskmanager_memory_all - get_flink_taskmanager_overhead(
            flink_taskmanager_memory_all)
    runtime_resource["flink_jobmanager_memory"] = get_flink_jobmanager_memory(
        worker_memory_for_flink)

    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    flink_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_FLINK)
    flink_config["flink_resource"] = runtime_resource
    return cluster_config


def get_runtime_processes():
    return RUNTIME_PROCESSES


def _is_runtime_scripts(script_file):
    if script_file.endswith(".scala"):
        return True
    return False


def _get_runnable_command(target):
    command_parts = ["flink", "-i", double_quote(target)]
    return command_parts


def _get_flink_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    if not runtime_config:
        return None

    flink_config = _get_config(runtime_config)
    return flink_config.get("config")


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {}
    return runtime_envs


def _node_configure(runtime_config, head: bool):
    # TODO: move more runtime specific environment_variables to here
    flink_config = _get_config(runtime_config)
    metastore_uri = flink_config.get(METASTORE_URI_KEY)
    envs = {}
    if metastore_uri:
        envs["HIVE_METASTORE_URI"] = metastore_uri
    export_environment_variables(envs)


def get_runtime_logs():
    flink_logs_dir = os.path.join(os.getenv("FLINK_HOME"), "logs")
    jupyter_logs_dir = os.path.join(
        os.getenv("HOME"), "runtime", "jupyter", "logs")
    all_logs = {"flink": flink_logs_dir,
                "jupyter": jupyter_logs_dir
                }
    return all_logs


def _validate_config(config: Dict[str, Any], final=False):
    pass


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    endpoints = {
        "jupyter-web": {
            "name": "Jupyter Web UI",
            "url": http_address_string(head_host, FLINK_JUPYTER_WEB_PORT),
            "info": "default password is \'cloudtik\'"
         },
        "flink-history": {
            "name": "Flink History Server Web UI",
            "url": http_address_string(head_host, FLINK_HISTORY_SERVER_API_PORT)
         },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "jupyter-web": {
            "protocol": "TCP",
            "port": FLINK_JUPYTER_WEB_PORT,
        },
        "flink-history": {
            "protocol": "TCP",
            "port": FLINK_HISTORY_SERVER_API_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    flink_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(flink_config)
    flink_history_service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, FLINK_HISTORY_SERVER_SERVICE_TYPE)
    services = {
        flink_history_service_name: define_runtime_service_on_head(
            FLINK_HISTORY_SERVER_SERVICE_TYPE,
            service_discovery_config, FLINK_HISTORY_SERVER_API_PORT,
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP),
    }
    return services


def request_rest_jobs(
        config: Dict[str, Any], endpoint: str,
        on_head: bool = False):
    if endpoint is None:
        endpoint = "/jobs/overview"
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    return _request_rest_to_head(
        config, endpoint, FLINK_HISTORY_SERVER_API_PORT,
        on_head=on_head)


def get_runtime_default_storage(config: Dict[str, Any]):
    return get_runtime_default_storage_of(config, BUILT_IN_RUNTIME_HADOOP)


def get_runtime_endpoints(config: Dict[str, Any]):
    return get_runtime_endpoints_of(config, BUILT_IN_RUNTIME_FLINK)
