import os
from typing import Any, Dict

from cloudtik.core._private.cluster.cluster_tunnel_request import _request_rest_to_head
from cloudtik.core._private.util.core_utils import double_quote, http_address_string, export_environment_variables
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_SPARK, BUILT_IN_RUNTIME_YARN, \
    BUILT_IN_RUNTIME_HADOOP
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_PROTOCOL_HTTP
from cloudtik.core._private.utils import \
    round_memory_size_to_gb, RUNTIME_CONFIG_KEY, get_config_for_update, get_runtime_config, \
    get_node_type_resources, get_cluster_name
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_metastore_on_head, discover_metastore_from_workspace, METASTORE_URI_KEY
from cloudtik.runtime.common.utils import get_runtime_endpoints_of, get_runtime_default_storage_of

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["org.apache.spark.deploy.history.HistoryServer", False, "SparkHistoryServer", "head"],
]

SPARK_YARN_RESOURCE_MEMORY_RATIO = 0.8

SPARK_EXECUTOR_MEMORY_RATIO = 1
SPARK_DRIVER_MEMORY_RATIO = 0.1
SPARK_APP_MASTER_MEMORY_RATIO = 0.02
SPARK_DRIVER_MEMORY_MINIMUM = 1024
SPARK_DRIVER_MEMORY_MAXIMUM = 8192
SPARK_EXECUTOR_CORES_DEFAULT = 4
SPARK_EXECUTOR_CORES_SINGLE_BOUND = 8
SPARK_ADDITIONAL_OVERHEAD = 1024
SPARK_EXECUTOR_OVERHEAD_MINIMUM = 384
SPARK_EXECUTOR_OVERHEAD_RATIO = 0.1

SPARK_HISTORY_SERVER_API_PORT = 18080
SPARK_JUPYTER_WEB_PORT = 8888

SPARK_HISTORY_SERVER_SERVICE_TYPE = "spark-history"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_SPARK, {})


def get_yarn_resource_memory_ratio(cluster_config: Dict[str, Any]):
    yarn_resource_memory_ratio = SPARK_YARN_RESOURCE_MEMORY_RATIO
    runtime_config = get_runtime_config(cluster_config)
    yarn_config = runtime_config.get(BUILT_IN_RUNTIME_YARN, {})
    memory_ratio = yarn_config.get("yarn_resource_memory_ratio")
    if memory_ratio:
        yarn_resource_memory_ratio = memory_ratio
    return yarn_resource_memory_ratio


def get_spark_driver_memory(cluster_resource: Dict[str, Any]) -> int:
    spark_driver_memory = round_memory_size_to_gb(
        int(cluster_resource["head_memory"] * SPARK_DRIVER_MEMORY_RATIO))
    return max(min(
        spark_driver_memory, SPARK_DRIVER_MEMORY_MAXIMUM),
        SPARK_DRIVER_MEMORY_MINIMUM)


def get_spark_app_master_memory(worker_memory_for_spark: int) -> int:
    spark_app_master_memory = round_memory_size_to_gb(
        int(worker_memory_for_spark * SPARK_APP_MASTER_MEMORY_RATIO))
    return max(min(
        spark_app_master_memory, SPARK_DRIVER_MEMORY_MAXIMUM),
        SPARK_DRIVER_MEMORY_MINIMUM)


def get_spark_overhead(worker_memory_for_spark: int) -> int:
    # Calculate the spark overhead including one app master based on worker_memory_for_spark
    spark_app_master = get_spark_app_master_memory(worker_memory_for_spark)
    return spark_app_master + SPARK_ADDITIONAL_OVERHEAD


def get_spark_executor_overhead(spark_executor_memory_all: int) -> int:
    return max(int(spark_executor_memory_all * SPARK_EXECUTOR_OVERHEAD_RATIO),
               SPARK_EXECUTOR_OVERHEAD_MINIMUM)


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = _configure_runtime_resources(cluster_config)
    cluster_config = discover_metastore_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_SPARK)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_metastore_on_head(
        cluster_config, BUILT_IN_RUNTIME_SPARK)

    # call validate config to fail earlier
    _validate_config(cluster_config, final=True)
    return cluster_config


def _configure_runtime_resources(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_resource = get_node_type_resources(cluster_config)
    worker_cpu = cluster_resource["worker_cpu"]

    yarn_resource_memory_ratio = get_yarn_resource_memory_ratio(cluster_config)
    worker_memory_for_yarn = round_memory_size_to_gb(
        int(cluster_resource["worker_memory"] * yarn_resource_memory_ratio))

    executor_resource = {"spark_driver_memory": get_spark_driver_memory(cluster_resource)}

    # Calculate Spark executor cores
    spark_executor_cores = SPARK_EXECUTOR_CORES_DEFAULT
    if spark_executor_cores > worker_cpu:
        spark_executor_cores = worker_cpu
    elif (worker_cpu % SPARK_EXECUTOR_CORES_DEFAULT) != 0:
        if worker_cpu <= SPARK_EXECUTOR_CORES_SINGLE_BOUND:
            spark_executor_cores = worker_cpu
        elif worker_cpu <= SPARK_EXECUTOR_CORES_SINGLE_BOUND * 2:
            if (worker_cpu % 2) != 0:
                # Overload 1 core
                worker_cpu += 1
            spark_executor_cores = int(worker_cpu / 2)
        else:
            # Overload max number of SPARK_EXECUTOR_CORES_DEFAULT - 1 cores
            overload_cores = SPARK_EXECUTOR_CORES_DEFAULT - (
                    worker_cpu % SPARK_EXECUTOR_CORES_DEFAULT)
            worker_cpu += overload_cores

    executor_resource["spark_executor_cores"] = spark_executor_cores

    # For Spark executor memory, we use the following formula:
    # x = worker_memory_for_yarn
    # n = number_of_executors
    # m = spark_executor_memory
    # a = spark_overhead (app_master_memory + others)
    # x = n * m + a

    number_of_executors = int(worker_cpu / spark_executor_cores)
    worker_memory_for_spark = round_memory_size_to_gb(
        int(worker_memory_for_yarn * SPARK_EXECUTOR_MEMORY_RATIO))
    spark_overhead = round_memory_size_to_gb(
        get_spark_overhead(worker_memory_for_spark))
    worker_memory_for_executors = worker_memory_for_spark - spark_overhead
    spark_executor_memory_all = round_memory_size_to_gb(
        int(worker_memory_for_executors / number_of_executors))
    executor_resource["spark_executor_memory"] = \
        spark_executor_memory_all - get_spark_executor_overhead(
            spark_executor_memory_all)

    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    spark_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_SPARK)

    spark_config["spark_executor_resource"] = executor_resource
    return cluster_config


def get_runtime_processes():
    return RUNTIME_PROCESSES


def _is_runtime_scripts(script_file):
    if (script_file.endswith(".scala")
            or script_file.endswith(".jar")
            or script_file.endswith(".py")):
        return True
    return False


def _get_runnable_command(target, runtime_options):
    command_parts = []
    if target.endswith(".scala"):
        command_parts = ["spark-shell", "-i", double_quote(target)]
    elif target.endswith(".jar") or target.endswith(".py"):
        command_parts = ["spark-submit"]
        if runtime_options is not None:
            command_parts += runtime_options
        command_parts += [double_quote(target)]
    return command_parts


def _get_spark_config(config: Dict[str, Any]):
    runtime_config = get_runtime_config(config)
    if not runtime_config:
        return None
    spark_config = _get_config(runtime_config)
    return spark_config.get("config")


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {}
    return runtime_envs


def _node_configure(runtime_config, head: bool):
    spark_config = _get_config(runtime_config)
    metastore_uri = spark_config.get(METASTORE_URI_KEY)
    envs = {}
    if metastore_uri:
        envs["HIVE_METASTORE_URI"] = metastore_uri
    export_environment_variables(envs)


def get_runtime_logs():
    spark_logs_dir = os.path.join(os.getenv("SPARK_HOME"), "logs")
    jupyter_logs_dir = os.path.join(
        os.getenv("HOME"), "runtime", "jupyter", "logs")
    all_logs = {"spark": spark_logs_dir,
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
            "url": http_address_string(head_host, SPARK_JUPYTER_WEB_PORT),
            "info": "default password is \'cloudtik\'"
        },
        "history-server": {
            "name": "Spark History Server Web UI",
            "url": http_address_string(head_host, SPARK_HISTORY_SERVER_API_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "jupyter-web": {
            "protocol": "TCP",
            "port": SPARK_JUPYTER_WEB_PORT,
        },
        "history-server": {
            "protocol": "TCP",
            "port": SPARK_HISTORY_SERVER_API_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    spark_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(spark_config)
    spark_history_service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, SPARK_HISTORY_SERVER_SERVICE_TYPE)
    services = {
        spark_history_service_name: define_runtime_service_on_head(
            SPARK_HISTORY_SERVER_SERVICE_TYPE,
            service_discovery_config, SPARK_HISTORY_SERVER_API_PORT,
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP),
    }
    return services


def request_rest_applications(
        config: Dict[str, Any], endpoint: str,
        on_head: bool = False):
    if endpoint is None:
        endpoint = "/applications"
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    endpoint = "api/v1" + endpoint
    return _request_rest_to_head(
        config, endpoint, SPARK_HISTORY_SERVER_API_PORT,
        on_head=on_head)


def get_runtime_default_storage(config: Dict[str, Any]):
    return get_runtime_default_storage_of(config, BUILT_IN_RUNTIME_HADOOP)


def get_runtime_endpoints(config: Dict[str, Any]):
    return get_runtime_endpoints_of(config, BUILT_IN_RUNTIME_SPARK)
