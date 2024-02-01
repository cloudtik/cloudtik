import os
import time
from typing import Any, Dict, Optional

from cloudtik.core._private.cli_logger import cli_logger
from cloudtik.core._private.cluster.cluster_tunnel_request import _request_rest_to_head
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_YARN
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_SCHEDULER
from cloudtik.core._private.util.core_utils import http_address_string, address_string
from cloudtik.core._private.utils import \
    round_memory_size_to_gb, RUNTIME_CONFIG_KEY, get_config_for_update, get_node_type_resources, get_cluster_name
from cloudtik.core.scaling_policy import ScalingPolicy
from cloudtik.runtime.common.utils import get_runtime_endpoints_of
from cloudtik.runtime.yarn.scaling_policy import YARNScalingPolicy

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["proc_resourcemanager", False, "ResourceManager", "head"],
    ["proc_nodemanager", False, "NodeManager", "worker"],
]

YARN_RESOURCE_MEMORY_RATIO = 0.8

YARN_RESOURCE_MANAGER_PORT = 8032
YARN_WEB_API_PORT = 8088

YARN_REQUEST_REST_RETRY_DELAY_S = 5
YARN_REQUEST_REST_RETRY_COUNT = 36

YARN_SERVICE_TYPE = BUILT_IN_RUNTIME_YARN


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_YARN, {})


def get_yarn_resource_memory_ratio(cluster_config: Dict[str, Any]):
    yarn_resource_memory_ratio = YARN_RESOURCE_MEMORY_RATIO
    yarn_config = cluster_config.get(
        RUNTIME_CONFIG_KEY, {}).get(BUILT_IN_RUNTIME_YARN, {})
    memory_ratio = yarn_config.get("yarn_resource_memory_ratio")
    if memory_ratio:
        yarn_resource_memory_ratio = memory_ratio
    return yarn_resource_memory_ratio


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = _configure_runtime_resources(cluster_config)
    return cluster_config


def _configure_runtime_resources(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_resource = get_node_type_resources(cluster_config)
    worker_cpu = cluster_resource["worker_cpu"]

    container_resource = {"yarn_container_maximum_vcores": worker_cpu}

    yarn_resource_memory_ratio = get_yarn_resource_memory_ratio(cluster_config)
    worker_memory_for_yarn = round_memory_size_to_gb(
        int(cluster_resource["worker_memory"] * yarn_resource_memory_ratio))
    container_resource["yarn_container_maximum_memory"] = worker_memory_for_yarn

    runtime_config = get_config_for_update(cluster_config, RUNTIME_CONFIG_KEY)
    yarn_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_YARN)

    yarn_config["yarn_container_resource"] = container_resource
    return cluster_config


def get_runtime_processes():
    return RUNTIME_PROCESSES


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {}
    yarn_config = _get_config(runtime_config)

    # export yarn memory ratio to use if configured by user
    yarn_resource_memory_ratio = yarn_config.get("yarn_resource_memory_ratio")
    if yarn_resource_memory_ratio:
        runtime_envs["YARN_RESOURCE_MEMORY_RATIO"] = yarn_resource_memory_ratio

    # export yarn scheduler
    yarn_scheduler = yarn_config.get("yarn_scheduler")
    if yarn_scheduler:
        runtime_envs["YARN_SCHEDULER"] = yarn_scheduler

    return runtime_envs


def get_runtime_logs():
    hadoop_logs_dir = os.path.join(os.getenv("HADOOP_HOME"), "logs")
    all_logs = {"hadoop": hadoop_logs_dir}
    return all_logs


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    endpoints = {
        "yarn": {
            "name": "Yarn",
            "url": address_string(head_host, YARN_RESOURCE_MANAGER_PORT)
        },
        "yarn-web": {
            "name": "Yarn Web UI",
            "url": http_address_string(head_host, YARN_WEB_API_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "yarn": {
            "protocol": "TCP",
            "port": YARN_RESOURCE_MANAGER_PORT,
        },
        "yarn-web": {
            "protocol": "TCP",
            "port": YARN_WEB_API_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    yarn_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(yarn_config)
    yarn_service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, YARN_SERVICE_TYPE)
    services = {
        yarn_service_name: define_runtime_service_on_head(
            YARN_SERVICE_TYPE,
            service_discovery_config, YARN_RESOURCE_MANAGER_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_SCHEDULER]),
    }
    return services


def _get_scaling_policy(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        head_host: str) -> Optional[ScalingPolicy]:
    yarn_config = _get_config(runtime_config)
    if "scaling" not in yarn_config:
        return None

    return YARNScalingPolicy(
        cluster_config, head_host,
        rest_port=YARN_WEB_API_PORT)


def request_rest_yarn(
        config: Dict[str, Any], endpoint: Optional[str],
        on_head: bool = False):
    if endpoint is None:
        endpoint = "/cluster/metrics"
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    endpoint = "ws/v1" + endpoint
    return _request_rest_to_head(
        config, endpoint, YARN_WEB_API_PORT,
        on_head=on_head)


def request_rest_yarn_with_retry(
        config: Dict[str, Any], endpoint: Optional[str],
        retry=YARN_REQUEST_REST_RETRY_COUNT):
    while retry > 0:
        try:
            response = request_rest_yarn(config, endpoint)
            return response
        except Exception as e:
            retry = retry - 1
            if retry > 0:
                cli_logger.warning(
                    f"Error when requesting yarn api. Retrying in {YARN_REQUEST_REST_RETRY_DELAY_S} seconds.")
                time.sleep(YARN_REQUEST_REST_RETRY_DELAY_S)
            else:
                cli_logger.error(
                    "Failed to request yarn api: {}", str(e))
                raise e


def get_runtime_endpoints(config: Dict[str, Any]):
    return get_runtime_endpoints_of(config, BUILT_IN_RUNTIME_YARN)
