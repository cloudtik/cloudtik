import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_STORAGE
from cloudtik.core._private.util.core_utils import http_address_string
from cloudtik.core._private.utils import get_node_cluster_ip_of
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["proc_namenode", False, "NameNode", "head"],
    ["proc_datanode", False, "DataNode", "worker"],
]

HDFS_FORCE_CLEAN_KEY = "force_clean"

HDFS_WEB_PORT = 9870

HDFS_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS
HDFS_SERVICE_PORT = 9000


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HDFS, {})


def register_service(
        cluster_config: Dict[str, Any], head_node_id: str) -> None:
    head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
    head_host = get_cluster_head_host(cluster_config, head_ip)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_HDFS,
        service_addresses=[(head_host, HDFS_SERVICE_PORT)])


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    hdfs_config = _get_config(runtime_config)

    runtime_envs = {
        "HDFS_ENABLED": True,
        "HDFS_SERVICE_PORT": HDFS_SERVICE_PORT
    }

    force_clean = hdfs_config.get(HDFS_FORCE_CLEAN_KEY, False)
    if force_clean:
        runtime_envs["HDFS_FORCE_CLEAN"] = force_clean

    return runtime_envs


def _get_runtime_logs():
    hadoop_logs_dir = os.path.join(os.getenv("HADOOP_HOME"), "logs")
    all_logs = {"hadoop": hadoop_logs_dir}
    return all_logs


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    endpoints = {
        "hdfs-web": {
            "name": "HDFS Web UI",
            "url": http_address_string(head_host, HDFS_WEB_PORT)
        },
        "hdfs": {
            "name": "HDFS Service",
            "url": "hdfs://{}:{}".format(head_host, HDFS_SERVICE_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_ports = {
        "hdfs-web": {
            "protocol": "TCP",
            "port": HDFS_WEB_PORT,
        },
        "hdfs": {
            "protocol": "TCP",
            "port": HDFS_SERVICE_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    # service name is decided by the runtime itself
    # For in services backed by the collection of nodes of the cluster
    # service name is a combination of cluster_name + runtime_service_name
    hdfs_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(hdfs_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HDFS_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_head(
            HDFS_SERVICE_TYPE,
            service_discovery_config, HDFS_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_STORAGE]),
    }
    return services
