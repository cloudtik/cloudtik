import logging
import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ZOOKEEPER
from cloudtik.core._private.service_discovery.naming import get_cluster_node_address_type
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_worker, \
    get_service_discovery_config, ServiceRegisterException, SERVICE_DISCOVERY_FEATURE_KEY_VALUE
from cloudtik.core._private.util.runtime_utils import sort_nodes_by_seq_id, get_node_host_from_node_info
from cloudtik.core._private.utils import \
    get_cluster_name
from cloudtik.runtime.common.service_discovery.cluster import register_service_to_cluster
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

logger = logging.getLogger(__name__)


RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["org.apache.zookeeper.server.quorum.QuorumPeerMain", False, "ZooKeeper", "worker"],
]

ZOOKEEPER_SERVICE_TYPE = BUILT_IN_RUNTIME_ZOOKEEPER
ZOOKEEPER_SERVICE_PORT = 2181


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_ZOOKEEPER, {})


def _get_home_dir():
    return os.getenv("ZOOKEEPER_HOME")


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    zookeeper_logs_dir = os.path.join(home_dir, "logs")
    all_logs = {"zookeeper": zookeeper_logs_dir}
    return all_logs


def _bootstrap_runtime_config(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {"ZOOKEEPER_ENABLED": True}
    return runtime_envs


def _get_runtime_endpoints(cluster_config, cluster_head_ip):
    # TODO: future to retrieve the endpoints from service discovery
    return None


def _handle_node_constraints_reached(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any],
        node_type: str, head_info: Dict[str, Any], nodes_info: Dict[str, Any]):
    # We know this is called in the cluster scaler context
    server_ensemble = sort_nodes_by_seq_id(nodes_info)
    address_type = get_cluster_node_address_type(cluster_config)
    endpoints = [(get_node_host_from_node_info(node_info, address_type),
                  ZOOKEEPER_SERVICE_PORT
                  ) for node_info in server_ensemble]

    try:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_ZOOKEEPER,
            service_addresses=endpoints)
    except ServiceRegisterException as e:
        logger.warning(
            "Error happened: {}", str(e))
    register_service_to_cluster(
        BUILT_IN_RUNTIME_ZOOKEEPER,
        service_addresses=endpoints)


def _get_server_config(runtime_config: Dict[str, Any]):
    zookeeper_config = runtime_config.get(BUILT_IN_RUNTIME_ZOOKEEPER)
    if not zookeeper_config:
        return None

    return zookeeper_config.get("config")


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    zookeeper_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(zookeeper_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, ZOOKEEPER_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_worker(
            ZOOKEEPER_SERVICE_TYPE,
            service_discovery_config, ZOOKEEPER_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
    }
    return services
