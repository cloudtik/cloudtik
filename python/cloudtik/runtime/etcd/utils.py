import logging
import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ETCD
from cloudtik.core._private.service_discovery.naming import get_cluster_node_address_type
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service_on_worker, \
    get_service_discovery_config, ServiceRegisterException, SERVICE_DISCOVERY_FEATURE_KEY_VALUE
from cloudtik.core._private.util.runtime_utils import sort_nodes_by_seq_id, get_node_host_from_node_info
from cloudtik.core._private.utils import get_cluster_name
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

logger = logging.getLogger(__name__)

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["etcd", True, "etcd", "worker"],
]

ETCD_SERVICE_TYPE = BUILT_IN_RUNTIME_ETCD
ETCD_SERVICE_PORT = 2379
ETCD_PEER_PORT = 2380


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_ETCD, {})


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_ETCD)


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_ETCD: logs_dir}


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _bootstrap_runtime_config(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {
        "ETCD_CLUSTER_NAME": config["cluster_name"],
        "ETCD_SERVICE_PORT": ETCD_SERVICE_PORT
    }
    return runtime_envs


def _get_endpoints(nodes, address_type):
    return [(get_node_host_from_node_info(node_info, address_type),
             ETCD_PEER_PORT) for node_info in nodes]


def _handle_node_constraints_reached(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any],
        node_type: str, head_info: Dict[str, Any], nodes_info: Dict[str, Any]):
    # We know this is called in the cluster scaler context
    initial_cluster = sort_nodes_by_seq_id(nodes_info)
    address_type = get_cluster_node_address_type(cluster_config)
    endpoints = _get_endpoints(initial_cluster, address_type)
    try:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_ETCD,
            service_addresses=endpoints)
    except ServiceRegisterException as e:
        logger.warning("Error happened: {}", str(e))


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    # TODO: future to retrieve the endpoints from service discovery
    return None


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    etcd_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(etcd_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, ETCD_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_worker(
            ETCD_SERVICE_TYPE,
            service_discovery_config, ETCD_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE]),
    }
    return services
