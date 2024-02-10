import os
from typing import Dict, Any

from cloudtik.core._private.util.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.util.runtime_utils import save_yaml, get_node_host_from_node_state
from cloudtik.core._private.service_discovery.utils import ServiceAddressType
from cloudtik.core._private.state.state_utils import NODE_STATE_NODE_TYPE
from cloudtik.core._private.util.service.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.cluster_nodes import ClusterNodes
from cloudtik.runtime.prometheus.utils import _get_home_dir


def _parse_services(service_list_str) -> Dict[str, Any]:
    pull_services = {}
    if not service_list_str:
        return pull_services

    service_list = [x.strip() for x in service_list_str.split(",")]
    for service_str in service_list:
        service_parts = [x.strip() for x in service_str.split(":")]
        if len(service_parts) < 3:
            raise ValueError(
                "Invalid service specification. "
                "Format: service_name:service_port:node_type_1,...")
        service_name = service_parts[0]
        service_port = int(service_parts[1])
        service_node_types = service_parts[2:]
        pull_services[service_name] = (service_port, service_node_types)
    return pull_services


def _get_service_targets(
        service_name, service_port,
        service_node_types, live_nodes_by_node_type):
    nodes_of_service = _get_targets_of_node_types(
        live_nodes_by_node_type, service_node_types)
    if not nodes_of_service:
        return None

    targets = [get_address_string(
        node, service_port) for node in nodes_of_service]
    service_targets = {
        "labels": {
            "service": service_name
        },
        "targets": targets
    }
    return service_targets


def _get_targets_of_node_types(live_nodes_by_node_type, node_types):
    if len(node_types) == 1:
        return live_nodes_by_node_type.get(node_types[0])
    else:
        # more than one node types
        nodes = []
        for node_type in node_types:
            nodes += live_nodes_by_node_type.get(node_type, [])
        return nodes


class DiscoverLocalTargets(PullJob):
    """Pulling job for discovering local cluster nodes if service discovery is not available"""

    def __init__(
            self,
            interval=None,
            services=None,
            redis_address=None,
            redis_password=None,
            workspace_name=None,
            cluster_name=None,
            address_type=None):
        super().__init__(interval)
        self.pull_services = _parse_services(services)
        self.cluster_nodes = ClusterNodes(redis_address, redis_password)
        self.workspace_name = workspace_name
        self.cluster_name = cluster_name
        if address_type:
            address_type = ServiceAddressType.from_str(address_type)
        else:
            address_type = ServiceAddressType.NODE_IP
        self.address_type = address_type
        home_dir = _get_home_dir()
        self.config_file = os.path.join(home_dir, "conf", "local-targets.yaml")
        self.last_local_targets_hash = None

    def pull(self):
        live_nodes_by_node_type = self._get_live_nodes()

        local_targets = []
        for service_name, pull_service in self.pull_services.items():
            service_port, service_node_types = pull_service
            service_targets = _get_service_targets(
                service_name, service_port,
                service_node_types, live_nodes_by_node_type)
            if service_targets:
                local_targets.append(service_targets)

        local_targets_hash = get_json_object_hash(local_targets)
        if local_targets_hash != self.last_local_targets_hash:
            # save file only when data changed
            save_yaml(self.config_file, local_targets)
            self.last_local_targets_hash = local_targets_hash

    def _get_live_nodes(self):
        live_nodes_by_node_type = {}
        live_nodes = self.cluster_nodes.get_live_nodes()
        for node in live_nodes:
            node_type = node[NODE_STATE_NODE_TYPE]
            if node_type not in live_nodes_by_node_type:
                live_nodes_by_node_type[node_type] = []
            nodes_of_node_type = live_nodes_by_node_type[node_type]
            # We need only IP or hostname
            node_host = get_node_host_from_node_state(
                node, self.address_type,
                workspace_name=self.workspace_name,
                cluster_name=self.cluster_name)
            nodes_of_node_type.append(node_host)
        return live_nodes_by_node_type
