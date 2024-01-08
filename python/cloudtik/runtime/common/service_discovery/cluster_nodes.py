import json
import sys
import time

import ipaddr

from cloudtik.core._private.constants import CLOUDTIK_HEARTBEAT_TIMEOUT_S
from cloudtik.core._private.service_discovery.utils import ServiceAddressType
from cloudtik.core._private.state.control_state import ControlState
from cloudtik.core._private.state.state_utils import NODE_STATE_HEARTBEAT_TIME, NODE_STATE_NODE_TYPE, \
    NODE_STATE_NODE_SEQ_ID, NODE_STATE_NODE_IP, NODE_STATE_NODE_KIND, NODE_STATE_NODE_ID
from cloudtik.core._private.util.runtime_utils import get_cluster_redis_address, get_runtime_config_of_node_type, \
    get_node_host_from_node_state, get_runtime_workspace_name, get_runtime_cluster_name, get_runtime_node_address_type
from cloudtik.core._private.utils import is_runtime_enabled


class ClusterNodes:
    """Helper class for cluster nodes to discover other nodes info from state table"""

    def __init__(
            self,
            redis_address=None,
            redis_password=None):
        if not redis_address:
            raise RuntimeError(
                "Redis address is needed for pulling local targets.")

        (redis_host, redis_port) = redis_address.split(":")

        self.redis_address = redis_address
        self.redis_password = redis_password

        self.control_state = ControlState()
        self.control_state.initialize_control_state(
            redis_host, redis_port, redis_password)
        self.node_table = self.control_state.get_node_table()

    def get_live_nodes(self):
        live_nodes = []
        now = time.time()
        nodes_state_as_json = self.node_table.get_all().values()
        for node_state_as_json in nodes_state_as_json:
            node_state = json.loads(node_state_as_json)
            # Filter out the stale record in the node table
            delta = now - node_state.get(NODE_STATE_HEARTBEAT_TIME, 0)
            if delta < CLOUDTIK_HEARTBEAT_TIMEOUT_S:
                live_nodes.append(node_state)
        return live_nodes


def _filter_match(
        node_info, filter_node_type: str = None,
        filter_runtime_type: str = None):
    if filter_node_type:
        node_type = node_info[NODE_STATE_NODE_TYPE]
        if filter_node_type != node_type:
            return False
    if filter_runtime_type:
        # always getting the runtime config from state service
        node_type = node_info[NODE_STATE_NODE_TYPE]
        runtime_config = get_runtime_config_of_node_type(node_type)
        if runtime_config is not None:
            if not is_runtime_enabled(
                    runtime_config, filter_runtime_type):
                return False
    return True


def get_cluster_live_nodes(
        node_type: str = None,
        runtime_type: str = None,
        redis_address=None, redis_password=None):
    if not redis_address:
        redis_address, redis_password = get_cluster_redis_address()
    cluster_nodes = ClusterNodes(redis_address, redis_password)

    matched_nodes = []
    live_nodes = cluster_nodes.get_live_nodes()
    for node in live_nodes:
        if not _filter_match(
                node, node_type, runtime_type):
            continue
        matched_nodes.append(node)
    return matched_nodes


def _sort_nodes(
        nodes, sort_by: str = None, reverse: bool = False):
    def sort_by_int(node):
        # to the last for those who don't have the field
        return node.get(sort_by, sys.maxsize)

    def sort_by_str(node):
        return node.get(sort_by)

    def sort_by_ip(node):
        node_ip = node.get(NODE_STATE_NODE_IP)
        node_ip_addr = int(
            ipaddr.IPAddress(node_ip)) if node_ip else sys.maxsize
        return node_ip_addr

    def sort_by_default(node):
        return [node.get(NODE_STATE_NODE_KIND), sort_by_ip(node)]

    if (sort_by == NODE_STATE_NODE_SEQ_ID
            or sort_by == NODE_STATE_HEARTBEAT_TIME):
        sort_fn = sort_by_int
    elif (sort_by == NODE_STATE_NODE_ID
          or sort_by == NODE_STATE_NODE_KIND
          or sort_by == NODE_STATE_NODE_TYPE):
        sort_fn = sort_by_str
    elif sort_by == NODE_STATE_NODE_IP:
        sort_fn = sort_by_ip
    else:
        sort_fn = sort_by_default

    nodes.sort(key=sort_fn, reverse=reverse)
    return nodes


def get_cluster_live_nodes_address(
        node_type: str = None,
        runtime_type: str = None,
        host: bool = False,
        sort_by: str = None,
        reverse: bool = False,
        redis_address=None, redis_password=None):
    live_nodes = get_cluster_live_nodes(
        node_type=node_type, runtime_type=runtime_type,
        redis_address=redis_address, redis_password=redis_password)

    # sort by ip by default
    live_nodes = _sort_nodes(
        live_nodes, sort_by=sort_by, reverse=reverse)

    # Warning: this method must call under the context of runtime
    # environment variables
    workspace_name = get_runtime_workspace_name()
    cluster_name = get_runtime_cluster_name()
    address_type = ServiceAddressType.NODE_IP
    if host:
        # Which address type is available/used at runtime
        address_type = get_runtime_node_address_type()

    node_addresses = []
    for node in live_nodes:
        # We need only IP or hostname
        node_address = get_node_host_from_node_state(
            node, address_type,
            workspace_name=workspace_name,
            cluster_name=cluster_name)
        node_addresses.append(node_address)
    return node_addresses
