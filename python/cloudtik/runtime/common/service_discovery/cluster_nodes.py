import json
import time

from cloudtik.core._private.constants import CLOUDTIK_HEARTBEAT_TIMEOUT_S
from cloudtik.core._private.state.control_state import ControlState
from cloudtik.core._private.state.state_utils import NODE_STATE_HEARTBEAT_TIME


class ClusterNodes:
    """Helper class for cluster nodes to discover other nodes info from state table"""

    def __init__(self,
                 redis_address=None,
                 redis_password=None):
        if not redis_address:
            raise RuntimeError("Redis address is needed for pulling local targets.")

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
