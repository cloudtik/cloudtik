import logging
import os
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_NODE_IP, CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID
from cloudtik.core._private.core_utils import exec_with_output, strip_quote
from cloudtik.core._private.runtime_utils import RUNTIME_NODE_SEQ_ID, RUNTIME_NODE_IP, sort_nodes_by_seq_id, \
    load_and_save_yaml, get_runtime_value
from cloudtik.runtime.etcd.utils import ETCD_PEER_PORT, ETCD_SERVICE_PORT, _get_home_dir

logger = logging.getLogger(__name__)


###################################
# Calls from node when configuring
###################################


def _get_initial_cluster_from_nodes_info(initial_cluster):
    return ",".join(
        ["server{}=http://{}:{}".format(
            node[RUNTIME_NODE_SEQ_ID], node[RUNTIME_NODE_IP], ETCD_PEER_PORT) for node in initial_cluster])


def configure_initial_cluster(nodes_info: Dict[str, Any]):
    if nodes_info is None:
        raise RuntimeError("Missing nodes info for configuring server ensemble.")

    initial_cluster = sort_nodes_by_seq_id(nodes_info)
    initial_cluster_str = _get_initial_cluster_from_nodes_info(initial_cluster)

    _update_initial_cluster_config(initial_cluster_str)


def _update_initial_cluster_config(initial_cluster_str):
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "conf", "etcd.yaml")

    def update_initial_cluster(config_object):
        config_object["initial-cluster"] = initial_cluster_str

    load_and_save_yaml(config_file, update_initial_cluster)


def request_to_join_cluster(nodes_info: Dict[str, Any]):
    if nodes_info is None:
        raise RuntimeError("Missing nodes info for join to the cluster.")

    initial_cluster = sort_nodes_by_seq_id(nodes_info)

    node_ip = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_IP)
    if not node_ip:
        raise RuntimeError("Missing node ip environment variable for this node.")

    # exclude my own address from the initial cluster as endpoints
    endpoints = [node for node in initial_cluster if node[RUNTIME_NODE_IP] != node_ip]
    if not endpoints:
        raise RuntimeError("No exiting nodes found for contacting to join the cluster.")

    seq_id = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID)
    if not seq_id:
        raise RuntimeError("Missing sequence ip environment variable for this node.")

    _request_member_add(endpoints, node_ip, seq_id)


def _get_initial_cluster_from_output(output):
    output_lines = output.split('\n')
    initial_cluster_mark = "ETCD_INITIAL_CLUSTER="
    for output_line in output_lines:
        if output_line.startswith(initial_cluster_mark):
            return strip_quote(output_line[len(initial_cluster_mark):])


def _request_member_add(endpoints, node_ip, seq_id):
    # etcdctl --endpoints=http://existing_node_ip:2379 member add server --peer-urls=http://node_ip:2380
    cmd = ["etcdctl"]
    endpoints_str = ",".join(
        ["http://{}:{}".format(
            node[RUNTIME_NODE_IP], ETCD_SERVICE_PORT) for node in endpoints])
    cmd += ["--endpoints=" + endpoints_str]
    cmd += ["member", "add"]
    node_name = "server{}".format(seq_id)
    cmd += [node_name]
    peer_urls = "--peer-urls=http://{}:{}".format(node_ip, ETCD_PEER_PORT)
    cmd += [peer_urls]

    cmd_str = " ".join(cmd)
    output = exec_with_output(cmd_str).decode().strip()
    initial_cluster_str = _get_initial_cluster_from_output(output)
    if initial_cluster_str:
        # succeed
        _update_initial_cluster_config(initial_cluster_str)
