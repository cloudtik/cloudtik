import logging
import os
import subprocess
from shlex import quote
from typing import Any, Dict, List

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID
from cloudtik.core._private.util.core_utils import get_address_string
from cloudtik.core._private.util.runtime_utils import subscribe_runtime_config, RUNTIME_NODE_SEQ_ID, RUNTIME_NODE_IP, \
    sort_nodes_by_seq_id, get_runtime_node_ip, get_runtime_node_host, get_node_host_from_node_info, \
    get_runtime_node_address_type
from cloudtik.core._private.utils import \
    load_properties_file, save_properties_file
from cloudtik.runtime.zookeeper.utils import _get_home_dir, _get_server_config, ZOOKEEPER_SERVICE_PORT

logger = logging.getLogger(__name__)

ZOOKEEPER_QUORUM_RETRY = 30
ZOOKEEPER_QUORUM_RETRY_INTERVAL = 5


###################################
# Calls from node when configuring
###################################


class NoQuorumError(RuntimeError):
    pass


def _format_server_line(node_host, seq_id):
    # below two lines are equivalent
    # server.id=node_host:2888:3888;2181
    # server.id=node_host:2888:3888:participant;0.0.0.0:2181
    return "server.{}={}:2888:3888;{}".format(
        seq_id, node_host, ZOOKEEPER_SERVICE_PORT)


def update_configurations():
    # Merge user specified configuration and default configuration
    runtime_config = subscribe_runtime_config()
    server_config = _get_server_config(runtime_config)
    if not server_config:
        return

    # Read in the existing configurations
    home_dir = _get_home_dir()
    server_properties_file = os.path.join(home_dir, "conf", "zoo.cfg")
    server_properties, comments = load_properties_file(
        server_properties_file)

    # Merge with the user configurations
    server_properties.update(server_config)

    # Write back the configuration file
    save_properties_file(
        server_properties_file, server_properties, comments=comments)


def configure_server_ensemble(nodes_info: Dict[str, Any]):
    # This method calls from node when configuring
    if nodes_info is None:
        raise RuntimeError(
            "Missing nodes info for configuring server ensemble.")

    server_ensemble = sort_nodes_by_seq_id(nodes_info)
    _write_server_ensemble(server_ensemble)


def _write_server_ensemble(server_ensemble: List[Dict[str, Any]]):
    address_type = get_runtime_node_address_type()
    home_dir = _get_home_dir()
    zoo_cfg_file = os.path.join(home_dir, "conf", "zoo.cfg")

    mode = 'a' if os.path.exists(zoo_cfg_file) else 'w'
    with open(zoo_cfg_file, mode) as f:
        for node_info in server_ensemble:
            server_line = _format_server_line(
                get_node_host_from_node_info(
                    node_info, address_type), node_info[RUNTIME_NODE_SEQ_ID])
            f.write("{}\n".format(server_line))


def request_to_join_cluster(nodes_info: Dict[str, Any]):
    if nodes_info is None:
        raise RuntimeError(
            "Missing nodes info for join to the cluster.")

    initial_cluster = sort_nodes_by_seq_id(nodes_info)
    node_ip = get_runtime_node_ip()
    address_type = get_runtime_node_address_type()

    # exclude my own address from the initial cluster as endpoints
    endpoints = [get_node_host_from_node_info(node_info, address_type)
                 for node_info in initial_cluster if node_info[RUNTIME_NODE_IP] != node_ip]
    if not endpoints:
        raise RuntimeError(
            "No exiting nodes found for contacting to join the cluster.")

    seq_id = os.environ.get(CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID)
    if not seq_id:
        raise RuntimeError(
            "Missing sequence ip environment variable for this node.")

    node_host = get_runtime_node_host()
    _request_member_add(endpoints, node_host, seq_id)


def _request_member_add(endpoints, node_host, seq_id):
    home_dir = _get_home_dir()
    zk_cli = os.path.join(home_dir, "bin", "zkCli.sh")

    server_to_add = _format_server_line(node_host, seq_id)
    # trying each node if failed
    last_error = None
    for endpoint in endpoints:
        try:
            _try_member_add(endpoint, zk_cli, server_to_add)
            # output should contain: Committed new configuration
            # success without exception
            return
        except NoQuorumError as quorum_error:
            raise quorum_error
        except Exception as e:
            # Other error retrying other endpoints
            print(
                "Failed to add member through endpoint: "
                "{}. Retrying with other endpoints...".format(
                    endpoint))
            last_error = e
            continue

    if last_error is not None:
        raise last_error


def _try_member_add(endpoint, zk_cli, server_to_add):
    # zkCli.sh -server existing_server_ip:2181 reconfig -add server.id=node_host:2888:3888;2181
    cmd = ["bash", zk_cli]
    endpoints_str = get_address_string(
        endpoint, ZOOKEEPER_SERVICE_PORT)
    cmd += ["-server", endpoints_str]
    cmd += ["reconfig", "-add"]
    cmd += [quote(server_to_add)]

    cmd_str = " ".join(cmd)
    retries = ZOOKEEPER_QUORUM_RETRY
    env = os.environ.copy()
    env["ZOO_LOG4J_PROP"] = "ERROR,ROLLINGFILE"
    while retries > 0:
        try:
            return subprocess.check_output(
                cmd_str,
                shell=True,
                stderr=subprocess.STDOUT,
                env=env
            )
        except subprocess.CalledProcessError as e:
            retries -= 1
            output = e.output
            if output is not None:
                output_str = output.decode().strip()
                if "No quorum of new config is connected" in output_str:
                    # only retry for waiting for quorum
                    if retries == 0:
                        raise NoQuorumError(
                            "No quorum of new config is connected")
                    print(
                        "No quorum of new config is connected. "
                        "Waiting {} seconds and retrying...".format(
                            ZOOKEEPER_QUORUM_RETRY_INTERVAL))
                    continue
            raise e
