import json
import os
import time
from typing import Dict, Any

import yaml

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_NODE_TYPE, CLOUDTIK_RUNTIME_ENV_NODE_IP, \
    CLOUDTIK_RUNTIME_ENV_SECRETS, CLOUDTIK_RUNTIME_ENV_HEAD_IP, env_bool, CLOUDTIK_DATA_DISK_MOUNT_POINT, \
    CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX
from cloudtik.core._private.crypto import AESCipher
from cloudtik.core._private.provider_factory import _get_node_provider
from cloudtik.core._private.utils import load_head_cluster_config, _get_node_type_specific_runtime_config, \
    get_runtime_config_key, _get_key_from_kv, decode_cluster_secrets, CLOUDTIK_CLUSTER_NODES_INFO_NODE_TYPE, \
    _get_workers_ready, _get_worker_node_ips
from cloudtik.core.tags import STATUS_UP_TO_DATE

RUNTIME_NODE_ID = "node_id"
RUNTIME_NODE_IP = "node_ip"
RUNTIME_NODE_SEQ_ID = "node_seq_id"
RUNTIME_NODE_QUORUM_ID = "quorum_id"
RUNTIME_NODE_QUORUM_JOIN = "quorum_join"

DEFAULT_RETRY_NUM = 10
DEFAULT_RETRY_INTERVAL = 1


def get_runtime_value(name):
    return os.environ.get(name)


def get_runtime_bool(name, default=False):
    return env_bool(name, default)


def get_runtime_node_type():
    # Node type should always be set as env
    node_type = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_TYPE)
    if not node_type:
        raise RuntimeError(
            "Environment variable {} is not set.".format(CLOUDTIK_RUNTIME_ENV_NODE_TYPE))

    return node_type


def get_runtime_node_ip():
    # Node type should always be set as env
    node_ip = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_IP)
    if not node_ip:
        raise RuntimeError(
            "Environment variable {} is not set.".format(
                CLOUDTIK_RUNTIME_ENV_NODE_IP))
    return node_ip


def get_runtime_head_ip(head):
    if head:
        return get_runtime_node_ip()
    else:
        # worker node should always get head ip set
        head_ip = \
            get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_IP)
        if not head_ip:
            raise RuntimeError(
                "Environment variable {} is not set.".format(
                    CLOUDTIK_RUNTIME_ENV_HEAD_IP))
        return head_ip


def retrieve_runtime_config(node_type: str = None):
    # Retrieve the runtime config
    runtime_config_key = get_runtime_config_key(node_type)
    runtime_config_data = _get_key_from_kv(runtime_config_key)
    if runtime_config_data is None:
        return None

    encoded_secrets = get_runtime_value(CLOUDTIK_RUNTIME_ENV_SECRETS)
    if encoded_secrets:
        # Decrypt
        secrets = decode_cluster_secrets(encoded_secrets)
        cipher = AESCipher(secrets)
        runtime_config_str = cipher.decrypt(runtime_config_data)
    else:
        runtime_config_str = runtime_config_data.decode("utf-8")

    # To json object
    if runtime_config_str == "":
        return {}

    return json.loads(runtime_config_str)


def subscribe_runtime_config():
    node_type = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_TYPE)
    if node_type:
        # Try getting node type specific runtime config
        runtime_config = retrieve_runtime_config(node_type)
        if runtime_config is not None:
            return runtime_config

    return retrieve_runtime_config()


def get_runtime_config_from_node(head):
    if head:
        config = load_head_cluster_config()
        node_type = config["head_node_type"]
        return _get_node_type_specific_runtime_config(config, node_type)
    else:
        # from worker node, subscribe from head redis
        return subscribe_runtime_config()


def subscribe_nodes_info():
    if CLOUDTIK_RUNTIME_ENV_NODE_TYPE not in os.environ:
        raise RuntimeError("Not able to subscribe nodes info in lack of node type.")
    node_type = os.environ[CLOUDTIK_RUNTIME_ENV_NODE_TYPE]
    return _retrieve_nodes_info(node_type)


def _retrieve_nodes_info(node_type):
    nodes_info_key = CLOUDTIK_CLUSTER_NODES_INFO_NODE_TYPE.format(node_type)
    nodes_info_str = _get_key_from_kv(nodes_info_key)
    if nodes_info_str is None:
        return None

    return json.loads(nodes_info_str)


def sort_nodes_by_seq_id(nodes_info: Dict[str, Any]):
    sorted_nodes_info = []
    for node_id, node_info in nodes_info.items():
        if RUNTIME_NODE_IP not in node_info:
            raise RuntimeError("Missing node ip for node {}.".format(node_id))
        if RUNTIME_NODE_SEQ_ID not in node_info:
            raise RuntimeError("Missing node sequence id for node {}.".format(node_id))
        sorted_nodes_info += [node_info]

    def node_info_sort(node_info):
        return node_info[RUNTIME_NODE_SEQ_ID]

    sorted_nodes_info.sort(key=node_info_sort)
    return sorted_nodes_info


def load_and_save_json(config_file, update_func):
    # load and save json
    with open(config_file) as f:
        config_object = json.load(f)

    update_func(config_object)
    save_json(config_file, config_object)


def save_json(config_file, config_object):
    with open(config_file, "w") as f:
        f.write(json.dumps(config_object, indent=4))


def load_and_save_yaml(config_file, update_func):
    # load and save yaml
    with open(config_file) as f:
        config_object = yaml.safe_load(f)

    update_func(config_object)
    save_yaml(config_file, config_object)


def save_yaml(config_file, config_object):
    with open(config_file, "w") as f:
        yaml.dump(config_object, f, default_flow_style=False)


def get_data_disk_dirs():
    data_disk_dirs = []
    if not os.path.isdir(CLOUDTIK_DATA_DISK_MOUNT_POINT):
        return data_disk_dirs

    for name in os.listdir(CLOUDTIK_DATA_DISK_MOUNT_POINT):
        if not name.startswith(CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX):
            continue
        data_disk_dir = os.path.join(CLOUDTIK_DATA_DISK_MOUNT_POINT, name)
        if not os.path.isdir(data_disk_dir):
            continue
        data_disk_dirs.append(data_disk_dir)
    # Return sorted to make sure the dirs are in order
    return sorted(data_disk_dirs)


def get_first_data_disk_dir():
    data_disk_dirs = get_data_disk_dirs()
    if not data_disk_dirs:
        return None
    return data_disk_dirs[0]


def get_workers_ready_from_head():
    config = load_head_cluster_config()
    provider = _get_node_provider(config["provider"], config["cluster_name"])
    return _get_workers_ready(config, provider)


def get_worker_ips_ready_from_head(runtime=None):
    config = load_head_cluster_config()
    return _get_worker_node_ips(
        config, runtime=runtime, node_status=STATUS_UP_TO_DATE)


def run_func_with_retry(
        func, num_retries=DEFAULT_RETRY_NUM,
        retry_interval=DEFAULT_RETRY_INTERVAL):
    for i in range(num_retries):
        try:
            func()
            return
        except Exception:
            if i >= num_retries - 1:
                break
            # error retry
            time.sleep(retry_interval)
    raise RuntimeError("Function failed with {} reties.".format(num_retries))
