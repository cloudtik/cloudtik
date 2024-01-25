import json
import os
import time
from typing import Dict, Any

import yaml

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_NODE_TYPE, CLOUDTIK_RUNTIME_ENV_NODE_IP, \
    CLOUDTIK_RUNTIME_ENV_SECRETS, CLOUDTIK_RUNTIME_ENV_HEAD_IP, env_bool, CLOUDTIK_DATA_DISK_MOUNT_POINT, \
    CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX, CLOUDTIK_DEFAULT_PORT, CLOUDTIK_REDIS_DEFAULT_PASSWORD, \
    CLOUDTIK_RUNTIME_ENV_HEAD_HOST, CLOUDTIK_RUNTIME_ENV_NODE_HOST, CLOUDTIK_RUNTIME_ENV_WORKSPACE, \
    CLOUDTIK_RUNTIME_ENV_CLUSTER, CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID
from cloudtik.core._private.crypto import AESCipher
from cloudtik.core._private.service_discovery.naming import _get_cluster_node_fqdn_of, _get_cluster_node_sqdn_of, \
    get_address_type_of_hostname, _get_worker_node_hosts
from cloudtik.core._private.service_discovery.utils import ServiceAddressType
from cloudtik.core._private.util.redis_utils import create_redis_client, get_address_to_use_or_die
from cloudtik.core._private.state.state_utils import NODE_STATE_NODE_IP, NODE_STATE_NODE_SEQ_ID
from cloudtik.core._private.utils import load_head_cluster_config, _get_node_type_specific_runtime_config, \
    get_runtime_config_key, decode_cluster_secrets, CLOUDTIK_CLUSTER_NODES_INFO_NODE_TYPE, \
    _get_workers_ready, _get_worker_node_ips, CLOUDTIK_CLUSTER_VARIABLE, get_node_provider_of, get_head_node_type
from cloudtik.core.tags import STATUS_UP_TO_DATE

RUNTIME_NODE_ID = "node_id"
RUNTIME_NODE_IP = "node_ip"
RUNTIME_NODE_SEQ_ID = "node_seq_id"
RUNTIME_NODE_STATUS = "node_status"
RUNTIME_NODE_QUORUM_ID = "quorum_id"
RUNTIME_NODE_QUORUM_JOIN = "quorum_join"

DEFAULT_RETRY_NUM = 10
DEFAULT_RETRY_INTERVAL = 1


def get_runtime_value(name):
    return os.environ.get(name)


def get_runtime_bool(name, default=False):
    return env_bool(name, default)


def get_runtime_value_checked(name):
    value = get_runtime_value(name)
    if not value:
        raise RuntimeError(
            "Environment variable {} is not set.".format(name))
    return value


def get_runtime_node_type():
    # Node ip should always be set as env
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_NODE_TYPE)


def get_runtime_node_ip():
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_NODE_IP)


def get_runtime_head_ip(head=False):
    # worker node should always get head ip set
    head_ip = \
        get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_IP)
    if head_ip:
        return head_ip

    if head:
        return get_runtime_node_ip()
    else:
        raise RuntimeError(
            "Environment variable {} is not set.".format(
                CLOUDTIK_RUNTIME_ENV_HEAD_IP))


def get_runtime_node_host():
    # Node host should always be set as env
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_NODE_HOST)


def get_runtime_head_host(head=False):
    # We should always get head host set
    head_host = \
        get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_HOST)
    if head_host:
        return head_host
    if head:
        return get_runtime_node_host()
    else:
        raise RuntimeError(
            "Environment variable {} is not set.".format(
                CLOUDTIK_RUNTIME_ENV_HEAD_HOST))


def get_runtime_node_seq_id():
    # Error if node seq id is not set
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID)


def get_runtime_cluster_name():
    # cluster name should always be set as env
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_CLUSTER)


def get_runtime_workspace_name():
    # cluster name should always be set as env
    return get_runtime_value_checked(CLOUDTIK_RUNTIME_ENV_WORKSPACE)


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
    return _subscribe_runtime_config(node_type)


def _subscribe_runtime_config(node_type):
    if node_type:
        # Try getting node type specific runtime config
        runtime_config = retrieve_runtime_config(node_type)
        if runtime_config is not None:
            return runtime_config
    return subscribe_cluster_runtime_config()


def subscribe_cluster_runtime_config():
    return retrieve_runtime_config()


def get_runtime_config_from_node(head):
    if head:
        config = load_head_cluster_config()
        node_type = get_head_node_type(config)
        return _get_node_type_specific_runtime_config(config, node_type)
    else:
        # from worker node, subscribe from head redis
        return subscribe_runtime_config()


def get_runtime_config_of_node_type(node_type, head=False):
    if head:
        config = load_head_cluster_config()
        return _get_node_type_specific_runtime_config(
            config, node_type)
    else:
        # Try getting node type specific runtime config
        return _subscribe_runtime_config(node_type)


def subscribe_nodes_info():
    node_type = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_TYPE)
    if not node_type:
        raise RuntimeError(
            "Not able to subscribe nodes info in lack of node type.")
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
            raise RuntimeError(
                "Missing node ip for node {}.".format(node_id))
        if RUNTIME_NODE_SEQ_ID not in node_info:
            raise RuntimeError(
                "Missing node sequence id for node {}.".format(node_id))
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
    provider = get_node_provider_of(config)
    return _get_workers_ready(config, provider)


def get_worker_ips_ready_from_head(runtime=None):
    config = load_head_cluster_config()
    return _get_worker_node_ips(
        config, runtime=runtime, node_status=STATUS_UP_TO_DATE)


def get_worker_hosts_ready_from_head(runtime=None):
    config = load_head_cluster_config()
    return _get_worker_node_hosts(
        config, runtime=runtime, node_status=STATUS_UP_TO_DATE)


def run_func_with_retry(
        func, num_retries=DEFAULT_RETRY_NUM,
        retry_interval=DEFAULT_RETRY_INTERVAL):
    for i in range(num_retries):
        try:
            func()
            return
        except Exception as e:
            if i >= num_retries - 1:
                raise RuntimeError(
                    "Function failed with {} reties.".format(num_retries)) from e

            # error retry
            time.sleep(retry_interval)


def publish_cluster_variable(cluster_variable_name, cluster_variable_value):
    cluster_variable_key = CLOUDTIK_CLUSTER_VARIABLE.format(cluster_variable_name)
    return _put_key_to_kv(cluster_variable_key, cluster_variable_value)


def subscribe_cluster_variable(cluster_variable_name):
    cluster_variable_key = CLOUDTIK_CLUSTER_VARIABLE.format(cluster_variable_name)
    cluster_variable_value = _get_key_from_kv(cluster_variable_key)
    if cluster_variable_value is None:
        return None
    return cluster_variable_value.decode("utf-8")


def get_cluster_redis_address():
    # TODO: DNS naming service may not available at configuration stage.
    try:
        redis_host = get_runtime_head_ip()
        redis_address = "{}:{}".format(redis_host, CLOUDTIK_DEFAULT_PORT)
    except Exception:
        # if there is no head ip in environment, try to find one
        redis_address = get_address_to_use_or_die()

    redis_password = CLOUDTIK_REDIS_DEFAULT_PASSWORD
    return redis_address, redis_password


def get_redis_client(redis_address=None, redis_password=None):
    if not redis_address:
        redis_address, redis_password = get_cluster_redis_address()
    return create_redis_client(
        redis_address, redis_password)


def _get_key_from_kv(key):
    from cloudtik.core._private.state.kv_store import \
        kv_get, kv_initialized, kv_initialize_with_address
    if not kv_initialized():
        redis_address, redis_password = get_cluster_redis_address()
        kv_initialize_with_address(redis_address, redis_password)

    return kv_get(key)


def _put_key_to_kv(key, value):
    from cloudtik.core._private.state.kv_store import \
        kv_put, kv_initialized, kv_initialize_with_address
    if not kv_initialized():
        redis_address, redis_password = get_cluster_redis_address()
        kv_initialize_with_address(redis_address, redis_password)

    return kv_put(key, value)


def get_runtime_node_address_type():
    node_ip = get_runtime_node_ip()
    node_host = get_runtime_node_host()
    if node_ip == node_host:
        return ServiceAddressType.NODE_IP
    else:
        return get_address_type_of_hostname(node_host)


def get_node_host_from(
        node, address_type, get_node_seq_id, get_node_ip,
        workspace_name=None, cluster_name=None):
    if (address_type == ServiceAddressType.NODE_FQDN
            or address_type == ServiceAddressType.NODE_SQDN):
        if not cluster_name:
            cluster_name = get_runtime_cluster_name()
        node_seq_id = get_node_seq_id(node)
        if not node_seq_id:
            raise RuntimeError(
                "Node seq id is not available in node data.")
        if address_type == ServiceAddressType.NODE_FQDN:
            if not workspace_name:
                workspace_name = get_runtime_workspace_name()
            return _get_cluster_node_fqdn_of(
                workspace_name, cluster_name, node_seq_id)
        else:
            return _get_cluster_node_sqdn_of(
                cluster_name, node_seq_id)
    else:
        return get_node_ip(node)


def get_node_host_from_node_info(
        node_info, address_type,
        workspace_name=None, cluster_name=None):
    def get_node_seq_id(node):
        return node.get(RUNTIME_NODE_SEQ_ID)

    def get_node_ip(node):
        return node[RUNTIME_NODE_IP]

    return get_node_host_from(
        node_info, address_type,
        get_node_seq_id, get_node_ip,
        workspace_name=workspace_name, cluster_name=cluster_name)


def get_node_host_from_node_state(
        node_state, address_type,
        workspace_name=None, cluster_name=None):
    def get_node_seq_id(node):
        return node.get(NODE_STATE_NODE_SEQ_ID)

    def get_node_ip(node):
        return node[NODE_STATE_NODE_IP]

    return get_node_host_from(
        node_state, address_type,
        get_node_seq_id, get_node_ip,
        workspace_name=workspace_name, cluster_name=cluster_name)
