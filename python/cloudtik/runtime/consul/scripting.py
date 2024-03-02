import json
import logging
import os

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_QUORUM_JOIN, \
    CLOUDTIK_RUNTIME_ENV_HEAD_IP, CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID, CLOUDTIK_RUNTIME_ENV_NODE_ID
from cloudtik.core._private.util.core_utils import get_config_for_update, is_valid_dns_name, address_string
from cloudtik.core._private.util.runtime_utils import get_runtime_node_type, get_runtime_node_ip, \
    get_runtime_config_from_node, RUNTIME_NODE_IP, subscribe_nodes_info, sort_nodes_by_seq_id, \
    load_and_save_json, get_runtime_value, get_runtime_cluster_name
from cloudtik.core._private.service_discovery.utils import SERVICE_DISCOVERY_PORT, \
    SERVICE_DISCOVERY_TAGS, SERVICE_DISCOVERY_LABELS, SERVICE_DISCOVERY_CHECK_INTERVAL, \
    SERVICE_DISCOVERY_CHECK_TIMEOUT, SERVICE_DISCOVERY_LABEL_CLUSTER, SERVICE_DISCOVERY_LABEL_SEQ_ID, \
    SERVICE_DISCOVERY_LABEL_NODE_ID
from cloudtik.core._private.service_discovery.naming import get_cluster_node_name
from cloudtik.core.tags import QUORUM_JOIN_STATUS_INIT
from cloudtik.runtime.consul.utils import _get_home_dir, _is_disable_cluster_node_name, _get_config, \
    _get_services_of_node_type

logger = logging.getLogger(__name__)

SERVICE_CHECK_INTERVAL_DEFAULT = 10
SERVICE_CHECK_TIMEOUT_DEFAULT = 5


###################################
# Calls from node when configuring
###################################


def configure_consul(head):
    runtime_config = get_runtime_config_from_node(head)

    # configure join
    configure_agent(runtime_config, head)

    # Configure the Consul services
    configure_services(runtime_config)


def configure_agent(runtime_config, head):
    consul_server = get_runtime_value("CONSUL_SERVER")
    server_mode = True if consul_server == "true" else False
    _configure_agent(runtime_config, server_mode, head)

    if server_mode:
        quorum_join = get_runtime_value(CLOUDTIK_RUNTIME_ENV_QUORUM_JOIN)
        if quorum_join == QUORUM_JOIN_STATUS_INIT:
            _update_server_config_for_join()


def _configure_agent(runtime_config, server_mode, head):
    consul_config = _get_config(runtime_config)
    # Configure the retry join list for all the cases

    if server_mode:
        # join list for servers
        if head:
            # for head, use its own address
            node_ip = get_runtime_node_ip()
            join_list = [node_ip]
        else:
            # getting from the quorum nodes info
            join_list = _get_join_list_from_nodes_info()
    else:
        # client mode, get from the CONSUL_JOIN_LIST environments
        join_list_str = get_runtime_value("CONSUL_JOIN_LIST")
        if not join_list_str:
            raise RuntimeError("Missing join list environment variable for the running node.")
        join_list = join_list_str.split(',')

    cluster_name = get_runtime_cluster_name()
    _update_agent_config(consul_config, join_list, cluster_name)


def _get_join_list_from_nodes_info():
    nodes_info = subscribe_nodes_info()
    join_nodes = sort_nodes_by_seq_id(nodes_info)
    head_node_ip = get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_IP)
    if not head_node_ip:
        raise RuntimeError("Missing head node ip environment variable for the running node.")

    join_list = [head_node_ip]
    join_list += [node[RUNTIME_NODE_IP] for node in join_nodes]
    return join_list


def _update_agent_config(consul_config, join_list, cluster_name):
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "consul.d", "consul.json")

    def update_retry_join(config_object):
        config_object["retry_join"] = join_list

        seq_id = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID)
        if seq_id:
            node_meta = get_config_for_update(config_object, "node_meta")
            node_meta[SERVICE_DISCOVERY_LABEL_SEQ_ID] = seq_id

        node_id = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_ID)
        if node_id:
            node_meta = get_config_for_update(config_object, "node_meta")
            node_meta[SERVICE_DISCOVERY_LABEL_NODE_ID] = node_id

        if cluster_name:
            node_meta = get_config_for_update(config_object, "node_meta")
            node_meta[SERVICE_DISCOVERY_LABEL_CLUSTER] = cluster_name
            if not _is_disable_cluster_node_name(consul_config):
                if seq_id and is_valid_dns_name(cluster_name):
                    config_object["node_name"] = get_cluster_node_name(
                        cluster_name, seq_id)

    load_and_save_json(config_file, update_retry_join)


def _update_server_config_for_join():
    home_dir = _get_home_dir()
    config_file = os.path.join(home_dir, "consul.d", "server.json")

    def update_server_config(config_object):
        config_object.pop("bootstrap_expect", None)

    load_and_save_json(config_file, update_server_config)


def configure_services(runtime_config):
    """This method is called from configure.py script which is running on node.
    """
    node_type = get_runtime_node_type()
    services_config = _get_services_of_node_type(runtime_config, node_type)

    home_dir = _get_home_dir()
    config_dir = os.path.join(home_dir, "consul.d")
    services_file = os.path.join(config_dir, "services.json")
    if not services_config:
        # no services, remove the services file
        if os.path.isfile(services_file):
            os.remove(services_file)
    else:
        # generate the services configuration file
        os.makedirs(config_dir, exist_ok=True)
        services = _generate_services_def(services_config)
        with open(services_file, "w") as f:
            f.write(json.dumps(services, indent=4))


def _generate_service_def(service_name, service_config):
    node_ip = get_runtime_node_ip()
    port = service_config[SERVICE_DISCOVERY_PORT]
    check_interval = service_config.get(
        SERVICE_DISCOVERY_CHECK_INTERVAL, SERVICE_CHECK_INTERVAL_DEFAULT)
    check_timeout = service_config.get(
        SERVICE_DISCOVERY_CHECK_TIMEOUT, SERVICE_CHECK_TIMEOUT_DEFAULT)
    service_def = {
            "name": service_name,
            "address": node_ip,
            "port": port,
            "checks": [
                {
                    "tcp": address_string(node_ip, port),
                    "interval": "{}s".format(check_interval),
                    "timeout": "{}s".format(check_timeout),
                }
            ]
        }

    tags = service_config.get(SERVICE_DISCOVERY_TAGS)
    if tags:
        service_def["tags"] = tags

    labels = service_config.get(SERVICE_DISCOVERY_LABELS)
    if labels:
        service_def["meta"] = labels

    return service_def


def _generate_services_def(services_config):
    services = []
    for service_name, service_config in services_config.items():
        service_def = _generate_service_def(service_name, service_config)
        services.append(service_def)

    services_config = {
        "services": services
    }
    return services_config
