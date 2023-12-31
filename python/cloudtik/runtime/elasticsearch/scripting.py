import logging
import os

from cloudtik.core._private.constants import CLOUDTIK_FS_PATH
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ELASTICSEARCH
from cloudtik.core._private.util.core_utils import address_string, get_config_for_update
from cloudtik.core._private.util.runtime_utils import get_runtime_config_from_node, \
    get_runtime_head_host, load_and_save_yaml, get_runtime_node_seq_id, \
    get_runtime_node_type, get_runtime_cluster_name, get_worker_hosts_ready_from_head
from cloudtik.runtime.elasticsearch.utils import _get_home_dir, _get_config, _get_transport_port, \
    _get_clustering_config, _is_role_by_node_type, _get_node_type_of_roles, _is_snapshot_repository_enabled, \
    _is_role_support_snapshot_repository

logger = logging.getLogger(__name__)

###################################
# Calls from node at runtime
###################################


def configure_clustering(head):
    # For head, there are two cases to handle:
    # 1. the first time of the cluster creation: bootstrap the cluster
    # 2. restart after the cluster has formed: set the discovery seed hosts
    # by finding existing nodes.
    # For workers, there is only one case to handle:
    # set the discovery seed hosts and join the existing the cluster
    runtime_config = get_runtime_config_from_node(head)

    if head:
        # For head, check whether there are workers running.
        worker_hosts = get_worker_hosts_ready_from_head(
            runtime=BUILT_IN_RUNTIME_ELASTICSEARCH)
        if not worker_hosts:
            # The head will bootstrap the cluster
            _configure_cluster_bootstrap(runtime_config)
        else:
            _configure_cluster_joining(
                runtime_config, worker_hosts)
    else:
        # For workers, we assume the head must be bootstrapped and running
        # TODO: support to use service discovery to get a full view of seed hosts
        _configure_cluster_joining_head(runtime_config)


def _configure_node_roles_by_node_type(
        elasticsearch_config, config_object):
    clustering_config = _get_clustering_config(elasticsearch_config)
    if not _is_role_by_node_type(clustering_config):
        return None
    node_type_of_roles = _get_node_type_of_roles(clustering_config)
    if not node_type_of_roles:
        return None
    node_type = get_runtime_node_type()
    if node_type not in node_type_of_roles:
        return None

    roles = node_type_of_roles[node_type]
    # assign role based on node type
    node_config_object = get_config_for_update(config_object, "node")
    node_config_object["roles"] = roles
    return roles


def _need_snapshot_repository_for_roles(roles):
    for role in roles:
        if _is_role_support_snapshot_repository(role):
            return True
    return False


def _configure_node_roles(runtime_config, config_object):
    elasticsearch_config = _get_config(runtime_config)
    roles = _configure_node_roles_by_node_type(
            elasticsearch_config, config_object)
    set_snapshot_repository = _is_snapshot_repository_enabled(
        elasticsearch_config)
    if roles is not None:
        if set_snapshot_repository:
            set_snapshot_repository = _need_snapshot_repository_for_roles(
                roles)
    else:
        # default all roles
        node_config_object = config_object.get("node")
        if node_config_object:
            node_config_object.pop("roles", None)

    if set_snapshot_repository:
        _configure_snapshot_repository(config_object)


def _configure_snapshot_repository(config_object):
    cluster_name = get_runtime_cluster_name()
    snapshot_repository_path = os.path.join(
        CLOUDTIK_FS_PATH, BUILT_IN_RUNTIME_ELASTICSEARCH,
        "snapshots", cluster_name)

    # This is called at service starting, so the mount is ready
    os.makedirs(snapshot_repository_path, exist_ok=True)

    path_config_object = get_config_for_update(config_object, "path")
    path_config_object["repo"] = [snapshot_repository_path]


def _configure_cluster_bootstrap(runtime_config):
    def update_initial_master_nodes(config_object):
        cluster_config = get_config_for_update(config_object, "cluster")
        node_name = "node-{}".format(get_runtime_node_seq_id())
        cluster_config["initial_master_nodes"] = [node_name]
        _configure_node_roles(runtime_config, config_object)

    _update_config_file(update_initial_master_nodes)


def _configure_cluster_joining_head(runtime_config):
    head_host = get_runtime_head_host()
    cluster_hosts = [head_host]
    _configure_cluster_joining(runtime_config, cluster_hosts)


def _get_seed_hosts(hosts, port):
    return [address_string(host, port) for host in hosts]


def _configure_cluster_joining(
        runtime_config, cluster_hosts):
    # write the config file to update discovery.seed_hosts
    elasticsearch_config = _get_config(runtime_config)
    transport_port = _get_transport_port(elasticsearch_config)
    seed_hosts = _get_seed_hosts(cluster_hosts, transport_port)

    def update_discovery_seed_hosts(config_object):
        discovery_config = get_config_for_update(config_object, "discovery")
        discovery_config["seed_hosts"] = seed_hosts
        # remove initial_master_nodes when seed_hosts is set
        cluster_config = config_object.get("cluster")
        if cluster_config:
            cluster_config.pop("initial_master_nodes", None)
        _configure_node_roles(runtime_config, config_object)

    _update_config_file(update_discovery_seed_hosts)


def _update_config_file(update_fn):
    # write the config file to update discovery.seed_hosts
    home_dir = _get_home_dir()
    config_file = os.path.join(
        home_dir, "config", "elasticsearch.yml")

    load_and_save_yaml(
        config_file, update_fn)
