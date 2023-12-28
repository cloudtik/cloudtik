import logging
import os

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ELASTICSEARCH
from cloudtik.core._private.util.core_utils import address_string, get_config_for_update
from cloudtik.core._private.util.runtime_utils import get_runtime_config_from_node, \
    get_worker_ips_ready_from_head, get_runtime_head_host, load_and_save_yaml
from cloudtik.runtime.elasticsearch.utils import _get_home_dir, _get_config, _get_transport_port

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
        worker_hosts = get_worker_ips_ready_from_head(
            runtime=BUILT_IN_RUNTIME_ELASTICSEARCH)
        if not worker_hosts:
            # The head will bootstrap the cluster
            pass
        else:
            _configure_cluster_joining(
                runtime_config, worker_hosts)
    else:
        # For workers, we assume the head must be bootstrapped and running
        # TODO: support to use service discovery to get a full view of seed hosts
        _configure_cluster_joining_head(runtime_config)


def _configure_cluster_joining_head(runtime_config):
    head_host = get_runtime_head_host()
    cluster_hosts = [head_host]
    _configure_cluster_joining(runtime_config, cluster_hosts)


def _get_seed_hosts(hosts, port):
    return [address_string(host, port) for host in hosts]


def _configure_cluster_joining(
        runtime_config, cluster_hosts):
    # write the config file to update discovery.seed_hosts
    home_dir = _get_home_dir()
    config_file = os.path.join(
        home_dir, "config", "elasticsearch.yml")
    elasticsearch_config = _get_config(runtime_config)
    transport_port = _get_transport_port(elasticsearch_config)
    seed_hosts = _get_seed_hosts(cluster_hosts, transport_port)

    def update_discovery_seed_hosts(config_object):
        discovery_config = get_config_for_update(config_object, "discovery")
        discovery_config["seed_hosts"] = seed_hosts
        # remove initial_master_nodes when seed_hosts is set
        discovery_config.pop("initial_master_nodes", None)

    load_and_save_yaml(
        config_file, update_discovery_seed_hosts)
