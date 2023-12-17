from typing import Dict, Any, Optional

from cloudtik.core._private.core_utils import is_valid_dns_name
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_DNSMASQ, BUILT_IN_RUNTIME_BIND, \
    BUILT_IN_RUNTIME_COREDNS
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.utils import is_config_use_hostname, get_runtime_config, \
    get_workspace_name, get_cluster_name, is_node_seq_id_enabled, is_runtime_enabled, is_config_use_fqdn

CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME = "disable_cluster_node_name"
DNS_DEFAULT_RESOLVER_CONFIG_KEY = "default_resolver"

DNS_NAMING_RUNTIMES = [
    BUILT_IN_RUNTIME_DNSMASQ,
    BUILT_IN_RUNTIME_BIND,
    BUILT_IN_RUNTIME_COREDNS]


def get_cluster_node_name(cluster_name, seq_id):
    return "{}-{}".format(cluster_name, seq_id)


def get_cluster_node_fqdn(node_name, workspace_name):
    return "{}.node.{}.cloudtik".format(node_name, workspace_name)


def get_dns_naming_runtime(runtime_config):
    for runtime_type in DNS_NAMING_RUNTIMES:
        if is_runtime_enabled(runtime_config, runtime_type):
            return runtime_type
    return None


def get_resolvable_dns_naming_runtime(runtime_config: Dict[str, Any]):
    for runtime_type in DNS_NAMING_RUNTIMES:
        if is_runtime_enabled(runtime_config, runtime_type):
            dns_config = runtime_config.get(runtime_type, {})
            if dns_config.get(DNS_DEFAULT_RESOLVER_CONFIG_KEY, False):
                return runtime_type
    return None


def is_discoverable_cluster_node_name(runtime_config: Dict[str, Any]):
    runtime_type = get_service_discovery_runtime(runtime_config)
    if not runtime_type:
        return False
    # TODO: To support other popular options than Consul
    consul_config = runtime_config.get(runtime_type, {})
    if consul_config.get(CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME, False):
        return False
    return True


def is_resolvable_cluster_node_name(runtime_config: Dict[str, Any]):
    runtime_type = get_resolvable_dns_naming_runtime(runtime_config)
    if not runtime_type:
        return False
    return True


def is_cluster_hostname_available(config):
    runtime_config = get_runtime_config(config)
    if (not is_discoverable_cluster_node_name(runtime_config)
            or not is_resolvable_cluster_node_name(runtime_config)
            or not is_node_seq_id_enabled(config)):
        return False

    # the cluster name must be a valid DNS domain name
    cluster_name = get_cluster_name(config)
    if is_valid_dns_name(cluster_name):
        return False
    return True


def get_cluster_head_host(config: Dict[str, Any], head_ip) -> str:
    head_hostname = _get_cluster_head_hostname(config)
    if not head_hostname:
        return head_hostname
    return head_ip


def get_cluster_head_hostname(config: Dict[str, Any]) -> Optional[str]:
    if (is_cluster_hostname_available(config) and
            is_config_use_hostname(config)):
        return _get_cluster_head_hostname(config)
    return None


def _get_cluster_head_hostname(config: Dict[str, Any]) -> str:
    if is_config_use_fqdn(config):
        return get_cluster_head_fqdn(config)
    else:
        return get_cluster_head_name(config)


def get_cluster_head_name(config):
    cluster_name = get_cluster_name(config)
    return _get_cluster_head_name(cluster_name)


def _get_cluster_head_name(cluster_name):
    # Assume that the head node will always get SEQ ID of 1
    return get_cluster_node_name(cluster_name, 1)


def get_cluster_head_fqdn(config):
    workspace_name = get_workspace_name(config)
    head_name = get_cluster_head_name(config)
    return get_cluster_node_fqdn(head_name, workspace_name)


def _get_cluster_head_fqdn(workspace_name, cluster_name):
    head_name = _get_cluster_head_name(cluster_name)
    return get_cluster_node_fqdn(head_name, workspace_name)
