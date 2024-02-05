from typing import Dict, Any, Optional, List

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_NODE_HOST, CLOUDTIK_RUNTIME_ENV_HEAD_HOST, \
    CLOUDTIK_RUNTIME_ENV_NODE_IP, CLOUDTIK_RUNTIME_ENV_HEAD_IP
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_DNSMASQ, BUILT_IN_RUNTIME_BIND, \
    BUILT_IN_RUNTIME_COREDNS
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import ServiceAddressType
from cloudtik.core._private.util.core_utils import is_valid_dns_name
from cloudtik.core._private.utils import is_config_use_hostname, get_runtime_config, \
    get_workspace_name, get_cluster_name, is_runtime_enabled, is_config_use_fqdn, \
    _get_worker_nodes_info, NODE_INFO_NODE_IP, get_cluster_head_ip
from cloudtik.core.tags import CLOUDTIK_TAG_HEAD_NODE_SEQ_ID, CLOUDTIK_TAG_NODE_SEQ_ID

CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME = "disable_cluster_node_name"
CONSUL_CONFIG_SERVER_MODE = "server"

DNS_DEFAULT_RESOLVER_CONFIG_KEY = "default_resolver"

DNS_NAMING_RUNTIMES = [
    BUILT_IN_RUNTIME_DNSMASQ,
    BUILT_IN_RUNTIME_BIND,
    BUILT_IN_RUNTIME_COREDNS]

HEAD_NODE_SEQ_ID = CLOUDTIK_TAG_HEAD_NODE_SEQ_ID


def get_cluster_node_name(cluster_name, seq_id):
    if not cluster_name:
        raise RuntimeError("Invalid cluster name: should not be empty.")
    return "{}-{}".format(cluster_name, seq_id)


def get_cluster_node_sqdn(node_name):
    # short qualified domain name without workspace-name.dc
    return "{}.node.cloudtik".format(node_name)


def get_cluster_node_fqdn(node_name, workspace_name):
    if not workspace_name:
        raise RuntimeError("Invalid workspace name: should not be empty.")
    return "{}.node.{}.cloudtik".format(node_name, workspace_name)


def get_address_type_of_hostname(hostname):
    if not hostname:
        raise RuntimeError("Invalid hostname: hostname is empty.")
    components = hostname.split('.')
    if len(components) <= 3:
        return ServiceAddressType.NODE_SQDN
    else:
        return ServiceAddressType.NODE_FQDN


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
    # Server mode cannot use DNS because the Consul cluster itself is not yet ready
    # TODO: This can be improved to start consul with single node from head and workers join
    #  the cluster later
    if (consul_config.get(CONSUL_CONFIG_SERVER_MODE, False)
            or consul_config.get(CONSUL_CONFIG_DISABLE_CLUSTER_NODE_NAME, False)):
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
            or not is_resolvable_cluster_node_name(runtime_config)):
        return False

    # the cluster name must be a valid DNS domain name
    cluster_name = get_cluster_name(config)
    if not is_valid_dns_name(cluster_name):
        return False
    return True


def get_cluster_node_host(config: Dict[str, Any], node_seq_id, node_ip) -> str:
    node_hostname = get_cluster_node_hostname(config, node_seq_id)
    if node_hostname:
        return node_hostname
    return node_ip


def get_cluster_node_hostname(
        config: Dict[str, Any], node_seq_id) -> Optional[str]:
    if (node_seq_id is not None and
            is_cluster_hostname_available(config) and
            is_config_use_hostname(config)):
        return _get_cluster_node_hostname(config, node_seq_id)
    return None


def get_cluster_head_host(config: Dict[str, Any], head_ip=None) -> str:
    if not head_ip:
        head_ip = get_cluster_head_ip(config)
    return get_cluster_node_host(config, HEAD_NODE_SEQ_ID, head_ip)


def get_cluster_head_hostname(config: Dict[str, Any]) -> Optional[str]:
    return get_cluster_node_hostname(config, HEAD_NODE_SEQ_ID)


def _get_cluster_node_hostname(config: Dict[str, Any], node_seq_id) -> str:
    if is_config_use_fqdn(config):
        return get_cluster_node_fqdn_of(config, node_seq_id)
    else:
        return get_cluster_node_sqdn_of(config, node_seq_id)


def get_cluster_node_sqdn_of(config, node_seq_id):
    cluster_name = get_cluster_name(config)
    return _get_cluster_node_sqdn_of(cluster_name, node_seq_id)


def _get_cluster_node_sqdn_of(cluster_name, node_seq_id):
    node_name = get_cluster_node_name(cluster_name, node_seq_id)
    return get_cluster_node_sqdn(node_name)


def get_cluster_node_fqdn_of(config, node_seq_id):
    workspace_name = get_workspace_name(config)
    cluster_name = get_cluster_name(config)
    return _get_cluster_node_fqdn_of(
        workspace_name, cluster_name, node_seq_id)


def _get_cluster_node_fqdn_of(
        workspace_name, cluster_name, node_seq_id):
    node_name = get_cluster_node_name(cluster_name, node_seq_id)
    return get_cluster_node_fqdn(node_name, workspace_name)


def with_node_host_environment_variables(
        config: Dict[str, Any], node_seq_id, node_envs):
    node_ip = node_envs.get(CLOUDTIK_RUNTIME_ENV_NODE_IP)
    if not node_ip:
        raise RuntimeError("Node IP must be set.")
    node_host = get_cluster_node_host(config, node_seq_id, node_ip)
    node_envs[CLOUDTIK_RUNTIME_ENV_NODE_HOST] = node_host
    return node_envs


def with_head_host_environment_variables(
        config: Dict[str, Any], node_envs):
    head_ip = node_envs.get(CLOUDTIK_RUNTIME_ENV_HEAD_IP)
    if not head_ip:
        raise RuntimeError("Head IP must be set.")
    head_host = get_cluster_node_host(config, HEAD_NODE_SEQ_ID, head_ip)
    node_envs[CLOUDTIK_RUNTIME_ENV_HEAD_HOST] = head_host
    return node_envs


def get_cluster_node_address_type(
        config: Dict[str, Any]) -> ServiceAddressType:
    if (is_cluster_hostname_available(config)
            and is_config_use_hostname(config)):
        if is_config_use_fqdn(config):
            return ServiceAddressType.NODE_FQDN
        else:
            return ServiceAddressType.NODE_SQDN
    return ServiceAddressType.NODE_IP


def get_fqdn_of_node_info(config, node_info):
    node_seq_id = node_info[CLOUDTIK_TAG_NODE_SEQ_ID]
    return get_cluster_node_fqdn_of(config, node_seq_id)


def get_sqdn_of_node_info(config, node_info):
    node_seq_id = node_info[CLOUDTIK_TAG_NODE_SEQ_ID]
    return get_cluster_node_sqdn_of(config, node_seq_id)


def _get_hosts_of_nodes_info(config, nodes_info):
    if (is_cluster_hostname_available(config)
            and is_config_use_hostname(config)):
        if is_config_use_fqdn(config):
            return [get_fqdn_of_node_info(
                config, node_info) for node_info in nodes_info]
        else:
            return [get_sqdn_of_node_info(
                config, node_info) for node_info in nodes_info]
    # use ip as host instead
    return [node_info[NODE_INFO_NODE_IP] for node_info in nodes_info]


def _get_worker_node_hosts(
        config: Dict[str, Any],
        runtime: str = None,
        node_status: str = None) -> List[str]:
    nodes_info = _get_worker_nodes_info(
        config, runtime=runtime, node_status=node_status)
    # convert to hosts and sort
    return sorted(
        _get_hosts_of_nodes_info(config, nodes_info))
