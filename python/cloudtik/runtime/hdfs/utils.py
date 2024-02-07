import os
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host, \
    is_cluster_hostname_available
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_STORAGE, define_runtime_service, \
    get_service_discovery_config_copy, set_service_discovery_label
from cloudtik.core._private.util.core_utils import http_address_string, get_config_for_update, \
    export_environment_variables
from cloudtik.core._private.utils import get_node_cluster_ip_of, get_cluster_name, get_runtime_config, \
    get_runtime_config_for_update, _sum_min_workers
from cloudtik.runtime.common.hadoop import HDFS_SERVICE_PORT_DEFAULT, \
    HDFS_NUM_NAME_NODES_LABEL, HDFS_NAME_URI_KEY, HDFS_SERVICE_TYPE, \
    HDFS_NAME_SERVICE_TYPE, get_hdfs_name_service_address, with_hdfs_name_service
from cloudtik.runtime.common.health_check import \
    HEALTH_CHECK_PORT, HEALTH_CHECK_NODE_KIND, HEALTH_CHECK_NODE_KIND_NODE, \
    HEALTH_CHECK_NODE_KIND_HEAD
from cloudtik.runtime.common.service_discovery.discovery import DiscoveryType
from cloudtik.runtime.common.service_discovery.runtime_discovery import discover_zookeeper_on_head, \
    ZOOKEEPER_CONNECT_KEY, is_zookeeper_service_discovery, discover_runtime_service_addresses, \
    discover_hdfs_name_on_head, is_hdfs_name_service_discovery
from cloudtik.runtime.common.service_discovery.utils import get_service_addresses_string
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["proc_namenode", False, "NameNode", "node"],
    ["proc_datanode", False, "DataNode", "node"],
    ["proc_journalnode", False, "JournalNode", "node"],
]

HDFS_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
# default single name node on head and workers for data nodes
HDFS_CLUSTER_MODE_NONE = "simple"
# high availability cluster
HDFS_CLUSTER_MODE_HA_CLUSTER = "ha_cluster"

HDFS_HA_CLUSTER_CONFIG_KEY = "ha_cluster"

HDFS_HA_CLUSTER_ROLE_CONFIG_KEY = "cluster_role"
HDFS_HA_CLUSTER_ROLE_NAME = "name"
HDFS_HA_CLUSTER_ROLE_DATA = "data"
HDFS_HA_CLUSTER_ROLE_JOURNAL = "journal"

HDFS_HA_CLUSTER_NUM_JOURNAL_NODES_CONFIG_KEY = "num_journal_nodes"
HDFS_HA_CLUSTER_NUM_NAME_NODES_CONFIG_KEY = "num_name_nodes"
HDFS_HA_CLUSTER_AUTO_FAILOVER_CONFIG_KEY = "auto_failover"

HDFS_FORCE_CLEAN_KEY = "force_clean"
HDFS_HEALTH_CHECK_PORT_CONFIG_KEY = "health_check_port"

HDFS_JOURNAL_CONNECT_KEY = "journal_connect"
HDFS_JOURNAL_SERVICE_DISCOVERY_KEY = "journal_service_discovery"
HDFS_JOURNAL_SERVICE_SELECTOR_KEY = "journal_service_selector"

HDFS_SERVICE_PORT = HDFS_SERVICE_PORT_DEFAULT
HDFS_HTTP_PORT = 9870

HDFS_JOURNAL_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS + "-journal"
HDFS_JOURNAL_SERVICE_PORT = 8485
HDFS_JOURNAL_HTTP_PORT = 8480


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HDFS, {})


def _get_cluster_mode(hdfs_config: Dict[str, Any]):
    return hdfs_config.get(
        HDFS_CLUSTER_MODE_CONFIG_KEY, HDFS_CLUSTER_MODE_NONE)


def _get_service_port(hdfs_config: Dict[str, Any]):
    return HDFS_SERVICE_PORT


def _get_ha_cluster_config(hdfs_config: Dict[str, Any]):
    return hdfs_config.get(HDFS_HA_CLUSTER_CONFIG_KEY, {})


def _get_ha_cluster_role(ha_cluster_config: Dict[str, Any]):
    return ha_cluster_config.get(
        HDFS_HA_CLUSTER_ROLE_CONFIG_KEY, HDFS_HA_CLUSTER_ROLE_DATA)


def _get_ha_cluster_num_journal_nodes(ha_cluster_config: Dict[str, Any]):
    return ha_cluster_config.get(HDFS_HA_CLUSTER_NUM_JOURNAL_NODES_CONFIG_KEY)


def _get_ha_cluster_num_name_nodes(ha_cluster_config: Dict[str, Any]):
    return ha_cluster_config.get(HDFS_HA_CLUSTER_NUM_NAME_NODES_CONFIG_KEY)


def _is_ha_cluster_auto_failover(ha_cluster_config: Dict[str, Any]):
    return ha_cluster_config.get(HDFS_HA_CLUSTER_AUTO_FAILOVER_CONFIG_KEY, True)


def _get_health_check_port(ha_cluster_config: Dict[str, Any], service_port):
    default_port = 10000 + service_port
    return ha_cluster_config.get(
        HDFS_HEALTH_CHECK_PORT_CONFIG_KEY, default_port)


def register_service(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        head_node_id: str) -> None:
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            _register_name_service_to_workspace(
                hdfs_config, cluster_config)
    else:
        head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
        head_host = get_cluster_head_host(cluster_config, head_ip)
        _register_hdfs_service_to_workspace(
            hdfs_config, cluster_config, head_host)


def _register_hdfs_service_to_workspace(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        host):
    service_port = _get_service_port(hdfs_config)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_HDFS,
        service_addresses=[(host, service_port)])


def _register_name_service_to_workspace(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any]):
    service_port = _get_service_port(hdfs_config)
    ha_cluster_config = _get_ha_cluster_config(hdfs_config)
    cluster_name = get_cluster_name(cluster_config)
    num_name_nodes = _get_ha_cluster_num_name_nodes(ha_cluster_config)
    name_service_address = get_hdfs_name_service_address(cluster_name, num_name_nodes)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_HDFS,
        service_addresses=[(name_service_address, service_port)],
        service_name=HDFS_NAME_SERVICE_TYPE)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            # name cluster will set the number of name nodes as service label
            # and data nodes will get the number of name nodes through discovery
            num_name_nodes = _get_ha_cluster_num_name_nodes(ha_cluster_config)
            if not num_name_nodes:
                update_num_name_nodes(cluster_config)

    return cluster_config


def update_num_name_nodes(cluster_config: Dict[str, Any]):
    runtime_config = get_runtime_config_for_update(cluster_config)
    hdfs_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_HDFS)
    ha_cluster_config = get_config_for_update(hdfs_config, HDFS_HA_CLUSTER_CONFIG_KEY)
    num_name_nodes = _sum_min_workers(cluster_config)
    num_name_nodes += 1
    ha_cluster_config[HDFS_HA_CLUSTER_NUM_NAME_NODES_CONFIG_KEY] = num_name_nodes


def _validate_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        final=False):
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        _validate_ha_cluster_config(
            hdfs_config, cluster_config, final=final)


def validate_cluster_hostname(cluster_config: Dict[str, Any]):
    if not is_cluster_hostname_available(cluster_config):
        raise RuntimeError(
            "HDFS HA name or data cluster needs resolvable hostname from service discovery DNS.")


def _validate_ha_cluster_config(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        final=False):
    ha_cluster_config = _get_ha_cluster_config(hdfs_config)
    # cluster mode needs service discovery and host DNS service
    cluster_runtime_config = get_runtime_config(cluster_config)
    if not get_service_discovery_runtime(cluster_runtime_config):
        raise RuntimeError(
            "HDFS HA cluster needs Consul service discovery to be configured.")

    cluster_role = _get_ha_cluster_role(ha_cluster_config)
    if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
        validate_cluster_hostname(cluster_config)

        # this only needed for name cluster
        journal_uri = hdfs_config.get(HDFS_JOURNAL_CONNECT_KEY)
        if not journal_uri:
            # if there is service discovery mechanism,
            # assume we can get from service discovery if this is not final
            if (final or not is_journal_service_discovery(hdfs_config) or
                    not get_service_discovery_runtime(cluster_runtime_config)):
                raise ValueError(
                    "HDFS Journal must be configured for HDFS HA name cluster.")

        if _is_ha_cluster_auto_failover(ha_cluster_config):
            zookeeper_uri = hdfs_config.get(ZOOKEEPER_CONNECT_KEY)
            if not zookeeper_uri:
                # if there is service discovery mechanism,
                # assume we can get from service discovery if this is not final
                if (final or not is_zookeeper_service_discovery(hdfs_config) or
                        not get_service_discovery_runtime(cluster_runtime_config)):
                    raise ValueError(
                        "Zookeeper must be configured for HDFS HA name cluster with automatic failover.")
    elif cluster_role == HDFS_HA_CLUSTER_ROLE_DATA:
        validate_cluster_hostname(cluster_config)

        name_uri = hdfs_config.get(HDFS_NAME_URI_KEY)
        if not name_uri:
            # if there is service discovery mechanism,
            # assume we can get from service discovery if this is not final
            if (final or not is_hdfs_name_service_discovery(hdfs_config) or
                    not get_service_discovery_runtime(cluster_runtime_config)):
                raise ValueError(
                    "HDFS Name cluster must be configured for HDFS HA data cluster.")


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            cluster_config = discover_journal_on_head(
                cluster_config, BUILT_IN_RUNTIME_HDFS)
            if _is_ha_cluster_auto_failover(ha_cluster_config):
                cluster_config = discover_zookeeper_on_head(
                    cluster_config, BUILT_IN_RUNTIME_HDFS)
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_DATA:
            cluster_config = discover_hdfs_name_on_head(
                cluster_config, BUILT_IN_RUNTIME_HDFS)
    # call validate config to fail earlier
    _validate_config(
        runtime_config, cluster_config, final=True)
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    runtime_envs = {
        "HDFS_CLUSTER_MODE": cluster_mode,
        "HDFS_SERVICE_PORT": HDFS_SERVICE_PORT,
        "HDFS_HTTP_PORT": HDFS_HTTP_PORT,
    }

    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        runtime_envs["HDFS_CLUSTER_ROLE"] = cluster_role

        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            runtime_envs["HDFS_ENABLED"] = True
            auto_failover = _is_ha_cluster_auto_failover(ha_cluster_config)
            runtime_envs["HDFS_AUTO_FAILOVER"] = auto_failover

            # Use the cluster name as the name service ID
            # This is the default name cluster. data cluster will set based on discovery
            cluster_name = get_cluster_name(config)
            runtime_envs["HDFS_NAME_SERVICE"] = cluster_name

            num_name_nodes = _get_ha_cluster_num_name_nodes(ha_cluster_config)
            runtime_envs["HDFS_NUM_NAME_NODES"] = num_name_nodes
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_JOURNAL:
            runtime_envs["HDFS_JOURNAL_SERVICE_PORT"] = HDFS_JOURNAL_SERVICE_PORT
    else:
        runtime_envs["HDFS_ENABLED"] = True
    force_clean = hdfs_config.get(HDFS_FORCE_CLEAN_KEY, False)
    if force_clean:
        runtime_envs["HDFS_FORCE_CLEAN"] = force_clean

    return runtime_envs


def _node_configure(runtime_config, head: bool):
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    envs = {}
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            envs = _with_name_configure(hdfs_config, envs=envs)
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_DATA:
            envs = _with_data_configure(hdfs_config, envs=envs)

    export_environment_variables(envs)


def _with_name_configure(hdfs_config, envs=None):
    if envs is None:
        envs = {}

    journal_uri = hdfs_config.get(HDFS_JOURNAL_CONNECT_KEY)
    if journal_uri:
        envs["HDFS_JOURNAL_NODES"] = journal_uri

    ha_cluster_config = _get_ha_cluster_config(hdfs_config)
    if _is_ha_cluster_auto_failover(ha_cluster_config):
        zookeeper_uri = hdfs_config.get(ZOOKEEPER_CONNECT_KEY)
        if zookeeper_uri:
            envs["HDFS_ZOOKEEPER_QUORUM"] = zookeeper_uri

    return envs


def _with_data_configure(hdfs_config, envs=None):
    # set name service ID and name cluster name based on discovery
    hdfs_name_uri = hdfs_config.get(HDFS_NAME_URI_KEY)
    if not hdfs_name_uri:
        # This usually will not happen. Checks are done before this.
        raise RuntimeError(
            "Name uri is not configured for HDFS HA data cluster.")
    envs = with_hdfs_name_service(hdfs_name_uri, envs=envs)
    return envs


def _get_runtime_logs():
    hadoop_logs_dir = os.path.join(os.getenv("HADOOP_HOME"), "logs")
    all_logs = {"hadoop": hadoop_logs_dir}
    return all_logs


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            endpoints = _get_name_endpoints(head_host)
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_JOURNAL:
            endpoints = _get_journal_endpoints(head_host)
        else:
            endpoints = {}
    else:
        endpoints = _get_name_endpoints(head_host)
    return endpoints


def _get_name_endpoints(host):
    endpoints = {
        "hdfs-web": {
            "name": "HDFS Web UI",
            "url": http_address_string(host, HDFS_HTTP_PORT)
        },
        "hdfs": {
            "name": "HDFS Service",
            "url": "hdfs://{}:{}".format(host, HDFS_SERVICE_PORT)
        },
    }
    return endpoints


def _get_journal_endpoints(host):
    endpoints = {
        "journal-http": {
            "name": "Journal HTTP",
            "url": http_address_string(host, HDFS_JOURNAL_HTTP_PORT)
        },
        "journal": {
            "name": "Journal Node",
            "url": "hdfs://{}:{}".format(host, HDFS_JOURNAL_SERVICE_PORT)
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            service_ports = _get_name_head_service_ports()
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_JOURNAL:
            service_ports = _get_journal_head_service_ports()
        else:
            service_ports = {}
    else:
        service_ports = _get_name_head_service_ports()
    return service_ports


def _get_name_head_service_ports():
    service_ports = {
        "hdfs-web": {
            "protocol": "TCP",
            "port": HDFS_HTTP_PORT,
        },
        "hdfs": {
            "protocol": "TCP",
            "port": HDFS_SERVICE_PORT,
        },
    }
    return service_ports


def _get_journal_head_service_ports():
    service_ports = {
        "journal-http": {
            "protocol": "TCP",
            "port": HDFS_JOURNAL_HTTP_PORT,
        },
        "journal": {
            "protocol": "TCP",
            "port": HDFS_JOURNAL_SERVICE_PORT,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    # service name is decided by the runtime itself
    # For in services backed by the collection of nodes of the cluster
    # service name is a combination of cluster_name + runtime_service_name
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)

    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            services = _get_name_runtime_services(
                hdfs_config, cluster_config)
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_JOURNAL:
            services = _get_journal_runtime_services(
                hdfs_config, cluster_config)
        else:
            # data node role, no services exposed
            services = {}
    else:
        services = _get_simple_runtime_services(
            hdfs_config, cluster_config)
    return services


def _get_simple_runtime_services(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any]):
    cluster_name = get_cluster_name(cluster_config)
    service_discovery_config = get_service_discovery_config(hdfs_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HDFS_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service_on_head(
            HDFS_SERVICE_TYPE,
            service_discovery_config, HDFS_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_STORAGE]),
    }
    return services


def _get_name_runtime_services(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any]):
    cluster_name = get_cluster_name(cluster_config)
    service_discovery_config = get_service_discovery_config_copy(hdfs_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HDFS_NAME_SERVICE_TYPE)
    ha_cluster_config = _get_ha_cluster_config(hdfs_config)
    # name cluster will set the number of name nodes as service label
    # and data nodes will get the number of name nodes through discovery
    num_name_nodes = _get_ha_cluster_num_name_nodes(ha_cluster_config)
    set_service_discovery_label(
        service_discovery_config,
        HDFS_NUM_NAME_NODES_LABEL, num_name_nodes)

    services = {
        service_name: define_runtime_service(
            HDFS_NAME_SERVICE_TYPE,
            service_discovery_config, HDFS_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_STORAGE]),
    }
    return services


def _get_journal_runtime_services(
        hdfs_config: Dict[str, Any],
        cluster_config: Dict[str, Any]):
    cluster_name = get_cluster_name(cluster_config)
    service_discovery_config = get_service_discovery_config(hdfs_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HDFS_JOURNAL_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service(
            HDFS_JOURNAL_SERVICE_TYPE,
            service_discovery_config, HDFS_JOURNAL_SERVICE_PORT,
            features=[SERVICE_DISCOVERY_FEATURE_STORAGE]),
    }
    return services


def _get_health_check(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    service_port = _get_service_port(hdfs_config)
    health_check_port = _get_health_check_port(
        hdfs_config, service_port)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            health_check = {
                HEALTH_CHECK_PORT: health_check_port,
                HEALTH_CHECK_NODE_KIND: HEALTH_CHECK_NODE_KIND_NODE,
            }
            return health_check
    else:
        health_check = {
            HEALTH_CHECK_PORT: health_check_port,
            HEALTH_CHECK_NODE_KIND: HEALTH_CHECK_NODE_KIND_HEAD,
        }
        return health_check
    return None


"""
Journal service discovery conventions:
    1. The Journal service discovery flag is stored at HDFS_JOURNAL_SERVICE_DISCOVERY_KEY defined above
    2. The Journal service selector is stored at HDFS_JOURNAL_SERVICE_SELECTOR_KEY defined above
    3. The Journal connect is stored at HDFS_JOURNAL_CONNECT_KEY defined above
"""


def is_journal_service_discovery(runtime_type_config):
    return runtime_type_config.get(
        HDFS_JOURNAL_SERVICE_DISCOVERY_KEY, True)


def discover_journal_on_head(
        cluster_config: Dict[str, Any], runtime_type):
    runtime_config = get_runtime_config(cluster_config)
    runtime_type_config = runtime_config.get(runtime_type, {})
    if not is_journal_service_discovery(runtime_type_config):
        return cluster_config

    journal_uri = runtime_type_config.get(HDFS_JOURNAL_CONNECT_KEY)
    if journal_uri:
        # Journal already configured
        return cluster_config

    # There is service discovery to come here
    journal_addresses = discover_journal(
        runtime_type_config,
        HDFS_JOURNAL_SERVICE_SELECTOR_KEY,
        cluster_config=cluster_config)
    if journal_addresses:
        # check the number of journal nodes vs the configurations if user specified
        ha_cluster_config = _get_ha_cluster_config(runtime_type_config)
        journal_expect = _get_ha_cluster_num_journal_nodes(ha_cluster_config)
        if journal_expect:
            journal_num = len(journal_addresses)
            if journal_expect != journal_num:
                raise RuntimeError(
                    "The number of HDFS Journal nodes don't match. Got {}, expect: {}".format(
                        journal_num, journal_expect))
        journal_uri = get_service_addresses_string(
            journal_addresses, ";")
        if journal_uri:
            # do update
            runtime_type_config = get_config_for_update(
                runtime_config, runtime_type)
            runtime_type_config[HDFS_JOURNAL_CONNECT_KEY] = journal_uri
    return cluster_config


def discover_journal(
        config: Dict[str, Any],
        service_selector_key: str,
        cluster_config: Dict[str, Any]):
    service_addresses = discover_runtime_service_addresses(
        config, service_selector_key,
        cluster_config=cluster_config,
        discovery_type=DiscoveryType.CLUSTER,
        runtime_type=BUILT_IN_RUNTIME_HDFS,
        service_type=HDFS_JOURNAL_SERVICE_TYPE
    )
    return service_addresses
