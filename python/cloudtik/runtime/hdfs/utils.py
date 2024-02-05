import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import get_canonical_service_name, define_runtime_service_on_head, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_STORAGE, define_runtime_service
from cloudtik.core._private.util.core_utils import http_address_string
from cloudtik.core._private.utils import get_node_cluster_ip_of, get_cluster_name
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["proc_namenode", False, "NameNode", "head"],
    ["proc_datanode", False, "DataNode", "worker"],
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

HDFS_FORCE_CLEAN_KEY = "force_clean"

HDFS_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS
HDFS_SERVICE_PORT = 9000
HDFS_HTTP_PORT = 9870

HDFS_JOURNAL_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS + "-journal"
HDFS_JOURNAL_SERVICE_PORT = 8485
HDFS_JOURNAL_HTTP_PORT = 8480


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HDFS, {})


def _get_cluster_mode(hdfs_config: Dict[str, Any]):
    return hdfs_config.get(
        HDFS_CLUSTER_MODE_CONFIG_KEY, HDFS_CLUSTER_MODE_NONE)


def _get_ha_cluster_config(hdfs_config: Dict[str, Any]):
    return hdfs_config.get(HDFS_HA_CLUSTER_CONFIG_KEY, {})


def _get_ha_cluster_role(ha_cluster_config: Dict[str, Any]):
    return ha_cluster_config.get(
        HDFS_HA_CLUSTER_ROLE_CONFIG_KEY, HDFS_HA_CLUSTER_ROLE_DATA)


def register_service(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        head_node_id: str) -> None:
    head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
    head_host = get_cluster_head_host(cluster_config, head_ip)
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            _register_name_service_to_workspace(
                cluster_config, head_host)
    else:
        _register_name_service_to_workspace(
            cluster_config, head_host)


def _register_name_service_to_workspace(
        cluster_config: Dict[str, Any], host):
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_HDFS,
        service_addresses=[(host, HDFS_SERVICE_PORT)])


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    hdfs_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(hdfs_config)
    runtime_envs = {
        "HDFS_CLUSTER_MODE": cluster_mode,
        "HDFS_SERVICE_PORT": HDFS_SERVICE_PORT,
    }

    if cluster_mode == HDFS_CLUSTER_MODE_HA_CLUSTER:
        ha_cluster_config = _get_ha_cluster_config(hdfs_config)
        cluster_role = _get_ha_cluster_role(ha_cluster_config)
        runtime_envs["HDFS_CLUSTER_ROLE"] = cluster_role
        if cluster_role == HDFS_HA_CLUSTER_ROLE_NAME:
            runtime_envs["HDFS_ENABLED"] = True
        elif cluster_role == HDFS_HA_CLUSTER_ROLE_JOURNAL:
            runtime_envs["HDFS_JOURNAL_SERVICE_PORT"] = HDFS_JOURNAL_SERVICE_PORT
    else:
        runtime_envs["HDFS_ENABLED"] = True
    force_clean = hdfs_config.get(HDFS_FORCE_CLEAN_KEY, False)
    if force_clean:
        runtime_envs["HDFS_FORCE_CLEAN"] = force_clean

    return runtime_envs


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
    service_discovery_config = get_service_discovery_config(hdfs_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, HDFS_SERVICE_TYPE)
    services = {
        service_name: define_runtime_service(
            HDFS_SERVICE_TYPE,
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
