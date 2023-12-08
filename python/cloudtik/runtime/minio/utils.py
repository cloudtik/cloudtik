import os
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_WORKSPACE, CLOUDTIK_RUNTIME_ENV_CLUSTER, \
    CLOUDTIK_DATA_DISK_MOUNT_POINT, CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX
from cloudtik.core._private.core_utils import get_config_for_update
from cloudtik.core._private.provider_factory import _get_node_provider
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MINIO
from cloudtik.core._private.runtime_utils import get_runtime_value, get_data_disk_dirs
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime, \
    is_discoverable_cluster_node_name
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_PROTOCOL_HTTP, SERVICE_DISCOVERY_FEATURE_METRICS, \
    get_cluster_node_name, SERVICE_DISCOVERY_NODE_KIND_NODE, SERVICE_DISCOVERY_NODE_KIND_WORKER
from cloudtik.core._private.utils import get_runtime_config, get_runtime_config_for_update, _sum_min_workers, \
    enable_node_seq_id, is_node_seq_id_enabled
from cloudtik.runtime.common.service_discovery.consul import get_dns_hostname_of_node
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["minio", True, "MinIO", "node"],
    ]

MINIO_SERVICE_PORT_CONFIG_KEY = "port"
MINIO_CONSOLE_PORT_CONFIG_KEY = "console_port"
MINIO_SERVER_POOL_SIZE_CONFIG_KEY = "server_pool_size"
MINIO_SERVICE_ON_HEAD_CONFIG_KEY = "service_on_head"

# automatically set by runtime bootstrap to the cluster total nodes
MINIO_SERVER_CLUSTER_SIZE_CONFIG_KEY = "server_cluster_size"

MINIO_SERVICE_TYPE = BUILT_IN_RUNTIME_MINIO

MINIO_SERVICE_PORT_DEFAULT = 9000
MINIO_CONSOLE_PORT_DEFAULT = 9001
MINIO_SERVER_POOL_SIZE_DEFAULT = 4


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_MINIO, {})


def _get_service_port(minio_config: Dict[str, Any]):
    return minio_config.get(
        MINIO_SERVICE_PORT_CONFIG_KEY, MINIO_SERVICE_PORT_DEFAULT)


def _get_console_port(minio_config: Dict[str, Any]):
    return minio_config.get(
        MINIO_CONSOLE_PORT_CONFIG_KEY, MINIO_CONSOLE_PORT_DEFAULT)


def _get_server_pool_size(minio_config: Dict[str, Any]):
    return minio_config.get(
        MINIO_SERVER_POOL_SIZE_CONFIG_KEY, MINIO_SERVER_POOL_SIZE_DEFAULT)


def _is_service_on_head(minio_config: Dict[str, Any]):
    return minio_config.get(
        MINIO_SERVICE_ON_HEAD_CONFIG_KEY, True)


def _get_server_cluster_size(minio_config: Dict[str, Any]):
    server_cluster_size = minio_config.get(
        MINIO_SERVER_CLUSTER_SIZE_CONFIG_KEY)
    if not server_cluster_size:
        raise RuntimeError("MinIO server cluster size is invalid.")
    return server_cluster_size


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_MINIO)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"minio": logs_dir}


def _bootstrap_runtime_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    # We must enable the node seq id
    if not is_node_seq_id_enabled(cluster_config):
        enable_node_seq_id(cluster_config)

    runtime_config = get_runtime_config_for_update(cluster_config)
    minio_config = get_config_for_update(runtime_config, BUILT_IN_RUNTIME_MINIO)
    minio_workers = _sum_min_workers(cluster_config)
    if _is_service_on_head(minio_config):
        minio_workers += 1
    minio_config[MINIO_SERVER_CLUSTER_SIZE_CONFIG_KEY] = minio_workers
    return cluster_config


def _validate_config(config: Dict[str, Any], final=False):
    # Check Consul configured
    runtime_config = get_runtime_config(config)
    if not get_service_discovery_runtime(runtime_config):
        raise RuntimeError("MinIO needs Consul service discovery to be configured.")

    if not is_discoverable_cluster_node_name(runtime_config):
        raise RuntimeError("MinIO needs sequential cluster node name from service discovery DNS.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {"MINIO_ENABLED": True}

    minio_config = _get_config(runtime_config)

    service_port = _get_service_port(minio_config)
    runtime_envs["MINIO_SERVICE_PORT"] = service_port

    console_port = _get_console_port(minio_config)
    runtime_envs["MINIO_CONSOLE_PORT"] = console_port

    server_pool_size = _get_server_pool_size(minio_config)
    server_cluster_size = _get_server_cluster_size(minio_config)
    server_pools = server_cluster_size // server_pool_size
    valid_cluster_size = server_pools * server_pool_size
    runtime_envs["MINIO_CLUSTER_SIZE"] = valid_cluster_size

    max_node_seq_id = valid_cluster_size
    if not _is_service_on_head(minio_config):
        max_node_seq_id += 1
    runtime_envs["MINIO_MAX_SEQ_ID"] = max_node_seq_id

    runtime_envs["MINIO_SERVICE_ON_HEAD"] = _is_service_on_head(minio_config)
    return runtime_envs


def register_service(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any],
        head_node_id: str) -> None:
    minio_config = _get_config(runtime_config)
    if not _is_service_on_head(minio_config):
        return

    provider = _get_node_provider(
        cluster_config["provider"], cluster_config["cluster_name"])
    head_ip = provider.internal_ip(head_node_id)

    service_port = _get_service_port(minio_config)
    register_service_to_workspace(
        cluster_config, BUILT_IN_RUNTIME_MINIO,
        service_addresses=[(head_ip, service_port)])


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_head_ip):
    minio_config = _get_config(runtime_config)
    if not _is_service_on_head(minio_config):
        return {}
    endpoints = {
        "minio": {
            "name": "MinIO Service",
            "url": "http://{}:{}".format(
                cluster_head_ip, _get_service_port(minio_config))
        },
        "minio-console": {
            "name": "MinIO Console",
            "url": "http://{}:{}".format(
                cluster_head_ip, _get_console_port(minio_config))
        },
    }
    return endpoints


def _get_head_service_ports(
        runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    minio_config = _get_config(runtime_config)
    if not _is_service_on_head(minio_config):
        return {}
    service_ports = {
        "minio": {
            "protocol": "TCP",
            "port": _get_service_port(minio_config),
        },
        "minio-console": {
            "protocol": "TCP",
            "port": _get_console_port(minio_config),
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    minio_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(minio_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, MINIO_SERVICE_TYPE)
    service_port = _get_service_port(minio_config)
    if _is_service_on_head(minio_config):
        service_node_kind = SERVICE_DISCOVERY_NODE_KIND_NODE
    else:
        service_node_kind = SERVICE_DISCOVERY_NODE_KIND_WORKER
    services = {
        service_name: define_runtime_service(
            MINIO_SERVICE_TYPE,
            service_discovery_config, service_port,
            node_kind=service_node_kind,
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_METRICS]),
    }
    return services


#######################################
# Calls from node when running services
#######################################

def _configure(runtime_config, head: bool):
    minio_config = _get_config(runtime_config)

    server_pool_size = _get_server_pool_size(minio_config)
    server_cluster_size = _get_server_cluster_size(minio_config)
    server_pools = server_cluster_size // server_pool_size
    if not server_pools:
        raise RuntimeError(
            "The number of nodes ({}) is less than server pool size: {}".format(
                server_cluster_size, server_pool_size))

    os.environ["MINIO_VOLUMES"] = _get_minio_volumes(
        minio_config, server_pools, server_pool_size)


def _get_minio_volumes(
        minio_config, server_pools, server_pool_size):
    workspace_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_WORKSPACE)
    cluster_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_CLUSTER)
    data_dir_spec = _get_data_dir_spec()

    server_pool_specs = []
    for server_pool_id in range(server_pools):
        server_pool_spec = _get_server_pool_spec(
            minio_config,
            server_pool_id, server_pool_size,
            workspace_name, cluster_name, data_dir_spec)
        server_pool_specs.append(server_pool_spec)
    return " ".join(server_pool_specs)


def _get_data_dir_spec():
    # List the existing data directories and get the data disk number
    data_disk_dirs = get_data_disk_dirs()
    if not data_disk_dirs:
        # use MinIO home data path if there is no disk mounted
        home_dir = _get_home_dir()
        return os.path.join(home_dir, "data")

    number_disk = len(data_disk_dirs)
    data_disk_prefix = os.path.join(
        CLOUDTIK_DATA_DISK_MOUNT_POINT, CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX)
    if number_disk > 1:
        return data_disk_prefix + "{1..." + str(number_disk) + "}/minio"
    else:
        return data_disk_prefix + str(number_disk) + "/minio"


def _get_server_pool_spec(
        minio_config,
        server_pool_id, server_pool_size,
        workspace_name, cluster_name, data_dir_spec):
    id_start = server_pool_id * server_pool_size
    if not _is_service_on_head(minio_config):
        # skip head node seq id
        id_start += 1

    if server_pool_size == 1:
        expansion = str(id_start + 1)
    else:
        id_end = id_start + server_pool_size
        expansion = "{" + str(id_start + 1) + "..." + str(id_end) + "}"

    node_name = get_cluster_node_name(cluster_name, expansion)
    hostname = get_dns_hostname_of_node(node_name, workspace_name)
    service_port = _get_service_port(minio_config)
    return "http://{}:{}{}".format(hostname, service_port, data_dir_spec)
