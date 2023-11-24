import os
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_WORKSPACE, CLOUDTIK_RUNTIME_ENV_CLUSTER, \
    CLOUDTIK_DATA_DISK_MOUNT_POINT, CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MINIO
from cloudtik.core._private.runtime_utils import subscribe_nodes_info, \
    sort_nodes_by_seq_id, RUNTIME_NODE_SEQ_ID, get_runtime_value, get_data_disk_dirs
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime, \
    is_discoverable_cluster_node_name
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_PROTOCOL_HTTP, SERVICE_DISCOVERY_FEATURE_METRICS, \
    get_cluster_node_name
from cloudtik.core._private.utils import get_runtime_config
from cloudtik.runtime.common.service_discovery.consul import get_dns_hostname_of_node

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

MINIO_SERVICE_NAME = "minio"

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


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_MINIO)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"minio": logs_dir}


def _validate_config(config: Dict[str, Any], final=False):
    # Check Consul configured
    runtime_config = get_runtime_config(config)
    if not get_service_discovery_runtime(runtime_config):
        raise RuntimeError("MinIO needs Consul service discovery to be configured.")

    if is_discoverable_cluster_node_name(runtime_config):
        raise RuntimeError("MinIO needs sequential cluster node name from service discovery DNS.")


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    minio_config = _get_config(runtime_config)

    service_port = _get_service_port(minio_config)
    runtime_envs["MINIO_SERVICE_PORT"] = service_port

    console_port = _get_console_port(minio_config)
    runtime_envs["MINIO_CONSOLE_PORT"] = console_port

    return runtime_envs


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    minio_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(minio_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, MINIO_SERVICE_NAME)
    service_port = _get_service_port(minio_config)
    services = {
        service_name: define_runtime_service(
            service_discovery_config, service_port,
            protocol=SERVICE_DISCOVERY_PROTOCOL_HTTP,
            features=[SERVICE_DISCOVERY_FEATURE_METRICS]),
    }
    return services


#######################################
# Calls from node when running services
#######################################

def _configure(runtime_config, head: bool):
    minio_config = _get_config(runtime_config)
    os.environ["MINIO_VOLUMES"] = _get_minio_volumes(minio_config)


def _get_minio_volumes(minio_config):
    server_pool_size = _get_server_pool_size(minio_config)
    nodes_info = subscribe_nodes_info()
    # This method calls from node when configuring
    if nodes_info is None:
        if server_pool_size > 1:
            raise RuntimeError("Missing nodes for configuring MINIO deployment.")
        nodes_info = {}

    sorted_nodes_info = sort_nodes_by_seq_id(nodes_info)
    total_nodes = 1
    if sorted_nodes_info:
        # Use the max seq id as the number of works
        node_info = sorted_nodes_info[-1]
        max_seq_id = node_info[RUNTIME_NODE_SEQ_ID]
        total_nodes += max_seq_id

    server_pools = total_nodes // server_pool_size
    if not server_pools:
        raise RuntimeError(
            "The number of nodes ({}) is less than server pool size: ".format(
                total_nodes, server_pool_size))

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
    # List the existing data directories in mnt/cloudtik and get the data disk number
    # mnt/cloudtik/data-disk{1...4}/minio
    data_disk_dirs = get_data_disk_dirs()
    if not data_disk_dirs:
        # use MinIO home data path if there is no disk mounted
        home_dir = _get_home_dir()
        return os.path.join(home_dir, "data")

    number_disk = len(data_disk_dirs)
    data_disk_prefix = os.path.join(
        CLOUDTIK_DATA_DISK_MOUNT_POINT, CLOUDTIK_DATA_DISK_MOUNT_NAME_PREFIX)
    return data_disk_prefix + "{1..." + str(number_disk) + "}/minio"


def _get_server_pool_spec(
        minio_config,
        server_pool_id, server_pool_size,
        workspace_name, cluster_name, data_dir_spec):
    # http://cluster-name-{1...n}.workspace-name.cloudtik:9000/mnt/cloudtik/data-disk{1...n}/minio
    id_start = server_pool_id * server_pool_size
    id_end = id_start + server_pool_size
    expansion = "{" + str(id_start) + "..." + str(id_end) + "}"
    node_name = get_cluster_node_name(cluster_name, expansion)
    hostname = get_dns_hostname_of_node(node_name, workspace_name)
    service_port = _get_service_port(minio_config)
    return "http://{}:{}{}".format(hostname, service_port, data_dir_spec)
