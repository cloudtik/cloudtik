from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MINIO, BUILT_IN_RUNTIME_HADOOP, \
    BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.util.core_utils import address_string, service_address_from_string
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster

HDFS_SERVICE_PORT_DEFAULT = 9000
HDFS_NUM_NAME_NODES_LABEL = "hdfs-num-name-nodes"

HDFS_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS
HDFS_NAME_SERVICE_TYPE = BUILT_IN_RUNTIME_HDFS + "-name"

HDFS_NAME_URI_KEY = "hdfs_name_uri"
HDFS_NAME_SERVICE_DISCOVERY_KEY = "hdfs_name_service_discovery"
HDFS_NAME_SERVICE_SELECTOR_KEY = "hdfs_name_service_selector"

HDFS_URI_KEY = "hdfs_namenode_uri"
HDFS_SERVICE_DISCOVERY_KEY = "hdfs_service_discovery"
HDFS_SERVICE_SELECTOR_KEY = "hdfs_service_selector"

MINIO_URI_KEY = "minio_endpoint_uri"
MINIO_SERVICE_DISCOVERY_KEY = "minio_service_discovery"
MINIO_SERVICE_SELECTOR_KEY = "minio_service_selector"

MINIO_STORAGE_CONFIG_KEY = "minio_storage"
MINIO_STORAGE_BUCKET = "minio.bucket"
MINIO_STORAGE_ACCESS_KEY = "minio.access.key"
MINIO_STORAGE_SECRET_KEY = "minio.secret.key"

MINIO_STORAGE_BUCKET_DEFAULT = "default"
MINIO_STORAGE_ACCESS_KEY_DEFAULT = "minioadmin"
MINIO_STORAGE_SECRET_KEY_DEFAULT = "minioadmin"


def with_remote_storage(hadoop_config, envs=None):
    if envs is None:
        envs = {}
    hdfs_uri = hadoop_config.get(HDFS_URI_KEY)
    if hdfs_uri:
        envs["HDFS_NAMENODE_URI"] = hdfs_uri
    minio_uri = hadoop_config.get(MINIO_URI_KEY)
    if minio_uri:
        envs["MINIO_ENDPOINT_URI"] = minio_uri
    hdfs_name_uri = hadoop_config.get(HDFS_NAME_URI_KEY)
    if hdfs_name_uri:
        envs = with_hdfs_name_service(hdfs_name_uri, envs=envs)

    return envs


def with_hdfs_name_service(hdfs_name_uri, envs=None):
    if envs is None:
        envs = {}
    envs["HDFS_NAME_URI"] = hdfs_name_uri
    (name_service,
     num_name_nodes,
     name_service_port) = parse_hdfs_name_uri(hdfs_name_uri)
    envs["HDFS_NAME_SERVICE"] = name_service
    envs["HDFS_NUM_NAME_NODES"] = num_name_nodes
    envs["HDFS_SERVICE_PORT"] = name_service_port
    return envs


def with_storage_properties(runtime_config, envs=None):
    hadoop_config = runtime_config.get(BUILT_IN_RUNTIME_HADOOP, {})
    if (hadoop_config.get(MINIO_URI_KEY) or
            has_runtime_in_cluster(runtime_config, BUILT_IN_RUNTIME_MINIO)):
        if envs is None:
            envs = {}
        minio_storage = hadoop_config.get(MINIO_STORAGE_CONFIG_KEY, {})
        bucket = minio_storage.get(
            MINIO_STORAGE_BUCKET, MINIO_STORAGE_BUCKET_DEFAULT)
        access_key = minio_storage.get(
            MINIO_STORAGE_ACCESS_KEY, MINIO_STORAGE_ACCESS_KEY_DEFAULT)
        secret_key = minio_storage.get(
            MINIO_STORAGE_SECRET_KEY, MINIO_STORAGE_SECRET_KEY_DEFAULT)
        envs["MINIO_BUCKET"] = bucket
        envs["MINIO_ACCESS_KEY"] = access_key
        envs["MINIO_SECRET_KEY"] = secret_key
    return envs


def get_hdfs_name_service_address(name_cluster_name, num_name_nodes):
    return "{}@{}".format(name_cluster_name, num_name_nodes)


def get_hdfs_name_uri_of_service(service):
    # name uri in the format of: name-service-name@num-name-nodes:name-service-port
    name_cluster_name = service.cluster_name
    if not name_cluster_name:
        raise RuntimeError(
            "Service {} nodes are not come from the same cluster.".format(
                service.service_name))
    num_name_nodes = _get_num_name_nodes_from_service(service)
    if not num_name_nodes:
        raise RuntimeError(
            "Failed to get number of name nodes from service {}".format(
                service.service_name))
    name_service_address = get_hdfs_name_service_address(
        name_cluster_name, num_name_nodes)
    name_service_port = _get_name_service_port_from_service(service)
    hdfs_name_uri = address_string(
        name_service_address, name_service_port)
    return hdfs_name_uri


def _get_num_name_nodes_from_service(service):
    labels = service.labels
    if not labels:
        return None
    num_name_nodes_str = labels.get(HDFS_NUM_NAME_NODES_LABEL)
    if not num_name_nodes_str:
        return None
    return str(num_name_nodes_str)


def _get_name_service_port_from_service(service):
    service_addresses = service.service_addresses
    name_service_port = HDFS_SERVICE_PORT_DEFAULT
    if not service_addresses:
        return name_service_port
    # take one of them
    service_address = service_addresses[0]
    name_service_port = service_address[1]
    return name_service_port


def parse_hdfs_name_uri(hdfs_name_uri):
    # the name uri: name-service-name@num-name-nodes:name-service-port
    name_service_host, name_service_port = service_address_from_string(
        hdfs_name_uri, HDFS_SERVICE_PORT_DEFAULT)
    host_segments = name_service_host.split('@')
    n = len(host_segments)
    if n != 2:
        raise ValueError(
            "Invalid name uri format. Correct format: name-service-name@num-name-nodes")
    name_service_name = host_segments[0]
    num_name_nodes = str(host_segments[1])
    return name_service_name, num_name_nodes, name_service_port
