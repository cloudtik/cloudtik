from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MINIO, BUILT_IN_RUNTIME_HADOOP
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster
from cloudtik.runtime.common.service_discovery.runtime_discovery import HDFS_URI_KEY, MINIO_URI_KEY

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
