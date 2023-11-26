import os

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


def configure_remote_storage(hadoop_config):
    hdfs_uri = hadoop_config.get(HDFS_URI_KEY)
    if hdfs_uri:
        os.environ["HDFS_NAMENODE_URI"] = hdfs_uri
    minio_uri = hadoop_config.get(MINIO_URI_KEY)
    if minio_uri:
        os.environ["MINIO_ENDPOINT_URI"] = minio_uri


def configure_storage_properties(runtime_config):
    hadoop_config = runtime_config.get(BUILT_IN_RUNTIME_HADOOP, {})
    if (hadoop_config.get(MINIO_URI_KEY) or
            has_runtime_in_cluster(runtime_config, BUILT_IN_RUNTIME_MINIO)):
        minio_storage = hadoop_config.get(MINIO_STORAGE_CONFIG_KEY, {})
        bucket = minio_storage.get(
            MINIO_STORAGE_BUCKET, MINIO_STORAGE_BUCKET_DEFAULT)
        access_key = minio_storage.get(
            MINIO_STORAGE_ACCESS_KEY, MINIO_STORAGE_ACCESS_KEY_DEFAULT)
        secret_key = minio_storage.get(
            MINIO_STORAGE_SECRET_KEY, MINIO_STORAGE_SECRET_KEY_DEFAULT)
        os.environ["MINIO_BUCKET"] = bucket
        os.environ["MINIO_ACCESS_KEY"] = access_key
        os.environ["MINIO_SECRET_KEY"] = secret_key
