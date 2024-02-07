from typing import Any, Dict

from cloudtik.core._private.service_discovery.naming import is_resolvable_cluster_node_name
from cloudtik.core._private.util.core_utils import export_environment_variables
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HADOOP, BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.utils import \
    is_use_managed_cloud_storage, \
    get_runtime_config, PROVIDER_STORAGE_CONFIG_KEY, get_provider_config
from cloudtik.runtime.common.hadoop import with_remote_storage, with_storage_properties, HDFS_NAME_URI_KEY
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_hdfs_on_head, discover_hdfs_from_workspace, \
    is_hdfs_service_discovery, HDFS_URI_KEY, is_minio_service_discovery, MINIO_URI_KEY, discover_minio_from_workspace, \
    discover_minio_on_head, is_hdfs_name_service_discovery, discover_hdfs_name_from_workspace, \
    discover_hdfs_name_on_head


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HADOOP, {})


def _is_hdfs_name_capable(
        runtime_config: Dict[str, Any]) -> bool:
    if (not get_service_discovery_runtime(runtime_config)
            or not is_resolvable_cluster_node_name(runtime_config)):
        return False
    return True


def discover_hadoop_fs_from_workspace(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_hdfs_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)
    cluster_config = discover_minio_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)

    runtime_config = get_runtime_config(cluster_config)
    if _is_hdfs_name_capable(runtime_config):
        cluster_config = discover_hdfs_name_from_workspace(
            cluster_config, BUILT_IN_RUNTIME_HADOOP)
    return cluster_config


def discover_hadoop_fs_on_head(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_hdfs_on_head(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)
    cluster_config = discover_minio_on_head(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)

    runtime_config = get_runtime_config(cluster_config)
    if _is_hdfs_name_capable(runtime_config):
        cluster_config = discover_hdfs_name_on_head(
            cluster_config, BUILT_IN_RUNTIME_HADOOP)
    return cluster_config


def has_local_storage_in_cluster(runtime_config):
    if has_runtime_in_cluster(runtime_config, BUILT_IN_RUNTIME_HDFS):
        return True
    return False


def has_remote_storage(runtime_config):
    hadoop_config = _get_config(runtime_config)
    if (hadoop_config.get(HDFS_URI_KEY)
            or hadoop_config.get(MINIO_URI_KEY)):
        return True
    if (_is_hdfs_name_capable(runtime_config)
            and hadoop_config.get(HDFS_NAME_URI_KEY)):
        return True
    return False


def is_storage_service_discovery(runtime_config):
    hadoop_config = _get_config(runtime_config)
    if (is_hdfs_service_discovery(hadoop_config)
            or is_minio_service_discovery(hadoop_config)):
        return True
    if (_is_hdfs_name_capable(runtime_config)
            and is_hdfs_name_service_discovery(hadoop_config)):
        return True
    return False


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_hadoop_fs_from_workspace(
        cluster_config)
    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_hadoop_fs_on_head(
        cluster_config)

    # call validate config to fail earlier
    _validate_config(cluster_config, final=True)
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {"HADOOP_CLIENT": True}
    return runtime_envs


def _node_configure(runtime_config, head: bool):
    hadoop_config = _get_config(runtime_config)
    envs = {}
    hadoop_default_cluster = hadoop_config.get(
        "hadoop_default_cluster", False)
    if hadoop_default_cluster:
        envs["HADOOP_DEFAULT_CLUSTER"] = hadoop_default_cluster

    envs = with_remote_storage(hadoop_config, envs)
    # export storage properties if needed
    envs = with_storage_properties(runtime_config, envs)
    export_environment_variables(envs)


def _is_valid_storage_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    # if local storage enabled, we ignore the cloud storage configurations
    if has_local_storage_in_cluster(runtime_config):
        return True

    # check if there is remote storage configured
    if has_remote_storage(runtime_config):
        return True

    # Check any cloud storage is configured
    provider_config = get_provider_config(config)
    if (PROVIDER_STORAGE_CONFIG_KEY in provider_config or
            (not final and is_use_managed_cloud_storage(config))):
        return True

    # if there is service discovery mechanism, assume we can get from service discovery
    if (not final and
            get_service_discovery_runtime(runtime_config) and
            is_storage_service_discovery(runtime_config)):
        return True

    return False


def _validate_config(config: Dict[str, Any], final=False):
    if not _is_valid_storage_config(config, final=final):
        raise ValueError(
            "No storage configuration found for Hadoop.")
