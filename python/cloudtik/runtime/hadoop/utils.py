import os
from typing import Any, Dict

from cloudtik.core._private.core_utils import get_env_string_value
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HADOOP, BUILT_IN_RUNTIME_HDFS
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.utils import \
    is_use_managed_cloud_storage, \
    get_runtime_config, PROVIDER_STORAGE_CONFIG_KEY
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_hdfs_on_head, discover_hdfs_from_workspace, \
    is_hdfs_service_discovery, HDFS_URI_KEY


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_HADOOP, {})


def _prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_config = discover_hdfs_from_workspace(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)

    return cluster_config


def _prepare_config_on_head(cluster_config: Dict[str, Any]):
    cluster_config = discover_hdfs_on_head(
        cluster_config, BUILT_IN_RUNTIME_HADOOP)

    # call validate config to fail earlier
    _validate_config(cluster_config, final=True)
    return cluster_config


def _with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    runtime_envs = {"HADOOP_CLIENT": True}
    return runtime_envs


def _configure(runtime_config, head: bool):
    hadoop_config = _get_config(runtime_config)

    hadoop_default_cluster = hadoop_config.get(
        "hadoop_default_cluster", False)
    if hadoop_default_cluster:
        os.environ["HADOOP_DEFAULT_CLUSTER"] = get_env_string_value(
            hadoop_default_cluster)

    hdfs_uri = hadoop_config.get(HDFS_URI_KEY)
    if hdfs_uri:
        os.environ["HDFS_NAMENODE_URI"] = hdfs_uri


def _is_valid_storage_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    # if HDFS enabled, we ignore the cloud storage configurations
    if has_runtime_in_cluster(runtime_config, BUILT_IN_RUNTIME_HDFS):
        return True
    # check if there is remote HDFS configured
    hadoop_config = _get_config(runtime_config)
    if hadoop_config.get(HDFS_URI_KEY) is not None:
        return True

    # Check any cloud storage is configured
    provider_config = config["provider"]
    if (PROVIDER_STORAGE_CONFIG_KEY in provider_config or
            (not final and is_use_managed_cloud_storage(config))):
        return True

    # if there is service discovery mechanism, assume we can get from service discovery
    if (not final and is_hdfs_service_discovery(hadoop_config) and
            get_service_discovery_runtime(runtime_config)):
        return True

    return False


def _validate_config(config: Dict[str, Any], final=False):
    if not _is_valid_storage_config(config, final=final):
        raise ValueError("No storage configuration found for Hadoop.")
